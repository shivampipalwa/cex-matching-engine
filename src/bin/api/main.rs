mod auth;
mod book;
mod ws;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use bigdecimal::BigDecimal;
use futures_util::StreamExt;
use jsonwebtoken::{DecodingKey, EncodingKey};
use matching_engine::{
    snapshot::SnapshotConfig,
    types::{
        AccountId, CancelRequest,
        Command::{self},
        CommandEnvelope,
        CommandResponse::{self},
        Currency, DepositRequest, EventBatch, OrderRequest, OrderType, Pair, ResponseEnvelope,
        Side, WithdrawRequest,
    },
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, oneshot},
    time::timeout,
};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

use crate::auth::{ApiError, AuthUser, ClientOrderId};

/// How long a handler waits for the engine's reply before giving up.
/// On timeout we return 504 — the command may still be durably logged and
/// to mitigate duplicate requests we use 'client_order_id'
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Ring buffer size for the websocket. A subscriber that falls
/// this many batches behind gets `Lagged` and is disconnected (see `ws.rs`)
/// rather than the channel growing unbounded.
const EVENT_BROADCAST_CAPACITY: usize = 1024;

/// Event batches between book-projection snapshots. Also gates how far
/// db_writer may trim `events`.
const BOOK_SNAPSHOT_EVERY: u64 = 5_000;

const DEFAULT_CANDLES_LIMIT: i64 = 200;
const MAX_CANDLES_LIMIT: i64 = 1000;

/// Correlation-id → one-shot waiting handler. The shared subscriber removes
/// an entry and fires its sender when the matching replies with the `results`.
/// std `Mutex`: every critical section is a single map op and we never hold
/// the guard across an `.await`.
type Pending = Arc<Mutex<HashMap<Uuid, oneshot::Sender<ResponseEnvelope>>>>;

struct Keys {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

#[derive(Clone)]
struct AppState {
    /// Cheap to clone and share; used by handlers to `XADD` commands.
    xadd_conn: MultiplexedConnection,
    pending: Pending,
    pg_pool: PgPool,
    keys: Arc<Keys>,
    /// In-memory order-book projection, fed by the `events` stream. See `book`.
    book_store: Arc<book::BookStore>,
    /// Websocket: very `events` batch, broadcast to whichever
    /// public/private feed connections currently subscribe. `Sender` is
    /// `Clone`; each websocket connection calls `.subscribe()` for its own
    /// `Receiver`. See `ws.rs`.
    event_tx: broadcast::Sender<Arc<EventBatch>>,
    /// The only account allowed to list/delist trading pairs. Env-configured
    /// rather than a real roles system — there's exactly one admin action.
    admin_account_id: AccountId,
}

/// Request body for `POST /orders`. Carries intent and the client-supplied idempotency key.
#[derive(Deserialize)]
struct PlaceOrderBody {
    pair: Pair,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
}

#[derive(Deserialize)]
struct DepositBody {
    amount: u64,
    currency: Currency,
}

#[derive(Deserialize)]
struct WithdrawBody {
    amount: u64,
    currency: Currency,
}

// Projection rows. Amounts are NUMERIC in Postgres; BigDecimal keeps them exact.
#[derive(Serialize, sqlx::FromRow)]
struct BalanceRow {
    currency: String,
    available: BigDecimal,
    reserved: BigDecimal,
}

#[derive(Serialize, sqlx::FromRow)]
struct OrderRow {
    order_id: BigDecimal,
    pair: String,
    side: String,
    order_type: String,
    price: BigDecimal,
    size: BigDecimal,
    filled_qty: BigDecimal,
    status: String,
}

#[derive(Deserialize)]
struct CandlesQuery {
    interval: String,
    start: Option<i64>,
    end: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize, sqlx::FromRow)]
struct CandleRow {
    time: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: i64,
}

fn interval_seconds(interval: &str) -> Option<i64> {
    Some(match interval {
        "1s" => 1,
        "15m" => 900,
        "1h" => 3600,
        "4h" => 14400,
        "1d" => 86400,
        "1w" => 604800,
        _ => return None,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let client = Client::open(redis_url)?;
    let xadd_conn = client.get_multiplexed_async_connection().await?;
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

    {
        let client = client.clone();
        let pending = pending.clone();
        tokio::spawn(async move {
            if let Err(e) = run_result_subscriber(client, pending).await {
                eprintln!("result subscriber terminated: {e}");
            }
        });
    }
    let pg_pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let admin_account_id: AccountId = std::env::var("ADMIN_ACCOUNT_ID")?.parse()?;
    let keys = Keys {
        encoding_key: EncodingKey::from_secret(&std::env::var("JWT_SECRET")?.as_bytes()),
        decoding_key: DecodingKey::from_secret(&std::env::var("JWT_SECRET")?.as_bytes()),
    };

    // Book projection: replay `events` once to build the book fully (same as
    // engine's recovery), THEN start serving — no request should ever
    // see a partially-built book.
    let book_store = Arc::new(book::BookStore::new());
    let mut book_conn = client.get_multiplexed_async_connection().await?;
    let book_snapshot = SnapshotConfig::from_env(
        "BOOK_SNAPSHOT_PATH",
        "BOOK_SNAPSHOT_EVERY",
        BOOK_SNAPSHOT_EVERY,
    );
    let last_id = book::bootstrap(&mut book_conn, &book_store, book_snapshot.as_ref()).await?;
    // Lagging receivers get dropped (see EVENT_BROADCAST_CAPACITY); the
    // initial receiver returned here is unused — every real subscriber comes
    // from `event_tx.subscribe()` in a websocket handler.
    let (event_tx, _) = broadcast::channel::<Arc<EventBatch>>(EVENT_BROADCAST_CAPACITY);
    {
        let store = book_store.clone();
        let event_tx = event_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = book::tail(book_conn, store, last_id, event_tx, book_snapshot).await {
                eprintln!("book projection terminated: {e}");
            }
        });
    }

    let state = AppState {
        xadd_conn,
        pending,
        pg_pool,
        keys: Arc::new(keys),
        book_store,
        event_tx,
        admin_account_id,
    };
    let app = Router::new()
        .route("/auth/signup", post(auth::signup))
        .route("/auth/login", post(auth::login))
        .route("/orders", post(place_order).get(get_orders))
        .route("/orders/:id", delete(cancel_order))
        .route("/deposits", post(deposit))
        .route("/withdrawals", post(withdraw))
        .route("/balances", get(get_balances))
        .route("/book/:pair", get(book::get_book))
        .route("/candles/:pair", get(get_candles))
        .route("/ws/market/:pair", get(ws::public_market))
        .route("/ws/orders", get(ws::private_orders))
        .route("/admin/pairs", post(list_pair))
        .route("/admin/pairs/:pair", delete(delist_pair))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    // 0.0.0.0 by default: inside a container, loopback is unreachable from
    // anywhere useful.
    let addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("API listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

/// for every request- one task, one pub/sub connection.
/// `SUBSCRIBE`s to `results` redis channel, reads the `correlation_id` from
/// the message body, and sends the reply back to the waiter via sender.
async fn run_result_subscriber(client: Client, pending: Pending) -> Result<(), redis::RedisError> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe("results").await?;
    let mut stream = pubsub.on_message();

    while let Some(msg) = stream.next().await {
        let Ok(payload) = msg.get_payload::<String>() else {
            continue;
        };
        let Ok(envelope) = serde_json::from_str::<ResponseEnvelope>(&payload) else {
            continue;
        };

        // Take the waiter out of the map and wake it. If it's absent the request
        // already timed out and cleaned up — silently drop the reply.
        let waiter = pending.lock().unwrap().remove(&envelope.correlation_id);
        if let Some(tx) = waiter {
            let _ = tx.send(envelope); // Err just means the receiver is gone.
        }
    }
    Ok(())
}

/// Log a command and wait for the engine's correlated reply.
async fn submit_command(
    state: &mut AppState,
    account_id: AccountId,
    client_order_id: u64,
    command: Command,
) -> Result<CommandResponse, ApiError> {
    let correlation_id = Uuid::new_v4();

    // Serialize before registering so a failure here can't orphan a waiter.
    let json_envelope = serde_json::to_string(&CommandEnvelope {
        correlation_id,
        account_id,
        client_order_id,
        command,
    })
    .map_err(|e| ApiError::Internal(e.to_string()))?;

    let (tx, rx) = oneshot::channel::<ResponseEnvelope>();
    state.pending.lock().unwrap().insert(correlation_id, tx);

    let xadd: Result<String, redis::RedisError> = state
        .xadd_conn
        .xadd("commands", "*", &[("data", json_envelope)])
        .await;
    if let Err(e) = xadd {
        state.pending.lock().unwrap().remove(&correlation_id);
        return Err(ApiError::Internal(e.to_string()));
    }

    match timeout(REQUEST_TIMEOUT, rx).await {
        Ok(Ok(reply)) => Ok(reply.response),
        // timed out, or the subscriber dropped our sender
        _ => {
            state.pending.lock().unwrap().remove(&correlation_id);
            Err(ApiError::Timeout)
        }
    }
}

/// The one place engine responses become HTTP responses.
fn response_for(resp: CommandResponse) -> Response {
    match resp {
        CommandResponse::Place(Ok(r)) => (StatusCode::OK, Json(r)).into_response(),
        CommandResponse::Place(Err(e)) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        CommandResponse::Cancel(true) => StatusCode::NO_CONTENT.into_response(),
        CommandResponse::Cancel(false) => ApiError::NotFound.into_response(),
        CommandResponse::Deposit(Ok(available)) => {
            (StatusCode::OK, Json(json!({ "available": available }))).into_response()
        }
        CommandResponse::Deposit(Err(e)) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        CommandResponse::Withdraw(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        CommandResponse::Withdraw(Err(e)) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        CommandResponse::ListPair(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        CommandResponse::ListPair(Err(e)) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        CommandResponse::DelistPair(true) => StatusCode::NO_CONTENT.into_response(),
        CommandResponse::DelistPair(false) => ApiError::NotFound.into_response(),
        CommandResponse::Duplicate => StatusCode::CONFLICT.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// `POST /orders`
async fn place_order(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Json(body): Json<PlaceOrderBody>,
) -> Response {
    let PlaceOrderBody {
        pair,
        order_type,
        side,
        price,
        size,
    } = body;
    let command = Command::Place(OrderRequest {
        pair,
        order_type,
        side,
        price,
        size,
    });
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

/// `DELETE /orders/:id` — the engine resolves the market and checks ownership.
async fn cancel_order(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Path(order_id): Path<u64>,
) -> Response {
    let command = Command::Cancel(CancelRequest { order_id });
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

/// `POST /deposits` — a dev affordance. A real exchange credits deposits from an
/// observed chain/bank event, never a client call.
async fn deposit(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Json(body): Json<DepositBody>,
) -> Response {
    let command = Command::Deposit(DepositRequest {
        amount: body.amount,
        currency: body.currency,
    });
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

/// `POST /withdrawals`
async fn withdraw(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Json(body): Json<WithdrawBody>,
) -> Response {
    let command = Command::Withdraw(WithdrawRequest {
        amount: body.amount,
        currency: body.currency,
    });
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

fn require_admin(state: &AppState, account_id: AccountId) -> Result<(), ApiError> {
    if account_id != state.admin_account_id {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

#[derive(Deserialize)]
struct ListPairBody {
    pair: Pair,
}

/// `POST /admin/pairs` — admin only. Opens a market for trading.
async fn list_pair(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Json(body): Json<ListPairBody>,
) -> Response {
    if let Err(e) = require_admin(&state, account_id) {
        return e.into_response();
    }
    let command = Command::ListPair(body.pair);
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

/// `DELETE /admin/pairs/:pair` — admin only. Closes a market to new orders.
async fn delist_pair(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    ClientOrderId(client_order_id): ClientOrderId,
    Path(raw_pair): Path<String>,
) -> Response {
    if let Err(e) = require_admin(&state, account_id) {
        return e.into_response();
    }
    let pair = match Pair::try_from(raw_pair) {
        Ok(p) => p,
        Err(_) => return ApiError::BadRequest("invalid pair, expected BASE-QUOTE").into_response(),
    };
    let command = Command::DelistPair(pair);
    match submit_command(&mut state, account_id, client_order_id, command).await {
        Ok(resp) => response_for(resp),
        Err(e) => e.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// `GET /balances`
async fn get_balances(
    State(state): State<AppState>,
    AuthUser(account_id): AuthUser,
) -> Result<Json<Vec<BalanceRow>>, ApiError> {
    let rows = sqlx::query_as::<_, BalanceRow>(
        "SELECT currency, available, reserved FROM balances
          WHERE account_id = $1 ORDER BY currency",
    )
    .bind(account_id as i64)
    .fetch_all(&state.pg_pool)
    .await?;
    Ok(Json(rows))
}

/// `GET /orders`
async fn get_orders(
    State(state): State<AppState>,
    AuthUser(account_id): AuthUser,
) -> Result<Json<Vec<OrderRow>>, ApiError> {
    let rows = sqlx::query_as::<_, OrderRow>(
        "SELECT order_id, pair, side, order_type, price, size, filled_qty, status
           FROM orders WHERE account_id = $1 ORDER BY order_id DESC",
    )
    .bind(account_id as i64)
    .fetch_all(&state.pg_pool)
    .await?;
    Ok(Json(rows))
}

/// `GET /candles/:pair`
async fn get_candles(
    State(state): State<AppState>,
    Path(raw_pair): Path<String>,
    Query(q): Query<CandlesQuery>,
) -> Result<Json<Vec<CandleRow>>, ApiError> {
    let pair = Pair::try_from(raw_pair)
        .map_err(|_| ApiError::BadRequest("invalid pair, expected BASE-QUOTE"))?;
    let seconds = interval_seconds(&q.interval).ok_or(ApiError::BadRequest(
        "invalid interval, expected one of 1s/15m/1h/4h/1d/1w",
    ))?;
    let limit = q
        .limit
        .unwrap_or(DEFAULT_CANDLES_LIMIT)
        .clamp(1, MAX_CANDLES_LIMIT);

    let rows = sqlx::query_as::<_, CandleRow>(
        "SELECT bucket AS time, open, high, low, close, volume FROM (
            SELECT
                (floor(extract(epoch FROM created_at) / $2::double precision) * $2::double precision)::bigint AS bucket,
                (array_agg(price ORDER BY created_at ASC))[1]::bigint AS open,
                max(price)::bigint AS high,
                min(price)::bigint AS low,
                (array_agg(price ORDER BY created_at DESC))[1]::bigint AS close,
                sum(qty)::bigint AS volume
            FROM trades
            WHERE pair = $1
              AND ($3::bigint IS NULL OR extract(epoch FROM created_at) >= $3)
              AND ($4::bigint IS NULL OR extract(epoch FROM created_at) < $4)
            GROUP BY 1
            ORDER BY bucket DESC
            LIMIT $5
        ) sub ORDER BY bucket ASC",
    )
    .bind(pair.to_string())
    .bind(seconds)
    .bind(q.start)
    .bind(q.end)
    .bind(limit)
    .fetch_all(&state.pg_pool)
    .await?;
    Ok(Json(rows))
}
