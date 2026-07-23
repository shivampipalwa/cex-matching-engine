use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use matching_engine::types::{
    Command::{self},
    CommandEnvelope,
    CommandResponse::{self},
    Currency, OrderRequest, OrderType, ResponseEnvelope, Side,
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::Deserialize;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

/// How long a handler waits for the engine's correlated reply before giving up.
/// On timeout we return 504 — the command may still be durably logged and
/// execute later, which is exactly why the client sends a `client_order_id`
/// (its retry is deduped by the engine).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Correlation-id → the one-shot waiting handler. The shared subscriber removes
/// an entry and fires its sender when the matching reply lands on `results`.
/// A std `Mutex` is correct here: every critical section is a single map op and
/// we never hold the guard across an `.await`.
type Pending = Arc<Mutex<HashMap<Uuid, oneshot::Sender<ResponseEnvelope>>>>;

#[derive(Clone)]
struct AppState {
    /// Cheap to clone and share; used by handlers to `XADD` commands.
    xadd_conn: MultiplexedConnection,
    pending: Pending,
}

/// Request body for `POST /orders`. Carries *intent* plus the client-supplied
/// idempotency key.
///
/// NOTE: `account_id` is here only until M6.2 — once JWT auth lands, identity is
/// stamped from the verified token and this field comes off the body.
#[derive(Deserialize)]
struct PlaceOrderBody {
    account_id: u64,
    client_order_id: u64,
    base_currency: Currency,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::open("redis://127.0.0.1:6379/")?;
    let xadd_conn = client.get_multiplexed_async_connection().await?;
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

    // Start the shared result subscriber BEFORE we serve, so no reply can be
    // missed for a request accepted the instant the server comes up.
    {
        let client = client.clone();
        let pending = pending.clone();
        tokio::spawn(async move {
            if let Err(e) = run_result_subscriber(client, pending).await {
                eprintln!("result subscriber terminated: {e}");
            }
        });
    }

    let state = AppState { xadd_conn, pending };
    let app = Router::new()
        .route("/orders", post(place_order))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("API listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

/// One task, one pub/sub connection, for every in-flight request. It plainly
/// `SUBSCRIBE`s the single `results` channel, reads the `correlation_id` from
/// the message body (not the channel name), and hands the reply to the waiter.
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

/// `POST /orders` — turn the request into a command, ride the correlation flow,
/// and map the engine's reply to an HTTP status.
///
/// ---- IMPLEMENT ME (M6.1) ----
/// You'll want these extra imports:
///   matching_engine::types::{Command, CommandEnvelope, OrderRequest, CommandResponse}
///   redis::AsyncCommands            // for `.xadd`
///   tokio::time::timeout
///
/// Steps:
///   1. Build the intent-only command:
///        Command::Place(OrderRequest { base_currency, order_type, side, price, size })
///   2. Mint `let correlation_id = Uuid::new_v4();` and
///        `let (tx, rx) = oneshot::channel();`
///   3. REGISTER before you publish — insert `tx` into `state.pending` under
///      `correlation_id` NOW. (Register-before-XADD: the engine can publish the
///      reply before you'd otherwise be listening.)
///   4. `XADD commands` a serialized CommandEnvelope { correlation_id,
///      account_id: body.account_id, client_order_id: body.client_order_id,
///      command }. Clone `state.xadd_conn` for a `&mut`.
///   5. `match timeout(REQUEST_TIMEOUT, rx).await` — on `Err(_)` (timed out) or
///      `Ok(Err(_))` (sender dropped), REMOVE your entry from `state.pending`
///      (or it leaks) and return `StatusCode::GATEWAY_TIMEOUT`.
///   6. Map `envelope.response` (a `CommandResponse`) to a response:
///        Place(Ok(r))   -> (StatusCode::OK, Json(r))
///        Place(Err(e))  -> (StatusCode::BAD_REQUEST, Json(e))
///        Duplicate      -> StatusCode::CONFLICT
///        _              -> StatusCode::INTERNAL_SERVER_ERROR  // unreachable here
async fn place_order(
    State(mut state): State<AppState>,
    Json(body): Json<PlaceOrderBody>,
) -> impl IntoResponse {
    // let _ = (&state, &body); // silence unused until the flow is wired
    let PlaceOrderBody {
        account_id,
        client_order_id,
        base_currency,
        order_type,
        side,
        price,
        size,
    } = body;
    let command = Command::Place(OrderRequest {
        base_currency,
        order_type,
        side,
        price,
        size,
    });
    let correlation_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel::<ResponseEnvelope>();
    let Ok(json_envelope) = serde_json::to_string(&CommandEnvelope {
        correlation_id,
        account_id,
        client_order_id,
        command,
    }) else {
        return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    // Drop mutex so the lock is not held across await
    {
        let mut map = state.pending.lock().unwrap();
        map.insert(correlation_id, tx);
    }

    let Ok(_): Result<String, redis::RedisError> = state
        .xadd_conn
        .xadd("commands", "*", &[("data", json_envelope)])
        .await
    else {
        state.pending.lock().unwrap().remove(&correlation_id);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    match timeout(REQUEST_TIMEOUT, rx).await {
        Ok(Ok(reply)) => response_for(reply.response),
        _ => {
            state.pending.lock().unwrap().remove(&correlation_id);
            StatusCode::GATEWAY_TIMEOUT.into_response()
        }
    }

    // StatusCode::NOT_IMPLEMENTED
}

fn response_for(resp: CommandResponse) -> Response {
    match resp {
        CommandResponse::Place(Ok(r)) => (StatusCode::OK, Json(r)).into_response(),
        CommandResponse::Place(Err(e)) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        CommandResponse::Duplicate => StatusCode::CONFLICT.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
