mod auth;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use jsonwebtoken::{DecodingKey, EncodingKey};
use matching_engine::types::{
    Command::{self},
    CommandEnvelope,
    CommandResponse::{self},
    Currency, OrderRequest, OrderType, ResponseEnvelope, Side,
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::Deserialize;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

use crate::auth::AuthUser;

/// How long a handler waits for the engine's reply before giving up.
/// On timeout we return 504 — the command may still be durably logged and
/// to mitigate duplicate requests we use 'client_order_id'
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

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
}

/// Request body for `POST /orders`. Carries intent and the client-supplied idempotency key.
#[derive(Deserialize)]
struct PlaceOrderBody {
    client_order_id: u64,
    base_currency: Currency,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let client = Client::open("redis://127.0.0.1:6379/")?;
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
    let keys = Keys {
        encoding_key: EncodingKey::from_secret(&std::env::var("JWT_SECRET")?.as_bytes()),
        decoding_key: DecodingKey::from_secret(&std::env::var("JWT_SECRET")?.as_bytes()),
    };
    let state = AppState {
        xadd_conn,
        pending,
        pg_pool,
        keys: Arc::new(keys),
    };
    let app = Router::new()
        .route("/auth/signup", post(auth::signup))
        .route("/auth/login", post(auth::login))
        .route("/orders", post(place_order))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("API listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

/// for every request- one task, one pub/sub connection.
/// `SUBSCRIBE`s to `results` channel, reads the `correlation_id` from
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

/// `POST /orders`
async fn place_order(
    State(mut state): State<AppState>,
    AuthUser(account_id): AuthUser,
    Json(body): Json<PlaceOrderBody>,
) -> impl IntoResponse {
    let PlaceOrderBody {
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
