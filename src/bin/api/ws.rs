//! Websocket feeds. Both read the same `broadcast::Sender<Arc<EventBatch>>`
//! that `book::tail` emit every `events`-stream batch into — one Redis
//! connection feeds the in-memory book projection AND every open websocket

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Response},
};
use matching_engine::types::{AccountId, Event, EventBatch, Pair, Side};
use serde::{Deserialize, Serialize};
use tokio::{sync::broadcast, time::timeout};

use crate::AppState;
use crate::auth::ApiError;

// Public feed: GET /ws/market/:pair — book deltas + trade tape, no auth.
// One connection = one pair (mirrors GET /book/:pair).

/// Wire format for the public feed — deliberately NOT the internal `Event`
/// type. `Event::Trade` carries `maker_account`/`taker_account`; broadcasting
/// that as-is would leak who traded with whom. `seq` is the source
/// `EventBatch.seq`, the same number `GET /book/:pair` returns as
/// `sequence` — a client reconciles by subscribing first, buffering these,
/// then fetching the REST snapshot and dropping everything with `seq <=
/// sequence` before applying the rest live.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MarketMessage {
    BookDelta {
        seq: u64,
        side: Side,
        price: u64,
        qty: u64,
    },
    Trade {
        seq: u64,
        price: u64,
        qty: u64,
        taker_side: Side,
    },
}

/// `GET /ws/market/:pair` — public market data, no auth (same trust level as
/// `GET /book/:pair`). Pair is parsed before the upgrade so a bad pair gets an
/// ordinary 400 instead of failing after the socket is already open.
pub(crate) async fn public_market(
    State(state): State<AppState>,
    Path(raw_pair): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let pair = match Pair::try_from(raw_pair) {
        Ok(p) => p,
        Err(_) => return ApiError::BadRequest("invalid pair, expected BASE-QUOTE").into_response(),
    };
    // Subscribe before upgrading: once we commit to serving this connection
    // we don't want to risk missing a batch between upgrade and subscribe.
    let rx = state.event_tx.subscribe();
    ws.on_upgrade(move |socket| public_market_loop(socket, rx, pair))
}

async fn public_market_loop(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<Arc<EventBatch>>,
    pair: Pair,
) {
    loop {
        let batch = match rx.recv().await {
            Ok(batch) => batch,
            // Lagged (fell behind the broadcast buffer) or Closed (sender
            // dropped — process shutting down). Either way this connection's
            // view may now have a gap it can't detect from here, so close it
            // rather than guess — the client reconnects and re-snapshots.
            Err(_) => break,
        };
        for event in &batch.events {
            let msg = match event {
                Event::BookDelta {
                    pair: p,
                    side,
                    price,
                    qty,
                } if *p == pair => MarketMessage::BookDelta {
                    seq: batch.seq,
                    side: *side,
                    price: *price,
                    qty: *qty,
                },
                Event::Trade(t) if t.pair == pair => MarketMessage::Trade {
                    seq: batch.seq,
                    price: t.price,
                    qty: t.qty,
                    taker_side: t.taker_side,
                },
                _ => continue,
            };
            let text = serde_json::to_string(&msg).expect("MarketMessage always serializes");
            if socket.send(Message::Text(text)).await.is_err() {
                return; // client gone
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Private feed: GET /ws/orders — this account's order lifecycle (requires auth)
// ---------------------------------------------------------------------------

const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct AuthMessage {
    token: String,
}

/// `GET /ws/orders` — private, auth required. The upgrade itself carries no
/// credentials: a browser's `WebSocket` constructor can't set an
/// `Authorization` header, so unlike every REST write endpoint this can't
/// reuse the `AuthUser` extractor directly. Instead the upgrade always
/// succeeds and the FIRST frame the client sends must be `{"token":
/// "<jwt>"}`, verified with the same `verify()` `AuthUser` uses — just read
/// from a message instead of a header. No valid token within
/// `AUTH_TIMEOUT` closes the socket.
pub(crate) async fn private_orders(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| private_orders_loop(socket, state))
}

async fn private_orders_loop(mut socket: WebSocket, state: AppState) {
    let Some(account_id) = authenticate(&mut socket, &state).await else {
        let _ = socket.send(Message::Close(None)).await;
        return;
    };

    // Subscribe only after auth succeeds — an unauthenticated connection
    // shouldn't be holding a broadcast receiver slot.
    let mut rx = state.event_tx.subscribe();
    loop {
        let batch = match rx.recv().await {
            Ok(batch) => batch,
            Err(_) => break,
        };
        for event in &batch.events {
            let owner = match event {
                Event::OrderAccepted {
                    account_id: owner, ..
                } => owner,
                Event::OrderUpdated {
                    account_id: owner, ..
                } => owner,
                _ => continue,
            };
            if *owner != account_id {
                continue;
            }
            // The raw `Event` is fine to forward as-is here — unlike the
            // public feed, this connection is already scoped to one account,
            // so there's no cross-account field to redact.
            let text = serde_json::to_string(event).expect("Event always serializes");
            if socket.send(Message::Text(text)).await.is_err() {
                return;
            }
        }
    }
}

/// Wait for `{"token": "<jwt>"}` as the first frame and verify it.
async fn authenticate(socket: &mut WebSocket, state: &AppState) -> Option<AccountId> {
    let Ok(Some(Ok(Message::Text(text)))) = timeout(AUTH_TIMEOUT, socket.recv()).await else {
        return None;
    };
    let auth: AuthMessage = serde_json::from_str(&text).ok()?;
    crate::auth::verify(&auth.token, &state.keys.decoding_key)
        .ok()
        .map(|claims| claims.sub)
}
