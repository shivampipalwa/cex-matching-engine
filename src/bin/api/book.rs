//! In-memory order-book projection: consumes `BookDelta` events off the shared
//! `events` stream and maintains price-level aggregates per pair, entirely in
//! this process's memory. Chosen over a Postgres/Redis store because a snapshot
//! read is then a pure in-process memory read (no network hop), and it scales
//! for free with API instance count.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    sync::{Arc, RwLock},
};

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use matching_engine::types::{Event, EventBatch, Pair, Side};
use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamRangeReply, StreamReadOptions, StreamReadReply},
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::AppState;
use crate::auth::ApiError;

const DEFAULT_DEPTH: usize = 20;
const MAX_DEPTH: usize = 1000; // guard against a client asking for an absurd depth

/// One market's price-level aggregates. `qty` at a price is the level's
/// CURRENT total — mirrors `BookDelta`'s "set", never accumulated.
#[derive(Default)]
struct Book {
    bids: BTreeMap<u64, u64>, // ascending; best bid = LAST (highest)
    asks: BTreeMap<u64, u64>, // ascending; best ask = FIRST (lowest)
}

/// Books + the seq they're current as of, under ONE lock. A reader must never
/// see levels from one seq paired with a `last_seq` from another — the same
/// atomicity concern db_writer solves with a transaction, solved here with a
/// lock since this state lives in memory, not Postgres.
struct BookState {
    books: HashMap<Pair, Book>,
    last_seq: u64,
}

pub(crate) struct BookStore {
    state: RwLock<BookState>,
}

impl BookStore {
    pub(crate) fn new() -> Self {
        BookStore {
            state: RwLock::new(BookState {
                books: HashMap::new(),
                last_seq: 0,
            }),
        }
    }

    /// Apply one command's WHOLE batch while holding the write lock for the
    /// entire batch (not per-event) — a reader can never observe a state that
    /// reflects only some of a command's deltas.
    fn apply_batch(&self, batch: &EventBatch) {
        let mut state = self.state.write().unwrap();
        for event in &batch.events {
            if let Event::BookDelta {
                pair,
                side,
                price,
                qty,
            } = event
            {
                let book = state.books.entry(*pair).or_default();
                let side_book = match side {
                    Side::Bid => &mut book.bids,
                    Side::Ask => &mut book.asks,
                };
                if *qty == 0 {
                    side_book.remove(price);
                } else {
                    side_book.insert(*price, *qty);
                }
            }
        }
        // Advance on EVERY batch we've processed, not just ones with a
        // BookDelta — the reported sequence is "as of what point have we seen
        // everything", which is true regardless of whether anything changed.
        state.last_seq = batch.seq;
    }

    fn snapshot(&self, pair: Pair, depth: usize) -> BookSnapshot {
        let state = self.state.read().unwrap();
        let empty = Book::default();
        let book = state.books.get(&pair).unwrap_or(&empty);
        BookSnapshot {
            pair,
            sequence: state.last_seq,
            bids: book
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(&price, &qty)| BookLevel { price, qty })
                .collect(),
            asks: book
                .asks
                .iter()
                .take(depth)
                .map(|(&price, &qty)| BookLevel { price, qty })
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct BookLevel {
    price: u64,
    qty: u64,
}

#[derive(Serialize)]
pub(crate) struct BookSnapshot {
    pair: Pair,
    /// The EventBatch seq this snapshot reflects — a client bootstrapping a
    /// live book buffers deltas, fetches this snapshot, then discards
    /// buffered deltas with seq <= this one before applying the rest.
    sequence: u64,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

/// Replay the whole `events` stream once at startup to build the book from
/// scratch (mirrors the engine's own recovery). Returns the last entry's
/// stream id, so the live tail knows where to pick up from.
pub(crate) async fn bootstrap(
    conn: &mut MultiplexedConnection,
    store: &BookStore,
) -> Result<String, Box<dyn Error>> {
    let reply: StreamRangeReply = conn.xrange("events", "-", "+").await?;
    let mut last_id = "0".to_string();
    for entry in reply.ids {
        let Some(data): Option<String> = entry.get("data") else {
            continue;
        };
        let Ok(batch) = serde_json::from_str::<EventBatch>(&data) else {
            continue;
        };
        store.apply_batch(&batch);
        last_id = entry.id.clone();
    }
    Ok(last_id)
}

/// Live tail: plain `XREAD BLOCK` starting just after `last_id`. Also fans
/// every batch out to `event_tx` — the M7 websocket feeds' shared broadcast
/// channel — so there's still only one Redis connection / one task reading
/// `events` per API process; websockets are a second reader of the same tail,
/// not a second tailer.
pub(crate) async fn tail(
    mut conn: MultiplexedConnection,
    store: Arc<BookStore>,
    mut last_id: String,
    event_tx: broadcast::Sender<Arc<EventBatch>>,
) -> Result<(), Box<dyn Error>> {
    let opts = StreamReadOptions::default().block(5000);
    loop {
        let reply: StreamReadReply = conn
            .xread_options(&["events"], &[last_id.as_str()], &opts)
            .await?;
        for key in reply.keys {
            for entry in key.ids {
                last_id = entry.id.clone();
                let Some(data): Option<String> = entry.get("data") else {
                    continue;
                };
                let Ok(batch) = serde_json::from_str::<EventBatch>(&data) else {
                    continue;
                };
                store.apply_batch(&batch);
                // Errors only when there are currently zero receivers — not a
                // failure, just nobody subscribed at this instant.
                let _ = event_tx.send(Arc::new(batch));
            }
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct BookQuery {
    depth: Option<usize>,
}

/// `GET /book/:pair` — public market data, no auth. Parsed explicitly (rather
/// than `Path<Pair>`) so a malformed pair gets our own 400, not axum's default
/// path-rejection body.
pub(crate) async fn get_book(
    State(state): State<AppState>,
    Path(raw_pair): Path<String>,
    Query(q): Query<BookQuery>,
) -> Response {
    let pair = match Pair::try_from(raw_pair) {
        Ok(p) => p,
        Err(_) => return ApiError::BadRequest("invalid pair, expected BASE-QUOTE").into_response(),
    };
    let depth = q.depth.unwrap_or(DEFAULT_DEPTH).min(MAX_DEPTH);
    Json(state.book_store.snapshot(pair, depth)).into_response()
}
