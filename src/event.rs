use serde::{Deserialize, Serialize};

use crate::book::{OrderStatus, OrderType, Side, Trade};
use crate::market::{AccountId, Currency, Pair};

// output event stream from engine for db writer, etc.
#[derive(Debug, Serialize, Deserialize)]
pub enum Event {
    Trade(Trade),
    BalanceChanged {
        account_id: AccountId,
        currency: Currency,
        available: u64,
        reserved: u64,
    },
    OrderAccepted {
        order_id: u64,
        account_id: AccountId,
        pair: Pair,
        side: Side,
        order_type: OrderType,
        price: u64,
        size: u64,
    },
    OrderCancelled {
        order_id: u64,
    },
    OrderUpdated {
        order_id: u64,
        account_id: AccountId,
        pair: Pair,
        filled_qty: u64,
        remaining_qty: u64,
        status: OrderStatus,
    },
    /// A price level's aggregate quantity changed. `qty` is the level's NEW
    /// total (remove level if qty=0)
    BookDelta {
        pair: Pair,
        side: Side,
        price: u64,
        qty: u64,
    },
}

/// One `events`-stream entry per command. `seq` is engine-assigned (its state, not
/// the Redis id) so it's deterministic under replay
#[derive(Debug, Serialize, Deserialize)]
pub struct EventBatch {
    pub seq: u64,
    pub events: Vec<Event>,
}
