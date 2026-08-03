// ---------------------------------------------------------------------------
// Test helpers, shared by the book and engine test modules.
//
// The golden rule here: NO test calls `add_order` / builds an `Order` directly.
// Everything routes through `place(...)`. That way, when `add_order`'s signature
// changes in later milestones (M1 makes it emit trades instead of a filled
// quantity), this ONE helper is the only thing that needs updating — not the
// dozens of call sites in the tests.
// ---------------------------------------------------------------------------

use std::ops::{Deref, DerefMut};

use crate::book::{MatchResponse, OrderBook, OrderRequest, OrderType, Side};
use crate::engine::Engine;
use crate::market::{Currency, Pair};

pub(crate) const TEST_PAIR: Pair = Pair {
    base: Currency::SOL,
    quote: Currency::USD,
};

/// A book plus the id counter the engine normally owns, so book-only tests
/// can keep calling `place(...)`. Derefs to `OrderBook`.
pub(crate) struct TestBook {
    pub book: OrderBook,
    pub next_id: u64,
}

impl Deref for TestBook {
    type Target = OrderBook;
    fn deref(&self) -> &OrderBook {
        &self.book
    }
}
impl DerefMut for TestBook {
    fn deref_mut(&mut self) -> &mut OrderBook {
        &mut self.book
    }
}

pub(crate) fn new_book() -> TestBook {
    TestBook {
        book: OrderBook::new(),
        next_id: 0,
    }
}

/// A fresh `Engine` (no books + empty ledger) for the ledger/settlement
/// tests that need the full reserve → match → settle path.
pub(crate) fn new_engine() -> Engine {
    let mut engine = Engine::new();
    engine.listed_pairs.insert(TEST_PAIR);
    engine
}

/// Places an order and returns the full `MatchResponse` (order id + trades).
/// Use this when a test needs to assert on the emitted trades themselves —
/// execution price, maker/taker ids, taker side.
pub(crate) fn place_full(
    book: &mut TestBook,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
) -> MatchResponse {
    // Book-only matching tests don't touch the ledger, so a fixed account
    // and pair are fine here — they're just carried into trades.
    let order_request = OrderRequest {
        pair: TEST_PAIR,
        order_type,
        side,
        price,
        size,
    };
    let id = book.next_id;
    book.next_id += 1;
    book.book.add_order(id, 1, &order_request)
}

/// Convenience wrapper over `place_full` returning just
/// `(assigned_order_id, filled_quantity)` for tests that only care about
/// quantities.
///
/// - `assigned_order_id` is the id the engine stamped on the order — always
///   read it from here, never hardcode it or read it before `add_order`.
/// - `filled_quantity` is how much of the incoming order matched.
pub(crate) fn place(
    book: &mut TestBook,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
) -> (u64, u64) {
    let result = place_full(book, order_type, side, price, size);
    let filled = result.trades.iter().map(|t| t.qty).sum();
    (result.order_id, filled)
}
