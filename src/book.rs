use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
};

use serde::{Deserialize, Serialize};

use crate::error::RejectReason;
use crate::market::{AccountId, Pair};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Bid, // Buy
    Ask, // Sell
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    Market, // execute immediately at the best available price
    Limit,  // execute at a specific price or better
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            OrderStatus::Open => "open",
            OrderStatus::PartiallyFilled => "partially_filled",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: u64, // assigned by the engine from a monotonic counter (0 = unassigned placeholder)
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
    pub remaining_size: u64,
    pub account_id: AccountId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderLocation {
    pub owner: AccountId,
    pub side: Side,
    pub price: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderRequest {
    pub pair: Pair,
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Trade {
    pub pair: Pair,
    pub price: u64,
    pub qty: u64,
    pub maker_id: u64,
    pub taker_id: u64,
    pub taker_side: Side,
    pub maker_account: AccountId,
    pub taker_account: AccountId,
}

/// Post-command state of one order. `filled_qty` is cumulative so consumers can
/// `SET` it (idempotent on redelivery) instead of incrementing.
#[derive(Debug, Clone, Copy)]
pub struct OrderUpdate {
    pub order_id: u64,
    pub account_id: AccountId,
    pub filled_qty: u64,
    pub remaining_size: u64,
    pub status: OrderStatus,
}

#[derive(Debug)]
pub struct MatchResponse {
    pub order_id: u64,
    pub trades: Vec<Trade>,
    /// The taker plus every maker this command touched.
    pub updates: Vec<OrderUpdate>,
    pub taker_remaining: u64,
    /// (side, price, new_qty) for every price level this command changed.
    pub book_deltas: Vec<(Side, u64, u64)>,
}

// Stores Orders in a BTreeMap as:
// key = price
// value = queue of orders
// Used btreemap to keep the orders sorted by price
#[derive(Debug, Serialize, Deserialize)]
pub struct OrderBook {
    pub bids: BTreeMap<u64, VecDeque<Order>>,
    pub asks: BTreeMap<u64, VecDeque<Order>>,
    pub order_index: HashMap<u64, OrderLocation>,
    /// (side, price) levels whose aggregate qty changed due to current command.
    /// Drained into BookDelta events by `take_dirty_levels` — this patter is same as
    /// `Ledger.dirty`.
    #[serde(skip)]
    pub dirty_levels: HashSet<(Side, u64)>,
    /// Anchors the price band. None until this market's first trade.
    pub last_trade_price: Option<u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            dirty_levels: HashSet::new(),
            last_trade_price: None,
        }
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    // returns filled quantity
    // `order_id` is assigned by the engine (global counter), not the book.
    pub(crate) fn add_order(
        &mut self,
        order_id: u64,
        account_id: AccountId,
        order_request: &OrderRequest,
    ) -> MatchResponse {
        let mut order = Order {
            id: order_id,
            account_id: account_id,
            order_type: order_request.order_type,
            side: order_request.side,
            price: order_request.price,
            size: order_request.size,
            remaining_size: order_request.size,
        };

        let mut trades = vec![];
        let mut updates: Vec<OrderUpdate> = vec![];

        while order.remaining_size > 0 {
            // get the queue for bid/ask orders with best price

            let best_price_queue = match order.side {
                Side::Bid => {
                    let Some((&lowest_ask_price, _)) = self.asks.first_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if lowest_ask_price > order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.asks.get_mut(&lowest_ask_price).unwrap()
                }
                Side::Ask => {
                    let Some((&highest_bid_price, _)) = self.bids.last_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if highest_bid_price < order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.bids.get_mut(&highest_bid_price).unwrap()
                }
            };

            while !best_price_queue.is_empty() && order.remaining_size > 0 {
                if let Some(best_price_order) = best_price_queue.front_mut() {
                    let trade_qty = min(order.remaining_size, best_price_order.remaining_size);
                    if order.remaining_size <= best_price_order.remaining_size {
                        best_price_order.remaining_size -= order.remaining_size;
                        order.remaining_size = 0;
                    } else {
                        order.remaining_size -= best_price_order.remaining_size;
                        best_price_order.remaining_size = 0;
                    }
                    let trade = Trade {
                        pair: order_request.pair,
                        price: best_price_order.price,
                        qty: trade_qty,
                        maker_id: best_price_order.id,
                        taker_id: order.id,
                        taker_side: order.side,
                        maker_account: best_price_order.account_id,
                        taker_account: order.account_id,
                    };
                    trades.push(trade);

                    self.last_trade_price = Some(trade.price);

                    // The maker's level changed size — mark it before it might
                    // get popped below. `take_dirty_levels` recomputes the
                    // CURRENT aggregate later, so this is correct whether the
                    // level survives, shrinks, or empties out.
                    self.dirty_levels
                        .insert((best_price_order.side, best_price_order.price));

                    // Capture the maker's post-fill state now — a filled maker is
                    // about to be dropped from the book.
                    updates.push(OrderUpdate {
                        order_id: best_price_order.id,
                        account_id: best_price_order.account_id,
                        filled_qty: best_price_order.size - best_price_order.remaining_size,
                        remaining_size: best_price_order.remaining_size,
                        status: if best_price_order.remaining_size == 0 {
                            OrderStatus::Filled
                        } else {
                            OrderStatus::PartiallyFilled
                        },
                    });

                    // remove order from queue if it is filled
                    if best_price_order.remaining_size == 0 {
                        self.order_index.remove(&best_price_order.id);
                        best_price_queue.pop_front();
                    }
                }
            }
            // remove first entry in map if its empty
            if best_price_queue.is_empty() {
                match order.side {
                    Side::Bid => {
                        self.asks.pop_first();
                    }
                    Side::Ask => {
                        self.bids.pop_last();
                    }
                }
            }
        }
        // handle when remaining size is more than 0
        if order.remaining_size > 0 {
            match order.order_type {
                OrderType::Limit => match order.side {
                    Side::Ask => {
                        let price = order.price; // read before `order` moves below
                        self.asks
                            .entry(price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
                        self.dirty_levels.insert((Side::Ask, price));
                    }
                    Side::Bid => {
                        let price = order.price;
                        self.bids
                            .entry(price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
                        self.dirty_levels.insert((Side::Bid, price));
                    }
                },
                OrderType::Market => {}
            }
        }
        if let OrderType::Limit = order.order_type {
            if order.remaining_size > 0 {
                self.order_index.insert(
                    order.id,
                    OrderLocation {
                        owner: account_id,
                        side: order.side,
                        price: order.price,
                    },
                );
            }
        }
        // Taker's own state. A market order's unfilled remainder never rests, so
        // it's terminal -> Cancelled. No update when a limit order simply rests
        // untouched: OrderAccepted already reported open/0.
        let taker_filled = order.size - order.remaining_size;
        let taker_status = if order.remaining_size == 0 {
            Some(OrderStatus::Filled)
        } else if order.order_type == OrderType::Market {
            Some(OrderStatus::Cancelled)
        } else if taker_filled > 0 {
            Some(OrderStatus::PartiallyFilled)
        } else {
            None
        };
        if let Some(status) = taker_status {
            updates.push(OrderUpdate {
                order_id: order.id,
                account_id: order.account_id,
                filled_qty: taker_filled,
                remaining_size: order.remaining_size,
                status,
            });
        }

        MatchResponse {
            order_id: order.id,
            trades,
            updates,
            taker_remaining: order.remaining_size,
            book_deltas: self.take_dirty_levels(),
        }
    }

    // Cost to fill up to `size` off the ask side at current levels, without
    // mutating anything. Stops early if asks run out before `size` does — the
    // caller treats that as "reserve for what's actually fillable."
    pub(crate) fn market_buy_cost(&self, size: u64) -> Result<u64, RejectReason> {
        let mut remaining = size;
        let mut cost: u64 = 0;
        for (&price, level) in self.asks.iter() {
            if remaining == 0 {
                break;
            }
            let level_qty: u64 = level.iter().map(|o| o.remaining_size).sum();
            let take = min(remaining, level_qty);
            let level_cost = price.checked_mul(take).ok_or(RejectReason::InvalidAmount)?;
            cost = cost
                .checked_add(level_cost)
                .ok_or(RejectReason::InvalidAmount)?;
            remaining -= take;
        }
        Ok(cost)
    }

    /// Would this order match one of the same account's resting orders?
    /// Mirrors `add_order`'s crossing rule, read-only.
    pub(crate) fn would_self_trade(&self, account_id: AccountId, req: &OrderRequest) -> bool {
        fn scan<'a>(
            levels: impl Iterator<Item = (&'a u64, &'a VecDeque<Order>)>,
            account_id: AccountId,
            size: u64,
            crosses: impl Fn(u64) -> bool,
        ) -> bool {
            let mut remaining = size;
            for (&price, level) in levels {
                if !crosses(price) {
                    return false;
                }
                for resting in level {
                    if resting.account_id == account_id {
                        return true;
                    }
                    remaining = remaining.saturating_sub(resting.remaining_size);
                    if remaining == 0 {
                        return false;
                    }
                }
            }
            false
        }

        let market = req.order_type == OrderType::Market;
        match req.side {
            Side::Bid => scan(self.asks.iter(), account_id, req.size, |p| {
                market || p <= req.price
            }),
            Side::Ask => scan(self.bids.iter().rev(), account_id, req.size, |p| {
                market || p >= req.price
            }),
        }
    }

    /// Drain the dirty-level set into `(side, price, new_qty)` triples,
    /// recomputing each level's CURRENT aggregate — correct regardless of
    /// whether the level survived, shrank, or emptied out since being marked.
    fn take_dirty_levels(&mut self) -> Vec<(Side, u64, u64)> {
        let dirty: Vec<(Side, u64)> = self.dirty_levels.drain().collect();
        dirty
            .into_iter()
            .map(|(side, price)| {
                let level = match side {
                    Side::Bid => self.bids.get(&price),
                    Side::Ask => self.asks.get(&price),
                };
                let qty = level
                    .map(|q| q.iter().map(|o| o.remaining_size).sum())
                    .unwrap_or(0);
                (side, price, qty)
            })
            .collect()
    }

    // Returns the removed order plus the (0 or 1) book deltas its removal caused.
    pub(crate) fn cancel_order(
        &mut self,
        account_id: AccountId,
        order_id: u64,
    ) -> Option<(Order, Vec<(Side, u64, u64)>)> {
        let Some(OrderLocation { owner, side, price }) = self.order_index.get(&order_id) else {
            return None;
        };

        if account_id != *owner {
            return None;
        }
        // Copy out before further mutable borrows of self.bids/self.asks.
        let side = *side;
        let price = *price;

        let Some(order_queue) = (match side {
            Side::Bid => self.bids.get_mut(&price),
            Side::Ask => self.asks.get_mut(&price),
        }) else {
            return None;
        };

        let Some(index) = order_queue.iter().position(|&order| order.id == order_id) else {
            return None;
        };
        let removed = order_queue.remove(index)?;
        if order_queue.is_empty() {
            match side {
                Side::Bid => {
                    self.bids.remove(&price);
                }
                Side::Ask => {
                    self.asks.remove(&price);
                }
            }
        };
        // remove order_id from the index/hashmap
        self.order_index.remove(&order_id);
        self.dirty_levels.insert((side, price));
        Some((removed, self.take_dirty_levels()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    // --- TEST 1: Basic Insertion (No Match) ---
    // Limit orders route to the correct side when the opposite side is empty.
    #[test]
    fn test_insert_limit_orders_no_match() {
        let mut book = new_book();

        let (bid_id, filled_bid) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        let (_ask_id, filled_ask) = place(&mut book, OrderType::Limit, Side::Ask, 110, 15);

        // Nothing crosses the spread, so nothing fills.
        assert_eq!(filled_bid, 0);
        assert_eq!(filled_ask, 0);

        // Both orders rest on their own side of the book.
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);
        assert_eq!(book.asks.get(&110).unwrap().len(), 1);

        // The resting order carries exactly the id the engine handed back to us.
        assert_eq!(book.bids.get(&100).unwrap()[0].id, bid_id);
    }

    // --- TEST 2: Exact Match & Price Level Cleanup ---
    // A perfect match fills both orders and removes the now-empty price level.
    #[test]
    fn test_exact_match_and_cleanup() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        // Incoming bid is fully filled.
        assert_eq!(filled, 10);

        // Empty price level must be removed from the BTreeMap (else it's a leak).
        assert!(
            book.asks.get(&100).is_none(),
            "Empty price levels must be removed from the BTreeMap"
        );
    }

    // --- TEST 3: Partial Fill (Incoming Order is Smaller) ---
    // The resting order stays at the front of the queue with a reduced size.
    #[test]
    fn test_partial_fill_incoming_smaller() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 20);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 5);

        // Incoming bid fully filled (5 of 5).
        assert_eq!(filled, 5);

        // Resting ask remains with 15 outstanding.
        assert_eq!(book.asks.get(&100).unwrap()[0].remaining_size, 15);
    }

    // --- TEST 4: Partial Fill (Incoming Order is Larger) ---
    // Incoming order eats the resting order; the remainder settles into the book.
    #[test]
    fn test_partial_fill_incoming_larger() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 25);

        // Only 10 available to match.
        assert_eq!(filled, 10);

        // Ask price level is consumed and gone.
        assert!(book.asks.get(&100).is_none());

        // Bid remainder (15) rests in the book.
        assert_eq!(book.bids.get(&100).unwrap()[0].remaining_size, 15);
    }

    // --- TEST 5: Time Priority (FIFO) ---
    // Orders at the same price match in arrival order.
    #[test]
    fn test_time_priority_fifo() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);

        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 15);
        assert_eq!(filled, 15);

        let ask_queue = book.asks.get(&100).unwrap();

        // ask1 fully consumed; ask2 partially (5 left) at the front; ask3 untouched.
        assert_eq!(ask_queue.len(), 2);
        assert_eq!(ask_queue[0].remaining_size, 5);
        assert_eq!(ask_queue[1].remaining_size, 10);
    }

    // --- TEST 6: Market Order Sweeps Multiple Price Levels ---
    #[test]
    fn test_market_order_sweep() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 105, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 110, 10);

        // Aggressive market buy for 25 (price is irrelevant for market orders).
        let (_id, filled) = place(&mut book, OrderType::Market, Side::Bid, 0, 25);
        assert_eq!(filled, 25);

        // First two levels destroyed.
        assert!(book.asks.get(&100).is_none());
        assert!(book.asks.get(&105).is_none());

        // Third level has 5 remaining.
        assert_eq!(book.asks.get(&110).unwrap()[0].remaining_size, 5);
    }

    // --- TEST 7: Market Order Liquidity Exhaustion ---
    // A market order larger than the whole book fills what it can and vanishes.
    #[test]
    fn test_market_order_exhausts_book() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);

        // Market buy for 50, but only 10 exist.
        let (_id, filled) = place(&mut book, OrderType::Market, Side::Bid, 0, 50);
        assert_eq!(filled, 10);

        // Book is empty and, crucially, the market order never rested.
        assert!(book.asks.is_empty());
        assert!(
            book.bids.is_empty(),
            "Market orders must not be placed in the BTreeMap"
        );
    }
}

#[cfg(test)]
mod cancel_tests {
    use super::*;
    use crate::test_util::*;

    // --- TEST 1: Basic Cancellation & Cleanup ---
    #[test]
    fn test_cancel_single_order() {
        let mut book = new_book();

        let (bid_id, _) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);

        assert!(book.cancel_order(1, bid_id).is_some());

        // Empty price level must be removed after cancellation.
        assert!(
            book.bids.get(&100).is_none(),
            "Empty price levels must be removed after cancellation"
        );
        assert!(book.bids.is_empty());
    }

    // --- TEST 2: Cancel from the Middle of a Queue ---
    // Cancelling one order preserves FIFO order of the rest.
    #[test]
    fn test_cancel_middle_of_queue() {
        let mut book = new_book();

        let (id1, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 10);
        let (id2, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 15);
        let (id3, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 20);

        assert_eq!(book.asks.get(&200).unwrap().len(), 3);

        assert!(book.cancel_order(1, id2).is_some());

        let queue = book.asks.get(&200).unwrap();
        assert_eq!(queue.len(), 2);

        // Time priority preserved for the survivors.
        assert_eq!(queue[0].id, id1);
        assert_eq!(queue[1].id, id3);
    }

    // --- TEST 3: Cancel a Non-Existent Order ---
    // Must return false and leave the book untouched.
    #[test]
    fn test_cancel_non_existent_order() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 150, 10);

        // An id the engine could never have assigned (ids start at 0 and climb).
        assert!(book.cancel_order(1, u64::MAX).is_none());

        assert_eq!(book.asks.get(&150).unwrap().len(), 1);
        assert_eq!(book.asks.get(&150).unwrap()[0].size, 10);
    }

    // --- TEST 4: Cancel a Partially Filled Order ---
    #[test]
    fn test_cancel_partially_filled_order() {
        let mut book = new_book();

        let (ask_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 20);

        // Partially fill the resting ask.
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 5);
        assert_eq!(filled, 5);
        assert_eq!(book.asks.get(&100).unwrap()[0].remaining_size, 15);

        // Cancel the remainder.
        assert!(book.cancel_order(1, ask_id).is_some());

        assert!(book.asks.is_empty());
        assert!(book.bids.is_empty());
    }

    // --- TEST 5: Cancel by a Non-Owner is Rejected ---
    // Authorization: only the account that placed an order may cancel it. A
    // stranger's cancel must be a no-op that leaves the order resting.
    #[test]
    fn test_cancel_by_non_owner_is_rejected() {
        let mut book = new_book();

        // `place` always places as account 1 (see `place_full`).
        let (bid_id, _) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        // Account 2 attempts to cancel account 1's order — rejected, untouched.
        assert!(book.cancel_order(2, bid_id).is_none());
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);

        // The rejection didn't corrupt state: the real owner can still cancel.
        assert!(book.cancel_order(1, bid_id).is_some());
        assert!(book.bids.is_empty());
    }
}

#[cfg(test)]
mod trade_tests {
    use super::*;
    use crate::test_util::*;

    // A single clean match: one trade at the maker's price, correct ids and side.
    #[test]
    fn test_trade_details_single_match() {
        let mut book = new_book();

        let (maker_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let result = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        assert_eq!(result.trades.len(), 1);
        let t = &result.trades[0];
        assert_eq!(t.qty, 10);
        assert_eq!(t.price, 100);
        assert_eq!(t.maker_id, maker_id);
        assert_eq!(t.taker_id, result.order_id);
        assert_eq!(t.taker_side, Side::Bid);
    }

    // THE money invariant: a taker that crosses the spread executes at the
    // MAKER'S resting price, never its own limit price.
    #[test]
    fn test_execution_price_is_maker_price() {
        let mut book = new_book();

        // Maker rests at 95.
        let (maker_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 95, 10);
        // Taker is willing to pay up to 100 but must fill at the maker's 95.
        let result = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        assert_eq!(result.trades.len(), 1);
        assert_eq!(
            result.trades[0].price, 95,
            "execution price must be the maker's resting price, not the taker's limit"
        );
        assert_eq!(result.trades[0].maker_id, maker_id);
    }

    // A market order sweeping three levels: three trades, each at its maker's price.
    #[test]
    fn test_market_sweep_trade_prices() {
        let mut book = new_book();

        let (m1, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (m2, _) = place(&mut book, OrderType::Limit, Side::Ask, 105, 10);
        let (m3, _) = place(&mut book, OrderType::Limit, Side::Ask, 110, 10);

        let result = place_full(&mut book, OrderType::Market, Side::Bid, 0, 25);

        assert_eq!(result.trades.len(), 3);

        // Prices follow the makers as the sweep goes deeper.
        assert_eq!(result.trades[0].price, 100);
        assert_eq!(result.trades[1].price, 105);
        assert_eq!(result.trades[2].price, 110);

        // Quantities: two full levels then a partial.
        assert_eq!(result.trades[0].qty, 10);
        assert_eq!(result.trades[1].qty, 10);
        assert_eq!(result.trades[2].qty, 5);

        // Makers appear in sweep order.
        assert_eq!(result.trades[0].maker_id, m1);
        assert_eq!(result.trades[1].maker_id, m2);
        assert_eq!(result.trades[2].maker_id, m3);

        // The total_cost the receipt would report.
        let total_cost: u64 = result.trades.iter().map(|t| t.qty * t.price).sum();
        assert_eq!(total_cost, 10 * 100 + 10 * 105 + 5 * 110); // 2625
    }

    // taker_side reflects the aggressor, so the trade tape can color the print.
    #[test]
    fn test_taker_side_reflects_aggressor() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Bid, 100, 10); // resting buy
        let result = place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // incoming sell

        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].taker_side, Side::Ask);
        assert_eq!(result.trades[0].price, 100);
    }
}

// Book deltas: the (side, price, new_qty) triples that feed the M6.4 book
// projection. The subtlety worth pinning down is that `dirty_levels` is a
// HashSet — so a level touched twice in one command (e.g. a sweep across two
// orders resting at the same price) still emits exactly ONE delta, and it
// must carry the aggregate qty as of the END of the command, not a snapshot
// from partway through matching.
#[cfg(test)]
mod book_delta_tests {
    use super::*;
    use crate::test_util::*;

    // A limit order with nothing to match against just rests — one delta, at
    // its own level, qty equal to its full size.
    #[test]
    fn test_resting_order_produces_one_book_delta() {
        let mut book = new_book();
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(res.book_deltas, vec![(Side::Bid, 100, 10)]);
    }

    // Sweeping two resting orders at the SAME price level must still produce
    // ONE delta for that level (not two), carrying the final aggregate —
    // proof that dirty_levels dedups and take_dirty_levels reads current state.
    #[test]
    fn test_sweep_across_same_level_dedups_to_one_delta() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // ask #1
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // ask #2

        // Bid for 6: fully consumes ask #1 (5), takes 1 from ask #2 (leaves 4).
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 6);

        // Fully filled — the bid itself never rests, so no Bid-side delta.
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 4)]);
    }

    // A partial fill leaves the maker's level non-zero — the delta must
    // report that remainder, not zero and not the pre-fill size.
    #[test]
    fn test_partial_fill_reports_remaining_aggregate() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 4);
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 6)]);
    }

    // A full fill empties the level: the delta's qty is 0 even though the
    // BTreeMap entry itself is removed (there's nothing left to sum).
    #[test]
    fn test_full_fill_reports_zero_qty() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 0)]);
        assert!(book.asks.get(&100).is_none());
    }

    // Cancelling the only order at a level reports qty 0 (level gone).
    #[test]
    fn test_cancel_last_order_at_level_reports_zero() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_, deltas) = book.cancel_order(1, 0).unwrap();
        assert_eq!(deltas, vec![(Side::Ask, 100, 0)]);
    }

    // Cancelling one of several orders at a level reports the survivors'
    // aggregate, not zero.
    #[test]
    fn test_cancel_one_of_several_reports_remaining_aggregate() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // id 0
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // id 1
        let (_, deltas) = book.cancel_order(1, 0).unwrap();
        assert_eq!(deltas, vec![(Side::Ask, 100, 5)]);
    }

    // A resting order untouched by a later command emits no delta for THAT
    // command — dirty_levels is drained per-command, not accumulated forever.
    #[test]
    fn test_unrelated_command_emits_no_stale_delta() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        // A second, unrelated resting order at a different level.
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 50, 3);
        assert_eq!(res.book_deltas, vec![(Side::Bid, 50, 3)]); // not the ask too
    }
}
