use std::collections::{BTreeMap, VecDeque};

use inline_string::InlineString;

#[derive(Clone, Copy, Debug)]
enum Side {
    Bid, // Buy
    Ask, // Sell
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OrderType {
    Market, // execute immediately at the best available price
    Limit,  // execute at a specific price or better
}

#[derive(Clone, Copy, Debug)]
struct Order {
    id: InlineString<32>,
    order_type: OrderType,
    side: Side,
    price: u64,
    size: u64,
    remaining_size: u64,
}

// Stores Orders in a BTreeMap as:
// key = price
// value = queue of orders
// Used btreemap to keep the orders sorted by price
#[derive(Debug)]
struct OrderBook {
    bids: BTreeMap<u64, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
}

impl OrderBook {
    fn add_order(&mut self, order: &mut Order) -> u64 {
        while order.remaining_size > 0 {
            // get the queue for bid/ask orders with best price
            let mut best_price_queue = match order.side {
                Side::Bid => {
                    let Some((&lowest_ask_price, _)) = self.asks.first_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if lowest_ask_price > order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.asks.entry(lowest_ask_price).or_insert_with(|| {
                        unreachable!("self.asks is guaranteed to have this price")
                    })
                }
                Side::Ask => {
                    let Some((&highest_bid_price, _)) = self.bids.last_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if highest_bid_price < order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.bids.entry(highest_bid_price).or_insert_with(|| {
                        unreachable!("self.asks is guaranteed to have this price")
                    })
                }
            };

            while !best_price_queue.is_empty() && order.remaining_size > 0 {
                if let Some(best_price_order) = best_price_queue.front_mut() {
                    if order.remaining_size <= best_price_order.remaining_size {
                        best_price_order.remaining_size -= order.remaining_size;
                        order.remaining_size = 0;
                    } else {
                        order.remaining_size -= best_price_order.remaining_size;
                        best_price_order.remaining_size = 0;
                    }
                    // remove ask order from queue if it is filled
                    if best_price_order.remaining_size == 0 {
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
                        self.bids.pop_first();
                    }
                }
            }
        }
        // handle when remaining size is more than 0
        if order.remaining_size > 0 {
            match order.order_type {
                OrderType::Limit => match order.side {
                    Side::Ask => {
                        self.asks
                            .entry(order.price)
                            .or_insert(VecDeque::new())
                            .push_back(*order);
                    }
                    Side::Bid => {
                        self.bids
                            .entry(order.price)
                            .or_insert(VecDeque::new())
                            .push_back(*order);
                    }
                },
                OrderType::Market => {}
            }
        }
        order.remaining_size
    }
}

fn main() {
    // let order_book = OrderBook::
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;

    // --- HELPER FUNCTION ---
    // A quick way to spin up orders so our tests aren't buried in boilerplate.
    // Note: Assuming InlineString<32> implements From<&str> or similar.
    // Adjust the `id` field initialization to match your specific crate.
    fn make_order(id: &str, order_type: OrderType, side: Side, price: u64, size: u64) -> Order {
        Order {
            id: InlineString::from_str(id).unwrap(),
            order_type,
            side,
            price,
            size,
            remaining_size: size,
        }
    }

    fn new_book() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    // --- TEST 1: Basic Insertion (No Match) ---
    // Tests that limit orders correctly route to the right side of the book
    // when the opposite side is empty.
    #[test]
    fn test_insert_limit_orders_no_match() {
        let mut book = new_book();

        let mut bid_order = make_order("bid_1", OrderType::Limit, Side::Bid, 100, 10);
        let rem_bid = book.add_order(&mut bid_order);

        let mut ask_order = make_order("ask_1", OrderType::Limit, Side::Ask, 110, 15);
        let rem_ask = book.add_order(&mut ask_order);

        // Orders should remain fully unfilled
        assert_eq!(rem_bid, 10);
        assert_eq!(rem_ask, 15);

        // State verification
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);
        assert_eq!(book.asks.get(&110).unwrap().len(), 1);
        assert_eq!(book.bids.get(&100).unwrap()[0].id.as_str(), "bid_1"); // Adjust string check based on your crate
    }

    // --- TEST 2: Exact Match & Price Level Cleanup ---
    // Tests that when two orders perfectly match, they are filled,
    // and critically, the empty price level is removed from the BTreeMap.
    #[test]
    fn test_exact_match_and_cleanup() {
        let mut book = new_book();

        // 1. Add a resting Ask at 100 for 10 units
        let mut ask = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        // 2. Add an incoming Bid at 100 for 10 units
        let mut bid = make_order("bid_1", OrderType::Limit, Side::Bid, 100, 10);
        let rem = book.add_order(&mut bid);

        // Incoming order is fully filled
        assert_eq!(rem, 0);
        assert_eq!(bid.remaining_size, 0);

        // MENTOR CHECK: If the queue is empty, the key should be removed from the BTreeMap!
        // If this fails, you have a memory leak in your orderbook design.
        assert!(
            book.asks.get(&100).is_none(),
            "Empty price levels must be removed from the BTreeMap"
        );
    }

    // --- TEST 3: Partial Fill (Incoming Order is Smaller) ---
    // Tests that the resting order stays at the front of the queue with a reduced size.
    #[test]
    fn test_partial_fill_incoming_smaller() {
        let mut book = new_book();

        let mut ask = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 20);
        book.add_order(&mut ask);

        let mut bid = make_order("bid_1", OrderType::Limit, Side::Bid, 100, 5);
        let rem = book.add_order(&mut bid);

        // Incoming Bid is fully filled
        assert_eq!(rem, 0);

        // Resting Ask should still be in the book with 15 remaining
        let resting_ask = &book.asks.get(&100).unwrap()[0];
        assert_eq!(resting_ask.remaining_size, 15);
    }

    // --- TEST 4: Partial Fill (Incoming Order is Larger) ---
    // Tests that the incoming order eats the resting order and the remainder
    // settles into the book.
    #[test]
    fn test_partial_fill_incoming_larger() {
        let mut book = new_book();

        let mut ask = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        let mut bid = make_order("bid_1", OrderType::Limit, Side::Bid, 100, 25);
        let rem = book.add_order(&mut bid);

        // 10 units filled, 15 remaining
        assert_eq!(rem, 15);
        assert_eq!(bid.remaining_size, 15);

        // Ask price level should be gone
        assert!(book.asks.get(&100).is_none());

        // Bid remainder should be resting in the book
        assert_eq!(book.bids.get(&100).unwrap()[0].remaining_size, 15);
    }

    // --- TEST 5: Time Priority (FIFO) ---
    // Crucial for exchanges. Orders at the same price must be matched in the order they arrived.
    #[test]
    fn test_time_priority_fifo() {
        let mut book = new_book();

        // Add three Asks at the exact same price
        let mut ask1 = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 10);
        let mut ask2 = make_order("ask_2", OrderType::Limit, Side::Ask, 100, 10);
        let mut ask3 = make_order("ask_3", OrderType::Limit, Side::Ask, 100, 10);

        book.add_order(&mut ask1);
        book.add_order(&mut ask2);
        book.add_order(&mut ask3);

        // Incoming Bid for 15 units
        let mut bid = make_order("bid_1", OrderType::Limit, Side::Bid, 100, 15);
        book.add_order(&mut bid);

        let ask_queue = book.asks.get(&100).unwrap();

        // ask_1 should be completely gone
        // ask_2 should be partially filled (5 remaining) and now at the front of the queue
        // ask_3 should be completely untouched (10 remaining)
        assert_eq!(ask_queue.len(), 2);
        assert_eq!(ask_queue[0].id.as_str(), "ask_2");
        assert_eq!(ask_queue[0].remaining_size, 5);
        assert_eq!(ask_queue[1].id.as_str(), "ask_3");
        assert_eq!(ask_queue[1].remaining_size, 10);
    }

    // --- TEST 6: Market Order Sweeps Multiple Price Levels ---
    // Tests that market orders cross the spread and consume liquidity aggressively.
    #[test]
    fn test_market_order_sweep() {
        let mut book = new_book();

        // Sellers resting at increasing prices
        let mut ask1 = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 10);
        let mut ask2 = make_order("ask_2", OrderType::Limit, Side::Ask, 105, 10);
        let mut ask3 = make_order("ask_3", OrderType::Limit, Side::Ask, 110, 10);

        book.add_order(&mut ask1);
        book.add_order(&mut ask2);
        book.add_order(&mut ask3);

        // Aggressive Market Buy for 25 units
        // Note: Price doesn't matter for Market orders, setting it to 0
        let mut market_bid = make_order("mkt_bid", OrderType::Market, Side::Bid, 0, 25);
        let rem = book.add_order(&mut market_bid);

        // Should completely fill
        assert_eq!(rem, 0);

        // ask_1 and ask_2 should be destroyed
        assert!(book.asks.get(&100).is_none());
        assert!(book.asks.get(&105).is_none());

        // ask_3 should have 5 remaining
        assert_eq!(book.asks.get(&110).unwrap()[0].remaining_size, 5);
    }

    // --- TEST 7: Market Order Liquidity Exhaustion ---
    // Tests what happens when a market order is larger than the entire book.
    #[test]
    fn test_market_order_exhausts_book() {
        let mut book = new_book();

        let mut ask = make_order("ask_1", OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        // Aggressive Market Buy for 50 units (only 10 exist in book)
        let mut market_bid = make_order("mkt_bid", OrderType::Market, Side::Bid, 0, 50);
        let rem = book.add_order(&mut market_bid);

        // 40 units could not be filled
        assert_eq!(rem, 40);
        assert_eq!(market_bid.remaining_size, 40);

        // Book should be completely empty
        assert!(book.asks.is_empty());

        // MENTOR CHECK: Market orders should NEVER be stored in the book.
        assert!(
            book.bids.is_empty(),
            "Market orders must not be placed in the BTreeMap"
        );
    }
}
