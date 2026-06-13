use crate::types::{Order, OrderType, Side};
use rand::{RngExt, rng};
use std::collections::{BTreeMap, HashMap, VecDeque};

impl Order {
    fn new(order_type: OrderType, side: Side, price: u64, size: u64) -> Order {
        let rnd = rng();
        let mut id: u64 = rng().random();
        match side {
            Side::Bid => {
                id &= !(1 << 63);
            }
            Side::Ask => {
                id |= 1 << 63;
            }
        };
        Order {
            id,
            order_type,
            side,
            price,
            size,
            remaining_size: size,
        }
    }
}

// Stores Orders in a BTreeMap as:
// key = price
// value = queue of orders
// Used btreemap to keep the orders sorted by price
#[derive(Debug)]
struct OrderBook {
    bids: BTreeMap<u64, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
    order_price_map: HashMap<u64, u64>,
}

impl OrderBook {
    fn add_order(&mut self, order: &mut Order) -> u64 {
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
                    if order.remaining_size <= best_price_order.remaining_size {
                        best_price_order.remaining_size -= order.remaining_size;
                        order.remaining_size = 0;
                    } else {
                        order.remaining_size -= best_price_order.remaining_size;
                        best_price_order.remaining_size = 0;
                    }
                    // remove ask order from queue if it is filled
                    if best_price_order.remaining_size == 0 {
                        self.order_price_map.remove(&best_price_order.id);
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
        if let OrderType::Limit = order.order_type {
            if order.remaining_size > 0 {
                self.order_price_map.insert(order.id, order.price);
            }
        }
        order.remaining_size
    }

    fn cancel_order(&mut self, order_id: u64) {
        let side_bit = (order_id >> 63) & 1;

        println!("ORDER ID: {}", order_id);
        println!("SIDE BIT: {}", side_bit);

        let Some(price) = self.order_price_map.get(&order_id) else {
            return;
        };

        let Some(order_queue) = (match side_bit {
            0 => self.bids.get_mut(price),
            1 => self.asks.get_mut(price),
            _ => None,
        }) else {
            return;
        };

        if let Some(index) = order_queue.iter().position(|&order| order.id == order_id) {
            let cancelled_order = order_queue.remove(index);
            println!("Removed: {:?}", cancelled_order);
        }
        if order_queue.is_empty() {
            if side_bit == 0 {
                self.bids.remove(price);
            } else if side_bit == 1 {
                self.asks.remove(price);
            }
        }
    }
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
    fn make_order(order_type: OrderType, side: Side, price: u64, size: u64) -> Order {
        Order::new(order_type, side, price, size)
    }

    fn new_book() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_price_map: HashMap::new(),
        }
    }

    // --- TEST 1: Basic Insertion (No Match) ---
    // Tests that limit orders correctly route to the right side of the book
    // when the opposite side is empty.
    #[test]
    fn test_insert_limit_orders_no_match() {
        let mut book = new_book();

        let mut bid_order = make_order(OrderType::Limit, Side::Bid, 100, 10);
        let rem_bid = book.add_order(&mut bid_order);

        let mut ask_order = make_order(OrderType::Limit, Side::Ask, 110, 15);
        let rem_ask = book.add_order(&mut ask_order);

        // Orders should remain fully unfilled
        assert_eq!(rem_bid, 10);
        assert_eq!(rem_ask, 15);

        // State verification
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);
        assert_eq!(book.asks.get(&110).unwrap().len(), 1);
        assert_eq!(book.bids.get(&100).unwrap()[0].id & (1 << 63), 0); // Adjust string check based on your crate
    }

    // --- TEST 2: Exact Match & Price Level Cleanup ---
    // Tests that when two orders perfectly match, they are filled,
    // and critically, the empty price level is removed from the BTreeMap.
    #[test]
    fn test_exact_match_and_cleanup() {
        let mut book = new_book();

        // 1. Add a resting Ask at 100 for 10 units
        let mut ask = make_order(OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        // 2. Add an incoming Bid at 100 for 10 units
        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 10);
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

        let mut ask = make_order(OrderType::Limit, Side::Ask, 100, 20);
        book.add_order(&mut ask);

        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 5);
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

        let mut ask = make_order(OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 25);
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
        let mut ask1 = make_order(OrderType::Limit, Side::Ask, 100, 10);
        let mut ask2 = make_order(OrderType::Limit, Side::Ask, 100, 10);
        let mut ask3 = make_order(OrderType::Limit, Side::Ask, 100, 10);

        book.add_order(&mut ask1);
        book.add_order(&mut ask2);
        book.add_order(&mut ask3);

        // Incoming Bid for 15 units
        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 15);
        book.add_order(&mut bid);

        let ask_queue = book.asks.get(&100).unwrap();

        // ask_1 should be completely gone
        // ask_2 should be partially filled (5 remaining) and now at the front of the queue
        // ask_3 should be completely untouched (10 remaining)
        assert_eq!(ask_queue.len(), 2);
        assert_eq!(ask_queue[0].remaining_size, 5);
        assert_eq!(ask_queue[1].remaining_size, 10);
    }

    // --- TEST 6: Market Order Sweeps Multiple Price Levels ---
    // Tests that market orders cross the spread and consume liquidity aggressively.
    #[test]
    fn test_market_order_sweep() {
        let mut book = new_book();

        // Sellers resting at increasing prices
        let mut ask1 = make_order(OrderType::Limit, Side::Ask, 100, 10);
        let mut ask2 = make_order(OrderType::Limit, Side::Ask, 105, 10);
        let mut ask3 = make_order(OrderType::Limit, Side::Ask, 110, 10);

        book.add_order(&mut ask1);
        book.add_order(&mut ask2);
        book.add_order(&mut ask3);

        // Aggressive Market Buy for 25 units
        // Note: Price doesn't matter for Market orders, setting it to 0
        let mut market_bid = make_order(OrderType::Market, Side::Bid, 0, 25);
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

        let mut ask = make_order(OrderType::Limit, Side::Ask, 100, 10);
        book.add_order(&mut ask);

        // Aggressive Market Buy for 50 units (only 10 exist in book)
        let mut market_bid = make_order(OrderType::Market, Side::Bid, 0, 50);
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

#[cfg(test)]
mod cancel_tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    // --- HELPER FUNCTION ---
    // Update this to use your new auto-generating ID constructor.
    // For example, if you implemented `Order::new(...)`, use that.
    fn make_order(order_type: OrderType, side: Side, price: u64, size: u64) -> Order {
        // Replace with your actual initialization logic
        Order::new(order_type, side, price, size)
    }

    fn new_book() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_price_map: HashMap::new(),
        }
    }

    // --- TEST 1: Basic Cancellation & Cleanup ---
    // Tests that an order is removed and the price level is deleted
    // from the BTreeMap if it becomes empty.
    #[test]
    fn test_cancel_single_order() {
        let mut book = new_book();
        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 10);

        // Capture the auto-generated ID so we know what to cancel
        let order_id = bid.id.clone();

        book.add_order(&mut bid);

        // Ensure it's in the book
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);

        // Cancel the order
        book.cancel_order(order_id);

        // MENTOR CHECK: The price level MUST be removed from the BTreeMap
        assert!(
            book.bids.get(&100).is_none(),
            "Empty price levels must be removed after cancellation"
        );
        assert!(book.bids.is_empty());
    }

    // --- TEST 2: Cancel from the Middle of a Queue ---
    // Tests that if multiple orders exist at the same price, canceling one
    // does not affect the others and maintains FIFO order.
    #[test]
    fn test_cancel_middle_of_queue() {
        let mut book = new_book();

        let mut ask1 = make_order(OrderType::Limit, Side::Ask, 200, 10);
        let mut ask2 = make_order(OrderType::Limit, Side::Ask, 200, 15);
        let mut ask3 = make_order(OrderType::Limit, Side::Ask, 200, 20);

        let id1 = ask1.id.clone();
        let id2 = ask2.id.clone();
        let id3 = ask3.id.clone();

        book.add_order(&mut ask1);
        book.add_order(&mut ask2);
        book.add_order(&mut ask3);

        // Queue should have 3 orders
        assert_eq!(book.asks.get(&200).unwrap().len(), 3);

        // Cancel the middle order (ask2)
        book.cancel_order(id2);

        let queue = book.asks.get(&200).unwrap();

        // Queue should now have 2 orders
        assert_eq!(queue.len(), 2);

        // Verify time priority (FIFO) is maintained for remaining orders
        assert_eq!(queue[0].id, id1);
        assert_eq!(queue[1].id, id3);
    }

    // --- TEST 3: Cancel a Non-Existent Order ---
    // The engine should handle this gracefully without panicking or crashing.
    #[test]
    fn test_cancel_non_existent_order() {
        let mut book = new_book();
        let mut ask = make_order(OrderType::Limit, Side::Ask, 150, 10);

        book.add_order(&mut ask);

        // Attempt to cancel a random, non-existent ID
        book.cancel_order(123);

        // Book state should be completely untouched
        assert_eq!(book.asks.get(&150).unwrap().len(), 1);
        assert_eq!(book.asks.get(&150).unwrap()[0].size, 10);
    }

    // --- TEST 4: Cancel Order Partially Filled ---
    // Tests canceling an order that has already been partially matched.
    #[test]
    fn test_cancel_partially_filled_order() {
        let mut book = new_book();

        // Resting ask for 20 units
        let mut ask = make_order(OrderType::Limit, Side::Ask, 100, 20);
        let ask_id = ask.id;
        book.add_order(&mut ask);

        // Incoming bid for 5 units (partially fills the ask)
        let mut bid = make_order(OrderType::Limit, Side::Bid, 100, 5);
        book.add_order(&mut bid);

        // Verify it was partially filled
        assert_eq!(book.asks.get(&100).unwrap()[0].remaining_size, 15);

        // Cancel the remainder of the resting ask
        book.cancel_order(ask_id);

        // The book should now be completely empty
        assert!(book.asks.is_empty());
        assert!(book.bids.is_empty());
    }
}
