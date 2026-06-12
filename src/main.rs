use std::collections::{BTreeMap, VecDeque};

enum Side {
    Bid, // Buy
    Ask, // Sell
}

enum OrderType {
    Market, // execute immediately at the best available price
    Limit,  // execute at a specific price or better
}

struct Order {
    id: String,
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
struct OrderBook {
    bids: BTreeMap<u64, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
}

impl OrderBook {
    fn add_order(&mut self, order: &mut Order) -> u64 {
        match order.order_type {
            OrderType::Market => match order.side {
                Side::Bid => {
                    while !self.asks.is_empty() && order.remaining_size > 0 {
                        // get the queue for ask orders with lowest price
                        let (&lowest_ask_price, _) = self.asks.first_key_value().unwrap();
                        let mut lowest_ask_queue = self.asks.get_mut(&lowest_ask_price).unwrap();

                        while !lowest_ask_queue.is_empty() && order.remaining_size > 0 {
                            if let Some(lowest_ask_order) = lowest_ask_queue.front_mut() {
                                if order.remaining_size <= lowest_ask_order.remaining_size {
                                    lowest_ask_order.remaining_size -= order.remaining_size;
                                    order.remaining_size = 0;
                                } else {
                                    order.remaining_size -= lowest_ask_order.remaining_size;
                                    lowest_ask_order.remaining_size = 0;
                                }
                                // remove ask order from queue if it is filled
                                if lowest_ask_order.remaining_size == 0 {
                                    lowest_ask_queue.pop_front();
                                }
                            }
                        }
                        // remove first entry in map if its empty
                        if lowest_ask_queue.is_empty() {
                            self.asks.pop_first();
                        }
                    }
                    // handle when remaining size is more than 0
                    order.remaining_size
                }
                Side::Ask => {}
            },
            OrderType::Limit => {}
        }
    }
}

fn main() {
    // let order_book = OrderBook::
}
