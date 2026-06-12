use std::collections::{BTreeMap, VecDeque};

use inline_string::InlineString;

#[derive(Clone, Copy)]
enum Side {
    Bid, // Buy
    Ask, // Sell
}

#[derive(Clone, Copy)]
enum OrderType {
    Market, // execute immediately at the best available price
    Limit,  // execute at a specific price or better
}

#[derive(Clone, Copy)]
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
                    self.asks.entry(lowest_ask_price).or_insert_with(|| {
                        unreachable!("self.asks is guaranteed to have this price")
                    })
                }
                Side::Ask => {
                    let Some((&highest_bid_price, _)) = self.bids.last_key_value() else {
                        break;
                    };
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
