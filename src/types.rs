#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Side {
    Bid, // Buy
    Ask, // Sell
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderType {
    Market, // execute immediately at the best available price
    Limit,  // execute at a specific price or better
}

#[derive(Clone, Copy, Debug)]
pub struct Order {
    pub id: u64, // First bit: bid-> 0, ask->1
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
    pub remaining_size: u64,
}
