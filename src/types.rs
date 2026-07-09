use tokio::sync::oneshot;

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

pub enum EngineMessage {
    AddOrder {
        order: Order,
        response_tx: oneshot::Sender<u64>,
    },
    CancelOrder {
        order_id: u64,
        response_tx: oneshot::Sender<bool>,
    },
}
