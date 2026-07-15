use std::collections::{BTreeMap, HashMap, VecDeque};

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
    pub id: u64, // assigned by the engine from a monotonic counter (0 = unassigned placeholder)
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
    pub remaining_size: u64,
    pub account_id: AccountId,
}

pub enum EngineMessage {
    AddOrder {
        order_request: OrderRequest,
        response_tx: oneshot::Sender<Result<PlaceOrderResponse, RejectReason>>,
    },
    CancelOrder {
        cancel_request: CancelRequest,
        response_tx: oneshot::Sender<bool>,
    },
    DepositUsd {
        deposit_request: DepositRequest,
        response_tx: oneshot::Sender<u64>,
    },
    WithdrawUsd {
        withdraw_request: WithdrawRequest,
        response_tx: oneshot::Sender<Result<(), RejectReason>>,
    },
}

#[derive(Debug)]
pub struct OrderLocation {
    pub side: Side,
    pub price: u64,
}

#[derive(Debug)]
pub struct Trade {
    pub price: u64,
    pub qty: u64,
    pub maker_id: u64,
    pub taker_id: u64,
    pub taker_side: Side,
    pub maker_account: AccountId,
    pub taker_account: AccountId,
}

#[derive(Debug)]
pub struct OrderRequest {
    pub account_id: AccountId,
    pub base_currency: Currency,
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
}

#[derive(Debug)]
pub struct CancelRequest {
    pub order_id: u64,
    pub base_currency: Currency,
}

#[derive(Debug)]
pub struct DepositRequest {
    pub account_id: AccountId,
    pub amount: u64,
    pub currency: Currency,
}

#[derive(Debug)]
pub struct WithdrawRequest {
    pub account_id: AccountId,
    pub amount: u64,
}

#[derive(Debug)]
pub struct MatchResponse {
    pub order_id: u64,
    pub trades: Vec<Trade>,
}

#[derive(Debug)]
pub struct PlaceOrderResponse {
    pub order_id: u64,
    pub filled_qty: u64,
    pub total_cost: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Currency {
    USD,
    SOL,
}

pub type AccountId = u64;

#[derive(Default)]
pub struct Balance {
    pub available: u64,
    pub reserved: u64,
}

pub struct Ledger {
    pub balances: HashMap<AccountId, HashMap<Currency, Balance>>,
}

pub struct Engine {
    pub book: OrderBook,
    pub ledger: Ledger,
}

// Stores Orders in a BTreeMap as:
// key = price
// value = queue of orders
// Used btreemap to keep the orders sorted by price
#[derive(Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<u64, VecDeque<Order>>,
    pub asks: BTreeMap<u64, VecDeque<Order>>,
    pub order_index: HashMap<u64, OrderLocation>,
    pub next_order_id: u64,
}

#[derive(Debug)]
pub enum RejectReason {
    InsufficientFunds,
    UnsupportedOrderType,
    InvalidAmount,
}
