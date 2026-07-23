use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Side {
    Bid, // Buy
    Ask, // Sell
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct OrderLocation {
    pub owner: AccountId,
    pub side: Side,
    pub price: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Trade {
    pub price: u64,
    pub qty: u64,
    pub maker_id: u64,
    pub taker_id: u64,
    pub taker_side: Side,
    pub maker_account: AccountId,
    pub taker_account: AccountId,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Place(OrderRequest),
    Cancel(CancelRequest),
    Deposit(DepositRequest),
    Withdraw(WithdrawRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub correlation_id: Uuid,
    pub account_id: AccountId,
    pub client_order_id: u64,
    pub command: Command,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub correlation_id: Uuid,
    pub response: CommandResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderRequest {
    pub base_currency: Currency,
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelRequest {
    pub order_id: u64,
    pub base_currency: Currency,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DepositRequest {
    pub amount: u64,
    pub currency: Currency,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    Place(Result<PlaceOrderResponse, RejectReason>),
    Cancel(bool),
    Deposit(u64),
    Withdraw(Result<(), RejectReason>),
    // A (account_id, client_order_id) we've already applied — a lost-ack retry.
    // No state was touched; the client reconciles the real outcome via query.
    Duplicate,
}

#[derive(Debug)]
pub struct MatchResponse {
    pub order_id: u64,
    pub trades: Vec<Trade>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlaceOrderResponse {
    pub order_id: u64,
    pub filled_qty: u64,
    pub total_cost: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    USD,
    SOL,
}

pub type AccountId = u64;

#[derive(Default, Debug)]
pub struct Balance {
    pub available: u64,
    pub reserved: u64,
}

#[derive(Debug)]
pub struct Ledger {
    pub balances: HashMap<AccountId, HashMap<Currency, Balance>>,
    pub dirty: HashSet<(AccountId, Currency)>,
}

#[derive(Debug)]
pub struct Engine {
    pub book: OrderBook,
    pub ledger: Ledger,
    pub dedup: HashSet<(AccountId, u64)>,
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

#[derive(Debug, Serialize, Deserialize)]
pub enum RejectReason {
    InsufficientFunds,
    UnsupportedOrderType,
    InvalidAmount,
}

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
        side: Side,
        order_type: OrderType,
        price: u64,
        size: u64,
    },
    OrderCancelled {
        order_id: u64,
    },
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Currency::USD => write!(f, "USD"),
            Currency::SOL => write!(f, "SOL"),
        }
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    // The api → stream → engine path depends on a command surviving JSON.
    // If this breaks, no command ever reaches the engine intact.
    #[test]
    fn place_command_round_trips() {
        let env = CommandEnvelope {
            correlation_id: Uuid::nil(),
            account_id: 7,
            client_order_id: 42,
            command: Command::Place(OrderRequest {
                base_currency: Currency::SOL,
                order_type: OrderType::Limit,
                side: Side::Bid,
                price: 100,
                size: 10,
            }),
        };

        let json = serde_json::to_string(&env).unwrap();
        let back: CommandEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(back.correlation_id, Uuid::nil());
        assert_eq!(back.account_id, 7);
        assert_eq!(back.client_order_id, 42);
        match back.command {
            Command::Place(req) => {
                assert_eq!(req.side, Side::Bid);
                assert_eq!(req.price, 100);
                assert_eq!(req.size, 10);
            }
            _ => panic!("expected Place"),
        }
    }

    // The engine → pub/sub → api path depends on a rejection surviving JSON.
    #[test]
    fn reject_response_round_trips() {
        let resp = CommandResponse::Place(Err(RejectReason::InsufficientFunds));
        let json = serde_json::to_string(&resp).unwrap();
        let back: CommandResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            CommandResponse::Place(Err(RejectReason::InsufficientFunds))
        ));
    }
}
