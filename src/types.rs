use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// A trading pair (market). `base` is what's bought/sold, `quote` is what it's
/// priced in. symbol string - "SOL-USD".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Pair {
    pub base: Currency,
    pub quote: Currency,
}

impl Pair {
    pub fn new(base: Currency, quote: Currency) -> Self {
        Pair { base, quote }
    }
    // A market must price one thing in another.
    pub fn is_valid(&self) -> bool {
        self.base != self.quote
    }
}

impl fmt::Display for Pair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.base, self.quote)
    }
}

impl From<Pair> for String {
    fn from(p: Pair) -> String {
        p.to_string()
    }
}

impl TryFrom<String> for Pair {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let Some((base, quote)) = s.split_once('-') else {
            return Err(format!("pair must look like BASE-QUOTE, got {s:?}"));
        };
        Ok(Pair {
            base: base.parse()?,
            quote: quote.parse()?,
        })
    }
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
    pub pair: Pair,
    pub order_type: OrderType,
    pub side: Side,
    pub price: u64,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelRequest {
    pub order_id: u64,
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
    Duplicate,
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
    /// One book per market. Created on first order for that pair.
    pub books: HashMap<Pair, OrderBook>,
    /// Routes a cancel (which carries only an order_id) to the right book.
    pub order_pair: HashMap<u64, Pair>,
    /// Global, so ids stay unique across every book.
    pub next_order_id: u64,
    /// Sequence stamped on each emitted EventBatch. Engine state, so silent
    /// replay reproduces the same numbering.
    pub next_seq: u64,
    pub ledger: Ledger,
    pub dedup: HashSet<(AccountId, u64)>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            books: HashMap::new(),
            order_pair: HashMap::new(),
            next_order_id: 0,
            next_seq: 0,
            ledger: Ledger {
                balances: HashMap::new(),
                dirty: HashSet::new(),
            },
            dedup: HashSet::new(),
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
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
    /// (side, price) levels whose aggregate qty changed due to current command.
    /// Drained into BookDelta events by `take_dirty_levels` — this patter is same as
    /// `Ledger.dirty`.
    pub dirty_levels: HashSet<(Side, u64)>,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            dirty_levels: HashSet::new(),
        }
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RejectReason {
    InsufficientFunds,
    UnsupportedOrderType,
    InvalidAmount,
    InvalidPair,
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
        pair: Pair,
        side: Side,
        order_type: OrderType,
        price: u64,
        size: u64,
    },
    OrderCancelled {
        order_id: u64,
    },
    OrderUpdated {
        order_id: u64,
        account_id: AccountId,
        pair: Pair,
        filled_qty: u64,
        remaining_qty: u64,
        status: OrderStatus,
    },
    /// A price level's aggregate quantity changed. `qty` is the level's NEW
    /// total (remove level if qty=0)
    BookDelta {
        pair: Pair,
        side: Side,
        price: u64,
        qty: u64,
    },
}

/// One `events`-stream entry per command. `seq` is engine-assigned (its state, not
/// the Redis id) so it's deterministic under replay
#[derive(Debug, Serialize, Deserialize)]
pub struct EventBatch {
    pub seq: u64,
    pub events: Vec<Event>,
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Currency::USD => write!(f, "USD"),
            Currency::SOL => write!(f, "SOL"),
        }
    }
}

impl FromStr for Currency {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "USD" => Ok(Currency::USD),
            "SOL" => Ok(Currency::SOL),
            other => Err(format!("unknown currency {other:?}")),
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
                pair: Pair::new(Currency::SOL, Currency::USD),
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
