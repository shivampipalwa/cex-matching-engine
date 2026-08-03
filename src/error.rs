use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum RejectReason {
    InsufficientFunds,
    UnsupportedOrderType,
    InvalidAmount,
    InvalidPair,
    DepositLimitExceeded,
    PriceOutOfBand,
    SelfTrade,
}
