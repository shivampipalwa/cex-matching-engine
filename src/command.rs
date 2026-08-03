use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::book::OrderRequest;
use crate::error::RejectReason;
use crate::market::{AccountId, Currency, Pair};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Place(OrderRequest),
    Cancel(CancelRequest),
    Deposit(DepositRequest),
    Withdraw(WithdrawRequest),
    ListPair(Pair),
    DelistPair(Pair),
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
    pub currency: Currency,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    Place(Result<PlaceOrderResponse, RejectReason>),
    Cancel(bool),
    // Ok = new available balance
    Deposit(Result<u64, RejectReason>),
    Withdraw(Result<(), RejectReason>),
    // true = newly listed, false = was already listed (still success).
    ListPair(Result<bool, RejectReason>),
    // true = was listed and is now removed, false = wasn't listed.
    DelistPair(bool),
    Duplicate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlaceOrderResponse {
    pub order_id: u64,
    pub filled_qty: u64,
    pub total_cost: u64,
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use crate::book::{OrderType, Side};

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
