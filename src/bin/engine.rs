use std::collections::{BTreeMap, HashMap};

use matching_engine::{
    engine::run_engine,
    types::{
        CancelRequest, Currency, DepositRequest, Engine, EngineMessage, Ledger, OrderBook,
        OrderRequest, OrderType, Side,
    },
};
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() {
    println!("Starting Matching Engine");

    let engine = Engine {
        book: OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            next_order_id: 0,
        },
        ledger: Ledger {
            balances: HashMap::new(),
        },
    };

    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(run_engine(engine, rx));

    // Fund the seller (acct 2) with SOL and the buyer (acct 1) with USD.
    for (account_id, currency, amount) in [(2, Currency::SOL, 10), (1, Currency::USD, 1000)] {
        let (rtx, rrx) = oneshot::channel();
        tx.send(EngineMessage::DepositUsd {
            deposit_request: DepositRequest {
                account_id,
                amount,
                currency,
            },
            response_tx: rtx,
        })
        .await
        .unwrap();
        println!(
            "deposit acct {account_id}: available = {}",
            rrx.await.unwrap()
        );
    }

    // Seller rests an ask: 10 SOL @ 100.
    let (rtx, rrx) = oneshot::channel();
    tx.send(EngineMessage::AddOrder {
        order_request: OrderRequest {
            account_id: 2,
            base_currency: Currency::SOL,
            order_type: OrderType::Limit,
            side: Side::Ask,
            price: 100,
            size: 10,
        },
        response_tx: rtx,
    })
    .await
    .unwrap();
    println!("ask: {:?}", rrx.await.unwrap());

    // Buyer takes it: limit buy 10 @ 100.
    let (rtx, rrx) = oneshot::channel();
    tx.send(EngineMessage::AddOrder {
        order_request: OrderRequest {
            account_id: 1,
            base_currency: Currency::SOL,
            order_type: OrderType::Limit,
            side: Side::Bid,
            price: 100,
            size: 10,
        },
        response_tx: rtx,
    })
    .await
    .unwrap();
    println!("bid: {:?}", rrx.await.unwrap());

    // A market buy is rejected in M2 (no way to reserve quote up front).
    let (rtx, rrx) = oneshot::channel();
    tx.send(EngineMessage::AddOrder {
        order_request: OrderRequest {
            account_id: 1,
            base_currency: Currency::SOL,
            order_type: OrderType::Market,
            side: Side::Bid,
            price: 0,
            size: 5,
        },
        response_tx: rtx,
    })
    .await
    .unwrap();
    println!("market bid: {:?}", rrx.await.unwrap());

    // Cancel path: order 999 never existed, so this comes back false.
    let (rtx, rrx) = oneshot::channel();
    tx.send(EngineMessage::CancelOrder {
        cancel_request: CancelRequest {
            order_id: 999,
            base_currency: Currency::SOL,
        },
        response_tx: rtx,
    })
    .await
    .unwrap();
    println!("cancel(999): {:?}", rrx.await.unwrap());
}
