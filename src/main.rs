// use std::collections::{BTreeMap, HashMap};

// use matching_engine::{
//     engine::{OrderBook, run_engine},
//     types::{EngineMessage, Order, OrderType, Side},
// };
// use tokio::sync::{mpsc, oneshot};

// #[tokio::main]
// async fn main() {
//     let (tx, rx) = mpsc::channel(1024);
//     let book = OrderBook {
//         bids: BTreeMap::new(),
//         asks: BTreeMap::new(),
//         order_price_map: HashMap::new(),
//     };
//     let handle = tokio::spawn(run_engine(book, rx));

//     let order1 = Order::new(OrderType::Limit, Side::Ask, 100, 10);
//     let (tx1, rx1) = oneshot::channel();
//     let msg = EngineMessage::AddOrder {
//         order: order1,
//         response_tx: tx1,
//     };
//     if let Err(_) = tx.send(msg).await {
//         println!("main fn dropped the oneshot receiver");
//     }

//     match rx1.await {
//         Ok(v) => println!("Filled quantity = {:?}", v),
//         Err(_) => println!("the sender dropped"),
//     }

//     let order2 = Order::new(OrderType::Market, Side::Bid, 0, 10);
//     let (tx2, rx2) = oneshot::channel();
//     let msg = EngineMessage::AddOrder {
//         order: order2,
//         response_tx: tx2,
//     };
//     if let Err(_) = tx.send(msg).await {
//         println!("main fn dropped the oneshot receiver");
//     }

//     match rx2.await {
//         Ok(v) => println!("Filled quantity = {:?}", v),
//         Err(_) => println!("the sender dropped"),
//     }
// }
