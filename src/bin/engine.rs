use matching_engine::{
    engine::run_engine,
    types::{Engine, Ledger, OrderBook},
};
use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting Matching Engine");

    let client = redis::Client::open("redis://127.0.0.1:6379")?;

    // connection dedicated to the BLOCKING read of commands
    let read_conn = client.get_multiplexed_async_connection().await?;
    // connection for PUBLISH + XACK (both are cheap operations and hence the connection is sharable)
    let mut pub_conn = client.get_multiplexed_async_connection().await?;

    // // create a consumer group (not using XREAD)
    let created: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("commands")
        .arg("engine-group")
        .arg("$")
        .arg("MKSTREAM")
        .query_async(&mut pub_conn)
        .await;
    if let Err(e) = created {
        if !e.to_string().contains("BUSYGROUP") {
            return Err(e.into());
        }
    }

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

    run_engine(engine, read_conn, pub_conn).await?;
    Ok(())
}
