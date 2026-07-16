use futures_util::StreamExt;
use matching_engine::types::{
    CancelRequest,
    Command::{self},
    CommandEnvelope, CommandResponse, Currency, DepositRequest, OrderRequest,
    OrderType::{Limit, Market},
    Side,
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use std::{error::Error, time::Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting API gateway");
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut xadd_conn = client.get_multiplexed_async_connection().await?; // for XADD

    let mut command = Command::Deposit(DepositRequest {
        account_id: 2,
        amount: 10,
        currency: Currency::SOL,
    });
    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    command = Command::Deposit(DepositRequest {
        account_id: 1,
        amount: 1000,
        currency: Currency::USD,
    });
    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    command = Command::Place(OrderRequest {
        account_id: 2,
        base_currency: Currency::SOL,
        order_type: Limit,
        side: Side::Ask,
        price: 100,
        size: 10,
    });
    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    command = Command::Place(OrderRequest {
        account_id: 1,
        base_currency: Currency::SOL,
        order_type: Limit,
        side: Side::Bid,
        price: 100,
        size: 10,
    });

    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    command = Command::Place(OrderRequest {
        account_id: 1,
        base_currency: Currency::SOL,
        order_type: Market,
        side: Side::Bid,
        price: 100,
        size: 10,
    });
    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    command = Command::Cancel(CancelRequest {
        order_id: 999,
        base_currency: Currency::SOL,
    });
    let resp = send_command(&client, &mut xadd_conn, command).await;
    println!("{:?}", resp);

    Ok(())
}

async fn send_command(
    client: &Client,
    xadd_conn: &mut MultiplexedConnection,
    command: Command,
) -> Result<CommandResponse, Box<dyn Error>> {
    let correlation_id = Uuid::new_v4();
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub
        .subscribe(format!("result:{}", correlation_id.to_string()))
        .await?;
    let json_envelope = serde_json::to_string(&CommandEnvelope {
        correlation_id,
        command,
    })?;
    let _: String = xadd_conn
        .xadd("commands", "*", &[("data", json_envelope)])
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(5), pubsub.on_message().next())
        .await?
        .unwrap();
    let payload: String = msg.get_payload()?;
    let cmd_resp = serde_json::from_str(&payload)?;

    Ok(cmd_resp)
}
