use bigdecimal::BigDecimal;
use matching_engine::types::{Event, OrderType, Side};
use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
};
use std::error::Error;

async fn ack_event(
    conn: &mut MultiplexedConnection,
    entry_id: &String,
) -> Result<(), Box<dyn Error>> {
    let _: i64 = conn.xack("events", "db-writers", &[entry_id]).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    // create sqlx pg pool
    let pool = sqlx::postgres::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    // sqlx::migrate!("./migrations").run(&pool).await?;

    // connect to redis
    let client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // create consumer group "db-writers" on "events" stream starting at 0
    let created: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("events")
        .arg("db-writers")
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;
    if let Err(e) = created {
        if !e.to_string().contains("BUSYGROUP") {
            return Err(e.into());
        }
    }

    let opts = StreamReadOptions::default()
        .group("db-writers", "db-1")
        .block(5000)
        .count(10);

    loop {
        let reply: StreamReadReply = conn.xread_options(&["events"], &[">"], &opts).await?;
        for key in reply.keys {
            for entry in key.ids {
                let entry_id = entry.id.clone();
                // get event data
                let Some(data): Option<String> = entry.get("data") else {
                    println!("Empty data");
                    ack_event(&mut conn, &entry_id).await?;
                    continue;
                };

                // parse event
                let Ok(event) = serde_json::from_str::<Event>(&data).inspect_err(|err| {
                    println!("Could not deserialize Event; Err:\n{}", err);
                }) else {
                    ack_event(&mut conn, &entry_id).await?;
                    continue;
                };

                // insert entry based on event
                match event {
                    Event::Trade(t) => {
                        sqlx::query(
                            "INSERT INTO trades
                               (event_id, price, qty, maker_id, taker_id, taker_side, maker_account, taker_account)
                             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                             ON CONFLICT (event_id) DO NOTHING",
                        )
                        .bind(&entry_id) // the events-stream id (idempotency key)
                        .bind(BigDecimal::from(t.price)) // u64 → NUMERIC
                        .bind(BigDecimal::from(t.qty))
                        .bind(BigDecimal::from(t.maker_id))
                        .bind(BigDecimal::from(t.taker_id))
                        .bind(match t.taker_side {
                            Side::Bid => "Bid",
                            Side::Ask => "Ask",
                        })
                        .bind(BigDecimal::from(t.maker_account))
                        .bind(BigDecimal::from(t.taker_account))
                        .execute(&pool)
                        .await?;
                    }
                    Event::BalanceChanged {
                        account_id,
                        currency,
                        available,
                        reserved,
                    } => {
                        sqlx::query(
                            "INSERT INTO balances
                              (account_id, currency, available, reserved)
                            VALUES ($1,$2,$3,$4)
                            ON CONFLICT (account_id, currency) DO UPDATE
                            SET available = EXCLUDED.available, reserved = EXCLUDED.reserved, updated_at = now()",
                        )
                        .bind(BigDecimal::from(account_id))
                        .bind(currency.to_string())
                        .bind(BigDecimal::from(available))
                        .bind(BigDecimal::from(reserved))
                        .execute(&pool)
                        .await?;
                    }
                    Event::OrderAccepted {
                        order_id,
                        account_id,
                        side,
                        order_type,
                        price,
                        size,
                    } => {
                        // Order details are immutable for a given order_id, so a
                        // redelivered accept is a no-op (DO NOTHING). Status
                        // transitions are driven by *other* events (e.g. cancel).
                        sqlx::query(
                            "INSERT INTO orders
                               (order_id, account_id, side, order_type, price, size, status)
                             VALUES ($1,$2,$3,$4,$5,$6,'open')
                             ON CONFLICT (order_id) DO NOTHING",
                        )
                        .bind(BigDecimal::from(order_id))
                        .bind(BigDecimal::from(account_id))
                        .bind(match side {
                            Side::Bid => "Bid",
                            Side::Ask => "Ask",
                        })
                        .bind(match order_type {
                            OrderType::Limit => "Limit",
                            OrderType::Market => "Market",
                        })
                        .bind(BigDecimal::from(price))
                        .bind(BigDecimal::from(size))
                        .execute(&pool)
                        .await?;
                    }
                    Event::OrderCancelled { order_id } => {
                        // Pure status flip — the row was created by OrderAccepted,
                        // which ran earlier in the (in-order) event stream.
                        sqlx::query(
                            "UPDATE orders SET status = 'cancelled', updated_at = now()
                             WHERE order_id = $1",
                        )
                        .bind(BigDecimal::from(order_id))
                        .execute(&pool)
                        .await?;
                    }
                }

                // ACK event for the group db-1
                ack_event(&mut conn, &entry_id).await?;
            }
        }
    }

    // Ok(())
}
