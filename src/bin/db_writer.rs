use bigdecimal::BigDecimal;
use matching_engine::{
    snapshot,
    types::{Event, EventBatch, OrderType, Side},
};
use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
};
use sqlx::{Postgres, Transaction};
use std::error::Error;

/// Committed batches between trim attempts — the check costs a GET, so it
/// isn't worth doing on every one.
const TRIM_EVERY_BATCHES: u64 = 100;

async fn ack_event(
    conn: &mut MultiplexedConnection,
    entry_id: &String,
) -> Result<(), Box<dyn Error>> {
    let _: i64 = conn.xack("events", "db-writers", &[entry_id]).await?;
    Ok(())
}

/// Writes one event's rows. Every statement runs on `tx`, so nothing is visible
/// to readers until the caller commits.
async fn apply_event(
    tx: &mut Transaction<'_, Postgres>,
    entry_id: &str,
    event: Event,
) -> Result<(), sqlx::Error> {
    match event {
        Event::Trade(t) => {
            sqlx::query(
                "INSERT INTO trades
                   (event_id, pair, price, qty, maker_id, taker_id, taker_side, maker_account, taker_account)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(entry_id) // the events-stream id (idempotency key)
            .bind(t.pair.to_string())
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
            .execute(&mut **tx)
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
            .execute(&mut **tx)
            .await?;
        }
        Event::OrderAccepted {
            order_id,
            account_id,
            pair,
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
                   (order_id, account_id, pair, side, order_type, price, size, status)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,'open')
                 ON CONFLICT (order_id) DO NOTHING",
            )
            .bind(BigDecimal::from(order_id))
            .bind(BigDecimal::from(account_id))
            .bind(pair.to_string())
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
            .execute(&mut **tx)
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
            .execute(&mut **tx)
            .await?;
        }
        Event::OrderUpdated {
            order_id,
            filled_qty,
            status,
            ..
        } => {
            // filled_qty is cumulative, so SET (not +=) — safe to
            // replay if the event is redelivered.
            sqlx::query(
                "UPDATE orders
                    SET filled_qty = $2, status = $3, updated_at = now()
                  WHERE order_id = $1",
            )
            .bind(BigDecimal::from(order_id))
            .bind(BigDecimal::from(filled_qty))
            .bind(status.to_string())
            .execute(&mut **tx)
            .await?;
        }
        Event::BookDelta { .. } => {
            // Consumed by the in-memory book projection in the API process
            // (M6.4 step 3), not persisted here — book levels are derived,
            // high-churn state with no query need in Postgres.
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    // create sqlx pg pool
    let pool = sqlx::postgres::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    // sqlx::migrate!("./migrations").run(&pool).await?;

    // connect to redis
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = redis::Client::open(redis_url)?;
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

    let mut batches_since_trim = 0u64;

    loop {
        let reply: StreamReadReply = conn.xread_options(&["events"], &[">"], &opts).await?;

        // Events whose rows are staged in this batch's transaction; acked only
        // after it commits.
        let mut staged: Vec<String> = vec![];
        // Unparseable entries: ack to skip, but never let them touch the tx.
        let mut poison: Vec<String> = vec![];
        let mut tx = pool.begin().await?;

        for key in reply.keys {
            for entry in key.ids {
                let entry_id = entry.id.clone();

                let Some(data): Option<String> = entry.get("data") else {
                    println!("Empty data");
                    poison.push(entry_id);
                    continue;
                };

                // One stream entry = one command's whole EventBatch, so this
                // loop can never see a half-applied command — a transaction
                // spanning any number of entries still only ever contains
                // WHOLE commands.
                let Ok(batch) = serde_json::from_str::<EventBatch>(&data).inspect_err(|err| {
                    println!("Could not deserialize EventBatch; Err:\n{}", err);
                }) else {
                    poison.push(entry_id);
                    continue;
                };

                for (i, event) in batch.events.into_iter().enumerate() {
                    // "{seq}:{index}" — engine-derived and stable across
                    // re-emission, unlike the old raw Redis entry id (which
                    // stops being unique now that one entry holds many events).
                    let event_id = format!("{}:{}", batch.seq, i);
                    apply_event(&mut tx, &event_id, event).await?;
                }
                staged.push(entry_id);
            }
        }

        // Commit BEFORE acking. A crash in between just redelivers the batch,
        // and the writes are idempotent. Acking first could lose them.
        if staged.is_empty() {
            drop(tx); // nothing staged -> rollback an empty tx
        } else {
            tx.commit().await?;
        }

        for entry_id in staged.iter().chain(poison.iter()) {
            ack_event(&mut conn, entry_id).await?;
        }

        // `events` has two independent readers: this one, and the API's book
        // projection. Trim to whichever is further behind, and only once both
        // have durably kept up — no key means the API isn't snapshotting, in
        // which case its bootstrap still needs the whole stream.
        if let Some(committed) = staged.last() {
            batches_since_trim += 1;
            if batches_since_trim >= TRIM_EVERY_BATCHES {
                batches_since_trim = 0;
                let book: Option<String> = conn.get(snapshot::BOOK_SNAPSHOT_KEY).await?;
                if let Some(book) = book {
                    let cut = snapshot::earlier(committed, &book);
                    snapshot::trim(&mut conn, "events", cut).await?;
                }
            }
        }
    }

    // Ok(())
}
