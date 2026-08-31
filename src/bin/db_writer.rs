use bigdecimal::BigDecimal;
use matching_engine::{
    book::{OrderType, Side},
    candle::INTERVAL_WIDTHS,
    event::{Event, EventBatch},
    snapshot,
};
use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
};
use sqlx::{Postgres, Transaction};
use std::{
    error::Error,
    time::{Duration, Instant},
};

/// Committed batches between trim attempts — the check costs a GET, so it
/// isn't worth doing on every one.
const TRIM_EVERY_BATCHES: u64 = 100;

/// Rows per DELETE. Bounded so a sweep never takes a long lock on a table the
/// API is reading.
const RETENTION_BATCH: i64 = 5_000;
/// Batches per sweep, so one pass can't stall event ingestion for minutes.
/// Anything left over is picked up an hour later.
const RETENTION_MAX_BATCHES: u32 = 20;
const RETENTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Terminal orders are the bulk of the growth — the market maker cancels and
/// reposts its whole quote ladder every tick — and nobody is reading a
/// week-old cancelled order.
const DEFAULT_ORDER_RETENTION_DAYS: i32 = 7;
/// Trades accumulate ~10x slower than orders. They're no longer what the chart
/// is drawn from — `candles` is — so this now only bounds how far back a
/// backfill or an ad-hoc query could reach, not the chart's history.
const DEFAULT_TRADE_RETENTION_DAYS: i32 = 365;
/// Only the 1s candles need pruning: 86,400 rows/day/pair against 96 for 15m
/// and 24 for 1h, so every coarser width is small enough to keep indefinitely
/// (a decade of 1h candles is ~88k rows). Nobody charts second-by-second
/// activity from last month, and keeping it would cost more than the entire
/// rest of the table.
const DEFAULT_CANDLE_1S_RETENTION_DAYS: i32 = 2;

struct Retention {
    order_days: i32,
    trade_days: i32,
    candle_1s_days: i32,
}

impl Retention {
    /// 0 disables a sweep, so a deployment can opt out of either one.
    fn from_env() -> Self {
        let days = |key: &str, default: i32| {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&d| d >= 0)
                .unwrap_or(default)
        };
        Retention {
            order_days: days("ORDER_RETENTION_DAYS", DEFAULT_ORDER_RETENTION_DAYS),
            trade_days: days("TRADE_RETENTION_DAYS", DEFAULT_TRADE_RETENTION_DAYS),
            candle_1s_days: days(
                "CANDLE_1S_RETENTION_DAYS",
                DEFAULT_CANDLE_1S_RETENTION_DAYS,
            ),
        }
    }

    async fn sweep(&self, pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
        // ctid keeps the DELETE to the rows the LIMIT actually picked, instead
        // of re-evaluating the predicate over the whole table.
        let orders = prune(
            pool,
            "DELETE FROM orders WHERE ctid IN (
               SELECT ctid FROM orders
                WHERE status IN ('filled', 'cancelled')
                  AND updated_at < now() - make_interval(days => $1)
                LIMIT $2)",
            self.order_days,
        )
        .await?;

        let trades = prune(
            pool,
            "DELETE FROM trades WHERE ctid IN (
               SELECT ctid FROM trades
                WHERE created_at < now() - make_interval(days => $1)
                LIMIT $2)",
            self.trade_days,
        )
        .await?;

        // Bucket is epoch seconds, so the cutoff is arithmetic rather than a
        // timestamp comparison — and idx_candles_interval_bucket leads on
        // interval_seconds, which the PK can't do since `pair` leads it.
        let candles = prune(
            pool,
            "DELETE FROM candles WHERE ctid IN (
               SELECT ctid FROM candles
                WHERE interval_seconds = 1
                  AND bucket < extract(epoch FROM now() - make_interval(days => $1))
                LIMIT $2)",
            self.candle_1s_days,
        )
        .await?;

        if orders > 0 || trades > 0 || candles > 0 {
            println!("Retention: pruned {orders} orders, {trades} trades, {candles} 1s candles");
        }
        Ok(())
    }
}

async fn prune(pool: &sqlx::PgPool, sql: &str, days: i32) -> Result<u64, Box<dyn Error>> {
    if days == 0 {
        return Ok(0);
    }
    let mut total = 0;
    for _ in 0..RETENTION_MAX_BATCHES {
        let deleted = sqlx::query(sql)
            .bind(days)
            .bind(RETENTION_BATCH)
            .execute(pool)
            .await?
            .rows_affected();
        total += deleted;
        if (deleted as i64) < RETENTION_BATCH {
            break;
        }
    }
    Ok(total)
}

async fn ack_event(
    conn: &mut MultiplexedConnection,
    entry_id: &String,
) -> Result<(), Box<dyn Error>> {
    let _: i64 = conn.xack("events", "db-writers", &[entry_id]).await?;
    Ok(())
}

/// When the engine published the batch, in unix millis.
///
/// Redis stream ids are `<millis>-<seq>`, stamped at XADD — so this is match
/// time, and it's part of the entry rather than regenerated, which means it
/// survives replay unchanged. `now()` would instead be *db_writer's* clock at
/// the moment it happened to process the entry: identical when keeping up,
/// badly wrong when catching up on a backlog, where an hour of trades would
/// all collapse into whichever bucket the replay landed in.
fn entry_millis(entry_id: &str) -> Option<i64> {
    entry_id.split_once('-')?.0.parse().ok()
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
            let millis = entry_millis(entry_id).unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0)
            });

            // RETURNING turns the existing dedup guard into a signal: `Some`
            // only when this row is new. The candle update below is a running
            // `volume + qty`, which unlike the SETs everywhere else in this
            // file is NOT idempotent — replaying an acked-but-uncommitted
            // entry would double-count it. Gating on the insert means a
            // redelivery skips both, and since they share `tx` the two can
            // never disagree.
            let inserted = sqlx::query_scalar::<_, String>(
                "INSERT INTO trades
                   (event_id, pair, price, qty, maker_id, taker_id, taker_side, maker_account, taker_account, created_at)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, to_timestamp($10::double precision / 1000.0))
                 ON CONFLICT (event_id) DO NOTHING
                 RETURNING event_id",
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
            .bind(millis)
            .fetch_optional(&mut **tx)
            .await?;

            if inserted.is_some() {
                // One statement for all six widths: `unnest` turns the width
                // list into rows, and integer division floors each to its
                // bucket start.
                //
                // `open` is absent from the DO UPDATE on purpose — it keeps
                // whatever the bucket's first trade inserted, while `close`
                // takes every later one. That's the only part of this that
                // depends on arrival order, and it holds because a
                // single-threaded engine XADDs in match order and one
                // db_writer consumes that stream in order. A second writer on
                // the same group would split entries between consumers and
                // could land them out of order — `high`/`low`/`volume` would
                // survive that, `open`/`close` would not.
                sqlx::query(
                    "INSERT INTO candles
                       (pair, interval_seconds, bucket, open, high, low, close, volume)
                     SELECT $1, w, ($2::bigint / w) * w, $3, $3, $3, $3, $4
                       FROM unnest($5::int[]) AS w
                     ON CONFLICT (pair, interval_seconds, bucket) DO UPDATE SET
                       high   = GREATEST(candles.high, EXCLUDED.high),
                       low    = LEAST(candles.low, EXCLUDED.low),
                       close  = EXCLUDED.close,
                       volume = candles.volume + EXCLUDED.volume",
                )
                .bind(t.pair.to_string())
                .bind(millis / 1000)
                .bind(t.price as i64)
                .bind(t.qty as i64)
                .bind(&INTERVAL_WIDTHS[..])
                .execute(&mut **tx)
                .await?;
            }
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

    let retention = Retention::from_env();
    println!(
        "Retention: orders {} days, trades {} days (0 = keep)",
        retention.order_days, retention.trade_days
    );
    // Sweep once at startup so a long-stopped deployment catches up rather
    // than waiting an hour, then hourly.
    let mut next_sweep = Instant::now();

    loop {
        if Instant::now() >= next_sweep {
            next_sweep = Instant::now() + RETENTION_INTERVAL;
            if let Err(e) = retention.sweep(&pool).await {
                // Never let a retention failure take down ingestion.
                println!("Retention sweep failed: {e}");
            }
        }

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
