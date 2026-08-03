use std::error::Error;

use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamRangeReply, StreamReadOptions, StreamReadReply},
};

use crate::command::{CommandEnvelope, ResponseEnvelope};
use crate::engine::{Engine, apply};
use crate::snapshot::{self, SnapshotConfig};

pub async fn recover(
    engine: &mut Engine,
    conn: &mut MultiplexedConnection,
    snapshot: Option<&SnapshotConfig>,
) -> Result<(), Box<dyn Error>> {
    println!("Starting Recovery");

    // `(id` is an exclusive start, so the anchor entry isn't applied twice.
    let from = match snapshot.and_then(|s| snapshot::load::<Engine>(&s.path)) {
        Some(snap) => {
            println!("Restored snapshot at {}", snap.last_id);
            let last_id = snap.last_id;
            // limits are this process's config, not the snapshot's
            let limits = engine.limits;
            *engine = snap.state;
            engine.limits = limits;
            format!("({last_id}")
        }
        None => "-".to_string(),
    };

    let reply: StreamRangeReply = conn.xrange("commands", from, "+").await?;
    let mut last_id = None;
    for id in reply.ids {
        // get command data
        let Some(data): Option<String> = id.get("data") else {
            println!("Empty data");
            continue;
        };

        // deserialize data into CommandEnvelope (correlation_id is irrelevant
        // during replay — we emit nothing, so there's no one to reply to)
        let Ok(CommandEnvelope {
            command,
            account_id,
            client_order_id,
            ..
        }) = serde_json::from_str(&data).inspect_err(|err| {
            println!("Could not deserialize CommandEnvelope; Err:\n{}", err);
        })
        else {
            continue;
        };

        // dispatch command and discard response
        let _ = apply(engine, account_id, client_order_id, command);
        last_id = Some(id);
    }
    println!("Recovery complete");

    // align the group cursor to the replay boundary
    if let Some(stream_id) = last_id {
        let _: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("SETID")
            .arg("commands")
            .arg("engine-group")
            .arg(stream_id.id)
            .query_async(conn)
            .await;
    }

    Ok(())
}

pub async fn run_engine(
    mut engine: Engine,
    mut read_conn: MultiplexedConnection,
    mut pub_conn: MultiplexedConnection,
    snapshot: Option<SnapshotConfig>,
) -> Result<(), Box<dyn Error>> {
    recover(&mut engine, &mut pub_conn, snapshot.as_ref()).await?;
    let opts = StreamReadOptions::default()
        .group("engine-group", "engine-1")
        .block(5000)
        .count(10);
    let mut since_snapshot = 0u64;
    loop {
        let reply: StreamReadReply = read_conn
            .xread_options(&["commands"], &[">"], &opts)
            .await?;

        for key in reply.keys {
            for entry in key.ids {
                let entry_id = entry.id.clone();

                // get command data
                let Some(data): Option<String> = entry.get("data") else {
                    println!("Empty data");
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };
                // deserialize to CommandEnvelope
                let Ok(CommandEnvelope {
                    correlation_id,
                    account_id,
                    client_order_id,
                    command,
                }) = serde_json::from_str(&data).inspect_err(|err| {
                    println!("Could not deserialize CommandEnvelope; Err:\n{}", err);
                })
                else {
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };

                // dispatch command (dedup happens inside `apply`)
                let (response, batch) = apply(&mut engine, account_id, client_order_id, command);

                // emit the whole command's events as ONE stream entry. This is
                // what makes a command's effects atomic for downstream
                // consumers (db_writer's transaction, the book projection): one
                // entry = one seq = one unit they can apply-or-not as a whole.
                if let Some(batch) = batch {
                    let Ok(batch_json) = serde_json::to_string(&batch).inspect_err(|err| {
                        println!("Invalid event batch: {:?}\nErr: {}", batch, err);
                    }) else {
                        // Unreachable in practice, but don't let a serialize
                        // bug wedge the command loop.
                        let _: i64 = pub_conn
                            .xack("commands", "engine-group", &[entry_id])
                            .await?;
                        continue;
                    };
                    let _: String = pub_conn
                        .xadd("events", "*", &[("data".to_string(), batch_json)])
                        .await?;
                }

                // serialize resp
                let resp_envelope = ResponseEnvelope {
                    correlation_id,
                    response,
                };

                let Ok(resp_json) = serde_json::to_string(&resp_envelope).inspect_err(|err| {
                    println!("Could not serialize Response; Err:\n{}", err);
                }) else {
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };

                // publish response
                let channel = format!("results");
                let _: Result<i64, redis::RedisError> = pub_conn
                    .publish(channel, &resp_json)
                    .await
                    .inspect_err(|err| {
                        println!(
                            "Could not publish response;\nresp_json: {}\nErr: {}",
                            resp_json, err
                        );
                    });

                // ACK command
                let _: i64 = pub_conn
                    .xack("commands", "engine-group", &[entry_id.as_str()])
                    .await?;

                since_snapshot += 1;
                if let Some(cfg) = &snapshot {
                    if since_snapshot >= cfg.every {
                        since_snapshot = 0;
                        // Trim only once the snapshot is on disk. The other
                        // order throws away the history recovery depends on.
                        match snapshot::save(&cfg.path, &entry_id, &engine) {
                            Ok(()) => snapshot::trim(&mut pub_conn, "commands", &entry_id).await?,
                            Err(e) => println!("Snapshot write failed, not trimming: {e}"),
                        }
                    }
                }
            }
        }
    }
    // Ok(())
}
