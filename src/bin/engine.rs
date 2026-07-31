use matching_engine::{engine::run_engine, snapshot::SnapshotConfig, types::Engine};
use std::error::Error;

/// Commands between snapshots. Each one lets the `commands` stream be trimmed,
/// so this also caps how much history a cold start has to replay.
const SNAPSHOT_EVERY: u64 = 5_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    println!("Starting Matching Engine");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = redis::Client::open(redis_url)?;

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

    let engine = Engine::new();

    // Unset ENGINE_SNAPSHOT_PATH and the engine replays the whole command log
    // on boot and never trims it — the pre-M8 behaviour.
    let snapshot = SnapshotConfig::from_env("ENGINE_SNAPSHOT_PATH", SNAPSHOT_EVERY);
    match &snapshot {
        Some(cfg) => println!("Snapshotting to {} every {SNAPSHOT_EVERY}", cfg.path.display()),
        None => println!("Snapshotting disabled"),
    }

    run_engine(engine, read_conn, pub_conn, snapshot).await?;
    Ok(())
}
