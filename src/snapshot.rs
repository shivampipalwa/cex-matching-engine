//! Periodic state snapshots, so a process doesn't have to replay its stream
//! from entry zero on every boot — and so the stream can be trimmed at all.
//!
//! A snapshot is state plus the id of the last stream entry folded into it.
//! Recovery loads it and resumes from just after that id. Trimming is only
//! ever safe up to a snapshot that is already on disk.

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde::{Serialize, de::DeserializeOwned};

const VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Snapshot<T> {
    version: u32,
    /// Last stream entry included in `state`.
    pub last_id: String,
    pub state: T,
}

#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub path: PathBuf,
    /// Stream entries between snapshots.
    pub every: u64,
}

impl SnapshotConfig {
    /// `<var>` if set, otherwise snapshotting (and therefore trimming) is off.
    pub fn from_env(var: &str, every: u64) -> Option<Self> {
        std::env::var(var).ok().filter(|p| !p.is_empty()).map(|p| SnapshotConfig {
            path: PathBuf::from(p),
            every,
        })
    }
}

/// Temp file + rename, so a crash mid-write leaves the previous snapshot
/// intact rather than a truncated one. Fsynced before the rename because the
/// caller trims the stream on the strength of this having landed.
pub fn save<T: Serialize>(path: &Path, last_id: &str, state: &T) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let snapshot = Snapshot {
        version: VERSION,
        last_id: last_id.to_string(),
        state,
    };
    let tmp = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&serde_json::to_vec(&snapshot)?)?;
    file.sync_all()?;
    fs::rename(&tmp, path)
}

/// `None` means "replay from the start" — a missing, unreadable, or
/// wrong-version snapshot is recoverable, so it should never stop a boot.
pub fn load<T: DeserializeOwned>(path: &Path) -> Option<Snapshot<T>> {
    let bytes = fs::read(path).ok()?;
    let snapshot: Snapshot<T> = serde_json::from_slice(&bytes)
        .inspect_err(|e| println!("Ignoring unreadable snapshot at {}: {e}", path.display()))
        .ok()?;
    if snapshot.version != VERSION {
        println!(
            "Ignoring snapshot at {}: version {} != {VERSION}",
            path.display(),
            snapshot.version
        );
        return None;
    }
    Some(snapshot)
}

/// Drop entries older than `min_id`. Approximate (`~`) so Redis can stop at a
/// node boundary — it may keep more than asked, never less, which is the safe
/// direction when the retained history is what recovery depends on.
pub async fn trim(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    min_id: &str,
) -> redis::RedisResult<()> {
    let _: () = redis::cmd("XTRIM")
        .arg(stream)
        .arg("MINID")
        .arg("~")
        .arg(min_id)
        .query_async(conn)
        .await?;
    Ok(())
}
