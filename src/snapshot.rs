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

/// Where the API publishes how far its book projection has been snapshotted.
/// db_writer reads it to know how far `events` may be trimmed — the two are
/// independent consumers of that stream, so the cut is the earlier of the two.
/// Absent (API not running, or snapshotting off) means don't trim at all.
pub const BOOK_SNAPSHOT_KEY: &str = "book:snapshot-id";

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
    /// No `path_var` means snapshotting — and therefore trimming — is off.
    pub fn from_env(path_var: &str, every_var: &str, default_every: u64) -> Option<Self> {
        let path = std::env::var(path_var).ok().filter(|p| !p.is_empty())?;
        let every = std::env::var(every_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(default_every);
        Some(SnapshotConfig {
            path: PathBuf::from(path),
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

/// `<ms>-<seq>`, compared numerically. Lexicographic ordering happens to agree
/// today but breaks the moment the millisecond part changes digit count.
fn parse_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// The earlier of two stream ids — the furthest a stream can be trimmed when
/// two independent consumers each need their own history retained.
pub fn earlier<'a>(a: &'a str, b: &'a str) -> &'a str {
    match (parse_id(a), parse_id(b)) {
        (Some(x), Some(y)) if y < x => b,
        (Some(_), Some(_)) => a,
        _ => a,
    }
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

#[cfg(test)]
mod tests {
    use super::earlier;

    #[test]
    fn picks_the_earlier_id() {
        assert_eq!(earlier("100-0", "200-0"), "100-0");
        assert_eq!(earlier("200-0", "100-0"), "100-0");
    }

    #[test]
    fn compares_the_sequence_part() {
        assert_eq!(earlier("100-5", "100-2"), "100-2");
        assert_eq!(earlier("100-2", "100-5"), "100-2");
    }

    #[test]
    fn equal_ids_are_their_own_earlier() {
        assert_eq!(earlier("100-1", "100-1"), "100-1");
    }

    // The reason this isn't a string compare: "9999999999999-0" is
    // lexicographically after "10000000000000-0" but chronologically before it.
    #[test]
    fn survives_a_millisecond_digit_rollover() {
        let before = "9999999999999-0";
        let after = "10000000000000-0";
        assert!(before > after); // lexicographic, and wrong
        assert_eq!(earlier(before, after), before);
        assert_eq!(earlier(after, before), before);
    }

    // A malformed id must not silently trim further than intended.
    #[test]
    fn unparseable_id_falls_back_to_first() {
        assert_eq!(earlier("100-0", "garbage"), "100-0");
    }
}
