//! Candle bucket widths, shared by the API's read path and db_writer's rollup
//! writes.
//!
//! One list on purpose: if db_writer aggregated a width the API can't ask for,
//! it would write rows nothing ever reads; if the API accepted one db_writer
//! doesn't maintain, that interval would silently return an empty chart.

/// Every width, in seconds, that gets a materialised row per trade.
pub const INTERVAL_WIDTHS: [i32; 6] = [1, 900, 3600, 14400, 86400, 604800];

/// Maps the API's labels onto [`INTERVAL_WIDTHS`]. `None` is a 400 — the set is
/// closed so a caller can't invent a width and force an unindexed aggregation.
pub fn interval_seconds(label: &str) -> Option<i64> {
    Some(match label {
        "1s" => 1,
        "15m" => 900,
        "1h" => 3600,
        "4h" => 14400,
        "1d" => 86400,
        "1w" => 604800,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two lists are written separately, so this pins them together: every
    /// label the API accepts must be a width db_writer actually maintains.
    #[test]
    fn every_label_maps_to_a_maintained_width() {
        for label in ["1s", "15m", "1h", "4h", "1d", "1w"] {
            let secs = interval_seconds(label).expect("label should map") as i32;
            assert!(
                INTERVAL_WIDTHS.contains(&secs),
                "{label} maps to {secs}s, which db_writer never writes"
            );
        }
        assert_eq!(INTERVAL_WIDTHS.len(), 6, "a width exists with no label");
    }

    #[test]
    fn unknown_label_is_rejected() {
        assert_eq!(interval_seconds("1m"), None);
        assert_eq!(interval_seconds(""), None);
    }
}
