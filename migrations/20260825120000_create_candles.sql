-- Pre-aggregated OHLCV, one row per (pair, interval, bucket).
--
-- `GET /candles/:pair` used to aggregate the whole `trades` table on every
-- request: the LIMIT applies after the GROUP BY, so returning 500 candles
-- still scanned all 1.7M trades and sorted them on disk — ~17s and growing
-- linearly with history. Reading pre-computed rows turns that into a B-tree
-- seek plus `limit` rows, which is O(limit) rather than O(all trades) and so
-- stays flat as history grows.

CREATE TABLE candles (
    pair             TEXT   NOT NULL,
    -- The bucket width, NOT a label like '1h'. Storing the number the bucket
    -- was actually computed from (floor(epoch / width) * width) keeps the row
    -- self-describing — a '1h' label would only mean something via a lookup
    -- table that lives in application code.
    interval_seconds INT    NOT NULL,
    -- Bucket start, epoch seconds. Matches what the API already returns as
    -- `time` and the bucketing the frontend mirrors, so the read path is a
    -- straight passthrough with no timezone or conversion questions.
    bucket           BIGINT NOT NULL,
    open             BIGINT NOT NULL,
    high             BIGINT NOT NULL,
    low              BIGINT NOT NULL,
    close            BIGINT NOT NULL,
    volume           BIGINT NOT NULL,
    -- Leading (pair, interval_seconds) equality then a backwards range scan on
    -- bucket is exactly the read path's shape, so this PK doubles as its index
    -- and no separate one is needed:
    --   WHERE pair = ? AND interval_seconds = ? ORDER BY bucket DESC LIMIT ?
    PRIMARY KEY (pair, interval_seconds, bucket)
);

-- Retention only ever targets the 1s buckets (86,400 rows/day/pair; every
-- coarser interval is small enough to keep forever). The PK can't serve
-- `WHERE interval_seconds = 1 AND bucket < ?` because `pair` leads it, so
-- pruning would scan the table without this.
CREATE INDEX idx_candles_interval_bucket ON candles (interval_seconds, bucket);
