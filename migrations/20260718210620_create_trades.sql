CREATE TABLE trades (
    event_id      TEXT PRIMARY KEY,
    pair          TEXT    NOT NULL,
    price         NUMERIC NOT NULL,
    qty           NUMERIC NOT NULL,
    maker_id      NUMERIC NOT NULL,
    taker_id      NUMERIC NOT NULL,
    taker_side    TEXT    NOT NULL,
    maker_account NUMERIC NOT NULL,
    taker_account NUMERIC NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_trades_pair ON trades (pair);
