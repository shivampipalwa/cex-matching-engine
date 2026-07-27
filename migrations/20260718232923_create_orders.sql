CREATE TABLE orders (
    order_id      NUMERIC PRIMARY KEY,
    account_id    NUMERIC NOT NULL,
    pair          TEXT    NOT NULL,
    side          TEXT    NOT NULL,
    order_type    TEXT    NOT NULL,
    price         NUMERIC NOT NULL,
    size          NUMERIC NOT NULL,
    filled_qty    NUMERIC NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_pair ON orders (pair);
CREATE INDEX idx_orders_account ON orders (account_id);
