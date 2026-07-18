CREATE TABLE orders (
    order_id      NUMERIC PRIMARY KEY,
    account_id    NUMERIC NOT NULL,
    side          TEXT    NOT NULL,
    order_type    TEXT    NOT NULL,
    price         NUMERIC NOT NULL,
    size          NUMERIC NOT NULL,
    status        TEXT    NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
