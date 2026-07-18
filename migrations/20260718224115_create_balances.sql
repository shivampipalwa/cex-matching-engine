CREATE TABLE balances (
    account_id NUMERIC NOT NULL,
    currency   TEXT    NOT NULL,
    available  NUMERIC NOT NULL,
    reserved   NUMERIC NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, currency)
);
-- INSERT ... ON CONFLICT (account_id, currency) DO UPDATE
--   SET available = EXCLUDED.available, reserved = EXCLUDED.reserved, updated_at = now()
