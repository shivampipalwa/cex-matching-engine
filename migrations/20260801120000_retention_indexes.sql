-- Indexes for the db_writer's retention sweep. Without these, pruning
-- sequential-scans the two largest tables every hour.

-- Partial: only terminal orders are ever pruned, and they're the bulk of the
-- table, so the index stays out of the way of the live ones.
CREATE INDEX idx_orders_terminal_updated
    ON orders (updated_at)
    WHERE status IN ('filled', 'cancelled');

CREATE INDEX idx_trades_created ON trades (created_at);
