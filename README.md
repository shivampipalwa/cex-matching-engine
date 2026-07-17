# matching-engine

A centralized exchange (CEX) backend, built from scratch in Rust — price-time
matching engine, an accounts ledger, and a durable command log over Redis
connecting a decoupled API gateway to the engine.

This is a learning project first: the goal is to get fluent in Rust and in the
distributed-systems ideas that make a real exchange correct (determinism,
replay, at-least-once delivery, conservation of funds), while ending up with
something end-to-end and demoable.

For the full design reasoning — every architectural decision and *why* it was
made that way — see [`ARCHITECTURE.md`](./ARCHITECTURE.md). This README is the
quick tour.

## Status

Actively being built, milestone by milestone. Currently:

- ✅ Price-time matching engine (limit + market orders, partial fills, FIFO)
- ✅ Accounts ledger (available/reserved balances, reserve → settle → refund → release)
- ✅ Two-process architecture: `api` and `engine`, decoupled via Redis
  - Commands (`Place`, `Cancel`, `Deposit`, `Withdraw`) flow in through a durable
    Redis Stream
  - Results flow back through ephemeral Redis pub/sub, correlated by UUID
- 🚧 Crash recovery (replay + snapshotting)
- ⬜ Durable output event stream + database projection
- ⬜ REST API + auth
- ⬜ WebSocket feeds (public market data + private order updates)
- ⬜ Multi-pair support, latency benchmarks, observability

## Architecture at a glance

![API and matching engine connected through Redis: the API XADDs commands and SUBSCRIBEs for a response, the engine XREADs commands and PUBLISHes the result](./docs/architecture.png)

- The **api** gateway is stateless. Each request generates a correlation id,
  `SUBSCRIBE`s to `result:{id}`, then `XADD`s the command onto the durable
  Redis Stream and awaits the matching reply.
- The **engine** is a single-threaded process that owns all state (order book
  + account balances) exclusively. It reads commands off the Stream in order
  (via a consumer group, for at-least-once delivery), applies them, and
  `PUBLISH`es the result back to the correlation id's channel.
- Because the engine applies every command strictly in log order, replaying
  that same log from the start always reconstructs the same state — the basis
  for crash recovery.
- Every trade settles through the ledger's `available`/`reserved` balance split,
  with a conservation invariant enforced in tests (a trade moves money between
  accounts; it never creates or destroys it).

See `ARCHITECTURE.md` for the reasoning behind every one of these calls — why
Streams and not pub/sub for the command log, why order IDs are a deterministic
counter and not random, why the engine is a strict singleton, etc.

## Running it

Requires a local Redis instance:

```bash
docker run -p 6379:6379 redis
```

Then, in separate terminals:

```bash
cargo run --bin engine   # the matching engine + ledger
cargo run --bin api      # demo client: deposits, places/matches orders, cancels
```

You can also inspect the command log directly:

```bash
docker exec -it <redis-container> redis-cli
> XRANGE commands - +
```

## Tests

```bash
cargo test
```

Covers order book matching (fills, partial fills, FIFO, market sweeps),
cancellation, trade pricing (execution at the maker's price), and the ledger
(reserve/settle/refund/release, rejection on insufficient funds, and the
conservation invariant).

## Project layout

```
src/
├── types.rs      # domain types: Order, Trade, Ledger, Command/CommandResponse wire types
├── engine.rs      # OrderBook (matching), Ledger (balances), Engine (orchestration), tests
├── lib.rs
└── bin/
    ├── engine.rs  # engine process: consumes the Redis command Stream, publishes results
    └── api.rs     # api process: demo client issuing commands over Redis
```
