# matching-engine

A centralized exchange backend in Rust — a price-time matching engine, an
accounts ledger, a REST API with JWT auth, public/private websocket feeds,
and a Postgres-backed trade/order history, wired together over Redis Streams.

## Status

- Price-time matching engine — limit + market orders, partial fills, FIFO
- Ledger with available/reserved balances (reserve → settle → refund → release)
- Multiple trading pairs, admin-gated whitelist — nothing trades until listed
- Three processes (`api`, `engine`, `db_writer`), decoupled over Redis Streams
- Crash recovery via command-log replay
- REST API with JWT auth; idempotent writes via `X-Client-Order-Id`
- Full order lifecycle — place, cancel, deposit, withdraw any currency
- Account-scoped reads off a Postgres projection
- In-memory order-book projection with sequence-numbered snapshots
- Public and private websocket feeds (book/trades, per-account order updates)
- Overflow-checked order sizing, bounded idempotency-key memory
- Benchmark suite for the matching engine (`cargo bench`)

Not yet built: structured logging/metrics, one engine per trading pair.

## Architecture

![Clients reach the API over HTTPS and WebSocket. Writes are XADDed to a Redis commands stream, consumed by a single matching engine, whose result is published back to the waiting API handler. The engine XADDs an events stream, which the DB Writer consumes via a consumer group into Postgres, and which the API tails with a plain XREAD to serve the in-memory book and the WebSocket feeds. Auth and account reads go straight from the API to Postgres.](./docs/architecture.svg)

- **`engine`** is single-threaded and owns all state — order books, ledger,
  idempotency set. It reads `commands` in order via a Redis consumer group,
  applies each one, publishes the result, and emits an event batch onto a
  second stream.
- **`api`** is stateless per request. A handler turns a request into a
  command, writes it to `commands`, and waits for the correlated result over
  pub/sub. It also tails the event stream to keep an in-memory order-book
  projection and to fan events out to websocket clients.
- **`db_writer`** tails the same event stream and projects trades, balances,
  and orders into Postgres, one transaction per batch.
- Every command is applied in strict log order, so replaying the log from
  the start always rebuilds the same state — the basis for crash recovery
  and for any fresh reader bootstrapping its own projection.
- Trades settle through the ledger's available/reserved split; conservation
  (a trade moves money, never creates or destroys it) is enforced in tests.

## Running it

Requires local Redis and Postgres:

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=exchange -p 5432:5432 postgres:17
docker run -d --name redis -p 6379:6379 redis
```

A `.env` in the repo root:

```
DATABASE_URL=postgres://postgres:pw@127.0.0.1:5432/exchange
JWT_SECRET=<any string>
ADMIN_ACCOUNT_ID=<account id allowed to list/delist trading pairs>
```

Run migrations, then each process in its own terminal:

```bash
sqlx migrate run
cargo run --bin engine
cargo run --bin db_writer
cargo run --bin api        # serves http://127.0.0.1:3000
```

Then:

```bash
curl -X POST localhost:3000/auth/signup -H 'content-type: application/json' \
  -d '{"email":"me@x.com","password":"pw"}'
# -> {"token": "..."}

# nothing is tradeable until listed — use the admin account (id 1 on a fresh DB)
curl -X POST localhost:3000/admin/pairs \
  -H "Authorization: Bearer <admin token>" -H 'X-Client-Order-Id: 1' \
  -H 'content-type: application/json' -d '{"pair":"SOL-USD"}'

curl -X POST localhost:3000/orders \
  -H "Authorization: Bearer <token>" -H 'X-Client-Order-Id: 2' \
  -H 'content-type: application/json' \
  -d '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":100,"size":10}'

curl localhost:3000/book/SOL-USD

websocat ws://localhost:3000/ws/market/SOL-USD   # public: book deltas + trades

websocat ws://localhost:3000/ws/orders           # private: your order updates
> {"token":"<token>"}                            # first frame must be this
```

Inspect the streams directly:

```bash
docker exec -it redis redis-cli
> XRANGE commands - +
> XRANGE events - +
```

Full endpoint and WebSocket reference: [`docs/API.md`](./docs/API.md).

## Tests

```bash
cargo test
```

57 unit tests covering order-book matching, cancellation, trade pricing, the
ledger, multi-pair isolation, fill tracking, event sequencing, the
idempotency window, the trading-pair whitelist, and market-buy orders.

```bash
./scripts/smoke.sh
```

An end-to-end test against the real running system: brings up
Redis/Postgres, starts all three binaries, and drives the HTTP and websocket
APIs directly — auth, matched and resting orders, deposits/withdrawals,
ownership-checked cancellation, the book projection, both websocket feeds,
the pair whitelist, and a market buy. Requires Node ≥ 22 in addition to
Redis/Postgres/`sqlx-cli`/`jq`.

## Performance

```bash
cargo bench
```

[`benches/engine_benchmarks.rs`](./benches/engine_benchmarks.rs) calls the
matching engine directly — no Redis, no HTTP — to measure the algorithm on
its own. Median of 100 samples, one dev laptop:

| Operation                            | Latency | Throughput     |
| ------------------------------------ | ------- | -------------- |
| Deposit                              | 316 ns  | ~3.2M/s        |
| Place a resting limit order          | 594 ns  | ~1.7M/s        |
| Place a crossing order (1 trade)     | 1.19 µs | ~840K/s        |
| Cancel                               | 641 ns  | ~1.6M/s        |
| Market buy sweeping 1,000 ask levels | 278 µs  | ~3.6M levels/s |

Book depth barely matters — placing an order costs about the same at 10
resting orders or 10,000, consistent with an O(log n) price-level index. A
market order sweeping N levels is the one case that scales with N, which is
expected: touching N levels means N fills, N trades, N book updates.

These are the engine's floor, not what a client sees end to end — every real
request still pays a Redis round trip on top.

## Project layout

```
src/
├── types.rs         # wire + domain types: Order, Trade, Pair, Command/CommandResponse,
│                     # Event/EventBatch, Engine/OrderBook, Ledger
├── engine.rs         # OrderBook (matching), Ledger, Engine (orchestration), apply(),
│                     # run_engine()/recover() (Redis loop + crash recovery), tests
├── lib.rs
└── bin/
    ├── engine.rs      # engine process: consumes `commands`, emits `events`
    ├── db_writer.rs   # projects `events` into Postgres (trades/balances/orders)
    └── api/
        ├── main.rs    # HTTP server: AppState, the correlation-flow helper, all handlers
        ├── auth.rs    # JWT sign/verify, argon2 hashing, AuthUser/ClientOrderId extractors
        ├── book.rs    # in-memory order-book projection + GET /book/:pair
        └── ws.rs      # GET /ws/market/:pair (public) + GET /ws/orders (private) handlers

migrations/                    # sqlx migrations (users, trades, balances, orders)
scripts/smoke.sh                # end-to-end smoke test against the real running system
scripts/ws_smoke.mjs            # smoke.sh's websocket-feed helper (Node's global WebSocket)
benches/engine_benchmarks.rs    # Criterion benchmarks against apply(), no transport
```
