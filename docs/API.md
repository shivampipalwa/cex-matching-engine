# API Reference

Everything a frontend needs to talk to this backend. All shapes below were
captured from the running system, not inferred from source.

Base URL: `http://127.0.0.1:3000` (WebSocket: `ws://127.0.0.1:3000`)

---

## Read this first — five things that will bite you

1. **Numbers come back as strings on some endpoints.** `GET /balances` and
   `GET /orders` return every numeric field as a JSON *string* (`"available":"6"`),
   because they're `NUMERIC` columns in Postgres. `POST /orders` returns real JSON
   *numbers* (`"filled_qty":4`). Don't assume; parse defensively.
2. **Order status has two spellings.** REST returns `partially_filled`; the
   private WebSocket returns `PartiallyFilled`. Normalize on the client.
3. **The two WebSocket feeds use different envelope styles.** Public messages
   have a `"type"` discriminator field. Private messages are wrapped in a
   single-key object (`{"OrderAccepted": {...}}`). See [WebSockets](#websockets).
4. **Most error responses have an empty body.** Status code is the whole signal.
   The exceptions are engine rejections, which return a bare JSON string.
5. **All money is integers.** No decimals anywhere — `u64` units. There is no
   defined precision/scale, so `100` means 100 whole units.

---

## Conventions

| | |
|---|---|
| Content type | `application/json` on all request bodies |
| Currencies | `USD`, `SOL` only. Anything else → `422` |
| Pair format | `BASE-QUOTE`, e.g. `SOL-USD`. `base` must ≠ `quote` |
| Amounts | Unsigned integers. No floats, no decimal strings on input |
| Side | `Bid` (buy) or `Ask` (sell) — exact casing |
| Order type | `Limit` or `Market` — exact casing |

### Authentication

Send `Authorization: Bearer <jwt>` on every endpoint marked 🔒. Tokens come from
signup/login, are HS256-signed, and **expire after 24 hours**. There is no refresh
endpoint — re-login when a call returns `401`.

### Idempotency: `X-Client-Order-Id`

**Every write endpoint requires an `X-Client-Order-Id: <u64>` header.** It's not
optional and not just for orders — deposits, withdrawals, cancels, and admin calls
all need it.

- The value must be **unique per account, forever** (well, per the most recent
  1000 writes — older ids are eventually evicted and become reusable).
- Reusing an id returns `409 Conflict` and the command is *not* re-executed.
- On a network timeout, **retry with the same id** — that's the entire point. You
  either get the `409` (it already landed) or it executes once.
- The server never tells you the original response on a `409`. Reconcile via
  `GET /orders`.

A monotonic counter in client storage is the simplest correct implementation.

### Error model

| Status | Body | Meaning |
|---|---|---|
| `400` | `"InsufficientFunds"` (bare JSON string) | Engine rejected the command — see [reject reasons](#reject-reasons) |
| `400` | *empty* | Missing/invalid `X-Client-Order-Id`, or malformed pair in the URL |
| `401` | *empty* | Missing, malformed, expired, or invalid token |
| `403` | *empty* | Authenticated, but not the admin account |
| `404` | *empty* | Order doesn't exist **or** isn't yours (deliberately indistinguishable) |
| `409` | *empty* | Duplicate `X-Client-Order-Id`, or email already registered |
| `422` | plain text | JSON body didn't deserialize (unknown currency, bad pair syntax, missing field) |
| `500` | *empty* | Internal error |
| `504` | *empty* | Engine didn't respond in 5s. **The command may still execute** — retry with the same `X-Client-Order-Id` |

Note `400` is overloaded: a body means the engine ran and said no; no body means
the request never got that far.

#### Reject reasons

Returned as a bare JSON string in the `400` body:

| Value | Cause |
|---|---|
| `"InsufficientFunds"` | Not enough `available` balance to reserve |
| `"InvalidPair"` | `base == quote`, or the pair isn't listed for trading |
| `"InvalidAmount"` | `price × size` overflows `u64` |
| `"UnsupportedOrderType"` | Currently unreachable |

---

## Endpoints

### `POST /auth/signup`

```json
{ "email": "me@x.com", "password": "pw" }
```

**`201`** → `{"token":"eyJ0eXAi..."}` · **`409`** email taken · **`422`** bad body

The account id is the JWT's `sub` claim. Note that a failed signup still consumes
an id, so ids aren't gapless.

### `POST /auth/login`

Same body. **`200`** → `{"token":"..."}` · **`401`** wrong email or password.

---

### `POST /orders` 🔒 + `X-Client-Order-Id`

```json
{ "pair": "SOL-USD", "order_type": "Limit", "side": "Bid", "price": 100, "size": 10 }
```

`price` is **required even for market orders** — send `0`; it's ignored.

**`200`** →
```json
{ "order_id": 1, "filled_qty": 4, "total_cost": 400 }
```

Numbers here are real JSON numbers. `filled_qty: 0` means the order rested. A
market order that couldn't fully fill is *cancelled*, never rested — compare
`filled_qty` to your requested `size` to detect it.

`total_cost` is in quote currency and reflects actual execution prices, which may
beat your limit (price improvement is refunded automatically).

**`400`** reject reason · **`401`** · **`409`** duplicate id · **`504`** timeout

### `DELETE /orders/:id` 🔒 + `X-Client-Order-Id`

**`204`** cancelled · **`404`** unknown order *or* not yours · **`409`** · **`504`**

Cancelling a partially-filled order keeps the fills and releases only the
remainder.

### `POST /deposits` 🔒 + `X-Client-Order-Id`

```json
{ "amount": 10000, "currency": "USD" }
```

**`200`** → `{"available": 10000}` (new available balance, as a number)

A dev affordance — a real exchange credits deposits from an observed chain/bank
event, never a client call.

### `POST /withdrawals` 🔒 + `X-Client-Order-Id`

```json
{ "amount": 20, "currency": "USD" }
```

**`204`** · **`400`** `"InsufficientFunds"` · **`409`** · **`504`**

Only draws from `available`; funds reserved against open orders are untouchable
until cancelled.

---

### `GET /balances` 🔒

**`200`** →
```json
[
  { "currency": "SOL", "available": "6",    "reserved": "0" },
  { "currency": "USD", "available": "9400", "reserved": "0" }
]
```

⚠️ **Values are strings.** Sorted by currency. Only currencies the account has
touched appear — an untouched currency is absent, not zero.

`reserved` is locked against open orders: still the user's money, not spendable.
Display total as `available + reserved`.

This reads from the Postgres projection, so it lags the engine by a few
milliseconds after a trade. The `POST /orders` response is authoritative for the
order you just placed.

### `GET /orders` 🔒

**`200`** →
```json
[
  { "order_id": "2", "pair": "SOL-USD", "side": "Bid", "order_type": "Market",
    "price": "0", "size": "2", "filled_qty": "2", "status": "filled" }
]
```

⚠️ **All values are strings**, including `order_id`. Sorted newest first
(`order_id` descending). Returns the account's full order history — every order
ever placed, not just open ones. There is no filtering, pagination, or limit
parameter; filter client-side on `status`.

`status`: `open` · `partially_filled` · `filled` · `cancelled`

Remaining quantity is `size - filled_qty` (parse both first).

---

### `GET /book/:pair`

Public, no auth.

Query: `?depth=N` — price levels per side, default `20`, max `1000`.

**`200`** →
```json
{
  "pair": "SOL-USD",
  "sequence": 4,
  "bids": [],
  "asks": [ { "price": 100, "qty": 4 } ]
}
```

Real numbers here, not strings. `bids` descending (best first), `asks` ascending
(best first). `qty` is the aggregate across all orders at that price.

`sequence` is the event-stream position this snapshot reflects — the key to
[reconciliation](#snapshot--delta-reconciliation).

A syntactically valid but never-traded pair returns `200` with empty sides.
**`400`** only for a malformed pair (bad syntax or unknown currency).

---

### `GET /candles/:pair`

Public, no auth. OHLCV candlesticks, aggregated on read from trade history —
this is what you'd feed a charting library (TradingView's `lightweight-charts`
accepts this shape close to as-is).

Query:

| Param | Required | Meaning |
|---|---|---|
| `interval` | yes | One of `1s`, `15m`, `1h`, `4h`, `1d`, `1w` — anything else is `400` |
| `start` | no | Unix seconds, inclusive. Omit for "most recent" |
| `end` | no | Unix seconds, exclusive |
| `limit` | no | Default `200`, clamped to `1000` |

**`200`** →
```json
[
  { "time": 1785361871, "open": 100, "high": 110, "low": 100, "close": 110, "volume": 2 },
  { "time": 1785361873, "open": 90,  "high": 90,  "low": 90,  "close": 90,  "volume": 3 }
]
```

Real numbers, ascending by `time`. `time` is the **start** of the bucket, in Unix
seconds. `volume` is total base-currency quantity traded in that bucket.

If neither `start` nor `end` is given, you get the most recent `limit` buckets
that actually had a trade. A pair with no trades in range returns `[]` — same
convention as `GET /book/:pair`'s never-traded case.

⚠️ **No gap-filling.** A bucket with zero trades is simply absent, not returned
as a flat candle at the previous close. A quiet hour shrinks the array instead of
padding it — if your chart wants continuous candles, backfill flat ones
client-side from the previous `close`.

⚠️ **Weekly buckets don't start on Monday.** Bucketing is `floor(unix_time /
interval_seconds)`, not calendar-aware — correct and UTC-midnight-aligned for
every interval except `1w`, which starts on Thursday (Unix epoch was a
Thursday). Daily, hourly, and sub-hour buckets are unaffected.

**`400`** bad `interval` or malformed pair · **`422`** unknown currency in pair

---

### Admin: `POST /admin/pairs` 🔒 + `X-Client-Order-Id`

Only the account whose id matches the server's `ADMIN_ACCOUNT_ID`.

```json
{ "pair": "SOL-USD" }
```

**`204`** listed (idempotent — listing twice also returns `204`) ·
**`400`** `"InvalidPair"` if `base == quote` · **`403`** not admin · **`422`** unknown currency

**No pair is tradeable until listed.** On a fresh database, orders fail with
`"InvalidPair"` until an admin lists the market.

### Admin: `DELETE /admin/pairs/:pair` 🔒 + `X-Client-Order-Id`

**`204`** delisted · **`404`** wasn't listed · **`403`** not admin

Blocks *new* orders. Existing resting orders stay in the book and remain
cancellable.

---

## WebSockets

Both feeds are served by the same process as the REST API. Neither replays
history — **you only receive events that occur after you connect.**

### Public: `ws://.../ws/market/:pair`

No auth. One connection per pair. Messages are **internally tagged** with `type`:

```json
{ "type": "book_delta", "seq": 5, "side": "Bid", "price": 50, "qty": 3 }
{ "type": "trade", "seq": 6, "price": 50, "qty": 1, "taker_side": "Ask" }
```

**`book_delta`** — `qty` is the level's **new absolute total**, not a delta.
*Set* it, don't add to it. `qty: 0` means the level is gone; remove it.

**`trade`** — carries no account identity by design. `taker_side` is the
aggressor: `Ask` means a seller crossed the spread (sold into a bid).

A malformed pair fails the handshake (client sees close code `1006`). A valid but
never-traded pair connects fine and simply stays quiet.

### Private: `ws://.../ws/orders`

Per-account order updates. **Auth is a first-message handshake, not a header** —
browsers can't set `Authorization` on a WebSocket:

1. Open the socket. The upgrade succeeds with no credentials.
2. **Immediately send** `{"token":"<jwt>"}` as the first frame.
3. Invalid token, malformed frame, or no frame within **5 seconds** → the server
   closes the connection.

Failed auth closes with **no status code** (browsers report `1005`; abnormal
closure reports `1006`) and **no reason string**. You cannot distinguish "bad
token" from "timed out" from the close event — treat any early close as an auth
failure and re-login.

Messages are **externally tagged** — a single-key wrapper object, unlike the
public feed:

```json
{ "OrderAccepted": { "order_id": 3, "account_id": 1, "pair": "SOL-USD",
                     "side": "Bid", "order_type": "Limit", "price": 50, "size": 3 } }

{ "OrderUpdated": { "order_id": 3, "account_id": 1, "pair": "SOL-USD",
                    "filled_qty": 1, "remaining_qty": 2, "status": "PartiallyFilled" } }
```

Read the message type with `Object.keys(msg)[0]`.

- **`OrderAccepted`** — the order entered the book. Fires for every accepted
  order, including ones that fill immediately.
- **`OrderUpdated`** — the order's state changed. `filled_qty` is **cumulative**
  (set it, don't accumulate). Fires on fills *and* on cancellation.

⚠️ `status` here is **PascalCase** (`Open`, `PartiallyFilled`, `Filled`,
`Cancelled`) — different from REST's snake_case.

Cancellation arrives as `OrderUpdated` with `status: "Cancelled"`; there is no
separate cancel message. A resting order that never trades produces only
`OrderAccepted` — no `OrderUpdated` follows until something happens to it.

You receive only your own account's events; the `account_id` field will always be
yours.

---

## Snapshot + delta reconciliation

To render a live order book without gaps or double-counting, **subscribe before
snapshotting**:

1. Open `ws://.../ws/market/SOL-USD`. Buffer every message; render nothing yet.
2. `GET /book/SOL-USD` → gives you levels plus a `sequence`.
3. Discard buffered messages with `seq <= sequence` (already in the snapshot).
4. Apply the rest in order, then go live.

Doing this in the other order (snapshot first, then subscribe) leaves an
undetectable gap: any event between the two calls is lost with no way to notice.

`seq` increases by exactly 1 per state-changing command, and **multiple messages
can share a `seq`** — one command that trades and changes two levels emits several
messages with the same number. Treat a `seq` as a transaction boundary, not a
per-message id. A gap in `seq` means you missed events; reconnect and re-snapshot.

The same pattern applies to the private feed, using `GET /orders` as the snapshot
— though it carries no sequence number, so reconciliation there is best-effort.

---

## Suggested client flow

```
signup/login                        → store token (24h)
GET /balances, GET /orders          → initial account state
open /ws/orders + send {token}      → live order updates
open /ws/market/:pair (buffer)      → then GET /book/:pair, then reconcile
POST /orders (X-Client-Order-Id: n++)
  200 → apply immediately (authoritative for this order)
  504 → retry same id; 409 means it landed
  400 → show the reject reason
```

Keep the `POST /orders` response as the source of truth for the order you just
placed, and let the WebSocket feed drive everything else. The REST read endpoints
lag slightly behind the engine.

## Known rough edges

Real inconsistencies, listed so they don't look like frontend bugs:

- String-vs-number and snake_case-vs-PascalCase splits described above.
- The two feeds use different tagging conventions.
- Close frames carry no code or reason, so auth failures are opaque.
- No pagination on `GET /orders` — grows without bound.
- No endpoint lists which pairs are currently tradeable; the frontend has to know
  or discover them by trial.
- No `GET /trades` for a raw trade list — `GET /candles/:pair` gives aggregated
  history, live trades are on the WebSocket, but there's no paginated "last N
  individual trades" endpoint.
- `GET /candles/:pair` doesn't gap-fill empty buckets — see its section above.
