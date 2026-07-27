#!/usr/bin/env bash
# End-to-end smoke test: infra -> servers -> auth -> deposits -> matched trade.
# Drives the real distributed system and asserts on HTTP codes + the DB projection.
#
# Usage:  ./scripts/smoke.sh
# Exits non-zero on the first failed assertion (CI-friendly).
set -euo pipefail

REDIS=trusting_lamport          # redis container name
PG=pg                           # postgres container name
API=http://127.0.0.1:3000
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Load DATABASE_URL / JWT_SECRET for sqlx + the api process.
set -a; source .env; set +a

PIDS=()
cleanup() { [[ ${#PIDS[@]} -gt 0 ]] && kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

redis() { docker exec "$REDIS" redis-cli "$@"; }
psql()  { docker exec "$PG" psql -U postgres -d exchange -tAc "$1"; }

pass() { echo "  ✓ $1"; }
fail() { echo "  ✗ $1"; exit 1; }
assert_eq() { [[ "$2" == "$3" ]] && pass "$1 ($2)" || fail "$1: expected '$3', got '$2'"; }

# POST helper: prints "<body>\n<http_code>". Args: url, json, [header]...
post() {
  local url="$1" body="$2"; shift 2
  local hdrs=(-H 'content-type: application/json')
  local h; for h in "$@"; do [[ -n "$h" ]] && hdrs+=(-H "$h"); done
  curl -s -w '\n%{http_code}' -X POST "$url" "${hdrs[@]}" -d "$body"
}
wait_for() { # poll a command until it succeeds, up to ~15s
  for _ in $(seq 1 50); do "$@" >/dev/null 2>&1 && return 0; sleep 0.3; done
  fail "timed out waiting for: $*"
}

echo "== 1. infra =="
docker start "$PG" "$REDIS" >/dev/null
wait_for docker exec "$PG" pg_isready -U postgres
wait_for docker exec "$REDIS" redis-cli ping
pass "postgres + redis up"

echo "== 2. migrations =="
sqlx migrate run --source ./migrations   # let output/errors through, never hide them
pass "migrations applied"

echo "== 3. clean slate =="
redis DEL commands events >/dev/null
# RESTART IDENTITY resets the users BIGSERIAL so signups get ids 1, 2 again.
psql "TRUNCATE trades, balances, orders, users RESTART IDENTITY;" >/dev/null
pass "streams flushed, tables truncated"

echo "== 4. build + start servers =="
cargo build --bins -q
./target/debug/engine    >/tmp/smoke_engine.log    2>&1 & PIDS+=($!)
./target/debug/db_writer >/tmp/smoke_dbwriter.log  2>&1 & PIDS+=($!)
# Wait until the engine's consumer group exists, so seeded deposits aren't
# added before the engine is consuming.
wait_for bash -c "docker exec $REDIS redis-cli XINFO GROUPS commands | grep -q engine-group"
./target/debug/api       >/tmp/smoke_api.log       2>&1 & PIDS+=($!)
wait_for curl -s -o /dev/null "$API/"
pass "engine, db_writer, api up"

echo "== 5. auth (signup two users; ids 1 and 2) =="
TOK1=$(post "$API/auth/signup" '{"email":"buyer@x.com","password":"pw1"}'  | head -1 | jq -r .token)
TOK2=$(post "$API/auth/signup" '{"email":"seller@x.com","password":"pw2"}' | head -1 | jq -r .token)
[[ -n "$TOK1" && "$TOK1" != null ]] && pass "buyer token" || fail "no buyer token"
[[ -n "$TOK2" && "$TOK2" != null ]] && pass "seller token" || fail "no seller token"
# reject: bad login
code=$(post "$API/auth/login" '{"email":"buyer@x.com","password":"wrong"}' | tail -1)
assert_eq "bad login rejected" "$code" "401"

echo "== 6. seed balances (XADD; no deposit endpoint yet) =="
redis XADD commands '*' data '{"correlation_id":"00000000-0000-0000-0000-000000000000","account_id":1,"client_order_id":900,"command":{"Deposit":{"amount":1000,"currency":"USD"}}}' >/dev/null
redis XADD commands '*' data '{"correlation_id":"00000000-0000-0000-0000-000000000000","account_id":2,"client_order_id":901,"command":{"Deposit":{"amount":10,"currency":"SOL"}}}' >/dev/null
sleep 0.5
pass "deposits seeded (acct1 1000 USD, acct2 10 SOL)"

echo "== 7. orders (identity from the JWT) =="
# seller (acct 2) rests an ask
resp=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Ask","price":100,"size":10}' "Authorization: Bearer $TOK2" "X-Client-Order-Id: 1")
assert_eq "ask accepted"     "$(echo "$resp" | tail -1)" "200"
assert_eq "ask rests (fill 0)" "$(echo "$resp" | head -1 | jq .filled_qty)" "0"
# buyer (acct 1) crosses and fills
resp=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":100,"size":10}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 2")
assert_eq "bid accepted"     "$(echo "$resp" | tail -1)" "200"
assert_eq "bid fills 10"     "$(echo "$resp" | head -1 | jq .filled_qty)" "10"
# no token -> 401
code=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":100,"size":10}' "X-Client-Order-Id: 3" | tail -1)
assert_eq "order without token rejected" "$code" "401"
# missing idempotency header -> 400
code=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":100,"size":10}' "Authorization: Bearer $TOK1" | tail -1)
assert_eq "order without X-Client-Order-Id rejected" "$code" "400"
# idempotency: re-send buyer's order verbatim -> 409
code=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":100,"size":10}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 2" | tail -1)
assert_eq "duplicate client_order_id rejected" "$code" "409"

echo "== 8. verify projection =="
sleep 0.5
assert_eq "one trade recorded"  "$(psql 'select count(*) from trades')" "1"
assert_eq "trade price"         "$(psql 'select price from trades')"    "100"
assert_eq "buyer holds 10 SOL"  "$(psql "select available from balances where account_id=1 and currency='SOL'")" "10"
assert_eq "seller holds 1000 USD" "$(psql "select available from balances where account_id=2 and currency='USD'")" "1000"
assert_eq "trade tagged with pair" "$(psql 'select pair from trades')" "SOL-USD"
# fill tracking: both sides fully filled, cumulative qty recorded
assert_eq "both orders filled"   "$(psql "select count(*) from orders where status='filled'")" "2"
assert_eq "no orders left open"  "$(psql "select count(*) from orders where status='open'")"   "0"
assert_eq "filled_qty recorded"  "$(psql 'select distinct filled_qty from orders')" "10"

echo
echo "ALL CHECKS PASSED"
