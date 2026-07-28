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
# the resting ask must show up in the live book projection WHILE it rests —
# proves the projection is actually being fed, not just reporting empty.
sleep 0.5
book=$(curl -s "$API/book/SOL-USD")
assert_eq "resting ask visible in book" "$(echo "$book" | jq -c '.asks')" '[{"price":100,"qty":10}]'
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

echo "== 8. deposits / withdrawals =="
code=$(post "$API/deposits" '{"amount":50,"currency":"USD"}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 10" | tail -1)
assert_eq "deposit accepted" "$code" "200"
code=$(post "$API/withdrawals" '{"amount":20}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 11" | tail -1)
assert_eq "withdrawal accepted" "$code" "204"
# more than available -> engine rejects
code=$(post "$API/withdrawals" '{"amount":999999}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 12" | tail -1)
assert_eq "overdraft withdrawal rejected" "$code" "400"

echo "== 9. cancel (ownership enforced by the engine) =="
# buyer rests a bid that won't cross
resp=$(post "$API/orders" '{"pair":"SOL-USD","order_type":"Limit","side":"Bid","price":1,"size":5}' "Authorization: Bearer $TOK1" "X-Client-Order-Id: 20")
OID=$(echo "$resp" | head -1 | jq -r .order_id)
assert_eq "resting bid accepted" "$(echo "$resp" | tail -1)" "200"
# the seller must NOT be able to cancel the buyer's order
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/orders/$OID" \
  -H "Authorization: Bearer $TOK2" -H 'X-Client-Order-Id: 21')
assert_eq "non-owner cancel rejected" "$code" "404"
# the owner can
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/orders/$OID" \
  -H "Authorization: Bearer $TOK1" -H 'X-Client-Order-Id: 22')
assert_eq "owner cancel succeeds" "$code" "204"
# unknown order
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/orders/999999" \
  -H "Authorization: Bearer $TOK1" -H 'X-Client-Order-Id: 23')
assert_eq "cancel of unknown order 404s" "$code" "404"

echo "== 10. reads (account-scoped, off the projection) =="
sleep 0.5
bal1=$(curl -s "$API/balances" -H "Authorization: Bearer $TOK1")
assert_eq "buyer sees own SOL balance" "$(echo "$bal1" | jq -r '.[] | select(.currency=="SOL") | .available')" "10"
ord1=$(curl -s "$API/orders" -H "Authorization: Bearer $TOK1")
# buyer placed: the filled bid + the cancelled bid
assert_eq "buyer sees only own orders" "$(echo "$ord1" | jq 'length')" "2"
assert_eq "cancelled order reflected" "$(echo "$ord1" | jq -r '[.[] | select(.status=="cancelled")] | length')" "1"
ord2=$(curl -s "$API/orders" -H "Authorization: Bearer $TOK2")
assert_eq "seller sees only own orders" "$(echo "$ord2" | jq 'length')" "1"
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/balances")
assert_eq "reads require auth" "$code" "401"

echo "== 11. verify projection =="
sleep 0.5
assert_eq "one trade recorded"  "$(psql 'select count(*) from trades')" "1"
assert_eq "trade price"         "$(psql 'select price from trades')"    "100"
assert_eq "buyer holds 10 SOL"  "$(psql "select available from balances where account_id=1 and currency='SOL'")" "10"
assert_eq "seller holds 1000 USD" "$(psql "select available from balances where account_id=2 and currency='USD'")" "1000"
assert_eq "trade tagged with pair" "$(psql 'select pair from trades')" "SOL-USD"
# fill tracking: both sides fully filled, cumulative qty recorded
assert_eq "both orders filled"   "$(psql "select count(*) from orders where status='filled'")" "2"
assert_eq "no orders left open"  "$(psql "select count(*) from orders where status='open'")"   "0"
assert_eq "filled_qty recorded"  "$(psql "select distinct filled_qty from orders where status='filled'")" "10"

echo "== 12. book projection (in-memory, public market data) =="
sleep 1  # let the live tail catch up on the cancel from step 9
book=$(curl -s "$API/book/SOL-USD")
assert_eq "book snapshot has a sequence field" "$(echo "$book" | jq 'has("sequence")')" "true"
# the fill emptied the ask level and the cancel emptied the bid level — the
# projection must reflect BOTH removals, not just additions.
assert_eq "SOL-USD book empty after fill+cancel (bids)" "$(echo "$book" | jq '.bids | length')" "0"
assert_eq "SOL-USD book empty after fill+cancel (asks)" "$(echo "$book" | jq '.asks | length')" "0"
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/book/NOTREAL-PAIR")
assert_eq "malformed pair rejected" "$code" "400"
# a syntactically valid but never-traded pair is an empty book, not an error.
untraded=$(curl -s -w '\n%{http_code}' "$API/book/USD-SOL")
assert_eq "untraded pair is 200" "$(echo "$untraded" | tail -1)" "200"
assert_eq "untraded pair has empty bids" "$(echo "$untraded" | head -1 | jq '.bids | length')" "0"
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/book/SOL-USD")
assert_eq "book endpoint requires no auth" "$code" "200"

echo
echo "ALL CHECKS PASSED"
