#!/usr/bin/env bash
# Seed SOL-USD on a running API with a realistic mix of orders: some that
# rest untouched (open), some that fully cross (filled), some that only
# partially cross (partially filled) — plus a random price walk so a candle
# chart has something to draw. Re-runnable: makes fresh buyer/seller accounts
# each run, so it never collides with a previous run's state.
#
# Usage:
#   ./scripts/seed.sh [rounds] [delay_seconds]
#     rounds        number of trade rounds to generate (default 20)
#     delay_seconds sleep between rounds, so a polling chart sees data
#                    land incrementally instead of all at once (default 2)
#
# Env:
#   API  base URL of the api bin (default http://127.0.0.1:3000)
set -euo pipefail

API=${API:-http://127.0.0.1:3000}
PAIR=SOL-USD
ROUNDS=${1:-20}
DELAY=${2:-2}
SUFFIX="$(date +%s)$RANDOM"

req() { # method path json-body [-H "..." ...]
  local method=$1 path=$2 body=$3; shift 3
  curl -s -X "$method" "$API$path" -H 'content-type: application/json' "$@" -d "$body"
}

signup() { req POST /auth/signup "{\"email\":\"$1-$SUFFIX@seed.local\",\"password\":\"pw\"}" | jq -r .token; }

# A plain counter, incremented in the caller's own shell (never inside a
# `$(...)` subshell, whose writes to CID wouldn't survive back to the parent).
CID=0

deposit() { # token amount currency
  CID=$((CID + 1))
  req POST /deposits "{\"amount\":$2,\"currency\":\"$3\"}" \
    -H "Authorization: Bearer $1" -H "X-Client-Order-Id: $CID" >/dev/null
}

order() { # token side order_type price size -> prints "status filled_qty/size"
  local tok=$1 side=$2 type=$3 price=$4 size=$5
  CID=$((CID + 1))
  local resp filled
  resp=$(req POST /orders \
    "{\"pair\":\"$PAIR\",\"order_type\":\"$type\",\"side\":\"$side\",\"price\":$price,\"size\":$size}" \
    -H "Authorization: Bearer $tok" -H "X-Client-Order-Id: $CID")
  filled=$(echo "$resp" | jq -r '.filled_qty // "rejected"')
  echo "$side $type price=$price size=$size filled=$filled"
}

# clamp a price so the random walk can't drift into silly territory
clamp() { local v=$1 lo=$2 hi=$3; (( v < lo )) && v=$lo; (( v > hi )) && v=$hi; echo "$v"; }

echo "== signup buyer + seller (suffix $SUFFIX) =="
BUYER_TOK=$(signup buyer)
SELLER_TOK=$(signup seller)
[[ -n "$BUYER_TOK" && "$BUYER_TOK" != null ]] || { echo "buyer signup failed" >&2; exit 1; }
[[ -n "$SELLER_TOK" && "$SELLER_TOK" != null ]] || { echo "seller signup failed" >&2; exit 1; }
echo "  buyer + seller ready"

echo "== fund accounts =="
deposit "$BUYER_TOK" 10000000 USD
deposit "$SELLER_TOK" 1000000 SOL
echo "  buyer: 10,000,000 USD / seller: 1,000,000 SOL"

echo "== deep resting orders (guaranteed to stay open) =="
order "$SELLER_TOK" Ask Limit 150 20
order "$BUYER_TOK"  Bid Limit 50  20

price=100
echo "== $ROUNDS rounds of trading, price walking from $price =="
for ((i = 1; i <= ROUNDS; i++)); do
  step=$(( (RANDOM % 5) - 2 )) # -2..2
  price=$(clamp $((price + step)) 80 120)

  echo "-- round $i (price ~$price) --"
  # chunky resting orders a bit off the touch: some will only get partially
  # eaten by the smaller taker orders below, so they end up PartiallyFilled
  # rather than Filled or Open.
  order "$SELLER_TOK" Ask Limit $((price + 2)) $(( 5 + RANDOM % 6 ))
  order "$BUYER_TOK"  Bid Limit $((price - 2)) $(( 5 + RANDOM % 6 ))

  # small market buy sweeps the best ask on the book (this round's or an
  # older one) -> a Filled taker order and a Filled/PartiallyFilled maker.
  order "$BUYER_TOK" Bid Market 0 $(( 1 + RANDOM % 3 ))
  # small crossing sell sweeps the best bid -> same idea, other side.
  order "$SELLER_TOK" Ask Limit $((price - 2)) $(( 1 + RANDOM % 3 ))

  sleep "$DELAY"
done

echo
echo "done. buyer/seller: buyer-$SUFFIX@seed.local / seller-$SUFFIX@seed.local (password: pw)"
