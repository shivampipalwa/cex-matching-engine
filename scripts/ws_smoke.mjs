// Helper for smoke.sh — not a general-purpose client. Opens the M7 websocket
// feeds, collects whatever arrives for a fixed window, and dumps it as one
// JSON object so smoke.sh can assert on it with jq. Requires Node's global
// `WebSocket` (stable since Node 22; no flag needed).
//
// argv: apiBase pair token1 token2 badToken
const [, , apiBase, pair, token1, token2, badToken] = process.argv;

const COLLECT_MS = 3500;

const market = new WebSocket(`${apiBase}/ws/market/${pair}`);
const orders1 = new WebSocket(`${apiBase}/ws/orders`); // authenticates as token1
const orders2 = new WebSocket(`${apiBase}/ws/orders`); // authenticates as token2 — must NOT see token1's orders
const badAuth = new WebSocket(`${apiBase}/ws/orders`); // bad token — must get closed

const market_ = [];
const orders1_ = [];
const orders2_ = [];
let badClosed = false;

const opened = (ws) => new Promise((resolve) => ws.addEventListener("open", resolve, { once: true }));

market.addEventListener("message", (e) => market_.push(JSON.parse(e.data)));
orders1.addEventListener("message", (e) => orders1_.push(JSON.parse(e.data)));
orders2.addEventListener("message", (e) => orders2_.push(JSON.parse(e.data)));
badAuth.addEventListener("close", () => {
  badClosed = true;
});

// Every real connection must be open (and the private ones authenticated)
// before smoke.sh fires the order it expects these feeds to report — a
// broadcast channel has no history, so subscribing late means missing it.
await opened(market);
await opened(orders1);
orders1.send(JSON.stringify({ token: token1 }));
await opened(orders2);
orders2.send(JSON.stringify({ token: token2 }));
await opened(badAuth);
badAuth.send(JSON.stringify({ token: badToken }));

// Signal readiness on stderr so it doesn't pollute the stdout JSON payload.
console.error("READY");

await new Promise((r) => setTimeout(r, COLLECT_MS));

console.log(
  JSON.stringify({
    market: market_,
    orders1: orders1_,
    orders2: orders2_,
    badClosed,
  }),
);
process.exit(0);
