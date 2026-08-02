//! Market maker for the public demo, so a visitor never lands on a dead book.
//!
//! Two fixed accounts: a maker that rests quotes on both sides, and a taker
//! that crosses into them. Splitting the roles is what keeps it clear of the
//! engine's self-trade check — the maker's own bid always sits below its own
//! ask, and the taker holds nothing resting.
//!
//! It drives the same public HTTP API as any other client. Nothing here gets
//! privileged access to the engine.

use matching_engine::types::{Currency, Limits, Pair, Side};
use serde::Deserialize;
use serde_json::json;
use std::{error::Error, time::Duration};

/// Quote levels per side.
///
/// The frontend's book panel sizes itself to fit however many levels the
/// container has room for — usually well more than 5 — so a thin book here
/// reads as "empty" even while the market is healthy, just because most of
/// the panel's rows have nothing to show. Ceiling on pushing this higher is
/// the price band: it rejects anything past ~20% of the last trade, and mid
/// is clamped to [60, 160], so at the floor (mid=60) there's only ~12 ticks
/// of room below before a deep bid starts getting silently rejected. 10
/// levels (deepest bid at mid - (SPREAD + 9)) stays inside that with margin
/// even at mid's lowest point.
const LEVELS: u64 = 10;
/// Gap between the maker's best bid and best ask, in price ticks.
///
/// Every taker print lands on one side of this spread or the other, so the
/// spread — not the mid's drift — sets the amplitude of the noise on the
/// chart. At 2 ticks around a mid near 60 that was a ~6% jump between
/// consecutive prints, which swamped the ±1 mid walk entirely and rendered as
/// static rather than a price. 1 tick halves the bounce; the mid still can't
/// move far enough in one tick for the new quotes to cross the previous
/// ones (see the re-quote comment in the main loop).
const SPREAD: u64 = 1;
/// Ticks between deposit top-ups. The engine's ceiling caps the holding, so an
/// over-eager top-up just gets rejected — this only keeps the noise down.
const TOPUP_EVERY: u64 = 25;

#[derive(Deserialize)]
struct AuthResponse {
    token: String,
}

#[derive(Deserialize)]
struct PlaceResponse {
    order_id: u64,
}

/// `GET /orders` renders the numeric columns as strings — they're `BigDecimal`
/// in the projection.
#[derive(Deserialize)]
struct OrderRow {
    order_id: String,
    status: String,
}

#[derive(Deserialize)]
struct BookLevel {
    price: u64,
}

#[derive(Deserialize)]
struct BookSnapshot {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

#[derive(Deserialize)]
struct Candle {
    close: i64,
}

/// xorshift64. The bot only needs jitter, not statistical quality, and this
/// avoids a dependency for it.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

struct Bot {
    http: reqwest::Client,
    base: String,
    pair: Pair,
    /// Seeded from the clock so a restart can't reuse ids the engine's dedup
    /// window still remembers.
    next_cid: u64,
}

impl Bot {
    async fn auth(&mut self, email: &str, password: &str) -> Result<String, Box<dyn Error>> {
        let body = json!({ "email": email, "password": password });
        for path in ["/auth/signup", "/auth/login"] {
            let resp = self
                .http
                .post(format!("{}{path}", self.base))
                .json(&body)
                .send()
                .await?;
            if resp.status().is_success() {
                return Ok(resp.json::<AuthResponse>().await?.token);
            }
        }
        Err(format!("could not sign up or log in as {email}").into())
    }

    fn cid(&mut self) -> u64 {
        self.next_cid += 1;
        self.next_cid
    }

    /// Ok(None) on a rejection — a bot that dies on the first 400 is useless.
    async fn post(
        &mut self,
        path: &str,
        token: &str,
        body: serde_json::Value,
    ) -> Result<Option<reqwest::Response>, Box<dyn Error>> {
        let cid = self.cid();
        let resp = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(token)
            .header("X-Client-Order-Id", cid.to_string())
            .json(&body)
            .send()
            .await?;
        Ok(resp.status().is_success().then_some(resp))
    }

    async fn deposit(&mut self, token: &str, currency: Currency, amount: u64) {
        let body = json!({ "amount": amount, "currency": currency.to_string() });
        let _ = self.post("/deposits", token, body).await;
    }

    async fn place(
        &mut self,
        token: &str,
        order_type: &str,
        side: Side,
        price: u64,
        size: u64,
    ) -> Option<u64> {
        let body = json!({
            "pair": self.pair.to_string(),
            "order_type": order_type,
            "side": match side { Side::Bid => "Bid", Side::Ask => "Ask" },
            "price": price,
            "size": size,
        });
        let resp = self.post("/orders", token, body).await.ok()??;
        resp.json::<PlaceResponse>().await.ok().map(|r| r.order_id)
    }

    async fn cancel(&mut self, token: &str, order_id: u64) {
        let cid = self.cid();
        let _ = self
            .http
            .delete(format!("{}/orders/{order_id}", self.base))
            .bearer_auth(token)
            .header("X-Client-Order-Id", cid.to_string())
            .send()
            .await;
    }

    /// Anything this account still has resting. Used once at startup to clear
    /// leftovers from a previous run — a stale resting order on both sides
    /// would make every later order look like a self-trade.
    async fn open_orders(&self, token: &str) -> Vec<u64> {
        let Ok(resp) = self
            .http
            .get(format!("{}/orders", self.base))
            .bearer_auth(token)
            .send()
            .await
        else {
            return vec![];
        };
        match resp.json::<Vec<OrderRow>>().await {
            Ok(rows) => rows
                .into_iter()
                .filter(|o| o.status == "open" || o.status == "partially_filled")
                .filter_map(|o| o.order_id.parse().ok())
                .collect(),
            Err(e) => {
                println!("Could not read open orders: {e}");
                vec![]
            }
        }
    }

    async fn book(&self) -> Option<BookSnapshot> {
        let resp = self
            .http
            .get(format!("{}/book/{}", self.base, self.pair))
            .send()
            .await
            .ok()?;
        resp.json::<BookSnapshot>().await.ok()
    }

    /// Close of the most recent trade, for seeding `mid` on startup. `mid`
    /// otherwise only updates from the *book*, which a fresh process hasn't
    /// quoted into yet — and startup cleanup just cancelled whatever the
    /// previous run left resting, so the book is briefly empty right when
    /// this matters most. Restarting with a stale hardcoded guess instead of
    /// the real last price risks every quote landing outside the price band
    /// around wherever the market actually drifted to, and getting silently
    /// rejected — a market that looks dead with no error anywhere.
    async fn last_price(&self) -> Option<u64> {
        let resp = self
            .http
            .get(format!(
                "{}/candles/{}?interval=1s&limit=1",
                self.base, self.pair
            ))
            .send()
            .await
            .ok()?;
        let candles: Vec<Candle> = resp.json().await.ok()?;
        candles.last().map(|c| c.close as u64)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let base = env_or("API_BASE", "http://127.0.0.1:3000");
    let pair = Pair::try_from(env_or("BOT_PAIR", "SOL-USD"))?;
    let tick = Duration::from_millis(env_or("BOT_TICK_MS", "4000").parse()?);
    let password = env_or("BOT_PASSWORD", "bot-password");
    let maker_email = env_or("BOT_MAKER_EMAIL", "maker@bot.local");
    let taker_email = env_or("BOT_TAKER_EMAIL", "taker@bot.local");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let mut bot = Bot {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
        base: base.clone(),
        pair,
        next_cid: now_ms,
    };
    let mut rng = Rng(now_ms | 1);

    // compose starts us alongside the API, not after it's listening
    println!("Bot waiting for {base}");
    let maker = loop {
        match bot.auth(&maker_email, &password).await {
            Ok(token) => break token,
            Err(e) => {
                println!("  not ready ({e}), retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    let taker = bot.auth(&taker_email, &password).await?;
    println!("Bot authenticated as {maker_email} / {taker_email}");

    // Whoever signs up first is account 1 and therefore the admin. On a fresh
    // database that's the maker, which is how the market gets listed at all.
    // A 403 here means a human already claimed account 1 and has to list it.
    let listed = bot
        .post("/admin/pairs", &maker, json!({ "pair": pair.to_string() }))
        .await?;
    match listed {
        Some(_) => println!("Listed {pair}"),
        None => println!("Could not list {pair} — not the admin account. Expecting it to exist."),
    }

    let limits = Limits::default();
    let quote_ceiling = (limits.deposit_ceiling)(pair.quote);
    let base_ceiling = (limits.deposit_ceiling)(pair.base);

    // The engine rejects a deposit that would breach the ceiling rather than
    // clamping it, so top up in chunks that always fit.
    let quote_chunk = (quote_ceiling / 5).max(1);
    let base_chunk = (base_ceiling / 5).max(1);

    for token in [maker.clone(), taker.clone()] {
        bot.deposit(&token, pair.quote, quote_ceiling).await;
        bot.deposit(&token, pair.base, base_ceiling).await;

        // Clear anything left resting by a previous run before quoting again.
        let stale = bot.open_orders(&token).await;
        if !stale.is_empty() {
            println!("Cancelling {} stale orders", stale.len());
            for order_id in stale {
                bot.cancel(&token, order_id).await;
            }
        }
    }

    // Anchors the quotes. Re-derived from the book each tick, so the bot
    // tracks where the market actually is rather than drifting on its own —
    // but the book has nothing in it yet this early (see last_price's doc
    // comment), so the *first* value has to come from the real last trade,
    // not a guess. 100 only survives as the fallback for a market that has
    // genuinely never traded.
    let mut mid: u64 = bot.last_price().await.unwrap_or(100);
    let mut resting: Vec<u64> = vec![];
    let mut ticks: u64 = 0;
    // -1, 0 or +1. Held across ticks and re-rolled occasionally, so both the
    // mid walk and the taker's choice of side lean the same way for a while.
    // Without it every print is an independent coin flip, which is why the
    // chart read as noise: a market equally likely to go up or down on every
    // single trade never produces the runs that make a price series look like
    // a price series.
    let mut trend: i64 = 0;

    println!("Quoting {pair} every {}ms", tick.as_millis());
    loop {
        ticks += 1;

        if ticks % TOPUP_EVERY == 0 {
            for token in [maker.clone(), taker.clone()] {
                bot.deposit(&token, pair.quote, quote_chunk).await;
                bot.deposit(&token, pair.base, base_chunk).await;
            }
        }

        if let Some(book) = bot.book().await {
            let best_bid = book.bids.iter().map(|l| l.price).max();
            let best_ask = book.asks.iter().map(|l| l.price).min();
            if let (Some(b), Some(a)) = (best_bid, best_ask) {
                mid = (b + a) / 2;
            }
        }

        // Re-roll the trend now and then; otherwise let it run. One-in-six
        // per tick gives runs averaging ~24s at the default 4s tick — long
        // enough to be visible as a move, short enough that the chart isn't
        // a straight line.
        if rng.below(6) == 0 {
            trend = match rng.below(3) {
                0 => -1,
                1 => 1,
                _ => 0,
            };
        }

        // Trend-weighted ±1 walk with a flat patch, so the walk drifts
        // instead of oscillating. `up_chance` out of 6: a neutral trend keeps
        // the original symmetric walk (2 up, 2 down, 2 flat), while a trend
        // leans it 3-to-1 its way so the price actually travels somewhere
        // over a minute. Clamped well inside the engine's ±20% band around
        // the last trade.
        let up_chance = (2 + trend) as u64;
        let roll = rng.below(6);
        mid = if roll < up_chance {
            mid + 1
        } else if roll < 4 {
            mid.saturating_sub(1)
        } else {
            mid
        }
        .clamp(60, 160);

        // Quote FIRST, then pull the previous quotes — never the other way
        // round. Cancelling all ten resting orders before placing their
        // replacements leaves the book with no levels at all for the width of
        // an HTTP round trip, and since this maker is the only liquidity on
        // the demo, that is a genuinely empty book. The public feed reports
        // every one of those deltas faithfully, so any connected client
        // watches the book empty and refill once per tick. Overlapping the
        // two sets costs nothing (the new orders rest at the same prices the
        // old ones did) and keeps depth continuously on screen.
        let previous = std::mem::take(&mut resting);

        for i in 0..LEVELS {
            let size = 1 + rng.below(5);
            let bid = mid.saturating_sub(SPREAD + i);
            if let Some(id) = bot.place(&maker, "Limit", Side::Bid, bid, size).await {
                resting.push(id);
            }
            let size = 1 + rng.below(5);
            let ask = mid + SPREAD + i;
            if let Some(id) = bot.place(&maker, "Limit", Side::Ask, ask, size).await {
                resting.push(id);
            }
        }

        for order_id in previous {
            bot.cancel(&maker, order_id).await;
        }

        // Several taker prints per tick, spread across it, rather than one
        // print per tick fired in a burst. Two reasons:
        //
        //   - A single trade per bucket means open == high == low == close,
        //     so every candle is a zero-height body that renders as a 1px
        //     dash. A bucket needs more than one print before a candle has a
        //     shape at all.
        //   - At the default 4s tick, a 1s chart had a trade in one bucket
        //     out of four and flat gap-fill in the rest. Spacing the prints
        //     out puts a real trade in most buckets.
        //
        // The sleeps between prints are what pace the loop; the tick sleep
        // that used to sit at the bottom is now distributed here.
        let prints = 2 + rng.below(4);
        let gap = tick / prints as u32;
        for _ in 0..prints {
            // Lean the same way as the trend, 3-to-1, so runs of buys or
            // sells walk the price instead of alternating across the spread.
            let bid_chance = (50 + trend * 25) as u64;
            let side = if rng.below(100) < bid_chance {
                Side::Bid
            } else {
                Side::Ask
            };

            // Market, not limit. A crossing limit order that isn't fully
            // filled RESTS, and once the taker has one resting on each side
            // every later order crosses its own book and gets rejected as a
            // self-trade — it wedges permanently. A market order's remainder
            // is cancelled, so the taker never accumulates anything.
            bot.place(&taker, "Market", side, 0, 1 + rng.below(3)).await;
            tokio::time::sleep(gap).await;
        }
    }
}
