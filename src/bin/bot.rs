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
const LEVELS: u64 = 5;
/// Gap between the maker's best bid and best ask, in price ticks.
const SPREAD: u64 = 2;
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

#[derive(Deserialize)]
struct BookLevel {
    price: u64,
}

#[derive(Deserialize)]
struct BookSnapshot {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
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
        side: Side,
        price: u64,
        size: u64,
    ) -> Option<u64> {
        let body = json!({
            "pair": self.pair.to_string(),
            "order_type": "Limit",
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

    async fn book(&self) -> Option<BookSnapshot> {
        let resp = self
            .http
            .get(format!("{}/book/{}", self.base, self.pair))
            .send()
            .await
            .ok()?;
        resp.json::<BookSnapshot>().await.ok()
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
    }

    // Anchors the quotes. Re-derived from the book each tick, so the bot
    // tracks where the market actually is rather than drifting on its own.
    let mut mid: u64 = 100;
    let mut resting: Vec<u64> = vec![];
    let mut ticks: u64 = 0;

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

        // ±1 with a flat patch, so the walk drifts instead of oscillating.
        // Clamped well inside the engine's ±20% band around the last trade.
        mid = match rng.below(4) {
            0 => mid.saturating_sub(1),
            1 => mid + 1,
            _ => mid,
        }
        .clamp(60, 160);

        for order_id in std::mem::take(&mut resting) {
            bot.cancel(&maker, order_id).await;
        }

        for i in 0..LEVELS {
            let size = 1 + rng.below(5);
            let bid = mid.saturating_sub(SPREAD + i);
            if let Some(id) = bot.place(&maker, Side::Bid, bid, size).await {
                resting.push(id);
            }
            let size = 1 + rng.below(5);
            let ask = mid + SPREAD + i;
            if let Some(id) = bot.place(&maker, Side::Ask, ask, size).await {
                resting.push(id);
            }
        }

        // The taker crosses into the maker's touch, which is what actually
        // prints a trade and moves the chart.
        let (side, price) = if rng.below(2) == 0 {
            (Side::Bid, mid + SPREAD)
        } else {
            (Side::Ask, mid.saturating_sub(SPREAD))
        };
        bot.place(&taker, side, price, 1 + rng.below(3)).await;

        tokio::time::sleep(tick).await;
    }
}
