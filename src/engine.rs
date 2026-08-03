use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::book::{MatchResponse, Order, OrderBook, OrderRequest, OrderStatus, OrderType, Side};
use crate::command::{CancelRequest, Command, CommandResponse, DepositRequest, PlaceOrderResponse};
use crate::error::RejectReason;
use crate::event::{Event, EventBatch};
use crate::ledger::Ledger;
use crate::market::{AccountId, Currency, Pair};

/// Abuse guards for a public deployment, where `/deposits` is an open faucet
/// and anyone can trade. Policy, not state — benchmarks turn them off with
/// `Limits::none()` to measure matching rather than these checks.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Most an account may hold of a currency (available + reserved). Capping
    /// the holding rather than the request means asking twice doesn't help.
    pub deposit_ceiling: fn(Currency) -> u64,
    /// How far a limit order may sit from the last traded price, in bps.
    pub price_band_bps: Option<u64>,
    pub prevent_self_trade: bool,
}

fn demo_deposit_ceiling(currency: Currency) -> u64 {
    match currency {
        Currency::USD => 100_000,
        Currency::SOL => 1_000,
    }
}

fn no_deposit_ceiling(_: Currency) -> u64 {
    u64::MAX
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            deposit_ceiling: demo_deposit_ceiling,
            price_band_bps: Some(2000),
            prevent_self_trade: true,
        }
    }
}

impl Limits {
    pub fn none() -> Self {
        Limits {
            deposit_ceiling: no_deposit_ceiling,
            price_band_bps: None,
            prevent_self_trade: false,
        }
    }
}

/// Idempotency keys seen so far, bounded to the most recent
/// `DEDUP_WINDOW` per account instead of growing forever. Eviction is by
/// insertion count, not wall-clock time, so it stays deterministic under replay.
#[derive(Debug, Serialize, Deserialize)]
pub struct Dedup {
    pub seen: HashSet<(AccountId, u64)>,
    pub order: HashMap<AccountId, VecDeque<u64>>,
}

/// Most recent client_order_ids kept per account before older ones are evicted.
const DEDUP_WINDOW: usize = 1000;

impl Dedup {
    /// Returns false if this (account_id, client_order_id) was already seen.
    fn insert(&mut self, account_id: AccountId, client_order_id: u64) -> bool {
        if !self.seen.insert((account_id, client_order_id)) {
            return false;
        }
        let recent = self.order.entry(account_id).or_default();
        recent.push_back(client_order_id);
        if recent.len() > DEDUP_WINDOW {
            let evicted = recent.pop_front().expect("just checked len > 0");
            self.seen.remove(&(account_id, evicted));
        }
        true
    }
}

/// Widened to at least ±1 so a market trading in single digits doesn't collapse
/// to a band of one price.
fn price_band(last: u64, bps: u64) -> (u64, u64) {
    let delta = (last.saturating_mul(bps) / 10_000).max(1);
    (last.saturating_sub(delta), last.saturating_add(delta))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Engine {
    /// One book per market. Created on first order for that pair.
    pub books: HashMap<Pair, OrderBook>,
    /// Routes a cancel (which carries only an order_id) to the right book.
    pub order_pair: HashMap<u64, Pair>,
    /// Global, so ids stay unique across every book.
    pub next_order_id: u64,
    /// Sequence stamped on each emitted EventBatch. Engine state, so silent
    /// replay reproduces the same numbering.
    pub next_seq: u64,
    pub ledger: Ledger,
    pub dedup: Dedup,
    /// Markets open for trading. `place_order` rejects any pair not in here —
    /// listing is itself a command, so this is replayable state like everything else.
    pub listed_pairs: HashSet<Pair>,
    /// Config, not state: never snapshotted, so a restore always comes back
    /// with the deployment's own limits rather than whatever was set when the
    /// snapshot was written.
    #[serde(skip)]
    pub limits: Limits,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            books: HashMap::new(),
            order_pair: HashMap::new(),
            next_order_id: 0,
            next_seq: 0,
            ledger: Ledger {
                balances: HashMap::new(),
                dirty: HashSet::new(),
            },
            dedup: Dedup {
                seen: HashSet::new(),
                order: HashMap::new(),
            },
            listed_pairs: HashSet::new(),
            limits: Limits::default(),
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    fn place_order(
        &mut self,
        account_id: AccountId,
        req: &OrderRequest,
    ) -> Result<MatchResponse, RejectReason> {
        if !req.pair.is_valid() || !self.listed_pairs.contains(&req.pair) {
            return Err(RejectReason::InvalidPair);
        }

        if let Some(book) = self.books.get(&req.pair) {
            if req.order_type == OrderType::Limit {
                if let (Some(bps), Some(last)) = (self.limits.price_band_bps, book.last_trade_price)
                {
                    let (low, high) = price_band(last, bps);
                    if req.price < low || req.price > high {
                        return Err(RejectReason::PriceOutOfBand);
                    }
                }
            }
            if self.limits.prevent_self_trade && book.would_self_trade(account_id, req) {
                return Err(RejectReason::SelfTrade);
            }
        }
        // A market buy has no limit price to size a reserve from, but nothing
        // else can touch the book before `add_order` below runs, so the exact
        // cost can be walked here instead of estimated.
        let quote_amount = if req.order_type == OrderType::Market && req.side == Side::Bid {
            self.books
                .get(&req.pair)
                .map(|book| book.market_buy_cost(req.size))
                .transpose()?
                .unwrap_or(0)
        } else {
            req.price
                .checked_mul(req.size)
                .ok_or(RejectReason::InvalidAmount)?
        };

        match req.side {
            Side::Bid => {
                self.ledger
                    .reserve(account_id, req.pair.quote, quote_amount)?;
            }
            Side::Ask => {
                self.ledger.reserve(account_id, req.pair.base, req.size)?;
            }
        }

        // match in this pair's book, creating it on first use
        let order_id = self.next_order_id;
        self.next_order_id += 1;
        let book = self.books.entry(req.pair).or_default();
        let match_result = book.add_order(order_id, account_id, req);

        // route future cancels; drop makers this trade fully filled
        let rested = book.order_index.contains_key(&order_id);
        let filled_makers: Vec<u64> = match_result
            .trades
            .iter()
            .map(|t| t.maker_id)
            .filter(|id| !book.order_index.contains_key(id))
            .collect();
        if rested {
            self.order_pair.insert(order_id, req.pair);
        }
        for id in filled_makers {
            self.order_pair.remove(&id);
        }

        //settle trades
        for trade in match_result.trades.iter() {
            self.ledger.settle(req.pair, trade);
        }

        // A market sell's unfilled remainder never rests, so its reserve has no
        // open commitment behind it — release it or it's stuck forever. A market
        // buy has no equivalent: its reserve was walked to the exact fillable
        // cost, so there's nothing left over to release.
        if req.order_type == OrderType::Market
            && req.side == Side::Ask
            && match_result.taker_remaining > 0
        {
            let _ = self
                .ledger
                .release(account_id, req.pair.base, match_result.taker_remaining);
        }

        // buyer surplus from reserved -> available on a price-improved fill.
        // Market buys have no limit price to compare against (and none is
        // needed — their reserve is already the exact walked cost).
        if req.side == Side::Bid && req.order_type == OrderType::Limit {
            let surplus = match_result
                .trades
                .iter()
                .map(|t| (req.price - t.price) * t.qty)
                .sum();
            if surplus > 0 {
                self.ledger.release(account_id, req.pair.quote, surplus)?;
            }
        }

        Ok(match_result)
    }

    fn deposit(
        &mut self,
        account_id: AccountId,
        req: &DepositRequest,
    ) -> Result<u64, RejectReason> {
        let after = self
            .ledger
            .held(account_id, req.currency)
            .checked_add(req.amount)
            .ok_or(RejectReason::InvalidAmount)?;
        if after > (self.limits.deposit_ceiling)(req.currency) {
            return Err(RejectReason::DepositLimitExceeded);
        }
        Ok(self.ledger.deposit(req.currency, account_id, req.amount))
    }

    // Returns the removed order (with its market) plus the book deltas its
    // removal caused, so callers can emit both the order's final state and the
    // level change.
    fn cancel_order(
        &mut self,
        account_id: AccountId,
        req: &CancelRequest,
    ) -> Option<(Pair, Order, Vec<(Side, u64, u64)>)> {
        let &pair = self.order_pair.get(&req.order_id)?;
        let book = self.books.get_mut(&pair)?;
        let (order, book_deltas) = book.cancel_order(account_id, req.order_id)?;
        let (currency, amount) = match order.side {
            Side::Bid => (pair.quote, order.remaining_size * order.price),
            Side::Ask => (pair.base, order.remaining_size),
        };
        let _ = self.ledger.release(order.account_id, currency, amount);
        self.order_pair.remove(&req.order_id);
        Some((pair, order, book_deltas))
    }
}

pub fn apply(
    engine: &mut Engine,
    account_id: AccountId,
    client_order_id: u64,
    command: Command,
) -> (CommandResponse, Option<EventBatch>) {
    // Idempotency: `insert` returns false if the key was already present, i.e.
    // this is a lost-ack retry. Bail before touching any state or emitting
    // anything.
    if !engine.dedup.insert(account_id, client_order_id) {
        return (CommandResponse::Duplicate, None);
    }

    let mut events = vec![];
    let command_response = match command {
        Command::Place(order_request) => {
            let place_order_res = engine.place_order(account_id, &order_request);
            if let Ok(match_response) = place_order_res {
                let response = PlaceOrderResponse {
                    order_id: match_response.order_id,
                    filled_qty: match_response.trades.iter().map(|t| t.qty).sum(),
                    total_cost: match_response.trades.iter().map(|t| t.qty * t.price).sum(),
                };
                events.push(Event::OrderAccepted {
                    order_id: match_response.order_id,
                    account_id: account_id,
                    pair: order_request.pair,
                    side: order_request.side,
                    order_type: order_request.order_type,
                    price: order_request.price,
                    size: order_request.size,
                });
                match_response
                    .trades
                    .iter()
                    .for_each(|t| events.push(Event::Trade(*t)));
                for u in match_response.updates.iter() {
                    events.push(Event::OrderUpdated {
                        order_id: u.order_id,
                        account_id: u.account_id,
                        pair: order_request.pair,
                        filled_qty: u.filled_qty,
                        remaining_qty: u.remaining_size,
                        status: u.status,
                    });
                }
                for (side, price, qty) in match_response.book_deltas.iter().copied() {
                    events.push(Event::BookDelta {
                        pair: order_request.pair,
                        side,
                        price,
                        qty,
                    });
                }
                CommandResponse::Place(Ok(response))
            } else {
                let e = place_order_res.unwrap_err();
                CommandResponse::Place(Err(e))
            }
        }
        Command::Cancel(cancel_request) => {
            let outcome = engine.cancel_order(account_id, &cancel_request);
            // `outcome` now carries a Vec (book_deltas), so it's not Copy —
            // capture the bool before the `if let` moves it.
            let cancelled = outcome.is_some();
            if let Some((pair, order, book_deltas)) = outcome {
                events.push(Event::OrderCancelled {
                    order_id: cancel_request.order_id,
                });
                events.push(Event::OrderUpdated {
                    order_id: order.id,
                    account_id: order.account_id,
                    pair,
                    filled_qty: order.size - order.remaining_size,
                    remaining_qty: order.remaining_size,
                    status: OrderStatus::Cancelled,
                });
                for (side, price, qty) in book_deltas {
                    events.push(Event::BookDelta {
                        pair,
                        side,
                        price,
                        qty,
                    });
                }
            }
            CommandResponse::Cancel(cancelled)
        }
        Command::Deposit(deposit_request) => {
            CommandResponse::Deposit(engine.deposit(account_id, &deposit_request))
        }
        Command::Withdraw(withdraw_request) => {
            let res = engine.ledger.withdraw(
                account_id,
                withdraw_request.currency,
                withdraw_request.amount,
            );
            CommandResponse::Withdraw(res)
        }
        Command::ListPair(pair) => {
            if !pair.is_valid() {
                CommandResponse::ListPair(Err(RejectReason::InvalidPair))
            } else {
                CommandResponse::ListPair(Ok(engine.listed_pairs.insert(pair)))
            }
        }
        Command::DelistPair(pair) => CommandResponse::DelistPair(engine.listed_pairs.remove(&pair)),
    };
    for (account_id, currency) in engine.ledger.take_dirty() {
        let b = &engine.ledger.balances[&account_id][&currency];
        events.push(Event::BalanceChanged {
            account_id,
            currency,
            available: b.available,
            reserved: b.reserved,
        });
    }

    // A command that changed nothing observable (e.g. a failed cancel) gets no
    // seq and emits no batch — seq numbers stay contiguous for consumers doing
    // gap detection. Assigning seq HERE (inside apply) means both the live loop
    // and silent replay advance `next_seq` identically — determinism for free.
    if events.is_empty() {
        return (command_response, None);
    }
    let seq = engine.next_seq;
    engine.next_seq += 1;
    (command_response, Some(EventBatch { seq, events }))
}

#[cfg(test)]
mod ledger_tests {
    use super::*;
    use crate::book::OrderUpdate;
    use crate::test_util::*;

    // (available, reserved) for one account+currency, zeros if absent.
    fn bal(engine: &Engine, acct: AccountId, ccy: Currency) -> (u64, u64) {
        engine
            .ledger
            .balances
            .get(&acct)
            .and_then(|m| m.get(&ccy))
            .map(|b| (b.available, b.reserved))
            .unwrap_or((0, 0))
    }

    // Total of a currency across every account (available + reserved).
    fn total(engine: &Engine, ccy: Currency) -> u64 {
        engine
            .ledger
            .balances
            .values()
            .filter_map(|m| m.get(&ccy))
            .map(|b| b.available + b.reserved)
            .sum()
    }

    // Submit through the full Engine path: reserve → match → settle → refund.
    fn submit(
        engine: &mut Engine,
        account_id: AccountId,
        side: Side,
        order_type: OrderType,
        price: u64,
        size: u64,
    ) -> Result<MatchResponse, RejectReason> {
        submit_pair(engine, account_id, TEST_PAIR, side, order_type, price, size)
    }

    // The update for one order out of a command's updates.
    fn find_update(res: &MatchResponse, order_id: u64) -> OrderUpdate {
        *res.updates
            .iter()
            .find(|u| u.order_id == order_id)
            .expect("no update for that order")
    }

    // Same as `submit` but for an explicit market.
    fn submit_pair(
        engine: &mut Engine,
        account_id: AccountId,
        pair: Pair,
        side: Side,
        order_type: OrderType,
        price: u64,
        size: u64,
    ) -> Result<MatchResponse, RejectReason> {
        engine.place_order(
            account_id,
            &OrderRequest {
                pair,
                order_type,
                side,
                price,
                size,
            },
        )
    }

    // A buy with no funds is rejected before anything is reserved or matched.
    #[test]
    fn test_buy_rejected_when_broke() {
        let mut engine = new_engine();
        let res = submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10);
        assert!(matches!(res, Err(RejectReason::InsufficientFunds)));
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
    }

    // Buyer as taker: full settlement moves USD one way, SOL the other,
    // and leaves nothing reserved on either side.
    #[test]
    fn test_buy_taker_settles() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10); // seller
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 10).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 1000); // buyer
        submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();

        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0)); // buyer paid
        assert_eq!(bal(&engine, 1, Currency::SOL), (10, 0)); // buyer received
        assert_eq!(bal(&engine, 2, Currency::USD), (1000, 0)); // seller received
        assert_eq!(bal(&engine, 2, Currency::SOL), (0, 0)); // seller delivered
    }

    // Seller as taker: exercises the Ask settlement branch.
    #[test]
    fn test_sell_taker_settles() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::USD, 1, 1000); // buyer rests a bid
        submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();
        engine.ledger.deposit(Currency::SOL, 2, 10); // seller takes
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 10).unwrap();

        assert_eq!(bal(&engine, 2, Currency::SOL), (0, 0));
        assert_eq!(bal(&engine, 2, Currency::USD), (1000, 0));
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
        assert_eq!(bal(&engine, 1, Currency::SOL), (10, 0));
    }

    // Taker buys cheaper than its limit — the surplus is refunded, not stuck.
    #[test]
    fn test_price_improvement_refund() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 95, 10).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 1000);
        submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();

        // Reserved 1000, spent 950, 50 refunded to available.
        assert_eq!(bal(&engine, 1, Currency::USD), (50, 0));
        assert_eq!(bal(&engine, 1, Currency::SOL), (10, 0));
        assert_eq!(bal(&engine, 2, Currency::USD), (950, 0));
    }

    // Different markets must never cross: a SOL-USD bid and a SOL-EUR-style
    // ask at the same price live in separate books and cannot match.
    #[test]
    fn test_orders_in_different_pairs_do_not_match() {
        let mut engine = new_engine();
        let other = Pair::new(Currency::USD, Currency::SOL); // a different market
        engine.listed_pairs.insert(other);

        engine.ledger.deposit(Currency::SOL, 2, 10);
        submit_pair(
            &mut engine,
            2,
            TEST_PAIR,
            Side::Ask,
            OrderType::Limit,
            100,
            10,
        )
        .unwrap();

        // Same price, other market, funded — must not touch the SOL-USD ask.
        engine.ledger.deposit(Currency::SOL, 1, 1000);
        let res = submit_pair(&mut engine, 1, other, Side::Bid, OrderType::Limit, 100, 10).unwrap();

        assert!(res.trades.is_empty(), "orders crossed between markets");
        assert_eq!(engine.books.len(), 2, "each pair gets its own book");
        // Both still rest, each in its own book.
        assert_eq!(engine.books[&TEST_PAIR].asks.get(&100).unwrap().len(), 1);
        assert_eq!(engine.books[&other].bids.get(&100).unwrap().len(), 1);
    }

    // Order ids come from one engine-wide counter, so they're unique across books.
    #[test]
    fn test_order_ids_unique_across_pairs() {
        let mut engine = new_engine();
        let other = Pair::new(Currency::USD, Currency::SOL);
        engine.listed_pairs.insert(other);

        // An Ask reserves the pair's base: SOL for SOL-USD, USD for USD-SOL.
        engine.ledger.deposit(Currency::SOL, 1, 100);
        engine.ledger.deposit(Currency::USD, 1, 100);
        let a = submit_pair(
            &mut engine,
            1,
            TEST_PAIR,
            Side::Ask,
            OrderType::Limit,
            100,
            1,
        )
        .unwrap();
        let b = submit_pair(&mut engine, 1, other, Side::Ask, OrderType::Limit, 100, 1).unwrap();

        assert_ne!(a.order_id, b.order_id);
    }

    // Cancel carries only an order_id — the engine resolves the market itself,
    // and releases the reserve in that market's currency.
    #[test]
    fn test_cancel_routes_to_correct_pair() {
        let mut engine = new_engine();
        let other = Pair::new(Currency::USD, Currency::SOL);
        engine.listed_pairs.insert(other);

        // Rest a bid in `other`, which reserves its quote (SOL).
        engine.ledger.deposit(Currency::SOL, 1, 1000);
        let res = submit_pair(&mut engine, 1, other, Side::Bid, OrderType::Limit, 10, 5).unwrap();
        assert_eq!(bal(&engine, 1, Currency::SOL), (950, 50));

        assert!(
            engine
                .cancel_order(
                    1,
                    &CancelRequest {
                        order_id: res.order_id
                    }
                )
                .is_some()
        );
        // Released back into SOL (that market's quote), not USD.
        assert_eq!(bal(&engine, 1, Currency::SOL), (1000, 0));
    }

    // A market must price one thing in another.
    #[test]
    fn test_same_base_and_quote_rejected() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 1, 100);
        let bad = Pair::new(Currency::SOL, Currency::SOL);
        let res = submit_pair(&mut engine, 1, bad, Side::Ask, OrderType::Limit, 10, 1);
        assert!(matches!(res, Err(RejectReason::InvalidPair)));
    }

    // A resting limit buy holds its full reserve; cancelling returns it.
    #[test]
    fn test_cancel_releases_reserve() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::USD, 1, 1000);
        let res = submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 1000)); // fully held

        let ok = engine
            .cancel_order(
                1,
                &CancelRequest {
                    order_id: res.order_id,
                },
            )
            .is_some();
        assert!(ok);
        assert_eq!(bal(&engine, 1, Currency::USD), (1000, 0)); // hold released
    }

    // A partial fill reports cumulative filled_qty and PartiallyFilled on the
    // maker; the taker that consumed it is Filled.
    #[test]
    fn test_partial_fill_updates_are_cumulative() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 20);
        let maker = submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 20).unwrap();

        engine.ledger.deposit(Currency::USD, 1, 1000);
        let taker = submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 6).unwrap();

        let m = find_update(&taker, maker.order_id);
        assert_eq!((m.filled_qty, m.remaining_size), (6, 14));
        assert_eq!(m.status, OrderStatus::PartiallyFilled);

        let t = find_update(&taker, taker.order_id);
        assert_eq!((t.filled_qty, t.remaining_size), (6, 0));
        assert_eq!(t.status, OrderStatus::Filled);

        // Hitting the same maker again accumulates rather than restarting.
        let taker2 = submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 4).unwrap();
        let m2 = find_update(&taker2, maker.order_id);
        assert_eq!((m2.filled_qty, m2.remaining_size), (10, 10));
    }

    // A resting limit order that nothing touched needs no update — OrderAccepted
    // already said open/0.
    #[test]
    fn test_untouched_resting_order_emits_no_update() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 1, 10);
        let res = submit(&mut engine, 1, Side::Ask, OrderType::Limit, 100, 10).unwrap();
        assert!(res.updates.is_empty());
    }

    // A market order's unfilled remainder never rests, so it's terminal AND its
    // reserve must come back — otherwise it's locked forever.
    #[test]
    fn test_market_leftover_is_cancelled_and_reserve_released() {
        let mut engine = new_engine();
        // Only 4 of the 10 can fill.
        engine.ledger.deposit(Currency::USD, 2, 1000);
        submit(&mut engine, 2, Side::Bid, OrderType::Limit, 100, 4).unwrap();

        engine.ledger.deposit(Currency::SOL, 1, 10);
        let res = submit(&mut engine, 1, Side::Ask, OrderType::Market, 0, 10).unwrap();

        let t = find_update(&res, res.order_id);
        assert_eq!((t.filled_qty, t.remaining_size), (4, 6));
        assert_eq!(t.status, OrderStatus::Cancelled);

        // 4 SOL sold, 6 back to available — nothing stuck in reserved.
        assert_eq!(bal(&engine, 1, Currency::SOL), (6, 0));
        assert_eq!(bal(&engine, 1, Currency::USD), (400, 0));
    }

    // Cancelling reports the fills it kept plus the released remainder.
    #[test]
    fn test_cancel_reports_partial_fill_then_cancelled() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 1, 10);
        let resting = submit(&mut engine, 1, Side::Ask, OrderType::Limit, 100, 10).unwrap();

        engine.ledger.deposit(Currency::USD, 2, 1000);
        submit(&mut engine, 2, Side::Bid, OrderType::Limit, 100, 3).unwrap();

        let (_, order, _) = engine
            .cancel_order(
                1,
                &CancelRequest {
                    order_id: resting.order_id,
                },
            )
            .unwrap();
        assert_eq!(order.size - order.remaining_size, 3); // kept its 3 fills
        assert_eq!(order.remaining_size, 7);
        assert_eq!(bal(&engine, 1, Currency::SOL), (7, 0)); // remainder released
    }

    // An order whose own price*size would overflow u64 is rejected before
    // anything is reserved or matched — the guard that keeps settle()'s
    // trade.price*trade.qty from silently wrapping around later.
    #[test]
    fn test_order_value_overflow_rejected() {
        let mut engine = new_engine();
        let res = submit(&mut engine, 1, Side::Bid, OrderType::Limit, u64::MAX, 2);
        assert!(matches!(res, Err(RejectReason::InvalidAmount)));
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
    }

    // Withdraw is no longer hardcoded to USD.
    #[test]
    fn test_withdraw_any_currency() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 1, 10);
        assert!(engine.ledger.withdraw(1, Currency::SOL, 4).is_ok());
        assert_eq!(bal(&engine, 1, Currency::SOL), (6, 0));
        // still enforces the balance it's actually checking against
        assert!(matches!(
            engine.ledger.withdraw(1, Currency::SOL, 100),
            Err(RejectReason::InsufficientFunds)
        ));
        assert!(matches!(
            engine.ledger.withdraw(1, Currency::USD, 1),
            Err(RejectReason::InsufficientFunds)
        ));
    }

    // A market buy sweeping one ask level is charged exactly that level's
    // price, with nothing left reserved afterward.
    #[test]
    fn test_market_buy_single_level() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 10).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 1000);

        let res = submit(&mut engine, 1, Side::Bid, OrderType::Market, 0, 10).unwrap();
        assert_eq!(res.trades.iter().map(|t| t.qty).sum::<u64>(), 10);
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
        assert_eq!(bal(&engine, 1, Currency::SOL), (10, 0));
        assert_eq!(bal(&engine, 2, Currency::USD), (1000, 0));
    }

    // A market buy walks every level it needs to fill, not just the first.
    #[test]
    fn test_market_buy_sweeps_multiple_levels() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 5).unwrap();
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 110, 5).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 5 * 100 + 5 * 110);

        submit(&mut engine, 1, Side::Bid, OrderType::Market, 0, 10).unwrap();
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
        assert_eq!(bal(&engine, 1, Currency::SOL), (10, 0));
    }

    // The reserve is the walked cost, not a guess — insufficient funds against
    // that exact cost is still rejected up front, same as a limit order.
    #[test]
    fn test_market_buy_insufficient_funds_rejected() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 10).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 999); // one short of 1000

        let res = submit(&mut engine, 1, Side::Bid, OrderType::Market, 0, 10);
        assert!(matches!(res, Err(RejectReason::InsufficientFunds)));
        assert_eq!(bal(&engine, 1, Currency::USD), (999, 0));
    }

    // Asking for more than the book can supply fills what's there and leaves
    // nothing reserved for the unfillable remainder — unlike a market sell,
    // there was never a reserve taken out for it in the first place.
    #[test]
    fn test_market_buy_partial_fill_no_stuck_reserve() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 5);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 5).unwrap();
        engine.ledger.deposit(Currency::USD, 1, 500);

        let res = submit(&mut engine, 1, Side::Bid, OrderType::Market, 0, 10).unwrap();
        assert_eq!(res.taker_remaining, 5);
        assert_eq!(
            find_update(&res, res.order_id).status,
            OrderStatus::Cancelled
        );
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
        assert_eq!(bal(&engine, 1, Currency::SOL), (5, 0));
    }

    // An empty book fills nothing and moves no funds — not an error.
    #[test]
    fn test_market_buy_empty_book() {
        let mut engine = new_engine();
        let res = submit(&mut engine, 1, Side::Bid, OrderType::Market, 0, 10).unwrap();
        assert!(res.trades.is_empty());
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 0));
    }

    // Conservation: a trade moves money but never creates or destroys it.
    #[test]
    fn test_conservation_across_trade() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::SOL, 2, 10);
        engine.ledger.deposit(Currency::USD, 1, 1000);
        submit(&mut engine, 2, Side::Ask, OrderType::Limit, 100, 10).unwrap();
        submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();

        assert_eq!(total(&engine, Currency::USD), 1000);
        assert_eq!(total(&engine, Currency::SOL), 10);
    }
}

// Sequence numbers and batching: one `events`-stream entry per command, and
// seq assigned inside `apply` so live processing and silent replay agree.
#[cfg(test)]
mod apply_tests {
    use super::*;

    fn deposit_cmd(amount: u64) -> Command {
        deposit_cmd_currency(amount, Currency::USD)
    }

    fn deposit_cmd_currency(amount: u64, currency: Currency) -> Command {
        Command::Deposit(DepositRequest { amount, currency })
    }

    // A command that changes state gets a batch; seq starts at 0 and climbs by
    // exactly 1 per batch-producing command — the numbering M6.4's book
    // projection and clients will reconcile snapshots/deltas against.
    #[test]
    fn test_seq_increments_only_for_batches_with_events() {
        let mut engine = Engine::new();

        let (_, batch1) = apply(&mut engine, 1, 1, deposit_cmd(100));
        let (_, batch2) = apply(&mut engine, 1, 2, deposit_cmd(50));

        assert_eq!(batch1.unwrap().seq, 0);
        assert_eq!(batch2.unwrap().seq, 1);
        assert_eq!(engine.next_seq, 2);
    }

    // A no-op command (here: a deduped retry) must not consume a seq — else
    // published seq numbers would have gaps that look like lost messages to a
    // consumer doing gap detection.
    #[test]
    fn test_noop_command_consumes_no_seq() {
        let mut engine = Engine::new();

        let (_, batch1) = apply(&mut engine, 1, 1, deposit_cmd(100));
        assert_eq!(batch1.unwrap().seq, 0);

        // Same (account_id, client_order_id) -> Duplicate, no events, no seq.
        let (resp, batch2) = apply(&mut engine, 1, 1, deposit_cmd(100));
        assert!(matches!(resp, CommandResponse::Duplicate));
        assert!(batch2.is_none());
        assert_eq!(engine.next_seq, 1); // unchanged by the no-op

        // The next real command picks up right after — no gap.
        let (_, batch3) = apply(&mut engine, 1, 2, deposit_cmd(50));
        assert_eq!(batch3.unwrap().seq, 1);
    }

    // A resting order that never trades still gets one batch: OrderAccepted
    // plus the BalanceChanged from reserving its funds — no Trade, no
    // OrderUpdated, since nothing filled.
    #[test]
    fn test_resting_order_gets_one_batch() {
        let mut engine = Engine::new();
        engine
            .listed_pairs
            .insert(Pair::new(Currency::SOL, Currency::USD));
        apply(&mut engine, 1, 1, deposit_cmd_currency(10, Currency::SOL));

        let place = Command::Place(OrderRequest {
            pair: Pair::new(Currency::SOL, Currency::USD),
            order_type: OrderType::Limit,
            side: Side::Ask,
            price: 100,
            size: 10,
        });
        let (_, batch) = apply(&mut engine, 1, 2, place);
        let events = batch.unwrap().events;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::OrderAccepted { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::BalanceChanged { .. }))
        );
        assert!(!events.iter().any(|e| matches!(e, Event::Trade(_))));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::OrderUpdated { .. }))
        );
    }

    // The dedup set is bounded to DEDUP_WINDOW entries per account (M8): once
    // an account's (client_order_id)th command pushes it past the window, the
    // oldest is evicted and becomes reusable — proving eviction actually
    // happens, not just that recent duplicates are still caught.
    #[test]
    fn test_dedup_window_evicts_oldest_client_order_id() {
        let mut engine = Engine::new();

        for client_order_id in 1..=DEDUP_WINDOW as u64 {
            let (resp, _) = apply(&mut engine, 1, client_order_id, deposit_cmd(1));
            assert!(matches!(resp, CommandResponse::Deposit(_)));
        }
        // Still within the window: a genuine duplicate is still caught.
        let (resp, _) = apply(&mut engine, 1, 1, deposit_cmd(1));
        assert!(matches!(resp, CommandResponse::Duplicate));

        // One more command evicts client_order_id 1 (the oldest).
        let (resp, _) = apply(&mut engine, 1, DEDUP_WINDOW as u64 + 1, deposit_cmd(1));
        assert!(matches!(resp, CommandResponse::Deposit(_)));

        // client_order_id 1 was evicted, so replaying it is treated as new,
        // not as a duplicate.
        let (resp, _) = apply(&mut engine, 1, 1, deposit_cmd(1));
        assert!(matches!(resp, CommandResponse::Deposit(_)));
    }

    // A crossing order's accept, trade(s), and both sides' fill updates all
    // land in ONE batch — this is the property that makes a command's effects
    // atomic downstream (db_writer's transaction, the book projection).
    #[test]
    fn test_crossing_order_batches_accept_trade_and_both_updates_together() {
        let mut engine = Engine::new();
        engine
            .listed_pairs
            .insert(Pair::new(Currency::SOL, Currency::USD));
        apply(&mut engine, 2, 1, deposit_cmd_currency(10, Currency::SOL));
        let ask = Command::Place(OrderRequest {
            pair: Pair::new(Currency::SOL, Currency::USD),
            order_type: OrderType::Limit,
            side: Side::Ask,
            price: 100,
            size: 10,
        });
        apply(&mut engine, 2, 2, ask);

        apply(&mut engine, 1, 3, deposit_cmd(1000));
        let bid = Command::Place(OrderRequest {
            pair: Pair::new(Currency::SOL, Currency::USD),
            order_type: OrderType::Limit,
            side: Side::Bid,
            price: 100,
            size: 10,
        });
        let (_, batch) = apply(&mut engine, 1, 4, bid);
        let events = batch.unwrap().events;

        let has = |pred: &dyn Fn(&Event) -> bool| events.iter().any(pred);
        assert!(has(&|e| matches!(e, Event::OrderAccepted { .. })));
        assert!(has(&|e| matches!(e, Event::Trade(_))));
        // Both maker and taker OrderUpdated for this trade are in THIS batch —
        // not split across two stream entries.
        let updates = events
            .iter()
            .filter(|e| matches!(e, Event::OrderUpdated { .. }))
            .count();
        assert_eq!(updates, 2);
    }
}

#[cfg(test)]
mod pair_whitelist_tests {
    use super::*;

    fn place_cmd(pair: Pair) -> Command {
        Command::Place(OrderRequest {
            pair,
            order_type: OrderType::Limit,
            side: Side::Ask,
            price: 100,
            size: 1,
        })
    }

    // Any pair is unlisted until an explicit ListPair command — the whitelist
    // starts empty, not open-by-default.
    #[test]
    fn test_unlisted_pair_rejected() {
        let mut engine = Engine::new();
        let pair = Pair::new(Currency::SOL, Currency::USD);
        engine.ledger.deposit(Currency::SOL, 1, 1);
        let res = engine.place_order(
            1,
            &OrderRequest {
                pair,
                order_type: OrderType::Limit,
                side: Side::Ask,
                price: 100,
                size: 1,
            },
        );
        assert!(matches!(res, Err(RejectReason::InvalidPair)));
    }

    // ListPair then Place: the pair becomes tradeable.
    #[test]
    fn test_listed_pair_accepted() {
        let mut engine = Engine::new();
        let pair = Pair::new(Currency::SOL, Currency::USD);
        let (resp, _) = apply(&mut engine, 1, 1, Command::ListPair(pair));
        assert!(matches!(resp, CommandResponse::ListPair(Ok(true))));

        engine.ledger.deposit(Currency::SOL, 1, 1);
        let (resp, _) = apply(&mut engine, 1, 2, place_cmd(pair));
        assert!(matches!(resp, CommandResponse::Place(Ok(_))));
    }

    // Listing an already-listed pair is a no-op success, not an error.
    #[test]
    fn test_relisting_is_idempotent() {
        let mut engine = Engine::new();
        let pair = Pair::new(Currency::SOL, Currency::USD);
        apply(&mut engine, 1, 1, Command::ListPair(pair));
        let (resp, _) = apply(&mut engine, 1, 2, Command::ListPair(pair));
        assert!(matches!(resp, CommandResponse::ListPair(Ok(false))));
    }

    // base == quote is rejected even as an admin action, not just at order time.
    #[test]
    fn test_listing_invalid_pair_rejected() {
        let mut engine = Engine::new();
        let bad = Pair::new(Currency::SOL, Currency::SOL);
        let (resp, _) = apply(&mut engine, 1, 1, Command::ListPair(bad));
        assert!(matches!(
            resp,
            CommandResponse::ListPair(Err(RejectReason::InvalidPair))
        ));
    }

    // DelistPair closes a market back down to new orders.
    #[test]
    fn test_delisted_pair_rejected_again() {
        let mut engine = Engine::new();
        let pair = Pair::new(Currency::SOL, Currency::USD);
        apply(&mut engine, 1, 1, Command::ListPair(pair));

        let (resp, _) = apply(&mut engine, 1, 2, Command::DelistPair(pair));
        assert!(matches!(resp, CommandResponse::DelistPair(true)));

        engine.ledger.deposit(Currency::SOL, 1, 1);
        let (resp, _) = apply(&mut engine, 1, 3, place_cmd(pair));
        assert!(matches!(
            resp,
            CommandResponse::Place(Err(RejectReason::InvalidPair))
        ));
    }

    // Delisting a pair that was never listed is reported, not treated as an error.
    #[test]
    fn test_delisting_unlisted_pair_reports_false() {
        let mut engine = Engine::new();
        let pair = Pair::new(Currency::SOL, Currency::USD);
        let (resp, _) = apply(&mut engine, 1, 1, Command::DelistPair(pair));
        assert!(matches!(resp, CommandResponse::DelistPair(false)));
    }
}

#[cfg(test)]
mod guardrail_tests {
    use super::*;
    use crate::test_util::{TEST_PAIR, new_engine};

    fn deposit(
        engine: &mut Engine,
        account: AccountId,
        currency: Currency,
        amount: u64,
    ) -> Result<u64, RejectReason> {
        engine.deposit(account, &DepositRequest { amount, currency })
    }

    fn limit(
        engine: &mut Engine,
        account: AccountId,
        side: Side,
        price: u64,
        size: u64,
    ) -> Result<MatchResponse, RejectReason> {
        engine.place_order(
            account,
            &OrderRequest {
                pair: TEST_PAIR,
                order_type: OrderType::Limit,
                side,
                price,
                size,
            },
        )
    }

    // Two orders that cross, from different accounts, to set last_trade_price.
    fn seed_price(engine: &mut Engine, price: u64) {
        let _ = deposit(engine, 1, Currency::USD, price * 2);
        let _ = deposit(engine, 2, Currency::SOL, 2);
        limit(engine, 2, Side::Ask, price, 1).unwrap();
        limit(engine, 1, Side::Bid, price, 1).unwrap();
        assert_eq!(engine.books[&TEST_PAIR].last_trade_price, Some(price));
    }

    #[test]
    fn test_deposit_up_to_ceiling_allowed() {
        let mut engine = new_engine();
        let ceiling = (engine.limits.deposit_ceiling)(Currency::USD);
        assert_eq!(deposit(&mut engine, 1, Currency::USD, ceiling), Ok(ceiling));
    }

    #[test]
    fn test_deposit_past_ceiling_rejected() {
        let mut engine = new_engine();
        let ceiling = (engine.limits.deposit_ceiling)(Currency::USD);
        assert_eq!(
            deposit(&mut engine, 1, Currency::USD, ceiling + 1),
            Err(RejectReason::DepositLimitExceeded)
        );
    }

    // The ceiling caps the holding, so repeating the request can't get past it.
    #[test]
    fn test_repeated_deposits_cannot_exceed_ceiling() {
        let mut engine = new_engine();
        let ceiling = (engine.limits.deposit_ceiling)(Currency::USD);
        assert!(deposit(&mut engine, 1, Currency::USD, ceiling).is_ok());
        assert_eq!(
            deposit(&mut engine, 1, Currency::USD, 1),
            Err(RejectReason::DepositLimitExceeded)
        );
    }

    // Funds parked in a resting order still count, or you could park and top up.
    #[test]
    fn test_reserved_funds_count_toward_ceiling() {
        let mut engine = new_engine();
        let ceiling = (engine.limits.deposit_ceiling)(Currency::SOL);
        deposit(&mut engine, 1, Currency::SOL, ceiling).unwrap();
        limit(&mut engine, 1, Side::Ask, 100, ceiling).unwrap();
        assert_eq!(engine.ledger.balances[&1][&Currency::SOL].available, 0);
        assert_eq!(
            deposit(&mut engine, 1, Currency::SOL, 1),
            Err(RejectReason::DepositLimitExceeded)
        );
    }

    // Losing money frees room under the ceiling again.
    #[test]
    fn test_can_top_back_up_after_spending() {
        let mut engine = new_engine();
        let ceiling = (engine.limits.deposit_ceiling)(Currency::USD);
        deposit(&mut engine, 1, Currency::USD, ceiling).unwrap();
        engine.ledger.withdraw(1, Currency::USD, 500).unwrap();
        assert_eq!(deposit(&mut engine, 1, Currency::USD, 500), Ok(ceiling));
    }

    // Nothing to anchor a band to until the market has traded once.
    #[test]
    fn test_no_band_before_first_trade() {
        let mut engine = new_engine();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        assert!(limit(&mut engine, 1, Side::Ask, 99_999, 1).is_ok());
    }

    #[test]
    fn test_order_inside_band_accepted() {
        let mut engine = new_engine();
        seed_price(&mut engine, 100);
        deposit(&mut engine, 3, Currency::SOL, 10).unwrap();
        assert!(limit(&mut engine, 3, Side::Ask, 119, 1).is_ok()); // +19%
    }

    #[test]
    fn test_order_outside_band_rejected() {
        let mut engine = new_engine();
        seed_price(&mut engine, 100);
        deposit(&mut engine, 3, Currency::SOL, 10).unwrap();
        // +21%
        assert_eq!(
            limit(&mut engine, 3, Side::Ask, 121, 1).unwrap_err(),
            RejectReason::PriceOutOfBand
        );
    }

    // The cheap way to paint the chart: a lone bid at price 1.
    #[test]
    fn test_lowball_bid_rejected_by_band() {
        let mut engine = new_engine();
        seed_price(&mut engine, 100);
        deposit(&mut engine, 3, Currency::USD, 1_000).unwrap();
        assert_eq!(
            limit(&mut engine, 3, Side::Bid, 1, 1).unwrap_err(),
            RejectReason::PriceOutOfBand
        );
    }

    // Rejected before anything is reserved.
    #[test]
    fn test_band_rejection_reserves_nothing() {
        let mut engine = new_engine();
        seed_price(&mut engine, 100);
        deposit(&mut engine, 3, Currency::USD, 1_000).unwrap();
        assert!(limit(&mut engine, 3, Side::Bid, 1, 1).is_err());
        assert_eq!(engine.ledger.balances[&3][&Currency::USD].reserved, 0);
        assert_eq!(engine.ledger.balances[&3][&Currency::USD].available, 1_000);
    }

    #[test]
    fn test_self_trade_rejected() {
        let mut engine = new_engine();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::USD, 1_000).unwrap();
        limit(&mut engine, 1, Side::Ask, 100, 5).unwrap();
        assert_eq!(
            limit(&mut engine, 1, Side::Bid, 100, 5).unwrap_err(),
            RejectReason::SelfTrade
        );
    }

    // Resting against your own book on the other side is fine — it only matters
    // when the incoming order would actually cross into it.
    #[test]
    fn test_own_resting_order_not_crossed_is_fine() {
        let mut engine = new_engine();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::USD, 1_000).unwrap();
        limit(&mut engine, 1, Side::Ask, 100, 5).unwrap();
        assert!(limit(&mut engine, 1, Side::Bid, 90, 5).is_ok());
    }

    #[test]
    fn test_market_order_self_trade_rejected() {
        let mut engine = new_engine();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::USD, 1_000).unwrap();
        limit(&mut engine, 1, Side::Ask, 100, 5).unwrap();
        let res = engine.place_order(
            1,
            &OrderRequest {
                pair: TEST_PAIR,
                order_type: OrderType::Market,
                side: Side::Bid,
                price: 0,
                size: 1,
            },
        );
        assert_eq!(res.unwrap_err(), RejectReason::SelfTrade);
    }

    // Someone else's order sitting in front is still crossed into, so the scan
    // has to look past the touch rather than stopping at the best level.
    #[test]
    fn test_self_trade_detected_behind_another_account() {
        let mut engine = new_engine();
        deposit(&mut engine, 2, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::USD, 1_000).unwrap();
        limit(&mut engine, 2, Side::Ask, 100, 1).unwrap();
        limit(&mut engine, 1, Side::Ask, 101, 5).unwrap();
        assert_eq!(
            limit(&mut engine, 1, Side::Bid, 101, 4).unwrap_err(),
            RejectReason::SelfTrade
        );
    }

    // ...but an order that fills entirely against the other account never
    // reaches its own resting order, so it stands.
    #[test]
    fn test_no_self_trade_when_filled_before_reaching_own_order() {
        let mut engine = new_engine();
        deposit(&mut engine, 2, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::SOL, 10).unwrap();
        deposit(&mut engine, 1, Currency::USD, 1_000).unwrap();
        limit(&mut engine, 2, Side::Ask, 100, 5).unwrap();
        limit(&mut engine, 1, Side::Ask, 101, 5).unwrap();
        assert!(limit(&mut engine, 1, Side::Bid, 101, 5).is_ok());
    }

    #[test]
    fn test_limits_none_disables_every_guard() {
        let mut engine = new_engine();
        engine.limits = Limits::none();
        deposit(&mut engine, 1, Currency::USD, u64::MAX / 4).unwrap();
        deposit(&mut engine, 1, Currency::SOL, u64::MAX / 4).unwrap();
        limit(&mut engine, 1, Side::Ask, 100, 5).unwrap();
        assert!(limit(&mut engine, 1, Side::Bid, 100, 5).is_ok()); // self-trade ok
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::snapshot;
    use crate::test_util::TEST_PAIR;
    use serde_json::Value;

    /// HashSet iteration order isn't stable, so a snapshot is never byte-equal
    /// run to run. Sort the two set-valued fields before comparing — and only
    /// those, since the book's level queues are FIFO and their order is the
    /// thing worth checking.
    fn state(engine: &Engine) -> Value {
        let mut v = serde_json::to_value(engine).unwrap();
        for path in [&["dedup", "seen"][..], &["listed_pairs"][..]] {
            let mut node = &mut v;
            for key in path {
                node = &mut node[*key];
            }
            if let Some(arr) = node.as_array_mut() {
                arr.sort_by_key(|x| x.to_string());
            }
        }
        v
    }

    // Deposits, a listing, resting orders on both sides, and a crossing order
    // so last_trade_price is set. cid is unique per command or dedup eats it.
    fn script() -> Vec<(AccountId, u64, Command)> {
        let mut cmds = vec![
            (1, 1, Command::ListPair(TEST_PAIR)),
            (
                1,
                2,
                Command::Deposit(DepositRequest {
                    amount: 50_000,
                    currency: Currency::USD,
                }),
            ),
            (
                2,
                3,
                Command::Deposit(DepositRequest {
                    amount: 500,
                    currency: Currency::SOL,
                }),
            ),
        ];
        let mut cid = 4;
        for i in 0..8 {
            cmds.push((
                2,
                cid,
                Command::Place(OrderRequest {
                    pair: TEST_PAIR,
                    order_type: OrderType::Limit,
                    side: Side::Ask,
                    price: 100 + i,
                    size: 3,
                }),
            ));
            cid += 1;
            cmds.push((
                1,
                cid,
                Command::Place(OrderRequest {
                    pair: TEST_PAIR,
                    order_type: OrderType::Limit,
                    side: Side::Bid,
                    price: 90 + i,
                    size: 2,
                }),
            ));
            cid += 1;
        }
        // crosses the resting asks
        cmds.push((
            1,
            cid,
            Command::Place(OrderRequest {
                pair: TEST_PAIR,
                order_type: OrderType::Limit,
                side: Side::Bid,
                price: 104,
                size: 5,
            }),
        ));
        cmds
    }

    fn run(engine: &mut Engine, cmds: &[(AccountId, u64, Command)]) {
        for (account, cid, cmd) in cmds {
            let cmd = serde_json::from_value(serde_json::to_value(cmd).unwrap()).unwrap();
            apply(engine, *account, *cid, cmd);
        }
    }

    fn round_trip(engine: &Engine) -> Engine {
        serde_json::from_str(&serde_json::to_string(engine).unwrap()).unwrap()
    }

    #[test]
    fn test_engine_survives_json_round_trip() {
        let mut engine = Engine::new();
        run(&mut engine, &script());
        assert!(engine.books[&TEST_PAIR].last_trade_price.is_some());
        assert_eq!(state(&engine), state(&round_trip(&engine)));
    }

    // The whole point of the anchor: resuming from a snapshot has to land in
    // exactly the state a replay from entry zero would have produced.
    #[test]
    fn test_resume_from_snapshot_matches_full_replay() {
        let cmds = script();
        let split = cmds.len() / 2;

        let mut full = Engine::new();
        run(&mut full, &cmds);

        let mut resumed = Engine::new();
        run(&mut resumed, &cmds[..split]);
        let mut resumed = round_trip(&resumed);
        run(&mut resumed, &cmds[split..]);

        assert_eq!(state(&full), state(&resumed));
    }

    // A snapshot taken mid-command-batch would otherwise carry dirty entries
    // that re-emit events the log already reported.
    #[test]
    fn test_scratch_fields_are_not_persisted() {
        let mut engine = Engine::new();
        run(&mut engine, &script());
        engine.ledger.dirty.insert((1, Currency::USD));
        engine
            .books
            .get_mut(&TEST_PAIR)
            .unwrap()
            .dirty_levels
            .insert((Side::Bid, 100));

        let restored = round_trip(&engine);
        assert!(restored.ledger.dirty.is_empty());
        assert!(restored.books[&TEST_PAIR].dirty_levels.is_empty());
    }

    #[test]
    fn test_limits_come_from_config_not_snapshot() {
        let mut engine = Engine::new();
        engine.limits = Limits::none();
        run(&mut engine, &script());
        assert!(round_trip(&engine).limits.price_band_bps.is_some());
    }

    #[test]
    fn test_save_load_file_round_trip() {
        let mut engine = Engine::new();
        run(&mut engine, &script());
        let path = std::env::temp_dir().join(format!("engine-snap-{}.json", std::process::id()));

        snapshot::save(&path, "1234-0", &engine).unwrap();
        let loaded = snapshot::load::<Engine>(&path).unwrap();
        assert_eq!(loaded.last_id, "1234-0");
        assert_eq!(state(&engine), state(&loaded.state));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_missing_snapshot_loads_as_none() {
        let path = std::env::temp_dir().join("engine-snap-does-not-exist.json");
        assert!(snapshot::load::<Engine>(&path).is_none());
    }

    // A corrupt snapshot must fall back to full replay, not wedge the boot.
    #[test]
    fn test_corrupt_snapshot_loads_as_none() {
        let path = std::env::temp_dir().join(format!("engine-bad-{}.json", std::process::id()));
        std::fs::write(&path, b"{not json").unwrap();
        assert!(snapshot::load::<Engine>(&path).is_none());
        let _ = std::fs::remove_file(&path);
    }
}
