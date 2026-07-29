use crate::types::{
    AccountId, Balance, CancelRequest, Command, CommandEnvelope, CommandResponse, Currency, Dedup,
    Engine, Event, EventBatch, Ledger, MatchResponse, Order, OrderBook, OrderLocation, OrderRequest,
    OrderStatus, OrderType, OrderUpdate, Pair, PlaceOrderResponse, RejectReason, ResponseEnvelope,
    Side, Trade,
};
use redis::{AsyncCommands, aio::MultiplexedConnection, streams::StreamRangeReply};
use std::{cmp::min, collections::VecDeque, error::Error};

use redis::streams::{StreamReadOptions, StreamReadReply};

impl OrderBook {
    // returns filled quantity
    // `order_id` is assigned by the engine (global counter), not the book.
    fn add_order(
        &mut self,
        order_id: u64,
        account_id: AccountId,
        order_request: &OrderRequest,
    ) -> MatchResponse {
        let mut order = Order {
            id: order_id,
            account_id: account_id,
            order_type: order_request.order_type,
            side: order_request.side,
            price: order_request.price,
            size: order_request.size,
            remaining_size: order_request.size,
        };

        let mut trades = vec![];
        let mut updates: Vec<OrderUpdate> = vec![];

        while order.remaining_size > 0 {
            // get the queue for bid/ask orders with best price

            let best_price_queue = match order.side {
                Side::Bid => {
                    let Some((&lowest_ask_price, _)) = self.asks.first_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if lowest_ask_price > order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.asks.get_mut(&lowest_ask_price).unwrap()
                }
                Side::Ask => {
                    let Some((&highest_bid_price, _)) = self.bids.last_key_value() else {
                        break;
                    };
                    // Continue only if price is matched for limit orders
                    if highest_bid_price < order.price && order.order_type == OrderType::Limit {
                        break;
                    }
                    self.bids.get_mut(&highest_bid_price).unwrap()
                }
            };

            while !best_price_queue.is_empty() && order.remaining_size > 0 {
                if let Some(best_price_order) = best_price_queue.front_mut() {
                    let trade_qty = min(order.remaining_size, best_price_order.remaining_size);
                    if order.remaining_size <= best_price_order.remaining_size {
                        best_price_order.remaining_size -= order.remaining_size;
                        order.remaining_size = 0;
                    } else {
                        order.remaining_size -= best_price_order.remaining_size;
                        best_price_order.remaining_size = 0;
                    }
                    let trade = Trade {
                        pair: order_request.pair,
                        price: best_price_order.price,
                        qty: trade_qty,
                        maker_id: best_price_order.id,
                        taker_id: order.id,
                        taker_side: order.side,
                        maker_account: best_price_order.account_id,
                        taker_account: order.account_id,
                    };
                    trades.push(trade);

                    // The maker's level changed size — mark it before it might
                    // get popped below. `take_dirty_levels` recomputes the
                    // CURRENT aggregate later, so this is correct whether the
                    // level survives, shrinks, or empties out.
                    self.dirty_levels
                        .insert((best_price_order.side, best_price_order.price));

                    // Capture the maker's post-fill state now — a filled maker is
                    // about to be dropped from the book.
                    updates.push(OrderUpdate {
                        order_id: best_price_order.id,
                        account_id: best_price_order.account_id,
                        filled_qty: best_price_order.size - best_price_order.remaining_size,
                        remaining_size: best_price_order.remaining_size,
                        status: if best_price_order.remaining_size == 0 {
                            OrderStatus::Filled
                        } else {
                            OrderStatus::PartiallyFilled
                        },
                    });

                    // remove order from queue if it is filled
                    if best_price_order.remaining_size == 0 {
                        self.order_index.remove(&best_price_order.id);
                        best_price_queue.pop_front();
                    }
                }
            }
            // remove first entry in map if its empty
            if best_price_queue.is_empty() {
                match order.side {
                    Side::Bid => {
                        self.asks.pop_first();
                    }
                    Side::Ask => {
                        self.bids.pop_last();
                    }
                }
            }
        }
        // handle when remaining size is more than 0
        if order.remaining_size > 0 {
            match order.order_type {
                OrderType::Limit => match order.side {
                    Side::Ask => {
                        let price = order.price; // read before `order` moves below
                        self.asks
                            .entry(price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
                        self.dirty_levels.insert((Side::Ask, price));
                    }
                    Side::Bid => {
                        let price = order.price;
                        self.bids
                            .entry(price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
                        self.dirty_levels.insert((Side::Bid, price));
                    }
                },
                OrderType::Market => {}
            }
        }
        if let OrderType::Limit = order.order_type {
            if order.remaining_size > 0 {
                self.order_index.insert(
                    order.id,
                    OrderLocation {
                        owner: account_id,
                        side: order.side,
                        price: order.price,
                    },
                );
            }
        }
        // Taker's own state. A market order's unfilled remainder never rests, so
        // it's terminal -> Cancelled. No update when a limit order simply rests
        // untouched: OrderAccepted already reported open/0.
        let taker_filled = order.size - order.remaining_size;
        let taker_status = if order.remaining_size == 0 {
            Some(OrderStatus::Filled)
        } else if order.order_type == OrderType::Market {
            Some(OrderStatus::Cancelled)
        } else if taker_filled > 0 {
            Some(OrderStatus::PartiallyFilled)
        } else {
            None
        };
        if let Some(status) = taker_status {
            updates.push(OrderUpdate {
                order_id: order.id,
                account_id: order.account_id,
                filled_qty: taker_filled,
                remaining_size: order.remaining_size,
                status,
            });
        }

        MatchResponse {
            order_id: order.id,
            trades,
            updates,
            taker_remaining: order.remaining_size,
            book_deltas: self.take_dirty_levels(),
        }
    }

    // Cost to fill up to `size` off the ask side at current levels, without
    // mutating anything. Stops early if asks run out before `size` does — the
    // caller treats that as "reserve for what's actually fillable."
    fn market_buy_cost(&self, size: u64) -> Result<u64, RejectReason> {
        let mut remaining = size;
        let mut cost: u64 = 0;
        for (&price, level) in self.asks.iter() {
            if remaining == 0 {
                break;
            }
            let level_qty: u64 = level.iter().map(|o| o.remaining_size).sum();
            let take = min(remaining, level_qty);
            let level_cost = price
                .checked_mul(take)
                .ok_or(RejectReason::InvalidAmount)?;
            cost = cost
                .checked_add(level_cost)
                .ok_or(RejectReason::InvalidAmount)?;
            remaining -= take;
        }
        Ok(cost)
    }

    /// Drain the dirty-level set into `(side, price, new_qty)` triples,
    /// recomputing each level's CURRENT aggregate — correct regardless of
    /// whether the level survived, shrank, or emptied out since being marked.
    fn take_dirty_levels(&mut self) -> Vec<(Side, u64, u64)> {
        let dirty: Vec<(Side, u64)> = self.dirty_levels.drain().collect();
        dirty
            .into_iter()
            .map(|(side, price)| {
                let level = match side {
                    Side::Bid => self.bids.get(&price),
                    Side::Ask => self.asks.get(&price),
                };
                let qty = level
                    .map(|q| q.iter().map(|o| o.remaining_size).sum())
                    .unwrap_or(0);
                (side, price, qty)
            })
            .collect()
    }

    // Returns the removed order plus the (0 or 1) book deltas its removal caused.
    fn cancel_order(
        &mut self,
        account_id: AccountId,
        order_id: u64,
    ) -> Option<(Order, Vec<(Side, u64, u64)>)> {
        let Some(OrderLocation { owner, side, price }) = self.order_index.get(&order_id) else {
            return None;
        };

        if account_id != *owner {
            return None;
        }
        // Copy out before further mutable borrows of self.bids/self.asks.
        let side = *side;
        let price = *price;

        let Some(order_queue) = (match side {
            Side::Bid => self.bids.get_mut(&price),
            Side::Ask => self.asks.get_mut(&price),
        }) else {
            return None;
        };

        let Some(index) = order_queue.iter().position(|&order| order.id == order_id) else {
            return None;
        };
        let removed = order_queue.remove(index)?;
        if order_queue.is_empty() {
            match side {
                Side::Bid => {
                    self.bids.remove(&price);
                }
                Side::Ask => {
                    self.asks.remove(&price);
                }
            }
        };
        // remove order_id from the index/hashmap
        self.order_index.remove(&order_id);
        self.dirty_levels.insert((side, price));
        Some((removed, self.take_dirty_levels()))
    }
}

impl Ledger {
    // returns available balance
    fn deposit(&mut self, currency: Currency, account_id: AccountId, amount: u64) -> u64 {
        let balance = self
            .balances
            .entry(account_id)
            .or_default()
            .entry(currency)
            .or_default();
        balance.available += amount;
        self.dirty.insert((account_id, currency));
        balance.available
    }

    fn withdraw(
        &mut self,
        account_id: AccountId,
        currency: Currency,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let acc_balances = self.balances.entry(account_id).or_default();
        let balance = acc_balances.entry(currency).or_insert(Balance {
            available: 0,
            reserved: 0,
        });
        if balance.available < amount {
            return Err(RejectReason::InsufficientFunds);
        }
        balance.available -= amount;
        self.dirty.insert((account_id, currency));
        Ok(())
    }

    // Ok(reserved amount before reserving)
    fn reserve(
        &mut self,
        account_id: AccountId,
        currency: Currency,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let acc_balances = self.balances.entry(account_id).or_default();
        let balance = acc_balances.entry(currency).or_insert(Balance {
            available: 0,
            reserved: 0,
        });
        if balance.available < amount {
            return Err(RejectReason::InsufficientFunds);
        }
        balance.available -= amount;
        balance.reserved += amount;
        self.dirty.insert((account_id, currency));
        Ok(())
    }

    fn release(
        &mut self,
        account_id: AccountId,
        currency: Currency,
        amount: u64,
    ) -> Result<(), RejectReason> {
        let acc_balances = self.balances.entry(account_id).or_default();
        let balance = acc_balances.entry(currency).or_insert(Balance {
            available: 0,
            reserved: 0,
        });
        if balance.reserved < amount {
            println!("released amount can not be more than reserved amount");
            return Err(RejectReason::InvalidAmount);
        }
        balance.reserved -= amount;
        balance.available += amount;
        self.dirty.insert((account_id, currency));
        Ok(())
    }

    fn settle(&mut self, pair: Pair, trade: &Trade) {
        let (currency, quote) = (pair.base, pair.quote);
        // place_order() rejects any order whose own price*size overflows, and a
        // trade's price/qty are each bounded by the crossing order's price/size —
        // so this can only fire if that guard was bypassed, i.e. a real bug.
        let cost = trade
            .price
            .checked_mul(trade.qty)
            .expect("trade cost overflow should have been rejected at order placement");
        match trade.taker_side {
            Side::Bid => {
                let takers_balance = self.balances.entry(trade.taker_account).or_default();
                takers_balance.entry(quote).or_default().reserved -= cost;
                takers_balance.entry(currency).or_default().available += trade.qty;

                let makers_balance = self.balances.entry(trade.maker_account).or_default();
                makers_balance.entry(quote).or_default().available += cost;
                makers_balance.entry(currency).or_default().reserved -= trade.qty;
            }
            Side::Ask => {
                let takers_balance = self.balances.entry(trade.taker_account).or_default();
                takers_balance.entry(quote).or_default().available += cost;
                takers_balance.entry(currency).or_default().reserved -= trade.qty;

                let makers_balance = self.balances.entry(trade.maker_account).or_default();
                makers_balance.entry(quote).or_default().reserved -= cost;
                makers_balance.entry(currency).or_default().available += trade.qty;
            }
        }
        self.dirty.insert((trade.taker_account, quote));
        self.dirty.insert((trade.taker_account, currency));
        self.dirty.insert((trade.maker_account, quote));
        self.dirty.insert((trade.maker_account, currency));
    }

    pub fn take_dirty(&mut self) -> Vec<(AccountId, Currency)> {
        self.dirty.drain().collect()
    }
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

impl Engine {
    fn place_order(
        &mut self,
        account_id: AccountId,
        req: &OrderRequest,
    ) -> Result<MatchResponse, RejectReason> {
        if !req.pair.is_valid() || !self.listed_pairs.contains(&req.pair) {
            return Err(RejectReason::InvalidPair);
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
                self.ledger.reserve(account_id, req.pair.quote, quote_amount)?;
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
        if req.order_type == OrderType::Market && req.side == Side::Ask && match_result.taker_remaining > 0 {
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
            let available_balance =
                engine
                    .ledger
                    .deposit(deposit_request.currency, account_id, deposit_request.amount);
            CommandResponse::Deposit(available_balance)
        }
        Command::Withdraw(withdraw_request) => {
            let res = engine
                .ledger
                .withdraw(account_id, withdraw_request.currency, withdraw_request.amount);
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

pub async fn recover(
    engine: &mut Engine,
    conn: &mut MultiplexedConnection,
) -> Result<(), Box<dyn Error>> {
    println!("Starting Recovery");
    let reply: StreamRangeReply = conn.xrange("commands", "-", "+").await?;
    let mut last_id = None;
    for id in reply.ids {
        // get command data
        let Some(data): Option<String> = id.get("data") else {
            println!("Empty data");
            continue;
        };

        // deserialize data into CommandEnvelope (correlation_id is irrelevant
        // during replay — we emit nothing, so there's no one to reply to)
        let Ok(CommandEnvelope {
            command,
            account_id,
            client_order_id,
            ..
        }) = serde_json::from_str(&data).inspect_err(|err| {
            println!("Could not deserialize CommandEnvelope; Err:\n{}", err);
        })
        else {
            continue;
        };

        // dispatch command and discard response
        let _ = apply(engine, account_id, client_order_id, command);
        last_id = Some(id);
    }
    println!("Recovery complete");

    // align the group cursor to the replay boundary
    if let Some(stream_id) = last_id {
        let _: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("SETID")
            .arg("commands")
            .arg("engine-group")
            .arg(stream_id.id)
            .query_async(conn)
            .await;
    }

    Ok(())
}

pub async fn run_engine(
    mut engine: Engine,
    mut read_conn: MultiplexedConnection,
    mut pub_conn: MultiplexedConnection,
) -> Result<(), Box<dyn Error>> {
    recover(&mut engine, &mut pub_conn).await?;
    let opts = StreamReadOptions::default()
        .group("engine-group", "engine-1")
        .block(5000)
        .count(10);
    loop {
        let reply: StreamReadReply = read_conn
            .xread_options(&["commands"], &[">"], &opts)
            .await?;

        for key in reply.keys {
            for entry in key.ids {
                let entry_id = entry.id.clone();

                // get command data
                let Some(data): Option<String> = entry.get("data") else {
                    println!("Empty data");
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };
                // deserialize to CommandEnvelope
                let Ok(CommandEnvelope {
                    correlation_id,
                    account_id,
                    client_order_id,
                    command,
                }) = serde_json::from_str(&data).inspect_err(|err| {
                    println!("Could not deserialize CommandEnvelope; Err:\n{}", err);
                })
                else {
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };

                // dispatch command (dedup happens inside `apply`)
                let (response, batch) = apply(&mut engine, account_id, client_order_id, command);

                // emit the whole command's events as ONE stream entry. This is
                // what makes a command's effects atomic for downstream
                // consumers (db_writer's transaction, the book projection): one
                // entry = one seq = one unit they can apply-or-not as a whole.
                if let Some(batch) = batch {
                    let Ok(batch_json) = serde_json::to_string(&batch).inspect_err(|err| {
                        println!("Invalid event batch: {:?}\nErr: {}", batch, err);
                    }) else {
                        // Unreachable in practice, but don't let a serialize
                        // bug wedge the command loop.
                        let _: i64 = pub_conn
                            .xack("commands", "engine-group", &[entry_id])
                            .await?;
                        continue;
                    };
                    let _: String = pub_conn
                        .xadd("events", "*", &[("data".to_string(), batch_json)])
                        .await?;
                }

                // serialize resp
                let resp_envelope = ResponseEnvelope {
                    correlation_id,
                    response,
                };

                let Ok(resp_json) = serde_json::to_string(&resp_envelope).inspect_err(|err| {
                    println!("Could not serialize Response; Err:\n{}", err);
                }) else {
                    let _: i64 = pub_conn
                        .xack("commands", "engine-group", &[entry_id])
                        .await?;
                    continue;
                };

                // publish response
                let channel = format!("results");
                let _: Result<i64, redis::RedisError> = pub_conn
                    .publish(channel, &resp_json)
                    .await
                    .inspect_err(|err| {
                        println!(
                            "Could not publish response;\nresp_json: {}\nErr: {}",
                            resp_json, err
                        );
                    });

                // ACK command
                let _: i64 = pub_conn
                    .xack("commands", "engine-group", &[entry_id])
                    .await?;
            }
        }
    }
    // Ok(())
}

// ---------------------------------------------------------------------------
// Test helpers, shared by every test module below.
//
// The golden rule here: NO test calls `add_order` / builds an `Order` directly.
// Everything routes through `place(...)`. That way, when `add_order`'s signature
// changes in later milestones (M1 makes it emit trades instead of a filled
// quantity), this ONE helper is the only thing that needs updating — not the
// dozens of call sites in the tests.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod test_util {
    use super::*;
    use std::ops::{Deref, DerefMut};

    pub const TEST_PAIR: Pair = Pair {
        base: Currency::SOL,
        quote: Currency::USD,
    };

    /// A book plus the id counter the engine normally owns, so book-only tests
    /// can keep calling `place(...)`. Derefs to `OrderBook`.
    pub struct TestBook {
        pub book: OrderBook,
        pub next_id: u64,
    }

    impl Deref for TestBook {
        type Target = OrderBook;
        fn deref(&self) -> &OrderBook {
            &self.book
        }
    }
    impl DerefMut for TestBook {
        fn deref_mut(&mut self) -> &mut OrderBook {
            &mut self.book
        }
    }

    pub fn new_book() -> TestBook {
        TestBook {
            book: OrderBook::new(),
            next_id: 0,
        }
    }

    /// A fresh `Engine` (no books + empty ledger) for the ledger/settlement
    /// tests that need the full reserve → match → settle path.
    pub fn new_engine() -> Engine {
        let mut engine = Engine::new();
        engine.listed_pairs.insert(TEST_PAIR);
        engine
    }

    /// Places an order and returns the full `MatchResponse` (order id + trades).
    /// Use this when a test needs to assert on the emitted trades themselves —
    /// execution price, maker/taker ids, taker side.
    pub fn place_full(
        book: &mut TestBook,
        order_type: OrderType,
        side: Side,
        price: u64,
        size: u64,
    ) -> MatchResponse {
        // Book-only matching tests don't touch the ledger, so a fixed account
        // and pair are fine here — they're just carried into trades.
        let order_request = OrderRequest {
            pair: TEST_PAIR,
            order_type,
            side,
            price,
            size,
        };
        let id = book.next_id;
        book.next_id += 1;
        book.book.add_order(id, 1, &order_request)
    }

    /// Convenience wrapper over `place_full` returning just
    /// `(assigned_order_id, filled_quantity)` for tests that only care about
    /// quantities.
    ///
    /// - `assigned_order_id` is the id the engine stamped on the order — always
    ///   read it from here, never hardcode it or read it before `add_order`.
    /// - `filled_quantity` is how much of the incoming order matched.
    pub fn place(
        book: &mut TestBook,
        order_type: OrderType,
        side: Side,
        price: u64,
        size: u64,
    ) -> (u64, u64) {
        let result = place_full(book, order_type, side, price, size);
        let filled = result.trades.iter().map(|t| t.qty).sum();
        (result.order_id, filled)
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;

    // --- TEST 1: Basic Insertion (No Match) ---
    // Limit orders route to the correct side when the opposite side is empty.
    #[test]
    fn test_insert_limit_orders_no_match() {
        let mut book = new_book();

        let (bid_id, filled_bid) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        let (_ask_id, filled_ask) = place(&mut book, OrderType::Limit, Side::Ask, 110, 15);

        // Nothing crosses the spread, so nothing fills.
        assert_eq!(filled_bid, 0);
        assert_eq!(filled_ask, 0);

        // Both orders rest on their own side of the book.
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);
        assert_eq!(book.asks.get(&110).unwrap().len(), 1);

        // The resting order carries exactly the id the engine handed back to us.
        assert_eq!(book.bids.get(&100).unwrap()[0].id, bid_id);
    }

    // --- TEST 2: Exact Match & Price Level Cleanup ---
    // A perfect match fills both orders and removes the now-empty price level.
    #[test]
    fn test_exact_match_and_cleanup() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        // Incoming bid is fully filled.
        assert_eq!(filled, 10);

        // Empty price level must be removed from the BTreeMap (else it's a leak).
        assert!(
            book.asks.get(&100).is_none(),
            "Empty price levels must be removed from the BTreeMap"
        );
    }

    // --- TEST 3: Partial Fill (Incoming Order is Smaller) ---
    // The resting order stays at the front of the queue with a reduced size.
    #[test]
    fn test_partial_fill_incoming_smaller() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 20);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 5);

        // Incoming bid fully filled (5 of 5).
        assert_eq!(filled, 5);

        // Resting ask remains with 15 outstanding.
        assert_eq!(book.asks.get(&100).unwrap()[0].remaining_size, 15);
    }

    // --- TEST 4: Partial Fill (Incoming Order is Larger) ---
    // Incoming order eats the resting order; the remainder settles into the book.
    #[test]
    fn test_partial_fill_incoming_larger() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 25);

        // Only 10 available to match.
        assert_eq!(filled, 10);

        // Ask price level is consumed and gone.
        assert!(book.asks.get(&100).is_none());

        // Bid remainder (15) rests in the book.
        assert_eq!(book.bids.get(&100).unwrap()[0].remaining_size, 15);
    }

    // --- TEST 5: Time Priority (FIFO) ---
    // Orders at the same price match in arrival order.
    #[test]
    fn test_time_priority_fifo() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);

        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 15);
        assert_eq!(filled, 15);

        let ask_queue = book.asks.get(&100).unwrap();

        // ask1 fully consumed; ask2 partially (5 left) at the front; ask3 untouched.
        assert_eq!(ask_queue.len(), 2);
        assert_eq!(ask_queue[0].remaining_size, 5);
        assert_eq!(ask_queue[1].remaining_size, 10);
    }

    // --- TEST 6: Market Order Sweeps Multiple Price Levels ---
    #[test]
    fn test_market_order_sweep() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 105, 10);
        place(&mut book, OrderType::Limit, Side::Ask, 110, 10);

        // Aggressive market buy for 25 (price is irrelevant for market orders).
        let (_id, filled) = place(&mut book, OrderType::Market, Side::Bid, 0, 25);
        assert_eq!(filled, 25);

        // First two levels destroyed.
        assert!(book.asks.get(&100).is_none());
        assert!(book.asks.get(&105).is_none());

        // Third level has 5 remaining.
        assert_eq!(book.asks.get(&110).unwrap()[0].remaining_size, 5);
    }

    // --- TEST 7: Market Order Liquidity Exhaustion ---
    // A market order larger than the whole book fills what it can and vanishes.
    #[test]
    fn test_market_order_exhausts_book() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 100, 10);

        // Market buy for 50, but only 10 exist.
        let (_id, filled) = place(&mut book, OrderType::Market, Side::Bid, 0, 50);
        assert_eq!(filled, 10);

        // Book is empty and, crucially, the market order never rested.
        assert!(book.asks.is_empty());
        assert!(
            book.bids.is_empty(),
            "Market orders must not be placed in the BTreeMap"
        );
    }
}

#[cfg(test)]
mod cancel_tests {
    use super::test_util::*;
    use super::*;

    // --- TEST 1: Basic Cancellation & Cleanup ---
    #[test]
    fn test_cancel_single_order() {
        let mut book = new_book();

        let (bid_id, _) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);

        assert!(book.cancel_order(1, bid_id).is_some());

        // Empty price level must be removed after cancellation.
        assert!(
            book.bids.get(&100).is_none(),
            "Empty price levels must be removed after cancellation"
        );
        assert!(book.bids.is_empty());
    }

    // --- TEST 2: Cancel from the Middle of a Queue ---
    // Cancelling one order preserves FIFO order of the rest.
    #[test]
    fn test_cancel_middle_of_queue() {
        let mut book = new_book();

        let (id1, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 10);
        let (id2, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 15);
        let (id3, _) = place(&mut book, OrderType::Limit, Side::Ask, 200, 20);

        assert_eq!(book.asks.get(&200).unwrap().len(), 3);

        assert!(book.cancel_order(1, id2).is_some());

        let queue = book.asks.get(&200).unwrap();
        assert_eq!(queue.len(), 2);

        // Time priority preserved for the survivors.
        assert_eq!(queue[0].id, id1);
        assert_eq!(queue[1].id, id3);
    }

    // --- TEST 3: Cancel a Non-Existent Order ---
    // Must return false and leave the book untouched.
    #[test]
    fn test_cancel_non_existent_order() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Ask, 150, 10);

        // An id the engine could never have assigned (ids start at 0 and climb).
        assert!(book.cancel_order(1, u64::MAX).is_none());

        assert_eq!(book.asks.get(&150).unwrap().len(), 1);
        assert_eq!(book.asks.get(&150).unwrap()[0].size, 10);
    }

    // --- TEST 4: Cancel a Partially Filled Order ---
    #[test]
    fn test_cancel_partially_filled_order() {
        let mut book = new_book();

        let (ask_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 20);

        // Partially fill the resting ask.
        let (_bid_id, filled) = place(&mut book, OrderType::Limit, Side::Bid, 100, 5);
        assert_eq!(filled, 5);
        assert_eq!(book.asks.get(&100).unwrap()[0].remaining_size, 15);

        // Cancel the remainder.
        assert!(book.cancel_order(1, ask_id).is_some());

        assert!(book.asks.is_empty());
        assert!(book.bids.is_empty());
    }

    // --- TEST 5: Cancel by a Non-Owner is Rejected ---
    // Authorization: only the account that placed an order may cancel it. A
    // stranger's cancel must be a no-op that leaves the order resting.
    #[test]
    fn test_cancel_by_non_owner_is_rejected() {
        let mut book = new_book();

        // `place` always places as account 1 (see `place_full`).
        let (bid_id, _) = place(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        // Account 2 attempts to cancel account 1's order — rejected, untouched.
        assert!(book.cancel_order(2, bid_id).is_none());
        assert_eq!(book.bids.get(&100).unwrap().len(), 1);

        // The rejection didn't corrupt state: the real owner can still cancel.
        assert!(book.cancel_order(1, bid_id).is_some());
        assert!(book.bids.is_empty());
    }
}

#[cfg(test)]
mod trade_tests {
    use super::test_util::*;
    use super::*;

    // A single clean match: one trade at the maker's price, correct ids and side.
    #[test]
    fn test_trade_details_single_match() {
        let mut book = new_book();

        let (maker_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let result = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        assert_eq!(result.trades.len(), 1);
        let t = &result.trades[0];
        assert_eq!(t.qty, 10);
        assert_eq!(t.price, 100);
        assert_eq!(t.maker_id, maker_id);
        assert_eq!(t.taker_id, result.order_id);
        assert_eq!(t.taker_side, Side::Bid);
    }

    // THE money invariant: a taker that crosses the spread executes at the
    // MAKER'S resting price, never its own limit price.
    #[test]
    fn test_execution_price_is_maker_price() {
        let mut book = new_book();

        // Maker rests at 95.
        let (maker_id, _) = place(&mut book, OrderType::Limit, Side::Ask, 95, 10);
        // Taker is willing to pay up to 100 but must fill at the maker's 95.
        let result = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);

        assert_eq!(result.trades.len(), 1);
        assert_eq!(
            result.trades[0].price, 95,
            "execution price must be the maker's resting price, not the taker's limit"
        );
        assert_eq!(result.trades[0].maker_id, maker_id);
    }

    // A market order sweeping three levels: three trades, each at its maker's price.
    #[test]
    fn test_market_sweep_trade_prices() {
        let mut book = new_book();

        let (m1, _) = place(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (m2, _) = place(&mut book, OrderType::Limit, Side::Ask, 105, 10);
        let (m3, _) = place(&mut book, OrderType::Limit, Side::Ask, 110, 10);

        let result = place_full(&mut book, OrderType::Market, Side::Bid, 0, 25);

        assert_eq!(result.trades.len(), 3);

        // Prices follow the makers as the sweep goes deeper.
        assert_eq!(result.trades[0].price, 100);
        assert_eq!(result.trades[1].price, 105);
        assert_eq!(result.trades[2].price, 110);

        // Quantities: two full levels then a partial.
        assert_eq!(result.trades[0].qty, 10);
        assert_eq!(result.trades[1].qty, 10);
        assert_eq!(result.trades[2].qty, 5);

        // Makers appear in sweep order.
        assert_eq!(result.trades[0].maker_id, m1);
        assert_eq!(result.trades[1].maker_id, m2);
        assert_eq!(result.trades[2].maker_id, m3);

        // The total_cost the receipt would report.
        let total_cost: u64 = result.trades.iter().map(|t| t.qty * t.price).sum();
        assert_eq!(total_cost, 10 * 100 + 10 * 105 + 5 * 110); // 2625
    }

    // taker_side reflects the aggressor, so the trade tape can color the print.
    #[test]
    fn test_taker_side_reflects_aggressor() {
        let mut book = new_book();

        place(&mut book, OrderType::Limit, Side::Bid, 100, 10); // resting buy
        let result = place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // incoming sell

        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].taker_side, Side::Ask);
        assert_eq!(result.trades[0].price, 100);
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::test_util::*;
    use super::*;

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
        assert_eq!(find_update(&res, res.order_id).status, OrderStatus::Cancelled);
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
    use crate::types::DepositRequest;

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
        assert!(events.iter().any(|e| matches!(e, Event::OrderAccepted { .. })));
        assert!(events.iter().any(|e| matches!(e, Event::BalanceChanged { .. })));
        assert!(!events.iter().any(|e| matches!(e, Event::Trade(_))));
        assert!(!events.iter().any(|e| matches!(e, Event::OrderUpdated { .. })));
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
        let res = engine.place_order(1, &OrderRequest {
            pair,
            order_type: OrderType::Limit,
            side: Side::Ask,
            price: 100,
            size: 1,
        });
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

// Book deltas: the (side, price, new_qty) triples that feed the M6.4 book
// projection. The subtlety worth pinning down is that `dirty_levels` is a
// HashSet — so a level touched twice in one command (e.g. a sweep across two
// orders resting at the same price) still emits exactly ONE delta, and it
// must carry the aggregate qty as of the END of the command, not a snapshot
// from partway through matching.
#[cfg(test)]
mod book_delta_tests {
    use super::test_util::*;
    use super::*;

    // A limit order with nothing to match against just rests — one delta, at
    // its own level, qty equal to its full size.
    #[test]
    fn test_resting_order_produces_one_book_delta() {
        let mut book = new_book();
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(res.book_deltas, vec![(Side::Bid, 100, 10)]);
    }

    // Sweeping two resting orders at the SAME price level must still produce
    // ONE delta for that level (not two), carrying the final aggregate —
    // proof that dirty_levels dedups and take_dirty_levels reads current state.
    #[test]
    fn test_sweep_across_same_level_dedups_to_one_delta() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // ask #1
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // ask #2

        // Bid for 6: fully consumes ask #1 (5), takes 1 from ask #2 (leaves 4).
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 6);

        // Fully filled — the bid itself never rests, so no Bid-side delta.
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 4)]);
    }

    // A partial fill leaves the maker's level non-zero — the delta must
    // report that remainder, not zero and not the pre-fill size.
    #[test]
    fn test_partial_fill_reports_remaining_aggregate() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 4);
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 6)]);
    }

    // A full fill empties the level: the delta's qty is 0 even though the
    // BTreeMap entry itself is removed (there's nothing left to sum).
    #[test]
    fn test_full_fill_reports_zero_qty() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 100, 10);
        assert_eq!(res.book_deltas, vec![(Side::Ask, 100, 0)]);
        assert!(book.asks.get(&100).is_none());
    }

    // Cancelling the only order at a level reports qty 0 (level gone).
    #[test]
    fn test_cancel_last_order_at_level_reports_zero() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        let (_, deltas) = book.cancel_order(1, 0).unwrap();
        assert_eq!(deltas, vec![(Side::Ask, 100, 0)]);
    }

    // Cancelling one of several orders at a level reports the survivors'
    // aggregate, not zero.
    #[test]
    fn test_cancel_one_of_several_reports_remaining_aggregate() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // id 0
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 5); // id 1
        let (_, deltas) = book.cancel_order(1, 0).unwrap();
        assert_eq!(deltas, vec![(Side::Ask, 100, 5)]);
    }

    // A resting order untouched by a later command emits no delta for THAT
    // command — dirty_levels is drained per-command, not accumulated forever.
    #[test]
    fn test_unrelated_command_emits_no_stale_delta() {
        let mut book = new_book();
        place_full(&mut book, OrderType::Limit, Side::Ask, 100, 10);
        // A second, unrelated resting order at a different level.
        let res = place_full(&mut book, OrderType::Limit, Side::Bid, 50, 3);
        assert_eq!(res.book_deltas, vec![(Side::Bid, 50, 3)]); // not the ask too
    }
}
