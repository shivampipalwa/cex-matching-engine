use crate::types::{
    AccountId, Balance, CancelRequest, Currency, DepositRequest, Engine, EngineMessage, Ledger,
    MatchResponse, Order, OrderBook, OrderLocation, OrderRequest, OrderType, PlaceOrderResponse,
    RejectReason, Side, Trade, WithdrawRequest,
};
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, VecDeque},
};
use tokio::sync::mpsc;

impl OrderBook {
    // returns filled quantity
    fn add_order(&mut self, order_request: &OrderRequest) -> MatchResponse {
        let mut order = Order {
            id: self.next_order_id,
            account_id: order_request.account_id,
            order_type: order_request.order_type,
            side: order_request.side,
            price: order_request.price,
            size: order_request.size,
            remaining_size: order_request.size,
        };

        // println!("{:?}", self);

        let mut trades = vec![];

        self.next_order_id += 1;
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
                        price: best_price_order.price,
                        qty: trade_qty,
                        maker_id: best_price_order.id,
                        taker_id: order.id,
                        taker_side: order.side,
                        maker_account: best_price_order.account_id,
                        taker_account: order.account_id,
                    };
                    trades.push(trade);
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
                        self.asks
                            .entry(order.price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
                    }
                    Side::Bid => {
                        self.bids
                            .entry(order.price)
                            .or_insert(VecDeque::new())
                            .push_back(order);
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
                        side: order.side,
                        price: order.price,
                    },
                );
            }
        }
        MatchResponse {
            order_id: order.id,
            trades,
        }
    }

    fn cancel_order(&mut self, order_id: u64) -> Option<Order> {
        let Some(OrderLocation { side, price }) = self.order_index.get(&order_id) else {
            return None;
        };

        let Some(order_queue) = (match side {
            Side::Bid => self.bids.get_mut(price),
            Side::Ask => self.asks.get_mut(price),
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
                    self.bids.remove(price);
                }
                Side::Ask => {
                    self.asks.remove(price);
                }
            }
        };
        // remove order_id from the index/hashmap
        self.order_index.remove(&order_id);
        return Some(removed);
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
        balance.available
    }

    fn withdraw(&mut self, account_id: AccountId, amount: u64) -> Result<(), RejectReason> {
        let acc_balances = self.balances.entry(account_id).or_default();
        let usd_balance = acc_balances.entry(Currency::USD).or_insert(Balance {
            available: 0,
            reserved: 0,
        });
        if usd_balance.available < amount {
            return Err(RejectReason::InsufficientFunds);
        }
        usd_balance.available -= amount;
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
        Ok(())
    }

    fn settle(&mut self, currency: Currency, trade: &Trade) {
        match trade.taker_side {
            Side::Bid => {
                // update taker's ledger
                let takers_balance = self.balances.entry(trade.taker_account).or_default();
                let takers_quote_balance = takers_balance.entry(Currency::USD).or_default();
                takers_quote_balance.reserved -= trade.price * trade.qty; //overflow
                let takers_base_balance = takers_balance.entry(currency).or_default();
                takers_base_balance.available += trade.qty;

                //update maker;s ledger
                let makers_balance = self.balances.entry(trade.maker_account).or_default();
                let makers_quote_balance = makers_balance.entry(Currency::USD).or_default();
                makers_quote_balance.available += trade.price * trade.qty; //overflow
                let makers_base_balance = makers_balance.entry(currency).or_default();
                makers_base_balance.reserved -= trade.qty;
            }
            Side::Ask => {
                // update taker's ledger
                let takers_balance = self.balances.entry(trade.taker_account).or_default();
                let takers_quote_balance = takers_balance.entry(Currency::USD).or_default();
                takers_quote_balance.available += trade.price * trade.qty; //overflow
                let takers_base_balance = takers_balance.entry(currency).or_default();
                takers_base_balance.reserved -= trade.qty;

                //update maker;s ledger
                let makers_balance = self.balances.entry(trade.maker_account).or_default();
                let makers_quote_balance = makers_balance.entry(Currency::USD).or_default();
                makers_quote_balance.reserved -= trade.price * trade.qty; //overflow
                let makers_base_balance = makers_balance.entry(currency).or_default();
                makers_base_balance.available += trade.qty;
            }
        }
    }
}

impl Engine {
    fn place_order(&mut self, req: &OrderRequest) -> Result<MatchResponse, RejectReason> {
        // Market buy orders not supported currently, need to implement quote price for it
        if req.order_type == OrderType::Market && req.side == Side::Bid {
            return Err(RejectReason::UnsupportedOrderType);
        }

        // reserve funds
        match req.side {
            Side::Bid => {
                // reserve USD
                self.ledger
                    .reserve(req.account_id, Currency::USD, req.price * req.size)?; // overflow
            }
            Side::Ask => {
                // reserve base
                self.ledger
                    .reserve(req.account_id, req.base_currency, req.size)?;
            }
        }

        // match orders
        let match_result = self.book.add_order(req);

        //settle trades
        for trade in match_result.trades.iter() {
            self.ledger.settle(req.base_currency, trade);
        }

        // buyer surplus form reserved -> available in case of price-improved fill
        if req.side == Side::Bid {
            let surplus = match_result
                .trades
                .iter()
                .map(|t| (req.price - t.price) * t.qty)
                .sum();
            if surplus > 0 {
                self.ledger
                    .release(req.account_id, Currency::USD, surplus)?;
            }
        }

        Ok(match_result)
    }
    fn cancel_order(&mut self, req: &CancelRequest) -> bool {
        let Some(order) = self.book.cancel_order(req.order_id) else {
            return false;
        };
        let (currency, amount) = match order.side {
            Side::Bid => (Currency::USD, order.remaining_size * order.price),
            Side::Ask => (req.base_currency, order.remaining_size),
        };
        let _ = self.ledger.release(order.account_id, currency, amount);
        true
    }
}

pub async fn run_engine(mut engine: Engine, mut receiver: mpsc::Receiver<EngineMessage>) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            EngineMessage::AddOrder {
                order_request,
                response_tx,
            } => {
                let place_order_res = engine.place_order(&order_request);
                let Ok(match_response) = place_order_res else {
                    let e = place_order_res.unwrap_err();
                    if let Err(e) = response_tx.send(Err(e)) {
                        println!(
                            "Oneshot reciever closed for Order: {:?};\nRequest Type: Add Order;\nErr: {:?}",
                            order_request, e
                        )
                    }
                    continue;
                };
                let response = PlaceOrderResponse {
                    order_id: match_response.order_id,
                    filled_qty: match_response.trades.iter().map(|t| t.qty).sum(),
                    total_cost: match_response.trades.iter().map(|t| t.qty * t.price).sum(),
                };
                if let Err(e) = response_tx.send(Ok(response)) {
                    println!(
                        "Oneshot reciever closed for Order: {:?};\nRequest Type: Add Order;\nErr: {:?}",
                        order_request, e
                    )
                };
            }

            EngineMessage::CancelOrder {
                cancel_request,
                response_tx,
            } => {
                let success = engine.cancel_order(&cancel_request);
                if let Err(e) = response_tx.send(success) {
                    println!(
                        "Oneshot receiver closed for CancelRequest: {:?};\nErr: {}\n",
                        cancel_request, e
                    )
                }
            }

            EngineMessage::DepositUsd {
                deposit_request,
                response_tx,
            } => {
                let available_balance = engine.ledger.deposit(
                    deposit_request.currency,
                    deposit_request.account_id,
                    deposit_request.amount,
                );
                if let Err(e) = response_tx.send(available_balance) {
                    println!(
                        "Oneshot receiver closed for DepositRequest: {:?};\nErr: {}\n",
                        deposit_request, e
                    )
                }
            }

            EngineMessage::WithdrawUsd {
                withdraw_request,
                response_tx,
            } => {
                let res = engine
                    .ledger
                    .withdraw(withdraw_request.account_id, withdraw_request.amount);
                if let Err(e) = response_tx.send(res) {
                    println!(
                        "Oneshot reciever closed for WithdrawRequest: {:?};\nErr: {:?}",
                        withdraw_request, e
                    )
                };
            }
        }
    }
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

    pub fn new_book() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            next_order_id: 0,
        }
    }

    /// A fresh `Engine` (empty book + empty ledger) for the ledger/settlement
    /// tests that need the full reserve → match → settle path.
    pub fn new_engine() -> Engine {
        Engine {
            book: new_book(),
            ledger: Ledger {
                balances: HashMap::new(),
            },
        }
    }

    /// Places an order and returns the full `MatchResponse` (order id + trades).
    /// Use this when a test needs to assert on the emitted trades themselves —
    /// execution price, maker/taker ids, taker side.
    pub fn place_full(
        book: &mut OrderBook,
        order_type: OrderType,
        side: Side,
        price: u64,
        size: u64,
    ) -> MatchResponse {
        // Book-only matching tests don't touch the ledger, so a fixed account
        // and base currency are fine here — they're just carried into trades.
        let order_request = OrderRequest {
            account_id: 1,
            base_currency: Currency::SOL,
            order_type,
            side,
            price,
            size,
        };
        book.add_order(&order_request)
    }

    /// Convenience wrapper over `place_full` returning just
    /// `(assigned_order_id, filled_quantity)` for tests that only care about
    /// quantities.
    ///
    /// - `assigned_order_id` is the id the engine stamped on the order — always
    ///   read it from here, never hardcode it or read it before `add_order`.
    /// - `filled_quantity` is how much of the incoming order matched.
    pub fn place(
        book: &mut OrderBook,
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

        assert!(book.cancel_order(bid_id).is_some());

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

        assert!(book.cancel_order(id2).is_some());

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
        assert!(book.cancel_order(u64::MAX).is_none());

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
        assert!(book.cancel_order(ask_id).is_some());

        assert!(book.asks.is_empty());
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
        engine.place_order(&OrderRequest {
            account_id,
            base_currency: Currency::SOL,
            order_type,
            side,
            price,
            size,
        })
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

    // A resting limit buy holds its full reserve; cancelling returns it.
    #[test]
    fn test_cancel_releases_reserve() {
        let mut engine = new_engine();
        engine.ledger.deposit(Currency::USD, 1, 1000);
        let res = submit(&mut engine, 1, Side::Bid, OrderType::Limit, 100, 10).unwrap();
        assert_eq!(bal(&engine, 1, Currency::USD), (0, 1000)); // fully held

        let ok = engine.cancel_order(&CancelRequest {
            order_id: res.order_id,
            base_currency: Currency::SOL,
        });
        assert!(ok);
        assert_eq!(bal(&engine, 1, Currency::USD), (1000, 0)); // hold released
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
