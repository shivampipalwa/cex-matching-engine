use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::book::{Side, Trade};
use crate::error::RejectReason;
use crate::market::{AccountId, Currency, Pair};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Balance {
    pub available: u64,
    pub reserved: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ledger {
    pub balances: HashMap<AccountId, HashMap<Currency, Balance>>,
    // scratch, always empty at a command boundary — not worth snapshotting
    #[serde(skip)]
    pub dirty: HashSet<(AccountId, Currency)>,
}

impl Ledger {
    // returns available balance
    pub(crate) fn deposit(
        &mut self,
        currency: Currency,
        account_id: AccountId,
        amount: u64,
    ) -> u64 {
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

    /// available + reserved
    pub(crate) fn held(&self, account_id: AccountId, currency: Currency) -> u64 {
        self.balances
            .get(&account_id)
            .and_then(|b| b.get(&currency))
            .map(|b| b.available + b.reserved)
            .unwrap_or(0)
    }

    pub(crate) fn withdraw(
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
    pub(crate) fn reserve(
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

    pub(crate) fn release(
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

    pub(crate) fn settle(&mut self, pair: Pair, trade: &Trade) {
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
