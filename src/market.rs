use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

pub type AccountId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    USD,
    SOL,
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Currency::USD => write!(f, "USD"),
            Currency::SOL => write!(f, "SOL"),
        }
    }
}

impl FromStr for Currency {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "USD" => Ok(Currency::USD),
            "SOL" => Ok(Currency::SOL),
            other => Err(format!("unknown currency {other:?}")),
        }
    }
}

/// A trading pair (market). `base` is what's bought/sold, `quote` is what it's
/// priced in. symbol string - "SOL-USD".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Pair {
    pub base: Currency,
    pub quote: Currency,
}

impl Pair {
    pub fn new(base: Currency, quote: Currency) -> Self {
        Pair { base, quote }
    }
    // A market must price one thing in another.
    pub fn is_valid(&self) -> bool {
        self.base != self.quote
    }
}

impl fmt::Display for Pair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.base, self.quote)
    }
}

impl From<Pair> for String {
    fn from(p: Pair) -> String {
        p.to_string()
    }
}

impl TryFrom<String> for Pair {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let Some((base, quote)) = s.split_once('-') else {
            return Err(format!("pair must look like BASE-QUOTE, got {s:?}"));
        };
        Ok(Pair {
            base: base.parse()?,
            quote: quote.parse()?,
        })
    }
}
