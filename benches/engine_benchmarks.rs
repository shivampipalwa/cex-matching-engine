// In-process matching-engine latency: apply() called directly, no Redis/HTTP
// hop, so this isolates engine cost from transport cost.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use matching_engine::book::{OrderRequest, OrderType, Side};
use matching_engine::command::{CancelRequest, Command, CommandResponse, DepositRequest};
use matching_engine::engine::{Engine, Limits, apply};
use matching_engine::market::{Currency, Pair};

const PAIR: Pair = Pair {
    base: Currency::SOL,
    quote: Currency::USD,
};

fn funded_engine() -> Engine {
    let mut engine = Engine::new();
    // measure matching, not the deposit ceiling / price band / self-trade checks
    engine.limits = Limits::none();
    engine.listed_pairs.insert(PAIR);
    // client_order_id 0 is reserved for these seed deposits — every benchmark
    // below uses ids >= 1, so nothing collides with them.
    apply(
        &mut engine,
        1,
        0,
        Command::Deposit(DepositRequest {
            amount: u64::MAX / 4,
            currency: Currency::USD,
        }),
    );
    apply(
        &mut engine,
        2,
        0,
        Command::Deposit(DepositRequest {
            amount: u64::MAX / 4,
            currency: Currency::SOL,
        }),
    );
    engine
}

fn place_cmd(side: Side, order_type: OrderType, price: u64, size: u64) -> Command {
    Command::Place(OrderRequest {
        pair: PAIR,
        order_type,
        side,
        price,
        size,
    })
}

fn bench_deposit(c: &mut Criterion) {
    c.bench_function("deposit", |b| {
        b.iter_batched(
            Engine::new,
            |mut engine| {
                apply(
                    &mut engine,
                    1,
                    1,
                    Command::Deposit(DepositRequest {
                        amount: 100,
                        currency: Currency::USD,
                    }),
                )
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_place_resting_order(c: &mut Criterion) {
    c.bench_function("place_resting_limit_order", |b| {
        b.iter_batched(
            funded_engine,
            |mut engine| apply(&mut engine, 1, 100, place_cmd(Side::Bid, OrderType::Limit, 100, 1)),
            BatchSize::SmallInput,
        )
    });
}

fn bench_place_crossing_order(c: &mut Criterion) {
    c.bench_function("place_crossing_order_single_trade", |b| {
        b.iter_batched(
            || {
                let mut engine = funded_engine();
                apply(&mut engine, 2, 2, place_cmd(Side::Ask, OrderType::Limit, 100, 1));
                engine
            },
            |mut engine| apply(&mut engine, 1, 100, place_cmd(Side::Bid, OrderType::Limit, 100, 1)),
            BatchSize::SmallInput,
        )
    });
}

fn bench_cancel(c: &mut Criterion) {
    c.bench_function("cancel_resting_order", |b| {
        b.iter_batched(
            || {
                let mut engine = funded_engine();
                let (resp, _) = apply(&mut engine, 1, 1, place_cmd(Side::Bid, OrderType::Limit, 100, 1));
                let order_id = match resp {
                    CommandResponse::Place(Ok(r)) => r.order_id,
                    _ => unreachable!(),
                };
                (engine, order_id)
            },
            |(mut engine, order_id)| apply(&mut engine, 1, 100, Command::Cancel(CancelRequest { order_id })),
            BatchSize::SmallInput,
        )
    });
}

// `iter_batched`'s timed loop drops whatever `routine` returns before the
// timer stops. If `routine` owns the engine, dropping a book with thousands
// of entries gets timed as if it were order-placement cost. `graveyard`
// moves the engine out of the closure into a Vec that's only dropped after
// the whole benchmark finishes, so deallocation never lands inside a sample.
fn bench_market_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("market_buy_sweep_n_levels");
    let mut graveyard: Vec<Engine> = Vec::new();
    for &levels in &[1u64, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(levels));
        group.bench_with_input(BenchmarkId::from_parameter(levels), &levels, |b, &levels| {
            b.iter_batched(
                || {
                    let mut engine = funded_engine();
                    for i in 0..levels {
                        apply(
                            &mut engine,
                            2,
                            1000 + i,
                            place_cmd(Side::Ask, OrderType::Limit, 1 + i, 1),
                        );
                    }
                    engine
                },
                |mut engine| {
                    let result = apply(&mut engine, 1, 1, place_cmd(Side::Bid, OrderType::Market, 0, levels));
                    graveyard.push(engine);
                    result
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_place_into_deep_book(c: &mut Criterion) {
    let mut group = c.benchmark_group("place_into_book_of_depth");
    let mut graveyard: Vec<Engine> = Vec::new();
    for &depth in &[10u64, 100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched(
                || {
                    let mut engine = funded_engine();
                    for i in 0..depth {
                        apply(
                            &mut engine,
                            1,
                            1000 + i,
                            place_cmd(Side::Bid, OrderType::Limit, 1 + i, 1),
                        );
                    }
                    engine
                },
                |mut engine| {
                    let result = apply(&mut engine, 2, 1, place_cmd(Side::Ask, OrderType::Limit, 100_000_000, 1));
                    graveyard.push(engine);
                    result
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_deposit,
    bench_place_resting_order,
    bench_place_crossing_order,
    bench_cancel,
    bench_market_sweep,
    bench_place_into_deep_book,
);
criterion_main!(benches);