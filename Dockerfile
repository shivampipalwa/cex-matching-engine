FROM rust:1-slim-bookworm AS builder
WORKDIR /build

# Dependency cache: build once against stub sources so this layer is only
# invalidated by a manifest change, not by every edit to src/.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin/api benches \
    && : > src/lib.rs \
    && echo 'fn main() {}' > src/bin/api/main.rs \
    && cp src/bin/api/main.rs src/bin/engine.rs \
    && cp src/bin/api/main.rs src/bin/db_writer.rs \
    && cp src/bin/api/main.rs src/bin/migrate.rs \
    && cp src/bin/api/main.rs src/bin/bot.rs \
    && cp src/bin/api/main.rs benches/engine_benchmarks.rs \
    && cargo build --release --bins \
    && rm -rf src benches

COPY migrations ./migrations
COPY benches ./benches
COPY src ./src
# COPY preserves the host's mtimes, which are older than the stub artifacts
# above — without the touch, cargo decides everything is up to date and the
# image ships the stubs.
RUN find src benches -name '*.rs' -exec touch {} + \
    && cargo build --release --bins

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home app
COPY --from=builder /build/target/release/api /usr/local/bin/
COPY --from=builder /build/target/release/engine /usr/local/bin/
COPY --from=builder /build/target/release/db_writer /usr/local/bin/
COPY --from=builder /build/target/release/migrate /usr/local/bin/
COPY --from=builder /build/target/release/bot /usr/local/bin/
# Snapshots live here; compose mounts a volume over it.
RUN mkdir -p /var/lib/exchange && chown app:app /var/lib/exchange
USER app
CMD ["api"]
