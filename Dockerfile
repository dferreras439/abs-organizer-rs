# syntax=docker/dockerfile:1

FROM rust:1.86-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/abs-organizer-rs /app/abs-organizer-rs
COPY --from=builder /app/static /app/static

ENV BIND_ADDR=0.0.0.0:3000
EXPOSE 3000

CMD ["/app/abs-organizer-rs"]