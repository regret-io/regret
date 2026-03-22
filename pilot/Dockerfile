FROM rust:1.92 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY pilot/ pilot/
RUN cargo build --release -p regret-pilot

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/regret-pilot /usr/local/bin/
EXPOSE 8080 9090
CMD ["regret-pilot"]
