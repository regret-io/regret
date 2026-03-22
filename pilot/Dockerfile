FROM rust:1.92-bookworm AS builder
RUN apt-get update && apt-get install -y clang libclang-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY pilot/ pilot/
RUN cargo build --release -p regret-pilot

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/regret-pilot /usr/local/bin/
EXPOSE 8080 9090
CMD ["regret-pilot"]
