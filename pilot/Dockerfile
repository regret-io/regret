FROM rust:1.92-bookworm AS builder
RUN apt-get update && apt-get install -y clang libclang-dev protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY regret.proto ./
COPY proto/ proto/
COPY pilot/ pilot/
RUN cargo build --release -p regret-pilot

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates libstdc++6 libssl3 curl && rm -rf /var/lib/apt/lists/* \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/
COPY --from=builder /build/target/release/regret-pilot /usr/local/bin/
EXPOSE 8080 9090
CMD ["regret-pilot"]
