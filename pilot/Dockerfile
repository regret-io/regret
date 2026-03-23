FROM rust:1.92-bookworm AS builder
RUN apt-get update && apt-get install -y clang libclang-dev protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# Step 1: Cache dependencies — only re-runs when Cargo.toml/Cargo.lock change
COPY Cargo.toml Cargo.lock ./
COPY regret.proto ./
COPY proto/Cargo.toml proto/Cargo.toml
COPY proto/build.rs proto/build.rs
COPY pilot/Cargo.toml pilot/Cargo.toml
RUN mkdir -p proto/src && echo "" > proto/src/lib.rs \
    && mkdir -p pilot/src && echo "fn main() {}" > pilot/src/main.rs
ARG BUILD_MODE=debug
RUN if [ "$BUILD_MODE" = "release" ]; then \
      cargo build --release -p regret-pilot 2>/dev/null || true; \
    else \
      cargo build -p regret-pilot 2>/dev/null || true; \
    fi

# Step 2: Build actual source — touch to invalidate cached dummy
COPY proto/ proto/
COPY pilot/ pilot/
RUN touch proto/src/lib.rs pilot/src/main.rs \
    && if [ "$BUILD_MODE" = "release" ]; then \
      cargo build --release -p regret-pilot; \
    else \
      cargo build -p regret-pilot; \
    fi

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates libstdc++6 libssl3 curl && rm -rf /var/lib/apt/lists/* \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/
ARG BUILD_MODE=debug
COPY --from=builder /build/target/${BUILD_MODE}/regret-pilot /usr/local/bin/
EXPOSE 8080 9090
CMD ["regret-pilot"]
