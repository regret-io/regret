# Regret — task runner

# Default recipe
default:
    @just --list

# Sync proto file to Java SDK
sync-proto:
    cp regret.proto sdk/java/src/main/proto/regret.proto

# Build all Rust crates
build:
    cargo build --workspace

# Run all tests
test:
    cargo test --workspace

# Run pilot locally with default settings
run-pilot port="8080" grpc-port="9090" data-dir="/tmp/regret-dev":
    DATA_DIR={{data-dir}} \
    ROCKSDB_PATH={{data-dir}}/rocksdb \
    DATABASE_URL="sqlite:///{{data-dir}}/regret.db" \
    HTTP_PORT={{port}} \
    GRPC_PORT={{grpc-port}} \
    cargo run -p regret-pilot

# Build Java SDK
build-java-sdk:
    cd sdk/java && mvn compile

# Install Java SDK to local Maven repo
install-java-sdk:
    cd sdk/java && mvn install -q

# Build Oxia adapter
build-oxia-adapter: install-java-sdk
    cd adapters/oxia-java && mvn compile

# Package Oxia adapter as fat jar
package-oxia-adapter: install-java-sdk
    cd adapters/oxia-java && mvn package -q

# Clean all build artifacts
clean:
    cargo clean
    cd sdk/java && mvn clean -q || true
    cd adapters/oxia-java && mvn clean -q || true

# Smoke test: start pilot, create hypothesis, generate, run, check status
smoke-test port="18080" grpc-port="19090":
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf /tmp/regret-smoke
    DATA_DIR=/tmp/regret-smoke ROCKSDB_PATH=/tmp/regret-smoke/rocksdb \
        DATABASE_URL="sqlite:///tmp/regret-smoke/regret.db" \
        HTTP_PORT={{port}} GRPC_PORT={{grpc-port}} \
        cargo run -p regret-pilot 2>/dev/null &
    PILOT_PID=$!
    trap "kill $PILOT_PID 2>/dev/null; wait $PILOT_PID 2>/dev/null" EXIT
    sleep 3

    echo "=== Health ==="
    curl -sf http://localhost:{{port}}/health | jq .

    echo "=== Create hypothesis ==="
    HYP=$(curl -sf -X POST http://localhost:{{port}}/api/hypothesis \
        -H 'Content-Type: application/json' \
        -d '{"name":"smoke","profile":"basic-kv","state_machine":{"type":"basic-kv"}}')
    echo "$HYP" | jq .
    HYP_ID=$(echo "$HYP" | jq -r '.id')

    echo "=== Generate ==="
    curl -sf -X POST "http://localhost:{{port}}/api/hypothesis/${HYP_ID}/generate" \
        -H 'Content-Type: application/json' \
        -d '{"profile":"basic-kv","ops":100,"keys":10,"seed":42}' | jq .

    echo "=== Start run ==="
    curl -sf -X POST "http://localhost:{{port}}/api/hypothesis/${HYP_ID}/run" \
        -H 'Content-Type: application/json' \
        -d '{"execution":{"batch_size":50,"checkpoint_every":2}}' | jq .
    sleep 2

    echo "=== Status ==="
    curl -sf "http://localhost:{{port}}/api/hypothesis/${HYP_ID}/status" | jq .

    echo "=== Bundle ==="
    curl -sf -o /tmp/regret-smoke/bundle.zip "http://localhost:{{port}}/api/hypothesis/${HYP_ID}/bundle"
    unzip -l /tmp/regret-smoke/bundle.zip

    echo "=== SMOKE TEST PASSED ==="
