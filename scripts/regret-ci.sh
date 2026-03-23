#!/usr/bin/env bash
set -euo pipefail

# Regret CI — create hypotheses, trigger upgrade test, wait for results.
#
# Usage:
#   ./scripts/regret-ci.sh \
#     --api http://regret:8080 \
#     --candidate-image streamnative/oxia:v0.5.0-rc1 \
#     --stable-image streamnative/oxia:v0.4.0 \
#     --duration 3h \
#     --adapter-addr http://oxia-adapter:9090

API=""
CANDIDATE_IMAGE=""
STABLE_IMAGE=""
DURATION="3h"
CHECKPOINT_EVERY="10m"
ADAPTER_ADDR="http://oxia-adapter:9090"
ADAPTER_NAME="oxia"
POLL_INTERVAL=300  # 5 minutes
GENERATORS="basic-kv kv-cas kv-ephemeral kv-secondary-index kv-sequence"
UPGRADE_SCENARIO="oxia-upgrade-test"

while [[ $# -gt 0 ]]; do
  case $1 in
    --api) API="$2"; shift 2;;
    --candidate-image) CANDIDATE_IMAGE="$2"; shift 2;;
    --stable-image) STABLE_IMAGE="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --checkpoint-every) CHECKPOINT_EVERY="$2"; shift 2;;
    --adapter-addr) ADAPTER_ADDR="$2"; shift 2;;
    --adapter-name) ADAPTER_NAME="$2"; shift 2;;
    --poll-interval) POLL_INTERVAL="$2"; shift 2;;
    --generators) GENERATORS="$2"; shift 2;;
    --upgrade-scenario) UPGRADE_SCENARIO="$2"; shift 2;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
done

if [[ -z "$API" || -z "$CANDIDATE_IMAGE" || -z "$STABLE_IMAGE" ]]; then
  echo "Usage: $0 --api <url> --candidate-image <image> --stable-image <image> [options]"
  exit 1
fi

echo "=== Regret CI ==="
echo "API:             $API"
echo "Candidate:       $CANDIDATE_IMAGE"
echo "Stable:          $STABLE_IMAGE"
echo "Duration:        $DURATION"
echo "Checkpoint:      $CHECKPOINT_EVERY"
echo "Generators:      $GENERATORS"
echo ""

HYPOTHESIS_IDS=()
FIRST_HYP_ID=""

# Step 1: Create hypotheses for all generators
for gen in $GENERATORS; do
  echo "Creating hypothesis: ci-${gen}..."
  RESULT=$(curl -sf "${API}/api/hypothesis" \
    -X POST -H 'Content-Type: application/json' \
    -d "{
      \"name\": \"ci-${gen}-$(date +%s)\",
      \"generator\": \"${gen}\",
      \"adapter\": \"${ADAPTER_NAME}\",
      \"adapter_addr\": \"${ADAPTER_ADDR}\",
      \"duration\": \"${DURATION}\",
      \"checkpoint_every\": \"${CHECKPOINT_EVERY}\",
      \"tolerance\": {\"structural\":[{\"field\":\"version_id\",\"ignore\":true}]}
    }")
  HYP_ID=$(echo "$RESULT" | jq -r .id)
  HYPOTHESIS_IDS+=("$HYP_ID")
  if [[ -z "$FIRST_HYP_ID" ]]; then
    FIRST_HYP_ID="$HYP_ID"
  fi
  echo "  Created: $HYP_ID ($gen)"
done

echo ""
echo "Created ${#HYPOTHESIS_IDS[@]} hypotheses. Primary: $FIRST_HYP_ID"

# Step 2: Start all runs
for hyp_id in "${HYPOTHESIS_IDS[@]}"; do
  echo "Starting run: $hyp_id"
  curl -sf "${API}/api/hypothesis/${hyp_id}/run" \
    -X POST -H 'Content-Type: application/json' -d '{}' > /dev/null
done

echo "All runs started."

# Step 3: Trigger upgrade test scenario with image overrides
echo ""
echo "Triggering upgrade test: $UPGRADE_SCENARIO"

# Find scenario ID by name
SCENARIO_ID=$(curl -sf "${API}/api/chaos/scenarios" \
  | jq -r ".items[] | select(.name==\"${UPGRADE_SCENARIO}\") | .id")

if [[ -z "$SCENARIO_ID" || "$SCENARIO_ID" == "null" ]]; then
  echo "ERROR: Chaos scenario '${UPGRADE_SCENARIO}' not found"
  exit 1
fi

INJECT_RESULT=$(curl -sf "${API}/api/chaos/scenarios/${SCENARIO_ID}/inject" \
  -X POST -H 'Content-Type: application/json' \
  -d "{
    \"overrides\": {
      \"upgrade_test\": {
        \"hypothesis_id\": \"${FIRST_HYP_ID}\",
        \"candidate_image\": \"${CANDIDATE_IMAGE}\",
        \"stable_image\": \"${STABLE_IMAGE}\"
      }
    }
  }")
INJECTION_ID=$(echo "$INJECT_RESULT" | jq -r .injection_id)
echo "Injection started: $INJECTION_ID"

# Step 4: Poll all hypotheses until done
echo ""
echo "Polling status every ${POLL_INTERVAL}s..."

while true; do
  ALL_DONE=true
  TOTAL_VIOLATIONS=0
  TOTAL_FAILED_CK=0

  for hyp_id in "${HYPOTHESIS_IDS[@]}"; do
    STATUS=$(curl -sf "${API}/api/hypothesis/${hyp_id}/status")
    STATE=$(echo "$STATUS" | jq -r .status)
    VIOLATIONS=$(echo "$STATUS" | jq -r '.progress.safety_violations // 0')
    FAILED_CK=$(echo "$STATUS" | jq -r '.progress.failed_checkpoints // 0')
    OPS=$(echo "$STATUS" | jq -r '.progress.completed_ops // 0')
    ELAPSED=$(echo "$STATUS" | jq -r '.progress.elapsed_secs // 0')

    if [[ "$STATE" == "running" ]]; then
      ALL_DONE=false
    fi

    TOTAL_VIOLATIONS=$((TOTAL_VIOLATIONS + VIOLATIONS))
    TOTAL_FAILED_CK=$((TOTAL_FAILED_CK + FAILED_CK))

    echo "  $hyp_id: status=$STATE ops=$OPS elapsed=${ELAPSED}s violations=$VIOLATIONS"
  done

  if [[ "$ALL_DONE" == "true" ]]; then
    break
  fi

  sleep "$POLL_INTERVAL"
done

# Step 5: Report results
echo ""
echo "=== Results ==="
FAILED=false
for hyp_id in "${HYPOTHESIS_IDS[@]}"; do
  STATUS=$(curl -sf "${API}/api/hypothesis/${hyp_id}/status")
  STATE=$(echo "$STATUS" | jq -r .status)
  VIOLATIONS=$(echo "$STATUS" | jq -r '.progress.safety_violations // 0')
  FAILED_CK=$(echo "$STATUS" | jq -r '.progress.failed_checkpoints // 0')
  OPS=$(echo "$STATUS" | jq -r '.progress.completed_ops // 0')
  PASSED_CK=$(echo "$STATUS" | jq -r '.progress.passed_checkpoints // 0')

  if [[ "$VIOLATIONS" != "0" || "$FAILED_CK" != "0" ]]; then
    echo "  FAIL $hyp_id: violations=$VIOLATIONS failed_checkpoints=$FAILED_CK ops=$OPS"
    FAILED=true
  else
    echo "  PASS $hyp_id: ops=$OPS checkpoints=$PASSED_CK"
  fi
done

# Step 6: Cleanup
echo ""
echo "Cleaning up hypotheses..."
for hyp_id in "${HYPOTHESIS_IDS[@]}"; do
  curl -sf "${API}/api/hypothesis/${hyp_id}" -X DELETE > /dev/null 2>&1 || true
done

if [[ "$FAILED" == "true" ]]; then
  echo ""
  echo "CANDIDATE RELEASE FAILED"
  exit 1
else
  echo ""
  echo "CANDIDATE RELEASE PASSED"
  exit 0
fi
