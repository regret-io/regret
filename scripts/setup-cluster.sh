#!/usr/bin/env bash
set -euo pipefail

# Setup a Kind cluster with Oxia + Regret for local development or CI.
#
# Usage:
#   ./scripts/setup-cluster.sh
#
# Prerequisites:
#   - docker, kind, kubectl, helm, jq

CLUSTER_NAME="${CLUSTER_NAME:-regret}"
NAMESPACE="${NAMESPACE:-regret-system}"
OXIA_NAMESPACE="${OXIA_NAMESPACE:-regret-test}"
OXIA_CHART_VERSION="${OXIA_CHART_VERSION:-0.0.4}"
OXIA_IMAGE_REPOSITORY="${OXIA_IMAGE_REPOSITORY:-oxia/oxia}"
OXIA_IMAGE_TAG="${OXIA_IMAGE_TAG:-v0.16.2-rc6}"

echo "=== Setting up Regret cluster ==="
echo "Cluster:   $CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo "Oxia image: ${OXIA_IMAGE_REPOSITORY}:${OXIA_IMAGE_TAG}"

# Step 1: Create Kind cluster
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Kind cluster '$CLUSTER_NAME' already exists, reusing."
else
  echo "Creating Kind cluster..."
  kind create cluster --name "$CLUSTER_NAME" --wait 60s
fi

kubectl cluster-info --context "kind-${CLUSTER_NAME}"

# Step 2: Create namespace
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Step 3: Deploy Oxia
echo ""
echo "=== Deploying Oxia ==="
helm repo add oxia https://charts.oxia.streamnative.io 2>/dev/null || true
helm repo update

helm upgrade --install oxia oxia/oxia-cluster \
  --namespace "$NAMESPACE" \
  --version "$OXIA_CHART_VERSION" \
  --set "image.repository=${OXIA_IMAGE_REPOSITORY}" \
  --set "image.tag=${OXIA_IMAGE_TAG}" \
  --set server.replicas=3 \
  --set coordinator.replicas=3 \
  --set "namespaces[0].name=${OXIA_NAMESPACE}" \
  --set "namespaces[0].initialShardCount=1" \
  --set "namespaces[0].replicationFactor=3" \
  --wait --timeout 5m

echo "Oxia deployed. Waiting for pods..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=oxia-cluster -n "$NAMESPACE" --timeout=120s

# Step 4: Build and load Regret images
echo ""
echo "=== Building Regret images ==="

echo "Building pilot..."
docker build -f pilot/Dockerfile -t regret/pilot:latest .

echo "Building studio..."
(cd studio && docker build -t regret/studio:latest .)

echo "Building oxia adapter..."
docker build -f adapters/oxia-java/Dockerfile -t regret/oxia-adapter:latest .

echo "Loading images into Kind..."
kind load docker-image regret/pilot:latest regret/studio:latest regret/oxia-adapter:latest --name "$CLUSTER_NAME"

# Step 5: Deploy Regret
echo ""
echo "=== Deploying Regret ==="
helm upgrade --install regret charts/regret \
  --namespace "$NAMESPACE" \
  --set image.pullPolicy=Never \
  --set studio.image.pullPolicy=Never \
  --wait --timeout 2m

echo "Waiting for pilot..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=regret -n "$NAMESPACE" --timeout=60s

# Step 6: Deploy Oxia adapter
echo ""
echo "=== Deploying Oxia adapter ==="
kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: oxia-adapter
  labels:
    app: oxia-adapter
spec:
  containers:
  - name: adapter
    image: regret/oxia-adapter:latest
    imagePullPolicy: Never
    ports:
    - containerPort: 9090
    env:
    - name: OXIA_ADDR
      value: "oxia.${NAMESPACE}.svc.cluster.local:6648"
    - name: OXIA_NAMESPACE
      value: "${OXIA_NAMESPACE}"
---
apiVersion: v1
kind: Service
metadata:
  name: oxia-adapter
spec:
  selector:
    app: oxia-adapter
  ports:
  - port: 9090
    targetPort: 9090
EOF

echo "Waiting for adapter..."
kubectl wait --for=condition=ready pod/oxia-adapter -n "$NAMESPACE" --timeout=60s

# Step 7: Setup via Regret API
echo ""
echo "=== Configuring Regret ==="
kubectl port-forward -n "$NAMESPACE" svc/regret 18080:8080 &
PF_PID=$!
sleep 3

API="http://localhost:18080"

# Create adapter record
curl -sf "${API}/api/adapters" -X POST -H 'Content-Type: application/json' \
  -d "{\"name\":\"oxia\",\"image\":\"regret/oxia-adapter:latest\",\"env\":{}}" > /dev/null 2>&1 || true

# Create infinite hypotheses for all generators
GENERATORS="basic-kv kv-cas kv-ephemeral kv-secondary-index kv-sequence"
ADAPTER_ADDR="http://oxia-adapter.${NAMESPACE}.svc.cluster.local:9090"

for gen in $GENERATORS; do
  echo "Creating infinite hypothesis: $gen"
  RESULT=$(curl -sf "${API}/api/hypothesis" -X POST -H 'Content-Type: application/json' \
    -d "{
      \"name\": \"baseline-${gen}\",
      \"generator\": \"${gen}\",
      \"adapter\": \"oxia\",
      \"adapter_addr\": \"${ADAPTER_ADDR}\",
      \"checkpoint_every\": \"10m\",
      \"tolerance\": {\"structural\":[{\"field\":\"version_id\",\"ignore\":true}]}
    }" 2>/dev/null) || continue
  HYP_ID=$(echo "$RESULT" | jq -r .id)
  echo "  Starting: $HYP_ID"
  curl -sf "${API}/api/hypothesis/${HYP_ID}/run" -X POST -H 'Content-Type: application/json' -d '{}' > /dev/null 2>&1 || true
done

# Start continuous chaos
echo ""
echo "Starting continuous chaos injection..."
SCENARIO_ID=$(curl -sf "${API}/api/chaos/scenarios" | jq -r '.items[] | select(.name=="oxia-continuous-chaos") | .id')
if [[ -n "$SCENARIO_ID" && "$SCENARIO_ID" != "null" ]]; then
  curl -sf "${API}/api/chaos/scenarios/${SCENARIO_ID}/inject" -X POST -H 'Content-Type: application/json' -d '{}' > /dev/null 2>&1
  echo "Continuous chaos started."
else
  echo "Warning: oxia-continuous-chaos scenario not found, skipping."
fi

kill $PF_PID 2>/dev/null || true

echo ""
echo "=== Setup complete ==="
echo ""
echo "Access:"
echo "  Pilot API:  kubectl port-forward -n $NAMESPACE svc/regret 8080:8080"
echo "  Studio UI:  kubectl port-forward -n $NAMESPACE svc/regret-studio 3000:3000"
echo "  Then open:  http://localhost:3000"
echo ""
echo "Run candidate test:"
echo "  ./scripts/regret-ci.sh --api http://localhost:8080 --candidate-image <image> --stable-image <image>"
