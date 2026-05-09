#!/usr/bin/env bash
# Sets up a 6-node cluster (5 CRDB + 1 workload) and initializes TPCC data.
# Run once before executing the gRPC/DRPC benchmark operations.
#
# Usage: ./bench/setup.sh [cluster-name] [cockroach-version] [warehouses]
#
# After this script completes, run the benchmarks with:
#   roachtest run-operations drpc-bench/tpcc --cluster CLUSTER --config bench/grpc.yaml
#   roachtest run-operations drpc-bench/tpcc --cluster CLUSTER --config bench/drpc.yaml

set -euo pipefail

CLUSTER=${1:-$USER-drpc-bench}
VERSION=${2:-v26.1.0}
WAREHOUSES=${3:-1000}

echo "==> Creating cluster: $CLUSTER (6 nodes, n2-standard-8)"
roachprod create "$CLUSTER" \
  --nodes 6 \
  --gce-machine-type n2-standard-8 \
  --lifetime 24h

echo "==> Staging CockroachDB $VERSION on all nodes"
roachprod stage "$CLUSTER" release "$VERSION"

echo "==> Starting CockroachDB on nodes 1-5"
roachprod start "$CLUSTER:1-5"

echo "==> Initializing TPCC dataset ($WAREHOUSES warehouses) — this may take several minutes"
roachprod run "$CLUSTER:6" -- \
  "cockroach workload init tpcc --warehouses=$WAREHOUSES '{pgurl:1-5}'"

echo ""
echo "Cluster $CLUSTER is ready. Run the benchmark with:"
echo ""
echo "  # gRPC (baseline)"
echo "  roachtest run-operations drpc-bench/tpcc \\"
echo "    --cluster $CLUSTER \\"
echo "    --config bench/grpc.yaml"
echo ""
echo "  # DRPC (--use-new-rpc)"
echo "  roachtest run-operations drpc-bench/tpcc \\"
echo "    --cluster $CLUSTER \\"
echo "    --config bench/drpc.yaml"
echo ""
echo "Fetch histograms after each run:"
echo "  roachprod get $CLUSTER:6 /tmp/drpc-tpcc-hist.json ./grpc-hist.json"
echo "  roachprod get $CLUSTER:6 /tmp/drpc-tpcc-hist.json ./drpc-hist.json"
echo ""
echo "Cleanup when done:"
echo "  roachprod destroy $CLUSTER"
