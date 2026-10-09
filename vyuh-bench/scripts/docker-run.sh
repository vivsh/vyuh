#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."

image="${VYUH_BENCH_IMAGE:-vyuh-bench:local}"
results="${VYUH_BENCH_RESULTS:-$HOME/Codex/vyuh/benchmarks/http/results}"
mkdir -p "$results"

docker build -f vyuh-bench/Dockerfile -t "$image" .
docker run --rm \
  -e BENCH_DURATION="${BENCH_DURATION:-3}" \
  -e BENCH_WARMUP="${BENCH_WARMUP:-1}" \
  -e BENCH_TIERS="${BENCH_TIERS:-t2}" \
  -e VYUH_BENCH_RESULTS=/results \
  -v "$results:/results" \
  "$image"
