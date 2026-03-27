#!/usr/bin/env bash
#
# run_ec2_benchmarks.sh — Build Release + run coroutine benchmark suite on EC2.
#
# Usage (run from TileDB repo root on EC2):
#   ./test/benchmarking/run_ec2_benchmarks.sh [S3_URI_PREFIX] [S3_REGION]
#
# Example:
#   ./test/benchmarking/run_ec2_benchmarks.sh \
#       s3://tiledb-ypatia-east-1/hackathon/per_tile/ us-east-1
#
# Requirements:
#   - AWS credentials available (instance profile or env vars)
#   - cmake, make, python3 on PATH
#   - ~4 GB RAM, 4+ cores recommended
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build_release"
INSTALL_DIR="${REPO_ROOT}/dist_release"
BENCH_DIR="${REPO_ROOT}/test/benchmarking"

S3_URI_PREFIX="${1:-}"
S3_REGION="${2:-us-east-1}"
TRIALS="${TRIALS:-5}"
CSV_OUT="${BENCH_DIR}/results_$(date +%Y%m%d_%H%M%S).csv"

echo "=== TileDB Coroutine Benchmark Run ==="
echo "Branch: $(git rev-parse --abbrev-ref HEAD)"
echo "Commit: $(git rev-parse --short HEAD)"
echo "S3 URI: ${S3_URI_PREFIX:-local}"
echo "Trials: ${TRIALS}"
echo ""

# ── 1. Release build with S3 ───────────────────────────────────────────────
echo "--- Building Release (S3=ON) ---"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

S3_FLAG="OFF"
if [ -n "${S3_URI_PREFIX}" ]; then
    S3_FLAG="ON"
fi

cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DTILEDB_S3="${S3_FLAG}" \
    -DTILEDB_WERROR=OFF \
    -DTILEDB_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
    "${REPO_ROOT}"

make -j"$(nproc)" tiledb
make install

echo ""
echo "--- Build complete. Library at ${INSTALL_DIR} ---"
echo ""

# ── 2. Build benchmarks ────────────────────────────────────────────────────
cd "${BENCH_DIR}"
echo "--- Building benchmarks ---"
mkdir -p build
cd build
cmake -DCMAKE_PREFIX_PATH="${INSTALL_DIR}" ../src
make -j"$(nproc)"
cd "${BENCH_DIR}"

echo ""
echo "--- Benchmarks built ---"
echo ""

# ── 3. Run comparison (sync vs coroutine) ─────────────────────────────────
echo "--- Running benchmark suite (${TRIALS} trials each) ---"
echo ""

BENCH_ARGS=(
    --tiledb "${INSTALL_DIR}"
    --mode compare
    --trials "${TRIALS}"
    --csv "${CSV_OUT}"
)

if [ -n "${S3_URI_PREFIX}" ]; then
    BENCH_ARGS+=(
        --backend s3
        --uri-prefix "${S3_URI_PREFIX}"
        --s3-region "${S3_REGION}"
    )
fi

python3 benchmark.py "${BENCH_ARGS[@]}"

echo ""
echo "=== Results written to ${CSV_OUT} ==="
echo ""
echo "Copy results back with:"
echo "  scp ec2-host:${CSV_OUT} ."
