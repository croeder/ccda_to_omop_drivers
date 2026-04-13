#!/usr/bin/env bash
# run_cluster.sh — start Spark cluster and run CCDA→OMOP conversion on Pi workers.
#
# Usage:
#   ./run_cluster.sh              # small test set (CCDA_OMOP_Conversion_Package/resources)
#   ./run_cluster.sh --large      # large dataset (CCDA-data/xml_load_test, 747 files)
#   ./run_cluster.sh /path/to/dir # explicit input directory
#
#   Override with env vars:
#     CCDA_INPUT_DIR   — directory of CCDA XML files (read by driver)
#     CCDA_OUTPUT_DIR  — output path on each worker node (default: /tmp/parquet2)
#     CCDA_CODEMAP     — path to map.csv (default: alongside package resources)
#
# Requirements:
#   - Spark master runs on this machine (10.0.1.175)
#   - Workers at WORKER_HOSTS have python3.13 + packages installed (run setup_pis.sh first)
#   - Passwordless SSH to worker hosts

set -euo pipefail

# ---- CONFIG ----
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
PKG_ROOT="$REPO_ROOT/CCDA_OMOP_Conversion_Package"
CCDA_DATA_DIR="$REPO_ROOT/CCDA-data"
NFS_INPUT="/srv/nfs/input"    # pi4 NFS share — visible to all workers
NFS_OUTPUT="/srv/nfs/output"  # pi4 NFS share — consolidated parquet output
SPARK_HOME="${SPARK_HOME:-/opt/spark/current}"
SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-10.0.1.175}"
SPARK_MASTER="spark://${SPARK_MASTER_HOST}:7077"
SPARK_LOG_DIR="${HOME}/spark-logs"
WORKER_HOSTS=("10.0.1.201" "10.0.1.202")
SSH_USER="${SSH_USER:-croeder}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON_BIN:-python3.13}"
PYSPARK_DRIVER_PYTHON_BIN="${PYSPARK_DRIVER_PYTHON_BIN:-/Users/croeder/homebrew/bin/python3.13}"

# Input/output dataset selection:
#   (default)  small test set from the conversion package, output to /tmp/parquet2
#   --large    NFS input share (747 files), output to NFS output share
#   positional arg overrides input dir; CCDA_OUTPUT_DIR overrides output
if [[ "${1:-}" == "--large" ]]; then
    export CCDA_INPUT_DIR="${CCDA_INPUT_DIR:-$NFS_INPUT}"
    export CCDA_OUTPUT_DIR="${CCDA_OUTPUT_DIR:-$NFS_OUTPUT}"
    shift
else
    export CCDA_INPUT_DIR="${1:-${CCDA_INPUT_DIR:-$PKG_ROOT/resources}}"
    [[ $# -gt 0 ]] && shift || true
fi
export CCDA_OUTPUT_DIR="${CCDA_OUTPUT_DIR:-/tmp/parquet2}"
export CCDA_CODEMAP="${CCDA_CODEMAP:-$PKG_ROOT/resources/map.csv}"

CONVERT_SCRIPT="$HERE/src/ccda_to_omop_spark/convert_2.py"

echo "Input dir:  $CCDA_INPUT_DIR"
echo "Output dir: $CCDA_OUTPUT_DIR (on each worker)"
echo "Codemap:    $CCDA_CODEMAP"
echo "Master:     $SPARK_MASTER"
echo ""

# ---- Start master if not running ----
if nc -z "$SPARK_MASTER_HOST" 7077 2>/dev/null; then
    echo "[OK] Spark master already running on port 7077"
else
    echo "[..] Starting Spark master..."
    mkdir -p "$SPARK_LOG_DIR"
    SPARK_LOG_DIR="$SPARK_LOG_DIR" "$SPARK_HOME/sbin/start-master.sh"
    sleep 3
    if nc -z "$SPARK_MASTER_HOST" 7077; then
        echo "[OK] Spark master started"
    else
        echo "[FAIL] Spark master did not start" >&2
        exit 1
    fi
fi

# ---- Start workers ----
for host in "${WORKER_HOSTS[@]}"; do
    already_up=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" \
        "pgrep -f 'spark.deploy.worker.Worker' && echo yes || echo no" 2>/dev/null)
    if [[ "$already_up" == "yes" ]]; then
        echo "[OK] Worker already running on $host"
    else
        echo "[..] Starting worker on $host..."
        ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" \
            "mkdir -p ~/spark-logs && SPARK_LOG_DIR=~/spark-logs /opt/spark/current/sbin/start-worker.sh $SPARK_MASTER" 2>&1 | tail -1
    fi
done

# Wait for workers to register
sleep 4
workers=$(curl -s "http://${SPARK_MASTER_HOST}:8080" | grep -oE "[0-9]+ Alive" | head -1)
echo "[OK] Workers registered: $workers"
echo ""

# ---- Submit job ----
echo "[..] Submitting job..."
PYSPARK_PYTHON="$PYSPARK_PYTHON_BIN" \
PYSPARK_DRIVER_PYTHON="$PYSPARK_DRIVER_PYTHON_BIN" \
"$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --conf spark.executor.memory=2g \
    "$CONVERT_SCRIPT" \
    2>&1 | tee /tmp/ccda_spark_run.log | grep -E "^OK:|^ERROR:"

echo ""
ok_count=$(grep -c "^OK:"    /tmp/ccda_spark_run.log || true)
err_count=$(grep -c "^ERROR:" /tmp/ccda_spark_run.log || true)
echo "Results: $ok_count OK, $err_count ERROR"

if [[ "$err_count" -gt 0 ]]; then
    echo ""
    echo "Errors:"
    grep "^ERROR:" /tmp/ccda_spark_run.log | sed 's/ERROR: .* :: /  /' | sort | uniq -c | sort -rn
fi

echo ""
echo "Parquet output per worker:"
for host in "${WORKER_HOSTS[@]}"; do
    count=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" \
        "find '$CCDA_OUTPUT_DIR' -name '*.parquet' 2>/dev/null | wc -l" 2>/dev/null || echo "?")
    echo "  $host: $count files in $CCDA_OUTPUT_DIR"
done
