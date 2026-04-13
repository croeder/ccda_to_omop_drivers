#!/usr/bin/env bash
# stop_cluster.sh — stop Spark workers on Pi nodes and the master on this machine.

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark/current}"
SPARK_LOG_DIR="${HOME}/spark-logs"
WORKER_HOSTS=("10.0.1.201" "10.0.1.202")
SSH_USER="${SSH_USER:-croeder}"

# ---- Stop workers and clean up work dirs ----
for host in "${WORKER_HOSTS[@]}"; do
    echo "[..] Stopping worker on $host..."
    ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" \
        "SPARK_LOG_DIR=~/spark-logs /opt/spark/current/sbin/stop-worker.sh 2>&1 | tail -1"

    echo "[..] Cleaning work directory on $host..."
    before=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" "df -h / | awk 'NR==2{print \$4}'")
    ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" \
        "echo chris | sudo -S rm -rf /opt/spark/current/work/app-*" 2>&1 | grep -v "^\[sudo\]" || true
    after=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" "df -h / | awk 'NR==2{print \$4}'")
    echo "    disk free: $before -> $after"
done

# ---- Stop master ----
echo "[..] Stopping Spark master..."
SPARK_LOG_DIR="$SPARK_LOG_DIR" "$SPARK_HOME/sbin/stop-master.sh" 2>&1 | tail -1

echo "Done."
