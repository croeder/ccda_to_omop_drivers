#!/usr/bin/env bash
# setup_pis.sh — verify and install required packages on Pi worker nodes.
#
# Usage:
#   ./setup_pis.sh [host1] [host2] ...
#   ./setup_pis.sh                    # uses default hosts below
#
# Requires passwordless SSH to each host (keys already set up).
# Uses sudo with password from SUDO_PASS env var, or prompts once.

set -euo pipefail

HOSTS=("${@:-10.0.1.201 10.0.1.202}")
SSH_USER="${SSH_USER:-croeder}"
PYTHON=python3.13
PACKAGES="pandas lxml typeguard numpy pyarrow ccda-to-omop==2.0.4"

# Prompt for sudo password once if not set
if [[ -z "${SUDO_PASS:-}" ]]; then
    read -rsp "Sudo password for $SSH_USER on Pis: " SUDO_PASS
    echo
fi

run_on() {
    local host=$1; shift
    ssh -o StrictHostKeyChecking=no "${SSH_USER}@${host}" "$@"
}

sudo_on() {
    local host=$1; shift
    run_on "$host" "echo '${SUDO_PASS}' | sudo -S $*"
}

setup_host() {
    local host=$1
    echo ""
    echo "=== $host ==="

    # ---- Python 3.13 ----
    if run_on "$host" "command -v $PYTHON" &>/dev/null; then
        ver=$(run_on "$host" "$PYTHON --version")
        echo "  [OK] $ver already installed"
    else
        echo "  [..] Installing Python 3.13 via deadsnakes PPA..."
        sudo_on "$host" "add-apt-repository -y ppa:deadsnakes/ppa 2>&1 | tail -2"
        sudo_on "$host" "apt-get install -y python3.13 python3.13-venv 2>&1 | tail -3"
        echo "  [OK] Python 3.13 installed"
    fi

    # ---- pip ----
    if ! run_on "$host" "$PYTHON -m pip --version" &>/dev/null; then
        echo "  [..] Installing pip..."
        sudo_on "$host" "apt-get install -y python3-pip --fix-missing 2>&1 | tail -3"
    fi

    # ---- Python packages ----
    echo "  [..] Installing/updating Python packages..."
    run_on "$host" "$PYTHON -m pip install --force-reinstall $PACKAGES --break-system-packages 2>&1 | tail -5"

    # ---- Spark work/logs permissions ----
    echo "  [..] Fixing Spark directory permissions..."
    sudo_on "$host" "mkdir -p /opt/spark/current/work /opt/spark/current/logs && chmod -R 777 /opt/spark/current/work /opt/spark/current/logs" 2>&1 | grep -v "^\[sudo\]" || true

    # ---- Verify ----
    echo "  [..] Verifying imports..."
    if run_on "$host" "$PYTHON -c 'import ccda_to_omop, pandas, lxml, typeguard, numpy, pyarrow; print(\"  [OK] all imports OK\")'"; then
        :
    else
        echo "  [FAIL] Import check failed on $host" >&2
        return 1
    fi

    # ---- Spark ----
    if run_on "$host" "test -x /opt/spark/current/bin/spark-submit"; then
        ver=$(run_on "$host" "/opt/spark/current/bin/spark-submit --version 2>&1 | grep 'version '")
        echo "  [OK] Spark: $ver"
    else
        echo "  [WARN] spark-submit not found at /opt/spark/current/bin/spark-submit"
    fi

    echo "  === $host ready ==="
}

for host in "${HOSTS[@]}"; do
    setup_host "$host"
done

echo ""
echo "All hosts configured."
