#!/bin/bash
# Common functions and configuration for dev scripts
# Source this file from other dev-*.sh scripts

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ── Logging Functions ─────────────────────────────────────────────────────────
log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
step() { echo -e "${BLUE}[STEP]${NC}  $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Common Configuration (override via env vars) ──────────────────────────────
NAMESPACE="${NAMESPACE:-default}"
HELM_RELEASE="${HELM_RELEASE:-batch-gateway}"

# Port configuration
LOCAL_PORT="${LOCAL_PORT:-8000}"
LOCAL_OBS_PORT="${LOCAL_OBS_PORT:-8081}"
LOCAL_PROCESSOR_PORT="${LOCAL_PROCESSOR_PORT:-9090}"
JAEGER_PORT="${JAEGER_PORT:-16686}"
PORT_FORWARD_STATE_DIR="${PORT_FORWARD_STATE_DIR:-/tmp}"
PROCESSOR_PORT_FORWARD_PID_FILE="${PORT_FORWARD_STATE_DIR}/${HELM_RELEASE}-processor-port-forward.pid"
PROCESSOR_PORT_FORWARD_STOP_FILE="${PORT_FORWARD_STATE_DIR}/${HELM_RELEASE}-processor-port-forward.stop"
PROCESSOR_PORT_FORWARD_LOG_FILE="${PORT_FORWARD_STATE_DIR}/${HELM_RELEASE}-processor-port-forward.log"

# Service names
REDIS_RELEASE="${REDIS_RELEASE:-redis}"
EXCHANGE_CLIENT_TYPE="${EXCHANGE_CLIENT_TYPE:-redis}"
POSTGRESQL_RELEASE="${POSTGRESQL_RELEASE:-postgresql}"
JAEGER_NAME="${JAEGER_NAME:-jaeger}"
PROMETHEUS_NAME="${PROMETHEUS_NAME:-prometheus}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9091}"
GRAFANA_NAME="${GRAFANA_NAME:-grafana}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
MINIO_PORT="${MINIO_PORT:-9002}"
VLLM_SIM_NAME="${VLLM_SIM_NAME:-vllm-sim}"
VLLM_SIM_B_NAME="${VLLM_SIM_B_NAME:-vllm-sim-b}"
VLLM_SIM_429_NAME="${VLLM_SIM_429_NAME:-vllm-sim-429}"
VLLM_SIM_ALWAYS_FAIL_NAME="${VLLM_SIM_ALWAYS_FAIL_NAME:-vllm-sim-always-fail}"
VLLM_SIM_AIMD_NAME="${VLLM_SIM_AIMD_NAME:-vllm-sim-aimd}"
MINIO_NAME="${MINIO_NAME:-minio}"

# Secret and PVC names
TLS_SECRET_NAME="${TLS_SECRET_NAME:-${HELM_RELEASE}-tls}"
APP_SECRET_NAME="${APP_SECRET_NAME:-${HELM_RELEASE}-secrets}"
FILES_PVC_NAME="${FILES_PVC_NAME:-${HELM_RELEASE}-files}"

# ── Shared Functions ──────────────────────────────────────────────────────────

stop_processor_port_forward() {
    rm -f "${PROCESSOR_PORT_FORWARD_STOP_FILE}"
    touch "${PROCESSOR_PORT_FORWARD_STOP_FILE}"

    if [ -f "${PROCESSOR_PORT_FORWARD_PID_FILE}" ]; then
        local pf_pid
        pf_pid="$(cat "${PROCESSOR_PORT_FORWARD_PID_FILE}")"
        if kill -0 "${pf_pid}" 2>/dev/null; then
            log "Stopping processor port-forward supervisor (pid ${pf_pid})..."
            pkill -TERM -P "${pf_pid}" 2>/dev/null || true
            kill "${pf_pid}" 2>/dev/null || true
        fi
    fi

    rm -f "${PROCESSOR_PORT_FORWARD_PID_FILE}"
}
