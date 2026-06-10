#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRAIN_DELAY="${DRAIN_DELAY:-2s}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-10s}"
CLIENT_DURATION="${CLIENT_DURATION:-9s}"
CONCURRENCY="${CONCURRENCY:-6}"
INTERVAL="${INTERVAL:-100ms}"
SIGNAL_AFTER="${SIGNAL_AFTER:-2s}"
KEEP_LOGS="${KEEP_LOGS:-0}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/graceful-shutdown-demo.XXXXXX")"
BIN_DIR="$TMP_DIR/bin"
LOG_DIR="$TMP_DIR/logs"
mkdir -p "$BIN_DIR" "$LOG_DIR"

PIDS=()
PID_A=""
PID_B=""
PID_C=""
PID_CLIENT=""

cleanup() {
  local status=$?
  set +e
  for pid in "${PIDS[@]}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${PIDS[@]}"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$KEEP_LOGS" != "1" && $status -eq 0 ]]; then
    rm -rf "$TMP_DIR"
  else
    echo "logs kept at: $LOG_DIR" >&2
  fi
  exit $status
}
trap cleanup EXIT

log() {
  printf '[demo] %s\n' "$*"
}

free_ports() {
  python3 - <<'PY'
import socket
sockets = []
try:
    for _ in range(3):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    print(" ".join(str(sock.getsockname()[1]) for sock in sockets))
finally:
    for sock in sockets:
        sock.close()
PY
}

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local timeout_seconds="$3"
  local started now
  started="$(date +%s)"
  while true; do
    if grep -q -- "$pattern" "$file" 2>/dev/null; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timeout waiting for pattern '$pattern' in $file" >&2
      echo "--- $file ---" >&2
      cat "$file" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

build_binaries() {
  log "building temporary binaries"
  (cd "$ROOT_DIR" && go build -o "$BIN_DIR/server" ./cmd/server)
  (cd "$ROOT_DIR" && go build -o "$BIN_DIR/client" ./cmd/client)
}

start_server() {
  local id="$1"
  local port="$2"
  local log_file="$LOG_DIR/server-$id.log"
  "$BIN_DIR/server" \
    -addr "127.0.0.1:$port" \
    -server-id "$id" \
    -drain-delay "$DRAIN_DELAY" \
    -shutdown-timeout "$SHUTDOWN_TIMEOUT" \
    >"$log_file" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  wait_for_log "$log_file" "server $id listening" 5
  echo "$pid"
}

assert_demo() {
  local client_log="$LOG_DIR/client.log"
  local server_a_log="$LOG_DIR/server-A.log"

  log "asserting server A published DRAINING and GRACEFUL_SHUTDOWN"
  grep -q '/A -> SERVICE_STATUS_DRAINING' "$client_log"
  grep -q '/A -> SERVICE_STATUS_GRACEFUL_SHUTDOWN' "$client_log"
  grep -q 'shutdown completed gracefully' "$server_a_log"

  local draining_line
  draining_line="$(grep -n '/A -> SERVICE_STATUS_DRAINING' "$client_log" | head -n1 | cut -d: -f1)"
  if [[ -z "$draining_line" ]]; then
    echo "could not find A DRAINING line in client log" >&2
    return 1
  fi

  if tail -n "+$draining_line" "$client_log" | grep -q 'handled by A '; then
    echo "client routed work to A after observing A as DRAINING" >&2
    echo "--- client log from DRAINING ---" >&2
    tail -n "+$draining_line" "$client_log" >&2
    return 1
  fi

  if ! grep -q 'handled by B ' "$client_log"; then
    echo "client log did not show work handled by B" >&2
    return 1
  fi
  if ! grep -q 'handled by C ' "$client_log"; then
    echo "client log did not show work handled by C" >&2
    return 1
  fi
}

main() {
  build_binaries

  read -r PORT_A PORT_B PORT_C < <(free_ports)
  log "using ports: A=$PORT_A B=$PORT_B C=$PORT_C"

  PID_A="$(start_server A "$PORT_A")"
  PID_B="$(start_server B "$PORT_B")"
  PID_C="$(start_server C "$PORT_C")"
  PIDS+=("$PID_A" "$PID_B" "$PID_C")

  log "starting client load"
  "$BIN_DIR/client" \
    -servers "127.0.0.1:$PORT_A,127.0.0.1:$PORT_B,127.0.0.1:$PORT_C" \
    -client-id demo-client \
    -concurrency "$CONCURRENCY" \
    -interval "$INTERVAL" \
    -duration "$CLIENT_DURATION" \
    >"$LOG_DIR/client.log" 2>&1 &
  PID_CLIENT=$!
  PIDS+=("$PID_CLIENT")

  wait_for_log "$LOG_DIR/client.log" '/A -> SERVICE_STATUS_SERVING' 5
  wait_for_log "$LOG_DIR/client.log" '/B -> SERVICE_STATUS_SERVING' 5
  wait_for_log "$LOG_DIR/client.log" '/C -> SERVICE_STATUS_SERVING' 5

  log "sending SIGINT to server A (pid $PID_A)"
  sleep "$SIGNAL_AFTER"
  kill -INT "$PID_A"

  wait_for_log "$LOG_DIR/client.log" '/A -> SERVICE_STATUS_DRAINING' 5
  wait_for_log "$LOG_DIR/client.log" '/A -> SERVICE_STATUS_GRACEFUL_SHUTDOWN' 8
  wait_for_log "$LOG_DIR/server-A.log" 'shutdown completed gracefully' 8

  log "waiting for client to finish"
  wait "$PID_CLIENT"

  assert_demo

  log "PASS: after A entered DRAINING, client work continued on B/C and was not routed to A"
  log "sample logs:"
  grep -E '/A ->|handled by [ABC] ' "$LOG_DIR/client.log" | tail -n 20 || true
  log "logs directory: $LOG_DIR"
}

main "$@"
