#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

backend_pid=""
frontend_pid=""

cleanup() {
  if [[ -n "${frontend_pid}" ]]; then
    kill "${frontend_pid}" 2>/dev/null || true
  fi
  if [[ -n "${backend_pid}" ]]; then
    kill "${backend_pid}" 2>/dev/null || true
  fi
}

trap cleanup EXIT

if [[ ! -f "${ROOT_DIR}/backend/.env" ]]; then
  echo "Missing backend/.env"
  exit 1
fi

echo "Starting backend on :8000..."
(
  cd "${ROOT_DIR}/backend"
  set -a
  source .env
  set +a
  exec uvicorn main:APP --reload --port 8000
) &
backend_pid=$!

echo "Starting frontend on :5173..."
(
  cd "${ROOT_DIR}/frontend"
  exec npm run dev
) &
frontend_pid=$!

echo "Opening http://localhost:5173 ..."
open "http://localhost:5173" >/dev/null 2>&1 || true

echo ""
echo "All services started. Press Ctrl-C to stop."
wait
