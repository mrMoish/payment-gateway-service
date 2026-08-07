#!/usr/bin/env bash
# End-to-end demo against the running compose stack:
#   1. normal flow: create -> submit -> callback -> state + history
#   2. 30 concurrent submits of one operation
#   3. restart recovery while the provider is down
# Then proves "exactly one payment per operation" from the simulator's own logs.
set -uo pipefail

BASE="${BASE:-http://localhost:8080}"
SUFFIX="${SUFFIX:-$(date +%s)}"

post() {
  # post <path> [json-body]
  local path="$1" body="${2:-}"
  if [ -n "$body" ]; then
    curl -sS -X POST "$BASE$path" -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X POST "$BASE$path"
  fi
}

show() {
  # show <label> <operation-id>
  printf '%-24s ' "$1"
  curl -sS "$BASE/operations/$2" \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["status"], d["providerPaymentId"])'
}

wait_status() {
  # wait_status <operation-id> <status> [tries]
  local op="$1" want="$2" tries="${3:-40}"
  for _ in $(seq 1 "$tries"); do
    local got
    got=$(curl -sS "$BASE/operations/$op" \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])' 2>/dev/null)
    [ "$got" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}

wait_health() {
  for _ in $(seq 1 60); do
    [ "$(curl -sS -o /dev/null -w '%{http_code}' "$BASE/health" 2>/dev/null)" = "200" ] && return 0
    sleep 0.5
  done
  return 1
}

OP_NORMAL="operation-normal-$SUFFIX"
OP_RACE="operation-race-$SUFFIX"
OP_RESTART="operation-restart-$SUFFIX"

echo "=== 1. normal flow ==="
post /operations "{\"operationId\":\"$OP_NORMAL\",\"amount\":\"1000.00\",\"currency\":\"RUB\",\"description\":\"Оплата заказа\"}" >/dev/null
echo -n "submit -> HTTP "
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$BASE/operations/$OP_NORMAL/submit"
wait_status "$OP_NORMAL" COMPLETED
show "after callback:" "$OP_NORMAL"
echo "history:"
curl -sS "$BASE/operations/$OP_NORMAL/events" | python3 -c '
import sys, json
for e in json.load(sys.stdin)["events"]:
    print(f"  {e[\"eventId\"]} {e[\"type\"]:<28} {e[\"fromStatus\"]} -> {e[\"toStatus\"]}")'

echo ""
echo "=== 2. thirty concurrent submits ==="
post /operations "{\"operationId\":\"$OP_RACE\",\"amount\":\"77.77\",\"currency\":\"RUB\"}" >/dev/null
for _ in $(seq 1 30); do
  curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$BASE/operations/$OP_RACE/submit" &
done | sort | uniq -c
wait
wait_status "$OP_RACE" COMPLETED
show "after callback:" "$OP_RACE"

echo ""
echo "=== 3. restart recovery (provider down during submit) ==="
docker compose stop provider-simulator >/dev/null 2>&1
post /operations "{\"operationId\":\"$OP_RESTART\",\"amount\":\"500.00\",\"currency\":\"RUB\"}" >/dev/null
post "/operations/$OP_RESTART/submit" >/dev/null
sleep 2
show "provider down:" "$OP_RESTART"
docker compose rm -sf candidate-service >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1
wait_health
wait_status "$OP_RESTART" COMPLETED 60
show "after restart:" "$OP_RESTART"
echo "history:"
curl -sS "$BASE/operations/$OP_RESTART/events" | python3 -c '
import sys, json
for e in json.load(sys.stdin)["events"]:
    print(f"  {e[\"eventId\"]} {e[\"type\"]:<28} {e[\"fromStatus\"]} -> {e[\"toStatus\"]}")'

echo ""
echo "=== 4. invariant: exactly one payment per operation ==="
"$(dirname "$0")/count_payments.sh"
