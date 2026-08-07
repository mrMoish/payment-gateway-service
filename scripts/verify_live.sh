#!/usr/bin/env bash
# Live verification against the running compose stack:
# receipt edge cases, then persistence across a full `down` + `up`.
set -uo pipefail

BASE="${BASE:-http://localhost:8080}"
OP="${OP:-operation-123}"

code() {
  # code <method> <path> [json-body]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$BASE$path" \
      -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$BASE$path"
  fi
}

field() {
  # field <path> <json-key>
  curl -sS "$BASE$1" | python3 -c "import sys,json;print(json.load(sys.stdin)['$2'])"
}

echo "=== receipt edge cases against the real stack ==="
PID=$(field "/operations/$OP" providerPaymentId)
echo "bound providerPaymentId: $PID"

echo -n "duplicate receipt     -> HTTP "
code POST /receipts "{\"providerPaymentId\":\"$PID\",\"operationId\":\"$OP\",\"result\":\"COMPLETED\"}"
echo ""

echo -n "conflicting REJECTED  -> HTTP "
code POST /receipts "{\"providerPaymentId\":\"$PID\",\"operationId\":\"$OP\",\"result\":\"REJECTED\"}"
echo ""

echo -n "mismatched paymentId  -> HTTP "
code POST /receipts "{\"providerPaymentId\":\"bogus\",\"operationId\":\"$OP\",\"result\":\"COMPLETED\"}"
echo ""

echo -n "duplicate create      -> HTTP "
code POST /operations "{\"operationId\":\"$OP\",\"amount\":\"1.00\",\"currency\":\"RUB\"}"
echo ""

echo -n "status still          -> "
field "/operations/$OP" status

echo ""
echo "=== full down + up, volume preserved ==="
docker compose down >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1

for _ in $(seq 1 40); do
  if [ "$(code GET /health)" = "200" ]; then break; fi
  sleep 0.5
done

echo -n "status after down/up  -> "
field "/operations/$OP" status
echo -n "providerPaymentId     -> "
field "/operations/$OP" providerPaymentId
echo -n "events preserved      -> "
curl -sS "$BASE/operations/$OP/events" \
  | python3 -c "import sys,json;print(len(json.load(sys.stdin)['events']),'events')"
echo -n "statuses              -> "
curl -sS "$BASE/metrics" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['operationsByStatus'])"
