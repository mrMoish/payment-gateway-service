#!/usr/bin/env bash
# Proves the main invariant from the provider's own logs.
#
# The simulator logs every accepted payment with a "replay" field:
#   replay=false -> a NEW payment was created
#   replay=true  -> the existing payment was returned for the same Idempotency-Key
# So "exactly one payment per operation" == exactly one replay=false per operationId.
#
# Exits non-zero if any operation got more than one payment.
# Note: `docker compose down` discards container logs, so run this against the
# stack that actually served the requests.
set -uo pipefail

logs=$(docker compose logs provider-simulator 2>/dev/null | grep '"msg":"payment accepted"')

if [ -z "$logs" ]; then
  echo "No accepted payments found in the simulator logs."
  echo "Run a scenario first (create + submit), and note that 'docker compose down' clears logs."
  exit 0
fi

printf '%s\n' "$logs" | python3 "$(dirname "$0")/count_payments.py"
