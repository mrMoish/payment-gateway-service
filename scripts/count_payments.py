"""Count provider payments per operation, reading simulator logs from stdin.

Exits non-zero if any operation has more than one payment, so this doubles as a
check you can run in CI.
"""

from __future__ import annotations

import collections
import json
import re
import sys

new_payments: collections.Counter[str] = collections.Counter()
total_calls: collections.Counter[str] = collections.Counter()

for line in sys.stdin:
    match = re.search(r"\{.*\}", line)
    if not match:
        continue
    try:
        record = json.loads(match.group())
    except ValueError:
        continue
    operation_id = record.get("operationId")
    if not operation_id:
        continue
    total_calls[operation_id] += 1
    # replay=false means the provider created a new payment for this key.
    if record.get("replay") is False:
        new_payments[operation_id] += 1

print(f"{'operationId':<26}{'payments':>10}{'calls':>8}")
for operation_id in sorted(total_calls):
    print(f"{operation_id:<26}{new_payments[operation_id]:>10}{total_calls[operation_id]:>8}")

violations = sorted(op for op, count in new_payments.items() if count != 1)
print()
if violations:
    print(f"VIOLATIONS: {violations}")
    sys.exit(1)
print("VIOLATIONS: none — exactly one payment per operation")
