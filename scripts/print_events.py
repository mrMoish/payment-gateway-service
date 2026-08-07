"""Print an operation's event history compactly, reading the API JSON from stdin."""

from __future__ import annotations

import json
import sys

payload = json.load(sys.stdin)
for event in payload["events"]:
    print(
        f"  {event['eventId']} {event['type']:<30} "
        f"{event['fromStatus']} -> {event['toStatus']}"
    )
