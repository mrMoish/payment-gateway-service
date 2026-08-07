"""Domain model: statuses, transitions, event and intent vocabularies.

The single invariant this module encodes:

* an operation reaches a *final* status (COMPLETED / REJECTED) **only** through
  a callback receipt (:func:`status_for_receipt_result`);
* every other actor (the provider's synchronous ``202 Accepted`` response, the
  submission worker, the recovery sweeper) may only ever record facts —
  ``providerPaymentId``, attempts, errors — never a status transition into or
  out of a final state.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final


class OperationStatus(StrEnum):
    """Lifecycle of a payment operation."""

    CREATED = "CREATED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


FINAL_STATUSES: Final[frozenset[OperationStatus]] = frozenset(
    {OperationStatus.COMPLETED, OperationStatus.REJECTED}
)


def is_final(status: str) -> bool:
    return status in FINAL_STATUSES


class ReceiptResult(StrEnum):
    """Result carried by a provider callback receipt."""

    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


def status_for_receipt_result(result: str) -> OperationStatus:
    """The *only* function in the codebase that maps anything to a final status."""
    return {
        ReceiptResult.COMPLETED: OperationStatus.COMPLETED,
        ReceiptResult.REJECTED: OperationStatus.REJECTED,
    }[ReceiptResult(result)]


class IntentState(StrEnum):
    """State of a durable submission intent (the outbox row).

    ``PENDING``   — needs a provider call (initial state, and the state a lease
                    returns to after a failed or interrupted attempt).
    ``IN_FLIGHT`` — leased by a worker; a provider call may be in progress.
                    Only an efficiency hint: correctness never depends on it,
                    because every attempt reuses the same Idempotency-Key.
    ``ACCEPTED``  — provider answered ``202`` and we durably stored the
                    ``providerPaymentId``. No further calls are needed; we wait
                    for the receipt.
    ``SETTLED``   — a receipt fixed the final status; nothing left to do.
    ``BLOCKED``   — provider rejected the request itself (non-retryable 4xx).
                    Retried on a long cycle; the operation stays PROCESSING
                    because only the provider may decide the payment's fate.
    """

    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    ACCEPTED = "ACCEPTED"
    SETTLED = "SETTLED"
    BLOCKED = "BLOCKED"


#: Intent states that still require the worker to act.
ACTIONABLE_INTENT_STATES: Final[tuple[str, ...]] = (
    IntentState.PENDING,
    IntentState.IN_FLIGHT,
    IntentState.BLOCKED,
)


class EventType(StrEnum):
    """Types recorded in the per-operation event history."""

    CREATED = "CREATED"
    SUBMIT_INTENT_RECORDED = "SUBMIT_INTENT_RECORDED"
    PROVIDER_ATTEMPT_FAILED = "PROVIDER_ATTEMPT_FAILED"
    PROVIDER_ACCEPTED = "PROVIDER_ACCEPTED"
    PROVIDER_PAYMENT_ID_MISMATCH = "PROVIDER_PAYMENT_ID_MISMATCH"
    PROVIDER_REQUEST_REJECTED = "PROVIDER_REQUEST_REJECTED"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    RECEIPT_IGNORED_ALREADY_FINAL = "RECEIPT_IGNORED_ALREADY_FINAL"
    RECEIPT_APPLIED_WITHOUT_INTENT = "RECEIPT_APPLIED_WITHOUT_INTENT"
    RECOVERY_INTENT_RESCHEDULED = "RECOVERY_INTENT_RESCHEDULED"
    RECOVERY_INTENT_REPAIRED = "RECOVERY_INTENT_REPAIRED"


SUPPORTED_CURRENCIES: Final[frozenset[str]] = frozenset({"RUB"})
