"""Background submission worker and startup recovery.

Shape of one cycle:

1. claim a batch of actionable intents (short write transaction);
2. call the provider **outside any transaction** — this is what lets a callback
   receipt land before, or during, the provider's HTTP response;
3. record the outcome in another short transaction.

The operation is never locked for the duration of the external call. Safety does
not come from mutual exclusion but from the intent's immutable Idempotency-Key:
if a lease expires, a process dies mid-call, or a response is lost, the next
attempt replays a byte-identical request and the provider returns the original
``providerPaymentId`` rather than creating a second payment.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
from typing import Final

from .config import Settings
from .domain import EventType, IntentState
from .provider import ProviderClient, ProviderError
from .storage import LeasedIntent, ProviderPaymentIdMismatch, Storage

logger = logging.getLogger(__name__)

#: Metrics counter names.
PROVIDER_RETRIES: Final[str] = "provider_retries"


class SubmissionWorker:
    """Polls for submission intents and drives them to a provider acceptance."""

    def __init__(
        self,
        *,
        storage: Storage,
        provider: ProviderClient,
        settings: Settings,
        rng: random.Random | None = None,
    ) -> None:
        self._storage = storage
        self._provider = provider
        self._settings = settings
        self._rng = rng or random.Random()
        self._owner = f"{settings.service_name}-{os.getpid()}"
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        #: In-flight attempt tasks, awaited on shutdown so nothing is lost.
        self._in_flight: set[asyncio.Task[None]] = set()
        self._wakeup = asyncio.Event()

    # --- lifecycle -----------------------------------------------------------

    def recover(self) -> list[str]:
        """Resume unfinished PROCESSING operations; always the same key."""
        resumed = self._storage.recover_intents()
        if resumed:
            logger.info(
                "Recovered unfinished submission intents after restart",
                extra={"resumedCount": len(resumed), "operationIds": resumed},
            )
        else:
            logger.info("No unfinished submission intents to recover")
        return resumed

    def start(self) -> None:
        if self._task is not None:  # pragma: no cover - guarded by the app
            return
        self._task = asyncio.create_task(self._run(), name="submission-worker")

    def notify(self) -> None:
        """Wake the worker immediately (a fresh intent was just recorded)."""
        self._wakeup.set()

    async def stop(self) -> None:
        """Graceful shutdown: stop claiming, let in-flight attempts finish.

        An attempt that does not finish within the grace period is cancelled. Its
        intent keeps its lease until it expires, after which it is reclaimed —
        with the same Idempotency-Key, so no intent is lost and none is
        duplicated as a second payment.
        """
        self._stopping.set()
        self._wakeup.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        if self._in_flight:
            logger.info(
                "Waiting for in-flight provider attempts to finish",
                extra={"inFlight": len(self._in_flight)},
            )
            done, pending = await asyncio.wait(
                self._in_flight, timeout=self._settings.shutdown_grace_seconds
            )
            for task in pending:
                task.cancel()
            if pending:
                logger.warning(
                    "Cancelled provider attempts that exceeded the shutdown grace period; "
                    "they will be retried with the same Idempotency-Key",
                    extra={"cancelled": len(pending)},
                )
                await asyncio.gather(*pending, return_exceptions=True)

    # --- main loop -----------------------------------------------------------

    async def _run(self) -> None:
        logger.info("Submission worker started", extra={"owner": self._owner})
        try:
            while not self._stopping.is_set():
                try:
                    claimed = await asyncio.to_thread(
                        self._storage.claim_intents,
                        owner=self._owner,
                        limit=self._settings.worker_batch_size,
                        lease_seconds=self._settings.worker_lease_seconds,
                    )
                except Exception:
                    logger.exception("Failed to claim submission intents")
                    claimed = []

                for intent in claimed:
                    task = asyncio.create_task(
                        self._process(intent), name=f"submit:{intent.operation_id}"
                    )
                    self._in_flight.add(task)
                    task.add_done_callback(self._in_flight.discard)

                if claimed:
                    continue  # more may be due; poll again without sleeping

                await self._sleep_until_next_poll()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise
        finally:
            logger.info("Submission worker stopped", extra={"owner": self._owner})

    async def _sleep_until_next_poll(self) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                self._wakeup.wait(), timeout=self._settings.worker_poll_interval_seconds
            )
        self._wakeup.clear()

    # --- one intent ----------------------------------------------------------

    async def _process(self, intent: LeasedIntent) -> None:
        """Run one bounded attempt cycle for a single intent."""
        policy = self._provider.policy
        log_context = {
            "operationId": intent.operation_id,
            "correlationId": intent.operation_id,
            "idempotencyKey": intent.idempotency_key,
        }
        last_error = "no attempt was made"
        retries = 0

        for attempt in range(1, policy.max_attempts + 1):
            if self._stopping.is_set() and attempt > 1:
                # Do not start a *new* attempt while shutting down; the intent is
                # released below and resumed after restart with the same key.
                last_error = "shutdown requested before retry"
                break
            try:
                accepted = await self._provider.submit_payment(
                    idempotency_key=intent.idempotency_key,
                    request_body=intent.request_body,
                    attempt=attempt,
                )
            except ProviderError as exc:
                last_error = str(exc)
                await asyncio.to_thread(
                    self._storage.bump_provider_stats, calls=1, retries=1 if attempt > 1 else 0
                )
                if attempt > 1:
                    retries += 1
                if not exc.retryable:
                    await self._block(intent, str(exc), log_context)
                    return
                if attempt == policy.max_attempts:
                    break
                delay = policy.delay_for(attempt, self._rng)
                logger.info(
                    "Retrying provider call after backoff",
                    extra={
                        **log_context,
                        "attempt": attempt,
                        "nextAttempt": attempt + 1,
                        "backoffSeconds": round(delay, 3),
                    },
                )
                if await self._interruptible_sleep(delay):
                    last_error = "shutdown requested during backoff"
                    break
                continue

            await asyncio.to_thread(
                self._storage.bump_provider_stats, calls=1, retries=1 if attempt > 1 else 0
            )
            if attempt > 1:
                retries += 1
            await self._record_acceptance(intent, accepted.provider_payment_id, log_context)
            return

        # Attempt budget exhausted (or shutdown): stay PROCESSING and retry later.
        # We must not conclude anything about the payment — the provider may have
        # accepted it and the receipt may still arrive.
        logger.warning(
            "Provider attempt cycle exhausted; operation stays PROCESSING",
            extra={**log_context, "attempts": policy.max_attempts, "retries": retries,
                   "lastError": last_error[:500]},
        )
        await asyncio.to_thread(
            self._storage.release_intent,
            intent.operation_id,
            state=IntentState.PENDING,
            error=last_error,
            retry_after_seconds=self._settings.worker_retry_delay_seconds,
            event_type=EventType.PROVIDER_ATTEMPT_FAILED,
            message=(
                "Provider attempt cycle failed; will retry with the same Idempotency-Key: "
                f"{last_error[:200]}"
            ),
        )

    async def _record_acceptance(
        self, intent: LeasedIntent, provider_payment_id: str, log_context: dict[str, object]
    ) -> None:
        """Persist the 202 outcome: bind the id, never set a final status."""
        try:
            bound, operation = await asyncio.to_thread(
                self._storage.bind_provider_payment_id,
                intent.operation_id,
                provider_payment_id,
                source="provider_response",
            )
        except ProviderPaymentIdMismatch as exc:
            # The receipt already bound a different payment. Stop calling and
            # leave the recorded state alone; a human needs to look at this.
            logger.error(
                "Provider response contradicts the bound providerPaymentId",
                extra={
                    **log_context,
                    "boundProviderPaymentId": exc.bound,
                    "providerPaymentId": exc.incoming,
                },
            )
            await asyncio.to_thread(self._storage.mark_intent_accepted, intent.operation_id)
            return

        logger.info(
            "Submission intent accepted by the provider; awaiting receipt",
            extra={
                **log_context,
                "providerPaymentId": provider_payment_id,
                "status": operation.status,
                "boundNow": bound,
            },
        )
        await asyncio.to_thread(self._storage.mark_intent_accepted, intent.operation_id)

    async def _block(
        self, intent: LeasedIntent, error: str, log_context: dict[str, object]
    ) -> None:
        """A non-retryable provider refusal: back off long, stay PROCESSING."""
        logger.error(
            "Provider refused the request; operation stays PROCESSING",
            extra={**log_context, "lastError": error[:500]},
        )
        await asyncio.to_thread(
            self._storage.release_intent,
            intent.operation_id,
            state=IntentState.BLOCKED,
            error=error,
            retry_after_seconds=self._settings.worker_blocked_retry_seconds,
            event_type=EventType.PROVIDER_REQUEST_REJECTED,
            message=f"Provider refused the request: {error[:200]}",
        )

    async def _interruptible_sleep(self, delay: float) -> bool:
        """Sleep, returning ``True`` if shutdown interrupted the wait."""
        if delay <= 0:
            return self._stopping.is_set()
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=delay)
        except TimeoutError:
            return False
        return True
