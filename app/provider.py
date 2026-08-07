"""Provider client: fixed Idempotency-Key, bounded retry, backoff + jitter.

The contract this client is built around:

* ``Idempotency-Key`` equals ``operationId`` and is *never* regenerated —
  neither on retry nor after a restart. The request body is passed in verbatim
  from storage, so the provider sees byte-identical repeats and returns the same
  ``providerPaymentId`` instead of creating a second payment.
* ``503`` and network/timeout errors are retryable within a bounded number of
  attempts.
* A ``202`` yields ``providerPaymentId`` and ``status: ACCEPTED``. That is a
  *fact*, not a verdict: the caller stores the id and leaves the operation
  PROCESSING.
* Exhausting the attempt budget is reported as an error, never as a decision
  about the payment — the provider may already have accepted it.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Any, Final

import httpx

logger = logging.getLogger(__name__)

#: The provider may answer 200/201 instead of 202 for an idempotent replay.
_ACCEPT_STATUSES: Final[frozenset[int]] = frozenset({200, 201, 202})


class ProviderError(Exception):
    """A single attempt failed. ``retryable`` drives the retry decision."""

    def __init__(self, message: str, *, retryable: bool, status_code: int | None = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ProviderAccepted:
    """The provider acknowledged the payment request."""

    provider_payment_id: str
    status: str
    status_code: int
    attempts: int


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_seconds: float
    max_seconds: float
    jitter: float

    def delay_for(self, attempt: int, rng: random.Random) -> float:
        """Exponential backoff capped at ``max_seconds``, with jitter.

        ``attempt`` is 1-based. With ``jitter == 1.0`` this is full jitter:
        a uniform draw from ``[0, backoff]``, which spreads a thundering herd of
        recovered intents instead of replaying them in lockstep.
        """
        backoff = min(self.base_seconds * (2 ** (attempt - 1)), self.max_seconds)
        if self.jitter <= 0:
            return backoff
        spread = min(self.jitter, 1.0) * backoff
        return max(0.0, backoff - spread + rng.random() * spread)


class ProviderClient:
    """Thin async client over ``POST {PROVIDER_URL}/payments``."""

    def __init__(
        self,
        *,
        payments_url: str,
        client: httpx.AsyncClient,
        policy: RetryPolicy,
    ) -> None:
        self._payments_url = payments_url
        self._client = client
        self._policy = policy

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    async def submit_payment(
        self, *, idempotency_key: str, request_body: str, attempt: int
    ) -> ProviderAccepted:
        """Perform one attempt. Raises :class:`ProviderError` on failure.

        ``request_body`` is the raw JSON string frozen at intent creation; it is
        sent as-is so repeats are byte-identical.
        """
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
            "X-Correlation-ID": idempotency_key,
        }
        log_context = {
            "operationId": idempotency_key,
            "correlationId": idempotency_key,
            "idempotencyKey": idempotency_key,
            "attempt": attempt,
        }

        try:
            response = await self._client.post(
                self._payments_url,
                content=request_body.encode("utf-8"),
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            # Ambiguous by nature: the provider may have accepted the payment.
            logger.warning(
                "Provider call timed out", extra={**log_context, "errorKind": "timeout"}
            )
            raise ProviderError(f"timeout: {exc!r}", retryable=True) from exc
        except httpx.HTTPError as exc:
            logger.warning(
                "Provider call failed with a network error",
                extra={**log_context, "errorKind": "network"},
            )
            raise ProviderError(f"network error: {exc!r}", retryable=True) from exc

        if response.status_code in _ACCEPT_STATUSES:
            return self._parse_accepted(response, log_context, attempt)

        retryable = response.status_code == 503 or response.status_code >= 500
        logger.warning(
            "Provider returned an error status",
            extra={
                **log_context,
                "providerStatusCode": response.status_code,
                "retryable": retryable,
                "responseBody": response.text[:500],
            },
        )
        raise ProviderError(
            f"provider returned HTTP {response.status_code}: {response.text[:200]}",
            retryable=retryable,
            status_code=response.status_code,
        )

    def _parse_accepted(
        self, response: httpx.Response, log_context: dict[str, Any], attempt: int
    ) -> ProviderAccepted:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProviderError(
                f"provider returned non-JSON body: {response.text[:200]}", retryable=True
            ) from exc
        if not isinstance(payload, dict):
            raise ProviderError(
                f"provider returned unexpected JSON: {response.text[:200]}", retryable=True
            )

        provider_payment_id = payload.get("providerPaymentId") or payload.get("paymentId")
        if not isinstance(provider_payment_id, str) or not provider_payment_id:
            # Without an id we cannot bind the payment; retry with the same key.
            raise ProviderError(
                f"provider response has no providerPaymentId: {response.text[:200]}",
                retryable=True,
            )

        status = payload.get("status") or "ACCEPTED"
        logger.info(
            "Provider accepted the payment",
            extra={
                **log_context,
                "providerPaymentId": provider_payment_id,
                "providerStatus": status,
                "providerStatusCode": response.status_code,
            },
        )
        return ProviderAccepted(
            provider_payment_id=provider_payment_id,
            status=str(status),
            status_code=response.status_code,
            attempts=attempt,
        )
