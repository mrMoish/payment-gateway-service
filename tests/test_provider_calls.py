"""Provider call semantics: idempotency key, retries, backoff, lost responses."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.asyncio


async def test_request_carries_the_mandated_headers_and_body(harness) -> None:
    await harness.create("operation-123", amount="1000.00")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")

    call = harness.provider.calls[0]
    assert call.idempotency_key == "operation-123", "Idempotency-Key must equal operationId"
    assert call.correlation_id == "operation-123", "X-Correlation-ID must equal operationId"
    assert json.loads(call.body) == {
        "operationId": "operation-123",
        "amount": "1000.00",
        "currency": "RUB",
    }


async def test_503_is_retried_with_the_same_key_and_an_identical_body(harness) -> None:
    harness.provider.fail_with_503 = 2
    await harness.create("operation-503")
    await harness.submit("operation-503")

    await harness.wait_for_status_field("operation-503", "providerPaymentId")

    assert harness.provider.call_count("operation-503") == 3
    assert harness.provider.payment_count("operation-503") == 1
    assert len(harness.provider.bodies_for("operation-503")) == 1, "the body must never change"
    assert {call.idempotency_key for call in harness.provider.calls} == {"operation-503"}


async def test_network_error_is_retried_and_the_operation_stays_processing(harness) -> None:
    harness.provider.fail_with_network_error = 2
    await harness.create("operation-network")
    await harness.submit("operation-network")

    await harness.wait_for_status_field("operation-network", "providerPaymentId")

    assert harness.provider.call_count("operation-network") == 3
    assert harness.provider.payment_count("operation-network") == 1


async def test_timeout_is_retried_with_the_same_key(harness) -> None:
    harness.provider.fail_with_timeout = 1
    await harness.create("operation-timeout")
    await harness.submit("operation-timeout")

    await harness.wait_for_status_field("operation-timeout", "providerPaymentId")

    assert harness.provider.payment_count("operation-timeout") == 1
    assert len(harness.provider.bodies_for("operation-timeout")) == 1


async def test_exhausted_attempts_leave_the_operation_processing(harness) -> None:
    """We must never decide the payment's fate on our own."""
    harness.provider.fail_with_503 = 99  # every attempt fails
    await harness.create("operation-down")
    await harness.submit("operation-down")

    async def attempted() -> bool:
        return harness.provider.call_count("operation-down") >= 3

    await harness.run_worker_until(attempted)

    operation = await harness.get("operation-down")
    assert operation["status"] == "PROCESSING", "an unreachable provider must not decide the status"
    assert operation["providerPaymentId"] is None
    assert "PROVIDER_ATTEMPT_FAILED" in await harness.event_types("operation-down")


async def test_a_lost_response_after_a_real_acceptance_creates_no_second_payment(harness) -> None:
    """The response is dropped *after* the provider registered the payment.

    The retry reuses the same Idempotency-Key, so the provider returns the
    original payment instead of creating another one.
    """
    harness.provider.lose_response = 1
    await harness.create("operation-lost")
    await harness.submit("operation-lost")

    await harness.wait_for_status_field("operation-lost", "providerPaymentId")

    assert harness.provider.call_count("operation-lost") == 2, "one lost + one successful retry"
    assert harness.provider.payment_count("operation-lost") == 1, "exactly one payment"
    operation = await harness.get("operation-lost")
    assert operation["providerPaymentId"] == harness.provider.payments["operation-lost"]


async def test_repeated_lost_responses_still_create_one_payment(harness) -> None:
    harness.provider.lose_response = 2
    await harness.create("operation-lost-twice")
    await harness.submit("operation-lost-twice")

    await harness.wait_for_status_field("operation-lost-twice", "providerPaymentId")

    assert harness.provider.payment_count("operation-lost-twice") == 1


async def test_lost_response_then_a_receipt_settles_the_operation(harness) -> None:
    """Even if we never learn the id from HTTP, the receipt carries it."""
    harness.provider.lose_response = 99
    await harness.create("operation-only-callback")
    await harness.submit("operation-only-callback")

    async def payment_registered() -> bool:
        return "operation-only-callback" in harness.provider.payments

    await harness.run_worker_until(payment_registered)
    provider_payment_id = harness.provider.payments["operation-only-callback"]

    # The provider's response never arrived; the callback is the only channel.
    assert (await harness.get("operation-only-callback"))["providerPaymentId"] is None
    assert (await harness.receipt("operation-only-callback", provider_payment_id)).status_code == 204

    operation = await harness.get("operation-only-callback")
    assert operation["status"] == "COMPLETED"
    assert operation["providerPaymentId"] == provider_payment_id


async def test_non_retryable_4xx_keeps_the_operation_processing(harness) -> None:
    harness.provider.fail_with_400 = 1
    await harness.create("operation-400")
    await harness.submit("operation-400")

    async def called() -> bool:
        return harness.provider.call_count("operation-400") >= 1

    await harness.run_worker_until(called)

    # A single attempt: 400 is not retried inside the cycle.
    assert harness.provider.call_count("operation-400") == 1
    operation = await harness.get("operation-400")
    assert operation["status"] == "PROCESSING"
    assert "PROVIDER_REQUEST_REJECTED" in await harness.event_types("operation-400")


async def test_metrics_expose_processing_count_and_retries(harness) -> None:
    harness.provider.fail_with_503 = 2
    await harness.create("operation-metrics")
    await harness.submit("operation-metrics")
    await harness.wait_for_status_field("operation-metrics", "providerPaymentId")

    metrics = await harness.metrics()

    assert metrics["operationsProcessing"] == 1
    assert metrics["providerRetriesTotal"] >= 2
    assert metrics["providerCallsTotal"] >= 3

    await harness.receipt("operation-metrics", harness.provider.payments["operation-metrics"])
    assert (await harness.metrics())["operationsProcessing"] == 0


async def test_prometheus_metrics_are_exposed(harness) -> None:
    await harness.create("operation-prom")
    await harness.submit("operation-prom")

    response = await harness.client.get("/metrics/prometheus")

    assert response.status_code == 200
    assert "payment_operations_processing 1" in response.text
    assert "payment_provider_calls_total" in response.text
