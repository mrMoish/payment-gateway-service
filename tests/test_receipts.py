"""Receipt handling: the only authority on the final status."""

from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_receipt_completes_the_operation(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    provider_payment_id = harness.provider.payments["operation-123"]

    response = await harness.receipt("operation-123", provider_payment_id)

    assert response.status_code == 204
    operation = await harness.get("operation-123")
    assert operation["status"] == "COMPLETED"
    assert operation["providerPaymentId"] == provider_payment_id


async def test_receipt_rejects_the_operation(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")

    response = await harness.receipt(
        "operation-123", harness.provider.payments["operation-123"], result="REJECTED"
    )

    assert response.status_code == 204
    assert (await harness.get("operation-123"))["status"] == "REJECTED"


async def test_a_provider_202_alone_never_reaches_a_final_status(harness) -> None:
    """The synchronous provider response proves nothing about the outcome."""
    await harness.create("operation-123")
    await harness.submit("operation-123")

    await harness.wait_for_status_field("operation-123", "providerPaymentId")

    operation = await harness.get("operation-123")
    assert operation["status"] == "PROCESSING"
    assert operation["providerPaymentId"] is not None
    types = await harness.event_types("operation-123")
    assert "PROVIDER_ACCEPTED" in types
    assert "COMPLETED" not in types and "REJECTED" not in types


async def test_replayed_receipt_returns_204_without_a_second_transition(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    provider_payment_id = harness.provider.payments["operation-123"]

    first = await harness.receipt("operation-123", provider_payment_id)
    second = await harness.receipt("operation-123", provider_payment_id)
    third = await harness.receipt("operation-123", provider_payment_id)

    assert [first.status_code, second.status_code, third.status_code] == [204, 204, 204]
    types = await harness.event_types("operation-123")
    assert types.count("COMPLETED") == 1, "a replay must not add a second transition"
    assert (await harness.get("operation-123"))["status"] == "COMPLETED"


async def test_concurrent_identical_receipts_transition_once(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    provider_payment_id = harness.provider.payments["operation-123"]

    responses = await asyncio.gather(
        *(harness.receipt("operation-123", provider_payment_id) for _ in range(12))
    )

    assert {response.status_code for response in responses} == {204}
    types = await harness.event_types("operation-123")
    assert types.count("COMPLETED") == 1


async def test_late_conflicting_receipt_is_ignored_and_recorded(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    provider_payment_id = harness.provider.payments["operation-123"]
    await harness.receipt("operation-123", provider_payment_id, result="COMPLETED")

    response = await harness.receipt("operation-123", provider_payment_id, result="REJECTED")

    assert response.status_code == 204
    assert (await harness.get("operation-123"))["status"] == "COMPLETED"
    types = await harness.event_types("operation-123")
    assert "RECEIPT_IGNORED_ALREADY_FINAL" in types
    assert types.count("REJECTED") == 0


async def test_early_receipt_binds_the_provider_payment_id(harness) -> None:
    """A receipt that overtakes the provider's HTTP response sets the binding."""
    await harness.create("operation-early")
    await harness.submit("operation-early")

    # No provider call has happened yet: the worker never ran.
    assert (await harness.get("operation-early"))["providerPaymentId"] is None

    response = await harness.receipt("operation-early", "payment-from-callback")

    assert response.status_code == 204
    operation = await harness.get("operation-early")
    assert operation["status"] == "COMPLETED"
    assert operation["providerPaymentId"] == "payment-from-callback"


async def test_mismatching_provider_payment_id_returns_409(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")

    response = await harness.receipt("operation-123", "a-different-payment")

    assert response.status_code == 409
    # The established binding and status are untouched by the rejected receipt.
    operation = await harness.get("operation-123")
    assert operation["providerPaymentId"] == harness.provider.payments["operation-123"]
    assert operation["status"] == "PROCESSING"


async def test_mismatching_receipt_after_a_final_status_returns_409(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    await harness.receipt("operation-123", harness.provider.payments["operation-123"])

    response = await harness.receipt("operation-123", "some-other-payment")

    assert response.status_code == 409
    assert (await harness.get("operation-123"))["status"] == "COMPLETED"


async def test_receipt_on_a_created_operation_is_applied(harness) -> None:
    """A receipt is authoritative even with no recorded submit intent."""
    await harness.create("operation-no-intent")

    response = await harness.receipt("operation-no-intent", "payment-x")

    assert response.status_code == 204
    assert (await harness.get("operation-no-intent"))["status"] == "COMPLETED"
    assert "RECEIPT_APPLIED_WITHOUT_INTENT" in await harness.event_types("operation-no-intent")


async def test_a_settled_operation_is_never_sent_to_the_provider_again(harness) -> None:
    """An early receipt cancels the pending submission entirely."""
    await harness.create("operation-early")
    await harness.submit("operation-early")
    await harness.receipt("operation-early", "payment-from-callback")

    await harness.run_worker_briefly(0.3)

    assert harness.provider.call_count("operation-early") == 0
    assert (await harness.get("operation-early"))["status"] == "COMPLETED"


async def test_late_provider_202_does_not_roll_back_a_final_status(harness) -> None:
    """The provider answers slowly; the receipt wins. Status must stay final.

    This is the anti-pattern the spec calls out explicitly: a late provider
    response must never move a settled operation back to PROCESSING.
    """
    harness.provider.latency = 0.35
    completed = asyncio.Event()

    async def settle_before_the_response(operation_id: str, provider_payment_id: str) -> None:
        await harness.receipt(operation_id, provider_payment_id)
        completed.set()

    harness.provider.on_payment = settle_before_the_response

    await harness.create("operation-late-202")
    await harness.submit("operation-late-202")

    worker = harness.worker
    worker.start()
    try:
        await asyncio.wait_for(completed.wait(), timeout=5)
        assert (await harness.get("operation-late-202"))["status"] == "COMPLETED"
        # Let the provider's 202 land after the receipt has settled the operation.
        await asyncio.sleep(0.5)
    finally:
        await worker.stop()

    operation = await harness.get("operation-late-202")
    assert operation["status"] == "COMPLETED", "a late 202 must not reopen a settled operation"
    types = await harness.event_types("operation-late-202")
    assert types[-1] != "PROVIDER_ACCEPTED" or "COMPLETED" in types
    assert harness.provider.payment_count("operation-late-202") == 1
