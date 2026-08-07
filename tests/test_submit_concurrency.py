"""Concurrent and repeated submits must never produce a second payment."""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from app.storage import Storage

pytestmark = pytest.mark.asyncio


async def test_first_submit_returns_202_and_moves_to_processing(harness) -> None:
    await harness.create("operation-123")

    response = await harness.submit("operation-123")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "PROCESSING"
    assert "SUBMIT_INTENT_RECORDED" in await harness.event_types("operation-123")


async def test_thirty_concurrent_submits_create_exactly_one_intent(harness) -> None:
    await harness.create("operation-race")

    responses = await asyncio.gather(
        *(harness.submit("operation-race") for _ in range(30))
    )

    codes = Counter(response.status_code for response in responses)
    assert codes[202] == 1, f"exactly one submit must record the intent, got {codes}"
    assert codes[200] == 29
    # Every loser reports the already-stored state, never CREATED.
    assert {response.json()["status"] for response in responses} == {"PROCESSING"}


async def test_concurrent_submits_lead_to_exactly_one_provider_payment(harness) -> None:
    await harness.create("operation-race")

    await asyncio.gather(*(harness.submit("operation-race") for _ in range(30)))
    await harness.wait_for_status_field("operation-race", "providerPaymentId")

    assert harness.provider.payment_count("operation-race") == 1
    assert harness.provider.call_count("operation-race") == 1


async def test_concurrent_submits_record_a_single_transition_event(harness) -> None:
    await harness.create("operation-race")

    await asyncio.gather(*(harness.submit("operation-race") for _ in range(30)))

    types = await harness.event_types("operation-race")
    assert types.count("SUBMIT_INTENT_RECORDED") == 1


async def test_repeated_submit_while_processing_returns_200_without_a_new_intent(harness) -> None:
    await harness.create("operation-123")
    assert (await harness.submit("operation-123")).status_code == 202

    second = await harness.submit("operation-123")

    assert second.status_code == 200
    assert second.json()["status"] == "PROCESSING"
    assert (await harness.event_types("operation-123")).count("SUBMIT_INTENT_RECORDED") == 1


async def test_repeated_submit_after_a_final_status_returns_200_and_keeps_it(harness) -> None:
    await harness.create("operation-123")
    await harness.submit("operation-123")
    await harness.wait_for_status_field("operation-123", "providerPaymentId")
    await harness.receipt("operation-123", harness.provider.payments["operation-123"])

    response = await harness.submit("operation-123")

    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETED"
    # No new provider call may follow a submit on a settled operation.
    await harness.run_worker_briefly()
    assert harness.provider.call_count("operation-123") == 1


async def test_submit_on_a_rejected_operation_does_not_resend(harness) -> None:
    await harness.create("operation-rejected")
    await harness.submit("operation-rejected")
    await harness.wait_for_status_field("operation-rejected", "providerPaymentId")
    await harness.receipt(
        "operation-rejected", harness.provider.payments["operation-rejected"], result="REJECTED"
    )

    response = await harness.submit("operation-rejected")

    assert response.status_code == 200
    assert response.json()["status"] == "REJECTED"
    await harness.run_worker_briefly()
    assert harness.provider.call_count("operation-rejected") == 1


async def test_parallel_submits_across_processes_still_yield_one_intent(
    harness, database_path: str
) -> None:
    """Two independent Storage handles race, as two processes on one volume would."""
    await harness.create("operation-multiproc")

    def submit(_: int) -> bool:
        return Storage(database_path).record_submit_intent("operation-multiproc").intent_created

    winners = await asyncio.gather(*(asyncio.to_thread(submit, i) for i in range(16)))

    assert sum(winners) == 1


async def test_submit_does_not_hold_a_lock_during_the_provider_call(harness) -> None:
    """A receipt must be processable while the provider call is still in flight.

    The provider stalls; meanwhile the receipt arrives and settles the operation.
    If submit held a lock across the external call, this would deadlock or block.
    """
    harness.provider.latency = 0.4

    async def send_receipt_mid_flight(operation_id: str, provider_payment_id: str) -> None:
        response = await harness.receipt(operation_id, provider_payment_id)
        assert response.status_code == 204

    harness.provider.on_payment = send_receipt_mid_flight

    await harness.create("operation-inflight")
    await harness.submit("operation-inflight")

    await harness.wait_for_status("operation-inflight", "COMPLETED", timeout=5.0)
    assert harness.provider.payment_count("operation-inflight") == 1
