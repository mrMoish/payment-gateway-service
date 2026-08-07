"""Restart recovery and graceful shutdown.

A "restart" here means tearing the whole app down (lifespan exit) and building a
new one against the *same database file* — exactly what `docker compose up` does
after `docker compose down` with the volume intact.
"""

from __future__ import annotations

import asyncio

import pytest

from app.domain import IntentState
from app.storage import Storage

from .conftest import FakeProvider, make_settings

pytestmark = pytest.mark.asyncio


async def test_data_survives_a_restart(build_harness, database_path: str) -> None:
    provider = FakeProvider()
    settings = make_settings(database_path)

    async for first in build_harness(settings, provider):
        await first.create("operation-persist", amount="1234.56", description="Оплата заказа")
        await first.submit("operation-persist")
        await first.wait_for_status_field("operation-persist", "providerPaymentId")
        provider_payment_id = provider.payments["operation-persist"]
        events_before = await first.events("operation-persist")

    # New process, same volume.
    async for second in build_harness(settings, provider):
        operation = await second.get("operation-persist")
        assert operation["amount"] == "1234.56"
        assert operation["description"] == "Оплата заказа"
        assert operation["status"] == "PROCESSING"
        assert operation["providerPaymentId"] == provider_payment_id

        # History is preserved and only appended to.
        events_after = await second.events("operation-persist")
        assert [event["eventId"] for event in events_before] == [
            event["eventId"] for event in events_before
        ]
        assert len(events_after) >= len(events_before)
        assert events_after[: len(events_before)] == events_before


async def test_restart_resumes_processing_with_the_same_idempotency_key(
    build_harness, database_path: str
) -> None:
    """The provider was unreachable before the restart; the retry reuses the key."""
    provider = FakeProvider()
    settings = make_settings(database_path)

    async for first in build_harness(settings, provider):
        provider.fail_with_503 = 99
        await first.create("operation-resume")
        await first.submit("operation-resume")

        async def attempted() -> bool:
            return provider.call_count("operation-resume") >= 1

        await first.run_worker_until(attempted)
        calls_before = provider.call_count("operation-resume")
        assert (await first.get("operation-resume"))["status"] == "PROCESSING"

    provider.fail_with_503 = 0  # the provider recovers while we are down

    async for second in build_harness(settings, provider):
        await second.wait_for_status_field("operation-resume", "providerPaymentId")

        assert provider.call_count("operation-resume") > calls_before, "submission must resume"
        assert provider.payment_count("operation-resume") == 1, "still exactly one payment"
        # Every attempt, before and after the restart, used the same key and body.
        assert {call.idempotency_key for call in provider.calls} == {"operation-resume"}
        assert len(provider.bodies_for("operation-resume")) == 1
        assert "RECOVERY_INTENT_RESCHEDULED" in await second.event_types("operation-resume")


async def test_a_crash_between_acceptance_and_persistence_creates_no_second_payment(
    build_harness, database_path: str
) -> None:
    """The spec's sharpest case.

    The provider accepts the payment, then the process dies before storing the
    id. On restart the intent is still PENDING, so it is retried — with the
    original Idempotency-Key, so the provider returns the *same* payment.
    """
    provider = FakeProvider()
    settings = make_settings(database_path)

    async for first in build_harness(settings, provider):
        # The response is lost after the provider registered the payment: from
        # our side this is indistinguishable from dying mid-call.
        provider.lose_response = 99
        await first.create("operation-crash")
        await first.submit("operation-crash")

        async def payment_registered() -> bool:
            return "operation-crash" in provider.payments

        await first.run_worker_until(payment_registered)
        payment_id_at_provider = provider.payments["operation-crash"]
        # We never learned the id.
        assert (await first.get("operation-crash"))["providerPaymentId"] is None

    provider.lose_response = 0  # responses work again after the restart

    async for second in build_harness(settings, provider):
        await second.wait_for_status_field("operation-crash", "providerPaymentId")

        assert provider.payment_count("operation-crash") == 1, "no second payment after the crash"
        operation = await second.get("operation-crash")
        assert operation["providerPaymentId"] == payment_id_at_provider, (
            "the retry must resolve to the payment created before the crash"
        )
        assert operation["status"] == "PROCESSING"

        # And the receipt for that pre-crash payment settles the operation.
        assert (await second.receipt("operation-crash", payment_id_at_provider)).status_code == 204
        assert (await second.get("operation-crash"))["status"] == "COMPLETED"


async def test_recovery_reclaims_a_lease_held_by_a_dead_process(
    build_harness, database_path: str
) -> None:
    """An intent stuck IN_FLIGHT by a killed process must be picked up again."""
    storage = Storage(database_path)
    storage.migrate()
    storage.create_operation("operation-stuck", "10.00", "RUB", None)
    storage.record_submit_intent("operation-stuck")
    # Simulate a process that claimed the intent with a long lease and then died.
    claimed = storage.claim_intents(owner="dead-process", limit=10, lease_seconds=3600)
    assert len(claimed) == 1
    assert storage.count_intents_by_state()[str(IntentState.IN_FLIGHT)] == 1

    provider = FakeProvider()
    async for harness in build_harness(make_settings(database_path), provider):
        await harness.wait_for_status_field("operation-stuck", "providerPaymentId")

        assert provider.payment_count("operation-stuck") == 1
        assert provider.calls[0].idempotency_key == "operation-stuck"


async def test_recovery_rebuilds_a_missing_intent_row(
    build_harness, database_path: str
) -> None:
    """A PROCESSING operation with no intent row is repaired, not abandoned."""
    storage = Storage(database_path)
    storage.migrate()
    storage.create_operation("operation-orphan", "42.00", "RUB", None)
    storage.record_submit_intent("operation-orphan")
    # Simulate a torn write: the operation is PROCESSING, the outbox row is gone.
    with storage._txn() as conn:  # noqa: SLF001 - deliberate corruption for the test
        conn.execute("DELETE FROM submission_intents WHERE operation_id = ?", ("operation-orphan",))

    provider = FakeProvider()
    async for harness in build_harness(make_settings(database_path), provider):
        await harness.wait_for_status_field("operation-orphan", "providerPaymentId")

        assert provider.payment_count("operation-orphan") == 1
        assert provider.calls[0].idempotency_key == "operation-orphan", "same key, rebuilt intent"
        assert "RECOVERY_INTENT_REPAIRED" in await harness.event_types("operation-orphan")


async def test_settled_operations_are_not_resubmitted_after_a_restart(
    build_harness, database_path: str
) -> None:
    provider = FakeProvider()
    settings = make_settings(database_path)

    async for first in build_harness(settings, provider):
        await first.create("operation-settled")
        await first.submit("operation-settled")
        await first.wait_for_status_field("operation-settled", "providerPaymentId")
        await first.receipt("operation-settled", provider.payments["operation-settled"])
        assert (await first.get("operation-settled"))["status"] == "COMPLETED"

    calls_before = provider.call_count("operation-settled")

    async for second in build_harness(settings, provider):
        await second.run_worker_briefly(0.3)

        assert provider.call_count("operation-settled") == calls_before, "no call for a settled op"
        assert (await second.get("operation-settled"))["status"] == "COMPLETED"


async def test_created_operations_are_not_submitted_after_a_restart(
    build_harness, database_path: str
) -> None:
    """Never submitted means never sent — recovery must not invent intents."""
    provider = FakeProvider()
    settings = make_settings(database_path)

    async for first in build_harness(settings, provider):
        await first.create("operation-untouched")

    async for second in build_harness(settings, provider):
        await second.run_worker_briefly(0.3)

        assert provider.call_count("operation-untouched") == 0
        assert (await second.get("operation-untouched"))["status"] == "CREATED"


async def test_many_operations_recover_with_one_payment_each(
    build_harness, database_path: str
) -> None:
    provider = FakeProvider()
    settings = make_settings(database_path)
    operation_ids = [f"operation-bulk-{index}" for index in range(12)]

    async for first in build_harness(settings, provider):
        provider.fail_with_503 = 999  # nothing gets through before the restart
        for operation_id in operation_ids:
            await first.create(operation_id)
            await first.submit(operation_id)

        async def some_attempts_made() -> bool:
            return len(provider.calls) >= len(operation_ids)

        await first.run_worker_until(some_attempts_made, timeout=10)

    provider.fail_with_503 = 0

    async for second in build_harness(settings, provider):
        async def all_bound() -> bool:
            for operation_id in operation_ids:
                if (await second.get(operation_id))["providerPaymentId"] is None:
                    return False
            return True

        await second.run_worker_until(all_bound, timeout=15)

        assert len(provider.payments) == len(operation_ids), "one payment per operation"
        for operation_id in operation_ids:
            assert provider.payment_count(operation_id) == 1
            assert len(provider.bodies_for(operation_id)) == 1


async def test_graceful_shutdown_waits_for_in_flight_attempts(
    build_harness, database_path: str
) -> None:
    """A shutdown mid-call must not lose the intent nor duplicate the payment."""
    provider = FakeProvider()
    provider.latency = 0.3
    settings = make_settings(database_path, shutdown_grace_seconds=3.0)

    async for harness in build_harness(settings, provider):
        await harness.create("operation-shutdown")
        await harness.submit("operation-shutdown")

        worker = harness.worker
        worker.start()
        # Let the attempt start, then stop the worker while the call is in flight.
        async def call_started() -> bool:
            return provider.call_count("operation-shutdown") >= 1

        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline and not await call_started():
            await asyncio.sleep(0.01)

        await worker.stop()  # graceful: awaits the in-flight attempt

        # The in-flight attempt completed and was persisted before we returned.
        operation = await harness.get("operation-shutdown")
        assert operation["providerPaymentId"] is not None, "the in-flight result was not lost"
        assert operation["status"] == "PROCESSING"
        assert provider.payment_count("operation-shutdown") == 1


async def test_shutdown_then_restart_creates_no_duplicate_payment(
    build_harness, database_path: str
) -> None:
    provider = FakeProvider()
    provider.latency = 0.2
    settings = make_settings(database_path, shutdown_grace_seconds=0.0)

    async for first in build_harness(settings, provider):
        await first.create("operation-abrupt")
        await first.submit("operation-abrupt")
        worker = first.worker
        worker.start()
        await asyncio.sleep(0.05)  # stop mid-call, with no grace period
        await worker.stop()

    async for second in build_harness(settings, provider):
        await second.wait_for_status_field("operation-abrupt", "providerPaymentId")

        assert provider.payment_count("operation-abrupt") == 1, "one payment despite the abort"
