"""The mandated API contract: routes, status codes, payloads, validation."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_health_returns_200(harness) -> None:
    response = await harness.client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_create_operation_returns_201_with_the_mandated_fields(harness) -> None:
    response = await harness.create("operation-123", amount="1000.00", description="Оплата заказа")

    assert response.status_code == 201
    body = response.json()
    assert body["operationId"] == "operation-123"
    assert body["amount"] == "1000.00"
    assert body["currency"] == "RUB"
    assert body["description"] == "Оплата заказа"
    assert body["status"] == "CREATED"
    assert body["providerPaymentId"] is None


async def test_recreating_the_same_operation_id_returns_409(harness) -> None:
    assert (await harness.create("operation-123")).status_code == 201

    response = await harness.create("operation-123", amount="99.00")

    assert response.status_code == 409
    # The original operation is untouched by the rejected duplicate.
    assert (await harness.get("operation-123"))["amount"] == "1000.00"


@pytest.mark.parametrize(
    "amount",
    ["-5.00", "0", "0.00", "1.234", "abc", "", " ", "1e3", "+5.00", "1.", ".5"],
)
async def test_invalid_amount_is_rejected(harness, amount: str) -> None:
    response = await harness.create("operation-invalid", amount=amount)
    assert response.status_code == 422


async def test_numeric_amount_is_rejected(harness) -> None:
    response = await harness.client.post(
        "/operations",
        json={"operationId": "operation-numeric", "amount": 1000.00, "currency": "RUB"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("currency", ["USD", "EUR", "XXX", "RU", "RUBB"])
async def test_unsupported_currency_is_rejected(harness, currency: str) -> None:
    response = await harness.client.post(
        "/operations",
        json={"operationId": "operation-currency", "amount": "1.00", "currency": currency},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("currency", ["RUB", "rub", " rub "])
async def test_currency_is_normalised_to_upper_case(harness, currency: str) -> None:
    response = await harness.client.post(
        "/operations",
        json={"operationId": f"operation-{currency.strip()}", "amount": "1.00", "currency": currency},
    )
    assert response.status_code == 201
    assert response.json()["currency"] == "RUB"


async def test_missing_operation_id_is_rejected(harness) -> None:
    response = await harness.client.post(
        "/operations", json={"amount": "1.00", "currency": "RUB"}
    )
    assert response.status_code == 422


async def test_amount_two_decimals_are_preserved_verbatim(harness) -> None:
    await harness.create("operation-amount", amount="1000.50")
    assert (await harness.get("operation-amount"))["amount"] == "1000.50"


async def test_unknown_operation_returns_404_everywhere(harness) -> None:
    assert (await harness.client.get("/operations/ghost")).status_code == 404
    assert (await harness.client.post("/operations/ghost/submit")).status_code == 404
    assert (await harness.client.get("/operations/ghost/events")).status_code == 404


async def test_receipt_for_unknown_operation_returns_404(harness) -> None:
    response = await harness.receipt("ghost", "payment-1")
    assert response.status_code == 404


@pytest.mark.parametrize("result", ["PENDING", "completed", "", "OK"])
async def test_receipt_with_an_invalid_result_is_rejected(harness, result: str) -> None:
    await harness.create("operation-bad-result")
    response = await harness.receipt("operation-bad-result", "payment-1", result=result)
    assert response.status_code == 422


async def test_events_start_with_a_created_event(harness) -> None:
    await harness.create("operation-events")

    events = await harness.events("operation-events")

    assert len(events) == 1
    assert events[0] == {
        "eventId": 1,
        "type": "CREATED",
        "fromStatus": None,
        "toStatus": "CREATED",
        "message": "Operation created",
        "occurredAt": events[0]["occurredAt"],
    }


async def test_event_ids_increase_monotonically_within_an_operation(harness) -> None:
    await harness.create("operation-seq")
    await harness.submit("operation-seq")
    await harness.wait_for_status("operation-seq", "PROCESSING")
    await harness.receipt("operation-seq", harness.provider.payments["operation-seq"])

    ids = [event["eventId"] for event in await harness.events("operation-seq")]

    assert ids == sorted(ids)
    assert ids == list(range(1, len(ids) + 1))


async def test_event_ids_are_scoped_per_operation(harness) -> None:
    await harness.create("operation-a")
    await harness.create("operation-b")

    # Both histories start at 1 — ids are per-operation, not global.
    assert (await harness.events("operation-a"))[0]["eventId"] == 1
    assert (await harness.events("operation-b"))[0]["eventId"] == 1
