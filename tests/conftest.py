"""Test harness: a controllable in-process provider plus app factories.

The fake provider is a real ``httpx`` transport, so the client's headers, body
bytes, retry and backoff paths are all exercised. It also enforces genuine
idempotency: one payment per ``Idempotency-Key``, no matter how many times the
same key arrives.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager

from app.config import Settings
from app.main import create_app
from app.provider import ProviderClient, RetryPolicy
from app.storage import Storage


@dataclass
class RecordedCall:
    idempotency_key: str
    correlation_id: str | None
    body: str
    attempt_index: int


@dataclass
class FakeProvider:
    """A provider simulator with scriptable failures.

    ``payments`` maps Idempotency-Key -> providerPaymentId; its size is the
    number of payments actually created, which is what the invariant tests
    assert on.
    """

    payments: dict[str, str] = field(default_factory=dict)
    calls: list[RecordedCall] = field(default_factory=list)
    #: Number of upcoming attempts that answer 503.
    fail_with_503: int = 0
    #: Number of upcoming attempts that raise a network error.
    fail_with_network_error: int = 0
    #: Number of upcoming attempts that time out.
    fail_with_timeout: int = 0
    #: Answer 400 (non-retryable) this many times.
    fail_with_400: int = 0
    #: Drop the response after registering the payment ("lost response").
    lose_response: int = 0
    #: Extra latency before answering, in seconds.
    latency: float = 0.0
    #: Called with (operation_id, provider_payment_id) once a payment exists.
    on_payment: Callable[[str, str], Awaitable[None]] | None = None

    def payment_count(self, operation_id: str) -> int:
        return 1 if operation_id in self.payments else 0

    def call_count(self, operation_id: str) -> int:
        return sum(1 for call in self.calls if call.idempotency_key == operation_id)

    def bodies_for(self, operation_id: str) -> set[str]:
        return {call.body for call in self.calls if call.idempotency_key == operation_id}

    async def handle(self, request: httpx.Request) -> httpx.Response:
        key = request.headers["Idempotency-Key"]
        body = request.content.decode()
        self.calls.append(
            RecordedCall(
                idempotency_key=key,
                correlation_id=request.headers.get("X-Correlation-ID"),
                body=body,
                attempt_index=self.call_count(key) + 1,
            )
        )

        if self.fail_with_timeout > 0:
            self.fail_with_timeout -= 1
            raise httpx.ReadTimeout("simulated timeout", request=request)
        if self.fail_with_network_error > 0:
            self.fail_with_network_error -= 1
            raise httpx.ConnectError("simulated connection failure", request=request)
        if self.fail_with_503 > 0:
            self.fail_with_503 -= 1
            return httpx.Response(503, json={"error": "service unavailable"})
        if self.fail_with_400 > 0:
            self.fail_with_400 -= 1
            return httpx.Response(400, json={"error": "bad request"})

        # Idempotent: the same key always maps to the same payment.
        provider_payment_id = self.payments.setdefault(key, str(uuid.uuid4()))
        if self.on_payment is not None:
            await self.on_payment(key, provider_payment_id)
        if self.latency:
            await asyncio.sleep(self.latency)

        if self.lose_response > 0:
            # The payment exists on the provider side, but we never learn its id.
            self.lose_response -= 1
            raise httpx.ReadTimeout("response lost after the payment was accepted", request=request)

        return httpx.Response(
            202, json={"providerPaymentId": provider_payment_id, "status": "ACCEPTED"}
        )


def make_settings(database_path: str, **overrides: Any) -> Settings:
    defaults: dict[str, Any] = {
        "provider_url": "http://provider-simulator:8081",
        "database_path": database_path,
        "provider_max_attempts": 3,
        "provider_backoff_base_seconds": 0.001,
        "provider_backoff_max_seconds": 0.005,
        "provider_backoff_jitter": 1.0,
        "worker_enabled": False,
        "worker_poll_interval_seconds": 0.02,
        "worker_lease_seconds": 30.0,
        "worker_retry_delay_seconds": 0.02,
        "worker_blocked_retry_seconds": 0.05,
        "shutdown_grace_seconds": 2.0,
        "log_level": "CRITICAL",
    }
    return Settings(**(defaults | overrides))


def make_provider_client(
    provider: FakeProvider, settings: Settings
) -> tuple[ProviderClient, httpx.AsyncClient]:
    """Build a client over a MockTransport; the http client is returned to close."""
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(provider.handle),
        timeout=settings.provider_timeout_seconds,
    )
    client = ProviderClient(
        payments_url=settings.payments_url,
        client=http_client,
        policy=RetryPolicy(
            max_attempts=settings.provider_max_attempts,
            base_seconds=settings.provider_backoff_base_seconds,
            max_seconds=settings.provider_backoff_max_seconds,
            jitter=settings.provider_backoff_jitter,
        ),
    )
    return client, http_client


@pytest.fixture
def database_path(tmp_path) -> str:
    return str(tmp_path / "payments.db")


@pytest.fixture
def provider() -> FakeProvider:
    return FakeProvider()


@pytest.fixture
def storage(database_path: str) -> Storage:
    store = Storage(database_path)
    store.migrate()
    return store


@dataclass
class AppHarness:
    """A running app instance plus the handles the tests need."""

    client: httpx.AsyncClient
    app: Any
    provider: FakeProvider
    settings: Settings

    @property
    def storage(self) -> Storage:
        return self.app.state.storage

    @property
    def worker(self):
        return self.app.state.worker

    async def create(self, operation_id: str, amount: str = "1000.00", **extra: Any):
        payload = {"operationId": operation_id, "amount": amount, "currency": "RUB"} | extra
        return await self.client.post("/operations", json=payload)

    async def submit(self, operation_id: str):
        return await self.client.post(f"/operations/{operation_id}/submit")

    async def receipt(
        self,
        operation_id: str,
        provider_payment_id: str,
        result: str = "COMPLETED",
        message: str = "Payment completed",
    ):
        return await self.client.post(
            "/receipts",
            json={
                "providerPaymentId": provider_payment_id,
                "operationId": operation_id,
                "result": result,
                "message": message,
                "occurredAt": "2026-07-15T12:00:00Z",
            },
        )

    async def get(self, operation_id: str) -> dict[str, Any]:
        response = await self.client.get(f"/operations/{operation_id}")
        response.raise_for_status()
        return response.json()

    async def events(self, operation_id: str) -> list[dict[str, Any]]:
        response = await self.client.get(f"/operations/{operation_id}/events")
        response.raise_for_status()
        return response.json()["events"]

    async def event_types(self, operation_id: str) -> list[str]:
        return [event["type"] for event in await self.events(operation_id)]

    async def metrics(self) -> dict[str, Any]:
        response = await self.client.get("/metrics")
        response.raise_for_status()
        return response.json()

    async def run_worker_until(
        self, predicate: Callable[[], Awaitable[bool]], timeout: float = 5.0
    ) -> None:
        """Drive the worker until ``predicate`` holds, then stop it."""
        worker = self.worker
        worker.start()
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            while asyncio.get_running_loop().time() < deadline:
                if await predicate():
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("condition was not reached before the timeout")
        finally:
            await worker.stop()

    async def wait_for_status(self, operation_id: str, status: str, timeout: float = 5.0) -> None:
        async def reached() -> bool:
            return (await self.get(operation_id))["status"] == status

        await self.run_worker_until(reached, timeout=timeout)

    async def wait_for_status_field(
        self, operation_id: str, field_name: str, timeout: float = 5.0
    ) -> None:
        """Drive the worker until ``field_name`` on the operation is non-null."""

        async def reached() -> bool:
            return (await self.get(operation_id))[field_name] is not None

        await self.run_worker_until(reached, timeout=timeout)

    async def run_worker_briefly(self, duration: float = 0.2) -> None:
        """Let the worker run for a while — used to assert nothing happens."""
        worker = self.worker
        worker.start()
        try:
            await asyncio.sleep(duration)
        finally:
            await worker.stop()


@pytest_asyncio.fixture
async def harness(
    database_path: str, provider: FakeProvider
) -> AsyncIterator[AppHarness]:
    settings = make_settings(database_path)
    async for built in _build_harness(settings, provider):
        yield built


async def _build_harness(
    settings: Settings, provider: FakeProvider
) -> AsyncIterator[AppHarness]:
    provider_client, http_client = make_provider_client(provider, settings)
    app = create_app(
        settings,
        provider_client=provider_client,
        rng=random.Random(1234),  # deterministic jitter
    )
    try:
        async with LifespanManager(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
                timeout=10,
            ) as client:
                yield AppHarness(
                    client=client, app=app, provider=provider, settings=settings
                )
    finally:
        await http_client.aclose()


@pytest.fixture
def build_harness():
    """Factory for tests that need a custom Settings or a restart."""
    return _build_harness
