"""FastAPI application: the API contract, wiring, and lifecycle.

Route handlers stay thin — each one delegates to a single atomic storage call and
translates the outcome into the mandated status code. No handler performs a
provider HTTP call, so no request holds a lock across external I/O.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Final, TypeVar

import httpx
from fastapi import Depends, FastAPI, Path, Request, Response, status
from fastapi.responses import JSONResponse

from .config import Settings, load_settings
from .domain import OperationStatus
from .logging_setup import configure_logging
from .provider import ProviderClient, RetryPolicy
from .schemas import (
    CreateOperationRequest,
    ErrorResponse,
    EventResponse,
    EventsResponse,
    HealthResponse,
    OperationResponse,
    ReceiptRequest,
)
from .storage import (
    OperationAlreadyExists,
    OperationNotFound,
    ProviderPaymentIdMismatch,
    Storage,
)
from .worker import SubmissionWorker

logger = logging.getLogger(__name__)

OperationId = Annotated[str, Path(min_length=1, max_length=200)]

_T = TypeVar("_T")


async def run_in_thread(func: Callable[..., _T], /, *args: object, **kwargs: object) -> _T:
    """Run a blocking storage call off the event loop."""
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))

_ERROR_RESPONSES: Final[dict[int | str, dict[str, object]]] = {
    404: {"model": ErrorResponse, "description": "Operation not found"},
    409: {"model": ErrorResponse, "description": "Conflict"},
    422: {"model": ErrorResponse, "description": "Validation error"},
}


def build_storage(settings: Settings) -> Storage:
    storage = Storage(settings.database_path, busy_timeout_ms=settings.sqlite_busy_timeout_ms)
    storage.migrate()
    return storage


def create_app(
    settings: Settings | None = None,
    *,
    storage: Storage | None = None,
    provider_client: ProviderClient | None = None,
    rng: random.Random | None = None,
) -> FastAPI:
    """Build the application. Injectable pieces keep the tests hermetic."""
    settings = settings or load_settings()
    configure_logging(settings.log_level, settings.service_name)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = settings
        app.state.storage = storage or build_storage(settings)

        owned_http_client: httpx.AsyncClient | None = None
        client = provider_client
        if client is None:
            owned_http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(settings.provider_timeout_seconds)
            )
            client = ProviderClient(
                payments_url=settings.payments_url,
                client=owned_http_client,
                policy=RetryPolicy(
                    max_attempts=settings.provider_max_attempts,
                    base_seconds=settings.provider_backoff_base_seconds,
                    max_seconds=settings.provider_backoff_max_seconds,
                    jitter=settings.provider_backoff_jitter,
                ),
            )
        app.state.provider = client

        worker = SubmissionWorker(
            storage=app.state.storage, provider=client, settings=settings, rng=rng
        )
        app.state.worker = worker

        logger.info(
            "Service starting",
            extra={
                "providerUrl": settings.provider_url,
                "databasePath": settings.database_path,
                "workerEnabled": settings.worker_enabled,
            },
        )
        # Recovery runs before the worker so a restart resumes unfinished
        # PROCESSING operations with their original Idempotency-Key.
        worker.recover()
        if settings.worker_enabled:
            worker.start()

        try:
            yield
        finally:
            await worker.stop()
            if owned_http_client is not None:
                await owned_http_client.aclose()
            app.state.storage.close()
            logger.info("Service stopped")

    app = FastAPI(
        title="Payment gateway service",
        version="1.0.0",
        summary="Durable payment submission with callback-confirmed final status",
        lifespan=lifespan,
    )
    _register_exception_handlers(app)
    _register_routes(app)
    return app


# --- dependencies -----------------------------------------------------------


def get_storage(request: Request) -> Storage:
    return request.app.state.storage


def get_worker(request: Request) -> SubmissionWorker:
    return request.app.state.worker


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


StorageDep = Annotated[Storage, Depends(get_storage)]
WorkerDep = Annotated[SubmissionWorker, Depends(get_worker)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


# --- error handling ---------------------------------------------------------


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(OperationNotFound)
    async def _not_found(request: Request, exc: OperationNotFound) -> JSONResponse:
        operation_id = str(exc.args[0]) if exc.args else None
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorResponse(
                error="operation_not_found",
                message=f"Operation {operation_id} does not exist",
                operationId=operation_id,
            ).model_dump(),
        )

    @app.exception_handler(OperationAlreadyExists)
    async def _already_exists(request: Request, exc: OperationAlreadyExists) -> JSONResponse:
        operation_id = str(exc.args[0]) if exc.args else None
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=ErrorResponse(
                error="operation_already_exists",
                message=f"Operation {operation_id} already exists",
                operationId=operation_id,
            ).model_dump(),
        )

    @app.exception_handler(ProviderPaymentIdMismatch)
    async def _mismatch(request: Request, exc: ProviderPaymentIdMismatch) -> JSONResponse:
        logger.warning(
            "Receipt rejected: providerPaymentId mismatch",
            extra={
                "operationId": exc.operation_id,
                "correlationId": exc.operation_id,
                "boundProviderPaymentId": exc.bound,
                "providerPaymentId": exc.incoming,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=ErrorResponse(
                error="provider_payment_id_mismatch",
                message=str(exc),
                operationId=exc.operation_id,
            ).model_dump(),
        )


# --- routes -----------------------------------------------------------------


def _register_routes(app: FastAPI) -> None:
    @app.get("/health", response_model=HealthResponse, summary="Readiness probe")
    async def health(storage: StorageDep, settings: SettingsDep) -> HealthResponse:
        storage.health_check()
        return HealthResponse(status="ok", service=settings.service_name)

    @app.post(
        "/operations",
        status_code=status.HTTP_201_CREATED,
        response_model=OperationResponse,
        responses=_ERROR_RESPONSES,
        summary="Create an operation",
    )
    async def create_operation(
        payload: CreateOperationRequest, storage: StorageDep
    ) -> OperationResponse:
        operation = await run_in_thread(
            storage.create_operation,
            payload.operationId,
            payload.amount,
            payload.currency,
            payload.description,
        )
        logger.info(
            "Operation created",
            extra={
                "operationId": operation.operation_id,
                "correlationId": operation.operation_id,
                "amount": operation.amount,
                "currency": operation.currency,
            },
        )
        return OperationResponse.from_operation(operation)

    @app.post(
        "/operations/{operation_id}/submit",
        response_model=OperationResponse,
        responses=_ERROR_RESPONSES,
        summary="Durably schedule the provider submission",
    )
    async def submit_operation(
        operation_id: OperationId,
        response: Response,
        storage: StorageDep,
        worker: WorkerDep,
    ) -> OperationResponse:
        """202 when this call recorded the intent, 200 when it already existed.

        The provider call happens afterwards, in the worker — this handler holds
        no lock across external I/O, so a receipt may legitimately arrive before
        the provider's HTTP response.
        """
        outcome = await run_in_thread(storage.record_submit_intent, operation_id)

        if outcome.intent_created:
            response.status_code = status.HTTP_202_ACCEPTED
            logger.info(
                "Submission intent recorded",
                extra={
                    "operationId": operation_id,
                    "correlationId": operation_id,
                    "idempotencyKey": operation_id,
                    "status": outcome.operation.status,
                },
            )
            worker.notify()
        else:
            response.status_code = status.HTTP_200_OK
            logger.info(
                "Submit is a no-op; returning the stored state",
                extra={
                    "operationId": operation_id,
                    "correlationId": operation_id,
                    "status": outcome.operation.status,
                    "providerPaymentId": outcome.operation.provider_payment_id,
                },
            )
        return OperationResponse.from_operation(outcome.operation)

    @app.post(
        "/receipts",
        status_code=status.HTTP_204_NO_CONTENT,
        responses=_ERROR_RESPONSES,
        summary="Accept a provider callback receipt",
    )
    async def accept_receipt(payload: ReceiptRequest, storage: StorageDep) -> Response:
        """The single authority on the final status.

        Always 204 unless the receipt contradicts an established
        ``providerPaymentId`` binding (409) or names an unknown operation (404).
        """
        outcome = await run_in_thread(
            storage.apply_receipt,
            operation_id=payload.operationId,
            provider_payment_id=payload.providerPaymentId,
            result=str(payload.result),
            message=payload.message,
            occurred_at=payload.occurredAt,
        )
        logger.info(
            "Receipt processed",
            extra={
                "operationId": payload.operationId,
                "correlationId": payload.operationId,
                "providerPaymentId": payload.providerPaymentId,
                "receiptResult": str(payload.result),
                "applied": outcome.applied,
                "duplicate": outcome.duplicate,
                "reason": outcome.reason,
                "status": outcome.operation.status,
            },
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get(
        "/operations/{operation_id}",
        response_model=OperationResponse,
        responses=_ERROR_RESPONSES,
        summary="Current operation state",
    )
    async def get_operation(operation_id: OperationId, storage: StorageDep) -> OperationResponse:
        operation = await run_in_thread(storage.get_operation, operation_id)
        if operation is None:
            raise OperationNotFound(operation_id)
        return OperationResponse.from_operation(operation)

    @app.get(
        "/operations/{operation_id}/events",
        response_model=EventsResponse,
        responses=_ERROR_RESPONSES,
        summary="Transition history",
    )
    async def get_events(operation_id: OperationId, storage: StorageDep) -> EventsResponse:
        operation = await run_in_thread(storage.get_operation, operation_id)
        if operation is None:
            raise OperationNotFound(operation_id)
        events = await run_in_thread(storage.list_events, operation_id)
        return EventsResponse(
            operationId=operation_id,
            events=[EventResponse.from_event(event) for event in events],
        )

    @app.get("/metrics", summary="Operational metrics")
    async def metrics(storage: StorageDep) -> dict[str, object]:
        """Unfinished (PROCESSING) operations and provider retry counts."""
        statuses = await run_in_thread(storage.count_operations_by_status)
        intents = await run_in_thread(storage.count_intents_by_state)
        stats = await run_in_thread(storage.provider_stats)
        return {
            "operationsProcessing": statuses[str(OperationStatus.PROCESSING)],
            "operationsByStatus": statuses,
            "submissionIntentsByState": intents,
            "providerCallsTotal": stats["total_calls"],
            "providerRetriesTotal": stats["total_retries"],
        }

    @app.get("/metrics/prometheus", response_class=Response, summary="Metrics in Prometheus format")
    async def metrics_prometheus(storage: StorageDep) -> Response:
        statuses = await run_in_thread(storage.count_operations_by_status)
        stats = await run_in_thread(storage.provider_stats)
        lines = [
            "# HELP payment_operations_processing Operations awaiting a provider receipt.",
            "# TYPE payment_operations_processing gauge",
            f"payment_operations_processing {statuses[str(OperationStatus.PROCESSING)]}",
            "# HELP payment_operations_total Operations by status.",
            "# TYPE payment_operations_total gauge",
            *(
                f'payment_operations_total{{status="{name}"}} {count}'
                for name, count in sorted(statuses.items())
            ),
            "# HELP payment_provider_calls_total Provider HTTP attempts.",
            "# TYPE payment_provider_calls_total counter",
            f"payment_provider_calls_total {stats['total_calls']}",
            "# HELP payment_provider_retries_total Provider HTTP retries.",
            "# TYPE payment_provider_retries_total counter",
            f"payment_provider_retries_total {stats['total_retries']}",
        ]
        return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


app = create_app()
