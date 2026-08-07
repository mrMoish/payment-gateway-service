"""Request/response models. The API contract is fixed and must not drift."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Final

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .domain import SUPPORTED_CURRENCIES, OperationStatus, ReceiptResult
from .storage import Event, Operation

#: A positive decimal string with at most two fractional digits.
_AMOUNT_RE: Final[re.Pattern[str]] = re.compile(r"^\d+(\.\d{1,2})?$")


class CreateOperationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    operationId: Annotated[str, Field(min_length=1, max_length=200)]
    amount: Annotated[str, Field(min_length=1, max_length=40)]
    currency: Annotated[str, Field(min_length=3, max_length=3)]
    description: Annotated[str | None, Field(default=None, max_length=1000)] = None

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_amount(cls, value: Any) -> Any:
        # Numeric JSON literals would lose the "decimal string" guarantee, so
        # only strings are accepted; this keeps the error message clear.
        if isinstance(value, (int, float, Decimal)):
            raise ValueError("amount must be a decimal string, e.g. \"1000.00\"")
        return value

    @field_validator("amount")
    @classmethod
    def _validate_amount(cls, value: str) -> str:
        if not _AMOUNT_RE.fullmatch(value):
            raise ValueError(
                "amount must be a positive decimal string with at most 2 decimal places"
            )
        try:
            amount = Decimal(value)
        except InvalidOperation as exc:  # pragma: no cover - regex already filters
            raise ValueError("amount is not a valid decimal") from exc
        if amount <= 0:
            raise ValueError("amount must be greater than zero")
        return value

    @field_validator("currency")
    @classmethod
    def _validate_currency(cls, value: str) -> str:
        currency = value.upper()
        if currency not in SUPPORTED_CURRENCIES:
            raise ValueError(f"currency must be one of {sorted(SUPPORTED_CURRENCIES)}")
        return currency


class OperationResponse(BaseModel):
    operationId: str
    amount: str
    currency: str
    description: str | None
    status: OperationStatus
    providerPaymentId: str | None
    createdAt: str
    updatedAt: str

    @classmethod
    def from_operation(cls, operation: Operation) -> OperationResponse:
        return cls(
            operationId=operation.operation_id,
            amount=operation.amount,
            currency=operation.currency,
            description=operation.description,
            status=OperationStatus(operation.status),
            providerPaymentId=operation.provider_payment_id,
            createdAt=operation.created_at,
            updatedAt=operation.updated_at,
        )


class EventResponse(BaseModel):
    eventId: int
    type: str
    fromStatus: OperationStatus | None
    toStatus: OperationStatus | None
    message: str | None
    occurredAt: str

    @classmethod
    def from_event(cls, event: Event) -> EventResponse:
        return cls(
            eventId=event.event_id,
            type=event.type,
            fromStatus=OperationStatus(event.from_status) if event.from_status else None,
            toStatus=OperationStatus(event.to_status) if event.to_status else None,
            message=event.message,
            occurredAt=event.occurred_at,
        )


class EventsResponse(BaseModel):
    operationId: str
    events: list[EventResponse]


class ReceiptRequest(BaseModel):
    """Callback receipt from the provider — the only source of a final status."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    providerPaymentId: Annotated[str, Field(min_length=1, max_length=200)]
    operationId: Annotated[str, Field(min_length=1, max_length=200)]
    result: ReceiptResult
    message: Annotated[str | None, Field(default=None, max_length=1000)] = None
    occurredAt: Annotated[str | None, Field(default=None, max_length=64)] = None


class HealthResponse(BaseModel):
    status: str
    service: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    operationId: str | None = None
