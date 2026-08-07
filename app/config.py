"""Runtime configuration, read from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable process configuration."""

    #: Base URL of the provider; trailing slash stripped.
    provider_url: str = field(
        default_factory=lambda: _env_str("PROVIDER_URL", "http://provider-simulator:8081").rstrip("/")
    )
    #: SQLite file. Must live on a volume to survive container recreation.
    database_path: str = field(default_factory=lambda: _env_str("DATABASE_PATH", "/data/payments.db"))

    # --- provider call policy ------------------------------------------------
    provider_timeout_seconds: float = field(
        default_factory=lambda: _env_float("PROVIDER_TIMEOUT_SECONDS", 5.0, minimum=0.001)
    )
    #: Bounded attempts within a single worker cycle.
    provider_max_attempts: int = field(
        default_factory=lambda: _env_int("PROVIDER_MAX_ATTEMPTS", 4, minimum=1)
    )
    provider_backoff_base_seconds: float = field(
        default_factory=lambda: _env_float("PROVIDER_BACKOFF_BASE_SECONDS", 0.2, minimum=0.0)
    )
    provider_backoff_max_seconds: float = field(
        default_factory=lambda: _env_float("PROVIDER_BACKOFF_MAX_SECONDS", 5.0, minimum=0.0)
    )
    #: Jitter fraction in [0, 1]; 1.0 == full jitter, 0.0 == deterministic.
    provider_backoff_jitter: float = field(
        default_factory=lambda: _env_float("PROVIDER_BACKOFF_JITTER", 1.0, minimum=0.0)
    )

    # --- background worker ---------------------------------------------------
    worker_enabled: bool = field(default_factory=lambda: _env_bool("WORKER_ENABLED", True))
    worker_poll_interval_seconds: float = field(
        default_factory=lambda: _env_float("WORKER_POLL_INTERVAL_SECONDS", 0.5, minimum=0.01)
    )
    worker_batch_size: int = field(default_factory=lambda: _env_int("WORKER_BATCH_SIZE", 16, minimum=1))
    #: Lease duration for a claimed intent. An expired lease (crash/hang) is
    #: reclaimed and retried with the *same* Idempotency-Key.
    worker_lease_seconds: float = field(
        default_factory=lambda: _env_float("WORKER_LEASE_SECONDS", 60.0, minimum=1.0)
    )
    worker_retry_delay_seconds: float = field(
        default_factory=lambda: _env_float("WORKER_RETRY_DELAY_SECONDS", 2.0, minimum=0.0)
    )
    worker_blocked_retry_seconds: float = field(
        default_factory=lambda: _env_float("WORKER_BLOCKED_RETRY_SECONDS", 30.0, minimum=0.0)
    )
    shutdown_grace_seconds: float = field(
        default_factory=lambda: _env_float("SHUTDOWN_GRACE_SECONDS", 10.0, minimum=0.0)
    )

    # --- storage -------------------------------------------------------------
    sqlite_busy_timeout_ms: int = field(
        default_factory=lambda: _env_int("SQLITE_BUSY_TIMEOUT_MS", 10_000, minimum=0)
    )

    # --- logging -------------------------------------------------------------
    log_level: str = field(default_factory=lambda: _env_str("LOG_LEVEL", "INFO").upper())
    service_name: str = field(default_factory=lambda: _env_str("SERVICE_NAME", "candidate-service"))

    @property
    def payments_url(self) -> str:
        return f"{self.provider_url}/payments"


def load_settings() -> Settings:
    return Settings()
