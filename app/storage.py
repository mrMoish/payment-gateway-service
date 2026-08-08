"""Durable storage: schema and the atomic state transitions.

Every rule protecting the main invariant is a database constraint or a
conditional UPDATE (compare-and-set), not an application-level check:

* ``submission_intents.operation_id`` is the PRIMARY KEY — at most one intent
  per operation can exist;
* ``idempotency_key`` and ``request_body`` are written once and never updated,
  so every retry and every restart replays the identical provider request;
* ``CREATED -> PROCESSING`` is a CAS, so exactly one concurrent submit wins;
* a final status is written by exactly one function, :meth:`Storage.apply_receipt`,
  guarded by ``WHERE status NOT IN (final)`` — a rollback out of a final status
  is impossible;
* ``events.(operation_id, seq)`` is UNIQUE with ``seq`` allocated inside the
  writing transaction, so event ids are monotonic per operation.

No transaction in this module spans a provider HTTP call.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from .domain import (
    ACTIONABLE_INTENT_STATES,
    FINAL_STATUSES,
    EventType,
    IntentState,
    OperationStatus,
    is_final,
    status_for_receipt_result,
)

SCHEMA_VERSION: Final[int] = 1

_FINAL: Final[tuple[str, ...]] = tuple(sorted(str(s) for s in FINAL_STATUSES))
_FINAL_Q: Final[str] = ", ".join("?" * len(_FINAL))
_ACTIONABLE: Final[tuple[str, ...]] = tuple(str(s) for s in ACTIONABLE_INTENT_STATES)
_ACTIONABLE_Q: Final[str] = ", ".join("?" * len(_ACTIONABLE))

_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS operations (
    operation_id        TEXT    PRIMARY KEY,
    amount              TEXT    NOT NULL,
    currency            TEXT    NOT NULL,
    description         TEXT,
    status              TEXT    NOT NULL
        CHECK (status IN ('CREATED', 'PROCESSING', 'COMPLETED', 'REJECTED')),
    provider_payment_id TEXT,
    created_at          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL
);

-- Outbox. PRIMARY KEY on operation_id => at most one intent per operation.
CREATE TABLE IF NOT EXISTS submission_intents (
    operation_id     TEXT    PRIMARY KEY
        REFERENCES operations (operation_id) ON DELETE CASCADE,
    idempotency_key  TEXT    NOT NULL UNIQUE,   -- == operationId, never updated
    request_body     TEXT    NOT NULL,          -- frozen at creation, replayed verbatim
    state            TEXT    NOT NULL
        CHECK (state IN ('PENDING', 'IN_FLIGHT', 'ACCEPTED', 'SETTLED', 'BLOCKED')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TEXT    NOT NULL,
    lease_owner      TEXT,
    lease_expires_at TEXT,
    last_error       TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_claimable
    ON submission_intents (state, next_attempt_at);

CREATE TABLE IF NOT EXISTS events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT    NOT NULL
        REFERENCES operations (operation_id) ON DELETE CASCADE,
    seq          INTEGER NOT NULL,              -- monotonic per operation
    type         TEXT    NOT NULL,
    from_status  TEXT,
    to_status    TEXT,
    message      TEXT,
    details      TEXT,
    occurred_at  TEXT    NOT NULL,
    UNIQUE (operation_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_operation ON events (operation_id, seq);

-- Receipt de-duplication: a replayed receipt must not transition twice.
CREATE TABLE IF NOT EXISTS receipts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id        TEXT    NOT NULL
        REFERENCES operations (operation_id) ON DELETE CASCADE,
    provider_payment_id TEXT    NOT NULL,
    result              TEXT    NOT NULL,
    message             TEXT,
    occurred_at         TEXT,
    outcome             TEXT    NOT NULL,
    received_at         TEXT    NOT NULL,
    UNIQUE (operation_id, provider_payment_id, result)
);

CREATE TABLE IF NOT EXISTS provider_stats (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    total_calls   INTEGER NOT NULL DEFAULT 0,
    total_retries INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO provider_stats (id, total_calls, total_retries) VALUES (1, 0, 0);
"""


def utcnow() -> datetime:
    return datetime.now(UTC)


def to_iso(moment: datetime) -> str:
    """RFC3339 with a ``Z`` suffix, millisecond precision, sortable as text."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def canonical_request_body(operation_id: str, amount: str, currency: str) -> str:
    """Provider request body — byte-stable across every retry."""
    # Field order is fixed by the literal below; dicts preserve insertion order,
    # so the serialised bytes are identical on every replay.
    return json.dumps(
        {"operationId": operation_id, "amount": amount, "currency": currency},
        separators=(",", ":"),
        ensure_ascii=False,
    )


class StorageError(RuntimeError):
    pass


class OperationAlreadyExists(StorageError):
    pass


class OperationNotFound(StorageError):
    pass


class ProviderPaymentIdMismatch(StorageError):
    """A receipt referenced a different payment than the one already bound."""

    def __init__(self, operation_id: str, bound: str, incoming: str) -> None:
        super().__init__(
            f"operation {operation_id} is bound to {bound}, receipt carried {incoming}"
        )
        self.operation_id = operation_id
        self.bound = bound
        self.incoming = incoming


@dataclass(frozen=True, slots=True)
class Operation:
    operation_id: str
    amount: str
    currency: str
    description: str | None
    status: str
    provider_payment_id: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Operation:
        return cls(
            operation_id=row["operation_id"],
            amount=row["amount"],
            currency=row["currency"],
            description=row["description"],
            status=row["status"],
            provider_payment_id=row["provider_payment_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True, slots=True)
class Event:
    event_id: int
    type: str
    from_status: str | None
    to_status: str | None
    message: str | None
    occurred_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Event:
        return cls(
            event_id=row["seq"],
            type=row["type"],
            from_status=row["from_status"],
            to_status=row["to_status"],
            message=row["message"],
            occurred_at=row["occurred_at"],
        )


@dataclass(frozen=True, slots=True)
class LeasedIntent:
    """An intent claimed for one provider attempt cycle."""

    operation_id: str
    idempotency_key: str
    request_body: str
    attempts: int


@dataclass(frozen=True, slots=True)
class SubmitOutcome:
    operation: Operation
    intent_created: bool


@dataclass(frozen=True, slots=True)
class ReceiptOutcome:
    operation: Operation
    applied: bool
    duplicate: bool
    reason: str


class Storage:
    """Thread-safe SQLite storage: one connection per thread."""

    def __init__(self, database_path: str, *, busy_timeout_ms: int = 10_000) -> None:
        self._database_path = database_path
        self._busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        # Every connection this instance opens, so close() can release all of
        # them and not just the calling thread's. Writers run on the shared
        # asyncio.to_thread pool, whose threads outlive the instance, so a
        # thread-local-only close leaks one connection (three fds under WAL)
        # per pool thread per instance.
        self._lock = threading.Lock()
        self._connections: list[sqlite3.Connection] = []
        self._closed = False

    @property
    def database_path(self) -> str:
        return self._database_path

    # --- connections ---------------------------------------------------------

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._closed:
            # close() already shut this handle down on another thread; reusing
            # it would raise an opaque ProgrammingError deep inside a query.
            self._local.conn = None
            raise RuntimeError("Storage is closed")
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            if self._database_path != ":memory:":
                parent = os.path.dirname(os.path.abspath(self._database_path))
                if parent:
                    os.makedirs(parent, exist_ok=True)
            conn = sqlite3.connect(
                self._database_path,
                timeout=self._busy_timeout_ms / 1000,
                isolation_level=None,  # explicit transaction control
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL")
            # FULL: fsync per commit. The design assumes a returned commit is on
            # disk; we do not trade that for throughput.
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            with self._lock:
                if self._closed:
                    conn.close()
                    raise RuntimeError("Storage is closed")
                self._connections.append(conn)
            self._local.conn = conn
        return conn

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        """IMMEDIATE write transaction — the write lock is taken up front."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def migrate(self) -> None:
        # executescript() issues its own COMMIT, so it must not run inside an
        # explicit transaction. Every statement in _SCHEMA is idempotent.
        conn = self._conn
        conn.executescript(_SCHEMA)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def close(self) -> None:
        """Close every connection this instance opened, on any thread.

        Idempotent. Threads other than the caller may still hold a reference in
        their thread-local slot; ``_closed`` stops them from reusing it.
        """
        with self._lock:
            self._closed = True
            connections, self._connections = self._connections, []
        for conn in connections:
            try:
                conn.close()
            except sqlite3.Error:
                # A connection wedged mid-statement must not strand the rest.
                pass
        self._local.conn = None

    def health_check(self) -> None:
        self._conn.execute("SELECT 1 FROM operations LIMIT 1").fetchone()

    # --- helpers -------------------------------------------------------------

    @staticmethod
    def _append_event(
        conn: sqlite3.Connection,
        operation_id: str,
        event_type: str,
        *,
        from_status: str | None,
        to_status: str | None,
        message: str,
        details: dict[str, Any] | None = None,
        occurred_at: str | None = None,
    ) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM events WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        seq = int(row["max_seq"]) + 1
        conn.execute(
            """
            INSERT INTO events (operation_id, seq, type, from_status, to_status,
                                message, details, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                seq,
                str(event_type),
                from_status,
                to_status,
                message,
                json.dumps(details, ensure_ascii=False) if details else None,
                occurred_at or to_iso(utcnow()),
            ),
        )
        return seq

    @staticmethod
    def _fetch(conn: sqlite3.Connection, operation_id: str) -> Operation | None:
        row = conn.execute(
            "SELECT * FROM operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        return Operation.from_row(row) if row else None

    @staticmethod
    def _require(conn: sqlite3.Connection, operation_id: str) -> Operation:
        operation = Storage._fetch(conn, operation_id)
        if operation is None:
            raise OperationNotFound(operation_id)
        return operation

    # --- reads ---------------------------------------------------------------

    def get_operation(self, operation_id: str) -> Operation | None:
        return self._fetch(self._conn, operation_id)

    def list_events(self, operation_id: str) -> list[Event]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE operation_id = ? ORDER BY seq", (operation_id,)
        ).fetchall()
        return [Event.from_row(row) for row in rows]

    # --- create / submit -----------------------------------------------------

    def create_operation(
        self, operation_id: str, amount: str, currency: str, description: str | None
    ) -> Operation:
        now = to_iso(utcnow())
        try:
            with self._txn() as conn:
                conn.execute(
                    """
                    INSERT INTO operations (operation_id, amount, currency, description,
                                            status, provider_payment_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (operation_id, amount, currency, description,
                     str(OperationStatus.CREATED), now, now),
                )
                self._append_event(
                    conn,
                    operation_id,
                    EventType.CREATED,
                    from_status=None,
                    to_status=str(OperationStatus.CREATED),
                    message="Operation created",
                    occurred_at=now,
                )
                return self._require(conn, operation_id)
        except sqlite3.IntegrityError as exc:
            raise OperationAlreadyExists(operation_id) from exc

    def record_submit_intent(self, operation_id: str) -> SubmitOutcome:
        """Atomically move CREATED -> PROCESSING and persist the submit intent.

        The CAS on ``status = 'CREATED'`` picks exactly one winner among
        concurrent submits; the others read the stored state and report it
        unchanged. The Idempotency-Key and request body are written here once and
        never rewritten — that is what makes a second provider payment
        impossible across retries and restarts.
        """
        now = to_iso(utcnow())
        with self._txn() as conn:
            operation = self._require(conn, operation_id)

            cursor = conn.execute(
                "UPDATE operations SET status = ?, updated_at = ? "
                " WHERE operation_id = ? AND status = ?",
                (str(OperationStatus.PROCESSING), now, operation_id, str(OperationStatus.CREATED)),
            )
            if cursor.rowcount == 0:
                # Already PROCESSING or final: no new intent, current state wins.
                return SubmitOutcome(self._require(conn, operation_id), intent_created=False)

            conn.execute(
                """
                INSERT INTO submission_intents (operation_id, idempotency_key, request_body,
                                                state, attempts, next_attempt_at,
                                                created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    operation_id,
                    operation_id,  # Idempotency-Key == operationId, per contract
                    canonical_request_body(operation_id, operation.amount, operation.currency),
                    str(IntentState.PENDING),
                    now,
                    now,
                    now,
                ),
            )
            self._append_event(
                conn,
                operation_id,
                EventType.SUBMIT_INTENT_RECORDED,
                from_status=str(OperationStatus.CREATED),
                to_status=str(OperationStatus.PROCESSING),
                message="Submission intent durably recorded; awaiting provider receipt",
                details={"idempotencyKey": operation_id},
                occurred_at=now,
            )
            return SubmitOutcome(self._require(conn, operation_id), intent_created=True)

    # --- intent lifecycle (worker) -------------------------------------------

    def claim_intents(self, *, owner: str, limit: int, lease_seconds: float) -> list[LeasedIntent]:
        """Lease actionable intents. Short transaction; no HTTP inside.

        A lease only prevents duplicate *concurrent attempts*. If it expires
        while a call is genuinely in flight, the retry still carries the same
        Idempotency-Key, so the provider de-duplicates — not us.
        """
        now = utcnow()
        now_iso = to_iso(now)
        lease_until = to_iso(now + timedelta(seconds=lease_seconds))
        claimed: list[LeasedIntent] = []

        with self._txn() as conn:
            rows = conn.execute(
                f"""
                SELECT i.* FROM submission_intents AS i
                  JOIN operations AS o USING (operation_id)
                 WHERE i.state IN ({_ACTIONABLE_Q})
                   AND i.next_attempt_at <= ?
                   AND (i.lease_expires_at IS NULL OR i.lease_expires_at <= ?)
                   AND o.status NOT IN ({_FINAL_Q})
                 ORDER BY i.next_attempt_at
                 LIMIT ?
                """,
                (*_ACTIONABLE, now_iso, now_iso, *_FINAL, limit),
            ).fetchall()

            for row in rows:
                cursor = conn.execute(
                    """
                    UPDATE submission_intents
                       SET state = ?, lease_owner = ?, lease_expires_at = ?,
                           attempts = attempts + 1, updated_at = ?
                     WHERE operation_id = ? AND state = ?
                       AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                    """,
                    (str(IntentState.IN_FLIGHT), owner, lease_until, now_iso,
                     row["operation_id"], row["state"], now_iso),
                )
                if cursor.rowcount == 1:
                    claimed.append(
                        LeasedIntent(
                            operation_id=row["operation_id"],
                            idempotency_key=row["idempotency_key"],
                            request_body=row["request_body"],
                            attempts=int(row["attempts"]) + 1,
                        )
                    )
        return claimed

    def bind_provider_payment_id(
        self, operation_id: str, provider_payment_id: str, *, source: str
    ) -> tuple[bool, Operation]:
        """Store ``providerPaymentId`` without ever touching ``status``.

        This is the entire handling of a provider 202: record a fact, leave the
        operation PROCESSING. A late 202 therefore cannot roll back a final
        status that a receipt already fixed.
        """
        now = to_iso(utcnow())
        with self._txn() as conn:
            operation = self._require(conn, operation_id)

            if operation.provider_payment_id == provider_payment_id:
                return False, operation

            if operation.provider_payment_id is not None:
                raise ProviderPaymentIdMismatch(
                    operation_id, operation.provider_payment_id, provider_payment_id
                )

            conn.execute(
                "UPDATE operations SET provider_payment_id = ?, updated_at = ? "
                " WHERE operation_id = ? AND provider_payment_id IS NULL",
                (provider_payment_id, now, operation_id),
            )
            self._append_event(
                conn,
                operation_id,
                EventType.PROVIDER_ACCEPTED,
                from_status=operation.status,
                to_status=operation.status,  # deliberately unchanged
                message=f"Provider accepted the payment ({source}); awaiting receipt",
                details={"providerPaymentId": provider_payment_id, "source": source},
                occurred_at=now,
            )
            return True, self._require(conn, operation_id)

    def mark_intent_accepted(self, operation_id: str) -> None:
        """Provider returned 202 and the id is stored: stop calling."""
        now = to_iso(utcnow())
        with self._txn() as conn:
            conn.execute(
                """
                UPDATE submission_intents
                   SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
                       last_error = NULL, updated_at = ?
                 WHERE operation_id = ? AND state <> ?
                """,
                (str(IntentState.ACCEPTED), now, operation_id, str(IntentState.SETTLED)),
            )

    def release_intent(
        self,
        operation_id: str,
        *,
        state: IntentState,
        error: str,
        retry_after_seconds: float,
        event_type: EventType,
        message: str,
    ) -> None:
        """Release a leased intent after a failed attempt cycle.

        The operation stays PROCESSING: a network error means we do not know
        whether the provider accepted the payment, and we must not decide for it.
        """
        now = utcnow()
        now_iso = to_iso(now)
        with self._txn() as conn:
            operation = self._fetch(conn, operation_id)
            if operation is None:
                return
            conn.execute(
                """
                UPDATE submission_intents
                   SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
                       last_error = ?, next_attempt_at = ?, updated_at = ?
                 WHERE operation_id = ? AND state NOT IN (?, ?)
                """,
                (
                    str(state),
                    error[:500],
                    to_iso(now + timedelta(seconds=retry_after_seconds)),
                    now_iso,
                    operation_id,
                    str(IntentState.SETTLED),
                    str(IntentState.ACCEPTED),
                ),
            )
            if not is_final(operation.status):
                self._append_event(
                    conn,
                    operation_id,
                    event_type,
                    from_status=operation.status,
                    to_status=operation.status,
                    message=message,
                    details={"error": error[:500]},
                    occurred_at=now_iso,
                )

    # --- receipts ------------------------------------------------------------

    def apply_receipt(
        self,
        *,
        operation_id: str,
        provider_payment_id: str,
        result: str,
        message: str | None,
        occurred_at: str | None,
    ) -> ReceiptOutcome:
        """Apply a callback receipt — the only path to a final status.

        One transaction covers de-duplication, binding ``providerPaymentId``, the
        status transition and the event:

        * replay of the same receipt -> no second transition;
        * late receipt with the opposite result -> recorded as ignored, final
          status untouched;
        * receipt naming a different payment than the bound one -> 409;
        * receipt arriving before the provider's HTTP response -> it binds
          ``providerPaymentId``.
        """
        now = to_iso(utcnow())
        target_status = str(status_for_receipt_result(result))

        with self._txn() as conn:
            operation = self._require(conn, operation_id)

            # Mismatching id against an existing binding is a hard conflict.
            if (
                operation.provider_payment_id is not None
                and operation.provider_payment_id != provider_payment_id
            ):
                raise ProviderPaymentIdMismatch(
                    operation_id, operation.provider_payment_id, provider_payment_id
                )

            duplicate = conn.execute(
                "SELECT 1 FROM receipts "
                " WHERE operation_id = ? AND provider_payment_id = ? AND result = ?",
                (operation_id, provider_payment_id, result),
            ).fetchone()
            if duplicate is not None:
                return ReceiptOutcome(operation, applied=False, duplicate=True, reason="duplicate")

            if is_final(operation.status):
                # Late receipt: record it, never move a final status.
                outcome = (
                    "ignored_same_result"
                    if operation.status == target_status
                    else "ignored_conflicting_result"
                )
                self._insert_receipt(
                    conn, operation_id, provider_payment_id, result, message, occurred_at,
                    outcome, now,
                )
                self._append_event(
                    conn,
                    operation_id,
                    EventType.RECEIPT_IGNORED_ALREADY_FINAL,
                    from_status=operation.status,
                    to_status=operation.status,
                    message=f"Receipt {result} ignored: operation is already {operation.status}",
                    details={
                        "receiptResult": result,
                        "finalStatus": operation.status,
                        "providerPaymentId": provider_payment_id,
                    },
                    occurred_at=now,
                )
                return ReceiptOutcome(operation, applied=False, duplicate=False, reason=outcome)

            had_intent = conn.execute(
                "SELECT 1 FROM submission_intents WHERE operation_id = ?", (operation_id,)
            ).fetchone() is not None

            # An early receipt establishes the binding.
            if operation.provider_payment_id is None:
                conn.execute(
                    "UPDATE operations SET provider_payment_id = ?, updated_at = ? "
                    " WHERE operation_id = ? AND provider_payment_id IS NULL",
                    (provider_payment_id, now, operation_id),
                )

            conn.execute(
                f"UPDATE operations SET status = ?, updated_at = ? "
                f" WHERE operation_id = ? AND status NOT IN ({_FINAL_Q})",
                (target_status, now, operation_id, *_FINAL),
            )

            if not had_intent:
                # The provider reported a payment we never visibly requested. The
                # receipt is authoritative, so honour it and leave a trace.
                self._append_event(
                    conn,
                    operation_id,
                    EventType.RECEIPT_APPLIED_WITHOUT_INTENT,
                    from_status=operation.status,
                    to_status=operation.status,
                    message="Receipt arrived for an operation with no recorded submit intent",
                    details={"providerPaymentId": provider_payment_id},
                    occurred_at=now,
                )

            self._append_event(
                conn,
                operation_id,
                EventType(target_status),
                from_status=operation.status,
                to_status=target_status,
                message=message or f"Provider receipt: {result}",
                details={"providerPaymentId": provider_payment_id},
                occurred_at=now,
            )
            self._insert_receipt(
                conn, operation_id, provider_payment_id, result, message, occurred_at,
                "applied", now,
            )
            # Settled: this operation is never sent to the provider again.
            conn.execute(
                "UPDATE submission_intents SET state = ?, lease_owner = NULL, "
                " lease_expires_at = NULL, updated_at = ? WHERE operation_id = ?",
                (str(IntentState.SETTLED), now, operation_id),
            )
            return ReceiptOutcome(
                self._require(conn, operation_id), applied=True, duplicate=False, reason="applied"
            )

    @staticmethod
    def _insert_receipt(
        conn: sqlite3.Connection,
        operation_id: str,
        provider_payment_id: str,
        result: str,
        message: str | None,
        occurred_at: str | None,
        outcome: str,
        received_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO receipts (operation_id, provider_payment_id, result,
                                            message, occurred_at, outcome, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (operation_id, provider_payment_id, result, message, occurred_at, outcome, received_at),
        )

    # --- startup recovery ----------------------------------------------------

    def recover_intents(self) -> list[str]:
        """Make every unfinished intent claimable again, with the same key.

        Runs once before the worker starts. Leases from the dead process are
        meaningless, so they are cleared and the intents become due immediately —
        always with the original Idempotency-Key, never a new one. Also repairs
        two inconsistencies a crash can leave: PROCESSING operations whose intent
        row was never written, and intents left actionable although a receipt
        already settled the operation.
        """
        now = to_iso(utcnow())
        resumed: list[str] = []

        with self._txn() as conn:
            rows = conn.execute(
                f"""
                SELECT i.operation_id, i.state, i.attempts
                  FROM submission_intents AS i
                  JOIN operations AS o USING (operation_id)
                 WHERE o.status NOT IN ({_FINAL_Q}) AND i.state IN ({_ACTIONABLE_Q})
                """,
                (*_FINAL, *_ACTIONABLE),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    UPDATE submission_intents
                       SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
                           next_attempt_at = ?, updated_at = ?
                     WHERE operation_id = ?
                    """,
                    (str(IntentState.PENDING), now, now, row["operation_id"]),
                )
                self._append_event(
                    conn,
                    row["operation_id"],
                    EventType.RECOVERY_INTENT_RESCHEDULED,
                    from_status=str(OperationStatus.PROCESSING),
                    to_status=str(OperationStatus.PROCESSING),
                    message="Recovered after restart: resuming with the original Idempotency-Key",
                    details={
                        "previousIntentState": row["state"],
                        "attempts": row["attempts"],
                        "idempotencyKey": row["operation_id"],
                    },
                    occurred_at=now,
                )
                resumed.append(row["operation_id"])

            # PROCESSING with no intent row at all -> rebuild it.
            orphans = conn.execute(
                """
                SELECT o.operation_id, o.amount, o.currency
                  FROM operations AS o
                  LEFT JOIN submission_intents AS i USING (operation_id)
                 WHERE o.status = ? AND i.operation_id IS NULL
                """,
                (str(OperationStatus.PROCESSING),),
            ).fetchall()
            for row in orphans:
                conn.execute(
                    """
                    INSERT INTO submission_intents (operation_id, idempotency_key, request_body,
                                                    state, attempts, next_attempt_at,
                                                    created_at, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        row["operation_id"],
                        row["operation_id"],
                        canonical_request_body(row["operation_id"], row["amount"], row["currency"]),
                        str(IntentState.PENDING),
                        now,
                        now,
                        now,
                    ),
                )
                self._append_event(
                    conn,
                    row["operation_id"],
                    EventType.RECOVERY_INTENT_REPAIRED,
                    from_status=str(OperationStatus.PROCESSING),
                    to_status=str(OperationStatus.PROCESSING),
                    message="Recovered after restart: rebuilt the missing submission intent",
                    details={"idempotencyKey": row["operation_id"]},
                    occurred_at=now,
                )
                resumed.append(row["operation_id"])

            # Settled operations whose intent is still actionable.
            conn.execute(
                f"""
                UPDATE submission_intents
                   SET state = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE state <> ?
                   AND operation_id IN (SELECT operation_id FROM operations
                                         WHERE status IN ({_FINAL_Q}))
                """,
                (str(IntentState.SETTLED), now, str(IntentState.SETTLED), *_FINAL),
            )

        return resumed

    # --- metrics -------------------------------------------------------------

    def count_operations_by_status(self) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT status, COUNT(*) AS n FROM operations GROUP BY status"
        ).fetchall()
        counts = {str(status): 0 for status in OperationStatus}
        for row in rows:
            counts[row["status"]] = int(row["n"])
        return counts

    def count_intents_by_state(self) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT state, COUNT(*) AS n FROM submission_intents GROUP BY state"
        ).fetchall()
        counts = {str(state): 0 for state in IntentState}
        for row in rows:
            counts[row["state"]] = int(row["n"])
        return counts

    def bump_provider_stats(self, *, calls: int = 0, retries: int = 0) -> None:
        if calls == 0 and retries == 0:
            return
        with self._txn() as conn:
            conn.execute(
                "UPDATE provider_stats SET total_calls = total_calls + ?, "
                " total_retries = total_retries + ? WHERE id = 1",
                (calls, retries),
            )

    def provider_stats(self) -> dict[str, int]:
        row = self._conn.execute(
            "SELECT total_calls, total_retries FROM provider_stats WHERE id = 1"
        ).fetchone()
        return {"total_calls": int(row["total_calls"]), "total_retries": int(row["total_retries"])}
