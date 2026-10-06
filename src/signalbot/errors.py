"""Error taxonomy for the ingestion pipeline.

Two classes of failure exist on the WebSocket ingestion path:

* Transport failures (connect, receive, protocol, connection-age recycle) are
  recoverable: the consumer reconnects with bounded backoff.
* Fatal pipeline failures come from the handler (persistence, state bounds,
  immutable-identity conflicts). They must never be treated as a disconnect:
  the scanner task fails, the application logs CRITICAL and fails closed for
  operator attention (docs/ARCHITECTURE.md fail-closed contract).
"""

from __future__ import annotations

from sqlalchemy.exc import SQLAlchemyError

from signalbot.data.candles import CandleConflictError
from signalbot.persistence.repository import EventIdConflictError, OutboxCapacityError
from signalbot.signals.positions import PaperLifecycleBoundError

# SQLAlchemyError covers DataError, OperationalError and IntegrityError.
FATAL_PIPELINE_ERRORS: tuple[type[BaseException], ...] = (
    OutboxCapacityError,
    EventIdConflictError,
    CandleConflictError,
    PaperLifecycleBoundError,
    SQLAlchemyError,
)


def is_fatal_pipeline_error(error: BaseException) -> bool:
    return isinstance(error, FATAL_PIPELINE_ERRORS)
