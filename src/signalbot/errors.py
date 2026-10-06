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

# SQLAlchemyError covers DataError, OperationalError and IntegrityError.
FATAL_PIPELINE_ERRORS: tuple[type[BaseException], ...] = (
    OutboxCapacityError,
    EventIdConflictError,
    CandleConflictError,
    SQLAlchemyError,
)

# signals/positions.py raises plain RuntimeError for its symbol bounds. That module
# is bound by the frozen Guardian policy source-identity contract, so it is not
# edited to add a dedicated exception type; the bound errors are recognized by
# their stable message prefix instead.
_PAPER_LIFECYCLE_BOUND_PREFIX = "paper lifecycle "
_PAPER_LIFECYCLE_BOUND_MARKER = "symbol bound"


def _is_paper_lifecycle_bound_error(error: BaseException) -> bool:
    message = str(error)
    return (
        type(error) is RuntimeError
        and message.startswith(_PAPER_LIFECYCLE_BOUND_PREFIX)
        and _PAPER_LIFECYCLE_BOUND_MARKER in message
    )


def is_fatal_pipeline_error(error: BaseException) -> bool:
    return isinstance(error, FATAL_PIPELINE_ERRORS) or _is_paper_lifecycle_bound_error(error)
