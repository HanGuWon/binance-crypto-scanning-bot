from __future__ import annotations

from dataclasses import InitVar, dataclass
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBFeatureContractErrorV2,
    FamilyBKlineBarV2,
    canonical_family_b_kline_bar_v2,
)

_FACTORY_TOKEN = object()
MAX_CLOSED_KLINE_PATH_ROWS_V2: Final = 8_653


class ClosedKlinePathContractErrorV2(ValueError):
    """Raised when a bounded closed-kline path cannot be assessed."""


class ClosedKlinePathFailureV2(StrEnum):
    IDENTITY_DRIFT = "IDENTITY_DRIFT"
    ROW_NOT_CLOSED_EXACT_5M = "ROW_NOT_CLOSED_EXACT_5M"
    EVENT_PRECEDES_OWN_CLOSE = "EVENT_PRECEDES_OWN_CLOSE"
    RECEIPT_PRECEDES_EVENT = "RECEIPT_PRECEDES_EVENT"
    RECEIPT_AFTER_DECISION_CUTOFF = "RECEIPT_AFTER_DECISION_CUTOFF"
    HISTORY_GAP_OR_DUPLICATE = "HISTORY_GAP_OR_DUPLICATE"
    PREVIOUS_CLOSE_CHAIN_MISMATCH = "PREVIOUS_CLOSE_CHAIN_MISMATCH"


@dataclass(frozen=True, slots=True)
class ClosedKlinePathAssessmentV2:
    """Factory-owned canonical path assessment shared by price and volatility."""

    rows: tuple[FamilyBKlineBarV2, ...]
    canonical_rows: tuple[bytes, ...]
    latest_event_ms: int
    latest_receipt_ms: int
    failure: ClosedKlinePathFailureV2 | None
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ClosedKlinePathContractErrorV2(
                "closed-kline path assessments require their canonical factory"
            )
        if not self.rows or len(self.rows) != len(self.canonical_rows):
            raise ClosedKlinePathContractErrorV2(
                "closed-kline assessment rows and canonical rows must be non-empty and equal"
            )
        if self.rows != tuple(
            sorted(self.rows, key=lambda value: (value.bar_open_ms, value.close_event_id))
        ):
            raise ClosedKlinePathContractErrorV2(
                "closed-kline assessment rows must remain canonically ordered"
            )
        if self.latest_event_ms != max(value.event_ms for value in self.rows):
            raise ClosedKlinePathContractErrorV2(
                "closed-kline latest event differs from assessed rows"
            )
        if self.latest_receipt_ms != max(value.receipt_ms for value in self.rows):
            raise ClosedKlinePathContractErrorV2(
                "closed-kline latest receipt differs from assessed rows"
            )
        if self.failure is not None and not isinstance(
            self.failure, ClosedKlinePathFailureV2
        ):
            raise ClosedKlinePathContractErrorV2(
                "closed-kline failure must use ClosedKlinePathFailureV2"
            )

    @property
    def current(self) -> FamilyBKlineBarV2:
        return self.rows[-1]

    @property
    def decision_cutoff_ms(self) -> int:
        return self.current.bar_close_ms + DECISION_DELAY_MS_V2

    @property
    def valid(self) -> bool:
        return self.failure is None


def assess_closed_kline_path_v2(
    rows: tuple[FamilyBKlineBarV2, ...],
    *,
    maximum_rows: int,
) -> ClosedKlinePathAssessmentV2:
    """Canonicalize and assess one bounded target-symbol 5m kline path."""

    if type(rows) is not tuple or not rows:
        raise ClosedKlinePathContractErrorV2(
            "closed-kline rows must be a non-empty immutable tuple"
        )
    if any(not isinstance(value, FamilyBKlineBarV2) for value in rows):
        raise ClosedKlinePathContractErrorV2(
            "closed-kline rows contain an unsupported value"
        )
    if (
        type(maximum_rows) is not int
        or not 1 <= maximum_rows <= MAX_CLOSED_KLINE_PATH_ROWS_V2
    ):
        raise ClosedKlinePathContractErrorV2(
            "maximum_rows must be an integer in the frozen bounded path range"
        )
    if len(rows) > maximum_rows:
        raise ClosedKlinePathContractErrorV2(
            "closed-kline path exceeds its bounded maximum row count"
        )

    ordered = tuple(
        sorted(rows, key=lambda value: (value.bar_open_ms, value.close_event_id))
    )
    try:
        canonical_rows = tuple(
            canonical_family_b_kline_bar_v2(value) for value in ordered
        )
    except FamilyBFeatureContractErrorV2 as exc:
        raise ClosedKlinePathContractErrorV2(
            "closed-kline row differs from its canonical content"
        ) from exc
    current = ordered[-1]
    return ClosedKlinePathAssessmentV2(
        rows=ordered,
        canonical_rows=canonical_rows,
        latest_event_ms=max(value.event_ms for value in ordered),
        latest_receipt_ms=max(value.receipt_ms for value in ordered),
        failure=_first_failure(ordered, current),
        _factory_token=_FACTORY_TOKEN,
    )


def _first_failure(
    ordered: tuple[FamilyBKlineBarV2, ...],
    current: FamilyBKlineBarV2,
) -> ClosedKlinePathFailureV2 | None:
    identity = (
        current.symbol,
        current.venue,
        current.promoting_plan_sha256,
        current.capture_root_sha256,
        current.schema_sha256,
    )
    decision_cutoff_ms = current.bar_close_ms + DECISION_DELAY_MS_V2
    for index, value in enumerate(ordered):
        if (
            value.symbol,
            value.venue,
            value.promoting_plan_sha256,
            value.capture_root_sha256,
            value.schema_sha256,
        ) != identity:
            return ClosedKlinePathFailureV2.IDENTITY_DRIFT
        if (
            value.interval_ms != FIVE_MINUTE_MS_V2
            or value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
            or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
            or not value.closed
        ):
            return ClosedKlinePathFailureV2.ROW_NOT_CLOSED_EXACT_5M
        # Binance E is the publication event time, not candle data time.  A
        # final x=true update may therefore be emitted after k.T.  The exact
        # closed bar still binds all economic data through k.T; publication is
        # causal when it is no earlier than k.T and is received by D.
        if value.event_ms < value.bar_close_ms:
            return ClosedKlinePathFailureV2.EVENT_PRECEDES_OWN_CLOSE
        if value.receipt_ms < value.event_ms:
            return ClosedKlinePathFailureV2.RECEIPT_PRECEDES_EVENT
        if value.receipt_ms > decision_cutoff_ms:
            return ClosedKlinePathFailureV2.RECEIPT_AFTER_DECISION_CUTOFF
        if index == 0:
            continue
        previous = ordered[index - 1]
        if value.bar_open_ms != previous.bar_open_ms + FIVE_MINUTE_MS_V2:
            return ClosedKlinePathFailureV2.HISTORY_GAP_OR_DUPLICATE
        if value.previous_close != previous.close:
            return ClosedKlinePathFailureV2.PREVIOUS_CLOSE_CHAIN_MISMATCH
    return None
