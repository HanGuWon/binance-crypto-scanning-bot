"""M1-only price projection over canonical USD-M final-kline snapshots.

Each input is serialized by the upstream M1 canonicalizer before its digest and
selected source fields enter the ordered source root.  The projection therefore
inherits that canonicalizer's RFC 8785 safe-integer domain; its only derived
integer, the decision cutoff, is checked against the same bound before hashing.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmKline5mM1V2,
    UsdmMarketM1ContractErrorV2,
    canonical_usdm_market_m1_v2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    PriceClosePathCalculationV2,
    PriceEvidenceContractErrorV2,
    calculate_price_close_path_v2,
    canonical_price_close_path_calculation_v2,
)

PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2: Final = PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2 + 1
PRICE_KLINE_M1_AUTHORITY_STATUS_V2: Final = "M1_ONLY_UNBOUND"
PRICE_KLINE_M1_ROLE_V2: Final = "NON_PROMOTING_M1_CALCULATION_PROJECTION"

_SCHEMA_VERSION: Final = "r4b_price_kline_m1_projection_v2"
_LINEAGE_SCHEMA_VERSION: Final = "r4b_price_kline_m1_lineage_entry_v2"
_CLOSE_SCHEMA_VERSION: Final = "r4b_price_kline_m1_close_entry_v2"
_SOURCE_ROOT_SCHEMA_VERSION: Final = "r4b_price_kline_m1_source_root_v2"
_ECONOMIC_ROOT_SCHEMA_VERSION: Final = "r4b_price_kline_m1_economic_root_v2"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_PRICE_KLINE_M1_SOURCE_ROOT_V2\0"
_ECONOMIC_ROOT_DOMAIN: Final = b"R4B_PRICE_KLINE_M1_ECONOMIC_ROOT_V2\0"
_PROJECTION_DOMAIN: Final = b"R4B_PRICE_KLINE_M1_PROJECTION_V2\0"
_FACTORY_TOKEN: Final = object()
_ENTRY_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_CANONICAL_INTEGER: Final = 2**53 - 1


class PriceKlineM1ProjectionContractErrorV2(ValueError):
    """Raised when an M1-only price projection would overstate its authority."""


@dataclass(frozen=True, slots=True)
class PriceKlineM1LineageEntryV2:
    """One canonically ordered, source-complete kline M1 lineage entry."""

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    m1_payload_sha256: str
    m1_canonical_sha256: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    source_event_ms: int
    data_time_ms: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    bar_open_ms: int
    bar_close_ms: int
    closed: Literal[True]
    close: Decimal
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 lineage entries require their projection factory"
            )
        _validate_lineage_entry(self)


@dataclass(frozen=True, slots=True)
class PriceKlineM1CloseEntryV2:
    """One close actually consumed by the frozen price calculation."""

    bar_open_ms: int
    bar_close_ms: int
    close: Decimal
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 close entries require their projection factory"
            )
        _validate_close_entry(self)


@dataclass(frozen=True, slots=True)
class PriceKlineM1ProjectionV2:
    """Exact M1 kline projection with numeric, but never M2, readiness."""

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    plan_id: str
    protocol_sha256: str
    parser_contract_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    assumed_closed_bar_through_ms: int
    latest_source_event_ms: int
    latest_receipt_wall_ms: int
    latest_receipt_monotonic_ns: int
    ordered_source_lineage: tuple[PriceKlineM1LineageEntryV2, ...]
    economic_close_slice: tuple[PriceKlineM1CloseEntryV2, ...]
    source_lineage_root_sha256: str
    economic_close_slice_sha256: str
    calculation: PriceClosePathCalculationV2
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    projection_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    )
    authority_status: Literal["M1_ONLY_UNBOUND"] = field(
        init=False,
        default="M1_ONLY_UNBOUND",
    )
    role: str = field(init=False, default=PRICE_KLINE_M1_ROLE_V2)
    data_through_ms: None = field(init=False, default=None)
    m2_certificate_sha256: None = field(init=False, default=None)
    causal_inputs_complete: Literal[False] = field(init=False, default=False)
    producer_ready: Literal[False] = field(init=False, default=False)
    promoting_eligible: Literal[False] = field(init=False, default=False)
    source_row_count: int = field(
        init=False,
        default=PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2,
    )
    calculation_row_count: int = field(
        init=False,
        default=PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 projections require their canonical factory"
            )
        _validate_projection(self)
        object.__setattr__(
            self,
            "projection_sha256",
            hashlib.sha256(
                _PROJECTION_DOMAIN
                + canonical_json_line(_projection_document(self, include_projection_hash=False))
            ).hexdigest(),
        )

    @property
    def calculation_ready(self) -> bool:
        """Return numeric readiness without implying producer readiness."""

        return self.calculation.ready


def build_price_kline_m1_projection_v2(
    rows: tuple[UsdmKline5mM1V2, ...],
) -> PriceKlineM1ProjectionV2:
    """Build one exact 8,654-row M1-only price calculation projection.

    The first chronological row is a continuity anchor.  The remaining 8,653
    closes are the complete economic input to the frozen price calculation.
    Input order is irrelevant; slot identity and source cursor claims are not.
    """

    if type(rows) is not tuple or len(rows) != PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection requires exactly 8,654 immutable rows including anchor"
        )
    if any(not isinstance(row, UsdmKline5mM1V2) for row in rows):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection contains a non-kline M1 row"
        )

    canonical_rows: list[tuple[UsdmKline5mM1V2, bytes]] = []
    for row in rows:
        try:
            canonical = canonical_usdm_market_m1_v2(row)
        except (TypeError, ValueError, UsdmMarketM1ContractErrorV2) as exc:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 input row is not live-valid canonical M1"
            ) from exc
        canonical_rows.append((row, canonical))
    ordered = tuple(sorted(canonical_rows, key=lambda item: item[0].bar_open_ms))
    _validate_ordered_source_rows(tuple(item[0] for item in ordered))

    lineage = tuple(_lineage_entry(row, canonical) for row, canonical in ordered)
    economic = tuple(
        PriceKlineM1CloseEntryV2(
            bar_open_ms=row.bar_open_ms,
            bar_close_ms=row.bar_close_ms,
            close=row.close,
            _factory_token=_ENTRY_FACTORY_TOKEN,
        )
        for row, _canonical in ordered[1:]
    )
    current = ordered[-1][0]
    decision_cutoff_ms = current.bar_close_ms + DECISION_DELAY_MS_V2
    if decision_cutoff_ms > _MAX_CANONICAL_INTEGER:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 decision cutoff exceeds the inherited canonical integer domain"
        )
    source_root = _source_lineage_root(
        lineage,
        symbol=current.symbol,
        venue=current.venue,
        stream=current.stream,
        promoting_plan_sha256=current.promoting_plan_sha256,
        bar_open_ms=current.bar_open_ms,
        bar_close_ms=current.bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    economic_root = _economic_close_slice_root(
        economic,
        symbol=current.symbol,
        venue=current.venue,
        promoting_plan_sha256=current.promoting_plan_sha256,
        bar_open_ms=current.bar_open_ms,
        bar_close_ms=current.bar_close_ms,
    )
    calculation = calculate_price_close_path_v2(tuple(entry.close for entry in economic))
    reasons = (
        "EXACT_8654_CANONICAL_CLOSED_KLINE_M1_ROWS_WITH_ANCHOR",
        calculation.reason,
        "M2_CURSOR_FINALITY_AND_CAUSAL_INPUT_COMPLETENESS_UNBOUND",
        "NUMERIC_READY_DOES_NOT_IMPLY_PRODUCER_READY_OR_PROMOTION",
    )
    return PriceKlineM1ProjectionV2(
        symbol=current.symbol,
        venue=current.venue,
        route_id=current.route_id,
        stream=current.stream,
        promoting_plan_sha256=current.promoting_plan_sha256,
        plan_id=current.plan_id,
        protocol_sha256=current.protocol_sha256,
        parser_contract_sha256=current.parser_contract_sha256,
        bar_open_ms=current.bar_open_ms,
        bar_close_ms=current.bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        assumed_closed_bar_through_ms=current.bar_close_ms,
        latest_source_event_ms=max(entry.source_event_ms for entry in lineage),
        latest_receipt_wall_ms=max(entry.receipt_wall_ms for entry in lineage),
        latest_receipt_monotonic_ns=lineage[-1].receipt_monotonic_ns,
        ordered_source_lineage=lineage,
        economic_close_slice=economic,
        source_lineage_root_sha256=source_root,
        economic_close_slice_sha256=economic_root,
        calculation=calculation,
        reasons=reasons,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_price_kline_m1_projection_v2(
    projection: PriceKlineM1ProjectionV2,
) -> bytes:
    """Serialize and live-validate a sealed M1-only price projection."""

    if not isinstance(projection, PriceKlineM1ProjectionV2):
        raise PriceKlineM1ProjectionContractErrorV2("projection must be PriceKlineM1ProjectionV2")
    _validate_projection(projection)
    expected_source_root = _source_lineage_root(
        projection.ordered_source_lineage,
        symbol=projection.symbol,
        venue=projection.venue,
        stream=projection.stream,
        promoting_plan_sha256=projection.promoting_plan_sha256,
        bar_open_ms=projection.bar_open_ms,
        bar_close_ms=projection.bar_close_ms,
        decision_cutoff_ms=projection.decision_cutoff_ms,
    )
    if projection.source_lineage_root_sha256 != expected_source_root:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 source lineage differs from its sealed root"
        )
    expected_economic_root = _economic_close_slice_root(
        projection.economic_close_slice,
        symbol=projection.symbol,
        venue=projection.venue,
        promoting_plan_sha256=projection.promoting_plan_sha256,
        bar_open_ms=projection.bar_open_ms,
        bar_close_ms=projection.bar_close_ms,
    )
    if projection.economic_close_slice_sha256 != expected_economic_root:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 economic close slice differs from its sealed root"
        )
    expected_calculation = calculate_price_close_path_v2(
        tuple(entry.close for entry in projection.economic_close_slice)
    )
    if projection.calculation != expected_calculation:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 calculation differs from its exact close slice"
        )
    expected_projection = hashlib.sha256(
        _PROJECTION_DOMAIN
        + canonical_json_line(_projection_document(projection, include_projection_hash=False))
    ).hexdigest()
    if projection.projection_sha256 != expected_projection:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection differs from canonical content"
        )
    return canonical_json_line(_projection_document(projection, include_projection_hash=True))


def _validate_ordered_source_rows(
    rows: tuple[UsdmKline5mM1V2, ...],
) -> None:
    first = rows[0]
    current = rows[-1]
    expected_identity = (
        first.symbol,
        first.venue,
        first.route_id,
        first.stream,
        first.promoting_plan_sha256,
        first.plan_id,
        first.protocol_sha256,
        first.parser_contract_sha256,
    )
    if (
        first.venue is not VenueV2.USDM_FUTURES
        or first.route_id != "usdm_market"
        or first.stream != f"{first.symbol.lower()}@kline_5m"
        or first.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 source route, stream, or parser is not exact"
        )
    decision_cutoff_ms = current.bar_close_ms + DECISION_DELAY_MS_V2
    if decision_cutoff_ms > _MAX_CANONICAL_INTEGER:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 decision cutoff exceeds the inherited canonical integer domain"
        )
    seen_slots: dict[int, str] = {}
    seen_m0: set[str] = set()
    seen_raw: set[str] = set()
    seen_m1: set[str] = set()
    seen_cursors: set[tuple[str, str, int, int]] = set()
    last_session_cursor: dict[str, tuple[int, int, int]] = {}
    last_owner_frame: dict[tuple[str, str, int], int] = {}
    last_connection_generation: dict[tuple[str, str], int] = {}
    previous: UsdmKline5mM1V2 | None = None
    for row in rows:
        identity = (
            row.symbol,
            row.venue,
            row.route_id,
            row.stream,
            row.promoting_plan_sha256,
            row.plan_id,
            row.protocol_sha256,
            row.parser_contract_sha256,
        )
        if identity != expected_identity:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 rows disagree on symbol, stream, plan, protocol, or parser"
            )
        if (
            not row.parser_bound
            or not row.live_reverification_required
            or row.current_authority_claimed
            or row.cursor_complete
            or row.causal_inputs_complete
        ):
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 row exposes an unsupported authority or cursor claim"
            )
        if not row.closed:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 projection requires x=true final candles"
            )
        prior_hash = seen_slots.get(row.bar_open_ms)
        if prior_hash is not None:
            qualifier = "duplicate" if prior_hash == row.m1_payload_sha256 else "conflicting"
            raise PriceKlineM1ProjectionContractErrorV2(
                f"price M1 source contains a {qualifier} candle slot"
            )
        seen_slots[row.bar_open_ms] = row.m1_payload_sha256
        if row.m0_leaf_sha256 in seen_m0:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 source repeats an M0 membership leaf"
            )
        if row.raw_payload_hash_v2 in seen_raw or row.m1_payload_sha256 in seen_m1:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 source repeats raw or parsed evidence"
            )
        seen_m0.add(row.m0_leaf_sha256)
        seen_raw.add(row.raw_payload_hash_v2)
        seen_m1.add(row.m1_payload_sha256)
        cursor = (
            row.session_id,
            row.connection_id,
            row.generation,
            row.frame_seq,
        )
        if cursor in seen_cursors:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 source repeats a WebSocket cursor"
            )
        seen_cursors.add(cursor)
        if (
            row.event_ms < row.bar_close_ms
            or row.event_ms > row.receipt_wall_ms
            or row.receipt_wall_ms > decision_cutoff_ms
        ):
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 data time, exchange event, receipt, or decision cutoff is noncausal"
            )
        session_cursor = last_session_cursor.get(row.session_id)
        if session_cursor is not None and (
            row.ingest_seq <= session_cursor[0]
            or row.receipt_wall_ms <= session_cursor[1]
            or row.receipt_monotonic_ns <= session_cursor[2]
        ):
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 receipt or ingest cursor is not ordered within session"
            )
        last_session_cursor[row.session_id] = (
            row.ingest_seq,
            row.receipt_wall_ms,
            row.receipt_monotonic_ns,
        )
        owner = (row.session_id, row.connection_id, row.generation)
        owner_frame = last_owner_frame.get(owner)
        if owner_frame is not None and row.frame_seq <= owner_frame:
            raise PriceKlineM1ProjectionContractErrorV2(
                "price M1 frame cursor is not ordered within connection generation"
            )
        last_owner_frame[owner] = row.frame_seq
        connection = (row.session_id, row.connection_id)
        prior_generation = last_connection_generation.get(connection)
        if prior_generation is not None and row.generation < prior_generation:
            raise PriceKlineM1ProjectionContractErrorV2("price M1 connection generation regresses")
        last_connection_generation[connection] = row.generation
        if previous is not None:
            if row.bar_open_ms != previous.bar_open_ms + FIVE_MINUTE_MS_V2:
                raise PriceKlineM1ProjectionContractErrorV2(
                    "price M1 candle slots are not exactly contiguous"
                )
            if row.event_ms <= previous.event_ms or row.receipt_wall_ms <= previous.receipt_wall_ms:
                raise PriceKlineM1ProjectionContractErrorV2(
                    "price M1 exchange events or wall receipts are not chronologically ordered"
                )
        previous = row


def _lineage_entry(
    row: UsdmKline5mM1V2,
    canonical: bytes,
) -> PriceKlineM1LineageEntryV2:
    return PriceKlineM1LineageEntryV2(
        symbol=row.symbol,
        venue=row.venue,
        route_id=row.route_id,
        stream=row.stream,
        promoting_plan_sha256=row.promoting_plan_sha256,
        capture_authority_sha256=row.capture_authority_sha256,
        protocol_sha256=row.protocol_sha256,
        parser_contract_sha256=row.parser_contract_sha256,
        m0_leaf_sha256=row.m0_leaf_sha256,
        raw_payload_hash_v2=row.raw_payload_hash_v2,
        m1_payload_sha256=row.m1_payload_sha256,
        m1_canonical_sha256=hashlib.sha256(canonical).hexdigest(),
        session_id=row.session_id,
        plan_id=row.plan_id,
        connection_id=row.connection_id,
        generation=row.generation,
        frame_seq=row.frame_seq,
        ingest_seq=row.ingest_seq,
        source_event_ms=row.event_ms,
        data_time_ms=row.bar_close_ms,
        receipt_wall_ms=row.receipt_wall_ms,
        receipt_monotonic_ns=row.receipt_monotonic_ns,
        bar_open_ms=row.bar_open_ms,
        bar_close_ms=row.bar_close_ms,
        closed=True,
        close=row.close,
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _validate_projection(value: PriceKlineM1ProjectionV2) -> None:
    if value.schema_version != _SCHEMA_VERSION:
        raise PriceKlineM1ProjectionContractErrorV2("unsupported price M1 projection schema")
    if (
        _SYMBOL_RE.fullmatch(value.symbol) is None
        or value.venue is not VenueV2.USDM_FUTURES
        or value.route_id != "usdm_market"
        or value.stream != f"{value.symbol.lower()}@kline_5m"
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection identity is not exact USD-M kline_5m"
        )
    for digest, name in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.protocol_sha256, "protocol_sha256"),
        (value.parser_contract_sha256, "parser_contract_sha256"),
        (value.source_lineage_root_sha256, "source_lineage_root_sha256"),
        (value.economic_close_slice_sha256, "economic_close_slice_sha256"),
    ):
        _require_sha256(digest, name)
    if value.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2:
        raise PriceKlineM1ProjectionContractErrorV2("price M1 projection parser contract differs")
    if (
        value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        or value.decision_cutoff_ms != value.bar_close_ms + DECISION_DELAY_MS_V2
        or value.assumed_closed_bar_through_ms != value.bar_close_ms
        or any(
            not _is_canonical_nonnegative_integer(integer)
            for integer in (
                value.bar_open_ms,
                value.bar_close_ms,
                value.decision_cutoff_ms,
                value.assumed_closed_bar_through_ms,
                value.latest_source_event_ms,
                value.latest_receipt_wall_ms,
                value.latest_receipt_monotonic_ns,
            )
        )
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection decision clock is inconsistent"
        )
    if (
        value.authority_status != PRICE_KLINE_M1_AUTHORITY_STATUS_V2
        or value.data_through_ms is not None
        or value.m2_certificate_sha256 is not None
        or value.causal_inputs_complete
        or value.producer_ready
        or value.promoting_eligible
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "M1-only projection cannot claim M2, producer, or promotion authority"
        )
    if (
        type(value.ordered_source_lineage) is not tuple
        or len(value.ordered_source_lineage) != PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2
        or type(value.economic_close_slice) is not tuple
        or len(value.economic_close_slice) != PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection row bounds differ from its exact window"
        )
    if any(
        not isinstance(entry, PriceKlineM1LineageEntryV2) for entry in value.ordered_source_lineage
    ) or any(
        not isinstance(entry, PriceKlineM1CloseEntryV2) for entry in value.economic_close_slice
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection contains an unsupported lineage or close entry"
        )
    for entry in value.ordered_source_lineage:
        _validate_lineage_entry(entry)
    for entry in value.economic_close_slice:
        _validate_close_entry(entry)
    source_current = value.ordered_source_lineage[-1]
    if (
        source_current.symbol != value.symbol
        or source_current.bar_open_ms != value.bar_open_ms
        or source_current.bar_close_ms != value.bar_close_ms
        or value.latest_source_event_ms
        != max(entry.source_event_ms for entry in value.ordered_source_lineage)
        or value.latest_receipt_wall_ms
        != max(entry.receipt_wall_ms for entry in value.ordered_source_lineage)
        or value.latest_receipt_monotonic_ns
        != value.ordered_source_lineage[-1].receipt_monotonic_ns
        or value.economic_close_slice
        != tuple(
            PriceKlineM1CloseEntryV2(
                bar_open_ms=entry.bar_open_ms,
                bar_close_ms=entry.bar_close_ms,
                close=entry.close,
                _factory_token=_ENTRY_FACTORY_TOKEN,
            )
            for entry in value.ordered_source_lineage[1:]
        )
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection scope differs from its ordered source rows"
        )
    if (
        type(value.reasons) is not tuple
        or len(value.reasons) != 4
        or any(
            not isinstance(reason, str)
            or not reason
            or reason.strip() != reason
            or len(reason) > 256
            for reason in value.reasons
        )
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection reasons are not exact bounded text"
        )
    try:
        canonical_price_close_path_calculation_v2(value.calculation)
    except PriceEvidenceContractErrorV2 as exc:
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 projection calculation is not canonical"
        ) from exc


def _validate_lineage_entry(value: PriceKlineM1LineageEntryV2) -> None:
    if (
        _SYMBOL_RE.fullmatch(value.symbol) is None
        or value.venue is not VenueV2.USDM_FUTURES
        or value.route_id != "usdm_market"
        or value.stream != f"{value.symbol.lower()}@kline_5m"
        or value.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    ):
        raise PriceKlineM1ProjectionContractErrorV2("price M1 lineage identity is not exact")
    for digest, name in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.capture_authority_sha256, "capture_authority_sha256"),
        (value.protocol_sha256, "protocol_sha256"),
        (value.parser_contract_sha256, "parser_contract_sha256"),
        (value.m0_leaf_sha256, "m0_leaf_sha256"),
        (value.raw_payload_hash_v2, "raw_payload_hash_v2"),
        (value.m1_payload_sha256, "m1_payload_sha256"),
        (value.m1_canonical_sha256, "m1_canonical_sha256"),
    ):
        _require_sha256(digest, name)
    if any(
        not isinstance(identity, str)
        or not identity
        or identity.strip() != identity
        or len(identity) > 256
        for identity in (value.session_id, value.plan_id, value.connection_id)
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 lineage source identity is not bounded text"
        )
    for integer, name in (
        (value.generation, "generation"),
        (value.frame_seq, "frame_seq"),
        (value.ingest_seq, "ingest_seq"),
    ):
        if not _is_canonical_positive_integer(integer):
            raise PriceKlineM1ProjectionContractErrorV2(
                f"price M1 lineage {name} must be a positive integer"
            )
    for integer, name in (
        (value.source_event_ms, "source_event_ms"),
        (value.data_time_ms, "data_time_ms"),
        (value.receipt_wall_ms, "receipt_wall_ms"),
        (value.receipt_monotonic_ns, "receipt_monotonic_ns"),
        (value.bar_open_ms, "bar_open_ms"),
        (value.bar_close_ms, "bar_close_ms"),
    ):
        if not _is_canonical_nonnegative_integer(integer):
            raise PriceKlineM1ProjectionContractErrorV2(
                f"price M1 lineage {name} must be a nonnegative integer"
            )
    if (
        value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        or value.data_time_ms != value.bar_close_ms
        or value.closed is not True
        or value.source_event_ms < value.data_time_ms
        or value.source_event_ms > value.receipt_wall_ms
        or not _is_positive_decimal(value.close)
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 lineage candle or causal clock is invalid"
        )


def _validate_close_entry(value: PriceKlineM1CloseEntryV2) -> None:
    if (
        not _is_canonical_nonnegative_integer(value.bar_open_ms)
        or value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or not _is_canonical_nonnegative_integer(value.bar_close_ms)
        or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        or not _is_positive_decimal(value.close)
    ):
        raise PriceKlineM1ProjectionContractErrorV2(
            "price M1 close entry is not an exact positive 5m close"
        )


def _source_lineage_root(
    entries: tuple[PriceKlineM1LineageEntryV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    stream: str,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> str:
    return hashlib.sha256(
        _SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "authority_status": PRICE_KLINE_M1_AUTHORITY_STATUS_V2,
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "decision_cutoff_ms": decision_cutoff_ms,
                "first_slot_open_ms": entries[0].bar_open_ms,
                "last_slot_open_ms": entries[-1].bar_open_ms,
                "ordered_rows": [_lineage_document(entry) for entry in entries],
                "promoting_plan_sha256": promoting_plan_sha256,
                "row_count": len(entries),
                "schema_version": _SOURCE_ROOT_SCHEMA_VERSION,
                "stream": stream,
                "symbol": symbol,
                "venue": venue.value,
            }
        )
    ).hexdigest()


def _economic_close_slice_root(
    entries: tuple[PriceKlineM1CloseEntryV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
) -> str:
    return hashlib.sha256(
        _ECONOMIC_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "promoting_plan_sha256": promoting_plan_sha256,
                "rows": [_close_document(entry) for entry in entries],
                "schema_version": _ECONOMIC_ROOT_SCHEMA_VERSION,
                "symbol": symbol,
                "venue": venue.value,
            }
        )
    ).hexdigest()


def _projection_document(
    value: PriceKlineM1ProjectionV2,
    *,
    include_projection_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "assumed_closed_bar_through_ms": value.assumed_closed_bar_through_ms,
        "authority_status": value.authority_status,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "calculation": _calculation_document(value.calculation),
        "calculation_row_count": value.calculation_row_count,
        "causal_inputs_complete": value.causal_inputs_complete,
        "data_through_ms": value.data_through_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "economic_close_slice": [_close_document(entry) for entry in value.economic_close_slice],
        "economic_close_slice_sha256": value.economic_close_slice_sha256,
        "latest_receipt_monotonic_ns": value.latest_receipt_monotonic_ns,
        "latest_receipt_wall_ms": value.latest_receipt_wall_ms,
        "latest_source_event_ms": value.latest_source_event_ms,
        "m2_certificate_sha256": value.m2_certificate_sha256,
        "ordered_source_lineage": [
            _lineage_document(entry) for entry in value.ordered_source_lineage
        ],
        "parser_contract_sha256": value.parser_contract_sha256,
        "plan_id": value.plan_id,
        "producer_ready": value.producer_ready,
        "promoting_eligible": value.promoting_eligible,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "protocol_sha256": value.protocol_sha256,
        "reasons": list(value.reasons),
        "role": value.role,
        "route_id": value.route_id,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "source_row_count": value.source_row_count,
        "stream": value.stream,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_projection_hash:
        document["projection_sha256"] = value.projection_sha256
    return document


def _lineage_document(value: PriceKlineM1LineageEntryV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "capture_authority_sha256": value.capture_authority_sha256,
        "close": str(value.close),
        "closed": value.closed,
        "connection_id": value.connection_id,
        "data_time_ms": value.data_time_ms,
        "frame_seq": value.frame_seq,
        "generation": value.generation,
        "ingest_seq": value.ingest_seq,
        "m0_leaf_sha256": value.m0_leaf_sha256,
        "m1_canonical_sha256": value.m1_canonical_sha256,
        "m1_payload_sha256": value.m1_payload_sha256,
        "parser_contract_sha256": value.parser_contract_sha256,
        "plan_id": value.plan_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "protocol_sha256": value.protocol_sha256,
        "raw_payload_hash_v2": value.raw_payload_hash_v2,
        "receipt_monotonic_ns": value.receipt_monotonic_ns,
        "receipt_wall_ms": value.receipt_wall_ms,
        "route_id": value.route_id,
        "schema_version": _LINEAGE_SCHEMA_VERSION,
        "session_id": value.session_id,
        "source_event_ms": value.source_event_ms,
        "stream": value.stream,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _close_document(value: PriceKlineM1CloseEntryV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close": str(value.close),
        "schema_version": _CLOSE_SCHEMA_VERSION,
    }


def _calculation_document(value: PriceClosePathCalculationV2) -> dict[str, object]:
    canonical_price_close_path_calculation_v2(value)
    return {
        "calculation_sha256": value.calculation_sha256,
        "composite": _decimal_or_none(value.composite),
        "current_return_1": _decimal_or_none(value.current_return_1),
        "current_return_12": _decimal_or_none(value.current_return_12),
        "direction": value.direction,
        "normalized_return_1": _decimal_or_none(value.normalized_return_1),
        "normalized_return_12": _decimal_or_none(value.normalized_return_12),
        "prior_location_1": _decimal_or_none(value.prior_location_1),
        "prior_location_12": _decimal_or_none(value.prior_location_12),
        "prior_mad_1": _decimal_or_none(value.prior_mad_1),
        "prior_mad_12": _decimal_or_none(value.prior_mad_12),
        "prior_observation_count": value.prior_observation_count,
        "prior_scale_1": _decimal_or_none(value.prior_scale_1),
        "prior_scale_12": _decimal_or_none(value.prior_scale_12),
        "reason": value.reason,
        "rule_version": value.rule_version,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _require_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PriceKlineM1ProjectionContractErrorV2(f"{name} must be a lowercase SHA-256 digest")


def _is_positive_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_canonical_nonnegative_integer(value: object) -> bool:
    return type(value) is int and 0 <= value <= _MAX_CANONICAL_INTEGER


def _is_canonical_positive_integer(value: object) -> bool:
    return type(value) is int and 0 < value <= _MAX_CANONICAL_INTEGER
