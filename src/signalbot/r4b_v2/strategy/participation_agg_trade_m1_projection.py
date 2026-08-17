"""M1-only participation projection over exact USD-M aggregate-trade rows."""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException, localcontext
from enum import StrEnum
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmAggTradeM1V2,
    UsdmMarketM1ContractErrorV2,
    canonical_usdm_market_m1_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import ROBUST_Z_PRIOR_WINDOW_V2
from signalbot.r4b_v2.strategy.family_a_features import (
    FamilyAContractMultiplierV2,
    FamilyAFeatureContractErrorV2,
    build_family_a_contract_multiplier_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    PARTICIPATION_FLOW_RULE_VERSION_V2,
    ParticipationFlowBarValueV2,
    ParticipationFlowCalculationV2,
    ParticipationFlowContractErrorV2,
    build_participation_flow_bar_value_v2,
    calculate_participation_flow_v2,
    canonical_participation_flow_bar_value_v2,
    canonical_participation_flow_calculation_v2,
)

PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 1
PARTICIPATION_AGG_TRADE_M1_AUTHORITY_STATUS_V2: Final = "M1_ONLY_UNBOUND"

_SCHEMA_VERSION: Final = "r4b_participation_agg_trade_m1_projection_v2"
_SOURCE_ENTRY_SCHEMA: Final = "r4b_participation_agg_trade_m1_lineage_entry_v2"
_ECONOMIC_ENTRY_SCHEMA: Final = "r4b_participation_agg_trade_m1_economic_entry_v2"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_AGG_TRADE_M1_SOURCE_ROOT_V2\0"
_ECONOMIC_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_AGG_TRADE_M1_ECONOMIC_ROOT_V2\0"
_PROJECTION_DOMAIN: Final = b"R4B_PARTICIPATION_AGG_TRADE_M1_PROJECTION_V2\0"
_FACTORY_TOKEN: Final = object()
_ENTRY_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_CANONICAL_INTEGER: Final = 2**53 - 1


class ParticipationAggTradeM1ProjectionContractErrorV2(ValueError):
    """Raised when an aggTrade projection violates its M1-only contract."""


class ParticipationAggTradeM1ProjectionStatusV2(StrEnum):
    NUMERIC_READY_M1_ONLY = "NUMERIC_READY_M1_ONLY"
    NUMERIC_NONREADY_M1_ONLY = "NUMERIC_NONREADY_M1_ONLY"
    UNAVAILABLE_MISSING_SLOT_UNKNOWN = "UNAVAILABLE_MISSING_SLOT_UNKNOWN"
    UNAVAILABLE_OBSERVED_SEQUENCE_GAP = "UNAVAILABLE_OBSERVED_SEQUENCE_GAP"


@dataclass(frozen=True, slots=True)
class ParticipationAggTradeM1LineageEntryV2:
    """One complete source-lineage snapshot for a retained aggTrade M1 row."""

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
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    source_event_ms: int
    data_time_ms: int
    slot_open_ms: int
    aggregate_trade_id: int
    first_trade_id: int
    last_trade_id: int
    price: Decimal
    quantity: Decimal
    normal_quantity: Decimal
    buyer_maker: bool
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation M1 lineage entries require their factory"
            )
        _validate_lineage_entry(self)


@dataclass(frozen=True, slots=True)
class ParticipationAggTradeM1EconomicEntryV2:
    """One source-independent trade row consumed by flow aggregation."""

    slot_open_ms: int
    aggregate_trade_id: int
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    price: Decimal
    quantity: Decimal
    normal_quantity: Decimal
    contract_multiplier: Decimal
    buyer_maker: bool
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation M1 economic entries require their factory"
            )
        _validate_economic_entry(self)


@dataclass(frozen=True, slots=True)
class ParticipationAggTradeM1ProjectionV2:
    """Observed aggTrade calculation that never claims capture completeness."""

    attempt_id: str
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
    expected_first_slot_open_ms: int
    latest_trade_time_ms: int
    latest_source_event_ms: int
    latest_receipt_wall_ms: int
    latest_receipt_monotonic_ns: int
    contract_multiplier_authority: FamilyAContractMultiplierV2
    ordered_source_lineage: tuple[ParticipationAggTradeM1LineageEntryV2, ...]
    economic_flow_rows: tuple[ParticipationAggTradeM1EconomicEntryV2, ...]
    observed_slot_values: tuple[ParticipationFlowBarValueV2, ...]
    missing_slot_open_ms: tuple[int, ...]
    aggregate_id_contiguous_observed: bool
    raw_trade_id_contiguous_observed: bool
    source_lineage_root_sha256: str
    economic_flow_root_sha256: str
    calculation: ParticipationFlowCalculationV2 | None
    status: ParticipationAggTradeM1ProjectionStatusV2
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    projection_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=PARTICIPATION_FLOW_RULE_VERSION_V2,
    )
    authority_status: Literal["M1_ONLY_UNBOUND"] = field(
        init=False,
        default="M1_ONLY_UNBOUND",
    )
    data_through_ms: None = field(init=False, default=None)
    m2_certificate_sha256: None = field(init=False, default=None)
    causal_inputs_complete: Literal[False] = field(init=False, default=False)
    producer_ready: Literal[False] = field(init=False, default=False)
    promoting_eligible: Literal[False] = field(init=False, default=False)
    exchange_trade_capture_complete: Literal[False] = field(
        init=False,
        default=False,
    )
    slot_absence_interpretation: Literal["UNKNOWN_NOT_ZERO"] = field(
        init=False,
        default="UNKNOWN_NOT_ZERO",
    )
    expected_slot_count: int = field(
        init=False,
        default=PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation M1 projections require their canonical factory"
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
        return self.calculation is not None and self.calculation.ready

    @property
    def all_expected_slots_nonempty_observed(self) -> bool:
        return not self.missing_slot_open_ms


def build_participation_agg_trade_m1_projection_v2(
    *,
    attempt_id: str,
    bar_open_ms: int,
    rows: tuple[UsdmAggTradeM1V2, ...],
    contract_multiplier_authority: FamilyAContractMultiplierV2,
) -> ParticipationAggTradeM1ProjectionV2:
    """Project retained aggTrades into the frozen 8,641-slot flow formula."""

    _validate_identity(attempt_id, "attempt_id")
    if type(bar_open_ms) is not int or bar_open_ms < 0 or bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation decision bar must be an aligned nonnegative 5m slot"
        )
    if type(rows) is not tuple or not rows:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation M1 rows must be a non-empty immutable tuple"
        )
    if any(not isinstance(row, UsdmAggTradeM1V2) for row in rows):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation M1 input contains a non-aggTrade row"
        )

    canonical_rows: list[tuple[UsdmAggTradeM1V2, bytes]] = []
    for row in rows:
        try:
            canonical = canonical_usdm_market_m1_v2(row)
        except (TypeError, ValueError, UsdmMarketM1ContractErrorV2) as exc:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation input row is not canonical factory-valid M1"
            ) from exc
        canonical_rows.append((row, canonical))
    ordered = tuple(
        sorted(
            canonical_rows,
            key=lambda item: (
                item[0].trade_time_ms,
                item[0].aggregate_trade_id,
                item[0].m1_payload_sha256,
            ),
        )
    )
    current = ordered[-1][0]
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    decision_cutoff_ms = bar_close_ms + DECISION_DELAY_MS_V2
    expected_first_slot_open_ms = bar_open_ms - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    if expected_first_slot_open_ms < 0:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation window would precede Unix epoch"
        )
    aggregate_contiguous, raw_contiguous = _validate_ordered_rows(
        tuple(item[0] for item in ordered),
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    _validate_multiplier_authority(
        contract_multiplier_authority,
        attempt_id=attempt_id,
        symbol=current.symbol,
        venue=current.venue,
        promoting_plan_sha256=current.promoting_plan_sha256,
        effective_from_ms=expected_first_slot_open_ms,
        effective_until_ms=bar_close_ms,
    )

    lineage = tuple(_lineage_entry(row, canonical) for row, canonical in ordered)
    economic = tuple(
        _economic_entry(row, contract_multiplier_authority.contract_multiplier)
        for row, _canonical in ordered
    )
    observed_values = _aggregate_economic_rows(economic)
    expected_slots = tuple(
        expected_first_slot_open_ms + index * FIVE_MINUTE_MS_V2
        for index in range(PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2)
    )
    observed_slots = {value.bar_open_ms for value in observed_values}
    missing_slots = tuple(slot for slot in expected_slots if slot not in observed_slots)
    calculation: ParticipationFlowCalculationV2 | None = None
    if not missing_slots and aggregate_contiguous and raw_contiguous:
        calculation = calculate_participation_flow_v2(
            current_bar=observed_values[-1],
            prior_bars=observed_values[:-1],
        )
    status = _projection_status(
        missing_slots=missing_slots,
        aggregate_contiguous=aggregate_contiguous,
        raw_contiguous=raw_contiguous,
        calculation=calculation,
    )
    reasons = _projection_reasons(status, calculation)
    source_root = _source_root(
        lineage,
        attempt_id=attempt_id,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        multiplier=contract_multiplier_authority,
    )
    economic_root = _economic_root(
        economic,
        observed_values,
        symbol=current.symbol,
        venue=current.venue,
        promoting_plan_sha256=current.promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        multiplier=contract_multiplier_authority,
    )
    return ParticipationAggTradeM1ProjectionV2(
        attempt_id=attempt_id,
        symbol=current.symbol,
        venue=current.venue,
        route_id=current.route_id,
        stream=current.stream,
        promoting_plan_sha256=current.promoting_plan_sha256,
        plan_id=current.plan_id,
        protocol_sha256=current.protocol_sha256,
        parser_contract_sha256=current.parser_contract_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        latest_trade_time_ms=max(value.data_time_ms for value in lineage),
        latest_source_event_ms=max(value.source_event_ms for value in lineage),
        latest_receipt_wall_ms=max(value.receipt_wall_ms for value in lineage),
        latest_receipt_monotonic_ns=lineage[-1].receipt_monotonic_ns,
        contract_multiplier_authority=contract_multiplier_authority,
        ordered_source_lineage=lineage,
        economic_flow_rows=economic,
        observed_slot_values=observed_values,
        missing_slot_open_ms=missing_slots,
        aggregate_id_contiguous_observed=aggregate_contiguous,
        raw_trade_id_contiguous_observed=raw_contiguous,
        source_lineage_root_sha256=source_root,
        economic_flow_root_sha256=economic_root,
        calculation=calculation,
        status=status,
        reasons=reasons,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_participation_agg_trade_m1_projection_v2(
    value: ParticipationAggTradeM1ProjectionV2,
) -> bytes:
    """Serialize and live-check one sealed participation M1 projection."""

    if not isinstance(value, ParticipationAggTradeM1ProjectionV2):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "value must be ParticipationAggTradeM1ProjectionV2"
        )
    _validate_projection(value)
    expected_source = _source_root(
        value.ordered_source_lineage,
        attempt_id=value.attempt_id,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        decision_cutoff_ms=value.decision_cutoff_ms,
        multiplier=value.contract_multiplier_authority,
    )
    if value.source_lineage_root_sha256 != expected_source:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation source lineage differs from its sealed root"
        )
    expected_economic = _economic_root(
        value.economic_flow_rows,
        value.observed_slot_values,
        symbol=value.symbol,
        venue=value.venue,
        promoting_plan_sha256=value.promoting_plan_sha256,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        multiplier=value.contract_multiplier_authority,
    )
    if value.economic_flow_root_sha256 != expected_economic:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation economic flow differs from its sealed root"
        )
    rebuilt_values = _aggregate_economic_rows(value.economic_flow_rows)
    if value.observed_slot_values != rebuilt_values:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation slot values differ from exact economic trades"
        )
    expected_calculation: ParticipationFlowCalculationV2 | None = None
    if (
        not value.missing_slot_open_ms
        and value.aggregate_id_contiguous_observed
        and value.raw_trade_id_contiguous_observed
    ):
        expected_calculation = calculate_participation_flow_v2(
            current_bar=rebuilt_values[-1],
            prior_bars=rebuilt_values[:-1],
        )
    if value.calculation != expected_calculation:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation calculation differs from its observed slot values"
        )
    expected_projection = hashlib.sha256(
        _PROJECTION_DOMAIN
        + canonical_json_line(_projection_document(value, include_projection_hash=False))
    ).hexdigest()
    if value.projection_sha256 != expected_projection:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation M1 projection differs from canonical content"
        )
    return canonical_json_line(_projection_document(value, include_projection_hash=True))


def _validate_ordered_rows(
    rows: tuple[UsdmAggTradeM1V2, ...],
    *,
    expected_first_slot_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> tuple[bool, bool]:
    first = rows[0]
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
        or first.stream != f"{first.symbol.lower()}@aggTrade"
        or first.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation source route, stream, or parser is not exact"
        )
    seen_aggregate: dict[int, str] = {}
    seen_m0: set[str] = set()
    seen_raw: set[str] = set()
    seen_m1: set[str] = set()
    seen_cursors: set[tuple[str, str, int, int]] = set()
    last_session: dict[str, tuple[int, int, int]] = {}
    last_owner_frame: dict[tuple[str, str, int], int] = {}
    aggregate_contiguous = True
    raw_contiguous = True
    previous: UsdmAggTradeM1V2 | None = None
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
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation rows disagree on symbol, stream, plan, protocol, or parser"
            )
        if (
            not row.parser_bound
            or not row.live_reverification_required
            or row.current_authority_claimed
            or row.cursor_complete
            or row.causal_inputs_complete
        ):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation row exposes an unsupported authority or cursor claim"
            )
        prior_hash = seen_aggregate.get(row.aggregate_trade_id)
        if prior_hash is not None:
            qualifier = "duplicate" if prior_hash == row.m1_payload_sha256 else "conflicting"
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                f"participation source contains a {qualifier} aggregate trade ID"
            )
        seen_aggregate[row.aggregate_trade_id] = row.m1_payload_sha256
        if row.m0_leaf_sha256 in seen_m0:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation source repeats an M0 membership leaf"
            )
        if row.raw_payload_hash_v2 in seen_raw or row.m1_payload_sha256 in seen_m1:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation source repeats raw or parsed evidence"
            )
        seen_m0.add(row.m0_leaf_sha256)
        seen_raw.add(row.raw_payload_hash_v2)
        seen_m1.add(row.m1_payload_sha256)
        cursor = (row.session_id, row.connection_id, row.generation, row.frame_seq)
        if cursor in seen_cursors:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation source repeats a WebSocket cursor"
            )
        seen_cursors.add(cursor)
        slot_open_ms = _slot_open_ms(row.trade_time_ms)
        if slot_open_ms < expected_first_slot_open_ms or row.trade_time_ms > bar_close_ms:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation trade T lies outside the exact 8,641-slot window"
            )
        if row.event_ms > row.receipt_wall_ms or row.receipt_wall_ms > decision_cutoff_ms:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation T, E, receipt, or current D is noncausal"
            )
        session = last_session.get(row.session_id)
        if session is not None and (
            row.ingest_seq <= session[0]
            or row.receipt_wall_ms < session[1]
            or row.receipt_monotonic_ns <= session[2]
        ):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation receipt or ingest cursor is not ordered within session"
            )
        last_session[row.session_id] = (
            row.ingest_seq,
            row.receipt_wall_ms,
            row.receipt_monotonic_ns,
        )
        owner = (row.session_id, row.connection_id, row.generation)
        prior_frame = last_owner_frame.get(owner)
        if prior_frame is not None and row.frame_seq <= prior_frame:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation frame cursor is not ordered within owner generation"
            )
        last_owner_frame[owner] = row.frame_seq
        if previous is not None:
            if row.aggregate_trade_id <= previous.aggregate_trade_id:
                raise ParticipationAggTradeM1ProjectionContractErrorV2(
                    "participation aggregate IDs conflict with T ordering"
                )
            if row.first_trade_id <= previous.last_trade_id:
                raise ParticipationAggTradeM1ProjectionContractErrorV2(
                    "participation raw trade ID intervals overlap or conflict"
                )
            aggregate_contiguous &= row.aggregate_trade_id == previous.aggregate_trade_id + 1
            raw_contiguous &= row.first_trade_id == previous.last_trade_id + 1
            if row.event_ms < previous.event_ms or row.receipt_wall_ms < previous.receipt_wall_ms:
                raise ParticipationAggTradeM1ProjectionContractErrorV2(
                    "participation exchange events or receipts regress in T order"
                )
        previous = row
    return aggregate_contiguous, raw_contiguous


def _validate_multiplier_authority(
    value: FamilyAContractMultiplierV2,
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    effective_from_ms: int,
    effective_until_ms: int,
) -> None:
    if not isinstance(value, FamilyAContractMultiplierV2):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation requires sealed contract multiplier authority"
        )
    try:
        rebuilt = build_family_a_contract_multiplier_v2(
            attempt_id=value.attempt_id,
            symbol=value.symbol,
            venue=value.venue,
            promoting_plan_sha256=value.promoting_plan_sha256,
            contract_multiplier=value.contract_multiplier,
            effective_from_ms=value.effective_from_ms,
            effective_until_ms=value.effective_until_ms,
            source_root_sha256=value.source_root_sha256,
            schema_sha256=value.schema_sha256,
        )
    except FamilyAFeatureContractErrorV2 as exc:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "contract multiplier authority is not factory-valid"
        ) from exc
    if rebuilt != value:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "contract multiplier authority differs from its sealed version"
        )
    if (
        (value.attempt_id, value.symbol, value.venue, value.promoting_plan_sha256)
        != (attempt_id, symbol, venue, promoting_plan_sha256)
        or value.effective_from_ms > effective_from_ms
        or value.effective_until_ms < effective_until_ms
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "contract multiplier authority does not cover the exact decision window"
        )


def _lineage_entry(
    row: UsdmAggTradeM1V2,
    canonical: bytes,
) -> ParticipationAggTradeM1LineageEntryV2:
    return ParticipationAggTradeM1LineageEntryV2(
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
        receipt_wall_ms=row.receipt_wall_ms,
        receipt_monotonic_ns=row.receipt_monotonic_ns,
        source_event_ms=row.event_ms,
        data_time_ms=row.trade_time_ms,
        slot_open_ms=_slot_open_ms(row.trade_time_ms),
        aggregate_trade_id=row.aggregate_trade_id,
        first_trade_id=row.first_trade_id,
        last_trade_id=row.last_trade_id,
        price=row.price,
        quantity=row.quantity,
        normal_quantity=row.normal_quantity,
        buyer_maker=row.buyer_maker,
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _economic_entry(
    row: UsdmAggTradeM1V2,
    multiplier: Decimal,
) -> ParticipationAggTradeM1EconomicEntryV2:
    return ParticipationAggTradeM1EconomicEntryV2(
        slot_open_ms=_slot_open_ms(row.trade_time_ms),
        aggregate_trade_id=row.aggregate_trade_id,
        first_trade_id=row.first_trade_id,
        last_trade_id=row.last_trade_id,
        trade_time_ms=row.trade_time_ms,
        price=row.price,
        quantity=row.quantity,
        normal_quantity=row.normal_quantity,
        contract_multiplier=multiplier,
        buyer_maker=row.buyer_maker,
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _aggregate_economic_rows(
    rows: tuple[ParticipationAggTradeM1EconomicEntryV2, ...],
) -> tuple[ParticipationFlowBarValueV2, ...]:
    grouped: defaultdict[int, list[ParticipationAggTradeM1EconomicEntryV2]] = defaultdict(list)
    for row in rows:
        _validate_economic_entry(row)
        grouped[row.slot_open_ms].append(row)
    values: list[ParticipationFlowBarValueV2] = []
    try:
        with localcontext(protocol_decimal_context_v2()):
            for slot_open_ms in sorted(grouped):
                trades = grouped[slot_open_ms]
                normal_buy = sum(
                    (
                        value.price * value.normal_quantity * value.contract_multiplier
                        for value in trades
                        if not value.buyer_maker
                    ),
                    Decimal(0),
                )
                normal_sell = sum(
                    (
                        value.price * value.normal_quantity * value.contract_multiplier
                        for value in trades
                        if value.buyer_maker
                    ),
                    Decimal(0),
                )
                total = sum(
                    (value.price * value.quantity * value.contract_multiplier for value in trades),
                    Decimal(0),
                )
                signed = normal_buy - normal_sell
                normal = normal_buy + normal_sell
                signed_share = signed / total if normal > 0 and total > 0 else None
                values.append(
                    build_participation_flow_bar_value_v2(
                        bar_open_ms=slot_open_ms,
                        bar_close_ms=slot_open_ms + FIVE_MINUTE_MS_V2 - 1,
                        signed_normal_notional=signed,
                        normal_notional=normal,
                        total_trade_notional=total,
                        signed_share=signed_share,
                    )
                )
    except DecimalException as exc:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation trade aggregation decimal arithmetic failed"
        ) from exc
    return tuple(values)


def _projection_status(
    *,
    missing_slots: tuple[int, ...],
    aggregate_contiguous: bool,
    raw_contiguous: bool,
    calculation: ParticipationFlowCalculationV2 | None,
) -> ParticipationAggTradeM1ProjectionStatusV2:
    if missing_slots:
        return ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN
    if not aggregate_contiguous or not raw_contiguous:
        return ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_OBSERVED_SEQUENCE_GAP
    if calculation is not None and calculation.ready:
        return ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_READY_M1_ONLY
    return ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_NONREADY_M1_ONLY


def _projection_reasons(
    status: ParticipationAggTradeM1ProjectionStatusV2,
    calculation: ParticipationFlowCalculationV2 | None,
) -> tuple[str, ...]:
    primary = {
        ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_READY_M1_ONLY: (
            "OBSERVED_M1_FLOW_CALCULATION_NUMERIC_READY"
        ),
        ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_NONREADY_M1_ONLY: (
            "OBSERVED_M1_FLOW_CALCULATION_NUMERIC_NONREADY"
        ),
        ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN: (
            "MISSING_AGGTRADE_SLOT_IS_UNKNOWN_NOT_ZERO"
        ),
        ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_OBSERVED_SEQUENCE_GAP: (
            "OBSERVED_AGGREGATE_OR_RAW_TRADE_ID_GAP"
        ),
    }[status]
    calculation_reason = calculation.reason if calculation is not None else "CALCULATION_WITHHELD"
    return (
        primary,
        calculation_reason,
        "M1_DOES_NOT_PROVE_EXCHANGE_TRADE_CAPTURE_COMPLETENESS",
        "M2_CURSOR_FINALITY_AND_CAUSAL_INPUT_COMPLETENESS_UNBOUND",
        "NUMERIC_READY_DOES_NOT_IMPLY_PRODUCER_READY_OR_PROMOTION",
    )


def _validate_projection(value: ParticipationAggTradeM1ProjectionV2) -> None:
    if value.schema_version != _SCHEMA_VERSION:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "unsupported participation M1 projection schema"
        )
    _validate_identity(value.attempt_id, "attempt_id")
    if (
        _SYMBOL_RE.fullmatch(value.symbol) is None
        or value.venue is not VenueV2.USDM_FUTURES
        or value.route_id != "usdm_market"
        or value.stream != f"{value.symbol.lower()}@aggTrade"
        or value.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation projection identity is not exact USD-M aggTrade"
        )
    for digest, name in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.protocol_sha256, "protocol_sha256"),
        (value.parser_contract_sha256, "parser_contract_sha256"),
        (value.source_lineage_root_sha256, "source_lineage_root_sha256"),
        (value.economic_flow_root_sha256, "economic_flow_root_sha256"),
    ):
        _require_sha256(digest, name)
    if (
        not _is_canonical_nonnegative_integer(value.bar_open_ms)
        or value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or not _is_canonical_nonnegative_integer(value.bar_close_ms)
        or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        or not _is_canonical_nonnegative_integer(value.decision_cutoff_ms)
        or value.decision_cutoff_ms != value.bar_close_ms + DECISION_DELAY_MS_V2
        or value.expected_first_slot_open_ms
        != value.bar_open_ms - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation projection decision window is inconsistent"
        )
    for integer, name in (
        (value.latest_trade_time_ms, "latest_trade_time_ms"),
        (value.latest_source_event_ms, "latest_source_event_ms"),
        (value.latest_receipt_wall_ms, "latest_receipt_wall_ms"),
        (value.latest_receipt_monotonic_ns, "latest_receipt_monotonic_ns"),
    ):
        if not _is_canonical_nonnegative_integer(integer):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                f"{name} exceeds the inherited canonical integer domain"
            )
    if (
        value.authority_status != PARTICIPATION_AGG_TRADE_M1_AUTHORITY_STATUS_V2
        or value.data_through_ms is not None
        or value.m2_certificate_sha256 is not None
        or value.causal_inputs_complete
        or value.producer_ready
        or value.promoting_eligible
        or value.exchange_trade_capture_complete
        or value.slot_absence_interpretation != "UNKNOWN_NOT_ZERO"
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "M1-only participation cannot claim M2 or producer authority"
        )
    _validate_multiplier_authority(
        value.contract_multiplier_authority,
        attempt_id=value.attempt_id,
        symbol=value.symbol,
        venue=value.venue,
        promoting_plan_sha256=value.promoting_plan_sha256,
        effective_from_ms=value.expected_first_slot_open_ms,
        effective_until_ms=value.bar_close_ms,
    )
    if (
        type(value.ordered_source_lineage) is not tuple
        or not value.ordered_source_lineage
        or type(value.economic_flow_rows) is not tuple
        or len(value.economic_flow_rows) != len(value.ordered_source_lineage)
        or type(value.observed_slot_values) is not tuple
        or not value.observed_slot_values
        or len(value.observed_slot_values) > PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation projection source/economic row bounds are inconsistent"
        )
    for entry in value.ordered_source_lineage:
        _validate_lineage_entry(entry)
    for entry in value.economic_flow_rows:
        _validate_economic_entry(entry)
    for bar_value in value.observed_slot_values:
        try:
            canonical_participation_flow_bar_value_v2(bar_value)
        except ParticipationFlowContractErrorV2 as exc:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation observed slot value is not canonical"
            ) from exc
    expected_missing = _expected_missing_slots(
        first_slot_open_ms=value.expected_first_slot_open_ms,
        observed=value.observed_slot_values,
    )
    if value.missing_slot_open_ms != expected_missing:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation missing-slot set differs from observed slots"
        )
    if value.calculation is not None:
        try:
            canonical_participation_flow_calculation_v2(value.calculation)
        except ParticipationFlowContractErrorV2 as exc:
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                "participation projection calculation is not canonical"
            ) from exc
    expected_status = _projection_status(
        missing_slots=value.missing_slot_open_ms,
        aggregate_contiguous=value.aggregate_id_contiguous_observed,
        raw_contiguous=value.raw_trade_id_contiguous_observed,
        calculation=value.calculation,
    )
    if value.status is not expected_status or value.reasons != _projection_reasons(
        expected_status, value.calculation
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation status or reasons contradict observed availability"
        )
    if (
        value.latest_trade_time_ms
        != max(entry.data_time_ms for entry in value.ordered_source_lineage)
        or value.latest_source_event_ms
        != max(entry.source_event_ms for entry in value.ordered_source_lineage)
        or value.latest_receipt_wall_ms
        != max(entry.receipt_wall_ms for entry in value.ordered_source_lineage)
        or value.latest_receipt_monotonic_ns
        != value.ordered_source_lineage[-1].receipt_monotonic_ns
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation latest source clocks differ from ordered lineage"
        )


def _validate_lineage_entry(value: ParticipationAggTradeM1LineageEntryV2) -> None:
    if (
        _SYMBOL_RE.fullmatch(value.symbol) is None
        or value.venue is not VenueV2.USDM_FUTURES
        or value.route_id != "usdm_market"
        or value.stream != f"{value.symbol.lower()}@aggTrade"
        or value.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation lineage identity is not exact"
        )
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
    for identity, name in (
        (value.session_id, "session_id"),
        (value.plan_id, "plan_id"),
        (value.connection_id, "connection_id"),
    ):
        _validate_identity(identity, name)
    for integer, name in (
        (value.generation, "generation"),
        (value.frame_seq, "frame_seq"),
        (value.ingest_seq, "ingest_seq"),
    ):
        if not _is_canonical_positive_integer(integer):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                f"participation lineage {name} must be a canonical positive integer"
            )
    for integer, name in (
        (value.receipt_wall_ms, "receipt_wall_ms"),
        (value.receipt_monotonic_ns, "receipt_monotonic_ns"),
        (value.source_event_ms, "source_event_ms"),
        (value.data_time_ms, "data_time_ms"),
        (value.slot_open_ms, "slot_open_ms"),
        (value.aggregate_trade_id, "aggregate_trade_id"),
        (value.first_trade_id, "first_trade_id"),
        (value.last_trade_id, "last_trade_id"),
    ):
        if not _is_canonical_nonnegative_integer(integer):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                f"participation lineage {name} exceeds canonical integer bounds"
            )
    if (
        value.slot_open_ms != _slot_open_ms(value.data_time_ms)
        or value.source_event_ms < value.data_time_ms
        or value.source_event_ms > value.receipt_wall_ms
        or value.first_trade_id > value.last_trade_id
        or not _is_positive_decimal(value.price)
        or not _is_positive_decimal(value.quantity)
        or not _is_nonnegative_decimal(value.normal_quantity)
        or value.normal_quantity > value.quantity
        or type(value.buyer_maker) is not bool
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation lineage trade economics or clocks are invalid"
        )


def _validate_economic_entry(value: ParticipationAggTradeM1EconomicEntryV2) -> None:
    for integer, name in (
        (value.slot_open_ms, "slot_open_ms"),
        (value.aggregate_trade_id, "aggregate_trade_id"),
        (value.first_trade_id, "first_trade_id"),
        (value.last_trade_id, "last_trade_id"),
        (value.trade_time_ms, "trade_time_ms"),
    ):
        if not _is_canonical_nonnegative_integer(integer):
            raise ParticipationAggTradeM1ProjectionContractErrorV2(
                f"participation economic {name} exceeds canonical integer bounds"
            )
    if (
        value.slot_open_ms != _slot_open_ms(value.trade_time_ms)
        or value.first_trade_id > value.last_trade_id
        or not _is_positive_decimal(value.price)
        or not _is_positive_decimal(value.quantity)
        or not _is_nonnegative_decimal(value.normal_quantity)
        or value.normal_quantity > value.quantity
        or not _is_positive_decimal(value.contract_multiplier)
        or type(value.buyer_maker) is not bool
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            "participation economic trade entry is invalid"
        )


def _expected_missing_slots(
    *,
    first_slot_open_ms: int,
    observed: tuple[ParticipationFlowBarValueV2, ...],
) -> tuple[int, ...]:
    observed_slots = {value.bar_open_ms for value in observed}
    return tuple(
        first_slot_open_ms + index * FIVE_MINUTE_MS_V2
        for index in range(PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2)
        if first_slot_open_ms + index * FIVE_MINUTE_MS_V2 not in observed_slots
    )


def _source_root(
    entries: tuple[ParticipationAggTradeM1LineageEntryV2, ...],
    *,
    attempt_id: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    multiplier: FamilyAContractMultiplierV2,
) -> str:
    return hashlib.sha256(
        _SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": attempt_id,
                "authority_status": PARTICIPATION_AGG_TRADE_M1_AUTHORITY_STATUS_V2,
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "contract_multiplier_authority": _multiplier_document(multiplier),
                "decision_cutoff_ms": decision_cutoff_ms,
                "ordered_rows": [_lineage_document(entry) for entry in entries],
                "row_count": len(entries),
                "schema_version": "r4b_participation_agg_trade_m1_source_root_v2",
            }
        )
    ).hexdigest()


def _economic_root(
    entries: tuple[ParticipationAggTradeM1EconomicEntryV2, ...],
    slot_values: tuple[ParticipationFlowBarValueV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    multiplier: FamilyAContractMultiplierV2,
) -> str:
    return hashlib.sha256(
        _ECONOMIC_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "contract_multiplier": str(multiplier.contract_multiplier),
                "contract_multiplier_version_sha256": multiplier.version_sha256,
                "ordered_trades": [_economic_document(entry) for entry in entries],
                "promoting_plan_sha256": promoting_plan_sha256,
                "schema_version": "r4b_participation_agg_trade_m1_economic_root_v2",
                "slot_values": [_bar_value_summary(value) for value in slot_values],
                "symbol": symbol,
                "venue": venue.value,
            }
        )
    ).hexdigest()


def _projection_document(
    value: ParticipationAggTradeM1ProjectionV2,
    *,
    include_projection_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "aggregate_id_contiguous_observed": (value.aggregate_id_contiguous_observed),
        "attempt_id": value.attempt_id,
        "authority_status": value.authority_status,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "calculation": _calculation_summary(value.calculation),
        "causal_inputs_complete": value.causal_inputs_complete,
        "contract_multiplier_authority": _multiplier_document(value.contract_multiplier_authority),
        "data_through_ms": value.data_through_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "economic_flow_root_sha256": value.economic_flow_root_sha256,
        "economic_flow_rows": [_economic_document(entry) for entry in value.economic_flow_rows],
        "exchange_trade_capture_complete": value.exchange_trade_capture_complete,
        "expected_first_slot_open_ms": value.expected_first_slot_open_ms,
        "expected_slot_count": value.expected_slot_count,
        "latest_receipt_monotonic_ns": value.latest_receipt_monotonic_ns,
        "latest_receipt_wall_ms": value.latest_receipt_wall_ms,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_trade_time_ms": value.latest_trade_time_ms,
        "m2_certificate_sha256": value.m2_certificate_sha256,
        "missing_slot_open_ms": list(value.missing_slot_open_ms),
        "observed_slot_values": [_bar_value_summary(item) for item in value.observed_slot_values],
        "ordered_source_lineage": [
            _lineage_document(entry) for entry in value.ordered_source_lineage
        ],
        "parser_contract_sha256": value.parser_contract_sha256,
        "plan_id": value.plan_id,
        "producer_ready": value.producer_ready,
        "promoting_eligible": value.promoting_eligible,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "protocol_sha256": value.protocol_sha256,
        "raw_trade_id_contiguous_observed": (value.raw_trade_id_contiguous_observed),
        "reasons": list(value.reasons),
        "route_id": value.route_id,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "slot_absence_interpretation": value.slot_absence_interpretation,
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "status": value.status.value,
        "stream": value.stream,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_projection_hash:
        document["projection_sha256"] = value.projection_sha256
    return document


def _lineage_document(
    value: ParticipationAggTradeM1LineageEntryV2,
) -> dict[str, object]:
    return {
        "aggregate_trade_id": value.aggregate_trade_id,
        "buyer_maker": value.buyer_maker,
        "capture_authority_sha256": value.capture_authority_sha256,
        "connection_id": value.connection_id,
        "data_time_ms": value.data_time_ms,
        "first_trade_id": value.first_trade_id,
        "frame_seq": value.frame_seq,
        "generation": value.generation,
        "ingest_seq": value.ingest_seq,
        "last_trade_id": value.last_trade_id,
        "m0_leaf_sha256": value.m0_leaf_sha256,
        "m1_canonical_sha256": value.m1_canonical_sha256,
        "m1_payload_sha256": value.m1_payload_sha256,
        "normal_quantity": str(value.normal_quantity),
        "parser_contract_sha256": value.parser_contract_sha256,
        "plan_id": value.plan_id,
        "price": str(value.price),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "protocol_sha256": value.protocol_sha256,
        "quantity": str(value.quantity),
        "raw_payload_hash_v2": value.raw_payload_hash_v2,
        "receipt_monotonic_ns": value.receipt_monotonic_ns,
        "receipt_wall_ms": value.receipt_wall_ms,
        "route_id": value.route_id,
        "schema_version": _SOURCE_ENTRY_SCHEMA,
        "session_id": value.session_id,
        "slot_open_ms": value.slot_open_ms,
        "source_event_ms": value.source_event_ms,
        "stream": value.stream,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _economic_document(
    value: ParticipationAggTradeM1EconomicEntryV2,
) -> dict[str, object]:
    return {
        "aggregate_trade_id": value.aggregate_trade_id,
        "buyer_maker": value.buyer_maker,
        "contract_multiplier": str(value.contract_multiplier),
        "first_trade_id": value.first_trade_id,
        "last_trade_id": value.last_trade_id,
        "normal_quantity": str(value.normal_quantity),
        "price": str(value.price),
        "quantity": str(value.quantity),
        "schema_version": _ECONOMIC_ENTRY_SCHEMA,
        "slot_open_ms": value.slot_open_ms,
        "trade_time_ms": value.trade_time_ms,
    }


def _bar_value_summary(value: ParticipationFlowBarValueV2) -> dict[str, object]:
    canonical_participation_flow_bar_value_v2(value)
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "bar_value_sha256": value.bar_value_sha256,
        "normal_notional": str(value.normal_notional),
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": None if value.signed_share is None else str(value.signed_share),
        "total_trade_notional": str(value.total_trade_notional),
    }


def _calculation_summary(
    value: ParticipationFlowCalculationV2 | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    canonical_participation_flow_calculation_v2(value)
    return {
        "activity_support": _decimal_or_none(value.activity_support),
        "calculation_sha256": value.calculation_sha256,
        "current_signed_share": _decimal_or_none(value.current_signed_share),
        "current_total_trade_notional": _decimal_or_none(value.current_total_trade_notional),
        "direction": value.direction,
        "prior_observation_count": value.prior_observation_count,
        "prior_signed_share_location": _decimal_or_none(value.prior_signed_share_location),
        "prior_signed_share_mad": _decimal_or_none(value.prior_signed_share_mad),
        "prior_signed_share_scale": _decimal_or_none(value.prior_signed_share_scale),
        "prior_total_notional_median": _decimal_or_none(value.prior_total_notional_median),
        "reason": value.reason,
        "scaled_signed_share_u": _decimal_or_none(value.scaled_signed_share_u),
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }


def _multiplier_document(value: FamilyAContractMultiplierV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "contract_multiplier": str(value.contract_multiplier),
        "effective_from_ms": value.effective_from_ms,
        "effective_until_ms": value.effective_until_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_sha256": value.schema_sha256,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
        "version_sha256": value.version_sha256,
    }


def _slot_open_ms(trade_time_ms: int) -> int:
    return trade_time_ms - trade_time_ms % FIVE_MINUTE_MS_V2


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _validate_identity(value: object, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            f"{name} must be bounded normalized text"
        )


def _require_sha256(value: object, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ParticipationAggTradeM1ProjectionContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _is_positive_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0


def _is_canonical_nonnegative_integer(value: object) -> bool:
    return type(value) is int and 0 <= value <= _MAX_CANONICAL_INTEGER


def _is_canonical_positive_integer(value: object) -> bool:
    return type(value) is int and 0 < value <= _MAX_CANONICAL_INTEGER
