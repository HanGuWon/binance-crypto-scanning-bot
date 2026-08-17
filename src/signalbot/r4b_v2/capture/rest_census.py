from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Final, Literal, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2,
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
    PublicOiRestAttemptPayloadV2,
)

LOCAL_SCHEDULE_EVIDENCE_V2: Final = "LOCAL_SCHEDULE_EVIDENCE"
PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2: Final = 12_000
PUBLIC_OI_REST_WAL_MAX_RECORD_BYTES_V2: Final = 20_000
PUBLIC_OI_REST_ATTEMPT_RECORD_HASH_SEMANTICS_V2: Final = "SHA256_CANONICAL_RAW_RECORD_V2_JSONL"

_ROUTE_ID = "usdm_public_rest"
_SLOT_SCHEMA = "r4b_v2_public_oi_rest_slot_census_v1"
_GAP_SCHEMA = "r4b_v2_public_oi_rest_forward_gap_range_v1"
_CLOSE_SCHEMA = "r4b_v2_public_oi_rest_coverage_close_v1"
_NORMAL_STOP = "NORMAL_STOP"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_IDENTITY_LENGTH = 256

_REST_PLAN_DOMAIN = b"R4B_V2_PUBLIC_OI_REST_PLAN\0"
_SYMBOL_CENSUS_DOMAIN = b"R4B_V2_PUBLIC_OI_SYMBOL_CENSUS\0"
_CELL_ID_DOMAIN = b"R4B_V2_PUBLIC_OI_CELL_ID\0"
_SLOT_EVENT_ID_DOMAIN = b"R4B_V2_PUBLIC_OI_SLOT_EVENT_ID\0"
_GAP_EVENT_ID_DOMAIN = b"R4B_V2_PUBLIC_OI_GAP_EVENT_ID\0"
_CLOSE_EVENT_ID_DOMAIN = b"R4B_V2_PUBLIC_OI_CLOSE_EVENT_ID\0"
_SLOT_PAYLOAD_DOMAIN = b"R4B_V2_PUBLIC_OI_SLOT_PAYLOAD\0"
_GAP_PAYLOAD_DOMAIN = b"R4B_V2_PUBLIC_OI_GAP_PAYLOAD\0"
_CLOSE_PAYLOAD_DOMAIN = b"R4B_V2_PUBLIC_OI_CLOSE_PAYLOAD\0"


class PublicOiRestCellOutcomeV2(StrEnum):
    """One acquisition-schedule outcome, never a data-quality claim."""

    ATTEMPT_RETAINED = "attempt_retained"
    UNSTARTED_SLOT_EXPIRED = "unstarted_slot_expired"
    UNSTARTED_FORWARD_CLOCK_GAP = "unstarted_forward_clock_gap"
    UNSTARTED_NORMAL_STOP = "unstarted_normal_stop"


@dataclass(frozen=True, slots=True)
class PublicOiRestSlotCensusEntryV2:
    symbol_ordinal: int
    cell_event_id: str
    outcome: PublicOiRestCellOutcomeV2
    attempt_ingest_seq: int | None
    attempt_record_sha256: str | None

    def __post_init__(self) -> None:
        _require_nonnegative_int(self.symbol_ordinal, "symbol_ordinal")
        if self.symbol_ordinal >= PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
            raise ValueError("symbol_ordinal exceeds the public OI census bound")
        _require_sha256(self.cell_event_id, "cell_event_id")
        if type(self.outcome) is not PublicOiRestCellOutcomeV2:
            raise TypeError("outcome must be an exact PublicOiRestCellOutcomeV2")
        if self.outcome is PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED:
            _require_positive_int(self.attempt_ingest_seq, "attempt_ingest_seq")
            _require_sha256(self.attempt_record_sha256, "attempt_record_sha256")
        elif self.attempt_ingest_seq is not None or self.attempt_record_sha256 is not None:
            raise ValueError("unstarted public OI cells forbid attempt references")

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        session_start_manifest_sha256: str,
        plan_bundle_sha256: str,
        symbol_ordinal: int,
        scheduled_slot_wall_ms: int,
        outcome: PublicOiRestCellOutcomeV2,
        attempt_ingest_seq: int | None = None,
        attempt_record_sha256: str | None = None,
    ) -> PublicOiRestSlotCensusEntryV2:
        _validate_plan(plan)
        _require_sha256(session_start_manifest_sha256, "session_start_manifest_sha256")
        _require_sha256(plan_bundle_sha256, "plan_bundle_sha256")
        _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
        if symbol_ordinal >= len(plan.symbols):
            raise ValueError("symbol_ordinal is outside the exact REST plan census")
        symbol = plan.symbols[symbol_ordinal]
        return cls(
            symbol_ordinal=symbol_ordinal,
            cell_event_id=public_oi_rest_cell_event_id_v2(
                session_start_manifest_sha256=session_start_manifest_sha256,
                plan_bundle_sha256=plan_bundle_sha256,
                rest_plan_sha256=public_oi_rest_plan_sha256_v2(plan),
                scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                symbol_ordinal=symbol_ordinal,
                symbol=symbol,
            ),
            outcome=outcome,
            attempt_ingest_seq=attempt_ingest_seq,
            attempt_record_sha256=attempt_record_sha256,
        )


@dataclass(frozen=True, slots=True)
class PublicOiRestSlotCensusV2:
    provenance: Literal["LOCAL_SCHEDULE_EVIDENCE"]
    data_completeness_claimed: Literal[False]
    session_id: str
    session_start_manifest_sha256: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_public_rest"]
    rest_plan_sha256: str
    symbols: tuple[str, ...]
    symbol_census_sha256: str
    scheduled_slot_wall_ms: int
    entries: tuple[PublicOiRestSlotCensusEntryV2, ...]
    closed_wall_ms: int
    closed_monotonic_ns: int
    event_id: str
    schema_version: Literal["r4b_v2_public_oi_rest_slot_census_v1"] = (
        "r4b_v2_public_oi_rest_slot_census_v1"
    )

    def __post_init__(self) -> None:
        _validate_common_payload_identity(
            provenance=self.provenance,
            data_completeness_claimed=self.data_completeness_claimed,
            session_id=self.session_id,
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            plan_id=self.plan_id,
            route_id=self.route_id,
            rest_plan_sha256=self.rest_plan_sha256,
            symbols=self.symbols,
            symbol_census_sha256=self.symbol_census_sha256,
        )
        if type(self.schema_version) is not str or self.schema_version != _SLOT_SCHEMA:
            raise ValueError("unsupported public OI slot-census schema")
        _require_aligned_slot(self.scheduled_slot_wall_ms, "scheduled_slot_wall_ms")
        _require_nonnegative_int(self.closed_wall_ms, "closed_wall_ms")
        _require_nonnegative_int(self.closed_monotonic_ns, "closed_monotonic_ns")
        if self.closed_wall_ms < self.scheduled_slot_wall_ms:
            raise ValueError("slot census closed before its UTC slot began")
        if type(self.entries) is not tuple or len(self.entries) != len(self.symbols):
            raise ValueError("slot census requires exactly one entry per planned symbol")
        for ordinal, (symbol, entry) in enumerate(zip(self.symbols, self.entries, strict=True)):
            if type(entry) is not PublicOiRestSlotCensusEntryV2:
                raise TypeError("slot census entries must be exact entry values")
            entry.__post_init__()
            if entry.symbol_ordinal != ordinal:
                raise ValueError("slot census entry differs from plan order or carrier slot")
            expected_cell_id = public_oi_rest_cell_event_id_v2(
                session_start_manifest_sha256=self.session_start_manifest_sha256,
                plan_bundle_sha256=self.plan_bundle_sha256,
                rest_plan_sha256=self.rest_plan_sha256,
                scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
                symbol_ordinal=ordinal,
                symbol=symbol,
            )
            if entry.cell_event_id != expected_cell_id:
                raise ValueError("slot census cell event identity differs")
            if entry.outcome is PublicOiRestCellOutcomeV2.UNSTARTED_FORWARD_CLOCK_GAP:
                raise ValueError("forward clock gaps require the compact range schema")
        outcomes = {entry.outcome for entry in self.entries}
        slot_end = self.scheduled_slot_wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
        if PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED in outcomes:
            if self.closed_wall_ms < slot_end:
                raise ValueError("an expired slot census must close at or after slot end")
        if PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP in outcomes:
            if self.closed_wall_ms > slot_end:
                raise ValueError("normal-stop omissions after slot end are slot expiry")
        expected_event_id = _slot_event_id(
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            rest_plan_sha256=self.rest_plan_sha256,
            symbol_census_sha256=self.symbol_census_sha256,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
        )
        if self.event_id != expected_event_id:
            raise ValueError("slot census carrier event identity differs")
        self._check_size()

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        session_id: str,
        session_start_manifest_sha256: str,
        plan_bundle_sha256: str,
        scheduled_slot_wall_ms: int,
        entries: tuple[PublicOiRestSlotCensusEntryV2, ...],
        closed_wall_ms: int,
        closed_monotonic_ns: int,
    ) -> PublicOiRestSlotCensusV2:
        binding = _plan_binding(plan)
        return cls(
            provenance=LOCAL_SCHEDULE_EVIDENCE_V2,
            data_completeness_claimed=False,
            session_id=session_id,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
            plan_id=plan.name,
            route_id=_ROUTE_ID,
            rest_plan_sha256=binding[0],
            symbols=plan.symbols,
            symbol_census_sha256=binding[1],
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            entries=entries,
            closed_wall_ms=closed_wall_ms,
            closed_monotonic_ns=closed_monotonic_ns,
            event_id=_slot_event_id(
                session_start_manifest_sha256=session_start_manifest_sha256,
                plan_bundle_sha256=plan_bundle_sha256,
                rest_plan_sha256=binding[0],
                symbol_census_sha256=binding[1],
                scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            ),
        )

    def validate_against_plan(self, plan: ProvisionalPromotingRestCapturePlanV2) -> None:
        _validate_payload_against_plan(self, plan)

    def canonical_bytes(self) -> bytes:
        return _bounded_canonical_bytes(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_SLOT_PAYLOAD_DOMAIN + self.canonical_bytes()).hexdigest()

    def _check_size(self) -> None:
        _bounded_canonical_bytes(self)

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    ) -> PublicOiRestSlotCensusV2:
        document = _decode_canonical_document(encoded, _SLOT_SCHEMA)
        _require_exact_fields(document, cls)
        entries_value = document["entries"]
        if type(entries_value) is not list:
            raise TypeError("slot census entries must decode as an array")
        entries = tuple(_entry_from_document(value) for value in entries_value)
        payload = cls(
            provenance=cast(
                Literal["LOCAL_SCHEDULE_EVIDENCE"], _required_str(document, "provenance")
            ),
            data_completeness_claimed=cast(
                Literal[False], _required_bool(document, "data_completeness_claimed")
            ),
            session_id=_required_str(document, "session_id"),
            session_start_manifest_sha256=_required_str(document, "session_start_manifest_sha256"),
            plan_bundle_sha256=_required_str(document, "plan_bundle_sha256"),
            plan_id=_required_str(document, "plan_id"),
            route_id=cast(Literal["usdm_public_rest"], _required_str(document, "route_id")),
            rest_plan_sha256=_required_str(document, "rest_plan_sha256"),
            symbols=_required_str_tuple(document, "symbols"),
            symbol_census_sha256=_required_str(document, "symbol_census_sha256"),
            scheduled_slot_wall_ms=_required_int(document, "scheduled_slot_wall_ms"),
            entries=entries,
            closed_wall_ms=_required_int(document, "closed_wall_ms"),
            closed_monotonic_ns=_required_int(document, "closed_monotonic_ns"),
            event_id=_required_str(document, "event_id"),
            schema_version=cast(
                Literal["r4b_v2_public_oi_rest_slot_census_v1"],
                _required_str(document, "schema_version"),
            ),
        )
        _finish_parse(payload, encoded, plan)
        return payload


@dataclass(frozen=True, slots=True)
class PublicOiRestForwardGapRangeV2:
    provenance: Literal["LOCAL_SCHEDULE_EVIDENCE"]
    data_completeness_claimed: Literal[False]
    session_id: str
    session_start_manifest_sha256: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_public_rest"]
    rest_plan_sha256: str
    symbols: tuple[str, ...]
    symbol_census_sha256: str
    first_slot_wall_ms: int
    end_slot_exclusive_wall_ms: int
    covered_slot_count: int
    outcome: PublicOiRestCellOutcomeV2
    observed_wall_ms: int
    observed_monotonic_ns: int
    event_id: str
    schema_version: Literal["r4b_v2_public_oi_rest_forward_gap_range_v1"] = (
        "r4b_v2_public_oi_rest_forward_gap_range_v1"
    )

    def __post_init__(self) -> None:
        _validate_common_payload_identity(
            provenance=self.provenance,
            data_completeness_claimed=self.data_completeness_claimed,
            session_id=self.session_id,
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            plan_id=self.plan_id,
            route_id=self.route_id,
            rest_plan_sha256=self.rest_plan_sha256,
            symbols=self.symbols,
            symbol_census_sha256=self.symbol_census_sha256,
        )
        if type(self.schema_version) is not str or self.schema_version != _GAP_SCHEMA:
            raise ValueError("unsupported public OI forward-gap schema")
        _require_aligned_slot(self.first_slot_wall_ms, "first_slot_wall_ms")
        _require_aligned_slot(self.end_slot_exclusive_wall_ms, "end_slot_exclusive_wall_ms")
        if self.end_slot_exclusive_wall_ms <= self.first_slot_wall_ms:
            raise ValueError("forward-gap range must be non-empty and increasing")
        expected_count = (
            self.end_slot_exclusive_wall_ms - self.first_slot_wall_ms
        ) // PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
        _require_positive_int(self.covered_slot_count, "covered_slot_count")
        if self.covered_slot_count != expected_count:
            raise ValueError("forward-gap covered_slot_count differs from its range")
        if self.outcome is not PublicOiRestCellOutcomeV2.UNSTARTED_FORWARD_CLOCK_GAP:
            raise ValueError("forward-gap range requires its exact unstarted outcome")
        _require_nonnegative_int(self.observed_wall_ms, "observed_wall_ms")
        _require_nonnegative_int(self.observed_monotonic_ns, "observed_monotonic_ns")
        if self.observed_wall_ms < self.end_slot_exclusive_wall_ms:
            raise ValueError("forward-gap observation precedes the skipped range end")
        expected_event_id = _gap_event_id(
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            rest_plan_sha256=self.rest_plan_sha256,
            symbol_census_sha256=self.symbol_census_sha256,
            first_slot_wall_ms=self.first_slot_wall_ms,
            end_slot_exclusive_wall_ms=self.end_slot_exclusive_wall_ms,
        )
        if self.event_id != expected_event_id:
            raise ValueError("forward-gap carrier event identity differs")
        _bounded_canonical_bytes(self)

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        session_id: str,
        session_start_manifest_sha256: str,
        plan_bundle_sha256: str,
        first_slot_wall_ms: int,
        end_slot_exclusive_wall_ms: int,
        observed_wall_ms: int,
        observed_monotonic_ns: int,
    ) -> PublicOiRestForwardGapRangeV2:
        binding = _plan_binding(plan)
        _require_aligned_slot(first_slot_wall_ms, "first_slot_wall_ms")
        _require_aligned_slot(end_slot_exclusive_wall_ms, "end_slot_exclusive_wall_ms")
        covered_slot_count = (
            end_slot_exclusive_wall_ms - first_slot_wall_ms
        ) // PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
        return cls(
            provenance=LOCAL_SCHEDULE_EVIDENCE_V2,
            data_completeness_claimed=False,
            session_id=session_id,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
            plan_id=plan.name,
            route_id=_ROUTE_ID,
            rest_plan_sha256=binding[0],
            symbols=plan.symbols,
            symbol_census_sha256=binding[1],
            first_slot_wall_ms=first_slot_wall_ms,
            end_slot_exclusive_wall_ms=end_slot_exclusive_wall_ms,
            covered_slot_count=covered_slot_count,
            outcome=PublicOiRestCellOutcomeV2.UNSTARTED_FORWARD_CLOCK_GAP,
            observed_wall_ms=observed_wall_ms,
            observed_monotonic_ns=observed_monotonic_ns,
            event_id=_gap_event_id(
                session_start_manifest_sha256=session_start_manifest_sha256,
                plan_bundle_sha256=plan_bundle_sha256,
                rest_plan_sha256=binding[0],
                symbol_census_sha256=binding[1],
                first_slot_wall_ms=first_slot_wall_ms,
                end_slot_exclusive_wall_ms=end_slot_exclusive_wall_ms,
            ),
        )

    def validate_against_plan(self, plan: ProvisionalPromotingRestCapturePlanV2) -> None:
        _validate_payload_against_plan(self, plan)

    def canonical_bytes(self) -> bytes:
        return _bounded_canonical_bytes(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_GAP_PAYLOAD_DOMAIN + self.canonical_bytes()).hexdigest()

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    ) -> PublicOiRestForwardGapRangeV2:
        document = _decode_canonical_document(encoded, _GAP_SCHEMA)
        _require_exact_fields(document, cls)
        try:
            outcome = PublicOiRestCellOutcomeV2(_required_str(document, "outcome"))
        except ValueError as exc:
            raise ValueError("unsupported public OI census outcome") from exc
        payload = cls(
            provenance=cast(
                Literal["LOCAL_SCHEDULE_EVIDENCE"], _required_str(document, "provenance")
            ),
            data_completeness_claimed=cast(
                Literal[False], _required_bool(document, "data_completeness_claimed")
            ),
            session_id=_required_str(document, "session_id"),
            session_start_manifest_sha256=_required_str(document, "session_start_manifest_sha256"),
            plan_bundle_sha256=_required_str(document, "plan_bundle_sha256"),
            plan_id=_required_str(document, "plan_id"),
            route_id=cast(Literal["usdm_public_rest"], _required_str(document, "route_id")),
            rest_plan_sha256=_required_str(document, "rest_plan_sha256"),
            symbols=_required_str_tuple(document, "symbols"),
            symbol_census_sha256=_required_str(document, "symbol_census_sha256"),
            first_slot_wall_ms=_required_int(document, "first_slot_wall_ms"),
            end_slot_exclusive_wall_ms=_required_int(document, "end_slot_exclusive_wall_ms"),
            covered_slot_count=_required_int(document, "covered_slot_count"),
            outcome=outcome,
            observed_wall_ms=_required_int(document, "observed_wall_ms"),
            observed_monotonic_ns=_required_int(document, "observed_monotonic_ns"),
            event_id=_required_str(document, "event_id"),
            schema_version=cast(
                Literal["r4b_v2_public_oi_rest_forward_gap_range_v1"],
                _required_str(document, "schema_version"),
            ),
        )
        _finish_parse(payload, encoded, plan)
        return payload


@dataclass(frozen=True, slots=True)
class PublicOiRestCoverageCloseV2:
    provenance: Literal["LOCAL_SCHEDULE_EVIDENCE"]
    data_completeness_claimed: Literal[False]
    write_once: Literal[True]
    close_reason: Literal["NORMAL_STOP"]
    session_id: str
    session_start_manifest_sha256: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_public_rest"]
    rest_plan_sha256: str
    symbols: tuple[str, ...]
    symbol_census_sha256: str
    coverage_start_slot_wall_ms: int
    stop_requested_wall_ms: int
    stop_requested_monotonic_ns: int
    coverage_end_slot_exclusive_wall_ms: int
    last_census_ingest_seq: int | None
    event_id: str
    schema_version: Literal["r4b_v2_public_oi_rest_coverage_close_v1"] = (
        "r4b_v2_public_oi_rest_coverage_close_v1"
    )

    def __post_init__(self) -> None:
        _validate_common_payload_identity(
            provenance=self.provenance,
            data_completeness_claimed=self.data_completeness_claimed,
            session_id=self.session_id,
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            plan_id=self.plan_id,
            route_id=self.route_id,
            rest_plan_sha256=self.rest_plan_sha256,
            symbols=self.symbols,
            symbol_census_sha256=self.symbol_census_sha256,
        )
        if type(self.schema_version) is not str or self.schema_version != _CLOSE_SCHEMA:
            raise ValueError("unsupported public OI coverage-close schema")
        if type(self.write_once) is not bool or self.write_once is not True:
            raise ValueError("public OI coverage close must be write-once")
        if type(self.close_reason) is not str or self.close_reason != _NORMAL_STOP:
            raise ValueError("public OI coverage close requires NORMAL_STOP")
        _require_aligned_slot(self.coverage_start_slot_wall_ms, "coverage_start_slot_wall_ms")
        _require_nonnegative_int(self.stop_requested_wall_ms, "stop_requested_wall_ms")
        _require_nonnegative_int(self.stop_requested_monotonic_ns, "stop_requested_monotonic_ns")
        _require_aligned_slot(
            self.coverage_end_slot_exclusive_wall_ms,
            "coverage_end_slot_exclusive_wall_ms",
        )
        if self.stop_requested_wall_ms < self.coverage_start_slot_wall_ms:
            raise ValueError("coverage stop precedes its start slot")
        expected_end = _ceil_slot_exclusive(self.stop_requested_wall_ms)
        if self.coverage_end_slot_exclusive_wall_ms != expected_end:
            raise ValueError("coverage end is not the exact half-open stop boundary")
        has_slots = expected_end > self.coverage_start_slot_wall_ms
        if has_slots:
            _require_positive_int(self.last_census_ingest_seq, "last_census_ingest_seq")
        elif self.last_census_ingest_seq is not None:
            raise ValueError("empty coverage forbids a last census reference")
        expected_event_id = _close_event_id(
            session_start_manifest_sha256=self.session_start_manifest_sha256,
            plan_bundle_sha256=self.plan_bundle_sha256,
            rest_plan_sha256=self.rest_plan_sha256,
            symbol_census_sha256=self.symbol_census_sha256,
        )
        if self.event_id != expected_event_id:
            raise ValueError("coverage-close event identity differs")
        _bounded_canonical_bytes(self)

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        session_id: str,
        session_start_manifest_sha256: str,
        plan_bundle_sha256: str,
        coverage_start_slot_wall_ms: int,
        stop_requested_wall_ms: int,
        stop_requested_monotonic_ns: int,
        last_census_ingest_seq: int | None,
    ) -> PublicOiRestCoverageCloseV2:
        binding = _plan_binding(plan)
        return cls(
            provenance=LOCAL_SCHEDULE_EVIDENCE_V2,
            data_completeness_claimed=False,
            write_once=True,
            close_reason=_NORMAL_STOP,
            session_id=session_id,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
            plan_id=plan.name,
            route_id=_ROUTE_ID,
            rest_plan_sha256=binding[0],
            symbols=plan.symbols,
            symbol_census_sha256=binding[1],
            coverage_start_slot_wall_ms=coverage_start_slot_wall_ms,
            stop_requested_wall_ms=stop_requested_wall_ms,
            stop_requested_monotonic_ns=stop_requested_monotonic_ns,
            coverage_end_slot_exclusive_wall_ms=_ceil_slot_exclusive(stop_requested_wall_ms),
            last_census_ingest_seq=last_census_ingest_seq,
            event_id=_close_event_id(
                session_start_manifest_sha256=session_start_manifest_sha256,
                plan_bundle_sha256=plan_bundle_sha256,
                rest_plan_sha256=binding[0],
                symbol_census_sha256=binding[1],
            ),
        )

    def validate_against_plan(self, plan: ProvisionalPromotingRestCapturePlanV2) -> None:
        _validate_payload_against_plan(self, plan)

    def canonical_bytes(self) -> bytes:
        return _bounded_canonical_bytes(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_CLOSE_PAYLOAD_DOMAIN + self.canonical_bytes()).hexdigest()

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    ) -> PublicOiRestCoverageCloseV2:
        document = _decode_canonical_document(encoded, _CLOSE_SCHEMA)
        _require_exact_fields(document, cls)
        payload = cls(
            provenance=cast(
                Literal["LOCAL_SCHEDULE_EVIDENCE"], _required_str(document, "provenance")
            ),
            data_completeness_claimed=cast(
                Literal[False], _required_bool(document, "data_completeness_claimed")
            ),
            write_once=cast(Literal[True], _required_bool(document, "write_once")),
            close_reason=cast(Literal["NORMAL_STOP"], _required_str(document, "close_reason")),
            session_id=_required_str(document, "session_id"),
            session_start_manifest_sha256=_required_str(document, "session_start_manifest_sha256"),
            plan_bundle_sha256=_required_str(document, "plan_bundle_sha256"),
            plan_id=_required_str(document, "plan_id"),
            route_id=cast(Literal["usdm_public_rest"], _required_str(document, "route_id")),
            rest_plan_sha256=_required_str(document, "rest_plan_sha256"),
            symbols=_required_str_tuple(document, "symbols"),
            symbol_census_sha256=_required_str(document, "symbol_census_sha256"),
            coverage_start_slot_wall_ms=_required_int(document, "coverage_start_slot_wall_ms"),
            stop_requested_wall_ms=_required_int(document, "stop_requested_wall_ms"),
            stop_requested_monotonic_ns=_required_int(document, "stop_requested_monotonic_ns"),
            coverage_end_slot_exclusive_wall_ms=_required_int(
                document, "coverage_end_slot_exclusive_wall_ms"
            ),
            last_census_ingest_seq=_optional_int(document, "last_census_ingest_seq"),
            event_id=_required_str(document, "event_id"),
            schema_version=cast(
                Literal["r4b_v2_public_oi_rest_coverage_close_v1"],
                _required_str(document, "schema_version"),
            ),
        )
        _finish_parse(payload, encoded, plan)
        return payload


PublicOiRestCensusPayloadV2 = (
    PublicOiRestSlotCensusV2 | PublicOiRestForwardGapRangeV2 | PublicOiRestCoverageCloseV2
)


def public_oi_rest_plan_sha256_v2(plan: ProvisionalPromotingRestCapturePlanV2) -> str:
    _validate_plan(plan)
    document = {
        "schema_version": "r4b_v2_public_oi_rest_plan_binding_v1",
        "plan": asdict(plan),
    }
    return hashlib.sha256(_REST_PLAN_DOMAIN + canonical_json_line(document)).hexdigest()


def public_oi_rest_symbol_census_sha256_v2(
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> str:
    rest_plan_sha256 = public_oi_rest_plan_sha256_v2(plan)
    return _symbol_census_sha256(
        plan_id=plan.name,
        route_id=plan.route_id,
        rest_plan_sha256=rest_plan_sha256,
        symbols=plan.symbols,
    )


def public_oi_rest_cell_event_id_v2(
    *,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    rest_plan_sha256: str,
    scheduled_slot_wall_ms: int,
    symbol_ordinal: int,
    symbol: str,
) -> str:
    _require_sha256(session_start_manifest_sha256, "session_start_manifest_sha256")
    _require_sha256(plan_bundle_sha256, "plan_bundle_sha256")
    _require_sha256(rest_plan_sha256, "rest_plan_sha256")
    _require_aligned_slot(scheduled_slot_wall_ms, "scheduled_slot_wall_ms")
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
        raise ValueError("symbol_ordinal exceeds the public OI census bound")
    _require_symbol(symbol)
    return _domain_hash(
        _CELL_ID_DOMAIN,
        {
            "schema_version": "r4b_v2_public_oi_cell_id_v1",
            "session_start_manifest_sha256": session_start_manifest_sha256,
            "plan_bundle_sha256": plan_bundle_sha256,
            "rest_plan_sha256": rest_plan_sha256,
            "scheduled_slot_wall_ms": scheduled_slot_wall_ms,
            "symbol_ordinal": symbol_ordinal,
            "symbol": symbol,
        },
    )


def public_oi_rest_attempt_record_sha256_v2(record: RawRecordV2) -> str:
    """Hash the exact canonical outer attempt record retained by WAL/block writers.

    This is deliberately the SHA-256 of ``canonical_json_line(record)`` and
    therefore equals ``QueuedRawRecordV2.encoded_sha256``. It is not a body,
    payload, source-message, or domain-separated semantic hash.
    """

    if type(record) is not RawRecordV2:
        raise TypeError("attempt record hash requires an exact RawRecordV2")
    record.__post_init__()
    if (
        record.transport is not TransportV2.HTTPS
        or record.route_id != _ROUTE_ID
        or record.symbol is None
        or record.frame_seq is not None
        or record.source_logical_key != f"openInterest:{record.symbol}"
    ):
        raise ValueError("attempt record hash requires one exact public OI REST record")
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(record.payload_bytes())
    if (
        payload.symbol != record.symbol
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise ValueError("attempt payload identity differs from its outer raw record")
    return hashlib.sha256(canonical_json_line(record)).hexdigest()


def _plan_binding(plan: ProvisionalPromotingRestCapturePlanV2) -> tuple[str, str]:
    rest_hash = public_oi_rest_plan_sha256_v2(plan)
    return rest_hash, _symbol_census_sha256(
        plan_id=plan.name,
        route_id=plan.route_id,
        rest_plan_sha256=rest_hash,
        symbols=plan.symbols,
    )


def _symbol_census_sha256(
    *, plan_id: str, route_id: str, rest_plan_sha256: str, symbols: tuple[str, ...]
) -> str:
    _require_identity(plan_id, "plan_id")
    if route_id != _ROUTE_ID:
        raise ValueError("public OI census route differs")
    _require_sha256(rest_plan_sha256, "rest_plan_sha256")
    _validate_symbols(symbols)
    return _domain_hash(
        _SYMBOL_CENSUS_DOMAIN,
        {
            "schema_version": "r4b_v2_public_oi_symbol_census_v1",
            "plan_id": plan_id,
            "route_id": route_id,
            "rest_plan_sha256": rest_plan_sha256,
            "symbols": symbols,
        },
    )


def _slot_event_id(**values: object) -> str:
    return _domain_hash(
        _SLOT_EVENT_ID_DOMAIN,
        {"schema_version": "r4b_v2_public_oi_slot_event_id_v1", **values},
    )


def _gap_event_id(**values: object) -> str:
    return _domain_hash(
        _GAP_EVENT_ID_DOMAIN,
        {"schema_version": "r4b_v2_public_oi_gap_event_id_v1", **values},
    )


def _close_event_id(**values: object) -> str:
    return _domain_hash(
        _CLOSE_EVENT_ID_DOMAIN,
        {"schema_version": "r4b_v2_public_oi_close_event_id_v1", **values},
    )


def _domain_hash(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _validate_common_payload_identity(
    *,
    provenance: str,
    data_completeness_claimed: bool,
    session_id: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    plan_id: str,
    route_id: str,
    rest_plan_sha256: str,
    symbols: tuple[str, ...],
    symbol_census_sha256: str,
) -> None:
    if type(provenance) is not str or provenance != LOCAL_SCHEDULE_EVIDENCE_V2:
        raise ValueError("public OI census provenance must be LOCAL_SCHEDULE_EVIDENCE")
    if type(data_completeness_claimed) is not bool or data_completeness_claimed:
        raise ValueError("local public OI census may not claim data completeness")
    _require_identity(session_id, "session_id")
    _require_sha256(session_start_manifest_sha256, "session_start_manifest_sha256")
    _require_sha256(plan_bundle_sha256, "plan_bundle_sha256")
    _require_identity(plan_id, "plan_id")
    if type(route_id) is not str or route_id != _ROUTE_ID:
        raise ValueError("public OI census route must be exactly usdm_public_rest")
    _require_sha256(rest_plan_sha256, "rest_plan_sha256")
    _validate_symbols(symbols)
    _require_sha256(symbol_census_sha256, "symbol_census_sha256")
    expected = _symbol_census_sha256(
        plan_id=plan_id,
        route_id=route_id,
        rest_plan_sha256=rest_plan_sha256,
        symbols=symbols,
    )
    if symbol_census_sha256 != expected:
        raise ValueError("public OI symbol-census hash differs")


def _validate_payload_against_plan(
    payload: PublicOiRestCensusPayloadV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> None:
    binding = _plan_binding(plan)
    if (
        payload.plan_id != plan.name
        or payload.route_id != plan.route_id
        or payload.rest_plan_sha256 != binding[0]
        or payload.symbols != plan.symbols
        or payload.symbol_census_sha256 != binding[1]
    ):
        raise ValueError("public OI census payload differs from the exact REST plan")


def _validate_plan(plan: ProvisionalPromotingRestCapturePlanV2) -> None:
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("public OI census requires the exact promoting REST plan")
    plan.__post_init__()


def _entry_from_document(value: object) -> PublicOiRestSlotCensusEntryV2:
    if type(value) is not dict:
        raise TypeError("slot census entry must be an object")
    document = cast(dict[str, object], value)
    _require_exact_fields(document, PublicOiRestSlotCensusEntryV2)
    try:
        outcome = PublicOiRestCellOutcomeV2(_required_str(document, "outcome"))
    except ValueError as exc:
        raise ValueError("unsupported public OI census outcome") from exc
    return PublicOiRestSlotCensusEntryV2(
        symbol_ordinal=_required_int(document, "symbol_ordinal"),
        cell_event_id=_required_str(document, "cell_event_id"),
        outcome=outcome,
        attempt_ingest_seq=_optional_int(document, "attempt_ingest_seq"),
        attempt_record_sha256=_optional_str(document, "attempt_record_sha256"),
    )


def _finish_parse(
    payload: PublicOiRestCensusPayloadV2,
    encoded: bytes,
    plan: ProvisionalPromotingRestCapturePlanV2 | None,
) -> None:
    if payload.canonical_bytes() != encoded:
        raise ValueError("public OI census payload is not exact canonical JSONL")
    if plan is not None:
        payload.validate_against_plan(plan)


def _bounded_canonical_bytes(value: object) -> bytes:
    encoded = canonical_json_line(value)
    if len(encoded) > PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2:
        raise ValueError("canonical public OI census payload exceeds its 12KB outer-safe cap")
    return encoded


def _decode_canonical_document(encoded: bytes, expected_schema: str) -> dict[str, object]:
    if type(encoded) is not bytes:
        raise TypeError("canonical public OI census payload must be immutable bytes")
    if not encoded or len(encoded) > PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2:
        raise ValueError("canonical public OI census payload has an invalid byte length")
    if not encoded.endswith(b"\n") or encoded.count(b"\n") != 1:
        raise ValueError("canonical public OI census payload must be exactly one JSONL line")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("canonical public OI census payload must be UTF-8") from exc

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("canonical public OI census contains a duplicate key")
            result[key] = value
        return result

    def reject_float(_: str) -> object:
        raise ValueError("binary floats are forbidden in public OI census JSON")

    try:
        document = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_float=reject_float,
            parse_constant=reject_float,
        )
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical public OI census payload is invalid JSON") from exc
    if type(document) is not dict:
        raise ValueError("canonical public OI census payload must be an object")
    typed = cast(dict[str, object], document)
    if _required_str(typed, "schema_version") != expected_schema:
        raise ValueError("public OI census payload schema differs")
    return typed


def _require_exact_fields(document: dict[str, object], model: type[object]) -> None:
    model_fields = getattr(model, "__dataclass_fields__", None)
    if type(model_fields) is not dict:
        raise TypeError("public OI census model must be an exact dataclass type")
    if set(document) != set(cast(dict[str, object], model_fields)):
        raise ValueError("public OI census payload fields differ")


def _required_str(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if type(value) is not str:
        raise TypeError(f"{key} must be exact text")
    return value


def _optional_str(document: dict[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is not None and type(value) is not str:
        raise TypeError(f"{key} must be exact text or null")
    return cast(str | None, value)


def _required_int(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise TypeError(f"{key} must be an exact integer")
    return value


def _optional_int(document: dict[str, object], key: str) -> int | None:
    value = document.get(key)
    if value is not None and type(value) is not int:
        raise TypeError(f"{key} must be an exact integer or null")
    return cast(int | None, value)


def _required_bool(document: dict[str, object], key: str) -> bool:
    value = document.get(key)
    if type(value) is not bool:
        raise TypeError(f"{key} must be an exact boolean")
    return value


def _required_str_tuple(document: dict[str, object], key: str) -> tuple[str, ...]:
    value = document.get(key)
    if type(value) is not list or any(type(item) is not str for item in value):
        raise TypeError(f"{key} must be an exact text array")
    return tuple(cast(list[str], value))


def _validate_symbols(symbols: tuple[str, ...]) -> None:
    if (
        type(symbols) is not tuple
        or not symbols
        or len(symbols) > PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2
    ):
        raise ValueError("public OI symbol census must contain 1..32 symbols")
    if symbols != tuple(sorted(symbols)) or len(set(symbols)) != len(symbols):
        raise ValueError("public OI symbols must be unique lexicographic order")
    for symbol in symbols:
        _require_symbol(symbol)


def _require_symbol(symbol: str) -> None:
    if (
        type(symbol) is not str
        or not 5 <= len(symbol) <= 30
        or _SYMBOL_RE.fullmatch(symbol) is None
    ):
        raise ValueError("public OI census symbol must be normalized uppercase USDT")


def _require_identity(value: str, field: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _require_sha256(value: object, field: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _require_nonnegative_int(value: object, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")


def _require_positive_int(value: object, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _require_aligned_slot(value: object, field: str) -> None:
    _require_nonnegative_int(value, field)
    assert type(value) is int
    if value % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
        raise ValueError(f"{field} must be a 5-second UTC epoch multiple")


def _ceil_slot_exclusive(wall_ms: int) -> int:
    _require_nonnegative_int(wall_ms, "stop_requested_wall_ms")
    remainder = wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    if remainder == 0:
        return wall_ms
    return wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 - remainder
