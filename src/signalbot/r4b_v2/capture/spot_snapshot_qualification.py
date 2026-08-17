from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Literal

from signalbot.r4b_v2.canonical import canonical_json_line

SPOT_SNAPSHOT_LIMIT_GRID_V2 = (100, 500, 1000, 5000)
SPOT_SNAPSHOT_WEIGHT_GRID_V2 = (5, 25, 50, 250)
SPOT_SNAPSHOT_QUALIFICATION_DURATION_MS_V2 = 24 * 60 * 60 * 1_000
SPOT_TWO_SIDED_COMPLETENESS_NUMERATOR_V2 = 9_999
SPOT_TWO_SIDED_COMPLETENESS_DENOMINATOR_V2 = 10_000
MICRO_USDT_PER_USDT_V2 = 1_000_000
SPOT_CERTIFIED_NOTIONAL_MIN_MICRO_USDT_V2 = 1_000 * MICRO_USDT_PER_USDT_V2
SPOT_RECOVERY_P99_MAX_MS_V2 = 60_000
SPOT_RECOVERY_MAXIMUM_MS_V2 = 86_400

_LIMIT_TO_WEIGHT = dict(
    zip(SPOT_SNAPSHOT_LIMIT_GRID_V2, SPOT_SNAPSHOT_WEIGHT_GRID_V2, strict=True)
)
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,30}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SELECTED = "SELECTED"
_BLOCKED = "T0_BLOCKED_SPOT_SNAPSHOT_QUALIFICATION"
_SELECTION_RULE = "PER_SYMBOL_SMALLEST_PASSING_LIMIT"


class SpotSnapshotQualificationError(ValueError):
    """Raised when Spot snapshot engineering evidence is invalid or incomplete."""


class SpotSnapshotT0BlockedError(RuntimeError):
    """Raised when a blocked receipt is used as runtime selection authority."""


@dataclass(frozen=True, slots=True)
class SpotSnapshotQualificationSampleV2:
    """One outcome-free, already-haircutted candidate-limit observation."""

    sample_id: str
    bid_10bp_complete: bool
    ask_10bp_complete: bool
    bid_haircutted_certified_notional_micro_usdt: int
    ask_haircutted_certified_notional_micro_usdt: int
    schema_version: str = "r4b_v2_spot_snapshot_qualification_sample_v1"

    def __post_init__(self) -> None:
        _require_identity(self.sample_id, "sample_id")
        _require_bool(self.bid_10bp_complete, "bid_10bp_complete")
        _require_bool(self.ask_10bp_complete, "ask_10bp_complete")
        _require_nonnegative_int(
            self.bid_haircutted_certified_notional_micro_usdt,
            "bid_haircutted_certified_notional_micro_usdt",
        )
        _require_nonnegative_int(
            self.ask_haircutted_certified_notional_micro_usdt,
            "ask_haircutted_certified_notional_micro_usdt",
        )
        if self.schema_version != "r4b_v2_spot_snapshot_qualification_sample_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot qualification sample schema"
            )


@dataclass(frozen=True, slots=True)
class SpotSnapshotCandidateQualificationV2:
    """Order-independent metrics for one symbol and one documented depth limit."""

    symbol: str
    limit: int
    request_weight: int
    sample_count: int
    two_sided_10bp_complete_count: int
    bid_first_percentile_haircutted_notional_micro_usdt: int
    ask_first_percentile_haircutted_notional_micro_usdt: int
    measurement_root_sha256: str
    schema_version: str = "r4b_v2_spot_snapshot_candidate_qualification_v1"

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_limit_and_weight(self.limit, self.request_weight)
        _require_positive_int(self.sample_count, "sample_count")
        _require_nonnegative_int(
            self.two_sided_10bp_complete_count,
            "two_sided_10bp_complete_count",
        )
        if self.two_sided_10bp_complete_count > self.sample_count:
            raise SpotSnapshotQualificationError(
                "two_sided_10bp_complete_count cannot exceed sample_count"
            )
        _require_nonnegative_int(
            self.bid_first_percentile_haircutted_notional_micro_usdt,
            "bid_first_percentile_haircutted_notional_micro_usdt",
        )
        _require_nonnegative_int(
            self.ask_first_percentile_haircutted_notional_micro_usdt,
            "ask_first_percentile_haircutted_notional_micro_usdt",
        )
        _require_sha256(self.measurement_root_sha256, "measurement_root_sha256")
        if self.schema_version != "r4b_v2_spot_snapshot_candidate_qualification_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot candidate qualification schema"
            )

    @property
    def candidate_id(self) -> str:
        """Return the deterministic symbol-limit candidate identity."""

        return f"{self.symbol}:spot-depth-limit-{self.limit}"

    @property
    def failed_gates(self) -> tuple[str, ...]:
        """Return exact engineering gates failed by this candidate."""

        failures: list[str] = []
        if (
            self.two_sided_10bp_complete_count
            * SPOT_TWO_SIDED_COMPLETENESS_DENOMINATOR_V2
            < self.sample_count * SPOT_TWO_SIDED_COMPLETENESS_NUMERATOR_V2
        ):
            failures.append("two_sided_10bp_completeness")
        if (
            self.bid_first_percentile_haircutted_notional_micro_usdt
            < SPOT_CERTIFIED_NOTIONAL_MIN_MICRO_USDT_V2
        ):
            failures.append("bid_first_percentile_haircutted_certified_notional")
        if (
            self.ask_first_percentile_haircutted_notional_micro_usdt
            < SPOT_CERTIFIED_NOTIONAL_MIN_MICRO_USDT_V2
        ):
            failures.append("ask_first_percentile_haircutted_certified_notional")
        return tuple(failures)

    @property
    def passed(self) -> bool:
        """Return whether all selection gates pass at their inclusive boundaries."""

        return not self.failed_gates


@dataclass(frozen=True, slots=True)
class SpotRestWeightBudgetEvidenceV2:
    """Worst observed minute, including every required public REST category."""

    smallest_active_minute_weight_limit: int
    snapshots_weight: int
    retries_weight: int
    rule_polling_weight: int
    reference_bootstrap_weight: int
    block_trade_backfill_weight: int
    measurement_root_sha256: str
    schema_version: str = "r4b_v2_spot_rest_weight_budget_evidence_v1"

    def __post_init__(self) -> None:
        _require_positive_int(
            self.smallest_active_minute_weight_limit,
            "smallest_active_minute_weight_limit",
        )
        for field_name in (
            "snapshots_weight",
            "retries_weight",
            "rule_polling_weight",
            "reference_bootstrap_weight",
            "block_trade_backfill_weight",
        ):
            _require_nonnegative_int(getattr(self, field_name), field_name)
        _require_sha256(self.measurement_root_sha256, "measurement_root_sha256")
        if self.schema_version != "r4b_v2_spot_rest_weight_budget_evidence_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot REST weight budget evidence schema"
            )

    @property
    def total_weight(self) -> int:
        """Return all public REST weight charged in the worst observed minute."""

        return (
            self.snapshots_weight
            + self.retries_weight
            + self.rule_polling_weight
            + self.reference_bootstrap_weight
            + self.block_trade_backfill_weight
        )

    @property
    def passed(self) -> bool:
        """Return whether total weight is at most 25% of the active limit."""

        return self.total_weight * 4 <= self.smallest_active_minute_weight_limit


@dataclass(frozen=True, slots=True)
class SpotFinalPanelRecoveryProofV2:
    """Order-independent all-symbol recovery evidence for the final panel."""

    all_symbol_recovery_durations_ms: tuple[int, ...]
    unresolved_gap_count: int
    measurement_root_sha256: str
    schema_version: str = "r4b_v2_spot_final_panel_recovery_proof_v1"

    def __post_init__(self) -> None:
        if not isinstance(self.all_symbol_recovery_durations_ms, tuple):
            raise SpotSnapshotQualificationError(
                "all_symbol_recovery_durations_ms must be an immutable tuple"
            )
        if not self.all_symbol_recovery_durations_ms:
            raise SpotSnapshotQualificationError(
                "final-panel recovery proof requires at least one duration"
            )
        for duration_ms in self.all_symbol_recovery_durations_ms:
            _require_nonnegative_int(duration_ms, "all_symbol_recovery_duration_ms")
        object.__setattr__(
            self,
            "all_symbol_recovery_durations_ms",
            tuple(sorted(self.all_symbol_recovery_durations_ms)),
        )
        _require_nonnegative_int(self.unresolved_gap_count, "unresolved_gap_count")
        _require_sha256(self.measurement_root_sha256, "measurement_root_sha256")
        if self.schema_version != "r4b_v2_spot_final_panel_recovery_proof_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot final-panel recovery proof schema"
            )

    @property
    def p99_recovery_ms(self) -> int:
        """Return the deterministic nearest-rank 99th percentile."""

        return _nearest_rank(
            self.all_symbol_recovery_durations_ms,
            numerator=99,
            denominator=100,
        )

    @property
    def maximum_recovery_ms(self) -> int:
        """Return the maximum all-symbol recovery duration."""

        return self.all_symbol_recovery_durations_ms[-1]

    @property
    def failed_gates(self) -> tuple[str, ...]:
        """Return final-panel recovery gates that failed."""

        failures: list[str] = []
        if self.p99_recovery_ms > SPOT_RECOVERY_P99_MAX_MS_V2:
            failures.append("p99_all_symbol_recovery")
        if self.maximum_recovery_ms > SPOT_RECOVERY_MAXIMUM_MS_V2:
            failures.append("maximum_all_symbol_recovery")
        if self.unresolved_gap_count != 0:
            failures.append("unresolved_gap_count")
        return tuple(failures)

    @property
    def passed(self) -> bool:
        """Return whether all final-panel recovery gates pass."""

        return not self.failed_gates


@dataclass(frozen=True, slots=True)
class SpotSnapshotQualificationRunV2:
    """Complete engineering-only 24-hour evidence for every symbol-limit cell."""

    qualification_id: str
    window_start_wall_ms: int
    window_end_wall_ms: int
    actual_final_panel_sha256: str
    source_manifest_sha256: str
    runtime_manifest_sha256: str
    engineering_only: bool
    strategy_signal_or_return_data_accessed: bool
    candidates: tuple[SpotSnapshotCandidateQualificationV2, ...]
    rest_weight_budget: SpotRestWeightBudgetEvidenceV2
    recovery_proof: SpotFinalPanelRecoveryProofV2
    schema_version: str = "r4b_v2_spot_snapshot_qualification_run_v1"

    def __post_init__(self) -> None:
        _require_identity(self.qualification_id, "qualification_id")
        _require_nonnegative_int(self.window_start_wall_ms, "window_start_wall_ms")
        _require_nonnegative_int(self.window_end_wall_ms, "window_end_wall_ms")
        if (
            self.window_end_wall_ms - self.window_start_wall_ms
            != SPOT_SNAPSHOT_QUALIFICATION_DURATION_MS_V2
        ):
            raise SpotSnapshotQualificationError(
                "Spot snapshot qualification must cover exactly 24 complete hours"
            )
        for field_name in (
            "actual_final_panel_sha256",
            "source_manifest_sha256",
            "runtime_manifest_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if type(self.engineering_only) is not bool or not self.engineering_only:
            raise SpotSnapshotQualificationError("engineering_only must be true")
        if (
            type(self.strategy_signal_or_return_data_accessed) is not bool
            or self.strategy_signal_or_return_data_accessed
        ):
            raise SpotSnapshotQualificationError(
                "Spot snapshot qualification may not access strategy signals or returns"
            )
        if not isinstance(self.candidates, tuple) or not self.candidates:
            raise SpotSnapshotQualificationError(
                "candidates must be a non-empty immutable tuple"
            )
        for candidate in self.candidates:
            if not isinstance(candidate, SpotSnapshotCandidateQualificationV2):
                raise SpotSnapshotQualificationError(
                    "candidates must contain SpotSnapshotCandidateQualificationV2 values"
                )
        ordered = tuple(sorted(self.candidates, key=lambda item: (item.symbol, item.limit)))
        object.__setattr__(self, "candidates", ordered)
        _require_complete_symbol_grids(ordered)
        if not isinstance(self.rest_weight_budget, SpotRestWeightBudgetEvidenceV2):
            raise SpotSnapshotQualificationError(
                "rest_weight_budget must be SpotRestWeightBudgetEvidenceV2"
            )
        if not isinstance(self.recovery_proof, SpotFinalPanelRecoveryProofV2):
            raise SpotSnapshotQualificationError(
                "recovery_proof must be SpotFinalPanelRecoveryProofV2"
            )
        if self.schema_version != "r4b_v2_spot_snapshot_qualification_run_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot qualification run schema"
            )

    @property
    def symbols(self) -> tuple[str, ...]:
        """Return the normalized final-panel symbol set."""

        return tuple(sorted({candidate.symbol for candidate in self.candidates}))

    @property
    def failed_infrastructure_gates(self) -> tuple[str, ...]:
        """Return budget and recovery failures independent of candidate metrics."""

        failures: list[str] = []
        if not self.rest_weight_budget.passed:
            failures.append("REST_weight_budget")
        failures.extend(self.recovery_proof.failed_gates)
        return tuple(failures)

    @property
    def sha256(self) -> str:
        """Return the canonical RFC 8785 hash of the complete evidence run."""

        return hashlib.sha256(canonical_json_line(self)).hexdigest()

    def candidates_for_symbol(
        self, symbol: str
    ) -> tuple[SpotSnapshotCandidateQualificationV2, ...]:
        """Return one symbol's candidates in ascending-limit order."""

        _require_symbol(symbol)
        return tuple(candidate for candidate in self.candidates if candidate.symbol == symbol)


@dataclass(frozen=True, slots=True)
class SpotSnapshotSymbolSelectionV2:
    """Canonical selected depth limit for one final-panel Spot symbol."""

    symbol: str
    selected_limit: int
    request_weight: int
    candidate_measurement_root_sha256: str
    schema_version: str = "r4b_v2_spot_snapshot_symbol_selection_v1"

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_limit_and_weight(self.selected_limit, self.request_weight)
        _require_sha256(
            self.candidate_measurement_root_sha256,
            "candidate_measurement_root_sha256",
        )
        if self.schema_version != "r4b_v2_spot_snapshot_symbol_selection_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot symbol selection schema"
            )


SpotSnapshotSelectionStatusV2 = Literal[
    "SELECTED",
    "T0_BLOCKED_SPOT_SNAPSHOT_QUALIFICATION",
]


@dataclass(frozen=True, slots=True)
class SpotSnapshotSelectionReceiptV2:
    """Canonical pre-H_start receipt for deterministic per-symbol selections."""

    qualification: SpotSnapshotQualificationRunV2
    selection_wall_ms: int
    h_start_wall_ms: int | None
    status: SpotSnapshotSelectionStatusV2
    selections: tuple[SpotSnapshotSymbolSelectionV2, ...]
    ineligible_symbols: tuple[str, ...]
    failed_gates: tuple[str, ...]
    selection_rule: str = _SELECTION_RULE
    schema_version: str = "r4b_v2_spot_snapshot_selection_receipt_v1"

    def __post_init__(self) -> None:
        if not isinstance(self.qualification, SpotSnapshotQualificationRunV2):
            raise SpotSnapshotQualificationError(
                "qualification must be SpotSnapshotQualificationRunV2"
            )
        _require_nonnegative_int(self.selection_wall_ms, "selection_wall_ms")
        if self.selection_wall_ms < self.qualification.window_end_wall_ms:
            raise SpotSnapshotQualificationError(
                "Spot snapshot selection cannot precede its 24-hour qualification"
            )
        if self.selection_rule != _SELECTION_RULE:
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot candidate selection rule"
            )
        if self.schema_version != "r4b_v2_spot_snapshot_selection_receipt_v1":
            raise SpotSnapshotQualificationError(
                "unsupported Spot snapshot selection receipt schema"
            )
        expected_selections, expected_ineligible, expected_failures = _derive_selection(
            self.qualification
        )
        if self.ineligible_symbols != expected_ineligible:
            raise SpotSnapshotQualificationError(
                "selection receipt ineligible symbols differ from candidate gates"
            )
        if self.failed_gates != expected_failures:
            raise SpotSnapshotQualificationError(
                "selection receipt failed gates differ from qualification evidence"
            )
        if expected_failures:
            if (
                self.status != _BLOCKED
                or self.h_start_wall_ms is not None
                or self.selections
            ):
                raise SpotSnapshotQualificationError(
                    "failed Spot snapshot qualification must block T0 without selections"
                )
            return
        if self.status != _SELECTED or self.selections != expected_selections:
            raise SpotSnapshotQualificationError(
                "receipt selections are not the smallest passing limits"
            )
        if self.h_start_wall_ms is None:
            raise SpotSnapshotQualificationError(
                "passing Spot snapshot qualification requires H_start"
            )
        _require_nonnegative_int(self.h_start_wall_ms, "h_start_wall_ms")
        if self.selection_wall_ms >= self.h_start_wall_ms:
            raise SpotSnapshotQualificationError(
                "Spot snapshot selection must strictly precede H_start"
            )

    @property
    def sha256(self) -> str:
        """Return the canonical RFC 8785 hash of this selection receipt."""

        return hashlib.sha256(canonical_json_line(self)).hexdigest()

    def require_selected_limit(self, symbol: str, limit: int) -> None:
        """Fail closed unless a runtime symbol-limit pair matches this receipt."""

        _require_symbol(symbol)
        if self.status == _BLOCKED:
            raise SpotSnapshotT0BlockedError(
                "Spot snapshot qualification failed; T0 is blocked"
            )
        for selection in self.selections:
            if selection.symbol == symbol and selection.selected_limit == limit:
                return
        raise SpotSnapshotQualificationError(
            "runtime Spot snapshot limit differs from the canonical selection receipt"
        )


def qualify_spot_snapshot_candidate_v2(
    *,
    symbol: str,
    limit: int,
    samples: Iterable[SpotSnapshotQualificationSampleV2],
) -> SpotSnapshotCandidateQualificationV2:
    """Derive exact order-independent candidate metrics from raw qualification samples."""

    _require_symbol(symbol)
    request_weight = _request_weight(limit)
    observed = tuple(samples)
    if not observed:
        raise SpotSnapshotQualificationError(
            "Spot snapshot candidate requires at least one qualification sample"
        )
    if any(not isinstance(item, SpotSnapshotQualificationSampleV2) for item in observed):
        raise SpotSnapshotQualificationError(
            "samples must contain SpotSnapshotQualificationSampleV2 values"
        )
    ordered = tuple(sorted(observed, key=lambda item: item.sample_id))
    sample_ids = tuple(item.sample_id for item in ordered)
    if len(set(sample_ids)) != len(sample_ids):
        raise SpotSnapshotQualificationError(
            "Spot snapshot qualification sample IDs must be unique per candidate"
        )
    measurement_document = {
        "limit": limit,
        "samples": tuple(asdict(item) for item in ordered),
        "schema_version": "r4b_v2_spot_snapshot_candidate_measurement_v1",
        "symbol": symbol,
    }
    measurement_root_sha256 = hashlib.sha256(
        canonical_json_line(measurement_document)
    ).hexdigest()
    return SpotSnapshotCandidateQualificationV2(
        symbol=symbol,
        limit=limit,
        request_weight=request_weight,
        sample_count=len(ordered),
        two_sided_10bp_complete_count=sum(
            item.bid_10bp_complete and item.ask_10bp_complete for item in ordered
        ),
        bid_first_percentile_haircutted_notional_micro_usdt=_nearest_rank(
            tuple(
                item.bid_haircutted_certified_notional_micro_usdt for item in ordered
            ),
            numerator=1,
            denominator=100,
        ),
        ask_first_percentile_haircutted_notional_micro_usdt=_nearest_rank(
            tuple(
                item.ask_haircutted_certified_notional_micro_usdt for item in ordered
            ),
            numerator=1,
            denominator=100,
        ),
        measurement_root_sha256=measurement_root_sha256,
    )


def select_spot_snapshot_limits_v2(
    qualification: SpotSnapshotQualificationRunV2,
    *,
    selection_wall_ms: int,
    h_start_wall_ms: int | None,
) -> SpotSnapshotSelectionReceiptV2:
    """Choose each symbol's smallest passing limit or deterministically block T0."""

    selections, ineligible_symbols, failed_gates = _derive_selection(qualification)
    if failed_gates:
        if h_start_wall_ms is not None:
            raise SpotSnapshotQualificationError(
                "H_start must remain unset when Spot snapshot qualification fails"
            )
        return SpotSnapshotSelectionReceiptV2(
            qualification=qualification,
            selection_wall_ms=selection_wall_ms,
            h_start_wall_ms=None,
            status=_BLOCKED,
            selections=(),
            ineligible_symbols=ineligible_symbols,
            failed_gates=failed_gates,
        )
    return SpotSnapshotSelectionReceiptV2(
        qualification=qualification,
        selection_wall_ms=selection_wall_ms,
        h_start_wall_ms=h_start_wall_ms,
        status=_SELECTED,
        selections=selections,
        ineligible_symbols=(),
        failed_gates=(),
    )


def _derive_selection(
    qualification: SpotSnapshotQualificationRunV2,
) -> tuple[
    tuple[SpotSnapshotSymbolSelectionV2, ...],
    tuple[str, ...],
    tuple[str, ...],
]:
    selections: list[SpotSnapshotSymbolSelectionV2] = []
    ineligible_symbols: list[str] = []
    for symbol in qualification.symbols:
        passing = tuple(
            candidate
            for candidate in qualification.candidates_for_symbol(symbol)
            if candidate.passed
        )
        if not passing:
            ineligible_symbols.append(symbol)
            continue
        selected = passing[0]
        selections.append(
            SpotSnapshotSymbolSelectionV2(
                symbol=symbol,
                selected_limit=selected.limit,
                request_weight=selected.request_weight,
                candidate_measurement_root_sha256=selected.measurement_root_sha256,
            )
        )
    failures = list(qualification.failed_infrastructure_gates)
    failures.extend(f"symbol:{symbol}:no_passing_limit" for symbol in ineligible_symbols)
    return tuple(selections), tuple(ineligible_symbols), tuple(failures)


def _nearest_rank(
    values: Sequence[int],
    *,
    numerator: int,
    denominator: int,
) -> int:
    if not values:
        raise SpotSnapshotQualificationError("nearest-rank sample must be non-empty")
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or numerator < 1
        or denominator < 1
        or numerator > denominator
    ):
        raise SpotSnapshotQualificationError("nearest-rank probability is invalid")
    for value in values:
        _require_nonnegative_int(value, "nearest_rank_value")
    ordered = sorted(values)
    rank = (numerator * len(ordered) + denominator - 1) // denominator
    return ordered[rank - 1]


def _require_complete_symbol_grids(
    candidates: tuple[SpotSnapshotCandidateQualificationV2, ...],
) -> None:
    symbols = {candidate.symbol for candidate in candidates}
    for symbol in symbols:
        symbol_candidates = tuple(
            candidate for candidate in candidates if candidate.symbol == symbol
        )
        observed_limits = tuple(candidate.limit for candidate in symbol_candidates)
        if observed_limits != SPOT_SNAPSHOT_LIMIT_GRID_V2:
            raise SpotSnapshotQualificationError(
                "each Spot symbol must contain every frozen snapshot limit exactly once"
            )


def _request_weight(limit: int) -> int:
    if type(limit) is not int or limit not in _LIMIT_TO_WEIGHT:
        raise SpotSnapshotQualificationError(
            "limit is outside the frozen Spot snapshot grid"
        )
    return _LIMIT_TO_WEIGHT[limit]


def _require_limit_and_weight(limit: int, request_weight: int) -> None:
    expected_weight = _request_weight(limit)
    if type(request_weight) is not int or request_weight != expected_weight:
        raise SpotSnapshotQualificationError(
            "request_weight differs from the documented Spot depth weight"
        )


def _require_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise SpotSnapshotQualificationError(
            f"{field_name} must be a bounded normalized identity"
        )


def _require_symbol(symbol: str) -> None:
    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise SpotSnapshotQualificationError(
            "symbol must be an uppercase normalized market symbol"
        )


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise SpotSnapshotQualificationError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _require_bool(value: bool, field_name: str) -> None:
    if type(value) is not bool:
        raise SpotSnapshotQualificationError(f"{field_name} must be boolean")


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise SpotSnapshotQualificationError(f"{field_name} must be a positive integer")


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise SpotSnapshotQualificationError(
            f"{field_name} must be a nonnegative integer"
        )
