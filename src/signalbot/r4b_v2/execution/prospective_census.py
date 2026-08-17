"""Frozen, allocation-bounded census for one prospective efficacy attempt.

This module owns only the immutable universe/family/time grid.  It performs no
I/O, reads no market data, evaluates no strategy, and does not authorize PAPER
or production execution.  Durable admission and segment sealing are separate
owners that must bind this plan's hash.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Final

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
    validate_decision_bar_v2,
)
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    ProspectiveAttemptV2,
)

PROSPECTIVE_CENSUS_SCHEMA_V2: Final = "r4b_v2_prospective_census_plan_v2"
PROSPECTIVE_SEGMENT_SCHEMA_V2: Final = "r4b_v2_prospective_census_segment_v2"
PROSPECTIVE_CELL_SCHEMA_V2: Final = "r4b_v2_prospective_expected_cell_v2"
MAX_PROSPECTIVE_SYMBOLS_V2: Final = 250

_PLAN_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_CENSUS_PLAN_V2\0"
_UNIVERSE_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_UNIVERSE_V2\0"
_SEGMENT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_CENSUS_SEGMENT_V2\0"
_CELL_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_EXPECTED_CELL_V2\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_ALL_FAMILIES: Final = tuple(PromotingFamilyV2)


class ProspectiveCensusContractErrorV2(ValueError):
    """Raised when a frozen prospective census invariant is violated."""


@dataclass(frozen=True, slots=True)
class ProspectiveFamilyRuleBindingV2:
    """One isolated promoting family and its exact frozen rule version."""

    family: PromotingFamilyV2
    rule_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.family, PromotingFamilyV2):
            raise ProspectiveCensusContractErrorV2(
                "family must be PromotingFamilyV2"
            )
        _validate_identity(self.rule_version, "rule_version")


@dataclass(frozen=True, slots=True)
class ProspectiveCensusSegmentV2:
    """One bounded UTC-day projection of the attempt-wide cell grid."""

    attempt_plan_sha256: str
    day_start_ms: int
    first_bar_open_ms: int
    bar_open_stop_exclusive_ms: int
    expected_bar_count: int
    expected_cell_count: int
    segment_id: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_SEGMENT_SCHEMA_V2,
    )

    def __post_init__(self) -> None:
        _validate_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        _validate_nonnegative_ms(self.day_start_ms, "day_start_ms")
        if self.day_start_ms % MILLISECONDS_PER_DAY_V2 != 0:
            raise ProspectiveCensusContractErrorV2(
                "segment day_start_ms must align to UTC midnight"
            )
        _validate_nonnegative_ms(self.first_bar_open_ms, "first_bar_open_ms")
        _validate_nonnegative_ms(
            self.bar_open_stop_exclusive_ms,
            "bar_open_stop_exclusive_ms",
        )
        _validate_nonnegative_int(self.expected_bar_count, "expected_bar_count")
        _validate_nonnegative_int(self.expected_cell_count, "expected_cell_count")
        if self.first_bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise ProspectiveCensusContractErrorV2(
                "segment first bar must align to a 5m UTC boundary"
            )
        if self.bar_open_stop_exclusive_ms % FIVE_MINUTE_MS_V2 != 0:
            raise ProspectiveCensusContractErrorV2(
                "segment stop must align to a 5m UTC boundary"
            )
        if self.first_bar_open_ms < self.day_start_ms:
            raise ProspectiveCensusContractErrorV2(
                "segment first bar cannot precede its UTC day"
            )
        if self.bar_open_stop_exclusive_ms > (
            self.day_start_ms + MILLISECONDS_PER_DAY_V2
        ):
            raise ProspectiveCensusContractErrorV2(
                "segment stop cannot cross its UTC day"
            )
        expected_stop = (
            self.first_bar_open_ms
            + self.expected_bar_count * FIVE_MINUTE_MS_V2
        )
        if self.bar_open_stop_exclusive_ms != expected_stop:
            raise ProspectiveCensusContractErrorV2(
                "segment stop differs from its exact five-minute bar count"
            )
        object.__setattr__(
            self,
            "segment_id",
            _hash_document(_SEGMENT_DOMAIN, _segment_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveExpectedCellV2:
    """Deterministic identity of one family/symbol/closed-bar decision cell."""

    attempt_id: str
    attempt_plan_sha256: str
    segment_id: str
    family: PromotingFamilyV2
    rule_version: str
    symbol: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    cell_id: str = field(init=False)
    schema_version: str = field(init=False, default=PROSPECTIVE_CELL_SCHEMA_V2)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        _validate_sha256(self.segment_id, "segment_id")
        if not isinstance(self.family, PromotingFamilyV2):
            raise ProspectiveCensusContractErrorV2(
                "cell family must be PromotingFamilyV2"
            )
        _validate_identity(self.rule_version, "rule_version")
        _validate_symbol(self.symbol)
        try:
            validate_decision_bar_v2(
                self.bar_open_ms,
                self.bar_close_ms,
                self.decision_cutoff_ms,
            )
        except ValueError as exc:
            raise ProspectiveCensusContractErrorV2(str(exc)) from exc
        object.__setattr__(
            self,
            "cell_id",
            _hash_document(_CELL_DOMAIN, _cell_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveCensusPlanV2:
    """Frozen attempt-wide denominator without materializing millions of cells.

    ``created_at_ms`` is part of the immutable plan and must precede ``H_start``.
    It is not, by itself, a trusted receipt; the durable plan owner must capture
    and persist that receipt under the exact held writer lease.
    """

    attempt_id: str
    attempt: ProspectiveAttemptV2
    promoting_plan_sha256: str
    symbols: tuple[str, ...]
    context_symbols: tuple[str, ...]
    family_rules: tuple[ProspectiveFamilyRuleBindingV2, ...]
    paper_fok_rule_version: str
    execution_contract_sha256: str
    efficacy_gate_contract_sha256: str
    strategy_code_freeze_manifest_sha256: str
    created_at_ms: int
    universe_sha256: str = field(init=False)
    context_universe_sha256: str = field(init=False)
    plan_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=PROSPECTIVE_CENSUS_SCHEMA_V2)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        if not isinstance(self.attempt, ProspectiveAttemptV2):
            raise ProspectiveCensusContractErrorV2(
                "attempt must be ProspectiveAttemptV2"
            )
        if self.attempt.terminal_status is not None:
            raise ProspectiveCensusContractErrorV2(
                "census plan cannot bind an already terminal attempt"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(
            self.execution_contract_sha256,
            "execution_contract_sha256",
        )
        _validate_sha256(
            self.efficacy_gate_contract_sha256,
            "efficacy_gate_contract_sha256",
        )
        _validate_sha256(
            self.strategy_code_freeze_manifest_sha256,
            "strategy_code_freeze_manifest_sha256",
        )
        _validate_identity(self.paper_fok_rule_version, "paper_fok_rule_version")
        _validate_nonnegative_ms(self.created_at_ms, "created_at_ms")
        if not (
            self.attempt.qualification_start_ms
            <= self.created_at_ms
            < self.attempt.horizon.h_start_ms
        ):
            raise ProspectiveCensusContractErrorV2(
                "census plan must be created during qualification and before H_start"
            )

        normalized_symbols = _normalize_symbols(self.symbols)
        object.__setattr__(self, "symbols", normalized_symbols)
        normalized_context_symbols = _normalize_symbols(self.context_symbols)
        if not set(normalized_symbols).issubset(normalized_context_symbols):
            raise ProspectiveCensusContractErrorV2(
                "evaluation symbols must be a subset of context_symbols"
            )
        object.__setattr__(self, "context_symbols", normalized_context_symbols)
        _validate_family_rules(self.family_rules)
        universe_sha256 = _hash_document(
            _UNIVERSE_DOMAIN,
            {
                "schema_version": "r4b_v2_prospective_universe_v2",
                "symbols": list(normalized_symbols),
            },
        )
        object.__setattr__(self, "universe_sha256", universe_sha256)
        context_universe_sha256 = _hash_document(
            _UNIVERSE_DOMAIN,
            {
                "schema_version": "r4b_v2_prospective_context_universe_v2",
                "symbols": list(normalized_context_symbols),
            },
        )
        object.__setattr__(
            self,
            "context_universe_sha256",
            context_universe_sha256,
        )
        object.__setattr__(
            self,
            "plan_sha256",
            _hash_document(_PLAN_DOMAIN, _plan_document(self)),
        )

    @property
    def first_bar_open_ms(self) -> int:
        return self.attempt.horizon.h_start_ms

    @property
    def bar_open_limit_exclusive_ms(self) -> int:
        """Upper bound ``L`` such that every admitted bar open satisfies ``O < L``."""

        decision_offset_ms = (
            FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2
        )
        return self.attempt.horizon.admission_stop_ms - decision_offset_ms

    @property
    def expected_bar_count(self) -> int:
        return _grid_count(
            self.first_bar_open_ms,
            self.bar_open_limit_exclusive_ms,
        )

    @property
    def expected_cell_count(self) -> int:
        return (
            self.expected_bar_count
            * len(self.symbols)
            * len(self.family_rules)
        )

    @property
    def segment_count(self) -> int:
        first_day = _day_start(self.first_bar_open_ms)
        final_open = self._final_bar_open_ms
        if final_open is None:
            return 0
        return ((_day_start(final_open) - first_day) // MILLISECONDS_PER_DAY_V2) + 1

    @property
    def _final_bar_open_ms(self) -> int | None:
        if self.expected_bar_count == 0:
            return None
        return (
            self.first_bar_open_ms
            + (self.expected_bar_count - 1) * FIVE_MINUTE_MS_V2
        )

    def segment_for_day(self, day_start_ms: int) -> ProspectiveCensusSegmentV2:
        """Return the exact bounded segment for one in-horizon UTC day."""

        _validate_nonnegative_ms(day_start_ms, "day_start_ms")
        if day_start_ms % MILLISECONDS_PER_DAY_V2 != 0:
            raise ProspectiveCensusContractErrorV2(
                "segment day_start_ms must align to UTC midnight"
            )
        first_day = _day_start(self.first_bar_open_ms)
        final_open = self._final_bar_open_ms
        if final_open is None or not (
            first_day <= day_start_ms <= _day_start(final_open)
        ):
            raise ProspectiveCensusContractErrorV2(
                "segment day is outside the frozen prospective grid"
            )
        first_open = max(self.first_bar_open_ms, day_start_ms)
        raw_stop = min(
            self.bar_open_limit_exclusive_ms,
            day_start_ms + MILLISECONDS_PER_DAY_V2,
        )
        bar_count = _grid_count(first_open, raw_stop)
        stop = first_open + bar_count * FIVE_MINUTE_MS_V2
        return ProspectiveCensusSegmentV2(
            attempt_plan_sha256=self.plan_sha256,
            day_start_ms=day_start_ms,
            first_bar_open_ms=first_open,
            bar_open_stop_exclusive_ms=stop,
            expected_bar_count=bar_count,
            expected_cell_count=(
                bar_count * len(self.symbols) * len(self.family_rules)
            ),
        )

    def iter_segments(self) -> Iterator[ProspectiveCensusSegmentV2]:
        """Yield at most 365 bounded segment descriptors without cell allocation."""

        day_start_ms = _day_start(self.first_bar_open_ms)
        for _ in range(self.segment_count):
            yield self.segment_for_day(day_start_ms)
            day_start_ms += MILLISECONDS_PER_DAY_V2

    def expected_cell(
        self,
        *,
        family: PromotingFamilyV2,
        symbol: str,
        bar_open_ms: int,
    ) -> ProspectiveExpectedCellV2:
        """Validate membership and derive one deterministic expected-cell identity."""

        if not isinstance(family, PromotingFamilyV2):
            raise ProspectiveCensusContractErrorV2(
                "family must be PromotingFamilyV2"
            )
        rule_by_family = {
            binding.family: binding.rule_version for binding in self.family_rules
        }
        rule_version = rule_by_family.get(family)
        if rule_version is None:
            raise ProspectiveCensusContractErrorV2(
                "family is absent from the frozen census"
            )
        _validate_symbol(symbol)
        if symbol not in self.symbols:
            raise ProspectiveCensusContractErrorV2(
                "symbol is absent from the frozen census"
            )
        _validate_nonnegative_ms(bar_open_ms, "bar_open_ms")
        if bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise ProspectiveCensusContractErrorV2(
                "bar_open_ms must align to a 5m UTC boundary"
            )
        if not (
            self.first_bar_open_ms
            <= bar_open_ms
            < self.bar_open_limit_exclusive_ms
        ):
            raise ProspectiveCensusContractErrorV2(
                "bar_open_ms is outside the frozen admission grid"
            )
        segment = self.segment_for_day(_day_start(bar_open_ms))
        bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        decision_cutoff_ms = bar_close_ms + DECISION_DELAY_MS_V2
        if not self.attempt.horizon.admits_decision(decision_cutoff_ms):
            raise ProspectiveCensusContractErrorV2(
                "decision cutoff is outside the frozen efficacy admission window"
            )
        return ProspectiveExpectedCellV2(
            attempt_id=self.attempt_id,
            attempt_plan_sha256=self.plan_sha256,
            segment_id=segment.segment_id,
            family=family,
            rule_version=rule_version,
            symbol=symbol,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
        )

    def iter_expected_cells_for_segment(
        self,
        segment: ProspectiveCensusSegmentV2,
    ) -> Iterator[ProspectiveExpectedCellV2]:
        """Yield one day's bounded cell set in deterministic canonical order."""

        expected = self.segment_for_day(segment.day_start_ms)
        if segment != expected:
            raise ProspectiveCensusContractErrorV2(
                "segment differs from this plan's exact day projection"
            )
        for bar_open_ms in range(
            segment.first_bar_open_ms,
            segment.bar_open_stop_exclusive_ms,
            FIVE_MINUTE_MS_V2,
        ):
            for symbol in self.symbols:
                for binding in self.family_rules:
                    yield self.expected_cell(
                        family=binding.family,
                        symbol=symbol,
                        bar_open_ms=bar_open_ms,
                    )


def canonical_prospective_census_plan_v2(
    plan: ProspectiveCensusPlanV2,
) -> bytes:
    """Return the canonical self-hash-checked plan record."""

    if type(plan) is not ProspectiveCensusPlanV2:
        raise ProspectiveCensusContractErrorV2(
            "plan must be an exact ProspectiveCensusPlanV2"
        )
    document = _plan_document(plan)
    expected_sha256 = _hash_document(_PLAN_DOMAIN, document)
    if plan.plan_sha256 != expected_sha256:
        raise ProspectiveCensusContractErrorV2(
            "prospective census plan hash differs from canonical content"
        )
    return canonical_json_line({**document, "plan_sha256": plan.plan_sha256})


def canonical_prospective_census_segment_v2(
    segment: ProspectiveCensusSegmentV2,
) -> bytes:
    """Return the canonical self-hash-checked daily segment descriptor."""

    if type(segment) is not ProspectiveCensusSegmentV2:
        raise ProspectiveCensusContractErrorV2(
            "segment must be an exact ProspectiveCensusSegmentV2"
        )
    document = _segment_document(segment)
    expected_id = _hash_document(_SEGMENT_DOMAIN, document)
    if segment.segment_id != expected_id:
        raise ProspectiveCensusContractErrorV2(
            "prospective segment ID differs from canonical content"
        )
    return canonical_json_line({**document, "segment_id": segment.segment_id})


def canonical_prospective_expected_cell_v2(
    cell: ProspectiveExpectedCellV2,
) -> bytes:
    """Return the canonical self-hash-checked expected-cell descriptor."""

    if type(cell) is not ProspectiveExpectedCellV2:
        raise ProspectiveCensusContractErrorV2(
            "cell must be an exact ProspectiveExpectedCellV2"
        )
    document = _cell_document(cell)
    expected_id = _hash_document(_CELL_DOMAIN, document)
    if cell.cell_id != expected_id:
        raise ProspectiveCensusContractErrorV2(
            "prospective cell ID differs from canonical content"
        )
    return canonical_json_line({**document, "cell_id": cell.cell_id})


def _grid_count(first_open_ms: int, raw_stop_exclusive_ms: int) -> int:
    if raw_stop_exclusive_ms <= first_open_ms:
        return 0
    span = raw_stop_exclusive_ms - first_open_ms
    return (span + FIVE_MINUTE_MS_V2 - 1) // FIVE_MINUTE_MS_V2


def _day_start(value_ms: int) -> int:
    return value_ms - (value_ms % MILLISECONDS_PER_DAY_V2)


def _normalize_symbols(symbols: tuple[str, ...]) -> tuple[str, ...]:
    if type(symbols) is not tuple:
        raise ProspectiveCensusContractErrorV2("symbols must be an immutable tuple")
    if not 1 <= len(symbols) <= MAX_PROSPECTIVE_SYMBOLS_V2:
        raise ProspectiveCensusContractErrorV2(
            f"symbols must contain 1..{MAX_PROSPECTIVE_SYMBOLS_V2} members"
        )
    for symbol in symbols:
        _validate_symbol(symbol)
    normalized = tuple(sorted(symbols))
    if len(set(normalized)) != len(normalized):
        raise ProspectiveCensusContractErrorV2(
            "prospective symbols must be unique"
        )
    return normalized


def _validate_family_rules(
    bindings: tuple[ProspectiveFamilyRuleBindingV2, ...],
) -> None:
    if type(bindings) is not tuple or any(
        not isinstance(binding, ProspectiveFamilyRuleBindingV2)
        for binding in bindings
    ):
        raise ProspectiveCensusContractErrorV2(
            "family_rules must be an immutable binding tuple"
        )
    observed = tuple(binding.family for binding in bindings)
    if observed != _ALL_FAMILIES:
        raise ProspectiveCensusContractErrorV2(
            "family_rules must contain A, B, C exactly once in canonical order"
        )


def _plan_document(plan: ProspectiveCensusPlanV2) -> dict[str, object]:
    return {
        "attempt": {
            "attempt_index": plan.attempt.attempt_index,
            "h_max_ms": plan.attempt.horizon.h_max_ms,
            "h_start_ms": plan.attempt.horizon.h_start_ms,
            "qualification_start_ms": plan.attempt.qualification_start_ms,
        },
        "attempt_id": plan.attempt_id,
        "bar_open_limit_exclusive_ms": plan.bar_open_limit_exclusive_ms,
        "created_at_ms": plan.created_at_ms,
        "context_symbols": list(plan.context_symbols),
        "context_universe_sha256": plan.context_universe_sha256,
        "execution_contract_sha256": plan.execution_contract_sha256,
        "efficacy_gate_contract_sha256": plan.efficacy_gate_contract_sha256,
        "expected_bar_count": plan.expected_bar_count,
        "expected_cell_count": plan.expected_cell_count,
        "family_rules": [
            {
                "family": binding.family.value,
                "rule_version": binding.rule_version,
            }
            for binding in plan.family_rules
        ],
        "paper_fok_rule_version": plan.paper_fok_rule_version,
        "promoting_plan_sha256": plan.promoting_plan_sha256,
        "schema_version": plan.schema_version,
        "segment_count": plan.segment_count,
        "strategy_code_freeze_manifest_sha256": (
            plan.strategy_code_freeze_manifest_sha256
        ),
        "symbols": list(plan.symbols),
        "universe_sha256": plan.universe_sha256,
    }


def _segment_document(segment: ProspectiveCensusSegmentV2) -> dict[str, object]:
    return {
        "attempt_plan_sha256": segment.attempt_plan_sha256,
        "bar_open_stop_exclusive_ms": segment.bar_open_stop_exclusive_ms,
        "day_start_ms": segment.day_start_ms,
        "expected_bar_count": segment.expected_bar_count,
        "expected_cell_count": segment.expected_cell_count,
        "first_bar_open_ms": segment.first_bar_open_ms,
        "schema_version": segment.schema_version,
    }


def _cell_document(cell: ProspectiveExpectedCellV2) -> dict[str, object]:
    return {
        "attempt_id": cell.attempt_id,
        "attempt_plan_sha256": cell.attempt_plan_sha256,
        "bar_close_ms": cell.bar_close_ms,
        "bar_open_ms": cell.bar_open_ms,
        "decision_cutoff_ms": cell.decision_cutoff_ms,
        "family": cell.family.value,
        "rule_version": cell.rule_version,
        "schema_version": cell.schema_version,
        "segment_id": cell.segment_id,
        "symbol": cell.symbol,
    }


def _hash_document(domain: bytes, document: object) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _validate_identity(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
    ):
        raise ProspectiveCensusContractErrorV2(
            f"{field_name} must be a normalized non-empty bounded string"
        )


def _validate_symbol(value: object) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise ProspectiveCensusContractErrorV2(
            "symbol must be a normalized USDT symbol"
        )


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectiveCensusContractErrorV2(
            f"{field_name} must be lowercase SHA-256 hex"
        )


def _validate_nonnegative_ms(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ProspectiveCensusContractErrorV2(
            f"{field_name} must be a nonnegative Unix-millisecond integer"
        )


def _validate_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ProspectiveCensusContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
