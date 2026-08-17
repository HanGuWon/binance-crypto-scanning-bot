"""Frozen, non-promoting historical development proxy for D1 SCEFB-5M.

This module may reject D1 descriptively, but historical open prices are not
decision-time BBO/depth.  Every episode and aggregate therefore remains
``INCONCLUSIVE_NO_HISTORICAL_BBO`` and cannot authorize efficacy, probability,
promotion, PAPER fills, or production orders.

The public run boundary only loads a caller-pinned code freeze and byte-hash
authenticated inputs.  No threshold or universe member is configurable.
"""

from __future__ import annotations

import csv
import ctypes
import errno
import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from bisect import bisect_left, bisect_right
from collections.abc import Callable, Iterable, Sequence
from dataclasses import InitVar, asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, DecimalException, InvalidOperation, localcontext
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import BinaryIO, Final, Literal, cast

from signalbot.backtest.d1_scefb_historical_math import (
    D1HistoricalExecutionV0,
    D1HistoricalFeeCellV0,
    D1HistoricalFundingBoundaryAmbiguityV0,
    D1HistoricalFundingPointV0,
    D1HistoricalMathErrorV0,
    build_d1_historical_funding_point_v0,
    calculate_d1_historical_execution_v0,
    d1_historical_entry_execution_price_v0,
    d1_historical_exit_execution_price_v0,
    project_d1_historical_pnl_v0,
)
from signalbot.backtest.dataset import (
    DatasetManifest,
)
from signalbot.backtest.downstream_code_freeze import (
    DownstreamCodeFreezeAuthorityV1,
    load_downstream_code_freeze_v1,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import DECISION_DELAY_MS_V2
from signalbot.r4b_v2.strategy.d1_scefb import (
    D1_HOURLY_BAR_COUNT_V0,
    D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0,
    D1EntryDecisionV0,
    D1EntryReferenceKindV0,
    D1EntryStatusV0,
    D1ExitDecisionV0,
    D1ExitReasonV0,
    D1ExitStatusV0,
    D1FiveMinuteBarV0,
    D1HourlyBarV0,
    D1PaperPositionAnchorV0,
    D1SideV0,
    build_d1_entry_input_v0,
    build_d1_exit_input_v0,
    build_d1_five_minute_bar_v0,
    build_d1_hourly_bar_v0,
    build_d1_paper_position_anchor_v0,
    canonical_d1_entry_decision_v0,
    canonical_d1_exit_decision_v0,
    canonical_d1_five_minute_bar_v0,
    canonical_d1_hourly_bar_v0,
    evaluate_d1_entry_v0,
    evaluate_d1_exit_v0,
)

D1_HISTORICAL_DEVELOPMENT_RULE_V0: Final = "D1_SCEFB_HISTORICAL_DEVELOPMENT_A0_V0"
D1_HISTORICAL_RESULT_STATUS_V0: Final = "INCONCLUSIVE_NO_HISTORICAL_BBO"
D1_HISTORICAL_RECEIPT_CONVENTION_V0: Final = (
    "HISTORICAL_PROXY_RECEIPT_AND_DATA_THROUGH_EQUAL_EXCHANGE_CLOSE"
)
D1_HISTORICAL_UNIVERSE_V0: Final = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ARBUSDT",
    "OPUSDT",
    "SUIUSDT",
    "WIFUSDT",
)
D1_HISTORICAL_ALIAS_BY_SYMBOL_V0: Final = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "BNBUSDT": "BNB",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
    "DOGEUSDT": "DOGE",
    "ARBUSDT": "ARB",
    "OPUSDT": "OP",
    "SUIUSDT": "SUI",
    "WIFUSDT": "WIF",
}
D1_HISTORICAL_DATA_START_MS_V0: Final = 1_709_251_200_000
D1_HISTORICAL_DEVELOPMENT_START_MS_V0: Final = 1_719_792_000_000
D1_HISTORICAL_DEVELOPMENT_END_MS_V0: Final = 1_782_864_000_000
D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0: Final = 245_376
D1_HISTORICAL_HOURLY_ROW_COUNT_V0: Final = 20_448
D1_HISTORICAL_MAX_FIVE_MINUTE_ROWS_V0: Final = 245_376
D1_HISTORICAL_MAX_FIVE_MINUTE_DECOMPRESSED_BYTES_V0: Final = 256 * 1024 * 1024
D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0: Final = 30_000
D1_HISTORICAL_MAX_HOURLY_DECOMPRESSED_BYTES_V0: Final = 64 * 1024 * 1024
D1_HISTORICAL_MAX_FUNDING_ROWS_V0: Final = 10_000
D1_HISTORICAL_MAX_FUNDING_DECOMPRESSED_BYTES_V0: Final = 16 * 1024 * 1024
D1_HISTORICAL_MAX_AUTHORITY_BYTES_V0: Final = 1024 * 1024
D1_HISTORICAL_MAX_EPISODES_V0: Final = 1_000_000
D1_HISTORICAL_MAX_CENSORS_V0: Final = 100_000
D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0: Final = 256 * 1024 * 1024
D1_HISTORICAL_WINDOWS_ARTIFACT_DURABILITY_CONTRACT_V0: Final = (
    "WINDOWS_LOCAL_FIXED_NTFS_FILE_STAGING_OUTPUT_AND_PARENT_DIRECTORY_FLUSH_"
    "ATOMIC_RENAME_NOREPLACE_V0"
)
D1_HISTORICAL_POSIX_ARTIFACT_DURABILITY_CONTRACT_V0: Final = (
    "POSIX_FILE_AND_DIRECTORY_FSYNC_WITH_RENAMEAT2_NOREPLACE_V0"
)

D1_DEVELOPMENT_FREEZE_PURPOSE_V0: Final = (
    "D1_SCEFB_HISTORICAL_DEVELOPMENT_OUTCOME_BLIND_A1_POLICY_ORDER_CORRECTION"
)
D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0: Final = ("src/signalbot",)
_PREREGISTRATION_RELATIVE_PATH: Final = "docs/r4b-v2-d1-scefb-5m-preregistration-v0.md"
_INPUT_AUTHORITY_CORRECTION_RELATIVE_PATH: Final = (
    "docs/r4b-v2-d1-scefb-input-authority-path-correction-v0.md"
)
_FREEZE_POLICY_ORDER_CORRECTION_RELATIVE_PATH: Final = (
    "docs/r4b-v2-d1-scefb-development-freeze-policy-order-correction-a1.md"
)
D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze/"
    "freeze_manifest.json"
)
D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0: Final = (
    "328899911e4b1dd3acd9f12b5f1d8cd1f08f5df08b55d350670215649efa8316"
)
D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0: Final = (
    ".python-version",
    D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0,
    _PREREGISTRATION_RELATIVE_PATH,
    _FREEZE_POLICY_ORDER_CORRECTION_RELATIVE_PATH,
    _INPUT_AUTHORITY_CORRECTION_RELATIVE_PATH,
    "pyproject.toml",
    "tests/unit/r4b_v2/strategy/test_d1_scefb.py",
    "tests/unit/test_d1_scefb_historical_attempt_wal.py",
    "tests/unit/test_d1_scefb_historical_development.py",
    "tests/unit/test_d1_scefb_historical_math.py",
    "tests/unit/test_d1_scefb_historical_operator.py",
    "uv.lock",
)
D1_DEVELOPMENT_FREEZE_SUFFIXES_V0: Final = (".py",)

D1_HISTORICAL_FUNDING_AUTHORITY_PROTOCOL_V0: Final = (
    "d1_scefb_historical_funding_authority_v0_2026-07-21"
)
D1_HISTORICAL_FUNDING_AUTHORITY_SCHEMA_V0: Final = 1

_FIVE_MINUTE_MS: Final = 300_000
_HOUR_MS: Final = 3_600_000
_STANDARD_FUNDING_INTERVAL_MS: Final = 8 * _HOUR_MS
_UTC_DAY_MS: Final = 86_400_000
_MAX_CSV_LINE_BYTES: Final = 1024 * 1024
_BINARY_READ_CHUNK_BYTES: Final = 1024 * 1024
_NOTIONALS: Final = (Decimal("100"), Decimal("1000"))
_FEE_CELLS: Final = (
    (Decimal("1.0"), D1HistoricalFeeCellV0.PRIMARY_1_0),
    (Decimal("1.5"), D1HistoricalFeeCellV0.STRESS_1_5),
)
_MEAN_SCREEN_MIN: Final = Decimal("0.0005")
_PF_SCREEN_MIN: Final = Decimal("1.20")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_PATH_RE: Final = re.compile(r"^[A-Za-z0-9._/-]+$")

_INPUT_AUTHORITY_FACTORY_TOKEN: Final = object()
_FREEZE_FACTORY_TOKEN: Final = object()
_PROJECTION_FACTORY_TOKEN: Final = object()
_EPISODE_FACTORY_TOKEN: Final = object()
_CENSOR_FACTORY_TOKEN: Final = object()
_SUMMARY_FACTORY_TOKEN: Final = object()
_RESULT_FACTORY_TOKEN: Final = object()
_PREFILTER_FACTORY_TOKEN: Final = object()
_BREAKDOWN_FACTORY_TOKEN: Final = object()
_FEE_AGGREGATE_FACTORY_TOKEN: Final = object()
_ARTIFACT_FACTORY_TOKEN: Final = object()
_SERIALIZED_VERIFICATION_FACTORY_TOKEN: Final = object()
_REPLAY_CORE_RESULT_FACTORY_TOKEN: Final = object()
_AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN: Final = object()
_AUTHENTICATED_FUNDING_FACTORY_TOKEN: Final = object()

D1_HISTORICAL_SOURCE_ROOT_POLICY_STATIC_V0: Final = "STATIC_V0"
D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0: Final = "USED_ROWS_V0"
type D1HistoricalSourceRootPolicyV0 = Literal["STATIC_V0", "USED_ROWS_V0"]

_INPUT_AUTHORITY_HASH_DOMAIN: Final = b"D1_HISTORICAL_INPUT_AUTHORITY_V0\0"
_FREEZE_RECEIPT_HASH_DOMAIN: Final = b"D1_HISTORICAL_FREEZE_RECEIPT_V0\0"
_STATISTICAL_UNIT_ID_DOMAIN: Final = b"D1_HISTORICAL_STATISTICAL_UNIT_V0\0"
_EPISODE_HASH_DOMAIN: Final = b"D1_HISTORICAL_EPISODE_V0\0"
_CENSOR_HASH_DOMAIN: Final = b"D1_HISTORICAL_CENSOR_V0\0"
_SUMMARY_HASH_DOMAIN: Final = b"D1_HISTORICAL_SUMMARY_V0\0"
_RESULT_HASH_DOMAIN: Final = b"D1_HISTORICAL_RESULT_V0\0"
_SOURCE_ROOT_HASH_DOMAIN: Final = b"D1_HISTORICAL_SOURCE_ROOT_V0\0"
_ENTRY_REFERENCE_HASH_DOMAIN: Final = b"D1_HISTORICAL_ENTRY_REFERENCE_V0\0"
_EPISODE_SEQUENCE_ROOT_DOMAIN: Final = b"D1_HISTORICAL_EPISODE_SEQUENCE_ROOT_V0\0"
_CENSOR_SEQUENCE_ROOT_DOMAIN: Final = b"D1_HISTORICAL_CENSOR_SEQUENCE_ROOT_V0\0"
_ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0: Final = (
    "historical artifact publication is durability-ambiguous after the no-replace "
    "directory commit; do not retry, delete, or replace the target; inspect it read-only"
)
_USED_ROWS_ENTRY_SOURCE_ROOT_DOMAIN: Final = b"D1_HISTORICAL_USED_ROWS_ENTRY_ROOT_V0\0"
_USED_ROWS_EXIT_SOURCE_ROOT_DOMAIN: Final = b"D1_HISTORICAL_USED_ROWS_EXIT_ROOT_V0\0"
_USED_ROWS_FIVE_MINUTE_SEQUENCE_DOMAIN: Final = (
    b"D1_HISTORICAL_USED_ROWS_FIVE_MINUTE_SEQUENCE_V0\0"
)
_USED_ROWS_HOURLY_SEQUENCE_DOMAIN: Final = b"D1_HISTORICAL_USED_ROWS_HOURLY_SEQUENCE_V0\0"

_RUNNER_RELATIVE_PATH: Final = "src/signalbot/backtest/d1_scefb_historical_development.py"
_RULE_RELATIVE_PATH: Final = "src/signalbot/r4b_v2/strategy/d1_scefb.py"


class D1HistoricalDevelopmentContractErrorV0(ValueError):
    """Raised when the frozen D1 development contract is not exact."""


class D1HistoricalArtifactDurabilityErrorV0(D1HistoricalDevelopmentContractErrorV0):
    """Raised when artifact persistence cannot be proved without destructive recovery."""


class D1HistoricalDispositionV0(StrEnum):
    INCONCLUSIVE_LOW_INFORMATION = "INCONCLUSIVE_LOW_INFORMATION"
    RETROSPECTIVE_PROXY_REJECT = "RETROSPECTIVE_PROXY_REJECT"
    RETROSPECTIVE_PROXY_SCREEN_PASS_INCONCLUSIVE = "RETROSPECTIVE_PROXY_SCREEN_PASS_INCONCLUSIVE"
    INCONCLUSIVE_MIXED_PROXY_EVIDENCE = "INCONCLUSIVE_MIXED_PROXY_EVIDENCE"


class D1HistoricalCensorStageV0(StrEnum):
    ENTRY_REFERENCE = "ENTRY_REFERENCE"
    EXIT_OBSERVATION = "EXIT_OBSERVATION"
    EXIT_REFERENCE = "EXIT_REFERENCE"


class D1HistoricalFundingInconclusiveReasonV0(StrEnum):
    FUNDING_ENDPOINT_EQUALITY = "FUNDING_ENDPOINT_EQUALITY"
    MISSING_INTERIOR_FUNDING_MARK = "MISSING_INTERIOR_FUNDING_MARK"
    FUNDING_COVERAGE_UNAVAILABLE = "FUNDING_COVERAGE_UNAVAILABLE"


class D1HistoricalFundingCoverageStatusV0(StrEnum):
    EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE = "EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE"
    FUNDING_COVERAGE_UNAVAILABLE = "FUNDING_COVERAGE_UNAVAILABLE"


class D1HistoricalPrefilterStatusV0(StrEnum):
    CANDIDATE_LONG = "CANDIDATE_LONG"
    CANDIDATE_SHORT = "CANDIDATE_SHORT"
    NECESSARY_GATE_FALSE = "NECESSARY_GATE_FALSE"
    INVALID_INPUT_INCONCLUSIVE = "INVALID_INPUT_INCONCLUSIVE"


class D1HistoricalBreakdownKindV0(StrEnum):
    OVERALL = "OVERALL"
    SYMBOL = "SYMBOL"
    SIDE = "SIDE"
    SYMBOL_SIDE = "SYMBOL_SIDE"
    EXIT_REASON = "EXIT_REASON"


@dataclass(frozen=True, slots=True)
class D1HistoricalPrefilterResultV0:
    status: D1HistoricalPrefilterStatusV0
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PREFILTER_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "prefilter results must be evaluator-created"
            )
        if not isinstance(self.status, D1HistoricalPrefilterStatusV0):
            raise D1HistoricalDevelopmentContractErrorV0("prefilter status is unsupported")
        if (
            type(self.reasons) is not tuple
            or not self.reasons
            or any(not isinstance(value, str) or not value for value in self.reasons)
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "prefilter reasons must be a nonempty exact tuple"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalProjectionCellV0:
    statistical_unit_id: str
    notional_usdt: Decimal
    fee_multiplier: Decimal
    fee_rate_per_side: Decimal
    gross_return: Decimal
    executable_return_before_fee_funding: Decimal
    slippage_return: Decimal
    fee_return: Decimal
    funding_return: Decimal | None
    net_return: Decimal | None
    projected_net_pnl_usdt: Decimal | None
    _factory_token: InitVar[object | None] = None
    sizing_projection_creates_new_statistical_unit: bool = field(
        init=False,
        default=False,
    )
    schema_version: str = field(
        init=False,
        default="d1_historical_projection_cell_v0",
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PROJECTION_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "projection cells must be created by the development engine"
            )
        _require_sha256(self.statistical_unit_id, "statistical_unit_id")
        if self.notional_usdt not in _NOTIONALS:
            raise D1HistoricalDevelopmentContractErrorV0(
                "projection notional is outside the frozen 100/1000 cells"
            )
        expected_cell = dict(_FEE_CELLS).get(self.fee_multiplier)
        if expected_cell is None or self.fee_rate_per_side != expected_cell.rate_per_side:
            raise D1HistoricalDevelopmentContractErrorV0(
                "projection fee multiplier/rate is outside the frozen cells"
            )
        for value, label in (
            (self.gross_return, "gross_return"),
            (
                self.executable_return_before_fee_funding,
                "executable_return_before_fee_funding",
            ),
            (self.slippage_return, "slippage_return"),
            (self.fee_return, "fee_return"),
        ):
            _require_finite_decimal(value, label)
        optional = (
            self.funding_return,
            self.net_return,
            self.projected_net_pnl_usdt,
        )
        if any(value is None for value in optional) != all(value is None for value in optional):
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding, net return, and PnL must be present or absent together"
            )
        for value in optional:
            if value is not None:
                _require_finite_decimal(value, "optional projection value")


@dataclass(frozen=True, slots=True)
class D1HistoricalEpisodeV0:
    statistical_unit_id: str
    symbol: str
    side: D1SideV0
    signal_event_id: str
    signal_payload_sha256: str
    signal_bar_open_ms: int
    signal_decision_cutoff_ms: int
    entry_reference_time_ms: int
    entry_reference_price: Decimal
    entry_executable_price: Decimal
    exit_observation_open_ms: int
    exit_observation_close_ms: int
    exit_decision_event_id: str
    exit_decision_payload_sha256: str
    exit_reason: D1ExitReasonV0
    exit_reference_time_ms: int
    exit_reference_price: Decimal
    exit_executable_price: Decimal
    funding_event_count: int
    funding_evaluable: bool
    funding_inconclusive_reason: D1HistoricalFundingInconclusiveReasonV0 | None
    projections: tuple[D1HistoricalProjectionCellV0, ...]
    five_minute_manifest_sha256: str
    hourly_manifest_sha256: str
    funding_file_sha256: str
    _factory_token: InitVar[object | None] = None
    episode_sha256: str = field(init=False)
    status: str = field(init=False, default=D1_HISTORICAL_RESULT_STATUS_V0)
    historical_receipt_proxy: bool = field(init=False, default=True)
    historical_bbo_available: bool = field(init=False, default=False)
    paper_fill_claim: bool = field(init=False, default=False)
    execution_conclusive: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    prospective: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_historical_episode_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _EPISODE_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "episodes must be created by the development engine"
            )
        for value, label in (
            (self.statistical_unit_id, "statistical_unit_id"),
            (self.signal_event_id, "signal_event_id"),
            (self.signal_payload_sha256, "signal_payload_sha256"),
            (self.exit_decision_event_id, "exit_decision_event_id"),
            (self.exit_decision_payload_sha256, "exit_decision_payload_sha256"),
            (self.five_minute_manifest_sha256, "five_minute_manifest_sha256"),
            (self.hourly_manifest_sha256, "hourly_manifest_sha256"),
            (self.funding_file_sha256, "funding_file_sha256"),
        ):
            _require_sha256(value, label)
        _require_symbol(self.symbol)
        if not isinstance(self.side, D1SideV0):
            raise D1HistoricalDevelopmentContractErrorV0("episode side is invalid")
        for value, label in (
            (self.signal_bar_open_ms, "signal_bar_open_ms"),
            (self.signal_decision_cutoff_ms, "signal_decision_cutoff_ms"),
            (self.entry_reference_time_ms, "entry_reference_time_ms"),
            (self.exit_observation_open_ms, "exit_observation_open_ms"),
            (self.exit_observation_close_ms, "exit_observation_close_ms"),
            (self.exit_reference_time_ms, "exit_reference_time_ms"),
        ):
            _require_nonnegative_int(value, label)
        if not (
            self.signal_decision_cutoff_ms
            < self.entry_reference_time_ms
            < self.exit_reference_time_ms
        ):
            raise D1HistoricalDevelopmentContractErrorV0("episode entry/exit chronology is invalid")
        if not (
            self.exit_observation_open_ms
            <= self.exit_observation_close_ms
            < self.exit_reference_time_ms
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "episode exit observation/reference chronology is invalid"
            )
        for value, label in (
            (self.entry_reference_price, "entry_reference_price"),
            (self.entry_executable_price, "entry_executable_price"),
            (self.exit_reference_price, "exit_reference_price"),
            (self.exit_executable_price, "exit_executable_price"),
        ):
            _require_positive_decimal(value, label)
        _require_nonnegative_int(self.funding_event_count, "funding_event_count")
        if type(self.funding_evaluable) is not bool:
            raise D1HistoricalDevelopmentContractErrorV0("funding_evaluable must be boolean")
        if type(self.projections) is not tuple or len(self.projections) != 4:
            raise D1HistoricalDevelopmentContractErrorV0(
                "each episode requires exactly four declared projection cells"
            )
        expected_cells = tuple(
            (notional, multiplier)
            for notional in _NOTIONALS
            for multiplier, _fee_cell in _FEE_CELLS
        )
        actual_cells = tuple(
            (value.notional_usdt, value.fee_multiplier) for value in self.projections
        )
        if actual_cells != expected_cells or any(
            type(value) is not D1HistoricalProjectionCellV0
            or value.statistical_unit_id != self.statistical_unit_id
            for value in self.projections
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "projection cells must be exact, ordered, and share one statistical unit"
            )
        has_missing_projection = any(value.net_return is None for value in self.projections)
        if self.funding_evaluable:
            funding_state_valid = (
                self.funding_inconclusive_reason is None and not has_missing_projection
            )
        else:
            funding_state_valid = (
                isinstance(
                    self.funding_inconclusive_reason,
                    D1HistoricalFundingInconclusiveReasonV0,
                )
                and has_missing_projection
                and all(value.net_return is None for value in self.projections)
            )
        if not funding_state_valid:
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding evaluability must exactly control projection net returns"
            )
        object.__setattr__(
            self,
            "episode_sha256",
            _hash_document(
                _EPISODE_HASH_DOMAIN,
                _episode_document(self, include_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D1HistoricalCensorV0:
    symbol: str
    signal_event_id: str
    signal_bar_open_ms: int
    stage: D1HistoricalCensorStageV0
    reason: str
    _factory_token: InitVar[object | None] = None
    censor_sha256: str = field(init=False)
    status: str = field(init=False, default="INCONCLUSIVE_RIGHT_EDGE_CENSORED")
    contributes_statistical_n: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_historical_censor_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CENSOR_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "censors must be created by the development engine"
            )
        _require_symbol(self.symbol)
        _require_sha256(self.signal_event_id, "signal_event_id")
        _require_nonnegative_int(self.signal_bar_open_ms, "signal_bar_open_ms")
        if not isinstance(self.stage, D1HistoricalCensorStageV0):
            raise D1HistoricalDevelopmentContractErrorV0("censor stage is invalid")
        _require_identity(self.reason, "censor reason")
        object.__setattr__(
            self,
            "censor_sha256",
            _hash_document(_CENSOR_HASH_DOMAIN, _censor_document(self, include_hash=False)),
        )


@dataclass(frozen=True, slots=True)
class D1HistoricalFeeAggregateV0:
    fee_multiplier: Decimal
    fee_rate_per_side: Decimal
    episode_count: int
    evaluable_episode_count: int
    total_net_return: Decimal | None
    mean_net_return: Decimal | None
    profit_factor: Decimal | None
    profit_factor_infinite: bool
    projected_total_pnl_100_usdt: Decimal | None
    projected_total_pnl_1000_usdt: Decimal | None
    positive_symbol_count: int
    net_after_top_three_symbols: Decimal | None
    net_after_top_ten_episodes: Decimal | None
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FEE_AGGREGATE_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0("fee aggregates must be reducer-created")
        expected_cell = dict(_FEE_CELLS).get(self.fee_multiplier)
        if expected_cell is None or self.fee_rate_per_side != expected_cell.rate_per_side:
            raise D1HistoricalDevelopmentContractErrorV0("fee aggregate cell is unsupported")
        _require_nonnegative_int(self.episode_count, "episode_count")
        _require_nonnegative_int(self.evaluable_episode_count, "evaluable_episode_count")
        _require_nonnegative_int(self.positive_symbol_count, "positive_symbol_count")
        if self.evaluable_episode_count > self.episode_count or self.positive_symbol_count > len(
            D1_HISTORICAL_UNIVERSE_V0
        ):
            raise D1HistoricalDevelopmentContractErrorV0("fee aggregate counts do not reconcile")
        optional = (
            self.total_net_return,
            self.mean_net_return,
            self.profit_factor,
            self.projected_total_pnl_100_usdt,
            self.projected_total_pnl_1000_usdt,
            self.net_after_top_three_symbols,
            self.net_after_top_ten_episodes,
        )
        for value in optional:
            if value is not None:
                _require_finite_decimal(value, "fee aggregate metric")
        if type(self.profit_factor_infinite) is not bool:
            raise D1HistoricalDevelopmentContractErrorV0("profit_factor_infinite must be boolean")
        if self.evaluable_episode_count == 0 and any(value is not None for value in optional):
            raise D1HistoricalDevelopmentContractErrorV0(
                "empty fee aggregate cannot contain metrics"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalBreakdownV0:
    kind: D1HistoricalBreakdownKindV0
    key: str
    fee_multiplier: Decimal
    fee_rate_per_side: Decimal
    episode_count: int
    evaluable_episode_count: int
    positive_episode_count: int
    strict_positive_hit_rate: Decimal | None
    total_net_return: Decimal | None
    mean_net_return: Decimal | None
    median_net_return: Decimal | None
    profit_factor: Decimal | None
    profit_factor_infinite: bool
    mean_gross_return: Decimal | None
    mean_slippage_return: Decimal | None
    mean_fee_return: Decimal | None
    mean_funding_return: Decimal | None
    projected_total_pnl_100_usdt: Decimal | None
    projected_total_pnl_1000_usdt: Decimal | None
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _BREAKDOWN_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0("breakdowns must be reducer-created")
        if not isinstance(self.kind, D1HistoricalBreakdownKindV0):
            raise D1HistoricalDevelopmentContractErrorV0("breakdown kind is unsupported")
        _require_identity(self.key, "breakdown key")
        expected_cell = dict(_FEE_CELLS).get(self.fee_multiplier)
        if expected_cell is None or self.fee_rate_per_side != expected_cell.rate_per_side:
            raise D1HistoricalDevelopmentContractErrorV0("breakdown fee cell is unsupported")
        for value, label in (
            (self.episode_count, "episode_count"),
            (self.evaluable_episode_count, "evaluable_episode_count"),
            (self.positive_episode_count, "positive_episode_count"),
        ):
            _require_nonnegative_int(value, label)
        if not (self.positive_episode_count <= self.evaluable_episode_count <= self.episode_count):
            raise D1HistoricalDevelopmentContractErrorV0("breakdown counts do not reconcile")
        optional = (
            self.strict_positive_hit_rate,
            self.total_net_return,
            self.mean_net_return,
            self.median_net_return,
            self.profit_factor,
            self.mean_gross_return,
            self.mean_slippage_return,
            self.mean_fee_return,
            self.mean_funding_return,
            self.projected_total_pnl_100_usdt,
            self.projected_total_pnl_1000_usdt,
        )
        for value in optional:
            if value is not None:
                _require_finite_decimal(value, "breakdown metric")
        if type(self.profit_factor_infinite) is not bool:
            raise D1HistoricalDevelopmentContractErrorV0("profit_factor_infinite must be boolean")
        if self.evaluable_episode_count == 0 and any(value is not None for value in optional):
            raise D1HistoricalDevelopmentContractErrorV0(
                "empty breakdown cannot contain economic metrics"
            )
        if self.evaluable_episode_count > 0 and any(
            value is None
            for value in (
                self.strict_positive_hit_rate,
                self.total_net_return,
                self.mean_net_return,
                self.median_net_return,
                self.mean_gross_return,
                self.mean_slippage_return,
                self.mean_fee_return,
                self.mean_funding_return,
                self.projected_total_pnl_100_usdt,
                self.projected_total_pnl_1000_usdt,
            )
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "nonempty breakdown requires complete economic metrics"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentSummaryV0:
    episode_count: int
    evaluable_episode_count: int
    global_nonoverlap_evaluable_count: int
    funding_coverage_status_by_symbol: tuple[tuple[str, str], ...]
    funding_inconclusive_counts: tuple[tuple[str, int], ...]
    long_episode_count: int
    short_episode_count: int
    evaluable_long_episode_count: int
    evaluable_short_episode_count: int
    active_utc_day_count: int
    full_signal_count: int
    entered_position_count: int
    prefilter_candidate_count: int
    prefilter_necessary_gate_false_count: int
    invalid_input_inconclusive_count: int
    pending_or_active_suppressed_signal_count: int
    entry_distance_rejection_count: int
    right_edge_censor_count: int
    exit_reason_counts: tuple[tuple[str, int], ...]
    fee_aggregates: tuple[D1HistoricalFeeAggregateV0, ...]
    breakdowns: tuple[D1HistoricalBreakdownV0, ...]
    disposition: D1HistoricalDispositionV0
    _factory_token: InitVar[object | None] = None
    summary_sha256: str = field(init=False)
    statistical_unit: str = field(
        init=False,
        default="ONE_NONOVERLAPPING_SYMBOL_EPISODE",
    )
    global_correlation_guard: str = field(
        init=False,
        default="EARLIEST_EXIT_INTERVAL_SCHEDULE_ENTRY_GTE_PRIOR_EXIT",
    )
    projection_cells_multiply_n: bool = field(init=False, default=False)
    bootstrap_performed: bool = field(init=False, default=False)
    status: str = field(init=False, default=D1_HISTORICAL_RESULT_STATUS_V0)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_historical_summary_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SUMMARY_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "summary must be created by the development engine"
            )
        for value, label in (
            (self.episode_count, "episode_count"),
            (self.evaluable_episode_count, "evaluable_episode_count"),
            (
                self.global_nonoverlap_evaluable_count,
                "global_nonoverlap_evaluable_count",
            ),
            (self.long_episode_count, "long_episode_count"),
            (self.short_episode_count, "short_episode_count"),
            (self.evaluable_long_episode_count, "evaluable_long_episode_count"),
            (self.evaluable_short_episode_count, "evaluable_short_episode_count"),
            (self.active_utc_day_count, "active_utc_day_count"),
            (self.full_signal_count, "full_signal_count"),
            (self.entered_position_count, "entered_position_count"),
            (self.prefilter_candidate_count, "prefilter_candidate_count"),
            (
                self.prefilter_necessary_gate_false_count,
                "prefilter_necessary_gate_false_count",
            ),
            (
                self.invalid_input_inconclusive_count,
                "invalid_input_inconclusive_count",
            ),
            (
                self.pending_or_active_suppressed_signal_count,
                "pending_or_active_suppressed_signal_count",
            ),
            (self.entry_distance_rejection_count, "entry_distance_rejection_count"),
            (self.right_edge_censor_count, "right_edge_censor_count"),
        ):
            _require_nonnegative_int(value, label)
        if self.long_episode_count + self.short_episode_count != self.episode_count:
            raise D1HistoricalDevelopmentContractErrorV0(
                "long/short counts do not reconcile with episode_count"
            )
        if (
            self.evaluable_long_episode_count + self.evaluable_short_episode_count
            != self.evaluable_episode_count
            or self.evaluable_episode_count > self.episode_count
            or self.global_nonoverlap_evaluable_count > self.evaluable_episode_count
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "evaluable long/short counts do not reconcile"
            )
        expected_funding_reasons = tuple(
            (
                value.value,
                next(
                    (
                        count
                        for name, count in self.funding_inconclusive_counts
                        if name == value.value
                    ),
                    0,
                ),
            )
            for value in D1HistoricalFundingInconclusiveReasonV0
        )
        if (
            self.funding_inconclusive_counts != expected_funding_reasons
            or sum(count for _, count in self.funding_inconclusive_counts)
            != self.episode_count - self.evaluable_episode_count
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding inconclusive counts do not reconcile"
            )
        allowed_coverage_statuses = {value.value for value in D1HistoricalFundingCoverageStatusV0}
        if (
            type(self.funding_coverage_status_by_symbol) is not tuple
            or any(
                type(value) is not tuple
                or len(value) != 2
                or not isinstance(value[0], str)
                or not isinstance(value[1], str)
                or value[1] not in allowed_coverage_statuses
                for value in self.funding_coverage_status_by_symbol
            )
            or tuple(value[0] for value in self.funding_coverage_status_by_symbol)
            != D1_HISTORICAL_UNIVERSE_V0
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding coverage receipt must contain the exact ordered universe"
            )
        if type(self.exit_reason_counts) is not tuple or any(
            not isinstance(name, str) or type(count) is not int or count < 0
            for name, count in self.exit_reason_counts
        ):
            raise D1HistoricalDevelopmentContractErrorV0("exit reason counts are invalid")
        if sum(count for _, count in self.exit_reason_counts) != self.episode_count:
            raise D1HistoricalDevelopmentContractErrorV0(
                "exit reason counts do not reconcile with episode_count"
            )
        if type(self.fee_aggregates) is not tuple or tuple(
            value.fee_multiplier for value in self.fee_aggregates
        ) != tuple(value for value, _ in _FEE_CELLS):
            raise D1HistoricalDevelopmentContractErrorV0(
                "summary requires exact 1.0/1.5 fee aggregates"
            )
        if any(
            value.episode_count != self.episode_count
            or value.evaluable_episode_count != self.evaluable_episode_count
            for value in self.fee_aggregates
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "fee aggregates do not reconcile with evaluable N"
            )
        if type(self.breakdowns) is not tuple or any(
            type(value) is not D1HistoricalBreakdownV0 for value in self.breakdowns
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "summary breakdowns must be an exact immutable tuple"
            )
        object.__setattr__(
            self,
            "summary_sha256",
            _hash_document(_SUMMARY_HASH_DOMAIN, _summary_document(self, include_hash=False)),
        )


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentResultV0:
    run_id: str
    run_started_at_ms: int
    input_authority_sha256: str
    code_freeze_receipt_sha256: str
    code_freeze_manifest_sha256: str
    preregistration_sha256: str
    episodes: tuple[D1HistoricalEpisodeV0, ...]
    censors: tuple[D1HistoricalCensorV0, ...]
    summary: D1HistoricalDevelopmentSummaryV0
    _factory_token: InitVar[object | None] = None
    result_sha256: str = field(init=False)
    development_start_ms: int = field(
        init=False,
        default=D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
    )
    development_end_ms_exclusive: int = field(
        init=False,
        default=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    )
    universe: tuple[str, ...] = field(
        init=False,
        default=D1_HISTORICAL_UNIVERSE_V0,
    )
    historical_receipt_convention: str = field(
        init=False,
        default=D1_HISTORICAL_RECEIPT_CONVENTION_V0,
    )
    post_development_end_rows_used: bool = field(init=False, default=False)
    existing_result_artifact_used_as_input: bool = field(init=False, default=False)
    historical_bbo_available: bool = field(init=False, default=False)
    paper_fill_claim: bool = field(init=False, default=False)
    execution_conclusive: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    prospective: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    rule_version: str = field(init=False, default=D1_HISTORICAL_DEVELOPMENT_RULE_V0)
    schema_version: str = field(init=False, default="d1_historical_development_result_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "result must be created by the development engine"
            )
        _require_identity(self.run_id, "run_id")
        _require_nonnegative_int(self.run_started_at_ms, "run_started_at_ms")
        for value, label in (
            (self.input_authority_sha256, "input_authority_sha256"),
            (self.code_freeze_receipt_sha256, "code_freeze_receipt_sha256"),
            (self.code_freeze_manifest_sha256, "code_freeze_manifest_sha256"),
            (self.preregistration_sha256, "preregistration_sha256"),
        ):
            _require_sha256(value, label)
        if type(self.episodes) is not tuple or len(self.episodes) > D1_HISTORICAL_MAX_EPISODES_V0:
            raise D1HistoricalDevelopmentContractErrorV0("episode artifact is not bounded")
        if type(self.censors) is not tuple or len(self.censors) > D1_HISTORICAL_MAX_CENSORS_V0:
            raise D1HistoricalDevelopmentContractErrorV0("censor artifact is not bounded")
        if len(self.episodes) != self.summary.episode_count:
            raise D1HistoricalDevelopmentContractErrorV0(
                "result episodes do not reconcile with summary"
            )
        if len(self.censors) != self.summary.right_edge_censor_count:
            raise D1HistoricalDevelopmentContractErrorV0(
                "result censors do not reconcile with summary"
            )
        object.__setattr__(
            self,
            "result_sha256",
            _hash_document(_RESULT_HASH_DOMAIN, _result_document(self, include_hash=False)),
        )


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentArtifactsV0:
    output_dir: Path
    manifest_sha256: str
    result_sha256: str
    output_file_sha256: tuple[tuple[str, str], ...]
    total_size_bytes: int
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ARTIFACT_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "artifact receipts must be publisher-created"
            )
        if not isinstance(self.output_dir, Path) or not self.output_dir.is_absolute():
            raise D1HistoricalDevelopmentContractErrorV0("artifact output_dir must be absolute")
        _require_sha256(self.manifest_sha256, "artifact manifest_sha256")
        _require_sha256(self.result_sha256, "artifact result_sha256")
        if type(self.output_file_sha256) is not tuple or any(
            type(value) is not tuple
            or len(value) != 2
            or not isinstance(value[0], str)
            or _SHA256_RE.fullmatch(value[1]) is None
            for value in self.output_file_sha256
        ):
            raise D1HistoricalDevelopmentContractErrorV0("artifact output hashes are invalid")
        _require_nonnegative_int(self.total_size_bytes, "artifact total_size_bytes")
        if self.total_size_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
            raise D1HistoricalDevelopmentContractErrorV0(
                "artifact receipt exceeds the frozen byte cap"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalSerializedArtifactsVerificationV0:
    """Recomputed identity of exact serialized D1 result artifacts."""

    result_sha256: str
    summary_sha256: str
    episode_count: int
    censor_count: int
    episode_sequence_root_sha256: str
    censor_sequence_root_sha256: str
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SERIALIZED_VERIFICATION_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized verification must be verifier-created"
            )
        for value, label in (
            (self.result_sha256, "serialized result_sha256"),
            (self.summary_sha256, "serialized summary_sha256"),
            (self.episode_sequence_root_sha256, "serialized episode root"),
            (self.censor_sequence_root_sha256, "serialized censor root"),
        ):
            _require_sha256(value, label)
        _require_nonnegative_int(self.episode_count, "serialized episode_count")
        _require_nonnegative_int(self.censor_count, "serialized censor_count")


@dataclass(frozen=True, slots=True)
class D1HistoricalFundingFileBindingV0:
    """Byte-hash authority for one exact recorded public funding file."""

    symbol: str
    relative_path: str
    sha256: str

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_relative_path(self.relative_path, "funding relative_path")
        _require_sha256(self.sha256, "funding sha256")


@dataclass(frozen=True, slots=True)
class _LoadedKlineFileV0:
    symbol: str
    interval: Literal["5m", "1h"]
    manifest_sha256: str
    data_sha256: str
    candles: tuple[Candle, ...]


@dataclass(frozen=True, slots=True)
class D1HistoricalAuthenticatedFiveMinuteV0:
    """Exact byte-authenticated 5m source returned by the historical loader."""

    symbol: str
    manifest_sha256: str
    data_sha256: str
    candles: tuple[Candle, ...]
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "authenticated five-minute sources must be loader-created"
            )
        _require_symbol(self.symbol)
        _require_sha256(self.manifest_sha256, "five-minute manifest_sha256")
        _require_sha256(self.data_sha256, "five-minute data_sha256")
        if type(self.candles) is not tuple or any(
            not isinstance(value, Candle)
            or value.symbol != self.symbol
            or value.interval != "5m"
            for value in self.candles
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "authenticated five-minute candles must be one exact immutable symbol tuple"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalAuthenticatedFundingV0:
    """Exact byte-authenticated funding source returned by the historical loader."""

    symbol: str
    file_sha256: str
    start_time_ms: int
    end_time_ms: int
    points: tuple[D1HistoricalFundingPointV0, ...]
    exact_standard_8h_development_coverage: bool
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _AUTHENTICATED_FUNDING_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "authenticated funding sources must be loader-created"
            )
        _require_symbol(self.symbol)
        _require_sha256(self.file_sha256, "funding file_sha256")
        _require_nonnegative_int(self.start_time_ms, "funding start_time_ms")
        _require_nonnegative_int(self.end_time_ms, "funding end_time_ms")
        if self.end_time_ms < self.start_time_ms:
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding source interval must be nonempty"
            )
        if type(self.points) is not tuple or any(
            not isinstance(value, D1HistoricalFundingPointV0) for value in self.points
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "authenticated funding points must be an exact immutable tuple"
            )
        if type(self.exact_standard_8h_development_coverage) is not bool:
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding coverage receipt must be boolean"
            )


@dataclass(frozen=True, slots=True)
class D1HistoricalReplaySymbolInputV0:
    """Already validated inputs for one symbol in the shared D1 replay core.

    ``higher_timeframe_source_sha256`` identifies the exact replay provenance
    of ``hourly``. D1 supplies its authenticated native-1h manifest digest; a
    later replay may instead supply a digest for causally derived hourly bars.
    Legacy D1 episode serialization deliberately retains the historical field
    name ``hourly_manifest_sha256`` through the compatibility property.
    """

    symbol: str
    five_minute_manifest_sha256: str
    higher_timeframe_source_sha256: str
    funding_file_sha256: str
    source_root_sha256: str
    five_minute: tuple[Candle, ...]
    hourly: tuple[Candle, ...]
    funding: tuple[D1HistoricalFundingPointV0, ...]
    exact_standard_8h_development_funding_coverage: bool
    source_root_policy: D1HistoricalSourceRootPolicyV0 = (
        D1_HISTORICAL_SOURCE_ROOT_POLICY_STATIC_V0
    )

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        for value, label in (
            (self.five_minute_manifest_sha256, "five-minute provenance sha256"),
            (self.higher_timeframe_source_sha256, "higher-timeframe provenance sha256"),
            (self.funding_file_sha256, "funding provenance sha256"),
            (self.source_root_sha256, "source root sha256"),
        ):
            _require_sha256(value, label)
        if type(self.five_minute) is not tuple or any(
            not isinstance(value, Candle) for value in self.five_minute
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay five-minute inputs must be an exact immutable Candle tuple"
            )
        if type(self.hourly) is not tuple or any(
            not isinstance(value, Candle) for value in self.hourly
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay hourly inputs must be an exact immutable Candle tuple"
            )
        if type(self.funding) is not tuple or any(
            not isinstance(value, D1HistoricalFundingPointV0) for value in self.funding
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay funding inputs must be an exact immutable funding tuple"
            )
        if type(self.exact_standard_8h_development_funding_coverage) is not bool:
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay funding coverage receipt must be boolean"
            )
        if self.source_root_policy not in {
            D1_HISTORICAL_SOURCE_ROOT_POLICY_STATIC_V0,
            D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
        }:
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay source-root policy is unsupported"
            )

    @property
    def hourly_manifest_sha256(self) -> str:
        """Legacy serialized field value for higher-timeframe provenance."""

        return self.higher_timeframe_source_sha256


@dataclass(slots=True)
class _RunCountersV0:
    full_signal_count: int = 0
    entered_position_count: int = 0
    prefilter_candidate_count: int = 0
    prefilter_necessary_gate_false_count: int = 0
    invalid_input_inconclusive_count: int = 0
    pending_or_active_suppressed_signal_count: int = 0
    entry_distance_rejection_count: int = 0


@dataclass(frozen=True, slots=True)
class _SymbolRunResultV0:
    symbol: str
    exact_standard_8h_development_funding_coverage: bool
    episodes: tuple[D1HistoricalEpisodeV0, ...]
    censors: tuple[D1HistoricalCensorV0, ...]
    counters: _RunCountersV0


@dataclass(frozen=True, slots=True)
class D1HistoricalReplayCoreResultV0:
    """Metadata-free, immutable result of the shared historical replay core."""

    episodes: tuple[D1HistoricalEpisodeV0, ...]
    censors: tuple[D1HistoricalCensorV0, ...]
    summary: D1HistoricalDevelopmentSummaryV0
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REPLAY_CORE_RESULT_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay core results must be engine-created"
            )
        if type(self.episodes) is not tuple or any(
            type(value) is not D1HistoricalEpisodeV0 for value in self.episodes
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay core episodes must be an exact immutable tuple"
            )
        if type(self.censors) is not tuple or any(
            type(value) is not D1HistoricalCensorV0 for value in self.censors
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay core censors must be an exact immutable tuple"
            )
        if type(self.summary) is not D1HistoricalDevelopmentSummaryV0:
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay core summary must be exact D1HistoricalDevelopmentSummaryV0"
            )
        if (
            len(self.episodes) != self.summary.episode_count
            or len(self.censors) != self.summary.right_edge_censor_count
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay core records do not reconcile with summary"
            )


@dataclass(frozen=True, slots=True)
class _PendingEntryStateV0:
    decision: D1EntryDecisionV0
    signal_index: int
    entry_reference_index: int


@dataclass(frozen=True, slots=True)
class _ActivePositionStateV0:
    decision: D1EntryDecisionV0
    signal_index: int
    entry_reference_index: int
    entry_reference_price: Decimal
    position: D1PaperPositionAnchorV0


@dataclass(frozen=True, slots=True)
class _PendingExitStateV0:
    active: _ActivePositionStateV0
    exit_decision: D1ExitDecisionV0
    exit_reference_index: int


@dataclass(frozen=True, slots=True)
class _RightEdgeReservedStateV0:
    signal_event_id: str


@dataclass(frozen=True, slots=True)
class D1HistoricalKlineManifestBindingV0:
    symbol: str
    interval: Literal["5m", "1h"]
    relative_manifest_path: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        if self.interval not in ("5m", "1h"):
            raise D1HistoricalDevelopmentContractErrorV0("kline interval must be exactly 5m or 1h")
        _require_relative_path(self.relative_manifest_path, "relative_manifest_path")
        _require_sha256(self.manifest_sha256, "manifest_sha256")


@dataclass(frozen=True, slots=True)
class D1HistoricalInputAuthorityV0:
    kline_manifests: tuple[D1HistoricalKlineManifestBindingV0, ...]
    funding_manifest_relative_path: str
    funding_manifest_sha256: str
    _factory_token: InitVar[object | None] = None
    authority_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default="d1_historical_input_authority_v0",
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _INPUT_AUTHORITY_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical input authority must be factory-created"
            )
        if type(self.kline_manifests) is not tuple or any(
            type(value) is not D1HistoricalKlineManifestBindingV0 for value in self.kline_manifests
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "kline manifests must be exact immutable bindings"
            )
        expected_keys = tuple(
            (symbol, interval) for symbol in D1_HISTORICAL_UNIVERSE_V0 for interval in ("5m", "1h")
        )
        actual_keys = tuple((value.symbol, value.interval) for value in self.kline_manifests)
        if actual_keys != expected_keys:
            raise D1HistoricalDevelopmentContractErrorV0(
                "input authority must contain the exact ordered 10-symbol 5m/1h panel"
            )
        paths = tuple(value.relative_manifest_path for value in self.kline_manifests)
        if len(set(paths)) != len(paths):
            raise D1HistoricalDevelopmentContractErrorV0(
                "kline authority contains duplicate manifest paths"
            )
        _require_relative_path(
            self.funding_manifest_relative_path,
            "funding_manifest_relative_path",
        )
        if self.funding_manifest_relative_path in set(paths):
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding and kline manifest paths must be distinct"
            )
        _require_sha256(self.funding_manifest_sha256, "funding_manifest_sha256")
        object.__setattr__(
            self,
            "authority_sha256",
            _hash_document(
                _INPUT_AUTHORITY_HASH_DOMAIN,
                _input_authority_document(self, include_hash=False),
            ),
        )

    def binding(self, symbol: str, interval: str) -> D1HistoricalKlineManifestBindingV0:
        for value in self.kline_manifests:
            if value.symbol == symbol and value.interval == interval:
                return value
        raise D1HistoricalDevelopmentContractErrorV0(
            "input authority is missing a frozen kline binding"
        )


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentFreezeV0:
    manifest_sha256: str
    manifest_created_at_ms: int
    input_authority_sha256: str
    preregistration_sha256: str
    frozen_file_count: int
    _factory_token: InitVar[object | None] = None
    receipt_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default="d1_historical_development_freeze_v0",
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FREEZE_FACTORY_TOKEN:
            raise D1HistoricalDevelopmentContractErrorV0(
                "development freeze must come from the pinned manifest loader"
            )
        for value, label in (
            (self.manifest_sha256, "manifest_sha256"),
            (self.input_authority_sha256, "input_authority_sha256"),
            (self.preregistration_sha256, "preregistration_sha256"),
        ):
            _require_sha256(value, label)
        _require_nonnegative_int(self.manifest_created_at_ms, "manifest_created_at_ms")
        if type(self.frozen_file_count) is not int or self.frozen_file_count < 2:
            raise D1HistoricalDevelopmentContractErrorV0(
                "freeze must cover at least the rule and runner sources"
            )
        object.__setattr__(
            self,
            "receipt_sha256",
            _hash_document(_FREEZE_RECEIPT_HASH_DOMAIN, _freeze_document(self)),
        )


def build_d1_historical_input_authority_v0(
    *,
    kline_manifests: Sequence[D1HistoricalKlineManifestBindingV0],
    funding_manifest_relative_path: str,
    funding_manifest_sha256: str,
) -> D1HistoricalInputAuthorityV0:
    """Freeze exact externally supplied manifest identities before row access."""

    return D1HistoricalInputAuthorityV0(
        kline_manifests=tuple(kline_manifests),
        funding_manifest_relative_path=funding_manifest_relative_path,
        funding_manifest_sha256=funding_manifest_sha256,
        _factory_token=_INPUT_AUTHORITY_FACTORY_TOKEN,
    )


def canonical_d1_historical_input_authority_v0(
    authority: D1HistoricalInputAuthorityV0,
) -> bytes:
    if type(authority) is not D1HistoricalInputAuthorityV0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "authority must be exact D1HistoricalInputAuthorityV0"
        )
    expected = _hash_document(
        _INPUT_AUTHORITY_HASH_DOMAIN,
        _input_authority_document(authority, include_hash=False),
    )
    if authority.authority_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0(
            "input authority hash differs from canonical content"
        )
    return canonical_json_line(_input_authority_document(authority, include_hash=True))


def canonical_d1_historical_funding_authority_manifest_v0(
    bindings: Sequence[D1HistoricalFundingFileBindingV0],
) -> bytes:
    """Build the externally frozen funding authority without reading rate rows."""

    snapshot = tuple(bindings)
    if type(snapshot) is not tuple or any(
        type(value) is not D1HistoricalFundingFileBindingV0 for value in snapshot
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority accepts exact binding values only"
        )
    if tuple(value.symbol for value in snapshot) != D1_HISTORICAL_UNIVERSE_V0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority must contain the exact ordered D1 universe"
        )
    paths = tuple(value.relative_path for value in snapshot)
    if len(set(paths)) != len(paths):
        raise D1HistoricalDevelopmentContractErrorV0("funding authority contains duplicate paths")
    return canonical_json_line(
        {
            "files": [asdict(value) for value in snapshot],
            "historical_only": True,
            "protocol": D1_HISTORICAL_FUNDING_AUTHORITY_PROTOCOL_V0,
            "schema_version": D1_HISTORICAL_FUNDING_AUTHORITY_SCHEMA_V0,
        }
    )


def evaluate_d1_historical_prefilter_v0(
    *,
    prior_channel_bars: Sequence[D1FiveMinuteBarV0],
    current_bar: D1FiveMinuteBarV0,
) -> D1HistoricalPrefilterResultV0:
    """Apply only frozen necessary gates; the sealed D1 evaluator stays final."""

    prior = tuple(prior_channel_bars)
    invalid: list[str] = []
    if len(prior) != 24:
        invalid.append("PRIOR_CHANNEL_COUNT_NOT_24")
    if type(current_bar) is not D1FiveMinuteBarV0:
        invalid.append("CURRENT_BAR_WRONG_SEALED_TYPE")
    if any(type(value) is not D1FiveMinuteBarV0 for value in prior):
        invalid.append("PRIOR_CHANNEL_WRONG_SEALED_TYPE")
    if invalid:
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE,
            tuple(invalid),
        )
    try:
        for value in (*prior, current_bar):
            canonical_d1_five_minute_bar_v0(value)
    except ValueError:
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE,
            ("BAR_CANONICAL_INTEGRITY_FAILED",),
        )
    assert len(prior) == 24
    expected_first = current_bar.open_ms - len(prior) * _FIVE_MINUTE_MS
    if any(
        value.open_ms != expected_first + index * _FIVE_MINUTE_MS
        for index, value in enumerate(prior)
    ):
        invalid.append("PRIOR_CHANNEL_NOT_CONTIGUOUS")
    all_bars = (*prior, current_bar)
    if any(not value.is_closed for value in all_bars):
        invalid.append("PREFILTER_BAR_NOT_CLOSED")
    if any(value.data_through_ms > current_bar.close_ms for value in all_bars):
        invalid.append("PREFILTER_DATA_AFTER_SIGNAL_CLOSE")
    cutoff = current_bar.close_ms + DECISION_DELAY_MS_V2
    if any(value.receipt_ms > cutoff for value in all_bars):
        invalid.append("PREFILTER_RECEIPT_AFTER_DECISION_CUTOFF")
    if current_bar.quote_volume <= 0:
        invalid.append("ZERO_CURRENT_QUOTE_VOLUME")
    if current_bar.high_price == current_bar.low_price:
        invalid.append("ZERO_CURRENT_HIGH_LOW_RANGE")
    if invalid:
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE,
            tuple(invalid),
        )

    return _prefilter_arithmetic_v0(
        upper=max(value.high_price for value in prior),
        lower=min(value.low_price for value in prior),
        high=current_bar.high_price,
        low=current_bar.low_price,
        close=current_bar.close_price,
        quote_volume=current_bar.quote_volume,
        taker_buy_quote_volume=current_bar.taker_buy_quote_volume,
    )


def _prefilter_authenticated_candles_v0(
    *,
    prior_channel_bars: tuple[Candle, ...],
    current_bar: Candle,
) -> D1HistoricalPrefilterResultV0:
    if len(prior_channel_bars) != 24:
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE,
            ("PRIOR_CHANNEL_COUNT_NOT_24",),
        )
    if current_bar.quote_volume <= 0 or current_bar.high == current_bar.low:
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE,
            ("ZERO_PREFILTER_DENOMINATOR",),
        )
    return _prefilter_arithmetic_v0(
        upper=max(value.high for value in prior_channel_bars),
        lower=min(value.low for value in prior_channel_bars),
        high=current_bar.high,
        low=current_bar.low,
        close=current_bar.close,
        quote_volume=current_bar.quote_volume,
        taker_buy_quote_volume=current_bar.taker_buy_quote_volume,
    )


def _prefilter_arithmetic_v0(
    *,
    upper: Decimal,
    lower: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    quote_volume: Decimal,
    taker_buy_quote_volume: Decimal,
) -> D1HistoricalPrefilterResultV0:
    with localcontext(protocol_decimal_context_v2()):
        imbalance = (Decimal(2) * taker_buy_quote_volume - quote_volume) / quote_volume
        current_range = high - low
        long_location = (close - low) / current_range
        short_location = (high - close) / current_range
    if close > upper:
        failures = []
        if quote_volume < Decimal("200000"):
            failures.append("CURRENT_QUOTE_VOLUME_LT_200000")
        if imbalance < Decimal("0.20"):
            failures.append("LONG_TAKER_IMBALANCE_LT_0_20")
        if long_location < Decimal("0.75"):
            failures.append("LONG_CLOSE_LOCATION_LT_0_75")
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.CANDIDATE_LONG
            if not failures
            else D1HistoricalPrefilterStatusV0.NECESSARY_GATE_FALSE,
            ("LONG_NECESSARY_GATES_PASS",) if not failures else tuple(failures),
        )
    if close < lower:
        failures = []
        if quote_volume < Decimal("200000"):
            failures.append("CURRENT_QUOTE_VOLUME_LT_200000")
        if imbalance > Decimal("-0.20"):
            failures.append("SHORT_TAKER_IMBALANCE_GT_NEG_0_20")
        if short_location < Decimal("0.75"):
            failures.append("SHORT_CLOSE_LOCATION_LT_0_75")
        return _prefilter_result(
            D1HistoricalPrefilterStatusV0.CANDIDATE_SHORT
            if not failures
            else D1HistoricalPrefilterStatusV0.NECESSARY_GATE_FALSE,
            ("SHORT_NECESSARY_GATES_PASS",) if not failures else tuple(failures),
        )
    return _prefilter_result(
        D1HistoricalPrefilterStatusV0.NECESSARY_GATE_FALSE,
        ("NO_STRICT_PRIOR_CHANNEL_BREAKOUT",),
    )


def _prefilter_result(
    status: D1HistoricalPrefilterStatusV0,
    reasons: tuple[str, ...],
) -> D1HistoricalPrefilterResultV0:
    return D1HistoricalPrefilterResultV0(
        status=status,
        reasons=reasons,
        _factory_token=_PREFILTER_FACTORY_TOKEN,
    )


def _d1_five_minute_bar(candle: Candle) -> D1FiveMinuteBarV0:
    return build_d1_five_minute_bar_v0(
        open_ms=candle.open_time_ms,
        open_price=candle.open,
        high_price=candle.high,
        low_price=candle.low,
        close_price=candle.close,
        quote_volume=candle.quote_volume,
        taker_buy_quote_volume=candle.taker_buy_quote_volume,
        data_through_ms=candle.close_time_ms,
        receipt_ms=candle.close_time_ms,
        is_closed=candle.is_closed,
    )


def _d1_hourly_bar(candle: Candle) -> D1HourlyBarV0:
    return build_d1_hourly_bar_v0(
        open_ms=candle.open_time_ms,
        close_price=candle.close,
        data_through_ms=candle.close_time_ms,
        receipt_ms=candle.close_time_ms,
        is_closed=candle.is_closed,
    )


_KLINE_CSV_COLUMNS: Final = (
    "market",
    "symbol",
    "alias",
    "interval",
    "request_start_time_ms",
    "request_end_time_ms",
    "open_time_ms",
    "close_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "is_closed",
)


def _iter_authenticated_input_panel_v0(
    *,
    data_root: str | Path,
    authority: D1HistoricalInputAuthorityV0,
) -> Iterable[D1HistoricalReplaySymbolInputV0]:
    """Yield one authenticated symbol panel at a time to bound resident candles."""

    canonical_d1_historical_input_authority_v0(authority)
    root = _real_data_root(data_root)
    funding = load_d1_historical_authenticated_funding_v0(
        data_root=root,
        authority=authority,
    )
    funding_by_symbol = {value.symbol: value for value in funding}
    for symbol in D1_HISTORICAL_UNIVERSE_V0:
        yield _load_authenticated_symbol_data_v0(
            root=root,
            authority=authority,
            symbol=symbol,
            funding=funding_by_symbol[symbol],
        )


def _load_authenticated_input_panel_v0(
    *,
    data_root: str | Path,
    authority: D1HistoricalInputAuthorityV0,
) -> tuple[D1HistoricalReplaySymbolInputV0, ...]:
    """Compatibility helper; the public runner uses the sequential iterator."""

    return tuple(_iter_authenticated_input_panel_v0(data_root=data_root, authority=authority))


def _load_authenticated_symbol_data_v0(
    *,
    root: Path,
    authority: D1HistoricalInputAuthorityV0,
    symbol: str,
    funding: D1HistoricalAuthenticatedFundingV0,
) -> D1HistoricalReplaySymbolInputV0:
    five = load_d1_historical_authenticated_five_minute_v0(
        data_root=root,
        binding=authority.binding(symbol, "5m"),
    )
    hourly = _load_authenticated_kline_v0(
        root=root,
        binding=authority.binding(symbol, "1h"),
    )
    _validate_hourly_close_crosscheck_v0(
        symbol=symbol,
        five_minute=five.candles,
        hourly=hourly.candles,
    )
    source_root = _hash_document(
        _SOURCE_ROOT_HASH_DOMAIN,
        {
            "five_minute_data_sha256": five.data_sha256,
            "five_minute_manifest_sha256": five.manifest_sha256,
            "funding_file_sha256": funding.file_sha256,
            "hourly_data_sha256": hourly.data_sha256,
            "hourly_manifest_sha256": hourly.manifest_sha256,
            "symbol": symbol,
        },
    )
    return D1HistoricalReplaySymbolInputV0(
        symbol=symbol,
        five_minute_manifest_sha256=five.manifest_sha256,
        higher_timeframe_source_sha256=hourly.manifest_sha256,
        funding_file_sha256=funding.file_sha256,
        source_root_sha256=source_root,
        five_minute=five.candles,
        hourly=hourly.candles,
        funding=funding.points,
        exact_standard_8h_development_funding_coverage=(
            funding.exact_standard_8h_development_coverage
        ),
    )


def load_d1_historical_authenticated_five_minute_v0(
    *,
    data_root: str | Path,
    binding: D1HistoricalKlineManifestBindingV0,
) -> D1HistoricalAuthenticatedFiveMinuteV0:
    """Load one exact D1 5m file through the existing authenticated owner."""

    if type(binding) is not D1HistoricalKlineManifestBindingV0 or binding.interval != "5m":
        raise D1HistoricalDevelopmentContractErrorV0(
            "authenticated five-minute loader requires an exact 5m binding"
        )
    loaded = _load_authenticated_kline_v0(
        root=_real_data_root(data_root),
        binding=binding,
    )
    return D1HistoricalAuthenticatedFiveMinuteV0(
        symbol=loaded.symbol,
        manifest_sha256=loaded.manifest_sha256,
        data_sha256=loaded.data_sha256,
        candles=loaded.candles,
        _factory_token=_AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN,
    )


def _load_authenticated_kline_v0(
    *,
    root: Path,
    binding: D1HistoricalKlineManifestBindingV0,
) -> _LoadedKlineFileV0:
    manifest_path = _resolve_input_member(
        root,
        binding.relative_manifest_path,
        "kline manifest",
    )
    raw = _read_exact_regular_file(
        manifest_path,
        "kline manifest",
        maximum_bytes=D1_HISTORICAL_MAX_AUTHORITY_BYTES_V0,
    )
    if hashlib.sha256(raw).hexdigest() != binding.manifest_sha256:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} manifest hash differs"
        )
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise TypeError("manifest root must be an object")
        manifest = DatasetManifest(**payload)
    except (json.JSONDecodeError, UnicodeError, TypeError) as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} manifest is invalid"
        ) from error
    if manifest.schema_version != 2:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} manifest schema is invalid"
        )
    expected_raw = (
        json.dumps(
            asdict(manifest),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    if raw != expected_raw:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} manifest is not canonical"
        )
    _validate_kline_manifest_contract_v0(manifest, binding=binding)
    if (
        not isinstance(manifest.data_file, str)
        or not manifest.data_file
        or Path(manifest.data_file).name != manifest.data_file
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "kline manifest data_file must be a simple filename"
        )
    data_path = _resolve_existing_path(
        root,
        manifest_path.parent / manifest.data_file,
        "kline data file",
    )
    _require_sha256(manifest.sha256, "kline manifest data sha256")
    maximum_rows = (
        D1_HISTORICAL_MAX_FIVE_MINUTE_ROWS_V0
        if binding.interval == "5m"
        else D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0
    )
    maximum_decompressed_bytes = (
        D1_HISTORICAL_MAX_FIVE_MINUTE_DECOMPRESSED_BYTES_V0
        if binding.interval == "5m"
        else D1_HISTORICAL_MAX_HOURLY_DECOMPRESSED_BYTES_V0
    )
    selected: list[Candle] = []
    first_open_time_ms: int | None = None
    last_close_time_ms: int | None = None
    previous_open_time_ms: int | None = None
    step = _FIVE_MINUTE_MS if binding.interval == "5m" else _HOUR_MS

    def consume_row(row: dict[str, str], line_number: int) -> None:
        nonlocal first_open_time_ms, last_close_time_ms, previous_open_time_ms
        try:
            market = Market(row["market"])
            symbol = row["symbol"]
            alias = row["alias"]
            interval = row["interval"]
            request_start = int(row["request_start_time_ms"])
            request_end = int(row["request_end_time_ms"])
            open_time = int(row["open_time_ms"])
            close_time = int(row["close_time_ms"])
            if row["is_closed"] != "true":
                raise ValueError("candle is not closed")
            candle = Candle(
                market=market,
                symbol=symbol,
                interval=interval,
                open_time_ms=open_time,
                close_time_ms=close_time,
                open=_parse_exact_decimal(row["open"], "candle open"),
                high=_parse_exact_decimal(row["high"], "candle high"),
                low=_parse_exact_decimal(row["low"], "candle low"),
                close=_parse_exact_decimal(row["close"], "candle close"),
                volume=_parse_exact_decimal(row["volume"], "candle volume"),
                quote_volume=_parse_exact_decimal(row["quote_volume"], "candle quote volume"),
                trade_count=int(row["trade_count"]),
                taker_buy_base_volume=_parse_exact_decimal(
                    row["taker_buy_base_volume"], "candle taker-buy base volume"
                ),
                taker_buy_quote_volume=_parse_exact_decimal(
                    row["taker_buy_quote_volume"], "candle taker-buy quote volume"
                ),
                is_closed=True,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"invalid kline row {line_number} for {binding.symbol} {binding.interval}"
            ) from error
        if (
            market is not Market.FUTURES
            or symbol != binding.symbol
            or alias != D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[binding.symbol]
            or interval != binding.interval
            or request_start != manifest.request_start_time_ms
            or request_end != manifest.request_end_time_ms
            or not _kline_row_is_within_declared_request_v0(
                open_time_ms=open_time,
                close_time_ms=close_time,
                request_start_time_ms=request_start,
                request_end_time_ms=request_end,
            )
            or close_time != open_time + step - 1
            or (previous_open_time_ms is not None and open_time != previous_open_time_ms + step)
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                f"kline row {line_number} violates the frozen metadata/continuity contract"
            )
        if first_open_time_ms is None:
            first_open_time_ms = open_time
        previous_open_time_ms = open_time
        last_close_time_ms = close_time
        if D1_HISTORICAL_DATA_START_MS_V0 <= open_time < D1_HISTORICAL_DEVELOPMENT_END_MS_V0:
            selected.append(candle)

    actual_data_sha256, row_count = _stream_authenticated_gzip_csv_v0(
        path=data_path,
        expected_sha256=manifest.sha256,
        expected_columns=_KLINE_CSV_COLUMNS,
        maximum_rows=maximum_rows,
        maximum_decompressed_bytes=maximum_decompressed_bytes,
        label=f"{binding.symbol} {binding.interval} kline data",
        consume_row=consume_row,
    )
    actual_metadata = (
        Market.FUTURES.value,
        binding.symbol,
        D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[binding.symbol],
        binding.interval,
        manifest.request_start_time_ms,
        manifest.request_end_time_ms,
        row_count,
        first_open_time_ms,
        last_close_time_ms,
        0,
        0,
    )
    manifest_metadata = (
        manifest.market,
        manifest.symbol,
        manifest.alias,
        manifest.interval,
        manifest.request_start_time_ms,
        manifest.request_end_time_ms,
        manifest.row_count,
        manifest.first_open_time_ms,
        manifest.last_close_time_ms,
        manifest.gap_count,
        manifest.missing_intervals,
    )
    if actual_metadata != manifest_metadata:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} data contradicts its manifest"
        )
    selected_snapshot = tuple(selected)
    expected_count = (
        D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0
        if binding.interval == "5m"
        else D1_HISTORICAL_HOURLY_ROW_COUNT_V0
    )
    step = _FIVE_MINUTE_MS if binding.interval == "5m" else _HOUR_MS
    if (
        len(selected_snapshot) != expected_count
        or selected_snapshot[0].open_time_ms != D1_HISTORICAL_DATA_START_MS_V0
        or selected_snapshot[-1].close_time_ms != D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
        or any(
            value.open_time_ms != D1_HISTORICAL_DATA_START_MS_V0 + index * step
            for index, value in enumerate(selected_snapshot)
        )
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} authenticated subset is incomplete"
        )
    return _LoadedKlineFileV0(
        symbol=binding.symbol,
        interval=binding.interval,
        manifest_sha256=binding.manifest_sha256,
        data_sha256=actual_data_sha256,
        candles=selected_snapshot,
    )


def _kline_row_is_within_declared_request_v0(
    *,
    open_time_ms: int,
    close_time_ms: int,
    request_start_time_ms: int,
    request_end_time_ms: int,
) -> bool:
    return bool(open_time_ms >= request_start_time_ms and close_time_ms <= request_end_time_ms)


def _validate_kline_manifest_contract_v0(
    manifest: DatasetManifest,
    *,
    binding: D1HistoricalKlineManifestBindingV0,
) -> None:
    common_exact = (
        manifest.schema_version == 2
        and manifest.market == Market.FUTURES.value
        and manifest.symbol == binding.symbol
        and manifest.interval == binding.interval
        and manifest.alias == D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[binding.symbol]
        and manifest.request_end_time_ms == D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
        and manifest.last_close_time_ms == D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
        and type(manifest.gap_count) is int
        and type(manifest.missing_intervals) is int
        and manifest.gap_count == 0
        and manifest.missing_intervals == 0
    )
    if binding.interval == "5m":
        interval_exact = (
            manifest.request_start_time_ms == D1_HISTORICAL_DATA_START_MS_V0
            and manifest.row_count == D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0
            and manifest.first_open_time_ms == D1_HISTORICAL_DATA_START_MS_V0
        )
    else:
        # The authenticated 1h sources predate March 2024 by varying amounts;
        # only the exact March-2024..June-2026 subset is outcome-readable here.
        interval_exact = (
            manifest.request_start_time_ms <= D1_HISTORICAL_DATA_START_MS_V0
            and manifest.first_open_time_ms <= D1_HISTORICAL_DATA_START_MS_V0
            and manifest.row_count >= D1_HISTORICAL_HOURLY_ROW_COUNT_V0
            and manifest.row_count <= D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0
        )
    if not common_exact or not interval_exact:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{binding.symbol} {binding.interval} manifest violates the frozen range"
        )


def _validate_hourly_close_crosscheck_v0(
    *,
    symbol: str,
    five_minute: tuple[Candle, ...],
    hourly: tuple[Candle, ...],
) -> None:
    _require_symbol(symbol)
    if (
        len(five_minute) != D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0
        or len(hourly) != D1_HISTORICAL_HOURLY_ROW_COUNT_V0
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "hourly cross-check requires the exact authenticated subsets"
        )
    _validate_hourly_close_rows_v0(
        symbol=symbol,
        five_minute=five_minute,
        hourly=hourly,
    )


def _validate_hourly_close_rows_v0(
    *,
    symbol: str,
    five_minute: tuple[Candle, ...],
    hourly: tuple[Candle, ...],
) -> None:
    """Pure exact 12:1 close check; the file boundary owns full-panel counts."""

    _require_symbol(symbol)
    if not hourly or len(five_minute) != len(hourly) * 12:
        raise D1HistoricalDevelopmentContractErrorV0(
            "hourly cross-check rows must form a nonempty exact 12:1 panel"
        )
    for index, hourly_bar in enumerate(hourly):
        closing_five = five_minute[index * 12 + 11]
        if (
            hourly_bar.symbol != symbol
            or closing_five.symbol != symbol
            or hourly_bar.open_time_ms != closing_five.open_time_ms - 11 * _FIVE_MINUTE_MS
            or hourly_bar.close_time_ms != closing_five.close_time_ms
            or hourly_bar.close != closing_five.close
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                f"{symbol} 1h close does not equal the final corresponding 5m close"
            )


def load_d1_historical_authenticated_funding_v0(
    *,
    data_root: str | Path,
    authority: D1HistoricalInputAuthorityV0,
) -> tuple[D1HistoricalAuthenticatedFundingV0, ...]:
    """Load the exact ordered D1 funding panel through its manifest authority."""

    canonical_d1_historical_input_authority_v0(authority)
    root = _real_data_root(data_root)
    funding_files = _read_authenticated_funding_bindings_v0(
        root=root,
        funding_manifest_relative_path=authority.funding_manifest_relative_path,
        funding_manifest_sha256=authority.funding_manifest_sha256,
    )
    return load_d1_historical_authenticated_funding_bindings_v0(
        data_root=root,
        funding_manifest_relative_path=authority.funding_manifest_relative_path,
        funding_manifest_sha256=authority.funding_manifest_sha256,
        funding_files=funding_files,
    )


def load_d1_historical_authenticated_funding_bindings_v0(
    *,
    data_root: str | Path,
    funding_manifest_relative_path: str,
    funding_manifest_sha256: str,
    funding_files: Sequence[D1HistoricalFundingFileBindingV0],
) -> tuple[D1HistoricalAuthenticatedFundingV0, ...]:
    """Authenticate a supplied funding authority and load its exact files.

    Unlike the D1-authority wrapper, this boundary needs no kline authority and
    therefore does not require a caller to fabricate forbidden native-1h pins.
    """

    _require_relative_path(
        funding_manifest_relative_path,
        "funding_manifest_relative_path",
    )
    _require_sha256(funding_manifest_sha256, "funding_manifest_sha256")
    bindings = tuple(funding_files)
    expected = canonical_d1_historical_funding_authority_manifest_v0(bindings)
    root = _real_data_root(data_root)
    manifest_path = _resolve_input_member(
        root,
        funding_manifest_relative_path,
        "funding authority manifest",
    )
    raw = _read_exact_regular_file(
        manifest_path,
        "funding authority manifest",
        maximum_bytes=D1_HISTORICAL_MAX_AUTHORITY_BYTES_V0,
    )
    if hashlib.sha256(raw).hexdigest() != funding_manifest_sha256:
        raise D1HistoricalDevelopmentContractErrorV0("funding authority manifest hash differs")
    if raw != expected:
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority manifest is not canonical or differs from supplied bindings"
        )
    return tuple(
        _load_authenticated_funding_file_v0(root=root, binding=binding)
        for binding in bindings
    )


def _read_authenticated_funding_bindings_v0(
    *,
    root: Path,
    funding_manifest_relative_path: str,
    funding_manifest_sha256: str,
) -> tuple[D1HistoricalFundingFileBindingV0, ...]:
    """Discover D1's bindings before the generalized loader reauthenticates them."""

    manifest_path = _resolve_input_member(
        root,
        funding_manifest_relative_path,
        "funding authority manifest",
    )
    raw = _read_exact_regular_file(
        manifest_path,
        "funding authority manifest",
        maximum_bytes=D1_HISTORICAL_MAX_AUTHORITY_BYTES_V0,
    )
    if hashlib.sha256(raw).hexdigest() != funding_manifest_sha256:
        raise D1HistoricalDevelopmentContractErrorV0("funding authority manifest hash differs")
    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority manifest is not valid UTF-8 JSON"
        ) from error
    if not isinstance(decoded, dict) or set(decoded) != {
        "files",
        "historical_only",
        "protocol",
        "schema_version",
    }:
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority manifest fields are not exact"
        )
    files = decoded.get("files")
    if (
        decoded.get("historical_only") is not True
        or decoded.get("protocol") != D1_HISTORICAL_FUNDING_AUTHORITY_PROTOCOL_V0
        or decoded.get("schema_version") != D1_HISTORICAL_FUNDING_AUTHORITY_SCHEMA_V0
        or not isinstance(files, list)
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "funding authority protocol/schema/role is unsupported"
        )
    bindings: list[D1HistoricalFundingFileBindingV0] = []
    for item in files:
        if not isinstance(item, dict) or set(item) != {
            "relative_path",
            "sha256",
            "symbol",
        }:
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding authority file row fields are not exact"
            )
        try:
            bindings.append(
                D1HistoricalFundingFileBindingV0(
                    symbol=cast(str, item["symbol"]),
                    relative_path=cast(str, item["relative_path"]),
                    sha256=cast(str, item["sha256"]),
                )
            )
        except (KeyError, TypeError) as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                "funding authority file row is invalid"
            ) from error
    expected = canonical_d1_historical_funding_authority_manifest_v0(bindings)
    if raw != expected:
        raise D1HistoricalDevelopmentContractErrorV0("funding authority manifest is not canonical")
    return tuple(bindings)


_FUNDING_CSV_COLUMNS: Final = (
    "symbol",
    "start_time_ms",
    "end_time_ms",
    "funding_time_ms",
    "rate",
    "mark_price",
)


def _load_authenticated_funding_file_v0(
    *,
    root: Path,
    binding: D1HistoricalFundingFileBindingV0,
) -> D1HistoricalAuthenticatedFundingV0:
    source = _resolve_input_member(root, binding.relative_path, "funding data file")
    points: list[D1HistoricalFundingPointV0] = []
    previous_time: int | None = None

    def consume_row(row: dict[str, str], line_number: int) -> None:
        nonlocal previous_time
        try:
            symbol = row["symbol"]
            start = int(row["start_time_ms"])
            end = int(row["end_time_ms"])
            funding_time = int(row["funding_time_ms"])
            rate = _parse_exact_decimal(row["rate"], "funding rate")
            raw_mark = row["mark_price"]
            mark = None if raw_mark == "" else _parse_exact_decimal(raw_mark, "funding mark price")
        except (KeyError, TypeError, ValueError) as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"invalid funding row {line_number} for {binding.symbol}"
            ) from error
        if (
            symbol != binding.symbol
            or start != D1_HISTORICAL_DATA_START_MS_V0
            or end != D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
            or not start <= funding_time <= end
            or (previous_time is not None and funding_time <= previous_time)
            or (mark is not None and mark <= 0)
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                f"funding row {line_number} violates the frozen contract"
            )
        try:
            points.append(
                build_d1_historical_funding_point_v0(
                    funding_time_ms=funding_time,
                    rate=rate,
                    mark_price=mark,
                )
            )
        except D1HistoricalMathErrorV0 as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"funding row {line_number} is invalid"
            ) from error
        previous_time = funding_time

    actual_sha256, _row_count = _stream_authenticated_gzip_csv_v0(
        path=source,
        expected_sha256=binding.sha256,
        expected_columns=_FUNDING_CSV_COLUMNS,
        maximum_rows=D1_HISTORICAL_MAX_FUNDING_ROWS_V0,
        maximum_decompressed_bytes=D1_HISTORICAL_MAX_FUNDING_DECOMPRESSED_BYTES_V0,
        label=f"{binding.symbol} funding data",
        consume_row=consume_row,
    )
    return D1HistoricalAuthenticatedFundingV0(
        symbol=binding.symbol,
        file_sha256=actual_sha256,
        start_time_ms=D1_HISTORICAL_DATA_START_MS_V0,
        end_time_ms=D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
        points=tuple(points),
        exact_standard_8h_development_coverage=(
            _has_exact_standard_8h_development_funding_coverage_v0(tuple(points))
        ),
        _factory_token=_AUTHENTICATED_FUNDING_FACTORY_TOKEN,
    )


def _has_exact_standard_8h_development_funding_coverage_v0(
    points: tuple[D1HistoricalFundingPointV0, ...],
) -> bool:
    """Conservatively prove the standard UTC 8h grid over the outcome interval."""

    observed = tuple(
        value.funding_time_ms
        for value in points
        if D1_HISTORICAL_DEVELOPMENT_START_MS_V0
        <= value.funding_time_ms
        < D1_HISTORICAL_DEVELOPMENT_END_MS_V0
    )
    first_required = (
        (D1_HISTORICAL_DEVELOPMENT_START_MS_V0 + _STANDARD_FUNDING_INTERVAL_MS - 1)
        // _STANDARD_FUNDING_INTERVAL_MS
    ) * _STANDARD_FUNDING_INTERVAL_MS
    last_required = (
        (D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1) // _STANDARD_FUNDING_INTERVAL_MS
    ) * _STANDARD_FUNDING_INTERVAL_MS
    if first_required > last_required:
        return False
    expected_count = (last_required - first_required) // _STANDARD_FUNDING_INTERVAL_MS + 1
    return len(observed) == expected_count and all(
        value == first_required + index * _STANDARD_FUNDING_INTERVAL_MS
        for index, value in enumerate(observed)
    )


def _real_data_root(value: str | Path) -> Path:
    candidate = Path(value)
    try:
        metadata = candidate.stat(follow_symlinks=False)
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "data_root is missing or unreadable"
        ) from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalDevelopmentContractErrorV0(
            "data_root must be a real non-symlink directory"
        )
    return resolved


def _resolve_input_member(root: Path, relative: str, label: str) -> Path:
    _require_relative_path(relative, label)
    return _resolve_existing_path(root, root.joinpath(*relative.split("/")), label)


def _resolve_existing_path(root: Path, candidate: Path, label: str) -> Path:
    absolute = Path(os.path.abspath(candidate))
    try:
        absolute.relative_to(root)
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} escapes data_root") from error
    current = root
    for part in absolute.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise D1HistoricalDevelopmentContractErrorV0(
                f"{label} contains a symlink path component"
            )
    return absolute


class _HashingBinaryReaderV0:
    """Hash every compressed byte consumed from one already-open descriptor."""

    def __init__(self, source: BinaryIO) -> None:
        self._source = source
        self._digest = hashlib.sha256()
        self.name = getattr(source, "name", "")

    def read(self, size: int = -1) -> bytes:
        chunk = self._source.read(size)
        self._digest.update(chunk)
        return chunk

    @property
    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _file_identity_v0(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _open_stable_regular_binary_v0(
    path: Path,
    label: str,
) -> tuple[BinaryIO, os.stat_result]:
    try:
        before = path.stat(follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise D1HistoricalDevelopmentContractErrorV0(
                f"{label} must be a regular non-symlink file"
            )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except D1HistoricalDevelopmentContractErrorV0:
        raise
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} is missing or unreadable") from error
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _file_identity_v0(before) != _file_identity_v0(
            opened
        ):
            raise D1HistoricalDevelopmentContractErrorV0(f"{label} identity changed while opening")
        return os.fdopen(descriptor, "rb", buffering=0), opened
    except Exception:
        os.close(descriptor)
        raise


def _verify_open_file_stability_v0(
    *,
    path: Path,
    handle: BinaryIO,
    opened: os.stat_result,
    label: str,
) -> None:
    try:
        descriptor_after = os.fstat(handle.fileno())
        path_after = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} identity cannot be revalidated"
        ) from error
    expected = _file_identity_v0(opened)
    if (
        stat.S_ISLNK(path_after.st_mode)
        or not stat.S_ISREG(path_after.st_mode)
        or _file_identity_v0(descriptor_after) != expected
        or _file_identity_v0(path_after) != expected
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} identity or size changed during read"
        )


def _read_exact_regular_file(
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
) -> bytes:
    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "regular-file byte cap must be a positive integer"
        )
    handle, opened = _open_stable_regular_binary_v0(path, label)
    try:
        if opened.st_size > maximum_bytes:
            raise D1HistoricalDevelopmentContractErrorV0(f"{label} exceeds its frozen byte cap")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = handle.read(min(_BINARY_READ_CHUNK_BYTES, maximum_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise D1HistoricalDevelopmentContractErrorV0(f"{label} exceeds its frozen byte cap")
            chunks.append(chunk)
        _verify_open_file_stability_v0(
            path=path,
            handle=handle,
            opened=opened,
            label=label,
        )
        if total != opened.st_size:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"{label} bytes read differ from its opened size"
            )
    finally:
        handle.close()
    return b"".join(chunks)


def _stream_authenticated_gzip_csv_v0(
    *,
    path: Path,
    expected_sha256: str,
    expected_columns: tuple[str, ...],
    maximum_rows: int,
    maximum_decompressed_bytes: int,
    label: str,
    consume_row: Callable[[dict[str, str], int], None],
) -> tuple[str, int]:
    """Hash and stream-parse one gzip CSV from the same stable descriptor."""

    _require_sha256(expected_sha256, f"{label} expected sha256")
    if (
        type(maximum_rows) is not int
        or maximum_rows <= 0
        or type(maximum_decompressed_bytes) is not int
        or maximum_decompressed_bytes <= 0
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "gzip CSV row and decompressed-byte caps must be positive integers"
        )
    if (
        type(expected_columns) is not tuple
        or not expected_columns
        or len(set(expected_columns)) != len(expected_columns)
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "gzip CSV expected columns must be an exact unique tuple"
        )
    handle, opened = _open_stable_regular_binary_v0(path, label)
    hashing_reader = _HashingBinaryReaderV0(handle)
    row_count = 0
    decompressed_bytes = 0
    try:
        with gzip.GzipFile(
            fileobj=cast(BinaryIO, hashing_reader),
            mode="rb",
        ) as compressed:
            line_number = 0
            while True:
                remaining = maximum_decompressed_bytes - decompressed_bytes
                read_limit = min(_MAX_CSV_LINE_BYTES + 1, remaining + 1)
                raw_line = compressed.readline(read_limit)
                if not raw_line:
                    break
                if len(raw_line) > remaining:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} exceeds its frozen decompressed-byte cap"
                    )
                if len(raw_line) > _MAX_CSV_LINE_BYTES:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} contains a CSV line above the frozen line cap"
                    )
                decompressed_bytes += len(raw_line)
                line_number += 1
                if not raw_line.endswith(b"\n"):
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} CSV line {line_number} is not newline terminated"
                    )
                try:
                    decoded_line = raw_line.decode("utf-8")
                    parsed = next(csv.reader((decoded_line,), strict=True))
                except (UnicodeError, csv.Error, StopIteration) as error:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} CSV line {line_number} is invalid"
                    ) from error
                if line_number == 1:
                    if tuple(parsed) != expected_columns:
                        raise D1HistoricalDevelopmentContractErrorV0(
                            f"{label} CSV header does not match the exact schema"
                        )
                    continue
                if len(parsed) != len(expected_columns):
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} CSV row {line_number} has extra or missing cells"
                    )
                row_count += 1
                if row_count > maximum_rows:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        f"{label} exceeds its frozen row cap"
                    )
                consume_row(dict(zip(expected_columns, parsed, strict=True)), line_number)
        while hashing_reader.read(_BINARY_READ_CHUNK_BYTES):
            pass
        _verify_open_file_stability_v0(
            path=path,
            handle=handle,
            opened=opened,
            label=label,
        )
        actual_sha256 = hashing_reader.hexdigest
    except D1HistoricalDevelopmentContractErrorV0:
        raise
    except (OSError, EOFError, gzip.BadGzipFile) as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} is not a readable gzip CSV"
        ) from error
    finally:
        handle.close()
    if row_count == 0:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} contains no data rows")
    if actual_sha256 != expected_sha256:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} data hash differs")
    return actual_sha256, row_count


def _parse_exact_decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, str) or not value or value != value.strip():
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be nonempty canonical text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} is not an exact Decimal") from error
    if not parsed.is_finite():
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be finite")
    return parsed


def run_d1_historical_replay_core_v0(
    *,
    symbol_inputs: Iterable[D1HistoricalReplaySymbolInputV0],
    run_id: str,
    decision_start_ms: int,
    decision_end_ms: int,
) -> D1HistoricalReplayCoreResultV0:
    """Replay D1 over one already validated input per exact universe symbol."""

    _require_identity(run_id, "run_id")
    _require_nonnegative_int(decision_start_ms, "decision_start_ms")
    _require_nonnegative_int(decision_end_ms, "decision_end_ms")
    if decision_end_ms <= decision_start_ms:
        raise D1HistoricalDevelopmentContractErrorV0(
            "replay decision interval must be nonempty"
        )

    iterator = iter(symbol_inputs)
    symbol_results_list: list[_SymbolRunResultV0] = []
    for expected_symbol in D1_HISTORICAL_UNIVERSE_V0:
        try:
            value = next(iterator)
        except StopIteration as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay inputs must contain the exact ordered D1 universe"
            ) from error
        if (
            type(value) is not D1HistoricalReplaySymbolInputV0
            or value.symbol != expected_symbol
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "replay inputs must contain the exact ordered D1 universe"
            )
        symbol_results_list.append(
            _run_symbol_development_v0(
                data=value,
                run_id=run_id,
                decision_start_ms=decision_start_ms,
                decision_end_ms=decision_end_ms,
            )
        )
        # Release ~265k candles before advancing a streaming input iterator.
        del value
    try:
        next(iterator)
    except StopIteration:
        pass
    else:
        raise D1HistoricalDevelopmentContractErrorV0(
            "replay inputs must contain the exact ordered D1 universe"
        )

    symbol_results = tuple(symbol_results_list)
    episodes = tuple(episode for result in symbol_results for episode in result.episodes)
    censors = tuple(censor for result in symbol_results for censor in result.censors)
    if len(episodes) > D1_HISTORICAL_MAX_EPISODES_V0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "development episode artifact exceeds its frozen bound"
        )
    if len(censors) > D1_HISTORICAL_MAX_CENSORS_V0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "development censor artifact exceeds its frozen bound"
        )
    counters = _combine_run_counters(tuple(value.counters for value in symbol_results))
    summary = _summarize_development_v0(
        episodes=episodes,
        censors=censors,
        counters=counters,
        funding_coverage_status_by_symbol=tuple(
            (
                value.symbol,
                (
                    D1HistoricalFundingCoverageStatusV0.EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE.value
                    if value.exact_standard_8h_development_funding_coverage
                    else D1HistoricalFundingCoverageStatusV0.FUNDING_COVERAGE_UNAVAILABLE.value
                ),
            )
            for value in symbol_results
        ),
    )
    return D1HistoricalReplayCoreResultV0(
        episodes=episodes,
        censors=censors,
        summary=summary,
        _factory_token=_REPLAY_CORE_RESULT_FACTORY_TOKEN,
    )


def run_d1_historical_development_v0(
    *,
    data_root: str | Path,
    input_authority: D1HistoricalInputAuthorityV0,
    code_freeze: D1HistoricalDevelopmentFreezeV0,
    run_id: str,
    run_started_at_ms: int,
) -> D1HistoricalDevelopmentResultV0:
    """Run the frozen outcome-blind development proxy over the exact panel."""

    _require_identity(run_id, "run_id")
    _require_nonnegative_int(run_started_at_ms, "run_started_at_ms")
    canonical_d1_historical_input_authority_v0(input_authority)
    canonical_d1_historical_development_freeze_v0(code_freeze)
    if code_freeze.input_authority_sha256 != input_authority.authority_sha256:
        raise D1HistoricalDevelopmentContractErrorV0(
            "code freeze is bound to a different input authority"
        )
    if code_freeze.manifest_created_at_ms > run_started_at_ms:
        raise D1HistoricalDevelopmentContractErrorV0("run cannot precede its code freeze")
    core = run_d1_historical_replay_core_v0(
        symbol_inputs=_iter_authenticated_input_panel_v0(
            data_root=data_root,
            authority=input_authority,
        ),
        run_id=run_id,
        decision_start_ms=D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
        decision_end_ms=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    )
    result = D1HistoricalDevelopmentResultV0(
        run_id=run_id,
        run_started_at_ms=run_started_at_ms,
        input_authority_sha256=input_authority.authority_sha256,
        code_freeze_receipt_sha256=code_freeze.receipt_sha256,
        code_freeze_manifest_sha256=code_freeze.manifest_sha256,
        preregistration_sha256=code_freeze.preregistration_sha256,
        episodes=core.episodes,
        censors=core.censors,
        summary=core.summary,
        _factory_token=_RESULT_FACTORY_TOKEN,
    )
    canonical_d1_historical_development_result_v0(result)
    return result


@dataclass(slots=True)
class _ArtifactBudgetV0:
    maximum_bytes: int
    consumed_bytes: int = 0

    def consume(self, size: int) -> None:
        _require_nonnegative_int(size, "artifact chunk size")
        if self.consumed_bytes + size > self.maximum_bytes:
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical artifact publication exceeds the byte cap"
            )
        self.consumed_bytes += size


def write_d1_historical_development_artifacts_v0(
    *,
    result: D1HistoricalDevelopmentResultV0,
    input_authority: D1HistoricalInputAuthorityV0,
    code_freeze: D1HistoricalDevelopmentFreezeV0,
    output_dir: str | Path,
    maximum_total_bytes: int = D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
) -> D1HistoricalDevelopmentArtifactsV0:
    """Atomically publish one fresh, bounded, deterministic result directory."""

    if (
        type(maximum_total_bytes) is not int
        or maximum_total_bytes <= 0
        or maximum_total_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "artifact byte cap must be positive and no larger than the frozen cap"
        )
    canonical_d1_historical_development_result_v0(result)
    authority_raw = canonical_d1_historical_input_authority_v0(input_authority)
    freeze_raw = canonical_d1_historical_development_freeze_v0(code_freeze)
    if (
        result.input_authority_sha256 != input_authority.authority_sha256
        or result.code_freeze_receipt_sha256 != code_freeze.receipt_sha256
        or result.code_freeze_manifest_sha256 != code_freeze.manifest_sha256
        or result.preregistration_sha256 != code_freeze.preregistration_sha256
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "result, input authority, and code freeze bindings differ"
        )
    target = _fresh_artifact_target(output_dir)
    budget = _ArtifactBudgetV0(maximum_bytes=maximum_total_bytes)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
    output_metadata: dict[str, tuple[str, int]] = {}
    try:
        output_metadata["input-authority.jsonl"] = _write_bounded_artifact_file(
            staging / "input-authority.jsonl",
            (authority_raw,),
            budget=budget,
        )
        output_metadata["code-freeze-receipt.jsonl"] = _write_bounded_artifact_file(
            staging / "code-freeze-receipt.jsonl",
            (freeze_raw,),
            budget=budget,
        )
        output_metadata["episodes.jsonl"] = _write_bounded_artifact_file(
            staging / "episodes.jsonl",
            (canonical_d1_historical_episode_v0(value) for value in result.episodes),
            budget=budget,
        )
        output_metadata["censors.jsonl"] = _write_bounded_artifact_file(
            staging / "censors.jsonl",
            (canonical_d1_historical_censor_v0(value) for value in result.censors),
            budget=budget,
        )
        output_metadata["summary.jsonl"] = _write_bounded_artifact_file(
            staging / "summary.jsonl",
            (canonical_d1_historical_summary_v0(result.summary),),
            budget=budget,
        )
        output_metadata["result-index.jsonl"] = _write_bounded_artifact_file(
            staging / "result-index.jsonl",
            (canonical_d1_historical_development_result_v0(result),),
            budget=budget,
        )
        output_metadata["report.md"] = _write_bounded_artifact_file(
            staging / "report.md",
            (_development_report_markdown_v0(result),),
            budget=budget,
        )
        manifest_raw = canonical_json_line(
            {
                "durability_contract": d1_historical_artifact_durability_contract_v0(),
                "efficacy_claim": False,
                "execution_conclusive": False,
                "historical_bbo_available": False,
                "input_authority_sha256": result.input_authority_sha256,
                "outputs": {
                    name: {"sha256": digest, "size_bytes": size}
                    for name, (digest, size) in sorted(output_metadata.items())
                },
                "probability_claim": False,
                "production_order_placement": False,
                "promoting": False,
                "prospective": False,
                "protocol": D1_HISTORICAL_DEVELOPMENT_RULE_V0,
                "result_sha256": result.result_sha256,
                "schema_version": 1,
                "status": D1_HISTORICAL_RESULT_STATUS_V0,
            }
        )
        output_metadata["manifest.jsonl"] = _write_bounded_artifact_file(
            staging / "manifest.jsonl",
            (manifest_raw,),
            budget=budget,
        )
        _fsync_directory_if_supported_v0(staging)
        _publish_staging_no_replace(staging=staging, target=target)
        _revalidate_published_artifacts_v0(
            target=target,
            output_metadata=output_metadata,
        )
    except D1HistoricalArtifactDurabilityErrorV0 as error:
        if target.exists() and not staging.exists():
            if str(error) == _ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0:
                raise
            raise D1HistoricalArtifactDurabilityErrorV0(
                _ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0
            ) from error
        _remove_staging_after_failure(staging)
        raise
    except D1HistoricalDevelopmentContractErrorV0 as error:
        if target.exists() and not staging.exists():
            raise D1HistoricalArtifactDurabilityErrorV0(
                _ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0
            ) from error
        _remove_staging_after_failure(staging)
        raise
    except OSError as error:
        if target.exists() and not staging.exists():
            raise D1HistoricalArtifactDurabilityErrorV0(
                _ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0
            ) from error
        _remove_staging_after_failure(staging)
        raise D1HistoricalDevelopmentContractErrorV0(
            "cannot atomically publish historical development artifacts"
        ) from error
    manifest_sha256 = output_metadata["manifest.jsonl"][0]
    return D1HistoricalDevelopmentArtifactsV0(
        output_dir=target,
        manifest_sha256=manifest_sha256,
        result_sha256=result.result_sha256,
        output_file_sha256=tuple(
            (name, digest) for name, (digest, _size) in sorted(output_metadata.items())
        ),
        total_size_bytes=budget.consumed_bytes,
        _factory_token=_ARTIFACT_FACTORY_TOKEN,
    )


def _publish_staging_no_replace(*, staging: Path, target: Path) -> None:
    """Serialize publishers with an exclusive lock and never request replacement."""

    lock = target.parent / f".{target.name}.publish.lock"
    descriptor: int | None = None
    committed = False
    failure: Exception | None = None
    try:
        descriptor = os.open(
            lock,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        if target.exists() or target.is_symlink():
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical output target appeared during publication"
            )
        _rename_directory_no_replace_v0(staging=staging, target=target)
        committed = True
        _fsync_directory_if_supported_v0(target)
    except (D1HistoricalDevelopmentContractErrorV0, OSError) as error:
        failure = error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                if failure is None:
                    failure = error
        if descriptor is not None and lock.exists():
            try:
                lock.unlink()
            except OSError as error:
                if failure is None:
                    failure = error
        if committed:
            try:
                _fsync_directory_if_supported_v0(target.parent)
            except (D1HistoricalDevelopmentContractErrorV0, OSError) as error:
                if failure is None:
                    failure = error
    if failure is None:
        return
    if committed:
        raise D1HistoricalArtifactDurabilityErrorV0(
            _ARTIFACT_PUBLICATION_AMBIGUOUS_MESSAGE_V0
        ) from failure
    if isinstance(failure, D1HistoricalDevelopmentContractErrorV0):
        raise failure
    raise D1HistoricalDevelopmentContractErrorV0(
        "cannot coordinate historical artifact publication"
    ) from failure


def _remove_staging_after_failure(staging: Path) -> None:
    if not staging.exists():
        return
    try:
        shutil.rmtree(staging)
    except OSError as cleanup_error:
        raise D1HistoricalArtifactDurabilityErrorV0(
            "artifact publication failed and staging cleanup also failed"
        ) from cleanup_error


def d1_historical_artifact_durability_contract_v0() -> str:
    if os.name == "nt":
        return D1_HISTORICAL_WINDOWS_ARTIFACT_DURABILITY_CONTRACT_V0
    if sys.platform.startswith("linux"):
        return D1_HISTORICAL_POSIX_ARTIFACT_DURABILITY_CONTRACT_V0
    raise D1HistoricalDevelopmentContractErrorV0(
        "atomic durable artifact publication is unsupported on this platform"
    )


def _rename_directory_no_replace_v0(*, staging: Path, target: Path) -> None:
    """Atomically publish a directory without replacement on supported hosts."""

    if os.name == "nt":
        try:
            os.rename(staging, target)
        except FileExistsError as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical output target appeared during publication"
            ) from error
        return
    if not sys.platform.startswith("linux"):
        raise D1HistoricalDevelopmentContractErrorV0(
            "atomic directory no-replace is unsupported on this platform"
        )
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "Linux renameat2 no-replace is unavailable"
        ) from error
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(staging),
        -100,
        os.fsencode(target),
        1,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise D1HistoricalDevelopmentContractErrorV0(
            "historical output target appeared during publication"
        )
    raise D1HistoricalDevelopmentContractErrorV0(
        f"atomic directory publication failed with errno {error_number}"
    )


def _fsync_directory_if_supported_v0(path: Path) -> None:
    """Flush one real directory under the exact host durability contract."""

    if os.name == "nt":
        _windows_flush_directory_entry_v0(path)
        return
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "cannot fsync artifact publication directory"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


class _WindowsByHandleFileInformationV0(ctypes.Structure):
    _fields_ = (
        ("file_attributes", ctypes.c_uint32),
        ("creation_time_low", ctypes.c_uint32),
        ("creation_time_high", ctypes.c_uint32),
        ("last_access_time_low", ctypes.c_uint32),
        ("last_access_time_high", ctypes.c_uint32),
        ("last_write_time_low", ctypes.c_uint32),
        ("last_write_time_high", ctypes.c_uint32),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    )


def _windows_kernel32_v0():
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise D1HistoricalArtifactDurabilityErrorV0(
            "Win32 artifact directory durability APIs are unavailable"
        )
    try:
        return loader("kernel32", use_last_error=True)
    except (OSError, TypeError) as error:
        raise D1HistoricalArtifactDurabilityErrorV0(
            "Win32 artifact directory durability APIs are unavailable"
        ) from error


def _windows_api_v0(name: str):
    try:
        return getattr(_windows_kernel32_v0(), name)
    except AttributeError as error:
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"Win32 artifact directory durability API {name} is unavailable"
        ) from error


def _windows_last_error_v0() -> int:
    getter = getattr(ctypes, "get_last_error", None)
    return 0 if getter is None else int(getter())


def _windows_open_directory_handle_v0(path: Path) -> int:
    from ctypes import wintypes

    create_file = _windows_api_v0("CreateFileW")
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    generic_write = 0x40000000
    share_read_write_delete = 0x00000001 | 0x00000002 | 0x00000004
    open_existing = 3
    backup_semantics = 0x02000000
    open_reparse_point = 0x00200000
    write_through = 0x80000000
    handle = create_file(
        os.fspath(path),
        generic_write,
        share_read_write_delete,
        None,
        open_existing,
        backup_semantics | open_reparse_point | write_through,
        None,
    )
    value = handle if isinstance(handle, int) else getattr(handle, "value", None)
    invalid = ctypes.c_void_p(-1).value
    if value is None or value == invalid:
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"CreateFileW artifact directory handle failed with error {error_number}"
        )
    return int(value)


def _windows_file_information_v0(
    handle: int,
) -> _WindowsByHandleFileInformationV0:
    from ctypes import wintypes

    get_information = _windows_api_v0("GetFileInformationByHandle")
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_WindowsByHandleFileInformationV0),
    )
    get_information.restype = wintypes.BOOL
    information = _WindowsByHandleFileInformationV0()
    if not get_information(wintypes.HANDLE(handle), ctypes.byref(information)):
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            "GetFileInformationByHandle artifact directory identity failed "
            f"with error {error_number}"
        )
    return information


def _windows_directory_handle_identity_v0(
    information: _WindowsByHandleFileInformationV0,
) -> tuple[int, int]:
    file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
    return int(information.volume_serial_number), file_index


def _require_windows_real_directory_handle_v0(
    information: _WindowsByHandleFileInformationV0,
) -> None:
    file_attribute_directory = 0x00000010
    file_attribute_reparse_point = 0x00000400
    attributes = int(information.file_attributes)
    if not attributes & file_attribute_directory or attributes & file_attribute_reparse_point:
        raise D1HistoricalArtifactDurabilityErrorV0(
            "Win32 artifact durability handle must name one real directory"
        )


def _windows_flush_directory_handle_v0(handle: int) -> None:
    from ctypes import wintypes

    flush = _windows_api_v0("FlushFileBuffers")
    flush.argtypes = (wintypes.HANDLE,)
    flush.restype = wintypes.BOOL
    if not flush(wintypes.HANDLE(handle)):
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"FlushFileBuffers artifact directory failed with error {error_number}"
        )


def _windows_close_handle_v0(handle: int) -> None:
    from ctypes import wintypes

    close = _windows_api_v0("CloseHandle")
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    if not close(wintypes.HANDLE(handle)):
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"CloseHandle artifact directory failed with error {error_number}"
        )


def _windows_local_volume_identity_v0(path: Path) -> tuple[str, int]:
    """Qualify fixed NTFS for the 64-bit directory-identity contract.

    ReFS is deliberately rejected: its 128-bit file IDs are not represented
    uniquely by ``BY_HANDLE_FILE_INFORMATION``'s 64-bit file index.
    """

    from ctypes import wintypes

    volume_path = ctypes.create_unicode_buffer(261)
    get_volume_path = _windows_api_v0("GetVolumePathNameW")
    get_volume_path.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    get_volume_path.restype = wintypes.BOOL
    if not get_volume_path(os.fspath(path), volume_path, len(volume_path)):
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"GetVolumePathNameW artifact volume lookup failed with error {error_number}"
        )
    root = volume_path.value
    get_drive_type = _windows_api_v0("GetDriveTypeW")
    get_drive_type.argtypes = (wintypes.LPCWSTR,)
    get_drive_type.restype = wintypes.UINT
    if int(get_drive_type(root)) != 3:
        raise D1HistoricalArtifactDurabilityErrorV0(
            "artifact publication requires a local fixed Windows volume"
        )

    serial = wintypes.DWORD()
    filesystem = ctypes.create_unicode_buffer(64)
    get_volume_information = _windows_api_v0("GetVolumeInformationW")
    get_volume_information.argtypes = (
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
        wintypes.DWORD,
    )
    get_volume_information.restype = wintypes.BOOL
    if not get_volume_information(
        root,
        None,
        0,
        ctypes.byref(serial),
        None,
        None,
        filesystem,
        len(filesystem),
    ):
        error_number = _windows_last_error_v0()
        raise D1HistoricalArtifactDurabilityErrorV0(
            f"GetVolumeInformationW artifact volume lookup failed with error {error_number}"
        )
    filesystem_name = filesystem.value.upper()
    if filesystem_name != "NTFS":
        raise D1HistoricalArtifactDurabilityErrorV0(
            "artifact publication requires local fixed NTFS; ReFS and other "
            "filesystems are unsupported by the 64-bit directory identity contract"
        )
    serial_value = int(serial.value)
    return (
        f"{os.path.normcase(root)}|{filesystem_name}|{serial_value:08x}",
        serial_value,
    )


def _windows_flush_directory_entry_v0(path: Path) -> None:
    before = _require_real_artifact_directory_v0(path, "Windows directory flush target")
    _volume_identity, expected_volume_serial = _windows_local_volume_identity_v0(path)
    handle = _windows_open_directory_handle_v0(path)
    try:
        opened = _windows_file_information_v0(handle)
        _require_windows_real_directory_handle_v0(opened)
        opened_identity = _windows_directory_handle_identity_v0(opened)
        if opened_identity[0] != expected_volume_serial:
            raise D1HistoricalArtifactDurabilityErrorV0(
                "Win32 artifact directory handle differs from its qualified volume"
            )
        _windows_flush_directory_handle_v0(handle)
        after_flush = _windows_file_information_v0(handle)
        _require_windows_real_directory_handle_v0(after_flush)
        if _windows_directory_handle_identity_v0(after_flush) != opened_identity:
            raise D1HistoricalArtifactDurabilityErrorV0(
                "Win32 artifact directory identity changed while being flushed"
            )
        path_handle = _windows_open_directory_handle_v0(path)
        try:
            path_information = _windows_file_information_v0(path_handle)
            _require_windows_real_directory_handle_v0(path_information)
            if _windows_directory_handle_identity_v0(path_information) != opened_identity:
                raise D1HistoricalArtifactDurabilityErrorV0(
                    "Win32 artifact directory pathname identity changed during flush"
                )
        finally:
            _windows_close_handle_v0(path_handle)
        after = _require_real_artifact_directory_v0(
            path,
            "Windows directory flush target",
        )
        if _artifact_directory_identity_v0(after) != _artifact_directory_identity_v0(before):
            raise D1HistoricalArtifactDurabilityErrorV0(
                "Windows artifact directory pathname changed during flush"
            )
    finally:
        _windows_close_handle_v0(handle)


def _artifact_directory_identity_v0(metadata: os.stat_result) -> tuple[int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_mode


def _is_link_or_reparse_v0(metadata: os.stat_result) -> bool:
    if stat.S_ISLNK(metadata.st_mode):
        return True
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse_flag)


def _require_real_artifact_directory_v0(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalArtifactDurabilityErrorV0(f"{label} is unavailable") from error
    if _is_link_or_reparse_v0(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalArtifactDurabilityErrorV0(f"{label} must be a real non-reparse directory")
    return metadata


def _revalidate_published_artifacts_v0(
    *,
    target: Path,
    output_metadata: dict[str, tuple[str, int]],
) -> None:
    before = _require_real_artifact_directory_v0(target, "published artifact directory")
    try:
        with os.scandir(target) as entries:
            names = frozenset(value.name for value in entries)
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "cannot inspect published artifact directory"
        ) from error
    if names != frozenset(output_metadata):
        raise D1HistoricalDevelopmentContractErrorV0(
            "published artifact directory membership differs"
        )
    for name, (expected_sha256, expected_size) in sorted(output_metadata.items()):
        raw = _read_exact_regular_file(
            target / name,
            f"published artifact {name}",
            maximum_bytes=max(1, expected_size),
        )
        if len(raw) != expected_size or hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"published artifact {name} differs from its staged bytes"
            )
    after = _require_real_artifact_directory_v0(target, "published artifact directory")
    if _artifact_directory_identity_v0(after) != _artifact_directory_identity_v0(before):
        raise D1HistoricalDevelopmentContractErrorV0(
            "published artifact directory identity changed during final revalidation"
        )


def _fresh_artifact_target(value: str | Path) -> Path:
    target = Path(os.path.abspath(Path(value)))
    if target.exists() or target.is_symlink():
        raise D1HistoricalDevelopmentContractErrorV0(
            "historical output requires a fresh absent target directory"
        )
    current = Path(target.anchor)
    for part in target.parts[1:-1]:
        current /= part
        try:
            component = current.stat(follow_symlinks=False)
        except FileNotFoundError:
            break
        except OSError as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical output path component is unavailable"
            ) from error
        if _is_link_or_reparse_v0(component) or not stat.S_ISDIR(component.st_mode):
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical output path contains a reparse or non-directory component"
            )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "historical output parent is unavailable"
        ) from error
    current = Path(target.anchor)
    for part in target.parts[1:-1]:
        current /= part
        _require_real_artifact_directory_v0(
            current,
            "historical output path component",
        )
    if os.name == "nt":
        _windows_local_volume_identity_v0(target.parent)
    return target


def _write_bounded_artifact_file(
    path: Path,
    chunks: Iterable[bytes],
    *,
    budget: _ArtifactBudgetV0,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("xb") as handle:
            for chunk in chunks:
                if type(chunk) is not bytes:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        "artifact chunks must be exact bytes"
                    )
                budget.consume(len(chunk))
                handle.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "staging artifact file unexpectedly exists"
        ) from error
    return digest.hexdigest(), size


def canonical_d1_historical_censor_v0(value: D1HistoricalCensorV0) -> bytes:
    """Return and verify the canonical immutable D1 censor record."""

    if type(value) is not D1HistoricalCensorV0:
        raise D1HistoricalDevelopmentContractErrorV0("value must be exact D1HistoricalCensorV0")
    expected = _hash_document(
        _CENSOR_HASH_DOMAIN,
        _censor_document(value, include_hash=False),
    )
    if value.censor_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0("censor hash differs")
    return canonical_json_line(_censor_document(value, include_hash=True))


def _development_report_markdown_v0(
    result: D1HistoricalDevelopmentResultV0,
) -> bytes:
    summary = result.summary
    lines = [
        "# D1 SCEFB-5M historical development proxy",
        "",
        "> Status: `INCONCLUSIVE_NO_HISTORICAL_BBO`. This retrospective open-price proxy ",
        "> cannot establish probability, efficacy, promotion, PAPER fills, or order authority.",
        "",
        "## Run identity",
        "",
        f"- Run ID: `{result.run_id}`",
        f"- Result SHA-256: `{result.result_sha256}`",
        f"- Input authority SHA-256: `{result.input_authority_sha256}`",
        f"- Code freeze SHA-256: `{result.code_freeze_manifest_sha256}`",
        (
            f"- Development interval: `[{result.development_start_ms}, "
            f"{result.development_end_ms_exclusive})`"
        ),
        f"- Disposition: `{summary.disposition.value}`",
        "",
        "## Census",
        "",
        f"- Full sealed signals: {summary.full_signal_count}",
        f"- Entered historical proxy positions: {summary.entered_position_count}",
        f"- Completed episodes: {summary.episode_count}",
        (f"- Raw evaluable per-symbol non-overlapping episodes: {summary.evaluable_episode_count}"),
        (
            "- Global earliest-exit non-overlapping evaluable episodes: "
            f"{summary.global_nonoverlap_evaluable_count}"
        ),
        (
            "- Global scheduling boundary: accept an episode when "
            "`entry_reference_time_ms >= prior_selected_exit_reference_time_ms`"
        ),
        (
            f"- Evaluable long / short: {summary.evaluable_long_episode_count} / "
            f"{summary.evaluable_short_episode_count}"
        ),
        f"- Active UTC days: {summary.active_utc_day_count}",
        (
            "- Suppressed full signals while reserved: "
            f"{summary.pending_or_active_suppressed_signal_count}"
        ),
        f"- Entry-distance rejections: {summary.entry_distance_rejection_count}",
        f"- Right-edge censors: {summary.right_edge_censor_count}",
        "",
        "### Funding coverage input qualification",
        "",
    ]
    lines.extend(
        f"- `{symbol}`: `{status}`" for symbol, status in summary.funding_coverage_status_by_symbol
    )
    lines.extend(
        (
            "",
            "### Funding exclusions",
            "",
        )
    )
    lines.extend(f"- `{name}`: {count}" for name, count in summary.funding_inconclusive_counts)
    lines.extend(("", "### Exit reasons", ""))
    lines.extend(f"- `{name}`: {count}" for name, count in summary.exit_reason_counts)
    lines.extend(
        (
            "",
            "## Fee-cell disposition metrics",
            "",
            (
                "| Fee cell | N | Evaluable N | Total return | Mean return | PF | "
                "Positive symbols | After top 3 symbols | After top 10 episodes | "
                "PnL @100 | PnL @1000 |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for value in summary.fee_aggregates:
        lines.append(
            "| "
            + " | ".join(
                (
                    str(value.fee_multiplier),
                    str(value.episode_count),
                    str(value.evaluable_episode_count),
                    _markdown_decimal(value.total_net_return),
                    _markdown_decimal(value.mean_net_return),
                    "INF"
                    if value.profit_factor_infinite
                    else _markdown_decimal(value.profit_factor),
                    str(value.positive_symbol_count),
                    _markdown_decimal(value.net_after_top_three_symbols),
                    _markdown_decimal(value.net_after_top_ten_episodes),
                    _markdown_decimal(value.projected_total_pnl_100_usdt),
                    _markdown_decimal(value.projected_total_pnl_1000_usdt),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Deterministic breakdowns",
            "",
            (
                "All economic means below use evaluable episodes only. "
                "Nominal projections do not multiply N."
            ),
            "",
            (
                "| Fee | Group | Key | N | Eval N | Hit rate | Total | Mean | "
                "Median | PF | Gross mean | Slippage mean | Fee mean | Funding mean | "
                "PnL @100 | PnL @1000 |"
            ),
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for value in summary.breakdowns:
        lines.append(
            "| "
            + " | ".join(
                (
                    str(value.fee_multiplier),
                    value.kind.value,
                    value.key.replace("|", "\\|"),
                    str(value.episode_count),
                    str(value.evaluable_episode_count),
                    _markdown_decimal(value.strict_positive_hit_rate),
                    _markdown_decimal(value.total_net_return),
                    _markdown_decimal(value.mean_net_return),
                    _markdown_decimal(value.median_net_return),
                    "INF"
                    if value.profit_factor_infinite
                    else _markdown_decimal(value.profit_factor),
                    _markdown_decimal(value.mean_gross_return),
                    _markdown_decimal(value.mean_slippage_return),
                    _markdown_decimal(value.mean_fee_return),
                    _markdown_decimal(value.mean_funding_return),
                    _markdown_decimal(value.projected_total_pnl_100_usdt),
                    _markdown_decimal(value.projected_total_pnl_1000_usdt),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Non-claims",
            "",
            "- Historical BBO available: false",
            "- PAPER fill claim: false",
            "- Execution conclusive: false",
            "- Probability claim: false",
            "- Efficacy claim: false",
            "- Prospective: false",
            "- Promoting: false",
            "- Production order placement: false",
            "",
        )
    )
    return "\n".join(lines).encode("utf-8")


def _markdown_decimal(value: Decimal | None) -> str:
    return "-" if value is None else str(value)


def _run_symbol_development_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    run_id: str,
    decision_start_ms: int,
    decision_end_ms: int,
) -> _SymbolRunResultV0:
    if decision_end_ms <= decision_start_ms:
        raise D1HistoricalDevelopmentContractErrorV0("symbol decision interval must be nonempty")
    _require_symbol(data.symbol)
    five_open_times = tuple(value.open_time_ms for value in data.five_minute)
    hourly_close_times = tuple(value.close_time_ms for value in data.hourly)
    if any(
        current != previous + _FIVE_MINUTE_MS for previous, current in pairwise(five_open_times)
    ) or any(
        value.market is not Market.FUTURES
        or value.symbol != data.symbol
        or value.interval != "5m"
        or value.close_time_ms != value.open_time_ms + _FIVE_MINUTE_MS - 1
        or not value.is_closed
        for value in data.five_minute
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "symbol five-minute rows must be exact, closed, and contiguous"
        )
    if any(
        current != previous + _HOUR_MS
        for previous, current in pairwise(tuple(value.open_time_ms for value in data.hourly))
    ) or any(
        value.market is not Market.FUTURES
        or value.symbol != data.symbol
        or value.interval != "1h"
        or value.close_time_ms != value.open_time_ms + _HOUR_MS - 1
        or not value.is_closed
        for value in data.hourly
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "symbol hourly rows must be exact, closed, and contiguous"
        )
    if any(
        current.funding_time_ms <= previous.funding_time_ms
        for previous, current in pairwise(data.funding)
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "symbol funding rows must be strictly ordered and unique"
        )
    start_index = bisect_left(five_open_times, decision_start_ms)
    end_index = bisect_left(five_open_times, decision_end_ms)
    if (
        start_index >= len(five_open_times)
        or five_open_times[start_index] != decision_start_ms
        or end_index <= start_index
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "symbol data does not cover the exact decision interval"
        )
    episodes: list[D1HistoricalEpisodeV0] = []
    censors: list[D1HistoricalCensorV0] = []
    counters = _RunCountersV0()
    state: (
        _PendingEntryStateV0
        | _ActivePositionStateV0
        | _PendingExitStateV0
        | _RightEdgeReservedStateV0
        | None
    ) = None
    latest_release_fill_ms: int | None = None

    for index in range(start_index, end_index):
        current_candle = data.five_minute[index]

        if isinstance(state, _PendingExitStateV0):
            if state.exit_reference_index < index:
                raise D1HistoricalDevelopmentContractErrorV0("pending exit reference was skipped")
            if state.exit_reference_index == index:
                episodes.append(
                    _build_episode_v0(
                        data=data,
                        active=state.active,
                        exit_decision=state.exit_decision,
                        exit_reference_candle=current_candle,
                    )
                )
                latest_release_fill_ms = current_candle.open_time_ms
                state = None

        if isinstance(state, _PendingEntryStateV0):
            if state.entry_reference_index < index:
                raise D1HistoricalDevelopmentContractErrorV0("pending entry reference was skipped")
            if state.entry_reference_index == index:
                attempted = _attempt_historical_entry_v0(
                    data=data,
                    pending=state,
                    entry_reference_candle=current_candle,
                    counters=counters,
                )
                if attempted is None:
                    latest_release_fill_ms = current_candle.open_time_ms
                state = attempted

        decision: D1EntryDecisionV0 | None = None
        if index < 24:
            counters.invalid_input_inconclusive_count += 1
        else:
            prefilter = _prefilter_authenticated_candles_v0(
                prior_channel_bars=data.five_minute[index - 24 : index],
                current_bar=current_candle,
            )
            if prefilter.status is D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE:
                counters.invalid_input_inconclusive_count += 1
            elif prefilter.status is D1HistoricalPrefilterStatusV0.NECESSARY_GATE_FALSE:
                counters.prefilter_necessary_gate_false_count += 1
            else:
                counters.prefilter_candidate_count += 1
                decision = _evaluate_entry_at_index_v0(
                    data=data,
                    run_id=run_id,
                    index=index,
                    hourly_close_times=hourly_close_times,
                )
                if decision.status is D1EntryStatusV0.INCONCLUSIVE:
                    counters.invalid_input_inconclusive_count += 1
                elif decision.status is D1EntryStatusV0.SIGNAL:
                    counters.full_signal_count += 1
                    release_blocks_signal = (
                        latest_release_fill_ms is not None
                        and not _signal_cutoff_is_after_fill_v0(
                            fill_ms=latest_release_fill_ms,
                            cutoff_ms=decision.decision_cutoff_ms,
                        )
                    )
                    if state is not None or release_blocks_signal:
                        counters.pending_or_active_suppressed_signal_count += 1
                    else:
                        state = _reserve_entry_v0(
                            data=data,
                            decision=decision,
                            signal_index=index,
                            end_index=end_index,
                            censors=censors,
                        )

        if isinstance(state, _ActivePositionStateV0):
            exit_bars = tuple(
                _d1_five_minute_bar(value)
                for value in data.five_minute[state.entry_reference_index : index + 1]
            )
            exit_input = build_d1_exit_input_v0(
                position=state.position,
                source_root_sha256=_exit_source_root_v0(
                    data=data,
                    position=state.position,
                    bars_since_entry=exit_bars,
                ),
                bars_since_entry=exit_bars,
                required_data_available=True,
                authority_continuity_declared=True,
            )
            exit_decision = evaluate_d1_exit_v0(exit_input)
            canonical_d1_exit_decision_v0(exit_decision)
            if exit_decision.status is D1ExitStatusV0.INCONCLUSIVE_EXIT:
                raise D1HistoricalDevelopmentContractErrorV0(
                    "authenticated historical exit unexpectedly lost authority"
                )
            if exit_decision.status is D1ExitStatusV0.EXIT:
                reference_index = bisect_right(
                    five_open_times,
                    exit_decision.decision_cutoff_ms,
                )
                if reference_index >= end_index:
                    _append_censor(
                        censors,
                        symbol=data.symbol,
                        decision=state.decision,
                        stage=D1HistoricalCensorStageV0.EXIT_REFERENCE,
                        reason="DEVELOPMENT_END_BEFORE_CAUSAL_EXIT_REFERENCE_OPEN",
                    )
                    state = _RightEdgeReservedStateV0(signal_event_id=state.decision.event_id)
                else:
                    state = _PendingExitStateV0(
                        active=state,
                        exit_decision=exit_decision,
                        exit_reference_index=reference_index,
                    )

    if isinstance(state, _ActivePositionStateV0):
        _append_censor(
            censors,
            symbol=data.symbol,
            decision=state.decision,
            stage=D1HistoricalCensorStageV0.EXIT_OBSERVATION,
            reason="DEVELOPMENT_END_BEFORE_TERMINAL_EXIT_OBSERVATION",
        )
    elif isinstance(state, (_PendingEntryStateV0, _PendingExitStateV0)):
        raise D1HistoricalDevelopmentContractErrorV0(
            "right-edge pending state was not explicitly censored"
        )
    return _SymbolRunResultV0(
        symbol=data.symbol,
        exact_standard_8h_development_funding_coverage=(
            data.exact_standard_8h_development_funding_coverage
        ),
        episodes=tuple(episodes),
        censors=tuple(censors),
        counters=counters,
    )


def _signal_cutoff_is_after_fill_v0(*, fill_ms: int, cutoff_ms: int) -> bool:
    _require_nonnegative_int(fill_ms, "fill_ms")
    _require_nonnegative_int(cutoff_ms, "cutoff_ms")
    return cutoff_ms > fill_ms


def _evaluate_entry_at_index_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    run_id: str,
    index: int,
    hourly_close_times: tuple[int, ...],
) -> D1EntryDecisionV0:
    current = data.five_minute[index]
    prior = tuple(
        _d1_five_minute_bar(value)
        for value in data.five_minute[max(0, index - D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0) : index]
    )
    current_bar = _d1_five_minute_bar(current)
    # A derived hour is eligible only after its own close.  Rounding a mid-hour
    # 5m close up to the hour boundary would expose the still-forming hour when
    # the complete derived panel is already resident in memory.
    hour_end = bisect_right(hourly_close_times, current.close_time_ms)
    hourly = tuple(
        _d1_hourly_bar(value)
        for value in data.hourly[max(0, hour_end - D1_HOURLY_BAR_COUNT_V0) : hour_end]
    )
    entry_input = build_d1_entry_input_v0(
        attempt_id=f"{run_id}:{data.symbol}",
        symbol=data.symbol,
        venue=VenueV2.USDM_FUTURES,
        source_root_sha256=_entry_source_root_v0(
            data=data,
            prior_bars=prior,
            current_bar=current_bar,
            hourly_bars=hourly,
        ),
        prior_bars=prior,
        current_bar=current_bar,
        hourly_bars=hourly,
        required_fields_complete=True,
    )
    decision = evaluate_d1_entry_v0(entry_input)
    canonical_d1_entry_decision_v0(decision)
    return decision


def _entry_source_root_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    prior_bars: tuple[D1FiveMinuteBarV0, ...],
    current_bar: D1FiveMinuteBarV0,
    hourly_bars: tuple[D1HourlyBarV0, ...],
) -> str:
    """Bind decision identity to only the exact causal entry observations used."""

    if data.source_root_policy == D1_HISTORICAL_SOURCE_ROOT_POLICY_STATIC_V0:
        return data.source_root_sha256
    if data.source_root_policy != D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "replay source-root policy is unsupported"
        )
    return _hash_document(
        _USED_ROWS_ENTRY_SOURCE_ROOT_DOMAIN,
        {
            "current_bar_sha256": hashlib.sha256(
                canonical_d1_five_minute_bar_v0(current_bar)
            ).hexdigest(),
            "hourly_bars_count": len(hourly_bars),
            "hourly_bars_root_sha256": _ordered_canonical_bytes_root_v0(
                _USED_ROWS_HOURLY_SEQUENCE_DOMAIN,
                tuple(canonical_d1_hourly_bar_v0(value) for value in hourly_bars),
            ),
            "prior_bars_count": len(prior_bars),
            "prior_bars_root_sha256": _ordered_canonical_bytes_root_v0(
                _USED_ROWS_FIVE_MINUTE_SEQUENCE_DOMAIN,
                tuple(canonical_d1_five_minute_bar_v0(value) for value in prior_bars),
            ),
            "source_root_policy": D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
        },
    )


def _exit_source_root_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    position: D1PaperPositionAnchorV0,
    bars_since_entry: tuple[D1FiveMinuteBarV0, ...],
) -> str:
    """Bind exit identity to its entry event and exact observed post-entry path."""

    if data.source_root_policy == D1_HISTORICAL_SOURCE_ROOT_POLICY_STATIC_V0:
        return data.source_root_sha256
    if data.source_root_policy != D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "replay source-root policy is unsupported"
        )
    return _hash_document(
        _USED_ROWS_EXIT_SOURCE_ROOT_DOMAIN,
        {
            "bars_since_entry_count": len(bars_since_entry),
            "bars_since_entry_root_sha256": _ordered_canonical_bytes_root_v0(
                _USED_ROWS_FIVE_MINUTE_SEQUENCE_DOMAIN,
                tuple(
                    canonical_d1_five_minute_bar_v0(value)
                    for value in bars_since_entry
                ),
            ),
            "entry_event_id": position.entry_event_id,
            "source_root_policy": D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
        },
    )


def _reserve_entry_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    decision: D1EntryDecisionV0,
    signal_index: int,
    end_index: int,
    censors: list[D1HistoricalCensorV0],
) -> _PendingEntryStateV0 | _RightEdgeReservedStateV0:
    entry_reference_index = signal_index + 2
    expected_open = decision.bar_open_ms + 2 * _FIVE_MINUTE_MS
    if entry_reference_index >= end_index:
        _append_censor(
            censors,
            symbol=data.symbol,
            decision=decision,
            stage=D1HistoricalCensorStageV0.ENTRY_REFERENCE,
            reason="DEVELOPMENT_END_BEFORE_OPEN_T_PLUS_2",
        )
        return _RightEdgeReservedStateV0(signal_event_id=decision.event_id)
    if data.five_minute[entry_reference_index].open_time_ms != expected_open:
        raise D1HistoricalDevelopmentContractErrorV0("authenticated data is missing open(t+2)")
    return _PendingEntryStateV0(
        decision=decision,
        signal_index=signal_index,
        entry_reference_index=entry_reference_index,
    )


def _attempt_historical_entry_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    pending: _PendingEntryStateV0,
    entry_reference_candle: Candle,
    counters: _RunCountersV0,
) -> _ActivePositionStateV0 | None:
    decision = pending.decision
    assert decision.side is not None
    assert decision.signal_close is not None
    assert decision.frozen_atr is not None
    executable_entry = d1_historical_entry_execution_price_v0(
        side=decision.side,
        reference_price=entry_reference_candle.open,
    )
    with localcontext(protocol_decimal_context_v2()):
        admissible = (
            abs(executable_entry - decision.signal_close) <= Decimal("0.50") * decision.frozen_atr
        )
    if not admissible:
        counters.entry_distance_rejection_count += 1
        return None
    reference_sha256 = _hash_document(
        _ENTRY_REFERENCE_HASH_DOMAIN,
        {
            "entry_reference_price": str(entry_reference_candle.open),
            "entry_reference_time_ms": entry_reference_candle.open_time_ms,
            "five_minute_manifest_sha256": data.five_minute_manifest_sha256,
            "signal_event_id": decision.event_id,
            "symbol": data.symbol,
        },
    )
    position = build_d1_paper_position_anchor_v0(
        entry_decision=decision,
        entry_vwap=executable_entry,
        entry_fill_ms=entry_reference_candle.open_time_ms,
        entry_reference_kind=D1EntryReferenceKindV0.HISTORICAL_OPEN_PROXY,
        entry_vwap_source_sha256=reference_sha256,
    )
    counters.entered_position_count += 1
    return _ActivePositionStateV0(
        decision=decision,
        signal_index=pending.signal_index,
        entry_reference_index=pending.entry_reference_index,
        entry_reference_price=entry_reference_candle.open,
        position=position,
    )


def _append_censor(
    censors: list[D1HistoricalCensorV0],
    *,
    symbol: str,
    decision: D1EntryDecisionV0,
    stage: D1HistoricalCensorStageV0,
    reason: str,
) -> None:
    if len(censors) >= D1_HISTORICAL_MAX_CENSORS_V0:
        raise D1HistoricalDevelopmentContractErrorV0("censor artifact exceeds its frozen bound")
    censors.append(
        D1HistoricalCensorV0(
            symbol=symbol,
            signal_event_id=decision.event_id,
            signal_bar_open_ms=decision.bar_open_ms,
            stage=stage,
            reason=reason,
            _factory_token=_CENSOR_FACTORY_TOKEN,
        )
    )


def _build_episode_v0(
    *,
    data: D1HistoricalReplaySymbolInputV0,
    active: _ActivePositionStateV0,
    exit_decision: D1ExitDecisionV0,
    exit_reference_candle: Candle,
) -> D1HistoricalEpisodeV0:
    decision = active.decision
    assert decision.side is not None
    canonical_d1_entry_decision_v0(decision)
    canonical_d1_exit_decision_v0(exit_decision)
    entry_time = data.five_minute[active.entry_reference_index].open_time_ms
    exit_time = exit_reference_candle.open_time_ms
    funding_times = tuple(value.funding_time_ms for value in data.funding)
    relevant = data.funding[
        bisect_left(funding_times, entry_time) : bisect_right(funding_times, exit_time)
    ]
    interior = tuple(value for value in relevant if entry_time < value.funding_time_ms < exit_time)
    if not data.exact_standard_8h_development_funding_coverage:
        inconclusive_reason = D1HistoricalFundingInconclusiveReasonV0.FUNDING_COVERAGE_UNAVAILABLE
    elif any(value.funding_time_ms in {entry_time, exit_time} for value in relevant):
        inconclusive_reason = D1HistoricalFundingInconclusiveReasonV0.FUNDING_ENDPOINT_EQUALITY
    elif any(value.mark_price is None for value in interior):
        inconclusive_reason = D1HistoricalFundingInconclusiveReasonV0.MISSING_INTERIOR_FUNDING_MARK
    else:
        inconclusive_reason = None
    funding_evaluable = inconclusive_reason is None
    executions: dict[D1HistoricalFeeCellV0, D1HistoricalExecutionV0] = {}
    for _multiplier, fee_cell in _FEE_CELLS:
        try:
            executions[fee_cell] = calculate_d1_historical_execution_v0(
                side=decision.side,
                fee_cell=fee_cell,
                entry_time_ms=entry_time,
                exit_time_ms=exit_time,
                entry_reference_price=active.entry_reference_price,
                exit_reference_price=exit_reference_candle.open,
                funding_points=relevant if funding_evaluable else (),
            )
        except (
            D1HistoricalFundingBoundaryAmbiguityV0,
            D1HistoricalMathErrorV0,
        ) as error:
            raise D1HistoricalDevelopmentContractErrorV0(
                "historical execution math rejected a prevalidated episode"
            ) from error
    statistical_unit_id = _hash_document(
        _STATISTICAL_UNIT_ID_DOMAIN,
        {
            "entry_reference_time_ms": entry_time,
            "exit_decision_event_id": exit_decision.event_id,
            "exit_reference_time_ms": exit_time,
            "signal_event_id": decision.event_id,
            "symbol": data.symbol,
        },
    )
    projections: list[D1HistoricalProjectionCellV0] = []
    for notional in _NOTIONALS:
        for multiplier, fee_cell in _FEE_CELLS:
            execution = executions[fee_cell]
            projections.append(
                D1HistoricalProjectionCellV0(
                    statistical_unit_id=statistical_unit_id,
                    notional_usdt=notional,
                    fee_multiplier=multiplier,
                    fee_rate_per_side=fee_cell.rate_per_side,
                    gross_return=execution.gross_return,
                    executable_return_before_fee_funding=(
                        execution.execution_return_before_fee_and_funding
                    ),
                    slippage_return=execution.slippage_return,
                    fee_return=execution.fee_return,
                    funding_return=(execution.funding_return if funding_evaluable else None),
                    net_return=execution.net_return if funding_evaluable else None,
                    projected_net_pnl_usdt=(
                        project_d1_historical_pnl_v0(
                            execution,
                            notional_usdt=notional,
                        )
                        if funding_evaluable
                        else None
                    ),
                    _factory_token=_PROJECTION_FACTORY_TOKEN,
                )
            )
    primary_execution = executions[D1HistoricalFeeCellV0.PRIMARY_1_0]
    episode = D1HistoricalEpisodeV0(
        statistical_unit_id=statistical_unit_id,
        symbol=data.symbol,
        side=decision.side,
        signal_event_id=decision.event_id,
        signal_payload_sha256=decision.payload_sha256,
        signal_bar_open_ms=decision.bar_open_ms,
        signal_decision_cutoff_ms=decision.decision_cutoff_ms,
        entry_reference_time_ms=entry_time,
        entry_reference_price=active.entry_reference_price,
        entry_executable_price=primary_execution.entry_execution_price,
        exit_observation_open_ms=exit_decision.bar_open_ms,
        exit_observation_close_ms=exit_decision.bar_close_ms,
        exit_decision_event_id=exit_decision.event_id,
        exit_decision_payload_sha256=exit_decision.payload_sha256,
        exit_reason=exit_decision.exit_reason,
        exit_reference_time_ms=exit_time,
        exit_reference_price=exit_reference_candle.open,
        exit_executable_price=d1_historical_exit_execution_price_v0(
            side=decision.side,
            reference_price=exit_reference_candle.open,
        ),
        funding_event_count=len(interior),
        funding_evaluable=funding_evaluable,
        funding_inconclusive_reason=inconclusive_reason,
        projections=tuple(projections),
        five_minute_manifest_sha256=data.five_minute_manifest_sha256,
        hourly_manifest_sha256=data.hourly_manifest_sha256,
        funding_file_sha256=data.funding_file_sha256,
        _factory_token=_EPISODE_FACTORY_TOKEN,
    )
    canonical_d1_historical_episode_v0(episode)
    return episode


def _combine_run_counters(values: tuple[_RunCountersV0, ...]) -> _RunCountersV0:
    return _RunCountersV0(
        full_signal_count=sum(value.full_signal_count for value in values),
        entered_position_count=sum(value.entered_position_count for value in values),
        prefilter_candidate_count=sum(value.prefilter_candidate_count for value in values),
        prefilter_necessary_gate_false_count=sum(
            value.prefilter_necessary_gate_false_count for value in values
        ),
        invalid_input_inconclusive_count=sum(
            value.invalid_input_inconclusive_count for value in values
        ),
        pending_or_active_suppressed_signal_count=sum(
            value.pending_or_active_suppressed_signal_count for value in values
        ),
        entry_distance_rejection_count=sum(
            value.entry_distance_rejection_count for value in values
        ),
    )


def _select_global_nonoverlap_evaluable_v0(
    episodes: tuple[D1HistoricalEpisodeV0, ...],
) -> tuple[D1HistoricalEpisodeV0, ...]:
    """Apply deterministic earliest-exit scheduling to half-open global intervals."""

    ordered = sorted(
        (value for value in episodes if value.funding_evaluable),
        key=lambda value: (
            value.exit_reference_time_ms,
            value.entry_reference_time_ms,
            value.symbol,
            value.statistical_unit_id,
        ),
    )
    selected: list[D1HistoricalEpisodeV0] = []
    prior_selected_exit: int | None = None
    for value in ordered:
        if prior_selected_exit is None or value.entry_reference_time_ms >= prior_selected_exit:
            selected.append(value)
            prior_selected_exit = value.exit_reference_time_ms
    return tuple(selected)


def _summarize_development_v0(
    *,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    censors: tuple[D1HistoricalCensorV0, ...],
    counters: _RunCountersV0,
    funding_coverage_status_by_symbol: tuple[tuple[str, str], ...],
) -> D1HistoricalDevelopmentSummaryV0:
    evaluable = tuple(value for value in episodes if value.funding_evaluable)
    global_nonoverlap_evaluable = _select_global_nonoverlap_evaluable_v0(episodes)
    funding_counts = tuple(
        (
            reason.value,
            sum(value.funding_inconclusive_reason is reason for value in episodes),
        )
        for reason in D1HistoricalFundingInconclusiveReasonV0
    )
    exit_reason_counts = tuple(
        (
            reason.value,
            sum(value.exit_reason is reason for value in episodes),
        )
        for reason in D1ExitReasonV0
        if reason is not D1ExitReasonV0.KEEP
    )
    fee_aggregates = tuple(
        _build_fee_aggregate_v0(
            episodes=episodes,
            fee_multiplier=multiplier,
            fee_cell=fee_cell,
        )
        for multiplier, fee_cell in _FEE_CELLS
    )
    breakdowns = _build_all_breakdowns_v0(episodes)
    disposition = _development_disposition_v0(
        evaluable=evaluable,
        global_nonoverlap_evaluable_count=len(global_nonoverlap_evaluable),
        fee_aggregates=fee_aggregates,
    )
    summary = D1HistoricalDevelopmentSummaryV0(
        episode_count=len(episodes),
        evaluable_episode_count=len(evaluable),
        global_nonoverlap_evaluable_count=len(global_nonoverlap_evaluable),
        funding_coverage_status_by_symbol=funding_coverage_status_by_symbol,
        funding_inconclusive_counts=funding_counts,
        long_episode_count=sum(value.side is D1SideV0.LONG for value in episodes),
        short_episode_count=sum(value.side is D1SideV0.SHORT for value in episodes),
        evaluable_long_episode_count=sum(value.side is D1SideV0.LONG for value in evaluable),
        evaluable_short_episode_count=sum(value.side is D1SideV0.SHORT for value in evaluable),
        active_utc_day_count=len(
            {value.entry_reference_time_ms // _UTC_DAY_MS for value in evaluable}
        ),
        full_signal_count=counters.full_signal_count,
        entered_position_count=counters.entered_position_count,
        prefilter_candidate_count=counters.prefilter_candidate_count,
        prefilter_necessary_gate_false_count=(counters.prefilter_necessary_gate_false_count),
        invalid_input_inconclusive_count=(counters.invalid_input_inconclusive_count),
        pending_or_active_suppressed_signal_count=(
            counters.pending_or_active_suppressed_signal_count
        ),
        entry_distance_rejection_count=counters.entry_distance_rejection_count,
        right_edge_censor_count=len(censors),
        exit_reason_counts=exit_reason_counts,
        fee_aggregates=fee_aggregates,
        breakdowns=breakdowns,
        disposition=disposition,
        _factory_token=_SUMMARY_FACTORY_TOKEN,
    )
    canonical_d1_historical_summary_v0(summary)
    return summary


def _build_fee_aggregate_v0(
    *,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    fee_multiplier: Decimal,
    fee_cell: D1HistoricalFeeCellV0,
) -> D1HistoricalFeeAggregateV0:
    net_by_episode = tuple(
        (
            episode,
            _projection_for_cell(
                episode,
                fee_multiplier=fee_multiplier,
                notional=Decimal("100"),
            ).net_return,
        )
        for episode in episodes
    )
    evaluable = tuple(
        (episode, cast(Decimal, net)) for episode, net in net_by_episode if net is not None
    )
    if not evaluable:
        return D1HistoricalFeeAggregateV0(
            fee_multiplier=fee_multiplier,
            fee_rate_per_side=fee_cell.rate_per_side,
            episode_count=len(episodes),
            evaluable_episode_count=0,
            total_net_return=None,
            mean_net_return=None,
            profit_factor=None,
            profit_factor_infinite=False,
            projected_total_pnl_100_usdt=None,
            projected_total_pnl_1000_usdt=None,
            positive_symbol_count=0,
            net_after_top_three_symbols=None,
            net_after_top_ten_episodes=None,
            _factory_token=_FEE_AGGREGATE_FACTORY_TOKEN,
        )
    values = tuple(value for _, value in evaluable)
    with localcontext(protocol_decimal_context_v2()):
        total = sum(values, Decimal(0))
        mean = total / Decimal(len(values))
        projected_100 = total * Decimal("100")
        projected_1000 = total * Decimal("1000")
    profit_factor, infinite = _profit_factor(values)
    symbol_totals = {
        symbol: sum(
            (net for episode, net in evaluable if episode.symbol == symbol),
            Decimal(0),
        )
        for symbol in D1_HISTORICAL_UNIVERSE_V0
    }
    ranked_symbols = sorted(
        D1_HISTORICAL_UNIVERSE_V0,
        key=lambda symbol: (-symbol_totals[symbol], symbol),
    )
    removed_symbols = set(ranked_symbols[:3])
    with localcontext(protocol_decimal_context_v2()):
        after_top_three = sum(
            (value for episode, value in evaluable if episode.symbol not in removed_symbols),
            Decimal(0),
        )
        ordered_episode_values = sorted(values, reverse=True)
        after_top_ten = sum(ordered_episode_values[10:], Decimal(0))
    return D1HistoricalFeeAggregateV0(
        fee_multiplier=fee_multiplier,
        fee_rate_per_side=fee_cell.rate_per_side,
        episode_count=len(episodes),
        evaluable_episode_count=len(evaluable),
        total_net_return=total,
        mean_net_return=mean,
        profit_factor=profit_factor,
        profit_factor_infinite=infinite,
        projected_total_pnl_100_usdt=projected_100,
        projected_total_pnl_1000_usdt=projected_1000,
        positive_symbol_count=sum(value > 0 for value in symbol_totals.values()),
        net_after_top_three_symbols=after_top_three,
        net_after_top_ten_episodes=after_top_ten,
        _factory_token=_FEE_AGGREGATE_FACTORY_TOKEN,
    )


def _build_all_breakdowns_v0(
    episodes: tuple[D1HistoricalEpisodeV0, ...],
) -> tuple[D1HistoricalBreakdownV0, ...]:
    groups: list[
        tuple[
            D1HistoricalBreakdownKindV0,
            str,
            tuple[D1HistoricalEpisodeV0, ...],
        ]
    ] = [(D1HistoricalBreakdownKindV0.OVERALL, "ALL", episodes)]
    groups.extend(
        (
            D1HistoricalBreakdownKindV0.SYMBOL,
            symbol,
            tuple(value for value in episodes if value.symbol == symbol),
        )
        for symbol in D1_HISTORICAL_UNIVERSE_V0
    )
    groups.extend(
        (
            D1HistoricalBreakdownKindV0.SIDE,
            side.value,
            tuple(value for value in episodes if value.side is side),
        )
        for side in D1SideV0
    )
    groups.extend(
        (
            D1HistoricalBreakdownKindV0.SYMBOL_SIDE,
            f"{symbol}|{side.value}",
            tuple(value for value in episodes if value.symbol == symbol and value.side is side),
        )
        for symbol in D1_HISTORICAL_UNIVERSE_V0
        for side in D1SideV0
    )
    groups.extend(
        (
            D1HistoricalBreakdownKindV0.EXIT_REASON,
            reason.value,
            tuple(value for value in episodes if value.exit_reason is reason),
        )
        for reason in D1ExitReasonV0
        if reason is not D1ExitReasonV0.KEEP
    )
    return tuple(
        _build_breakdown_v0(
            kind=kind,
            key=key,
            episodes=group,
            fee_multiplier=multiplier,
            fee_cell=fee_cell,
        )
        for multiplier, fee_cell in _FEE_CELLS
        for kind, key, group in groups
    )


def _build_breakdown_v0(
    *,
    kind: D1HistoricalBreakdownKindV0,
    key: str,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    fee_multiplier: Decimal,
    fee_cell: D1HistoricalFeeCellV0,
) -> D1HistoricalBreakdownV0:
    projections = tuple(
        _projection_for_cell(
            episode,
            fee_multiplier=fee_multiplier,
            notional=Decimal("100"),
        )
        for episode in episodes
    )
    evaluable = tuple(value for value in projections if value.net_return is not None)
    if not evaluable:
        return D1HistoricalBreakdownV0(
            kind=kind,
            key=key,
            fee_multiplier=fee_multiplier,
            fee_rate_per_side=fee_cell.rate_per_side,
            episode_count=len(episodes),
            evaluable_episode_count=0,
            positive_episode_count=0,
            strict_positive_hit_rate=None,
            total_net_return=None,
            mean_net_return=None,
            median_net_return=None,
            profit_factor=None,
            profit_factor_infinite=False,
            mean_gross_return=None,
            mean_slippage_return=None,
            mean_fee_return=None,
            mean_funding_return=None,
            projected_total_pnl_100_usdt=None,
            projected_total_pnl_1000_usdt=None,
            _factory_token=_BREAKDOWN_FACTORY_TOKEN,
        )
    net = tuple(cast(Decimal, value.net_return) for value in evaluable)
    gross = tuple(value.gross_return for value in evaluable)
    slippage = tuple(value.slippage_return for value in evaluable)
    fee = tuple(value.fee_return for value in evaluable)
    funding = tuple(cast(Decimal, value.funding_return) for value in evaluable)
    positive = sum(value > 0 for value in net)
    profit_factor, infinite = _profit_factor(net)
    with localcontext(protocol_decimal_context_v2()):
        total = sum(net, Decimal(0))
        denominator = Decimal(len(net))
        return D1HistoricalBreakdownV0(
            kind=kind,
            key=key,
            fee_multiplier=fee_multiplier,
            fee_rate_per_side=fee_cell.rate_per_side,
            episode_count=len(episodes),
            evaluable_episode_count=len(evaluable),
            positive_episode_count=positive,
            strict_positive_hit_rate=Decimal(positive) / denominator,
            total_net_return=total,
            mean_net_return=total / denominator,
            median_net_return=_median_decimal(net),
            profit_factor=profit_factor,
            profit_factor_infinite=infinite,
            mean_gross_return=sum(gross, Decimal(0)) / denominator,
            mean_slippage_return=sum(slippage, Decimal(0)) / denominator,
            mean_fee_return=sum(fee, Decimal(0)) / denominator,
            mean_funding_return=sum(funding, Decimal(0)) / denominator,
            projected_total_pnl_100_usdt=total * Decimal("100"),
            projected_total_pnl_1000_usdt=total * Decimal("1000"),
            _factory_token=_BREAKDOWN_FACTORY_TOKEN,
        )


def _projection_for_cell(
    episode: D1HistoricalEpisodeV0,
    *,
    fee_multiplier: Decimal,
    notional: Decimal,
) -> D1HistoricalProjectionCellV0:
    for value in episode.projections:
        if value.fee_multiplier == fee_multiplier and value.notional_usdt == notional:
            return value
    raise D1HistoricalDevelopmentContractErrorV0("episode is missing a declared projection cell")


def _profit_factor(values: tuple[Decimal, ...]) -> tuple[Decimal | None, bool]:
    gains = sum((value for value in values if value > 0), Decimal(0))
    losses = -sum((value for value in values if value < 0), Decimal(0))
    if losses == 0:
        return None, gains > 0
    with localcontext(protocol_decimal_context_v2()):
        return gains / losses, False


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise D1HistoricalDevelopmentContractErrorV0("median requires at least one value")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    with localcontext(protocol_decimal_context_v2()):
        return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


def _development_disposition_v0(
    *,
    evaluable: tuple[D1HistoricalEpisodeV0, ...],
    global_nonoverlap_evaluable_count: int,
    fee_aggregates: tuple[D1HistoricalFeeAggregateV0, ...],
) -> D1HistoricalDispositionV0:
    primary = fee_aggregates[0]
    if global_nonoverlap_evaluable_count < 150:
        return D1HistoricalDispositionV0.INCONCLUSIVE_LOW_INFORMATION
    if (
        primary.mean_net_return is not None
        and primary.mean_net_return < 0
        and primary.profit_factor is not None
        and primary.profit_factor < Decimal(1)
    ):
        return D1HistoricalDispositionV0.RETROSPECTIVE_PROXY_REJECT
    evaluable_long = sum(value.side is D1SideV0.LONG for value in evaluable)
    evaluable_short = sum(value.side is D1SideV0.SHORT for value in evaluable)
    active_days = len({value.entry_reference_time_ms // _UTC_DAY_MS for value in evaluable})
    common = (
        len(evaluable) >= 500
        and global_nonoverlap_evaluable_count >= 150
        and evaluable_long >= 100
        and evaluable_short >= 100
        and active_days >= 45
    )
    both_cells = all(_fee_cell_passes_screen(value) for value in fee_aggregates)
    if common and both_cells:
        return D1HistoricalDispositionV0.RETROSPECTIVE_PROXY_SCREEN_PASS_INCONCLUSIVE
    return D1HistoricalDispositionV0.INCONCLUSIVE_MIXED_PROXY_EVIDENCE


def _fee_cell_passes_screen(value: D1HistoricalFeeAggregateV0) -> bool:
    profit_factor_pass = value.profit_factor_infinite or (
        value.profit_factor is not None and value.profit_factor >= _PF_SCREEN_MIN
    )
    return bool(
        value.evaluable_episode_count >= 500
        and value.total_net_return is not None
        and value.total_net_return > 0
        and value.mean_net_return is not None
        and value.mean_net_return >= _MEAN_SCREEN_MIN
        and profit_factor_pass
        and value.positive_symbol_count >= 6
        and value.net_after_top_three_symbols is not None
        and value.net_after_top_three_symbols > 0
        and value.net_after_top_ten_episodes is not None
        and value.net_after_top_ten_episodes > 0
    )


def load_d1_historical_development_freeze_v0(
    manifest_path: str | Path,
    *,
    workspace_root: str | Path,
    expected_manifest_sha256: str,
    input_authority: D1HistoricalInputAuthorityV0,
    preregistration_sha256: str,
) -> D1HistoricalDevelopmentFreezeV0:
    """Load and policy-check a pinned broad source freeze; never create one."""

    canonical_d1_historical_input_authority_v0(input_authority)
    _require_sha256(preregistration_sha256, "preregistration_sha256")
    authority = load_downstream_code_freeze_v1(
        manifest_path,
        workspace_root=workspace_root,
        expected_manifest_sha256=expected_manifest_sha256,
        required_upstream_sha256={
            "d1_input_authority": input_authority.authority_sha256,
            "d1_predecessor_freeze_001": (
                D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
            ),
            "d1_preregistration": preregistration_sha256,
        },
    )
    return _validate_freeze_authority(
        authority,
        input_authority_sha256=input_authority.authority_sha256,
        preregistration_sha256=preregistration_sha256,
    )


def canonical_d1_historical_development_freeze_v0(
    value: D1HistoricalDevelopmentFreezeV0,
) -> bytes:
    if type(value) is not D1HistoricalDevelopmentFreezeV0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "value must be exact D1HistoricalDevelopmentFreezeV0"
        )
    expected = _hash_document(_FREEZE_RECEIPT_HASH_DOMAIN, _freeze_document(value))
    if value.receipt_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0("development freeze receipt hash differs")
    return canonical_json_line({**_freeze_document(value), "receipt_sha256": expected})


def _validate_freeze_authority(
    authority: DownstreamCodeFreezeAuthorityV1,
    *,
    input_authority_sha256: str,
    preregistration_sha256: str,
) -> D1HistoricalDevelopmentFreezeV0:
    if type(authority) is not DownstreamCodeFreezeAuthorityV1:
        raise D1HistoricalDevelopmentContractErrorV0(
            "freeze authority has the wrong validated type"
        )
    policy_exact = (
        authority.purpose == D1_DEVELOPMENT_FREEZE_PURPOSE_V0
        and authority.include_trees == D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0
        and authority.include_files == D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
        and authority.included_suffixes == D1_DEVELOPMENT_FREEZE_SUFFIXES_V0
        and dict(authority.upstream_sha256)
        == {
            "d1_input_authority": input_authority_sha256,
            "d1_predecessor_freeze_001": (
                D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
            ),
            "d1_preregistration": preregistration_sha256,
        }
        and _RUNNER_RELATIVE_PATH in authority.file_sha256
        and _RULE_RELATIVE_PATH in authority.file_sha256
        and authority.file_sha256.get(
            D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0
        )
        == D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
        and authority.file_sha256.get(_PREREGISTRATION_RELATIVE_PATH) == preregistration_sha256
    )
    if not policy_exact:
        raise D1HistoricalDevelopmentContractErrorV0(
            "loaded code freeze differs from the exact D1 development policy"
        )
    try:
        created = datetime.fromisoformat(authority.created_at_utc)
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0("freeze created_at_utc is invalid") from error
    if created.tzinfo is None or created.utcoffset() != UTC.utcoffset(created):
        raise D1HistoricalDevelopmentContractErrorV0(
            "freeze created_at_utc must be timezone-aware UTC"
        )
    return D1HistoricalDevelopmentFreezeV0(
        manifest_sha256=authority.manifest_sha256,
        manifest_created_at_ms=int(created.timestamp() * 1_000),
        input_authority_sha256=input_authority_sha256,
        preregistration_sha256=preregistration_sha256,
        frozen_file_count=len(authority.file_sha256),
        _factory_token=_FREEZE_FACTORY_TOKEN,
    )


def _input_authority_document(
    value: D1HistoricalInputAuthorityV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "funding_manifest_relative_path": value.funding_manifest_relative_path,
        "funding_manifest_sha256": value.funding_manifest_sha256,
        "kline_manifests": [
            {
                "interval": item.interval,
                "manifest_sha256": item.manifest_sha256,
                "relative_manifest_path": item.relative_manifest_path,
                "symbol": item.symbol,
            }
            for item in value.kline_manifests
        ],
        "schema_version": value.schema_version,
    }
    if include_hash:
        document["authority_sha256"] = value.authority_sha256
    return document


def _freeze_document(value: D1HistoricalDevelopmentFreezeV0) -> dict[str, object]:
    return {
        "frozen_file_count": value.frozen_file_count,
        "input_authority_sha256": value.input_authority_sha256,
        "manifest_created_at_ms": value.manifest_created_at_ms,
        "manifest_sha256": value.manifest_sha256,
        "preregistration_sha256": value.preregistration_sha256,
        "schema_version": value.schema_version,
    }


def canonical_d1_historical_episode_v0(value: D1HistoricalEpisodeV0) -> bytes:
    """Revalidate and serialize one non-promoting historical episode."""

    if type(value) is not D1HistoricalEpisodeV0:
        raise D1HistoricalDevelopmentContractErrorV0("value must be exact D1HistoricalEpisodeV0")
    expected = _hash_document(
        _EPISODE_HASH_DOMAIN,
        _episode_document(value, include_hash=False),
    )
    if value.episode_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0("episode hash differs")
    return canonical_json_line(_episode_document(value, include_hash=True))


def canonical_d1_historical_summary_v0(
    value: D1HistoricalDevelopmentSummaryV0,
) -> bytes:
    """Revalidate and serialize the deterministic development summary."""

    if type(value) is not D1HistoricalDevelopmentSummaryV0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "value must be exact D1HistoricalDevelopmentSummaryV0"
        )
    expected = _hash_document(
        _SUMMARY_HASH_DOMAIN,
        _summary_document(value, include_hash=False),
    )
    if value.summary_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0("summary hash differs")
    return canonical_json_line(_summary_document(value, include_hash=True))


def canonical_d1_historical_development_result_v0(
    value: D1HistoricalDevelopmentResultV0,
) -> bytes:
    """Revalidate and serialize the complete historical development result."""

    if type(value) is not D1HistoricalDevelopmentResultV0:
        raise D1HistoricalDevelopmentContractErrorV0(
            "value must be exact D1HistoricalDevelopmentResultV0"
        )
    for episode in value.episodes:
        canonical_d1_historical_episode_v0(episode)
    for censor in value.censors:
        canonical_d1_historical_censor_v0(censor)
    canonical_d1_historical_summary_v0(value.summary)
    expected = _hash_document(
        _RESULT_HASH_DOMAIN,
        _result_document(value, include_hash=False),
    )
    if value.result_sha256 != expected:
        raise D1HistoricalDevelopmentContractErrorV0("result hash differs")
    return canonical_json_line(_result_document(value, include_hash=True))


def verify_d1_historical_serialized_artifacts_v0(
    *,
    episode_lines: Iterable[bytes],
    censor_lines: Iterable[bytes],
    summary_raw: bytes,
    result_index_raw: bytes,
    expected_run_id: str,
    expected_run_started_at_ms: int,
    expected_input_authority_sha256: str,
    expected_code_freeze_manifest_sha256: str,
    expected_code_freeze_receipt_sha256: str,
    expected_preregistration_sha256: str,
) -> D1HistoricalSerializedArtifactsVerificationV0:
    """Rebuild exact domain objects and recompute every serialized D1 binding."""

    _require_identity(expected_run_id, "serialized expected_run_id")
    _require_nonnegative_int(
        expected_run_started_at_ms,
        "serialized expected_run_started_at_ms",
    )
    for value, label in (
        (expected_input_authority_sha256, "serialized input authority"),
        (expected_code_freeze_manifest_sha256, "serialized freeze manifest"),
        (expected_code_freeze_receipt_sha256, "serialized freeze receipt"),
        (expected_preregistration_sha256, "serialized preregistration"),
    ):
        _require_sha256(value, label)

    episodes: list[D1HistoricalEpisodeV0] = []
    for raw in episode_lines:
        if len(episodes) >= D1_HISTORICAL_MAX_EPISODES_V0:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episode count exceeds its frozen bound"
            )
        episodes.append(_parse_serialized_episode_v0(raw))

    censors: list[D1HistoricalCensorV0] = []
    for raw in censor_lines:
        if len(censors) >= D1_HISTORICAL_MAX_CENSORS_V0:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized censor count exceeds its frozen bound"
            )
        censors.append(_parse_serialized_censor_v0(raw))

    episode_snapshot = tuple(episodes)
    censor_snapshot = tuple(censors)
    _validate_serialized_record_sequences_v0(
        episodes=episode_snapshot,
        censors=censor_snapshot,
    )

    summary = _decode_canonical_serialized_object_v0(summary_raw, "serialized summary")
    if set(summary) != _SERIALIZED_SUMMARY_KEYS_V0:
        raise D1HistoricalDevelopmentContractErrorV0("serialized summary fields are not exact")
    _verified_serialized_document_hash_v0(
        summary,
        hash_field="summary_sha256",
        domain=_SUMMARY_HASH_DOMAIN,
        label="serialized summary",
    )
    counters = _serialized_run_counters_v0(summary)
    funding_coverage = _serialized_funding_coverage_v0(summary)
    _validate_serialized_funding_coverage_consistency_v0(
        episodes=episode_snapshot,
        funding_coverage_status_by_symbol=funding_coverage,
    )
    _validate_serialized_counter_reconciliation_v0(
        episodes=episode_snapshot,
        censors=censor_snapshot,
        counters=counters,
    )
    rebuilt_summary = _summarize_development_v0(
        episodes=episode_snapshot,
        censors=censor_snapshot,
        counters=counters,
        funding_coverage_status_by_symbol=funding_coverage,
    )
    rebuilt_summary_raw = canonical_d1_historical_summary_v0(rebuilt_summary)
    if rebuilt_summary_raw != summary_raw:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized summary differs from the exact episode reducer"
        )

    result = _decode_canonical_serialized_object_v0(
        result_index_raw,
        "serialized result index",
    )
    if set(result) != _SERIALIZED_RESULT_KEYS_V0:
        raise D1HistoricalDevelopmentContractErrorV0("serialized result index fields are not exact")
    _verified_serialized_document_hash_v0(
        result,
        hash_field="result_sha256",
        domain=_RESULT_HASH_DOMAIN,
        label="serialized result index",
    )
    rebuilt_result = D1HistoricalDevelopmentResultV0(
        run_id=expected_run_id,
        run_started_at_ms=expected_run_started_at_ms,
        input_authority_sha256=expected_input_authority_sha256,
        code_freeze_receipt_sha256=expected_code_freeze_receipt_sha256,
        code_freeze_manifest_sha256=expected_code_freeze_manifest_sha256,
        preregistration_sha256=expected_preregistration_sha256,
        episodes=episode_snapshot,
        censors=censor_snapshot,
        summary=rebuilt_summary,
        _factory_token=_RESULT_FACTORY_TOKEN,
    )
    rebuilt_result_raw = canonical_d1_historical_development_result_v0(rebuilt_result)
    if rebuilt_result_raw != result_index_raw:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized result differs from exact records and frozen bindings"
        )
    episode_root = _ordered_hash_root(
        _EPISODE_SEQUENCE_ROOT_DOMAIN,
        tuple(value.episode_sha256 for value in episode_snapshot),
    )
    censor_root = _ordered_hash_root(
        _CENSOR_SEQUENCE_ROOT_DOMAIN,
        tuple(value.censor_sha256 for value in censor_snapshot),
    )
    return D1HistoricalSerializedArtifactsVerificationV0(
        result_sha256=rebuilt_result.result_sha256,
        summary_sha256=rebuilt_summary.summary_sha256,
        episode_count=len(episode_snapshot),
        censor_count=len(censor_snapshot),
        episode_sequence_root_sha256=episode_root,
        censor_sequence_root_sha256=censor_root,
        _factory_token=_SERIALIZED_VERIFICATION_FACTORY_TOKEN,
    )


def _parse_serialized_episode_v0(raw: bytes) -> D1HistoricalEpisodeV0:
    document = _decode_canonical_serialized_object_v0(raw, "serialized episode")
    if set(document) != _SERIALIZED_EPISODE_KEYS_V0:
        raise D1HistoricalDevelopmentContractErrorV0("serialized episode fields are not exact")
    _verified_serialized_document_hash_v0(
        document,
        hash_field="episode_sha256",
        domain=_EPISODE_HASH_DOMAIN,
        label="serialized episode",
    )
    _require_serialized_false_claims_v0(
        document,
        (
            "efficacy_claim",
            "execution_conclusive",
            "historical_bbo_available",
            "paper_fill_claim",
            "probability_claim",
            "production_order_placement",
            "promoting",
            "prospective",
        ),
        "serialized episode",
    )
    projections_raw = document.get("projections")
    if not isinstance(projections_raw, list) or len(projections_raw) != 4:
        raise D1HistoricalDevelopmentContractErrorV0("serialized episode projection cells differ")
    projections = tuple(
        _parse_serialized_projection_v0(value, index=index)
        for index, value in enumerate(projections_raw)
    )
    side = _serialized_side_v0(document.get("side"))
    exit_reason = _serialized_exit_reason_v0(document.get("exit_reason"))
    funding_evaluable = _serialized_bool_v0(
        document.get("funding_evaluable"),
        "serialized episode funding_evaluable",
    )
    funding_reason_raw = document.get("funding_inconclusive_reason")
    funding_reason = (
        None if funding_reason_raw is None else _serialized_funding_reason_v0(funding_reason_raw)
    )
    episode = D1HistoricalEpisodeV0(
        statistical_unit_id=_serialized_text_v0(
            document.get("statistical_unit_id"),
            "serialized episode statistical_unit_id",
        ),
        symbol=_serialized_text_v0(
            document.get("symbol"),
            "serialized episode symbol",
        ),
        side=side,
        signal_event_id=_serialized_text_v0(
            document.get("signal_event_id"),
            "serialized episode signal_event_id",
        ),
        signal_payload_sha256=_serialized_text_v0(
            document.get("signal_payload_sha256"),
            "serialized episode signal_payload_sha256",
        ),
        signal_bar_open_ms=_require_nonnegative_int(
            document.get("signal_bar_open_ms"),
            "serialized episode signal_bar_open_ms",
        ),
        signal_decision_cutoff_ms=_require_nonnegative_int(
            document.get("signal_decision_cutoff_ms"),
            "serialized episode signal_decision_cutoff_ms",
        ),
        entry_reference_time_ms=_require_nonnegative_int(
            document.get("entry_reference_time_ms"),
            "serialized episode entry_reference_time_ms",
        ),
        entry_reference_price=_serialized_decimal_v0(
            document.get("entry_reference_price"),
            "serialized episode entry_reference_price",
        ),
        entry_executable_price=_serialized_decimal_v0(
            document.get("entry_executable_price"),
            "serialized episode entry_executable_price",
        ),
        exit_observation_open_ms=_require_nonnegative_int(
            document.get("exit_observation_open_ms"),
            "serialized episode exit_observation_open_ms",
        ),
        exit_observation_close_ms=_require_nonnegative_int(
            document.get("exit_observation_close_ms"),
            "serialized episode exit_observation_close_ms",
        ),
        exit_decision_event_id=_serialized_text_v0(
            document.get("exit_decision_event_id"),
            "serialized episode exit_decision_event_id",
        ),
        exit_decision_payload_sha256=_serialized_text_v0(
            document.get("exit_decision_payload_sha256"),
            "serialized episode exit_decision_payload_sha256",
        ),
        exit_reason=exit_reason,
        exit_reference_time_ms=_require_nonnegative_int(
            document.get("exit_reference_time_ms"),
            "serialized episode exit_reference_time_ms",
        ),
        exit_reference_price=_serialized_decimal_v0(
            document.get("exit_reference_price"),
            "serialized episode exit_reference_price",
        ),
        exit_executable_price=_serialized_decimal_v0(
            document.get("exit_executable_price"),
            "serialized episode exit_executable_price",
        ),
        funding_event_count=_require_nonnegative_int(
            document.get("funding_event_count"),
            "serialized episode funding_event_count",
        ),
        funding_evaluable=funding_evaluable,
        funding_inconclusive_reason=funding_reason,
        projections=projections,
        five_minute_manifest_sha256=_serialized_text_v0(
            document.get("five_minute_manifest_sha256"),
            "serialized episode five_minute_manifest_sha256",
        ),
        hourly_manifest_sha256=_serialized_text_v0(
            document.get("hourly_manifest_sha256"),
            "serialized episode hourly_manifest_sha256",
        ),
        funding_file_sha256=_serialized_text_v0(
            document.get("funding_file_sha256"),
            "serialized episode funding_file_sha256",
        ),
        _factory_token=_EPISODE_FACTORY_TOKEN,
    )
    _validate_serialized_episode_timing_v0(episode)
    _validate_serialized_episode_economic_math_v0(episode)
    if canonical_d1_historical_episode_v0(episode) != raw:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode differs from its exact domain model"
        )
    return episode


def _parse_serialized_projection_v0(
    value: object,
    *,
    index: int,
) -> D1HistoricalProjectionCellV0:
    label = f"serialized projection {index}"
    if not isinstance(value, dict) or set(value) != _SERIALIZED_PROJECTION_KEYS_V0:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} fields are not exact")
    if (
        value.get("schema_version") != "d1_historical_projection_cell_v0"
        or value.get("sizing_projection_creates_new_statistical_unit") is not False
    ):
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} protocol fields differ")
    return D1HistoricalProjectionCellV0(
        statistical_unit_id=_serialized_text_v0(
            value.get("statistical_unit_id"),
            f"{label} statistical_unit_id",
        ),
        notional_usdt=_serialized_decimal_v0(
            value.get("notional_usdt"),
            f"{label} notional_usdt",
        ),
        fee_multiplier=_serialized_decimal_v0(
            value.get("fee_multiplier"),
            f"{label} fee_multiplier",
        ),
        fee_rate_per_side=_serialized_decimal_v0(
            value.get("fee_rate_per_side"),
            f"{label} fee_rate_per_side",
        ),
        gross_return=_serialized_decimal_v0(
            value.get("gross_return"),
            f"{label} gross_return",
        ),
        executable_return_before_fee_funding=_serialized_decimal_v0(
            value.get("executable_return_before_fee_funding"),
            f"{label} executable_return_before_fee_funding",
        ),
        slippage_return=_serialized_decimal_v0(
            value.get("slippage_return"),
            f"{label} slippage_return",
        ),
        fee_return=_serialized_decimal_v0(
            value.get("fee_return"),
            f"{label} fee_return",
        ),
        funding_return=_serialized_optional_decimal_v0(
            value.get("funding_return"),
            f"{label} funding_return",
        ),
        net_return=_serialized_optional_decimal_v0(
            value.get("net_return"),
            f"{label} net_return",
        ),
        projected_net_pnl_usdt=_serialized_optional_decimal_v0(
            value.get("projected_net_pnl_usdt"),
            f"{label} projected_net_pnl_usdt",
        ),
        _factory_token=_PROJECTION_FACTORY_TOKEN,
    )


def _validate_serialized_episode_economic_math_v0(
    episode: D1HistoricalEpisodeV0,
) -> None:
    expected_statistical_unit_id = _hash_document(
        _STATISTICAL_UNIT_ID_DOMAIN,
        {
            "entry_reference_time_ms": episode.entry_reference_time_ms,
            "exit_decision_event_id": episode.exit_decision_event_id,
            "exit_reference_time_ms": episode.exit_reference_time_ms,
            "signal_event_id": episode.signal_event_id,
            "symbol": episode.symbol,
        },
    )
    if episode.statistical_unit_id != expected_statistical_unit_id:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode statistical unit differs from its derived identity"
        )

    sign = Decimal(1) if episode.side is D1SideV0.LONG else Decimal(-1)
    try:
        with localcontext(protocol_decimal_context_v2()):
            expected_entry_execution = d1_historical_entry_execution_price_v0(
                side=episode.side,
                reference_price=episode.entry_reference_price,
            )
            expected_exit_execution = d1_historical_exit_execution_price_v0(
                side=episode.side,
                reference_price=episode.exit_reference_price,
            )
            expected_gross = (
                sign
                * (episode.exit_reference_price - episode.entry_reference_price)
                / episode.entry_reference_price
            )
            expected_execution = (
                sign
                * (expected_exit_execution - expected_entry_execution)
                / episode.entry_reference_price
            )
            expected_slippage = expected_gross - expected_execution
    except (DecimalException, D1HistoricalMathErrorV0) as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode economic arithmetic is outside the protocol Decimal domain"
        ) from error
    if (
        episode.entry_executable_price != expected_entry_execution
        or episode.exit_executable_price != expected_exit_execution
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode executable prices differ from frozen adverse-price math"
        )

    funding_return = episode.projections[0].funding_return
    if episode.funding_evaluable:
        assert funding_return is not None
        if any(value.funding_return != funding_return for value in episode.projections):
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episode funding return diverges across projection cells"
            )
        if episode.funding_event_count == 0 and funding_return != 0:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episode zero-event funding return must be zero"
            )

    try:
        with localcontext(protocol_decimal_context_v2()):
            for projection, (notional, (_multiplier, fee_cell)) in zip(
                episode.projections,
                ((notional, fee_binding) for notional in _NOTIONALS for fee_binding in _FEE_CELLS),
                strict=True,
            ):
                if (
                    projection.gross_return != expected_gross
                    or projection.executable_return_before_fee_funding != expected_execution
                    or projection.slippage_return != expected_slippage
                ):
                    raise D1HistoricalDevelopmentContractErrorV0(
                        "serialized episode projection return decomposition differs"
                    )
                expected_fee = (
                    fee_cell.rate_per_side
                    * (expected_entry_execution + expected_exit_execution)
                    / episode.entry_reference_price
                )
                if projection.fee_return != expected_fee:
                    raise D1HistoricalDevelopmentContractErrorV0(
                        "serialized episode projection fee arithmetic differs"
                    )
                if episode.funding_evaluable:
                    assert funding_return is not None
                    expected_net = expected_execution - expected_fee + funding_return
                    if projection.net_return != expected_net:
                        raise D1HistoricalDevelopmentContractErrorV0(
                            "serialized episode projection net-return arithmetic differs"
                        )
                    expected_pnl = expected_net * notional
                    if projection.projected_net_pnl_usdt != expected_pnl:
                        raise D1HistoricalDevelopmentContractErrorV0(
                            "serialized episode projection PnL arithmetic differs"
                        )
    except DecimalException as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode economic arithmetic is outside the protocol Decimal domain"
        ) from error


def _validate_serialized_episode_timing_v0(
    episode: D1HistoricalEpisodeV0,
) -> None:
    bar_open_times = (
        episode.signal_bar_open_ms,
        episode.entry_reference_time_ms,
        episode.exit_observation_open_ms,
        episode.exit_reference_time_ms,
    )
    if any(value % _FIVE_MINUTE_MS != 0 for value in bar_open_times):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode bar opens are not aligned to 5m UTC boundaries"
        )
    if episode.signal_decision_cutoff_ms != (
        episode.signal_bar_open_ms + _FIVE_MINUTE_MS - 1 + DECISION_DELAY_MS_V2
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode signal cutoff differs from the closed-bar clock"
        )
    if episode.entry_reference_time_ms != (episode.signal_bar_open_ms + 2 * _FIVE_MINUTE_MS):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode entry reference is not causal open(t+2)"
        )
    if episode.exit_observation_close_ms != (
        episode.exit_observation_open_ms + _FIVE_MINUTE_MS - 1
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode exit observation is not one exact closed 5m bar"
        )
    if episode.exit_reference_time_ms != (episode.exit_observation_open_ms + 2 * _FIVE_MINUTE_MS):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode exit reference is not the first causal open"
        )
    if episode.exit_observation_open_ms < episode.entry_reference_time_ms or any(
        value < D1_HISTORICAL_DEVELOPMENT_START_MS_V0
        or value >= D1_HISTORICAL_DEVELOPMENT_END_MS_V0
        for value in bar_open_times
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode timing is outside the development interval"
        )


def _parse_serialized_censor_v0(raw: bytes) -> D1HistoricalCensorV0:
    document = _decode_canonical_serialized_object_v0(raw, "serialized censor")
    if set(document) != _SERIALIZED_CENSOR_KEYS_V0:
        raise D1HistoricalDevelopmentContractErrorV0("serialized censor fields are not exact")
    _verified_serialized_document_hash_v0(
        document,
        hash_field="censor_sha256",
        domain=_CENSOR_HASH_DOMAIN,
        label="serialized censor",
    )
    stage = _serialized_censor_stage_v0(document.get("stage"))
    reason = _serialized_text_v0(
        document.get("reason"),
        "serialized censor reason",
    )
    expected_reason = {
        D1HistoricalCensorStageV0.ENTRY_REFERENCE: "DEVELOPMENT_END_BEFORE_OPEN_T_PLUS_2",
        D1HistoricalCensorStageV0.EXIT_OBSERVATION: (
            "DEVELOPMENT_END_BEFORE_TERMINAL_EXIT_OBSERVATION"
        ),
        D1HistoricalCensorStageV0.EXIT_REFERENCE: (
            "DEVELOPMENT_END_BEFORE_CAUSAL_EXIT_REFERENCE_OPEN"
        ),
    }[stage]
    if reason != expected_reason:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized censor stage/reason binding differs"
        )
    censor = D1HistoricalCensorV0(
        symbol=_serialized_text_v0(
            document.get("symbol"),
            "serialized censor symbol",
        ),
        signal_event_id=_serialized_text_v0(
            document.get("signal_event_id"),
            "serialized censor signal_event_id",
        ),
        signal_bar_open_ms=_require_nonnegative_int(
            document.get("signal_bar_open_ms"),
            "serialized censor signal_bar_open_ms",
        ),
        stage=stage,
        reason=reason,
        _factory_token=_CENSOR_FACTORY_TOKEN,
    )
    _validate_serialized_signal_bar_time_v0(
        censor.signal_bar_open_ms,
        label="serialized censor",
    )
    if canonical_d1_historical_censor_v0(censor) != raw:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized censor differs from its exact domain model"
        )
    return censor


def _validate_serialized_record_sequences_v0(
    *,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    censors: tuple[D1HistoricalCensorV0, ...],
) -> None:
    episode_hashes: set[str] = set()
    statistical_units: set[str] = set()
    event_ids: set[str] = set()
    manifests_by_symbol: dict[str, tuple[str, str, str]] = {}
    previous_episode_key: tuple[int, int] | None = None
    for episode in episodes:
        if (
            episode.episode_sha256 in episode_hashes
            or episode.statistical_unit_id in statistical_units
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episode sequence contains a duplicate identity"
            )
        if (
            episode.signal_event_id in event_ids
            or episode.exit_decision_event_id in event_ids
            or episode.signal_event_id == episode.exit_decision_event_id
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized record sequence contains a duplicate event identity"
            )
        key = (
            D1_HISTORICAL_UNIVERSE_V0.index(episode.symbol),
            episode.signal_bar_open_ms,
        )
        if previous_episode_key is not None and key <= previous_episode_key:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episodes are not in strict symbol/time order"
            )
        previous_episode_key = key
        episode_hashes.add(episode.episode_sha256)
        statistical_units.add(episode.statistical_unit_id)
        event_ids.update((episode.signal_event_id, episode.exit_decision_event_id))
        manifests = (
            episode.five_minute_manifest_sha256,
            episode.hourly_manifest_sha256,
            episode.funding_file_sha256,
        )
        prior_manifests = manifests_by_symbol.setdefault(episode.symbol, manifests)
        if manifests != prior_manifests:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized episode manifests vary within one symbol"
            )

    censor_hashes: set[str] = set()
    previous_censor_key: tuple[int, int] | None = None
    for censor in censors:
        if censor.censor_sha256 in censor_hashes:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized censor sequence contains a duplicate identity"
            )
        if censor.signal_event_id in event_ids:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized record sequence contains a duplicate event identity"
            )
        key = (
            D1_HISTORICAL_UNIVERSE_V0.index(censor.symbol),
            censor.signal_bar_open_ms,
        )
        if previous_censor_key is not None and key <= previous_censor_key:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized censors are not in strict symbol/time order"
            )
        previous_censor_key = key
        censor_hashes.add(censor.censor_sha256)
        event_ids.add(censor.signal_event_id)


def _validate_serialized_signal_bar_time_v0(value: int, *, label: str) -> None:
    if (
        value % _FIVE_MINUTE_MS != 0
        or value < D1_HISTORICAL_DEVELOPMENT_START_MS_V0
        or value >= D1_HISTORICAL_DEVELOPMENT_END_MS_V0
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} signal bar is outside the aligned development interval"
        )


def _serialized_run_counters_v0(summary: dict[str, object]) -> _RunCountersV0:
    return _RunCountersV0(
        full_signal_count=_require_nonnegative_int(
            summary.get("full_signal_count"),
            "serialized summary full_signal_count",
        ),
        entered_position_count=_require_nonnegative_int(
            summary.get("entered_position_count"),
            "serialized summary entered_position_count",
        ),
        prefilter_candidate_count=_require_nonnegative_int(
            summary.get("prefilter_candidate_count"),
            "serialized summary prefilter_candidate_count",
        ),
        prefilter_necessary_gate_false_count=_require_nonnegative_int(
            summary.get("prefilter_necessary_gate_false_count"),
            "serialized summary prefilter_necessary_gate_false_count",
        ),
        invalid_input_inconclusive_count=_require_nonnegative_int(
            summary.get("invalid_input_inconclusive_count"),
            "serialized summary invalid_input_inconclusive_count",
        ),
        pending_or_active_suppressed_signal_count=_require_nonnegative_int(
            summary.get("pending_or_active_suppressed_signal_count"),
            "serialized summary pending_or_active_suppressed_signal_count",
        ),
        entry_distance_rejection_count=_require_nonnegative_int(
            summary.get("entry_distance_rejection_count"),
            "serialized summary entry_distance_rejection_count",
        ),
    )


def _serialized_funding_coverage_v0(
    summary: dict[str, object],
) -> tuple[tuple[str, str], ...]:
    raw = summary.get("funding_coverage_status_by_symbol")
    if not isinstance(raw, list):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized funding coverage must be an exact list"
        )
    result: list[tuple[str, str]] = []
    for index, value in enumerate(raw):
        if not isinstance(value, list) or len(value) != 2:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized funding coverage rows must be exact pairs"
            )
        result.append(
            (
                _serialized_text_v0(
                    value[0],
                    f"serialized funding coverage symbol {index}",
                ),
                _serialized_text_v0(
                    value[1],
                    f"serialized funding coverage status {index}",
                ),
            )
        )
    snapshot = tuple(result)
    if tuple(symbol for symbol, _status in snapshot) != D1_HISTORICAL_UNIVERSE_V0 or any(
        status not in {value.value for value in D1HistoricalFundingCoverageStatusV0}
        for _symbol, status in snapshot
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized funding coverage receipt differs from the exact universe"
        )
    return snapshot


def _validate_serialized_funding_coverage_consistency_v0(
    *,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    funding_coverage_status_by_symbol: tuple[tuple[str, str], ...],
) -> None:
    coverage_by_symbol = dict(funding_coverage_status_by_symbol)
    unavailable = D1HistoricalFundingCoverageStatusV0.FUNDING_COVERAGE_UNAVAILABLE.value
    unavailable_reason = D1HistoricalFundingInconclusiveReasonV0.FUNDING_COVERAGE_UNAVAILABLE
    for episode in episodes:
        coverage_unavailable = coverage_by_symbol[episode.symbol] == unavailable
        episode_declares_unavailable = (
            not episode.funding_evaluable
            and episode.funding_inconclusive_reason is unavailable_reason
        )
        if coverage_unavailable != episode_declares_unavailable:
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized funding coverage and episode reason differ"
            )


def _validate_serialized_counter_reconciliation_v0(
    *,
    episodes: tuple[D1HistoricalEpisodeV0, ...],
    censors: tuple[D1HistoricalCensorV0, ...],
    counters: _RunCountersV0,
) -> None:
    pre_entry_censors = sum(
        value.stage is D1HistoricalCensorStageV0.ENTRY_REFERENCE for value in censors
    )
    post_entry_censors = sum(
        value.stage
        in {
            D1HistoricalCensorStageV0.EXIT_OBSERVATION,
            D1HistoricalCensorStageV0.EXIT_REFERENCE,
        }
        for value in censors
    )
    expected_entered = len(episodes) + post_entry_censors
    expected_full_signals = (
        expected_entered
        + pre_entry_censors
        + counters.entry_distance_rejection_count
        + counters.pending_or_active_suppressed_signal_count
    )
    if counters.entered_position_count != expected_entered:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized entered-position count does not reconcile"
        )
    if counters.full_signal_count != expected_full_signals:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized full-signal count does not reconcile"
        )
    if counters.prefilter_candidate_count < counters.full_signal_count:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized prefilter candidate count is below full signals"
        )


def _serialized_text_v0(value: object, label: str) -> str:
    return _require_identity(value, label)


def _serialized_bool_v0(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be boolean")
    return value


def _serialized_decimal_v0(value: object, label: str) -> Decimal:
    if not isinstance(value, str):
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be a finite decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} must be a finite decimal string"
        ) from error
    return _require_finite_decimal(parsed, label)


def _serialized_optional_decimal_v0(
    value: object,
    label: str,
) -> Decimal | None:
    if value is None:
        return None
    return _serialized_decimal_v0(value, label)


def _serialized_side_v0(value: object) -> D1SideV0:
    try:
        return D1SideV0(_serialized_text_v0(value, "serialized episode side"))
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode side is unsupported"
        ) from error


def _serialized_exit_reason_v0(value: object) -> D1ExitReasonV0:
    try:
        reason = D1ExitReasonV0(_serialized_text_v0(value, "serialized episode exit_reason"))
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode exit_reason is unsupported"
        ) from error
    if reason is D1ExitReasonV0.KEEP:
        raise D1HistoricalDevelopmentContractErrorV0("serialized terminal episode cannot use KEEP")
    return reason


def _serialized_funding_reason_v0(
    value: object,
) -> D1HistoricalFundingInconclusiveReasonV0:
    try:
        return D1HistoricalFundingInconclusiveReasonV0(
            _serialized_text_v0(
                value,
                "serialized episode funding_inconclusive_reason",
            )
        )
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized episode funding reason is unsupported"
        ) from error


def _serialized_censor_stage_v0(value: object) -> D1HistoricalCensorStageV0:
    try:
        return D1HistoricalCensorStageV0(_serialized_text_v0(value, "serialized censor stage"))
    except ValueError as error:
        raise D1HistoricalDevelopmentContractErrorV0(
            "serialized censor stage is unsupported"
        ) from error


def _decode_canonical_serialized_object_v0(raw: bytes, label: str) -> dict[str, object]:
    if type(raw) is not bytes or not raw.endswith(b"\n"):
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} must be one newline-terminated bytes record"
        )
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} is invalid JSON") from error
    if not isinstance(value, dict) or canonical_json_line(value) != raw:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} is not canonical JSONL")
    return cast(dict[str, object], value)


def _verified_serialized_document_hash_v0(
    document: dict[str, object],
    *,
    hash_field: str,
    domain: bytes,
    label: str,
) -> str:
    claimed = _require_sha256(document.get(hash_field), f"{label} {hash_field}")
    body = dict(document)
    del body[hash_field]
    expected = _hash_document(domain, body)
    if claimed != expected:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} domain hash differs")
    return claimed


def _require_serialized_false_claims_v0(
    document: dict[str, object],
    fields: tuple[str, ...],
    label: str,
) -> None:
    for field_name in fields:
        if document.get(field_name) is not False:
            raise D1HistoricalDevelopmentContractErrorV0(
                f"{label} false claim differs: {field_name}"
            )


def _validate_serialized_summary_nested_v0(
    summary: dict[str, object],
    episode_count: int,
    evaluable_count: int,
) -> None:
    fee_aggregates = summary.get("fee_aggregates")
    if not isinstance(fee_aggregates, list) or len(fee_aggregates) != len(_FEE_CELLS):
        raise D1HistoricalDevelopmentContractErrorV0("serialized summary fee aggregates differ")
    for aggregate in fee_aggregates:
        if (
            not isinstance(aggregate, dict)
            or set(aggregate) != _SERIALIZED_FEE_AGGREGATE_KEYS_V0
            or aggregate.get("episode_count") != episode_count
            or aggregate.get("evaluable_episode_count") != evaluable_count
        ):
            raise D1HistoricalDevelopmentContractErrorV0(
                "serialized summary fee aggregate schema/count differs"
            )
    breakdowns = summary.get("breakdowns")
    if not isinstance(breakdowns, list) or any(
        not isinstance(value, dict) or set(value) != _SERIALIZED_BREAKDOWN_KEYS_V0
        for value in breakdowns
    ):
        raise D1HistoricalDevelopmentContractErrorV0("serialized summary breakdown schema differs")


_SERIALIZED_PROJECTION_KEYS_V0: Final = frozenset(
    {
        "executable_return_before_fee_funding",
        "fee_multiplier",
        "fee_rate_per_side",
        "fee_return",
        "funding_return",
        "gross_return",
        "net_return",
        "notional_usdt",
        "projected_net_pnl_usdt",
        "schema_version",
        "sizing_projection_creates_new_statistical_unit",
        "slippage_return",
        "statistical_unit_id",
    }
)
_SERIALIZED_EPISODE_KEYS_V0: Final = frozenset(
    {
        "efficacy_claim",
        "entry_executable_price",
        "entry_reference_price",
        "entry_reference_time_ms",
        "episode_sha256",
        "execution_conclusive",
        "exit_decision_event_id",
        "exit_decision_payload_sha256",
        "exit_executable_price",
        "exit_observation_close_ms",
        "exit_observation_open_ms",
        "exit_reason",
        "exit_reference_price",
        "exit_reference_time_ms",
        "five_minute_manifest_sha256",
        "funding_evaluable",
        "funding_event_count",
        "funding_file_sha256",
        "funding_inconclusive_reason",
        "historical_bbo_available",
        "historical_receipt_proxy",
        "hourly_manifest_sha256",
        "paper_fill_claim",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "projections",
        "prospective",
        "schema_version",
        "side",
        "signal_bar_open_ms",
        "signal_decision_cutoff_ms",
        "signal_event_id",
        "signal_payload_sha256",
        "statistical_unit_id",
        "status",
        "symbol",
    }
)
_SERIALIZED_CENSOR_KEYS_V0: Final = frozenset(
    {
        "censor_sha256",
        "contributes_statistical_n",
        "reason",
        "schema_version",
        "signal_bar_open_ms",
        "signal_event_id",
        "stage",
        "status",
        "symbol",
    }
)
_SERIALIZED_FEE_AGGREGATE_KEYS_V0: Final = frozenset(
    {
        "episode_count",
        "evaluable_episode_count",
        "fee_multiplier",
        "fee_rate_per_side",
        "mean_net_return",
        "net_after_top_ten_episodes",
        "net_after_top_three_symbols",
        "positive_symbol_count",
        "profit_factor",
        "profit_factor_infinite",
        "projected_total_pnl_1000_usdt",
        "projected_total_pnl_100_usdt",
        "total_net_return",
    }
)
_SERIALIZED_BREAKDOWN_KEYS_V0: Final = frozenset(
    {
        "episode_count",
        "evaluable_episode_count",
        "fee_multiplier",
        "fee_rate_per_side",
        "key",
        "kind",
        "mean_fee_return",
        "mean_funding_return",
        "mean_gross_return",
        "mean_net_return",
        "mean_slippage_return",
        "median_net_return",
        "positive_episode_count",
        "profit_factor",
        "profit_factor_infinite",
        "projected_total_pnl_1000_usdt",
        "projected_total_pnl_100_usdt",
        "strict_positive_hit_rate",
        "total_net_return",
    }
)
_SERIALIZED_SUMMARY_KEYS_V0: Final = frozenset(
    {
        "active_utc_day_count",
        "bootstrap_performed",
        "breakdowns",
        "disposition",
        "efficacy_claim",
        "entered_position_count",
        "entry_distance_rejection_count",
        "episode_count",
        "evaluable_episode_count",
        "evaluable_long_episode_count",
        "evaluable_short_episode_count",
        "exit_reason_counts",
        "fee_aggregates",
        "full_signal_count",
        "funding_coverage_status_by_symbol",
        "funding_inconclusive_counts",
        "global_correlation_guard",
        "global_nonoverlap_evaluable_count",
        "invalid_input_inconclusive_count",
        "long_episode_count",
        "pending_or_active_suppressed_signal_count",
        "prefilter_candidate_count",
        "prefilter_necessary_gate_false_count",
        "probability_claim",
        "projection_cells_multiply_n",
        "promoting",
        "right_edge_censor_count",
        "schema_version",
        "short_episode_count",
        "statistical_unit",
        "status",
        "summary_sha256",
    }
)
_SERIALIZED_RESULT_KEYS_V0: Final = frozenset(
    {
        "censor_count",
        "censor_sequence_root_sha256",
        "code_freeze_manifest_sha256",
        "code_freeze_receipt_sha256",
        "development_end_ms_exclusive",
        "development_start_ms",
        "efficacy_claim",
        "episode_count",
        "episode_sequence_root_sha256",
        "execution_conclusive",
        "existing_result_artifact_used_as_input",
        "historical_bbo_available",
        "historical_receipt_convention",
        "input_authority_sha256",
        "paper_fill_claim",
        "post_development_end_rows_used",
        "preregistration_sha256",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
        "result_sha256",
        "rule_version",
        "run_id",
        "run_started_at_ms",
        "schema_version",
        "summary_sha256",
        "universe",
    }
)


def _projection_document(value: D1HistoricalProjectionCellV0) -> dict[str, object]:
    return {
        "executable_return_before_fee_funding": str(value.executable_return_before_fee_funding),
        "fee_multiplier": str(value.fee_multiplier),
        "fee_rate_per_side": str(value.fee_rate_per_side),
        "fee_return": str(value.fee_return),
        "funding_return": _decimal_or_none(value.funding_return),
        "gross_return": str(value.gross_return),
        "net_return": _decimal_or_none(value.net_return),
        "notional_usdt": str(value.notional_usdt),
        "projected_net_pnl_usdt": _decimal_or_none(value.projected_net_pnl_usdt),
        "schema_version": value.schema_version,
        "sizing_projection_creates_new_statistical_unit": (
            value.sizing_projection_creates_new_statistical_unit
        ),
        "slippage_return": str(value.slippage_return),
        "statistical_unit_id": value.statistical_unit_id,
    }


def _episode_document(
    value: D1HistoricalEpisodeV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "efficacy_claim": value.efficacy_claim,
        "entry_executable_price": str(value.entry_executable_price),
        "entry_reference_price": str(value.entry_reference_price),
        "entry_reference_time_ms": value.entry_reference_time_ms,
        "execution_conclusive": value.execution_conclusive,
        "exit_decision_event_id": value.exit_decision_event_id,
        "exit_decision_payload_sha256": value.exit_decision_payload_sha256,
        "exit_executable_price": str(value.exit_executable_price),
        "exit_observation_close_ms": value.exit_observation_close_ms,
        "exit_observation_open_ms": value.exit_observation_open_ms,
        "exit_reason": value.exit_reason.value,
        "exit_reference_price": str(value.exit_reference_price),
        "exit_reference_time_ms": value.exit_reference_time_ms,
        "five_minute_manifest_sha256": value.five_minute_manifest_sha256,
        "funding_evaluable": value.funding_evaluable,
        "funding_event_count": value.funding_event_count,
        "funding_file_sha256": value.funding_file_sha256,
        "funding_inconclusive_reason": (
            None
            if value.funding_inconclusive_reason is None
            else value.funding_inconclusive_reason.value
        ),
        "historical_bbo_available": value.historical_bbo_available,
        "historical_receipt_proxy": value.historical_receipt_proxy,
        "hourly_manifest_sha256": value.hourly_manifest_sha256,
        "paper_fill_claim": value.paper_fill_claim,
        "probability_claim": value.probability_claim,
        "production_order_placement": value.production_order_placement,
        "promoting": value.promoting,
        "projections": [_projection_document(item) for item in value.projections],
        "prospective": value.prospective,
        "schema_version": value.schema_version,
        "side": value.side.value,
        "signal_bar_open_ms": value.signal_bar_open_ms,
        "signal_decision_cutoff_ms": value.signal_decision_cutoff_ms,
        "signal_event_id": value.signal_event_id,
        "signal_payload_sha256": value.signal_payload_sha256,
        "statistical_unit_id": value.statistical_unit_id,
        "status": value.status,
        "symbol": value.symbol,
    }
    if include_hash:
        document["episode_sha256"] = value.episode_sha256
    return document


def _censor_document(
    value: D1HistoricalCensorV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "contributes_statistical_n": value.contributes_statistical_n,
        "reason": value.reason,
        "schema_version": value.schema_version,
        "signal_bar_open_ms": value.signal_bar_open_ms,
        "signal_event_id": value.signal_event_id,
        "stage": value.stage.value,
        "status": value.status,
        "symbol": value.symbol,
    }
    if include_hash:
        document["censor_sha256"] = value.censor_sha256
    return document


def _fee_aggregate_document(value: D1HistoricalFeeAggregateV0) -> dict[str, object]:
    return {
        "episode_count": value.episode_count,
        "evaluable_episode_count": value.evaluable_episode_count,
        "fee_multiplier": str(value.fee_multiplier),
        "fee_rate_per_side": str(value.fee_rate_per_side),
        "mean_net_return": _decimal_or_none(value.mean_net_return),
        "net_after_top_ten_episodes": _decimal_or_none(value.net_after_top_ten_episodes),
        "net_after_top_three_symbols": _decimal_or_none(value.net_after_top_three_symbols),
        "positive_symbol_count": value.positive_symbol_count,
        "profit_factor": _decimal_or_none(value.profit_factor),
        "profit_factor_infinite": value.profit_factor_infinite,
        "projected_total_pnl_1000_usdt": _decimal_or_none(value.projected_total_pnl_1000_usdt),
        "projected_total_pnl_100_usdt": _decimal_or_none(value.projected_total_pnl_100_usdt),
        "total_net_return": _decimal_or_none(value.total_net_return),
    }


def _breakdown_document(value: D1HistoricalBreakdownV0) -> dict[str, object]:
    return {
        "episode_count": value.episode_count,
        "evaluable_episode_count": value.evaluable_episode_count,
        "fee_multiplier": str(value.fee_multiplier),
        "fee_rate_per_side": str(value.fee_rate_per_side),
        "key": value.key,
        "kind": value.kind.value,
        "mean_fee_return": _decimal_or_none(value.mean_fee_return),
        "mean_funding_return": _decimal_or_none(value.mean_funding_return),
        "mean_gross_return": _decimal_or_none(value.mean_gross_return),
        "mean_net_return": _decimal_or_none(value.mean_net_return),
        "mean_slippage_return": _decimal_or_none(value.mean_slippage_return),
        "median_net_return": _decimal_or_none(value.median_net_return),
        "positive_episode_count": value.positive_episode_count,
        "profit_factor": _decimal_or_none(value.profit_factor),
        "profit_factor_infinite": value.profit_factor_infinite,
        "projected_total_pnl_1000_usdt": _decimal_or_none(value.projected_total_pnl_1000_usdt),
        "projected_total_pnl_100_usdt": _decimal_or_none(value.projected_total_pnl_100_usdt),
        "strict_positive_hit_rate": _decimal_or_none(value.strict_positive_hit_rate),
        "total_net_return": _decimal_or_none(value.total_net_return),
    }


def _summary_document(
    value: D1HistoricalDevelopmentSummaryV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "active_utc_day_count": value.active_utc_day_count,
        "bootstrap_performed": value.bootstrap_performed,
        "breakdowns": [_breakdown_document(item) for item in value.breakdowns],
        "disposition": value.disposition.value,
        "efficacy_claim": value.efficacy_claim,
        "entered_position_count": value.entered_position_count,
        "entry_distance_rejection_count": value.entry_distance_rejection_count,
        "episode_count": value.episode_count,
        "evaluable_episode_count": value.evaluable_episode_count,
        "evaluable_long_episode_count": value.evaluable_long_episode_count,
        "evaluable_short_episode_count": value.evaluable_short_episode_count,
        "exit_reason_counts": [list(item) for item in value.exit_reason_counts],
        "fee_aggregates": [_fee_aggregate_document(item) for item in value.fee_aggregates],
        "full_signal_count": value.full_signal_count,
        "funding_coverage_status_by_symbol": [
            list(item) for item in value.funding_coverage_status_by_symbol
        ],
        "funding_inconclusive_counts": [list(item) for item in value.funding_inconclusive_counts],
        "global_correlation_guard": value.global_correlation_guard,
        "global_nonoverlap_evaluable_count": value.global_nonoverlap_evaluable_count,
        "invalid_input_inconclusive_count": value.invalid_input_inconclusive_count,
        "long_episode_count": value.long_episode_count,
        "pending_or_active_suppressed_signal_count": (
            value.pending_or_active_suppressed_signal_count
        ),
        "prefilter_candidate_count": value.prefilter_candidate_count,
        "prefilter_necessary_gate_false_count": (value.prefilter_necessary_gate_false_count),
        "probability_claim": value.probability_claim,
        "projection_cells_multiply_n": value.projection_cells_multiply_n,
        "promoting": value.promoting,
        "right_edge_censor_count": value.right_edge_censor_count,
        "schema_version": value.schema_version,
        "short_episode_count": value.short_episode_count,
        "statistical_unit": value.statistical_unit,
        "status": value.status,
    }
    if include_hash:
        document["summary_sha256"] = value.summary_sha256
    return document


def _result_document(
    value: D1HistoricalDevelopmentResultV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "censor_count": len(value.censors),
        "censor_sequence_root_sha256": _ordered_hash_root(
            _CENSOR_SEQUENCE_ROOT_DOMAIN,
            tuple(item.censor_sha256 for item in value.censors),
        ),
        "code_freeze_manifest_sha256": value.code_freeze_manifest_sha256,
        "code_freeze_receipt_sha256": value.code_freeze_receipt_sha256,
        "development_end_ms_exclusive": value.development_end_ms_exclusive,
        "development_start_ms": value.development_start_ms,
        "efficacy_claim": value.efficacy_claim,
        "episode_count": len(value.episodes),
        "episode_sequence_root_sha256": _ordered_hash_root(
            _EPISODE_SEQUENCE_ROOT_DOMAIN,
            tuple(item.episode_sha256 for item in value.episodes),
        ),
        "execution_conclusive": value.execution_conclusive,
        "existing_result_artifact_used_as_input": (value.existing_result_artifact_used_as_input),
        "historical_bbo_available": value.historical_bbo_available,
        "historical_receipt_convention": value.historical_receipt_convention,
        "input_authority_sha256": value.input_authority_sha256,
        "paper_fill_claim": value.paper_fill_claim,
        "post_development_end_rows_used": value.post_development_end_rows_used,
        "preregistration_sha256": value.preregistration_sha256,
        "probability_claim": value.probability_claim,
        "production_order_placement": value.production_order_placement,
        "promoting": value.promoting,
        "prospective": value.prospective,
        "rule_version": value.rule_version,
        "run_id": value.run_id,
        "run_started_at_ms": value.run_started_at_ms,
        "schema_version": value.schema_version,
        "summary_sha256": value.summary.summary_sha256,
        "universe": list(value.universe),
    }
    if include_hash:
        document["result_sha256"] = value.result_sha256
    return document


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _ordered_hash_root(domain: bytes, values: tuple[str, ...]) -> str:
    root = hashlib.sha256(domain + b"EMPTY").digest()
    for index, value in enumerate(values):
        _require_sha256(value, "ordered sequence member")
        root = hashlib.sha256(
            domain + root + index.to_bytes(8, byteorder="big", signed=False) + bytes.fromhex(value)
        ).digest()
    return root.hex()


def _ordered_canonical_bytes_root_v0(domain: bytes, values: tuple[bytes, ...]) -> str:
    """Root an exact ordered sequence without relying on JSON text coercion."""

    if type(domain) is not bytes or not domain:
        raise D1HistoricalDevelopmentContractErrorV0(
            "canonical byte-sequence domain must be nonempty bytes"
        )
    if type(values) is not tuple or any(type(value) is not bytes for value in values):
        raise D1HistoricalDevelopmentContractErrorV0(
            "canonical byte-sequence members must be an exact immutable bytes tuple"
        )
    root = hashlib.sha256(domain + b"EMPTY").digest()
    for index, value in enumerate(values):
        root = hashlib.sha256(
            domain
            + root
            + index.to_bytes(8, byteorder="big", signed=False)
            + len(value).to_bytes(8, byteorder="big", signed=False)
            + value
        ).digest()
    return root.hex()


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_symbol(value: object) -> str:
    if not isinstance(value, str) or value not in D1_HISTORICAL_UNIVERSE_V0:
        raise D1HistoricalDevelopmentContractErrorV0("symbol is outside the frozen D1 universe")
    return value


def _require_relative_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
        or "//" in value
        or _PATH_RE.fullmatch(value) is None
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise D1HistoricalDevelopmentContractErrorV0(
            f"{label} must be a normalized relative POSIX path"
        )
    return value


def _require_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be a nonnegative integer")
    return value


def _require_finite_decimal(value: object, label: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be a finite Decimal")
    return value


def _require_positive_decimal(value: object, label: str) -> Decimal:
    parsed = _require_finite_decimal(value, label)
    if parsed <= 0:
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be positive")
    return parsed


def _require_identity(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 0x20 for character in value)
    ):
        raise D1HistoricalDevelopmentContractErrorV0(f"{label} must be nonempty normalized text")
    return value


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
