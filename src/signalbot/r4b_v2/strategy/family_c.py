from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from threading import RLock
from typing import Final, TypedDict

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryStatusV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_entry_decision_v2,
    canonical_paper_fok_full_fill_certificate_v2,
    issue_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.protocol import decision_clock as _decision_clock
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

FAMILY_C_RULE_VERSION_V2 = "R4B_CAUSAL_V2.2.0_FAMILY_C"
FAMILY_C_PRIOR_WINDOW_V2 = 8_640
FAMILY_C_PANEL_BAR_COUNT_V2 = FAMILY_C_PRIOR_WINDOW_V2 + 4
FAMILY_C_MINIMUM_MEMBERS_V2 = 20
FAMILY_C_HARD_HORIZON_BARS_V2 = 6
FIVE_MINUTE_MS_V2 = _decision_clock.FIVE_MINUTE_MS_V2
DECISION_DELAY_MS_V2 = _decision_clock.DECISION_DELAY_MS_V2
UTC_DAY_MS_V2 = 86_400_000

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_BETA_MIN = Decimal("0.25")
_BETA_MAX = Decimal("2.5")
_MAD_SCALE = Decimal("1.4826")
_SHOCK_SCORE_MIN = Decimal("2.5")
_LAG_SCORE_MIN = Decimal("1.5")
_ENTRY_ID_DOMAIN = b"R4B_FAMILY_C_DECISION_V2\0"
_EXIT_ID_DOMAIN = b"R4B_FAMILY_C_EXIT_V2\0"
_ENTRY_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_C_ENTRY_PAYLOAD_V2\0"
_EXIT_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_C_EXIT_PAYLOAD_V2\0"
_FEATURE_HASH_DOMAIN: Final = b"R4B_FAMILY_C_FEATURE_EVIDENCE_V2\0"
_UNIVERSE_ROOT_DOMAIN: Final = b"R4B_FAMILY_C_PRIOR_UNIVERSE_V2\0"
_CANDLE_SLICE_DOMAIN: Final = b"R4B_FAMILY_C_CANDLE_SLICE_V2\0"
_PANEL_ROOT_DOMAIN: Final = b"R4B_FAMILY_C_PANEL_ROOT_V2\0"
_EXIT_SOURCE_ROOT_DOMAIN: Final = b"R4B_FAMILY_C_EXIT_SOURCE_ROOT_V2\0"
_ENTRY_INPUT_DOMAIN: Final = b"R4B_FAMILY_C_ENTRY_INPUT_V2\0"
_EXIT_INPUT_DOMAIN: Final = b"R4B_FAMILY_C_EXIT_INPUT_V2\0"
_POSITION_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_C_POSITION_PAYLOAD_V2\0"
_LEDGER_ROOT_DOMAIN: Final = b"R4B_FAMILY_C_EPISODE_LEDGER_V2\0"
_EPISODE_STATE_SCHEMA_V2: Final = "r4b_family_c_episode_state_v2"
_FEATURE_FACTORY_TOKEN: Final = object()
_DECISION_FACTORY_TOKEN: Final = object()
_ENTRY_PREVIEW_FACTORY_TOKEN: Final = object()
_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN: Final = object()
_EXIT_INPUT_FACTORY_TOKEN: Final = object()
_POSITION_FACTORY_TOKEN: Final = object()
_ADMISSION_RECEIPT_FACTORY_TOKEN: Final = object()
_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN: Final = object()


class FamilyCContractError(ValueError):
    """Raised when a caller violates an immutable Family C contract."""


class FamilyCFeatureStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_HISTORY = "FEATURE_NOT_READY_HISTORY"
    FEATURE_NOT_READY_ZERO_MARKET_VARIANCE = "FEATURE_NOT_READY_ZERO_MARKET_VARIANCE"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    INCONCLUSIVE_CROSS_SECTION = "INCONCLUSIVE_CROSS_SECTION"
    DATA_INVALID = "DATA_INVALID"


class FamilyCSideV2(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class FamilyCEntryStatusV2(StrEnum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    NOT_ADMITTED_ACTIVE_POSITION = "NOT_ADMITTED_ACTIVE_POSITION"
    FEATURE_NOT_READY_HISTORY = "FEATURE_NOT_READY_HISTORY"
    FEATURE_NOT_READY_ZERO_MARKET_VARIANCE = "FEATURE_NOT_READY_ZERO_MARKET_VARIANCE"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    INCONCLUSIVE_CROSS_SECTION = "INCONCLUSIVE_CROSS_SECTION"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"


class FamilyCExitActionV2(StrEnum):
    HOLD = "HOLD"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"


class FamilyCExitReasonV2(StrEnum):
    HOLD = "HOLD"
    MISSING_MEMBER_INCONCLUSIVE = "MISSING_MEMBER_INCONCLUSIVE"
    MANDATORY_DATA_EMERGENCY = "MANDATORY_DATA_EMERGENCY"
    MANDATORY_TERMINAL_EMERGENCY = "MANDATORY_TERMINAL_EMERGENCY"
    ADVERSE_WIDENING = "ADVERSE_WIDENING"
    CATCHUP_COMPLETE = "CATCHUP_COMPLETE"
    HARD_HORIZON = "HARD_HORIZON"


class FamilyCIntervalStatusV2(StrEnum):
    COMPLETE = "COMPLETE"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"


class FamilyCMandatoryExitV2(StrEnum):
    DATA = "DATA"
    TERMINAL = "TERMINAL"


class FamilyCRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


class FamilyCEntryCommitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyCAdmissionDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyCExitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class _FamilyCExitProvenanceV2(TypedDict):
    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    episode_ledger_root_sha256: str
    exit_source_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int


@dataclass(frozen=True, slots=True)
class FamilyCPriorUniverseV2:
    """Prior-only entry-day universe bound to plan and capture lineage."""

    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    effective_day_start_ms: int
    eligibility_cutoff_ms: int
    members: tuple[str, ...]
    universe_root_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C universe requires USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(self.source_root_sha256, "source_root_sha256")
        _validate_nonnegative_int(
            self.effective_day_start_ms,
            "effective_day_start_ms",
        )
        if self.effective_day_start_ms % UTC_DAY_MS_V2 != 0:
            raise FamilyCContractError("universe day must align to 00:00 UTC")
        _validate_nonnegative_int(
            self.eligibility_cutoff_ms,
            "eligibility_cutoff_ms",
        )
        if self.eligibility_cutoff_ms >= self.effective_day_start_ms:
            raise FamilyCContractError(
                "daily eligibility cutoff must be strictly prior to entry day"
            )
        normalized = _normalized_member_set(self.members)
        object.__setattr__(self, "members", normalized)
        root = hashlib.sha256(
            _UNIVERSE_ROOT_DOMAIN
            + canonical_json_line(
                {
                    "effective_day_start_ms": self.effective_day_start_ms,
                    "eligibility_cutoff_ms": self.eligibility_cutoff_ms,
                    "members": list(normalized),
                    "promoting_plan_sha256": self.promoting_plan_sha256,
                    "schema_version": "r4b_family_c_prior_universe_v2",
                    "source_root_sha256": self.source_root_sha256,
                    "venue": self.venue.value,
                }
            )
        ).hexdigest()
        object.__setattr__(self, "universe_root_sha256", root)


@dataclass(frozen=True, slots=True)
class FamilyCClosedCandleV2:
    """One fully closed 5m candle with event/receipt and raw-record lineage."""

    symbol: str
    bar_open_ms: int
    bar_close_ms: int
    event_time_ms: int
    receipt_time_ms: int
    close: Decimal
    source_evidence_sha256: str
    closed: bool = True

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        _validate_nonnegative_int(self.bar_open_ms, "bar_open_ms")
        _validate_nonnegative_int(self.bar_close_ms, "bar_close_ms")
        if self.bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyCContractError("candle must align to a 5m UTC boundary")
        if self.bar_close_ms != self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
            raise FamilyCContractError("candle close time differs from its 5m slot")
        _validate_nonnegative_int(self.event_time_ms, "event_time_ms")
        _validate_nonnegative_int(self.receipt_time_ms, "receipt_time_ms")
        if type(self.closed) is not bool or not self.closed:
            raise FamilyCContractError("Family C raw panel accepts closed candles only")
        if not self.bar_open_ms <= self.event_time_ms <= self.bar_close_ms:
            raise FamilyCContractError("candle event after k.T or before k.t is forbidden")
        if self.receipt_time_ms < self.event_time_ms:
            raise FamilyCContractError("candle receipt cannot precede source event")
        if not _is_positive_finite(self.close):
            raise FamilyCContractError("candle close must be positive finite Decimal")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")


@dataclass(frozen=True, slots=True)
class FamilyCCandlePanelV2:
    """Exact member-complete t-8643..t closed-candle panel for Family C."""

    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe: FamilyCPriorUniverseV2
    current_bar_open_ms: int
    current_bar_close_ms: int
    decision_cutoff_ms: int
    candles: tuple[FamilyCClosedCandleV2, ...]
    symbol_slice_sha256s: tuple[tuple[str, str], ...] = field(init=False)
    panel_root_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C candle panel requires USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(self.source_root_sha256, "source_root_sha256")
        if not isinstance(self.universe, FamilyCPriorUniverseV2):
            raise FamilyCContractError("universe must be FamilyCPriorUniverseV2")
        canonical_family_c_prior_universe_v2(self.universe)
        if (
            self.venue,
            self.promoting_plan_sha256,
            self.source_root_sha256,
        ) != (
            self.universe.venue,
            self.universe.promoting_plan_sha256,
            self.universe.source_root_sha256,
        ):
            raise FamilyCContractError("panel lineage differs from prior-only universe")
        _validate_bar_times(
            self.current_bar_open_ms,
            self.current_bar_close_ms,
            self.decision_cutoff_ms,
        )
        day_start = self.current_bar_open_ms // UTC_DAY_MS_V2 * UTC_DAY_MS_V2
        if day_start != self.universe.effective_day_start_ms:
            raise FamilyCContractError("panel bar is outside its entry-day universe")
        if type(self.candles) is not tuple:
            raise FamilyCContractError("candles must be an immutable tuple")
        if any(not isinstance(item, FamilyCClosedCandleV2) for item in self.candles):
            raise FamilyCContractError("candles contains an unsupported value")
        expected_count = len(self.universe.members) * FAMILY_C_PANEL_BAR_COUNT_V2
        if len(self.candles) != expected_count:
            raise FamilyCContractError(
                "panel requires exactly 8,644 candles for every universe member"
            )
        ordered = tuple(
            sorted(
                self.candles,
                key=lambda item: (_symbol_key(item.symbol), item.bar_open_ms),
            )
        )
        keys = tuple((item.symbol, item.bar_open_ms) for item in ordered)
        if len(set(keys)) != len(keys):
            raise FamilyCContractError("panel contains duplicate member candle slots")
        if {item.symbol for item in ordered} != set(self.universe.members):
            raise FamilyCContractError("panel cannot drop or add a universe member")
        first_open_ms = self.current_bar_open_ms - (
            (FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
        )
        expected_opens = tuple(
            first_open_ms + index * FIVE_MINUTE_MS_V2
            for index in range(FAMILY_C_PANEL_BAR_COUNT_V2)
        )
        slice_hashes: list[tuple[str, str]] = []
        for symbol in self.universe.members:
            member_candles = tuple(item for item in ordered if item.symbol == symbol)
            if tuple(item.bar_open_ms for item in member_candles) != expected_opens:
                raise FamilyCContractError(
                    "member candle panel must be contiguous through the current bar"
                )
            if any(item.receipt_time_ms > self.decision_cutoff_ms for item in member_candles):
                raise FamilyCContractError("candle receipt after D is forbidden")
            slice_hash = hashlib.sha256(
                _CANDLE_SLICE_DOMAIN
                + canonical_json_line(
                    {
                        "rows": [_closed_candle_document(item) for item in member_candles],
                        "schema_version": "r4b_family_c_member_candle_slice_v2",
                        "symbol": symbol,
                    }
                )
            ).hexdigest()
            slice_hashes.append((symbol, slice_hash))
        object.__setattr__(self, "candles", ordered)
        object.__setattr__(self, "symbol_slice_sha256s", tuple(slice_hashes))
        panel_root = hashlib.sha256(
            _PANEL_ROOT_DOMAIN
            + canonical_json_line(
                {
                    "current_bar_close_ms": self.current_bar_close_ms,
                    "current_bar_open_ms": self.current_bar_open_ms,
                    "decision_cutoff_ms": self.decision_cutoff_ms,
                    "promoting_plan_sha256": self.promoting_plan_sha256,
                    "schema_version": "r4b_family_c_candle_panel_v2",
                    "source_root_sha256": self.source_root_sha256,
                    "symbol_slice_sha256s": [
                        {"sha256": digest, "symbol": symbol} for symbol, digest in slice_hashes
                    ],
                    "universe_root_sha256": self.universe.universe_root_sha256,
                    "venue": self.venue.value,
                }
            )
        ).hexdigest()
        object.__setattr__(self, "panel_root_sha256", panel_root)


@dataclass(frozen=True, slots=True)
class FamilyCRawMemberHistoryV2:
    """Outcome-free return inputs for one daily-eligible entry member."""

    symbol: str
    prior_one_bar_returns: tuple[Decimal | None, ...]
    prior_three_bar_returns: tuple[Decimal | None, ...]
    current_three_bar_return: Decimal | None

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        if type(self.prior_one_bar_returns) is not tuple:
            raise FamilyCContractError("prior_one_bar_returns must be an immutable tuple")
        if type(self.prior_three_bar_returns) is not tuple:
            raise FamilyCContractError("prior_three_bar_returns must be an immutable tuple")


@dataclass(frozen=True, slots=True)
class FamilyCPopulationBetaV2:
    covariance_pop: Decimal
    variance_pop: Decimal
    beta_raw: Decimal
    beta: Decimal

    def __post_init__(self) -> None:
        for field_name in ("covariance_pop", "variance_pop", "beta_raw", "beta"):
            if not _is_finite_decimal(getattr(self, field_name)):
                raise FamilyCContractError(f"{field_name} must be finite Decimal")
        if self.variance_pop <= 0:
            raise FamilyCContractError("population market variance must be positive")
        if self.beta != _clip_beta(self.beta_raw):
            raise FamilyCContractError("beta must be the frozen clipped beta_raw")


@dataclass(frozen=True, slots=True)
class FamilyCMemberFeatureV2:
    symbol: str
    beta_raw: Decimal
    beta: Decimal
    residual_scale: Decimal
    current_three_bar_return: Decimal
    g0: Decimal
    lag_score: Decimal

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        for field_name in (
            "beta_raw",
            "beta",
            "residual_scale",
            "current_three_bar_return",
            "g0",
            "lag_score",
        ):
            if not _is_finite_decimal(getattr(self, field_name)):
                raise FamilyCContractError(f"{field_name} must be finite Decimal")
        if self.beta != _clip_beta(self.beta_raw):
            raise FamilyCContractError("member beta must be clipped to [0.25, 2.5]")
        if self.residual_scale <= 0:
            raise FamilyCContractError("member residual_scale must be positive")
        with localcontext(protocol_decimal_context_v2()):
            if self.lag_score != self.g0 / self.residual_scale:
                raise FamilyCContractError("lag_score differs from g0 / residual_scale")


@dataclass(frozen=True, slots=True)
class _FamilyCFeatureBuildContextV2:
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    panel_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    current_closes: tuple[FamilyCSymbolCloseV2, ...]


@dataclass(frozen=True, slots=True)
class FamilyCFeatureSnapshotV2:
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    panel_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    current_closes: tuple[FamilyCSymbolCloseV2, ...]
    status: FamilyCFeatureStatusV2
    reasons: tuple[str, ...]
    member_set: tuple[str, ...]
    prior_observation_count: int
    m3_current: Decimal | None = None
    shock_scale: Decimal | None = None
    shock_score: Decimal | None = None
    breadth_count: int | None = None
    members: tuple[FamilyCMemberFeatureV2, ...] = ()
    _factory_token: InitVar[object] = None
    feature_evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FEATURE_FACTORY_TOKEN:
            raise FamilyCContractError(
                "Family C feature evidence must be created by its causal factory"
            )
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C features require USD-M Futures")
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "universe_root_sha256",
            "panel_root_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        if self.latest_source_event_ms > self.bar_close_ms:
            raise FamilyCContractError("future source event cannot enter features")
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise FamilyCContractError("source receipt after D cannot enter features")
        normalized_members = _normalized_member_set(self.member_set)
        object.__setattr__(self, "member_set", normalized_members)
        closes = _unique_closes(self.current_closes, "current_closes")
        if set(closes) != set(normalized_members):
            raise FamilyCContractError("current closes must exactly equal the feature member set")
        ordered_closes = tuple(
            FamilyCSymbolCloseV2(symbol, closes[symbol]) for symbol in normalized_members
        )
        object.__setattr__(self, "current_closes", ordered_closes)
        _validate_nonnegative_int(
            self.prior_observation_count,
            "prior_observation_count",
        )
        if not isinstance(self.status, FamilyCFeatureStatusV2):
            raise FamilyCContractError("status must be FamilyCFeatureStatusV2")
        if type(self.reasons) is not tuple or not self.reasons:
            raise FamilyCContractError("feature snapshot requires deterministic reasons")
        _validate_reasons(self.reasons)
        if self.status is not FamilyCFeatureStatusV2.READY:
            if (
                any(
                    value is not None
                    for value in (
                        self.m3_current,
                        self.shock_scale,
                        self.shock_score,
                        self.breadth_count,
                    )
                )
                or self.members
            ):
                raise FamilyCContractError(
                    "non-ready Family C snapshot cannot expose derived features"
                )
            _seal_feature_evidence(self)
            return
        if self.prior_observation_count != FAMILY_C_PRIOR_WINDOW_V2:
            raise FamilyCContractError("READY Family C snapshot requires exactly 8,640 prior rows")
        if not _is_finite_decimal(self.m3_current):
            raise FamilyCContractError("READY snapshot requires finite m3_current")
        if not _is_positive_finite(self.shock_scale):
            raise FamilyCContractError("READY snapshot requires positive shock_scale")
        if not _is_nonnegative_finite(self.shock_score):
            raise FamilyCContractError("READY snapshot requires nonnegative shock_score")
        assert self.m3_current is not None
        assert self.shock_scale is not None
        assert self.shock_score is not None
        with localcontext(protocol_decimal_context_v2()):
            if self.shock_score != abs(self.m3_current) / self.shock_scale:
                raise FamilyCContractError("shock_score differs from abs(m3) / shock_scale")
        if type(self.breadth_count) is not int or not (
            0 <= self.breadth_count <= len(self.member_set)
        ):
            raise FamilyCContractError("breadth_count escapes the frozen member set")
        if type(self.members) is not tuple:
            raise FamilyCContractError("members must be an immutable tuple")
        ordered = tuple(sorted(self.members, key=lambda item: _symbol_key(item.symbol)))
        if tuple(item.symbol for item in ordered) != self.member_set:
            raise FamilyCContractError(
                "READY member features must exactly equal the entry member set"
            )
        object.__setattr__(self, "members", ordered)
        shock_sign = _sign(self.m3_current)
        computed_breadth = sum(
            (shock_sign > 0 and item.current_three_bar_return > 0)
            or (shock_sign < 0 and item.current_three_bar_return < 0)
            for item in ordered
        )
        if self.breadth_count != computed_breadth:
            raise FamilyCContractError("breadth_count differs from member-complete signs")
        for item in ordered:
            with localcontext(protocol_decimal_context_v2()):
                expected_g0 = Decimal(shock_sign) * (
                    item.beta * self.m3_current - item.current_three_bar_return
                )
            if item.g0 != expected_g0:
                raise FamilyCContractError("member g0 differs from the frozen formula")
        _seal_feature_evidence(self)


@dataclass(frozen=True, slots=True)
class FamilyCEntryInputV2:
    attempt_id: str
    target_symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    features: FamilyCFeatureSnapshotV2
    causal_input_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.target_symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C entry requires USD-M Futures")
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "universe_root_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if not isinstance(self.features, FamilyCFeatureSnapshotV2):
            raise FamilyCContractError("features must be a FamilyCFeatureSnapshotV2")
        canonical_family_c_feature_evidence_v2(self.features)
        evidence_identity = (
            self.features.venue,
            self.features.promoting_plan_sha256,
            self.features.source_root_sha256,
            self.features.universe_root_sha256,
            self.features.bar_open_ms,
            self.features.bar_close_ms,
            self.features.decision_cutoff_ms,
        )
        input_identity = (
            self.venue,
            self.promoting_plan_sha256,
            self.source_root_sha256,
            self.universe_root_sha256,
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if evidence_identity != input_identity:
            raise FamilyCContractError("entry identity differs from its bound feature evidence")
        object.__setattr__(self, "causal_input_sha256", _entry_input_sha256(self))

    @property
    def closed_bar(self) -> bool:
        return True

    @property
    def causal_inputs_complete(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class FamilyCEntryDecisionV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    feature_evidence_sha256: str
    episode_ledger_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    status: FamilyCEntryStatusV2
    side: FamilyCSideV2 | None
    reasons: tuple[str, ...]
    invalidation: str
    selected_rank: int | None
    beta: Decimal | None
    m3: Decimal | None
    r_i3: Decimal | None
    g0: Decimal | None
    entry_member_set: tuple[str, ...]
    symbol_order: tuple[str, ...]
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_C_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise FamilyCContractError("Family C entry decisions must be created by the evaluator")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C entry decision requires USD-M Futures")
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "universe_root_sha256",
            "feature_evidence_sha256",
            "episode_ledger_root_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_entry_decision_state(self)
        event_id = hashlib.sha256(
            _ENTRY_ID_DOMAIN + canonical_json_line(_entry_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _ENTRY_PAYLOAD_DOMAIN
            + canonical_json_line(_entry_decision_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def emitted_signal(self) -> bool:
        return self.status is FamilyCEntryStatusV2.SIGNAL


@dataclass(frozen=True, slots=True)
class FamilyCEntryPreviewV2:
    """Factory-sealed, non-mutating snapshot for one transactional entry."""

    input_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    decision: FamilyCEntryDecisionV2
    already_committed: bool
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_PREVIEW_FACTORY_TOKEN:
            raise FamilyCContractError("Family C entry previews must be created by the ledger")
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.pre_root_sha256, "pre_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        if not isinstance(self.decision, FamilyCEntryDecisionV2):
            raise FamilyCContractError("preview decision must be FamilyCEntryDecisionV2")
        canonical_family_c_entry_decision_v2(self.decision)
        if type(self.already_committed) is not bool:
            raise FamilyCContractError("already_committed must be boolean")
        if (
            not self.already_committed
            and self.decision.episode_ledger_root_sha256 != self.pre_root_sha256
        ):
            raise FamilyCContractError("new Family C preview decision must bind its pre-root")


@dataclass(frozen=True, slots=True)
class FamilyCEntryCommitReceiptV2:
    """Ephemeral capability proving which transaction created an entry."""

    input_sha256: str
    event_id: str
    decision: FamilyCEntryDecisionV2
    preview_already_committed: bool
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyCEntryCommitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN:
            raise FamilyCContractError(
                "Family C entry commit receipts must be created by the ledger"
            )
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.event_id, "event_id")
        _validate_sha256(self.pre_root_sha256, "pre_root_sha256")
        _validate_sha256(self.post_root_sha256, "post_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        _validate_nonnegative_int(self.post_event_count, "post_event_count")
        if not isinstance(self.decision, FamilyCEntryDecisionV2):
            raise FamilyCContractError("receipt decision must be FamilyCEntryDecisionV2")
        canonical_family_c_entry_decision_v2(self.decision)
        if self.event_id != self.decision.event_id:
            raise FamilyCContractError("receipt event differs from its decision")
        if type(self.preview_already_committed) is not bool:
            raise FamilyCContractError("preview_already_committed must be boolean")
        if not isinstance(self.disposition, FamilyCEntryCommitDispositionV2):
            raise FamilyCContractError("disposition must be FamilyCEntryCommitDispositionV2")
        if self.disposition is FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
            if self.preview_already_committed:
                raise FamilyCContractError("pre-existing preview cannot claim a new commit")
            if (
                self.post_event_count != self.pre_event_count + 1
                or self.post_root_sha256 == self.pre_root_sha256
            ):
                raise FamilyCContractError("new commit receipt has invalid post-state")
            return
        if self.preview_already_committed:
            if (
                self.post_event_count != self.pre_event_count
                or self.post_root_sha256 != self.pre_root_sha256
            ):
                raise FamilyCContractError("pre-existing replay receipt must preserve its state")
            return
        if (
            self.post_event_count != self.pre_event_count + 1
            or self.post_root_sha256 == self.pre_root_sha256
        ):
            raise FamilyCContractError("concurrent pre-existing receipt has invalid post-state")


@dataclass(frozen=True, slots=True)
class FamilyCPositionV2:
    """Frozen rule state admitted only by a registry-pinned full PAPER fill."""

    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    feature_evidence_sha256: str
    entry_ledger_root_sha256: str
    admission_evidence_sha256: str
    paper_decision_event_id: str
    paper_decision_payload_sha256: str
    paper_registry_root_sha256: str
    paper_registry_event_count: int
    paper_registry_checkpoint_sha256: str
    paper_requested_quantity: Decimal
    paper_filled_quantity: Decimal
    paper_executable_notional: Decimal
    entry_vwap: Decimal
    side: FamilyCSideV2
    signal_bar_open_ms: int
    beta: Decimal
    m3: Decimal
    r_i3: Decimal
    g0: Decimal
    entry_member_set: tuple[str, ...]
    symbol_order: tuple[str, ...]
    entry_member_closes: tuple[FamilyCSymbolCloseV2, ...]
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise FamilyCContractError(
                "Family C position requires a registry-pinned full PAPER fill"
            )
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C position requires USD-M Futures")
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "universe_root_sha256",
            "feature_evidence_sha256",
            "entry_ledger_root_sha256",
            "admission_evidence_sha256",
            "paper_decision_event_id",
            "paper_decision_payload_sha256",
            "paper_registry_root_sha256",
            "paper_registry_checkpoint_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_nonnegative_int(
            self.paper_registry_event_count,
            "paper_registry_event_count",
        )
        if self.paper_registry_event_count < 1:
            raise FamilyCContractError("paper registry checkpoint cannot be empty")
        if not all(
            _is_positive_finite(value)
            for value in (
                self.paper_requested_quantity,
                self.paper_filled_quantity,
                self.paper_executable_notional,
                self.entry_vwap,
            )
        ):
            raise FamilyCContractError(
                "PAPER quantities, VWAP, and executable notional must be positive finite"
            )
        if self.paper_requested_quantity != self.paper_filled_quantity:
            raise FamilyCContractError("position requires requested equals full fill")
        if not isinstance(self.side, FamilyCSideV2):
            raise FamilyCContractError("side must be LONG or SHORT")
        _validate_nonnegative_int(self.signal_bar_open_ms, "signal_bar_open_ms")
        if self.signal_bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyCContractError("signal bar must align to a 5m UTC boundary")
        for field_name in ("beta", "m3", "r_i3", "g0"):
            if not _is_finite_decimal(getattr(self, field_name)):
                raise FamilyCContractError(f"{field_name} must be finite Decimal")
        if self.beta < _BETA_MIN or self.beta > _BETA_MAX:
            raise FamilyCContractError("position beta escapes [0.25, 2.5]")
        if self.g0 <= 0:
            raise FamilyCContractError("selected Family C position requires positive g0")
        expected_side = FamilyCSideV2.LONG if self.m3 > 0 else FamilyCSideV2.SHORT
        if self.m3 == 0 or self.side is not expected_side:
            raise FamilyCContractError("position side differs from frozen m3 sign")
        normalized = _normalized_member_set(self.entry_member_set)
        object.__setattr__(self, "entry_member_set", normalized)
        if self.symbol not in normalized:
            raise FamilyCContractError("position symbol is absent from entry_member_set")
        if tuple(sorted(self.symbol_order, key=_symbol_key)) != normalized:
            raise FamilyCContractError("symbol_order must be a permutation of entry members")
        if len(set(self.symbol_order)) != len(self.symbol_order):
            raise FamilyCContractError("symbol_order cannot contain duplicates")
        closes = _unique_closes(self.entry_member_closes, "entry_member_closes")
        if set(closes) != set(normalized):
            raise FamilyCContractError(
                "position entry closes must equal the frozen entry member set"
            )
        object.__setattr__(
            self,
            "entry_member_closes",
            tuple(FamilyCSymbolCloseV2(symbol, closes[symbol]) for symbol in normalized),
        )


@dataclass(frozen=True, slots=True)
class FamilyCSymbolMoveV2:
    symbol: str
    log_move: Decimal

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        if not _is_finite_decimal(self.log_move):
            raise FamilyCContractError("log_move must be finite Decimal")


@dataclass(frozen=True, slots=True)
class FamilyCSymbolCloseV2:
    symbol: str
    close: Decimal

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        if not _is_positive_finite(self.close):
            raise FamilyCContractError("close must be positive finite Decimal")


@dataclass(frozen=True, slots=True)
class FamilyCExitInputV2:
    position: FamilyCPositionV2
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    mandatory_exit: FamilyCMandatoryExitV2 | None
    member_moves: tuple[FamilyCSymbolMoveV2, ...]
    exit_source_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    _factory_token: InitVar[object] = None
    causal_input_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _EXIT_INPUT_FACTORY_TOKEN:
            raise FamilyCContractError(
                "Family C exit input must be created by its causal candle factory"
            )
        if not isinstance(self.position, FamilyCPositionV2):
            raise FamilyCContractError("position must be a FamilyCPositionV2")
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_sha256(self.exit_source_root_sha256, "exit_source_root_sha256")
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        if self.latest_source_event_ms > self.bar_close_ms:
            raise FamilyCContractError("future exit source event is forbidden")
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise FamilyCContractError("exit source receipt after D is forbidden")
        if self.mandatory_exit is not None and not isinstance(
            self.mandatory_exit, FamilyCMandatoryExitV2
        ):
            raise FamilyCContractError("mandatory_exit has an unsupported value")
        if type(self.member_moves) is not tuple:
            raise FamilyCContractError("member_moves must be an immutable tuple")
        if any(not isinstance(item, FamilyCSymbolMoveV2) for item in self.member_moves):
            raise FamilyCContractError("member_moves contains an unsupported value")
        symbols = tuple(item.symbol for item in self.member_moves)
        if len(set(symbols)) != len(symbols):
            raise FamilyCContractError("member_moves cannot contain duplicate symbols")
        if self.bar_open_ms <= self.position.signal_bar_open_ms:
            raise FamilyCContractError("exit evaluation must follow the signal bar")
        object.__setattr__(self, "causal_input_sha256", _exit_input_sha256(self))

    @property
    def closed_bar(self) -> bool:
        return True

    @property
    def causal_inputs_complete(self) -> bool:
        return True

    @property
    def horizon_bars(self) -> int:
        return (self.bar_open_ms - self.position.signal_bar_open_ms) // FIVE_MINUTE_MS_V2


@dataclass(frozen=True, slots=True)
class FamilyCExitDecisionV2:
    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    universe_root_sha256: str
    episode_ledger_root_sha256: str
    exit_source_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    action: FamilyCExitActionV2
    reason: FamilyCExitReasonV2
    reasons: tuple[str, ...]
    invalidation: str
    interval_status: FamilyCIntervalStatusV2
    asset_move: Decimal | None = None
    market_move: Decimal | None = None
    catch_h: Decimal | None = None
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_C_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise FamilyCContractError("Family C exit decisions must be created by the evaluator")
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyCContractError("Family C exit decision requires USD-M Futures")
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "universe_root_sha256",
            "episode_ledger_root_sha256",
            "exit_source_root_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_exit_decision_state(self)
        event_id = hashlib.sha256(
            _EXIT_ID_DOMAIN + canonical_json_line(_exit_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _EXIT_PAYLOAD_DOMAIN
            + canonical_json_line(_exit_decision_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def exits_position(self) -> bool:
        return self.action is not FamilyCExitActionV2.HOLD


@dataclass(frozen=True, slots=True)
class FamilyCAdmissionReceiptV2:
    """Ephemeral exact-owner proof for one PAPER-backed position admission."""

    input_sha256: str
    decision: FamilyCEntryDecisionV2
    position: FamilyCPositionV2
    position_sha256: str
    paper_decision: PaperFokEntryDecisionV2
    paper_certificate: PaperFokFullFillCertificateV2
    paper_registry_root_sha256: str
    paper_registry_event_count: int
    paper_registry_maximum_events: int
    paper_registry_checkpoint_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyCAdmissionDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise FamilyCContractError("Family C admission receipts must be created by the ledger")
        for value, field_name in (
            (self.input_sha256, "input_sha256"),
            (self.position_sha256, "position_sha256"),
            (self.paper_registry_root_sha256, "paper_registry_root_sha256"),
            (
                self.paper_registry_checkpoint_sha256,
                "paper_registry_checkpoint_sha256",
            ),
            (self.pre_root_sha256, "pre_root_sha256"),
            (self.post_root_sha256, "post_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        for value, field_name in (
            (self.paper_registry_event_count, "paper_registry_event_count"),
            (self.pre_event_count, "pre_event_count"),
            (self.post_event_count, "post_event_count"),
        ):
            _validate_nonnegative_int(value, field_name)
        if (
            type(self.paper_registry_maximum_events) is not int
            or self.paper_registry_maximum_events < 1
            or self.paper_registry_event_count > self.paper_registry_maximum_events
        ):
            raise FamilyCContractError("PAPER registry receipt count/capacity is invalid")
        if not isinstance(self.decision, FamilyCEntryDecisionV2):
            raise FamilyCContractError("admission receipt decision must be FamilyCEntryDecisionV2")
        if not isinstance(self.position, FamilyCPositionV2):
            raise FamilyCContractError("admission receipt position must be FamilyCPositionV2")
        if not isinstance(self.paper_decision, PaperFokEntryDecisionV2):
            raise FamilyCContractError("admission receipt requires a PAPER decision")
        if not isinstance(self.paper_certificate, PaperFokFullFillCertificateV2):
            raise FamilyCContractError("admission receipt requires a PAPER certificate")
        canonical_family_c_entry_decision_v2(self.decision)
        canonical_paper_fok_entry_decision_v2(self.paper_decision)
        canonical_paper_fok_full_fill_certificate_v2(self.paper_certificate)
        if self.position_sha256 != _position_sha256(self.position):
            raise FamilyCContractError("admission receipt position hash differs")
        if (
            self.position.entry_event_id != self.decision.event_id
            or self.position.entry_ledger_root_sha256 != self.decision.episode_ledger_root_sha256
            or self.position.paper_decision_event_id != self.paper_decision.event_id
            or self.position.paper_decision_payload_sha256 != self.paper_decision.payload_sha256
            or self.position.admission_evidence_sha256 != self.paper_certificate.certificate_sha256
            or self.paper_certificate.signal_event_id != self.decision.event_id
            or self.paper_certificate.decision_event_id != self.paper_decision.event_id
            or self.paper_certificate.decision_payload_sha256 != self.paper_decision.payload_sha256
            or not (
                self.paper_registry_root_sha256
                == self.position.paper_registry_root_sha256
                == self.paper_certificate.terminal_registry_replay_root_sha256
            )
            or not (
                self.paper_registry_event_count
                == self.position.paper_registry_event_count
                == self.paper_certificate.terminal_registry_event_count
            )
            or self.paper_registry_maximum_events
            != self.paper_certificate.terminal_registry_maximum_events
            or not (
                self.paper_registry_checkpoint_sha256
                == self.position.paper_registry_checkpoint_sha256
                == self.paper_certificate.terminal_registry_checkpoint_sha256
            )
        ):
            raise FamilyCContractError(
                "admission receipt PAPER, decision, position, or checkpoint evidence differs"
            )
        if not isinstance(self.disposition, FamilyCAdmissionDispositionV2):
            raise FamilyCContractError("admission disposition has the wrong type")
        if self.disposition is FamilyCAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
            if (
                self.post_event_count != self.pre_event_count
                or self.post_root_sha256 == self.pre_root_sha256
            ):
                raise FamilyCContractError("new admission receipt has invalid post-state")
            return
        if (
            self.post_event_count != self.pre_event_count
            or self.post_root_sha256 != self.pre_root_sha256
        ):
            raise FamilyCContractError("pre-existing admission receipt must preserve state")


@dataclass(frozen=True, slots=True)
class FamilyCExitMutationReceiptV2:
    """Ephemeral exact-owner proof for one sequential exit-ledger mutation."""

    input_sha256: str
    entry_event_id: str
    position: FamilyCPositionV2
    position_sha256: str
    decision: FamilyCExitDecisionV2
    pre_root_sha256: str
    pre_event_count: int
    pre_next_horizon: int
    pre_sticky_inconclusive: bool
    pre_terminal: bool
    pre_active_entry_event_id: str | None
    post_root_sha256: str
    post_event_count: int
    post_next_horizon: int
    post_sticky_inconclusive: bool
    post_terminal: bool
    post_active_entry_event_id: str | None
    disposition: FamilyCExitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _EXIT_MUTATION_RECEIPT_FACTORY_TOKEN:
            raise FamilyCContractError(
                "Family C exit mutation receipts must be created by the ledger"
            )
        for value, field_name in (
            (self.input_sha256, "input_sha256"),
            (self.entry_event_id, "entry_event_id"),
            (self.position_sha256, "position_sha256"),
            (self.pre_root_sha256, "pre_root_sha256"),
            (self.post_root_sha256, "post_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        for value, field_name in (
            (self.pre_event_count, "pre_event_count"),
            (self.post_event_count, "post_event_count"),
            (self.pre_next_horizon, "pre_next_horizon"),
            (self.post_next_horizon, "post_next_horizon"),
        ):
            _validate_nonnegative_int(value, field_name)
        if self.pre_next_horizon < 1 or self.post_next_horizon < 1:
            raise FamilyCContractError("exit receipt horizons must be positive")
        if any(
            type(value) is not bool
            for value in (
                self.pre_sticky_inconclusive,
                self.pre_terminal,
                self.post_sticky_inconclusive,
                self.post_terminal,
            )
        ):
            raise FamilyCContractError("exit receipt episode flags must be boolean")
        for value, field_name in (
            (self.pre_active_entry_event_id, "pre_active_entry_event_id"),
            (self.post_active_entry_event_id, "post_active_entry_event_id"),
        ):
            if value is not None:
                _validate_sha256(value, field_name)
        if not isinstance(self.position, FamilyCPositionV2):
            raise FamilyCContractError("exit receipt position must be FamilyCPositionV2")
        if not isinstance(self.decision, FamilyCExitDecisionV2):
            raise FamilyCContractError("exit receipt decision must be FamilyCExitDecisionV2")
        canonical_family_c_exit_decision_v2(self.decision)
        if (
            self.position_sha256 != _position_sha256(self.position)
            or self.entry_event_id != self.position.entry_event_id
            or not _family_c_exit_matches_position(self.decision, self.position)
        ):
            raise FamilyCContractError("exit receipt decision or position identity differs")
        if not isinstance(self.disposition, FamilyCExitDispositionV2):
            raise FamilyCContractError("exit disposition has the wrong type")
        if self.disposition is FamilyCExitDispositionV2.NEW_BY_THIS_TRANSACTION:
            expected_next_horizon = (
                self.pre_next_horizon if self.decision.exits_position else self.pre_next_horizon + 1
            )
            if (
                self.pre_terminal
                or self.pre_active_entry_event_id != self.entry_event_id
                or self.post_event_count != self.pre_event_count + 1
                or self.post_root_sha256 == self.pre_root_sha256
                or self.decision.episode_ledger_root_sha256 != self.pre_root_sha256
                or self.post_next_horizon != expected_next_horizon
                or (self.pre_sticky_inconclusive and not self.post_sticky_inconclusive)
                or self.post_terminal != self.decision.exits_position
                or self.post_active_entry_event_id
                != (None if self.decision.exits_position else self.entry_event_id)
            ):
                raise FamilyCContractError("new exit receipt has invalid state transition")
            return
        if (
            self.post_event_count != self.pre_event_count
            or self.post_root_sha256 != self.pre_root_sha256
            or self.post_next_horizon != self.pre_next_horizon
            or self.post_sticky_inconclusive != self.pre_sticky_inconclusive
            or self.post_terminal != self.pre_terminal
            or self.post_active_entry_event_id != self.pre_active_entry_event_id
        ):
            raise FamilyCContractError("pre-existing exit receipt must preserve state")


@dataclass(slots=True)
class _FamilyCEpisodeStateV2:
    position: FamilyCPositionV2
    position_sha256: str
    next_horizon: int = 1
    sticky_inconclusive: bool = False
    terminal: bool = False


class FamilyCDecisionRegistryV2:
    """Bounded append-once gate for deterministic duplicate/conflict handling."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyCContractError("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    def register(
        self,
        decision: FamilyCEntryDecisionV2 | FamilyCExitDecisionV2,
    ) -> FamilyCRegistryDispositionV2:
        if isinstance(decision, FamilyCEntryDecisionV2):
            payload = canonical_family_c_entry_decision_v2(decision)
        elif isinstance(decision, FamilyCExitDecisionV2):
            payload = canonical_family_c_exit_decision_v2(decision)
        else:
            raise FamilyCContractError("registry accepts Family C entry or exit decisions only")
        prior = self._payload_by_event_id.get(decision.event_id)
        if prior is not None:
            if prior != payload:
                raise FamilyCContractError(
                    "deterministic Family C event ID collides with different payload"
                )
            return FamilyCRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise FamilyCContractError("bounded Family C decision registry capacity exhausted")
        self._payload_by_event_id[decision.event_id] = payload
        return FamilyCRegistryDispositionV2.NEW


class FamilyCEpisodeLedgerV2:
    """Bounded atomic ledger for active suppression and sequential h=1..6 exits."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyCContractError("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._entry_results: dict[
            str,
            tuple[str, FamilyCEntryDecisionV2],
        ] = {}
        self._exit_results: dict[
            str,
            tuple[str, FamilyCExitDecisionV2],
        ] = {}
        self._episodes: dict[str, _FamilyCEpisodeStateV2] = {}
        self._active_by_key: dict[tuple[str, VenueV2, str], str] = {}
        self._entry_commit_lock = RLock()
        self._entry_commit_owner_token = object()
        self._entry_rollback_capabilities: dict[str, object] = {}
        self._lifecycle_owner_token = object()
        self._admission_rollback_capabilities: dict[str, object] = {}
        self._exit_rollback_capabilities: dict[str, object] = {}
        self._prospective_authority_token: object | None = None

    @property
    def event_count(self) -> int:
        return len(self._entry_results) + len(self._exit_results)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def root_sha256(self) -> str:
        return self._root_sha256_with_entries(self._entry_results)

    def _claim_prospective_decision_authority_v2(self) -> object:
        """Exclusively gate mutations for one fresh prospective attempt."""

        with self._entry_commit_lock:
            if self._prospective_authority_token is not None:
                raise FamilyCContractError(
                    "Family C prospective decision authority is already held"
                )
            genesis = FamilyCEpisodeLedgerV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.root_sha256 != genesis.root_sha256
                or self._entry_results
                or self._exit_results
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyCContractError(
                    "Family C prospective authority requires exact genesis state"
                )
            authority = object()
            self._prospective_authority_token = authority
            return authority

    def _release_unconsumed_prospective_decision_authority_v2(
        self,
        authority: object,
    ) -> None:
        """Release only a still-empty claim before the WAL claim is consumed."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(authority)
            genesis = FamilyCEpisodeLedgerV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.root_sha256 != genesis.root_sha256
                or self._entry_results
                or self._exit_results
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyCContractError(
                    "cannot release a non-genesis Family C prospective authority"
                )
            self._prospective_authority_token = None

    def _assert_prospective_mutation_authority_v2(
        self,
        authority: object | None,
    ) -> None:
        held = self._prospective_authority_token
        if held is None:
            if authority is not None:
                raise FamilyCContractError("Family C prospective authority was not claimed")
            return
        if authority is not held:
            raise FamilyCContractError(
                "Family C mutation requires the held prospective decision authority"
            )

    def _root_sha256_with_entries(
        self,
        entry_results: dict[str, tuple[str, FamilyCEntryDecisionV2]],
    ) -> str:
        entry_rows = [
            {
                "event_id": event_id,
                "input_sha256": input_sha256,
                "payload_sha256": decision.payload_sha256,
            }
            for event_id, (input_sha256, decision) in sorted(entry_results.items())
        ]
        exit_rows = [
            {
                "event_id": event_id,
                "input_sha256": input_sha256,
                "payload_sha256": decision.payload_sha256,
            }
            for event_id, (input_sha256, decision) in sorted(self._exit_results.items())
        ]
        episode_rows = [
            {
                "admission_evidence_sha256": (state.position.admission_evidence_sha256),
                "beta": str(state.position.beta),
                "entry_event_id": entry_event_id,
                "entry_member_closes": [
                    {"close": str(item.close), "symbol": item.symbol}
                    for item in state.position.entry_member_closes
                ],
                "entry_member_set": list(state.position.entry_member_set),
                "feature_evidence_sha256": (state.position.feature_evidence_sha256),
                "g0": str(state.position.g0),
                "m3": str(state.position.m3),
                "next_horizon": state.next_horizon,
                "paper_decision_event_id": (state.position.paper_decision_event_id),
                "paper_decision_payload_sha256": (state.position.paper_decision_payload_sha256),
                "paper_executable_notional": str(state.position.paper_executable_notional),
                "paper_filled_quantity": str(state.position.paper_filled_quantity),
                "paper_registry_checkpoint_sha256": (
                    state.position.paper_registry_checkpoint_sha256
                ),
                "paper_registry_event_count": (state.position.paper_registry_event_count),
                "paper_registry_root_sha256": (state.position.paper_registry_root_sha256),
                "paper_requested_quantity": str(state.position.paper_requested_quantity),
                "promoting_plan_sha256": (state.position.promoting_plan_sha256),
                "position_sha256": state.position_sha256,
                "r_i3": str(state.position.r_i3),
                "side": state.position.side.value,
                "source_root_sha256": state.position.source_root_sha256,
                "entry_vwap": str(state.position.entry_vwap),
                "sticky_inconclusive": state.sticky_inconclusive,
                "symbol": state.position.symbol,
                "symbol_order": list(state.position.symbol_order),
                "terminal": state.terminal,
                "universe_root_sha256": state.position.universe_root_sha256,
                "venue": state.position.venue.value,
            }
            for entry_event_id, state in sorted(self._episodes.items())
        ]
        active_rows = [
            {
                "entry_event_id": entry_event_id,
                "promoting_plan_sha256": plan_sha256,
                "symbol": symbol,
                "venue": venue.value,
            }
            for (plan_sha256, venue, symbol), entry_event_id in sorted(
                self._active_by_key.items(),
                key=lambda item: (item[0][0], item[0][1].value, item[0][2]),
            )
        ]
        return hashlib.sha256(
            _LEDGER_ROOT_DOMAIN
            + canonical_json_line(
                {
                    "active": active_rows,
                    "entries": entry_rows,
                    "episodes": episode_rows,
                    "exits": exit_rows,
                    "schema_version": "r4b_family_c_episode_ledger_v2",
                }
            )
        ).hexdigest()

    def is_active(
        self,
        *,
        promoting_plan_sha256: str,
        venue: VenueV2,
        symbol: str,
    ) -> bool:
        return (promoting_plan_sha256, venue, symbol) in self._active_by_key

    def evaluate_entry(
        self,
        item: FamilyCEntryInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyCEntryDecisionV2:
        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            preview = self.preview_entry(item)
            return self.commit_entry_preview(
                item,
                preview,
                _prospective_authority=_prospective_authority,
            )

    def preview_entry(self, item: FamilyCEntryInputV2) -> FamilyCEntryPreviewV2:
        """Evaluate against current owner state without mutating that state."""

        with self._entry_commit_lock:
            if not isinstance(item, FamilyCEntryInputV2):
                raise FamilyCContractError("item must be FamilyCEntryInputV2")
            logical_event_id = _entry_logical_event_id(item)
            input_sha256 = _entry_input_sha256(item)
            canonical_family_c_feature_evidence_v2(item.features)
            if item.causal_input_sha256 != input_sha256:
                raise FamilyCContractError("entry causal input hash differs from payload")
            prior = self._entry_results.get(logical_event_id)
            if prior is not None:
                if prior[0] != input_sha256:
                    raise FamilyCContractError(
                        "same Family C entry event received conflicting causal input"
                    )
                decision = prior[1]
                already_committed = True
            else:
                self._require_capacity()
                active_key = (
                    item.promoting_plan_sha256,
                    item.venue,
                    item.target_symbol,
                )
                pre_root_sha256 = self.root_sha256
                decision = _evaluate_family_c_entry_unsequenced_v2(
                    item,
                    active_position=active_key in self._active_by_key,
                    ledger_root_sha256=pre_root_sha256,
                )
                if decision.event_id != logical_event_id:
                    raise FamilyCContractError("entry evaluator changed its logical event ID")
                already_committed = False
            return FamilyCEntryPreviewV2(
                input_sha256=input_sha256,
                pre_root_sha256=self.root_sha256,
                pre_event_count=self.event_count,
                decision=decision,
                already_committed=already_committed,
                _factory_token=_ENTRY_PREVIEW_FACTORY_TOKEN,
            )

    def commit_entry_preview(
        self,
        item: FamilyCEntryInputV2,
        preview: FamilyCEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyCEntryDecisionV2:
        """Compatibility API returning the decision from a receipt-backed commit."""

        return self.commit_entry_preview_with_receipt(
            item,
            preview,
            _prospective_authority=_prospective_authority,
        ).decision

    def commit_entry_preview_with_receipt(
        self,
        item: FamilyCEntryInputV2,
        preview: FamilyCEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyCEntryCommitReceiptV2:
        """Commit exactly one preview and identify which call created it."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_entry_preview(item, preview)
            prior = self._entry_results.get(logical_event_id)
            if preview.already_committed:
                if (
                    self.event_count != preview.pre_event_count
                    or self.root_sha256 != preview.pre_root_sha256
                    or prior != (input_sha256, preview.decision)
                ):
                    raise FamilyCContractError("Family C entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyCEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if prior is not None:
                if prior != (input_sha256, preview.decision):
                    raise FamilyCContractError(
                        "Family C entry preview conflicts with committed input"
                    )
                entries_without_target = dict(self._entry_results)
                del entries_without_target[logical_event_id]
                if (
                    self.event_count != preview.pre_event_count + 1
                    or self._root_sha256_with_entries(entries_without_target)
                    != preview.pre_root_sha256
                ):
                    raise FamilyCContractError("Family C entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyCEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if (
                self.event_count != preview.pre_event_count
                or self.root_sha256 != preview.pre_root_sha256
            ):
                raise FamilyCContractError("Family C entry preview state drifted before commit")
            self._require_capacity()
            active_key = (
                item.promoting_plan_sha256,
                item.venue,
                item.target_symbol,
            )
            expected = _evaluate_family_c_entry_unsequenced_v2(
                item,
                active_position=active_key in self._active_by_key,
                ledger_root_sha256=preview.pre_root_sha256,
            )
            if expected != preview.decision or expected.event_id != logical_event_id:
                raise FamilyCContractError("Family C entry preview decision drifted before commit")
            self._entry_results[logical_event_id] = (input_sha256, preview.decision)
            rollback_capability = object()
            self._entry_rollback_capabilities[logical_event_id] = rollback_capability
            return self._entry_commit_receipt(
                preview,
                FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability,
            )

    def rollback_entry_preview(
        self,
        item: FamilyCEntryInputV2,
        preview: FamilyCEntryPreviewV2,
        receipt: FamilyCEntryCommitReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW receipt and restore its untouched pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_entry_preview(item, preview)
            self._validate_entry_commit_receipt(preview, receipt)
            if receipt.disposition is not FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyCContractError("cannot roll back a pre-existing Family C entry")
            if (
                self._entry_rollback_capabilities.get(logical_event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyCContractError("Family C entry receipt does not own the current commit")
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyCContractError("Family C entry preview state drifted before rollback")
            prior = self._entry_results.get(logical_event_id)
            if prior is None or prior != (input_sha256, preview.decision):
                raise FamilyCContractError("Family C entry preview conflicts with rollback target")
            entries_without_target = dict(self._entry_results)
            del entries_without_target[logical_event_id]
            if self._root_sha256_with_entries(entries_without_target) != receipt.pre_root_sha256:
                raise FamilyCContractError("Family C entry preview state drifted before rollback")
            del self._entry_results[logical_event_id]
            if (
                self.event_count != receipt.pre_event_count
                or self.root_sha256 != receipt.pre_root_sha256
            ):
                self._entry_results[logical_event_id] = prior
                raise FamilyCContractError(
                    "Family C entry rollback failed to restore its checkpoint"
                )
            del self._entry_rollback_capabilities[logical_event_id]
            return True

    def _entry_commit_receipt(
        self,
        preview: FamilyCEntryPreviewV2,
        disposition: FamilyCEntryCommitDispositionV2,
        rollback_capability: object,
    ) -> FamilyCEntryCommitReceiptV2:
        return FamilyCEntryCommitReceiptV2(
            input_sha256=preview.input_sha256,
            event_id=preview.decision.event_id,
            decision=preview.decision,
            preview_already_committed=preview.already_committed,
            pre_root_sha256=preview.pre_root_sha256,
            pre_event_count=preview.pre_event_count,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._entry_commit_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN,
        )

    def _validate_entry_commit_receipt(
        self,
        preview: FamilyCEntryPreviewV2,
        receipt: FamilyCEntryCommitReceiptV2,
    ) -> None:
        if not isinstance(receipt, FamilyCEntryCommitReceiptV2):
            raise FamilyCContractError("receipt must be FamilyCEntryCommitReceiptV2")
        if receipt._owner_token is not self._entry_commit_owner_token:
            raise FamilyCContractError("Family C entry receipt belongs to another ledger")
        if (
            receipt.input_sha256 != preview.input_sha256
            or receipt.event_id != preview.decision.event_id
            or receipt.decision != preview.decision
            or receipt.preview_already_committed != preview.already_committed
            or receipt.pre_root_sha256 != preview.pre_root_sha256
            or receipt.pre_event_count != preview.pre_event_count
        ):
            raise FamilyCContractError("Family C entry receipt differs from exact preview")

    def _validate_entry_preview(
        self,
        item: FamilyCEntryInputV2,
        preview: FamilyCEntryPreviewV2,
    ) -> tuple[str, str]:
        if not isinstance(item, FamilyCEntryInputV2):
            raise FamilyCContractError("item must be FamilyCEntryInputV2")
        if not isinstance(preview, FamilyCEntryPreviewV2):
            raise FamilyCContractError("preview must be FamilyCEntryPreviewV2")
        canonical_family_c_feature_evidence_v2(item.features)
        canonical_family_c_entry_decision_v2(preview.decision)
        logical_event_id = _entry_logical_event_id(item)
        input_sha256 = _entry_input_sha256(item)
        if item.causal_input_sha256 != input_sha256:
            raise FamilyCContractError("entry causal input hash differs from payload")
        if preview.input_sha256 != input_sha256 or preview.decision.event_id != logical_event_id:
            raise FamilyCContractError("Family C entry preview differs from exact input")
        if (
            not preview.already_committed
            and preview.decision.episode_ledger_root_sha256 != preview.pre_root_sha256
        ):
            raise FamilyCContractError("Family C entry preview decision lost pre-root")
        return logical_event_id, input_sha256

    def admit_position(
        self,
        item: FamilyCEntryInputV2,
        decision: FamilyCEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        _prospective_authority: object | None = None,
    ) -> FamilyCPositionV2:
        """Compatibility API returning the receipt-backed admitted position."""

        return self.admit_position_with_receipt(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
            _prospective_authority=_prospective_authority,
        ).position

    def admit_position_with_receipt(
        self,
        item: FamilyCEntryInputV2,
        decision: FamilyCEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        _prospective_authority: object | None = None,
    ) -> FamilyCAdmissionReceiptV2:
        """Admit exactly one PAPER-backed position and prove mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._admit_position_with_receipt_guarded(
                item,
                decision,
                paper_decision=paper_decision,
                certificate=certificate,
                paper_registry=paper_registry,
            )

    def _admit_position_with_receipt_guarded(
        self,
        item: FamilyCEntryInputV2,
        decision: FamilyCEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
    ) -> FamilyCAdmissionReceiptV2:
        if not isinstance(item, FamilyCEntryInputV2):
            raise FamilyCContractError("item must be FamilyCEntryInputV2")
        canonical_family_c_entry_decision_v2(decision)
        input_sha256 = _entry_input_sha256(item)
        if item.causal_input_sha256 != input_sha256:
            raise FamilyCContractError("entry causal input hash differs from payload")
        prior = self._entry_results.get(decision.event_id)
        if prior is None or prior[0] != input_sha256 or prior[1] != decision:
            raise FamilyCContractError(
                "signal decision is not the ledgered result of this exact input"
            )
        position = _position_from_paper_admission(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )
        position_sha256 = _position_sha256(position)
        checkpoint = paper_registry.terminal_checkpoint_v2()
        if (
            checkpoint.replay_root_sha256 != position.paper_registry_root_sha256
            or checkpoint.event_count != position.paper_registry_event_count
            or checkpoint.maximum_events != certificate.terminal_registry_maximum_events
            or checkpoint.checkpoint_sha256 != position.paper_registry_checkpoint_sha256
        ):
            raise FamilyCContractError(
                "PAPER registry changed while Family C admission evidence was captured"
            )
        active_key = (
            item.promoting_plan_sha256,
            item.venue,
            item.target_symbol,
        )
        pre_root_sha256 = self.root_sha256
        pre_event_count = self.event_count
        state = self._episodes.get(decision.event_id)
        if state is not None:
            if (
                state.position != position
                or state.position_sha256 != position_sha256
                or state.position_sha256 != _position_sha256(state.position)
            ):
                raise FamilyCContractError("conflicting Family C PAPER admission replay")
            active_event_id = self._active_by_key.get(active_key)
            if state.terminal:
                if active_event_id is not None:
                    raise FamilyCContractError(
                        "terminal admission replay conflicts with an active position"
                    )
            elif active_event_id != decision.event_id:
                raise FamilyCContractError("conflicting Family C PAPER admission replay")
            position = state.position
            position_sha256 = state.position_sha256
            return self._admission_receipt(
                input_sha256=input_sha256,
                decision=decision,
                position=position,
                position_sha256=position_sha256,
                paper_decision=paper_decision,
                certificate=certificate,
                checkpoint_root_sha256=checkpoint.replay_root_sha256,
                checkpoint_event_count=checkpoint.event_count,
                checkpoint_maximum_events=checkpoint.maximum_events,
                checkpoint_sha256=checkpoint.checkpoint_sha256,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=FamilyCAdmissionDispositionV2.PREEXISTING,
                rollback_capability=object(),
            )
        if active_key in self._active_by_key:
            raise FamilyCContractError(
                "another Family C position is already active for this plan and symbol"
            )
        if len(self._episodes) >= self._maximum_events:
            raise FamilyCContractError("bounded Family C episode capacity exhausted")
        self._episodes[decision.event_id] = _FamilyCEpisodeStateV2(
            position,
            position_sha256,
        )
        self._active_by_key[active_key] = decision.event_id
        rollback_capability = object()
        self._admission_rollback_capabilities[decision.event_id] = rollback_capability
        receipt: FamilyCAdmissionReceiptV2 | None = None
        try:
            receipt = self._admission_receipt(
                input_sha256=input_sha256,
                decision=decision,
                position=position,
                position_sha256=position_sha256,
                paper_decision=paper_decision,
                certificate=certificate,
                checkpoint_root_sha256=checkpoint.replay_root_sha256,
                checkpoint_event_count=checkpoint.event_count,
                checkpoint_maximum_events=checkpoint.maximum_events,
                checkpoint_sha256=checkpoint.checkpoint_sha256,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=FamilyCAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability=rollback_capability,
            )
        finally:
            if receipt is None:
                self._admission_rollback_capabilities.pop(decision.event_id, None)
                self._active_by_key.pop(active_key, None)
                self._episodes.pop(decision.event_id, None)
        assert receipt is not None
        return receipt

    def _admission_receipt(
        self,
        *,
        input_sha256: str,
        decision: FamilyCEntryDecisionV2,
        position: FamilyCPositionV2,
        position_sha256: str,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        checkpoint_root_sha256: str,
        checkpoint_event_count: int,
        checkpoint_maximum_events: int,
        checkpoint_sha256: str,
        pre_root_sha256: str,
        pre_event_count: int,
        disposition: FamilyCAdmissionDispositionV2,
        rollback_capability: object,
    ) -> FamilyCAdmissionReceiptV2:
        return FamilyCAdmissionReceiptV2(
            input_sha256=input_sha256,
            decision=decision,
            position=position,
            position_sha256=position_sha256,
            paper_decision=paper_decision,
            paper_certificate=certificate,
            paper_registry_root_sha256=checkpoint_root_sha256,
            paper_registry_event_count=checkpoint_event_count,
            paper_registry_maximum_events=checkpoint_maximum_events,
            paper_registry_checkpoint_sha256=checkpoint_sha256,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._lifecycle_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def rollback_position_admission(
        self,
        item: FamilyCEntryInputV2,
        decision: FamilyCEntryDecisionV2,
        receipt: FamilyCAdmissionReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW admission receipt and restore its pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            self._validate_admission_receipt(item, decision, receipt)
            if receipt.disposition is not FamilyCAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyCContractError("cannot roll back a pre-existing Family C admission")
            if (
                self._admission_rollback_capabilities.get(decision.event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyCContractError(
                    "Family C admission receipt does not own the current mutation"
                )
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyCContractError("Family C admission state drifted before rollback")
            state = self._episodes.get(decision.event_id)
            active_key = (item.promoting_plan_sha256, item.venue, item.target_symbol)
            if (
                state is None
                or state.position != receipt.position
                or state.position_sha256 != receipt.position_sha256
                or state.next_horizon != 1
                or state.sticky_inconclusive
                or state.terminal
                or self._active_by_key.get(active_key) != decision.event_id
                or any(
                    value.entry_event_id == decision.event_id
                    for _, value in self._exit_results.values()
                )
            ):
                raise FamilyCContractError("Family C admission target drifted before rollback")
            del self._active_by_key[active_key]
            del self._episodes[decision.event_id]
            if (
                self.event_count != receipt.pre_event_count
                or self.root_sha256 != receipt.pre_root_sha256
            ):
                self._episodes[decision.event_id] = state
                self._active_by_key[active_key] = decision.event_id
                raise FamilyCContractError(
                    "Family C admission rollback failed to restore its checkpoint"
                )
            del self._admission_rollback_capabilities[decision.event_id]
            return True

    def _validate_admission_receipt(
        self,
        item: FamilyCEntryInputV2,
        decision: FamilyCEntryDecisionV2,
        receipt: FamilyCAdmissionReceiptV2,
    ) -> None:
        if not isinstance(item, FamilyCEntryInputV2):
            raise FamilyCContractError("item must be FamilyCEntryInputV2")
        if not isinstance(decision, FamilyCEntryDecisionV2):
            raise FamilyCContractError("decision must be FamilyCEntryDecisionV2")
        if not isinstance(receipt, FamilyCAdmissionReceiptV2):
            raise FamilyCContractError("receipt must be FamilyCAdmissionReceiptV2")
        if receipt._owner_token is not self._lifecycle_owner_token:
            raise FamilyCContractError("Family C admission receipt belongs to another ledger")
        canonical_family_c_entry_decision_v2(decision)
        if (
            receipt.input_sha256 != _entry_input_sha256(item)
            or receipt.decision != decision
            or receipt.position.entry_event_id != decision.event_id
        ):
            raise FamilyCContractError("Family C admission receipt differs from exact input")
        FamilyCAdmissionReceiptV2(
            input_sha256=receipt.input_sha256,
            decision=receipt.decision,
            position=receipt.position,
            position_sha256=receipt.position_sha256,
            paper_decision=receipt.paper_decision,
            paper_certificate=receipt.paper_certificate,
            paper_registry_root_sha256=receipt.paper_registry_root_sha256,
            paper_registry_event_count=receipt.paper_registry_event_count,
            paper_registry_maximum_events=receipt.paper_registry_maximum_events,
            paper_registry_checkpoint_sha256=(receipt.paper_registry_checkpoint_sha256),
            pre_root_sha256=receipt.pre_root_sha256,
            pre_event_count=receipt.pre_event_count,
            post_root_sha256=receipt.post_root_sha256,
            post_event_count=receipt.post_event_count,
            disposition=receipt.disposition,
            _owner_token=receipt._owner_token,
            _rollback_capability=receipt._rollback_capability,
            _factory_token=_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def evaluate_exit(
        self,
        item: FamilyCExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyCExitDecisionV2:
        """Compatibility API returning the receipt-backed exit decision."""

        return self.evaluate_exit_with_receipt(
            item,
            _prospective_authority=_prospective_authority,
        ).decision

    def evaluate_exit_with_receipt(
        self,
        item: FamilyCExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyCExitMutationReceiptV2:
        """Evaluate exactly one sequential exit and prove mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._evaluate_exit_with_receipt_guarded(item)

    def _evaluate_exit_with_receipt_guarded(
        self,
        item: FamilyCExitInputV2,
    ) -> FamilyCExitMutationReceiptV2:
        if not isinstance(item, FamilyCExitInputV2):
            raise FamilyCContractError("item must be FamilyCExitInputV2")
        logical_event_id = _exit_logical_event_id(item)
        input_sha256 = _exit_input_sha256(item)
        if item.causal_input_sha256 != input_sha256:
            raise FamilyCContractError("exit causal input hash differs from payload")
        state = self._episodes.get(item.position.entry_event_id)
        if state is None or state.position != item.position:
            raise FamilyCContractError("exit position is absent from its episode ledger")
        if state.position_sha256 != _position_sha256(item.position):
            raise FamilyCContractError("exit position differs from admitted payload")
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        pre_root_sha256 = self.root_sha256
        pre_event_count = self.event_count
        pre_next_horizon = state.next_horizon
        pre_sticky_inconclusive = state.sticky_inconclusive
        pre_terminal = state.terminal
        pre_active_entry_event_id = self._active_by_key.get(active_key)
        prior = self._exit_results.get(logical_event_id)
        if prior is not None:
            if prior[0] != input_sha256:
                raise FamilyCContractError(
                    "same Family C exit event received conflicting causal input"
                )
            canonical_family_c_exit_decision_v2(prior[1])
            if not _family_c_exit_matches_position(prior[1], item.position):
                raise FamilyCContractError("stored Family C exit differs from its position")
            return self._exit_mutation_receipt(
                input_sha256=input_sha256,
                item=item,
                decision=prior[1],
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_next_horizon=pre_next_horizon,
                pre_sticky_inconclusive=pre_sticky_inconclusive,
                pre_terminal=pre_terminal,
                pre_active_entry_event_id=pre_active_entry_event_id,
                disposition=FamilyCExitDispositionV2.PREEXISTING,
                rollback_capability=object(),
            )
        self._require_capacity()
        if state.terminal:
            raise FamilyCContractError("Family C episode is already terminal")
        if not 1 <= item.horizon_bars <= FAMILY_C_HARD_HORIZON_BARS_V2:
            raise FamilyCContractError("Family C exit horizon must be in h=1..6")
        if item.horizon_bars != state.next_horizon:
            raise FamilyCContractError(f"expected Family C exit horizon h={state.next_horizon}")
        if pre_active_entry_event_id != item.position.entry_event_id:
            raise FamilyCContractError("active episode index differs from exit position")
        decision = _evaluate_family_c_exit_unsequenced_v2(
            item,
            ledger_root_sha256=self.root_sha256,
        )
        missing_mask = {move.symbol for move in item.member_moves} != set(
            item.position.entry_member_set
        )
        sticky_inconclusive = state.sticky_inconclusive or missing_mask
        if sticky_inconclusive and decision.interval_status is FamilyCIntervalStatusV2.COMPLETE:
            decision = _copy_exit_with_sticky_inconclusive(decision)
        self._exit_results[logical_event_id] = (input_sha256, decision)
        state.sticky_inconclusive = sticky_inconclusive
        if decision.exits_position:
            state.terminal = True
            active_key = (
                item.position.promoting_plan_sha256,
                item.position.venue,
                item.position.symbol,
            )
            if self._active_by_key.get(active_key) != item.position.entry_event_id:
                raise FamilyCContractError("active episode index differs from exit position")
            del self._active_by_key[active_key]
        else:
            state.next_horizon += 1
        rollback_capability = object()
        self._exit_rollback_capabilities[logical_event_id] = rollback_capability
        receipt: FamilyCExitMutationReceiptV2 | None = None
        try:
            receipt = self._exit_mutation_receipt(
                input_sha256=input_sha256,
                item=item,
                decision=decision,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_next_horizon=pre_next_horizon,
                pre_sticky_inconclusive=pre_sticky_inconclusive,
                pre_terminal=pre_terminal,
                pre_active_entry_event_id=pre_active_entry_event_id,
                disposition=FamilyCExitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability=rollback_capability,
            )
        finally:
            if receipt is None:
                self._exit_rollback_capabilities.pop(logical_event_id, None)
                self._exit_results.pop(logical_event_id, None)
                state.next_horizon = pre_next_horizon
                state.sticky_inconclusive = pre_sticky_inconclusive
                state.terminal = pre_terminal
                if pre_active_entry_event_id is None:
                    self._active_by_key.pop(active_key, None)
                else:
                    self._active_by_key[active_key] = pre_active_entry_event_id
        assert receipt is not None
        return receipt

    def _exit_mutation_receipt(
        self,
        *,
        input_sha256: str,
        item: FamilyCExitInputV2,
        decision: FamilyCExitDecisionV2,
        pre_root_sha256: str,
        pre_event_count: int,
        pre_next_horizon: int,
        pre_sticky_inconclusive: bool,
        pre_terminal: bool,
        pre_active_entry_event_id: str | None,
        disposition: FamilyCExitDispositionV2,
        rollback_capability: object,
    ) -> FamilyCExitMutationReceiptV2:
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        state = self._episodes[item.position.entry_event_id]
        return FamilyCExitMutationReceiptV2(
            input_sha256=input_sha256,
            entry_event_id=item.position.entry_event_id,
            position=item.position,
            position_sha256=_position_sha256(item.position),
            decision=decision,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            pre_next_horizon=pre_next_horizon,
            pre_sticky_inconclusive=pre_sticky_inconclusive,
            pre_terminal=pre_terminal,
            pre_active_entry_event_id=pre_active_entry_event_id,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            post_next_horizon=state.next_horizon,
            post_sticky_inconclusive=state.sticky_inconclusive,
            post_terminal=state.terminal,
            post_active_entry_event_id=self._active_by_key.get(active_key),
            disposition=disposition,
            _owner_token=self._lifecycle_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN,
        )

    def rollback_exit(
        self,
        item: FamilyCExitInputV2,
        receipt: FamilyCExitMutationReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW exit receipt and restore all episode state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_exit_mutation_receipt(
                item,
                receipt,
            )
            if receipt.disposition is not FamilyCExitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyCContractError("cannot roll back a pre-existing Family C exit")
            if (
                self._exit_rollback_capabilities.get(logical_event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyCContractError(
                    "Family C exit receipt does not own the current mutation"
                )
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyCContractError("Family C exit state drifted before rollback")
            state = self._episodes.get(receipt.entry_event_id)
            active_key = (
                receipt.position.promoting_plan_sha256,
                receipt.position.venue,
                receipt.position.symbol,
            )
            if (
                state is None
                or state.position != receipt.position
                or state.position_sha256 != receipt.position_sha256
                or state.next_horizon != receipt.post_next_horizon
                or state.sticky_inconclusive != receipt.post_sticky_inconclusive
                or state.terminal != receipt.post_terminal
                or self._active_by_key.get(active_key) != receipt.post_active_entry_event_id
                or self._exit_results.get(logical_event_id) != (input_sha256, receipt.decision)
            ):
                raise FamilyCContractError("Family C exit target drifted before rollback")
            del self._exit_results[logical_event_id]
            state.next_horizon = receipt.pre_next_horizon
            state.sticky_inconclusive = receipt.pre_sticky_inconclusive
            state.terminal = receipt.pre_terminal
            if receipt.pre_active_entry_event_id is None:
                self._active_by_key.pop(active_key, None)
            else:
                self._active_by_key[active_key] = receipt.pre_active_entry_event_id
            if (
                self.event_count != receipt.pre_event_count
                or self.root_sha256 != receipt.pre_root_sha256
            ):
                self._exit_results[logical_event_id] = (input_sha256, receipt.decision)
                state.next_horizon = receipt.post_next_horizon
                state.sticky_inconclusive = receipt.post_sticky_inconclusive
                state.terminal = receipt.post_terminal
                if receipt.post_active_entry_event_id is None:
                    self._active_by_key.pop(active_key, None)
                else:
                    self._active_by_key[active_key] = receipt.post_active_entry_event_id
                raise FamilyCContractError(
                    "Family C exit rollback failed to restore its checkpoint"
                )
            del self._exit_rollback_capabilities[logical_event_id]
            return True

    def _validate_exit_mutation_receipt(
        self,
        item: FamilyCExitInputV2,
        receipt: FamilyCExitMutationReceiptV2,
    ) -> tuple[str, str]:
        if not isinstance(item, FamilyCExitInputV2):
            raise FamilyCContractError("item must be FamilyCExitInputV2")
        if not isinstance(receipt, FamilyCExitMutationReceiptV2):
            raise FamilyCContractError("receipt must be FamilyCExitMutationReceiptV2")
        if receipt._owner_token is not self._lifecycle_owner_token:
            raise FamilyCContractError("Family C exit receipt belongs to another ledger")
        logical_event_id = _exit_logical_event_id(item)
        input_sha256 = _exit_input_sha256(item)
        if item.causal_input_sha256 != input_sha256:
            raise FamilyCContractError("exit causal input hash differs from payload")
        if (
            receipt.input_sha256 != input_sha256
            or receipt.entry_event_id != item.position.entry_event_id
            or receipt.position != item.position
            or receipt.position_sha256 != _position_sha256(item.position)
            or receipt.decision.event_id != logical_event_id
        ):
            raise FamilyCContractError("Family C exit receipt differs from exact input")
        FamilyCExitMutationReceiptV2(
            input_sha256=receipt.input_sha256,
            entry_event_id=receipt.entry_event_id,
            position=receipt.position,
            position_sha256=receipt.position_sha256,
            decision=receipt.decision,
            pre_root_sha256=receipt.pre_root_sha256,
            pre_event_count=receipt.pre_event_count,
            pre_next_horizon=receipt.pre_next_horizon,
            pre_sticky_inconclusive=receipt.pre_sticky_inconclusive,
            pre_terminal=receipt.pre_terminal,
            pre_active_entry_event_id=receipt.pre_active_entry_event_id,
            post_root_sha256=receipt.post_root_sha256,
            post_event_count=receipt.post_event_count,
            post_next_horizon=receipt.post_next_horizon,
            post_sticky_inconclusive=receipt.post_sticky_inconclusive,
            post_terminal=receipt.post_terminal,
            post_active_entry_event_id=receipt.post_active_entry_event_id,
            disposition=receipt.disposition,
            _owner_token=receipt._owner_token,
            _rollback_capability=receipt._rollback_capability,
            _factory_token=_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN,
        )
        return logical_event_id, input_sha256

    def export_state_v2(self) -> bytes:
        """Export the complete bounded episode state as canonical JSONL."""

        return canonical_json_line(
            {
                "active": _family_c_active_state_rows(self),
                "entries": [
                    {
                        "canonical_decision": canonical_family_c_entry_decision_v2(decision).decode(
                            "utf-8"
                        ),
                        "event_id": event_id,
                        "input_sha256": input_sha256,
                    }
                    for event_id, (input_sha256, decision) in sorted(self._entry_results.items())
                ],
                "episodes": [
                    {
                        "entry_event_id": entry_event_id,
                        "next_horizon": state.next_horizon,
                        "position": _family_c_position_document(state.position),
                        "position_sha256": state.position_sha256,
                        "sticky_inconclusive": state.sticky_inconclusive,
                        "terminal": state.terminal,
                    }
                    for entry_event_id, state in sorted(self._episodes.items())
                ],
                "event_count": self.event_count,
                "exits": [
                    {
                        "canonical_decision": canonical_family_c_exit_decision_v2(decision).decode(
                            "utf-8"
                        ),
                        "event_id": event_id,
                        "input_sha256": input_sha256,
                    }
                    for event_id, (input_sha256, decision) in sorted(self._exit_results.items())
                ],
                "maximum_events": self._maximum_events,
                "root_sha256": self.root_sha256,
                "schema_version": _EPISODE_STATE_SCHEMA_V2,
            }
        )

    @classmethod
    def restore_state_v2(
        cls,
        payload: bytes,
        *,
        maximum_events: int,
        expected_event_count: int,
        expected_root_sha256: str,
    ) -> FamilyCEpisodeLedgerV2:
        """Restore only when an external capacity/count/root checkpoint agrees."""

        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyCContractError("maximum_events must be a positive integer")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        _validate_sha256(expected_root_sha256, "expected_root_sha256")
        if type(payload) is not bytes or not payload:
            raise FamilyCContractError("episode state must be non-empty bytes")
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FamilyCContractError("episode state is invalid UTF-8 JSON") from error
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise FamilyCContractError("episode state must be canonical JSONL")
        if (
            set(document)
            != {
                "active",
                "entries",
                "episodes",
                "event_count",
                "exits",
                "maximum_events",
                "root_sha256",
                "schema_version",
            }
            or document.get("schema_version") != _EPISODE_STATE_SCHEMA_V2
        ):
            raise FamilyCContractError("episode state schema is unsupported")
        if type(document.get("maximum_events")) is not int:
            raise FamilyCContractError("episode state capacity must be an integer")
        if type(document.get("event_count")) is not int:
            raise FamilyCContractError("episode event count must be an integer")
        if document.get("maximum_events") != maximum_events:
            raise FamilyCContractError("episode state capacity differs")
        if document.get("event_count") != expected_event_count:
            raise FamilyCContractError("episode event count differs from external checkpoint")
        if document.get("root_sha256") != expected_root_sha256:
            raise FamilyCContractError("episode root differs from external checkpoint")

        ledger = cls(maximum_events=maximum_events)
        _restore_family_c_decision_rows(document.get("entries"), ledger, entry=True)
        _restore_family_c_decision_rows(document.get("exits"), ledger, entry=False)
        if ledger.event_count != expected_event_count:
            raise FamilyCContractError("restored episode event count differs")
        _restore_family_c_episode_rows(document.get("episodes"), ledger)
        if document.get("active") != _family_c_active_state_rows(ledger):
            raise FamilyCContractError("episode active index differs from replay")
        if ledger.root_sha256 != expected_root_sha256:
            raise FamilyCContractError("restored episode root differs")
        if ledger.export_state_v2() != payload:
            raise FamilyCContractError("episode state does not replay byte-for-byte")
        return ledger

    def _require_capacity(self) -> None:
        if self.event_count >= self._maximum_events:
            raise FamilyCContractError("bounded Family C episode ledger capacity exhausted")


def _family_c_active_state_rows(
    ledger: FamilyCEpisodeLedgerV2,
) -> list[dict[str, object]]:
    return [
        {
            "entry_event_id": entry_event_id,
            "promoting_plan_sha256": plan_sha256,
            "symbol": symbol,
            "venue": venue.value,
        }
        for (plan_sha256, venue, symbol), entry_event_id in sorted(
            ledger._active_by_key.items(),
            key=lambda item: (item[0][0], item[0][1].value, item[0][2]),
        )
    ]


def _restore_family_c_decision_rows(
    raw_rows: object,
    ledger: FamilyCEpisodeLedgerV2,
    *,
    entry: bool,
) -> None:
    if not isinstance(raw_rows, list):
        raise FamilyCContractError("episode decision rows must be a list")
    prior_event_id = ""
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "canonical_decision",
            "event_id",
            "input_sha256",
        }:
            raise FamilyCContractError("episode decision row has invalid shape")
        event_id = raw_row["event_id"]
        input_sha256 = raw_row["input_sha256"]
        canonical_decision = raw_row["canonical_decision"]
        _validate_sha256(event_id, "event_id")
        _validate_sha256(input_sha256, "input_sha256")
        if event_id <= prior_event_id:
            raise FamilyCContractError("episode decision rows are not strictly sorted")
        if not isinstance(canonical_decision, str):
            raise FamilyCContractError("canonical decision must be a string")
        encoded = canonical_decision.encode("utf-8")
        try:
            inner = json.loads(encoded)
        except json.JSONDecodeError as error:
            raise FamilyCContractError("episode decision payload is invalid") from error
        if not isinstance(inner, dict) or canonical_json_line(inner) != encoded:
            raise FamilyCContractError("episode decision payload is noncanonical")
        decision = _family_c_decision_from_replay_document(inner)
        if decision.event_id != event_id or _canonical_family_c_decision(decision) != encoded:
            raise FamilyCContractError("episode decision payload does not rederive")
        if entry and not isinstance(decision, FamilyCEntryDecisionV2):
            raise FamilyCContractError("entry row contains an exit decision")
        if not entry and not isinstance(decision, FamilyCExitDecisionV2):
            raise FamilyCContractError("exit row contains an entry decision")
        target = ledger._entry_results if entry else ledger._exit_results
        if event_id in target:
            raise FamilyCContractError("episode state repeats a decision event")
        if event_id in (ledger._exit_results if entry else ledger._entry_results):
            raise FamilyCContractError("entry and exit decisions share an event ID")
        target[event_id] = (input_sha256, decision)  # type: ignore[assignment]
        if ledger.event_count > ledger._maximum_events:
            raise FamilyCContractError("episode decision rows exceed capacity")
        prior_event_id = event_id


def _family_c_decision_from_replay_document(
    value: dict[str, object],
) -> FamilyCEntryDecisionV2 | FamilyCExitDecisionV2:
    role = value.get("role")
    try:
        if role == "ENTRY_DECISION":
            decision = _family_c_entry_decision_from_replay_document(value)
        elif role == "EXIT_DECISION":
            decision = _family_c_exit_decision_from_replay_document(value)
        else:
            raise FamilyCContractError("decision replay role is unsupported")
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, FamilyCContractError):
            raise
        raise FamilyCContractError("decision replay field value is invalid") from error
    if value.get("event_id") != decision.event_id:
        raise FamilyCContractError("decision replay event ID does not rederive")
    if value.get("payload_sha256") != decision.payload_sha256:
        raise FamilyCContractError("decision replay payload hash does not rederive")
    if value.get("rule_version") != FAMILY_C_RULE_VERSION_V2:
        raise FamilyCContractError("decision replay rule version differs")
    return decision


def _family_c_entry_decision_from_replay_document(
    value: dict[str, object],
) -> FamilyCEntryDecisionV2:
    required = {
        "attempt_id",
        "bar_close_ms",
        "bar_open_ms",
        "beta",
        "decision_cutoff_ms",
        "entry_member_set",
        "episode_ledger_root_sha256",
        "event_id",
        "family",
        "feature_evidence_sha256",
        "g0",
        "invalidation",
        "m3",
        "payload_sha256",
        "promoting_plan_sha256",
        "r_i3",
        "reasons",
        "role",
        "rule_version",
        "selected_rank",
        "side",
        "source_root_sha256",
        "status",
        "symbol",
        "symbol_order",
        "universe_root_sha256",
        "venue",
    }
    if set(value) != required or value.get("family") != "C":
        raise FamilyCContractError("entry replay fields are not exact")
    side_raw = value["side"]
    return FamilyCEntryDecisionV2(
        attempt_id=_family_c_json_str(value, "attempt_id"),
        symbol=_family_c_json_str(value, "symbol"),
        venue=VenueV2(_family_c_json_str(value, "venue")),
        promoting_plan_sha256=_family_c_json_str(value, "promoting_plan_sha256"),
        source_root_sha256=_family_c_json_str(value, "source_root_sha256"),
        universe_root_sha256=_family_c_json_str(value, "universe_root_sha256"),
        feature_evidence_sha256=_family_c_json_str(value, "feature_evidence_sha256"),
        episode_ledger_root_sha256=_family_c_json_str(value, "episode_ledger_root_sha256"),
        bar_open_ms=_family_c_json_int(value, "bar_open_ms"),
        bar_close_ms=_family_c_json_int(value, "bar_close_ms"),
        decision_cutoff_ms=_family_c_json_int(value, "decision_cutoff_ms"),
        status=FamilyCEntryStatusV2(_family_c_json_str(value, "status")),
        side=(None if side_raw is None else FamilyCSideV2(_family_c_json_str(value, "side"))),
        reasons=_family_c_json_string_tuple(value, "reasons"),
        invalidation=_family_c_json_str(value, "invalidation"),
        selected_rank=_family_c_json_optional_int(value, "selected_rank"),
        beta=_family_c_json_optional_decimal(value, "beta"),
        m3=_family_c_json_optional_decimal(value, "m3"),
        r_i3=_family_c_json_optional_decimal(value, "r_i3"),
        g0=_family_c_json_optional_decimal(value, "g0"),
        entry_member_set=_family_c_json_string_tuple(value, "entry_member_set"),
        symbol_order=_family_c_json_string_tuple(value, "symbol_order"),
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _family_c_exit_decision_from_replay_document(
    value: dict[str, object],
) -> FamilyCExitDecisionV2:
    required = {
        "action",
        "asset_move",
        "attempt_id",
        "bar_close_ms",
        "bar_open_ms",
        "catch_h",
        "decision_cutoff_ms",
        "entry_event_id",
        "episode_ledger_root_sha256",
        "event_id",
        "exit_source_root_sha256",
        "family",
        "interval_status",
        "invalidation",
        "market_move",
        "payload_sha256",
        "promoting_plan_sha256",
        "reason",
        "reasons",
        "role",
        "rule_version",
        "source_root_sha256",
        "symbol",
        "universe_root_sha256",
        "venue",
    }
    if set(value) != required or value.get("family") != "C":
        raise FamilyCContractError("exit replay fields are not exact")
    return FamilyCExitDecisionV2(
        entry_event_id=_family_c_json_str(value, "entry_event_id"),
        attempt_id=_family_c_json_str(value, "attempt_id"),
        symbol=_family_c_json_str(value, "symbol"),
        venue=VenueV2(_family_c_json_str(value, "venue")),
        promoting_plan_sha256=_family_c_json_str(value, "promoting_plan_sha256"),
        source_root_sha256=_family_c_json_str(value, "source_root_sha256"),
        universe_root_sha256=_family_c_json_str(value, "universe_root_sha256"),
        episode_ledger_root_sha256=_family_c_json_str(value, "episode_ledger_root_sha256"),
        exit_source_root_sha256=_family_c_json_str(value, "exit_source_root_sha256"),
        bar_open_ms=_family_c_json_int(value, "bar_open_ms"),
        bar_close_ms=_family_c_json_int(value, "bar_close_ms"),
        decision_cutoff_ms=_family_c_json_int(value, "decision_cutoff_ms"),
        action=FamilyCExitActionV2(_family_c_json_str(value, "action")),
        reason=FamilyCExitReasonV2(_family_c_json_str(value, "reason")),
        reasons=_family_c_json_string_tuple(value, "reasons"),
        invalidation=_family_c_json_str(value, "invalidation"),
        interval_status=FamilyCIntervalStatusV2(_family_c_json_str(value, "interval_status")),
        asset_move=_family_c_json_optional_decimal(value, "asset_move"),
        market_move=_family_c_json_optional_decimal(value, "market_move"),
        catch_h=_family_c_json_optional_decimal(value, "catch_h"),
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _canonical_family_c_decision(
    decision: FamilyCEntryDecisionV2 | FamilyCExitDecisionV2,
) -> bytes:
    if isinstance(decision, FamilyCEntryDecisionV2):
        return canonical_family_c_entry_decision_v2(decision)
    if isinstance(decision, FamilyCExitDecisionV2):
        return canonical_family_c_exit_decision_v2(decision)
    raise FamilyCContractError("checkpoint accepts Family C decisions only")


def _restore_family_c_episode_rows(
    raw_rows: object,
    ledger: FamilyCEpisodeLedgerV2,
) -> None:
    if not isinstance(raw_rows, list):
        raise FamilyCContractError("episode state rows must be a list")
    if len(raw_rows) > ledger._maximum_events:
        raise FamilyCContractError("episode state rows exceed capacity")
    prior_entry_event_id = ""
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "entry_event_id",
            "next_horizon",
            "position",
            "position_sha256",
            "sticky_inconclusive",
            "terminal",
        }:
            raise FamilyCContractError("episode row has invalid shape")
        entry_event_id = raw_row["entry_event_id"]
        position_sha256 = raw_row["position_sha256"]
        _validate_sha256(entry_event_id, "entry_event_id")
        _validate_sha256(position_sha256, "position_sha256")
        if entry_event_id <= prior_entry_event_id:
            raise FamilyCContractError("episode rows are not strictly sorted")
        position = _family_c_position_from_document(raw_row["position"])
        if (
            position.entry_event_id != entry_event_id
            or _position_sha256(position) != position_sha256
        ):
            raise FamilyCContractError("episode position hash does not rederive")
        entry_record = ledger._entry_results.get(entry_event_id)
        if entry_record is None:
            raise FamilyCContractError("episode has no matching entry decision")
        _validate_family_c_position_matches_entry(position, entry_record[1])
        next_horizon = raw_row["next_horizon"]
        sticky_inconclusive = raw_row["sticky_inconclusive"]
        terminal = raw_row["terminal"]
        if type(next_horizon) is not int or not (
            1 <= next_horizon <= FAMILY_C_HARD_HORIZON_BARS_V2
        ):
            raise FamilyCContractError("episode next horizon is invalid")
        if type(sticky_inconclusive) is not bool or type(terminal) is not bool:
            raise FamilyCContractError("episode flags must be boolean")
        exits = sorted(
            (
                decision
                for _, decision in ledger._exit_results.values()
                if decision.entry_event_id == entry_event_id
            ),
            key=lambda decision: decision.bar_open_ms,
        )
        _validate_family_c_episode_replay(
            position,
            exits,
            next_horizon=next_horizon,
            sticky_inconclusive=sticky_inconclusive,
            terminal=terminal,
        )
        state = _FamilyCEpisodeStateV2(
            position=position,
            position_sha256=position_sha256,
            next_horizon=next_horizon,
            sticky_inconclusive=sticky_inconclusive,
            terminal=terminal,
        )
        ledger._episodes[entry_event_id] = state
        if not terminal:
            active_key = (
                position.promoting_plan_sha256,
                position.venue,
                position.symbol,
            )
            if active_key in ledger._active_by_key:
                raise FamilyCContractError("episode state repeats an active symbol")
            ledger._active_by_key[active_key] = entry_event_id
        prior_entry_event_id = entry_event_id
    if any(
        decision.entry_event_id not in ledger._episodes
        for _, decision in ledger._exit_results.values()
    ):
        raise FamilyCContractError("episode state contains an orphan exit")


def _validate_family_c_position_matches_entry(
    position: FamilyCPositionV2,
    decision: FamilyCEntryDecisionV2,
) -> None:
    if decision.status is not FamilyCEntryStatusV2.SIGNAL:
        raise FamilyCContractError("episode position has no matching signal decision")
    if (
        position.entry_event_id != decision.event_id
        or position.attempt_id != decision.attempt_id
        or position.symbol != decision.symbol
        or position.venue is not decision.venue
        or position.promoting_plan_sha256 != decision.promoting_plan_sha256
        or position.source_root_sha256 != decision.source_root_sha256
        or position.universe_root_sha256 != decision.universe_root_sha256
        or position.feature_evidence_sha256 != decision.feature_evidence_sha256
        or position.entry_ledger_root_sha256 != decision.episode_ledger_root_sha256
        or position.side is not decision.side
        or position.signal_bar_open_ms != decision.bar_open_ms
        or position.beta != decision.beta
        or position.m3 != decision.m3
        or position.r_i3 != decision.r_i3
        or position.g0 != decision.g0
        or position.entry_member_set != decision.entry_member_set
        or position.symbol_order != decision.symbol_order
    ):
        raise FamilyCContractError("episode position differs from signal decision")


def _validate_family_c_episode_replay(
    position: FamilyCPositionV2,
    exits: list[FamilyCExitDecisionV2],
    *,
    next_horizon: int,
    sticky_inconclusive: bool,
    terminal: bool,
) -> None:
    horizons = tuple(
        (decision.bar_open_ms - position.signal_bar_open_ms) // FIVE_MINUTE_MS_V2
        for decision in exits
    )
    if horizons != tuple(range(1, len(exits) + 1)):
        raise FamilyCContractError("episode exits are not contiguous h1..h6")
    if any(
        (
            decision.attempt_id,
            decision.symbol,
            decision.venue,
            decision.promoting_plan_sha256,
            decision.source_root_sha256,
            decision.universe_root_sha256,
        )
        != (
            position.attempt_id,
            position.symbol,
            position.venue,
            position.promoting_plan_sha256,
            position.source_root_sha256,
            position.universe_root_sha256,
        )
        for decision in exits
    ):
        raise FamilyCContractError("episode exit identity differs from position")
    observed_terminal = bool(exits and exits[-1].exits_position)
    if any(decision.exits_position for decision in exits[:-1]):
        raise FamilyCContractError("episode has rows after a terminal exit")
    expected_next_horizon = len(exits) if observed_terminal else len(exits) + 1
    if terminal != observed_terminal or next_horizon != expected_next_horizon:
        raise FamilyCContractError("episode horizon or terminal state differs")
    observed_sticky = any(
        decision.reason is FamilyCExitReasonV2.MISSING_MEMBER_INCONCLUSIVE
        or "MISSING_ENTRY_MEMBER_INTERVAL_INCONCLUSIVE" in decision.reasons
        for decision in exits
    )
    if observed_sticky and not sticky_inconclusive:
        raise FamilyCContractError("episode lost sticky-inconclusive state")


def _family_c_position_document(
    position: FamilyCPositionV2,
) -> dict[str, object]:
    return {
        "admission_evidence_sha256": position.admission_evidence_sha256,
        "attempt_id": position.attempt_id,
        "beta": str(position.beta),
        "entry_event_id": position.entry_event_id,
        "entry_ledger_root_sha256": position.entry_ledger_root_sha256,
        "entry_member_closes": [
            {"close": str(item.close), "symbol": item.symbol}
            for item in position.entry_member_closes
        ],
        "entry_member_set": list(position.entry_member_set),
        "entry_vwap": str(position.entry_vwap),
        "feature_evidence_sha256": position.feature_evidence_sha256,
        "g0": str(position.g0),
        "m3": str(position.m3),
        "paper_decision_event_id": position.paper_decision_event_id,
        "paper_decision_payload_sha256": position.paper_decision_payload_sha256,
        "paper_executable_notional": str(position.paper_executable_notional),
        "paper_filled_quantity": str(position.paper_filled_quantity),
        "paper_registry_checkpoint_sha256": (position.paper_registry_checkpoint_sha256),
        "paper_registry_event_count": position.paper_registry_event_count,
        "paper_registry_root_sha256": position.paper_registry_root_sha256,
        "paper_requested_quantity": str(position.paper_requested_quantity),
        "promoting_plan_sha256": position.promoting_plan_sha256,
        "r_i3": str(position.r_i3),
        "side": position.side.value,
        "signal_bar_open_ms": position.signal_bar_open_ms,
        "source_root_sha256": position.source_root_sha256,
        "symbol": position.symbol,
        "symbol_order": list(position.symbol_order),
        "universe_root_sha256": position.universe_root_sha256,
        "venue": position.venue.value,
    }


def _family_c_position_from_document(raw: object) -> FamilyCPositionV2:
    required = {
        "admission_evidence_sha256",
        "attempt_id",
        "beta",
        "entry_event_id",
        "entry_ledger_root_sha256",
        "entry_member_closes",
        "entry_member_set",
        "entry_vwap",
        "feature_evidence_sha256",
        "g0",
        "m3",
        "paper_decision_event_id",
        "paper_decision_payload_sha256",
        "paper_executable_notional",
        "paper_filled_quantity",
        "paper_registry_checkpoint_sha256",
        "paper_registry_event_count",
        "paper_registry_root_sha256",
        "paper_requested_quantity",
        "promoting_plan_sha256",
        "r_i3",
        "side",
        "signal_bar_open_ms",
        "source_root_sha256",
        "symbol",
        "symbol_order",
        "universe_root_sha256",
        "venue",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise FamilyCContractError("position replay document has invalid shape")
    try:
        return FamilyCPositionV2(
            entry_event_id=_family_c_json_str(raw, "entry_event_id"),
            attempt_id=_family_c_json_str(raw, "attempt_id"),
            symbol=_family_c_json_str(raw, "symbol"),
            venue=VenueV2(_family_c_json_str(raw, "venue")),
            promoting_plan_sha256=_family_c_json_str(raw, "promoting_plan_sha256"),
            source_root_sha256=_family_c_json_str(raw, "source_root_sha256"),
            universe_root_sha256=_family_c_json_str(raw, "universe_root_sha256"),
            feature_evidence_sha256=_family_c_json_str(raw, "feature_evidence_sha256"),
            entry_ledger_root_sha256=_family_c_json_str(raw, "entry_ledger_root_sha256"),
            admission_evidence_sha256=_family_c_json_str(raw, "admission_evidence_sha256"),
            paper_decision_event_id=_family_c_json_str(raw, "paper_decision_event_id"),
            paper_decision_payload_sha256=_family_c_json_str(raw, "paper_decision_payload_sha256"),
            paper_registry_root_sha256=_family_c_json_str(raw, "paper_registry_root_sha256"),
            paper_registry_event_count=_family_c_json_int(raw, "paper_registry_event_count"),
            paper_registry_checkpoint_sha256=_family_c_json_str(
                raw, "paper_registry_checkpoint_sha256"
            ),
            paper_requested_quantity=_family_c_json_decimal(raw, "paper_requested_quantity"),
            paper_filled_quantity=_family_c_json_decimal(raw, "paper_filled_quantity"),
            paper_executable_notional=_family_c_json_decimal(raw, "paper_executable_notional"),
            entry_vwap=_family_c_json_decimal(raw, "entry_vwap"),
            side=FamilyCSideV2(_family_c_json_str(raw, "side")),
            signal_bar_open_ms=_family_c_json_int(raw, "signal_bar_open_ms"),
            beta=_family_c_json_decimal(raw, "beta"),
            m3=_family_c_json_decimal(raw, "m3"),
            r_i3=_family_c_json_decimal(raw, "r_i3"),
            g0=_family_c_json_decimal(raw, "g0"),
            entry_member_set=_family_c_json_string_tuple(raw, "entry_member_set"),
            symbol_order=_family_c_json_string_tuple(raw, "symbol_order"),
            entry_member_closes=_family_c_json_symbol_closes(raw, "entry_member_closes"),
            _factory_token=_POSITION_FACTORY_TOKEN,
        )
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, FamilyCContractError):
            raise
        raise FamilyCContractError("position replay field is invalid") from error


def _family_c_json_str(value: dict[str, object], field_name: str) -> str:
    item = value[field_name]
    if not isinstance(item, str):
        raise FamilyCContractError(f"{field_name} must be a string")
    return item


def _family_c_json_int(value: dict[str, object], field_name: str) -> int:
    item = value[field_name]
    if type(item) is not int:
        raise FamilyCContractError(f"{field_name} must be an integer")
    return item


def _family_c_json_optional_int(
    value: dict[str, object],
    field_name: str,
) -> int | None:
    item = value[field_name]
    if item is None:
        return None
    if type(item) is not int:
        raise FamilyCContractError(f"{field_name} must be an integer or null")
    return item


def _family_c_json_string_tuple(
    value: dict[str, object],
    field_name: str,
) -> tuple[str, ...]:
    item = value[field_name]
    if not isinstance(item, list) or not all(isinstance(element, str) for element in item):
        raise FamilyCContractError(f"{field_name} must be a string list")
    return tuple(item)


def _family_c_json_decimal(
    value: dict[str, object],
    field_name: str,
) -> Decimal:
    item = value[field_name]
    if not isinstance(item, str):
        raise FamilyCContractError(f"{field_name} must be a decimal string")
    try:
        result = Decimal(item)
    except InvalidOperation as error:
        raise FamilyCContractError(f"{field_name} must be a valid decimal string") from error
    if not result.is_finite():
        raise FamilyCContractError(f"{field_name} must be a finite decimal")
    return result


def _family_c_json_optional_decimal(
    value: dict[str, object],
    field_name: str,
) -> Decimal | None:
    if value[field_name] is None:
        return None
    return _family_c_json_decimal(value, field_name)


def _family_c_json_symbol_closes(
    value: dict[str, object],
    field_name: str,
) -> tuple[FamilyCSymbolCloseV2, ...]:
    raw_rows = value[field_name]
    if not isinstance(raw_rows, list):
        raise FamilyCContractError(f"{field_name} must be a list")
    closes: list[FamilyCSymbolCloseV2] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {"close", "symbol"}:
            raise FamilyCContractError(f"{field_name} row has invalid shape")
        closes.append(
            FamilyCSymbolCloseV2(
                symbol=_family_c_json_str(raw_row, "symbol"),
                close=_family_c_json_decimal(raw_row, "close"),
            )
        )
    return tuple(closes)


def construct_family_c_features_v2(
    panel: FamilyCCandlePanelV2,
) -> FamilyCFeatureSnapshotV2:
    """Build sealed features from the exact causal candle panel only."""

    if not isinstance(panel, FamilyCCandlePanelV2):
        raise FamilyCContractError("Family C READY evidence requires FamilyCCandlePanelV2")
    canonical_family_c_candle_panel_v2(panel)
    histories: list[FamilyCRawMemberHistoryV2] = []
    current_closes: list[FamilyCSymbolCloseV2] = []
    with localcontext(protocol_decimal_context_v2()):
        for symbol in panel.universe.members:
            member_candles = tuple(item for item in panel.candles if item.symbol == symbol)
            closes = tuple(item.close for item in member_candles)
            prior_one_bar = tuple(
                (closes[index] / closes[index - 1]).ln()
                for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1)
            )
            prior_three_bar = tuple(
                (closes[index] / closes[index - 3]).ln()
                for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1)
            )
            current_three_bar = (closes[-1] / closes[-4]).ln()
            histories.append(
                FamilyCRawMemberHistoryV2(
                    symbol=symbol,
                    prior_one_bar_returns=prior_one_bar,
                    prior_three_bar_returns=prior_three_bar,
                    current_three_bar_return=current_three_bar,
                )
            )
            current_closes.append(FamilyCSymbolCloseV2(symbol, closes[-1]))
    context = _FamilyCFeatureBuildContextV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe_root_sha256=panel.universe.universe_root_sha256,
        panel_root_sha256=panel.panel_root_sha256,
        bar_open_ms=panel.current_bar_open_ms,
        bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        latest_source_event_ms=max(item.event_time_ms for item in panel.candles),
        latest_source_receipt_ms=max(item.receipt_time_ms for item in panel.candles),
        current_closes=tuple(current_closes),
    )
    return _construct_family_c_math_v2(
        context,
        panel.universe.members,
        tuple(histories),
    )


def _construct_family_c_math_v2(
    context: _FamilyCFeatureBuildContextV2,
    expected_members: tuple[str, ...],
    histories: tuple[FamilyCRawMemberHistoryV2, ...],
) -> FamilyCFeatureSnapshotV2:
    """Apply the literal mathematics to factory-derived aligned log returns."""

    member_set = _normalized_member_set(expected_members)
    if type(histories) is not tuple:
        raise FamilyCContractError("histories must be an immutable tuple")
    if any(not isinstance(item, FamilyCRawMemberHistoryV2) for item in histories):
        raise FamilyCContractError("histories contains an unsupported value")
    observed_symbols = tuple(item.symbol for item in histories)
    if len(set(observed_symbols)) != len(observed_symbols) or set(observed_symbols) != set(
        member_set
    ):
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.INCONCLUSIVE_CROSS_SECTION,
            "ANY_MISSING_OR_EXTRA_DAILY_ELIGIBLE_MEMBER",
            member_set,
            0,
        )
    ordered = tuple(sorted(histories, key=lambda item: _symbol_key(item.symbol)))
    lengths = tuple(
        length
        for item in ordered
        for length in (
            len(item.prior_one_bar_returns),
            len(item.prior_three_bar_returns),
        )
    )
    if any(length > FAMILY_C_PRIOR_WINDOW_V2 for length in lengths):
        raise FamilyCContractError("Family C prior history exceeds exactly 8,640 rows")
    if any(length < FAMILY_C_PRIOR_WINDOW_V2 for length in lengths):
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.FEATURE_NOT_READY_HISTORY,
            "EXACT_8640_PRIOR_ROWS_REQUIRED",
            member_set,
            min(lengths, default=0),
        )
    raw_values = tuple(
        value
        for item in ordered
        for value in (
            *item.prior_one_bar_returns,
            *item.prior_three_bar_returns,
            item.current_three_bar_return,
        )
    )
    if any(value is None for value in raw_values):
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.INCONCLUSIVE_CROSS_SECTION,
            "MISSING_ENTRY_MEMBER_RETURN",
            member_set,
            FAMILY_C_PRIOR_WINDOW_V2,
        )
    if any(not _is_finite_decimal(value) for value in raw_values):
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.DATA_INVALID,
            "NONFINITE_OR_NONDECIMAL_RETURN",
            member_set,
            FAMILY_C_PRIOR_WINDOW_V2,
        )
    one_bar_rows = tuple(
        tuple(_require_decimal(item.prior_one_bar_returns[index]) for item in ordered)
        for index in range(FAMILY_C_PRIOR_WINDOW_V2)
    )
    three_bar_rows = tuple(
        tuple(_require_decimal(item.prior_three_bar_returns[index]) for item in ordered)
        for index in range(FAMILY_C_PRIOR_WINDOW_V2)
    )
    market_returns = tuple(_median_decimal(row) for row in one_bar_rows)
    prior_m3 = tuple(_median_decimal(row) for row in three_bar_rows)
    current_r3 = tuple(_require_decimal(item.current_three_bar_return) for item in ordered)
    m3_current = _median_decimal(current_r3)
    market_variance = _population_variance(market_returns)
    if market_variance == 0:
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_MARKET_VARIANCE,
            "MARKET_VARIANCE_EQ_ZERO",
            member_set,
            FAMILY_C_PRIOR_WINDOW_V2,
        )
    member_features: list[FamilyCMemberFeatureV2] = []
    shock_sign = _sign(m3_current)
    for member, r_i3 in zip(ordered, current_r3, strict=True):
        asset_returns = tuple(_require_decimal(value) for value in member.prior_one_bar_returns)
        beta_result = population_beta_v2(asset_returns, market_returns)
        with localcontext(protocol_decimal_context_v2()):
            residuals = tuple(
                asset - beta_result.beta * market
                for asset, market in zip(asset_returns, market_returns, strict=True)
            )
            residual_scale = _MAD_SCALE * _mad_decimal(residuals)
        if residual_scale <= 0:
            return _feature_not_ready(
                context,
                FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
                f"RESIDUAL_MAD_LE_ZERO:{member.symbol}",
                member_set,
                FAMILY_C_PRIOR_WINDOW_V2,
            )
        with localcontext(protocol_decimal_context_v2()):
            g0 = Decimal(shock_sign) * (beta_result.beta * m3_current - r_i3)
            lag_score = g0 / residual_scale
        member_features.append(
            FamilyCMemberFeatureV2(
                symbol=member.symbol,
                beta_raw=beta_result.beta_raw,
                beta=beta_result.beta,
                residual_scale=residual_scale,
                current_three_bar_return=r_i3,
                g0=g0,
                lag_score=lag_score,
            )
        )
    with localcontext(protocol_decimal_context_v2()):
        shock_scale = _MAD_SCALE * _mad_decimal(prior_m3)
    if shock_scale <= 0:
        return _feature_not_ready(
            context,
            FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
            "SHOCK_MAD_LE_ZERO",
            member_set,
            FAMILY_C_PRIOR_WINDOW_V2,
        )
    with localcontext(protocol_decimal_context_v2()):
        shock_score = abs(m3_current) / shock_scale
    breadth_count = sum(
        (shock_sign > 0 and value > 0) or (shock_sign < 0 and value < 0) for value in current_r3
    )
    return FamilyCFeatureSnapshotV2(
        venue=context.venue,
        promoting_plan_sha256=context.promoting_plan_sha256,
        source_root_sha256=context.source_root_sha256,
        universe_root_sha256=context.universe_root_sha256,
        panel_root_sha256=context.panel_root_sha256,
        bar_open_ms=context.bar_open_ms,
        bar_close_ms=context.bar_close_ms,
        decision_cutoff_ms=context.decision_cutoff_ms,
        latest_source_event_ms=context.latest_source_event_ms,
        latest_source_receipt_ms=context.latest_source_receipt_ms,
        current_closes=context.current_closes,
        status=FamilyCFeatureStatusV2.READY,
        reasons=("FAMILY_C_FEATURES_READY",),
        member_set=member_set,
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        m3_current=m3_current,
        shock_scale=shock_scale,
        shock_score=shock_score,
        breadth_count=breadth_count,
        members=tuple(member_features),
        _factory_token=_FEATURE_FACTORY_TOKEN,
    )


def population_beta_v2(
    asset_returns: tuple[Decimal, ...],
    market_returns: tuple[Decimal, ...],
) -> FamilyCPopulationBetaV2:
    """Compute ddof=0 covariance/variance beta and clip it to [0.25, 2.5]."""

    if type(asset_returns) is not tuple or type(market_returns) is not tuple:
        raise FamilyCContractError("population beta inputs must be immutable tuples")
    if (
        len(asset_returns) != FAMILY_C_PRIOR_WINDOW_V2
        or len(market_returns) != FAMILY_C_PRIOR_WINDOW_V2
    ):
        raise FamilyCContractError("population beta requires exactly 8,640 paired rows")
    if any(not _is_finite_decimal(value) for value in (*asset_returns, *market_returns)):
        raise FamilyCContractError("population beta inputs must be finite Decimal")
    with localcontext(protocol_decimal_context_v2()):
        count = Decimal(FAMILY_C_PRIOR_WINDOW_V2)
        asset_mean = sum(asset_returns, start=Decimal(0)) / count
        market_mean = sum(market_returns, start=Decimal(0)) / count
        covariance = (
            sum(
                (
                    (asset - asset_mean) * (market - market_mean)
                    for asset, market in zip(asset_returns, market_returns, strict=True)
                ),
                start=Decimal(0),
            )
            / count
        )
        variance = (
            sum(
                ((market - market_mean) ** 2 for market in market_returns),
                start=Decimal(0),
            )
            / count
        )
        if variance == 0:
            raise FamilyCContractError("population market variance is zero")
        beta_raw = covariance / variance
    return FamilyCPopulationBetaV2(
        covariance_pop=covariance,
        variance_pop=variance,
        beta_raw=beta_raw,
        beta=_clip_beta(beta_raw),
    )


def family_c_top_decile_count_v2(member_count: int) -> int:
    """Return fixed K=max(1, ceil(0.10*n)) without tie expansion."""

    if type(member_count) is not int or member_count < 1:
        raise FamilyCContractError("member_count must be a positive integer")
    return max(1, (member_count + 9) // 10)


def rank_family_c_members_v2(
    features: FamilyCFeatureSnapshotV2,
) -> tuple[FamilyCMemberFeatureV2, ...]:
    """Rank by L descending then normalized-symbol UTF-8 ascending."""

    if features.status is not FamilyCFeatureStatusV2.READY:
        raise FamilyCContractError("only READY Family C features may be ranked")
    return tuple(
        sorted(
            features.members,
            key=lambda item: (-item.lag_score, _symbol_key(item.symbol)),
        )
    )


def evaluate_family_c_entry_v2(
    item: FamilyCEntryInputV2,
    episode_ledger: FamilyCEpisodeLedgerV2,
) -> FamilyCEntryDecisionV2:
    """Atomically evaluate and ledger one causal Family C entry decision."""

    if not isinstance(item, FamilyCEntryInputV2):
        raise FamilyCContractError("item must be FamilyCEntryInputV2")
    if not isinstance(episode_ledger, FamilyCEpisodeLedgerV2):
        raise FamilyCContractError("episode_ledger must be FamilyCEpisodeLedgerV2")
    return episode_ledger.evaluate_entry(item)


def _evaluate_family_c_entry_unsequenced_v2(
    item: FamilyCEntryInputV2,
    *,
    active_position: bool,
    ledger_root_sha256: str,
) -> FamilyCEntryDecisionV2:
    if type(active_position) is not bool:
        raise FamilyCContractError("active_position must be ledger-derived boolean")
    _validate_sha256(ledger_root_sha256, "ledger_root_sha256")
    if item.target_symbol not in item.features.member_set:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.INCONCLUSIVE_CROSS_SECTION,
            ("TARGET_MISSING_FROM_ENTRY_MEMBER_SET",),
        )
    if item.features.status is not FamilyCFeatureStatusV2.READY:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            _entry_status_for_feature_status(item.features.status),
            item.features.reasons,
        )
    assert item.features.m3_current is not None
    assert item.features.shock_score is not None
    assert item.features.breadth_count is not None
    if item.features.m3_current == 0:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.NO_SIGNAL,
            ("M3_ZERO_COMPLETE_NO_C_SIGNAL",),
        )
    gate_failures: list[str] = []
    if item.features.shock_score < _SHOCK_SCORE_MIN:
        gate_failures.append("SHOCK_SCORE_LT_2_5")
    if item.features.breadth_count * 10 < len(item.features.member_set) * 7:
        gate_failures.append("BREADTH_LT_0_70")
    if len(item.features.member_set) < FAMILY_C_MINIMUM_MEMBERS_V2:
        gate_failures.append("MEMBER_COUNT_LT_20")
    if gate_failures:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.NO_SIGNAL,
            tuple(gate_failures),
        )
    ranked = rank_family_c_members_v2(item.features)
    rank = next(
        index for index, member in enumerate(ranked, start=1) if member.symbol == item.target_symbol
    )
    selected_count = family_c_top_decile_count_v2(len(ranked))
    member = next(value for value in ranked if value.symbol == item.target_symbol)
    if rank > selected_count:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.NO_SIGNAL,
            ("OUTSIDE_FIXED_TOP_DECILE_K",),
            symbol_order=tuple(value.symbol for value in ranked),
            selected_rank=rank,
        )
    if member.lag_score < _LAG_SCORE_MIN:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.NO_SIGNAL,
            ("LAG_SCORE_LT_1_5",),
            symbol_order=tuple(value.symbol for value in ranked),
            selected_rank=rank,
        )
    if active_position:
        return _entry_no_action(
            item,
            ledger_root_sha256,
            FamilyCEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION,
            ("FAMILY_SYMBOL_POSITION_ALREADY_OPEN",),
            symbol_order=tuple(value.symbol for value in ranked),
            selected_rank=rank,
        )
    side = FamilyCSideV2.LONG if item.features.m3_current > 0 else FamilyCSideV2.SHORT
    return FamilyCEntryDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.target_symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        source_root_sha256=item.source_root_sha256,
        universe_root_sha256=item.universe_root_sha256,
        feature_evidence_sha256=item.features.feature_evidence_sha256,
        episode_ledger_root_sha256=ledger_root_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        status=FamilyCEntryStatusV2.SIGNAL,
        side=side,
        reasons=(
            "COMMON_SHOCK_GATE_MET",
            f"FIXED_TOP_DECILE_RANK_{rank}_OF_{selected_count}",
            "LAG_SCORE_GTE_1_5",
            f"ACTION_{side.value}",
        ),
        invalidation="catch_h <= -0.50*g0 or catch_h >= 0.75*g0",
        selected_rank=rank,
        beta=member.beta,
        m3=item.features.m3_current,
        r_i3=member.current_three_bar_return,
        g0=member.g0,
        entry_member_set=item.features.member_set,
        symbol_order=tuple(value.symbol for value in ranked),
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def position_from_family_c_signal_v2(
    item: FamilyCEntryInputV2,
    decision: FamilyCEntryDecisionV2,
    episode_ledger: FamilyCEpisodeLedgerV2,
    *,
    paper_decision: PaperFokEntryDecisionV2,
    certificate: PaperFokFullFillCertificateV2,
    paper_registry: PaperFokDecisionRegistryV2,
) -> FamilyCPositionV2:
    """Atomically admit rule state from a registry-pinned full PAPER fill."""

    if not decision.emitted_signal or decision.side is None:
        raise FamilyCContractError("only an admitted Family C signal can create rule state")
    if not isinstance(episode_ledger, FamilyCEpisodeLedgerV2):
        raise FamilyCContractError("episode_ledger must be FamilyCEpisodeLedgerV2")
    return episode_ledger.admit_position(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )


def construct_family_c_log_moves_v2(
    entry_closes: tuple[FamilyCSymbolCloseV2, ...],
    current_closes: tuple[FamilyCSymbolCloseV2, ...],
) -> tuple[FamilyCSymbolMoveV2, ...]:
    """Construct raw log moves separately from the frozen decision engine."""

    entry = _unique_closes(entry_closes, "entry_closes")
    current = _unique_closes(current_closes, "current_closes")
    if not set(current).issubset(entry):
        raise FamilyCContractError("current_closes contains a non-entry member")
    moves: list[FamilyCSymbolMoveV2] = []
    with localcontext(protocol_decimal_context_v2()):
        for symbol in sorted(current, key=_symbol_key):
            moves.append(
                FamilyCSymbolMoveV2(
                    symbol=symbol,
                    log_move=(current[symbol] / entry[symbol]).ln(),
                )
            )
    return tuple(moves)


def build_family_c_exit_input_v2(
    *,
    position: FamilyCPositionV2,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    candles: tuple[FamilyCClosedCandleV2, ...],
    mandatory_exit: FamilyCMandatoryExitV2 | None = None,
) -> FamilyCExitInputV2:
    """Build the only exit input accepted by the sequential episode ledger."""

    if not isinstance(position, FamilyCPositionV2):
        raise FamilyCContractError("position must be FamilyCPositionV2")
    _validate_bar_times(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    if type(candles) is not tuple or any(
        not isinstance(item, FamilyCClosedCandleV2) for item in candles
    ):
        raise FamilyCContractError("exit candles must be an immutable candle tuple")
    symbols = tuple(item.symbol for item in candles)
    if len(set(symbols)) != len(symbols):
        raise FamilyCContractError("exit candles cannot duplicate members")
    if not set(symbols).issubset(position.entry_member_set):
        raise FamilyCContractError("exit candles cannot add a non-entry member")
    if any(
        item.bar_open_ms != bar_open_ms
        or item.bar_close_ms != bar_close_ms
        or item.receipt_time_ms > decision_cutoff_ms
        for item in candles
    ):
        raise FamilyCContractError("exit candles differ from the closed causal slot")
    current_closes = tuple(FamilyCSymbolCloseV2(item.symbol, item.close) for item in candles)
    moves = construct_family_c_log_moves_v2(
        position.entry_member_closes,
        current_closes,
    )
    ordered_candles = tuple(sorted(candles, key=lambda item: _symbol_key(item.symbol)))
    exit_source_root = hashlib.sha256(
        _EXIT_SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "candles": [_closed_candle_document(item) for item in ordered_candles],
                "decision_cutoff_ms": decision_cutoff_ms,
                "entry_event_id": position.entry_event_id,
                "promoting_plan_sha256": position.promoting_plan_sha256,
                "schema_version": "r4b_family_c_exit_source_root_v2",
                "source_root_sha256": position.source_root_sha256,
                "venue": position.venue.value,
            }
        )
    ).hexdigest()
    return FamilyCExitInputV2(
        position=position,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        mandatory_exit=mandatory_exit,
        member_moves=moves,
        exit_source_root_sha256=exit_source_root,
        latest_source_event_ms=max(
            (item.event_time_ms for item in ordered_candles),
            default=0,
        ),
        latest_source_receipt_ms=max(
            (item.receipt_time_ms for item in ordered_candles),
            default=0,
        ),
        _factory_token=_EXIT_INPUT_FACTORY_TOKEN,
    )


def evaluate_family_c_exit_v2(
    item: FamilyCExitInputV2,
    episode_ledger: FamilyCEpisodeLedgerV2,
) -> FamilyCExitDecisionV2:
    """Atomically evaluate expected h=1..6 with sticky episode state."""

    if not isinstance(item, FamilyCExitInputV2):
        raise FamilyCContractError("item must be FamilyCExitInputV2")
    if not isinstance(episode_ledger, FamilyCEpisodeLedgerV2):
        raise FamilyCContractError("episode_ledger must be FamilyCEpisodeLedgerV2")
    return episode_ledger.evaluate_exit(item)


def _evaluate_family_c_exit_unsequenced_v2(
    item: FamilyCExitInputV2,
    *,
    ledger_root_sha256: str,
) -> FamilyCExitDecisionV2:
    """Apply the literal exit priority after the ledger validates sequence."""

    _validate_sha256(ledger_root_sha256, "ledger_root_sha256")
    exit_action = (
        FamilyCExitActionV2.EXIT_LONG
        if item.position.side is FamilyCSideV2.LONG
        else FamilyCExitActionV2.EXIT_SHORT
    )
    if item.mandatory_exit is FamilyCMandatoryExitV2.DATA:
        return _exit(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.MANDATORY_DATA_EMERGENCY,
            "EXACT_DATA_EMERGENCY_REQUIRES_EXIT",
        )
    if item.mandatory_exit is FamilyCMandatoryExitV2.TERMINAL:
        return _exit(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.MANDATORY_TERMINAL_EMERGENCY,
            "TERMINAL_BOUNDARY_REQUIRES_EXIT",
        )
    if item.horizon_bars > FAMILY_C_HARD_HORIZON_BARS_V2:
        return _exit(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.MANDATORY_TERMINAL_EMERGENCY,
            "HARD_HORIZON_OVERDUE_FAIL_CLOSED",
        )
    observed = {move.symbol: move.log_move for move in item.member_moves}
    expected = set(item.position.entry_member_set)
    if set(observed) != expected:
        if item.horizon_bars == FAMILY_C_HARD_HORIZON_BARS_V2:
            return FamilyCExitDecisionV2(
                **_exit_provenance(item, ledger_root_sha256),
                action=exit_action,
                reason=FamilyCExitReasonV2.HARD_HORIZON,
                reasons=(
                    "MISSING_ENTRY_MEMBER_INTERVAL_INCONCLUSIVE",
                    "HARD_HORIZON_EXACT",
                ),
                invalidation="POSITION_EXIT_REQUIRED",
                interval_status=FamilyCIntervalStatusV2.INCONCLUSIVE_DATA,
                _factory_token=_DECISION_FACTORY_TOKEN,
            )
        return FamilyCExitDecisionV2(
            **_exit_provenance(item, ledger_root_sha256),
            action=FamilyCExitActionV2.HOLD,
            reason=FamilyCExitReasonV2.MISSING_MEMBER_INCONCLUSIVE,
            reasons=("MISSING_ENTRY_MEMBER_EARLY_EXIT_NOT_EVALUATED",),
            invalidation="catch_h <= -0.50*g0 or catch_h >= 0.75*g0",
            interval_status=FamilyCIntervalStatusV2.INCONCLUSIVE_DATA,
            _factory_token=_DECISION_FACTORY_TOKEN,
        )
    asset_move = observed[item.position.symbol]
    market_move = _median_decimal(
        tuple(observed[symbol] for symbol in item.position.entry_member_set)
    )
    with localcontext(protocol_decimal_context_v2()):
        catch_h = Decimal(_sign(item.position.m3)) * (asset_move - item.position.beta * market_move)
        adverse_boundary = Decimal("-0.50") * item.position.g0
        catchup_boundary = Decimal("0.75") * item.position.g0
    if catch_h <= adverse_boundary:
        return _exit_with_metrics(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.ADVERSE_WIDENING,
            "CATCH_H_LE_NEG_0_50_G0",
            asset_move,
            market_move,
            catch_h,
        )
    if catch_h >= catchup_boundary:
        return _exit_with_metrics(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.CATCHUP_COMPLETE,
            "CATCH_H_GE_0_75_G0",
            asset_move,
            market_move,
            catch_h,
        )
    if item.horizon_bars == FAMILY_C_HARD_HORIZON_BARS_V2:
        return _exit_with_metrics(
            item,
            ledger_root_sha256,
            exit_action,
            FamilyCExitReasonV2.HARD_HORIZON,
            "HARD_HORIZON_EXACT",
            asset_move,
            market_move,
            catch_h,
        )
    return FamilyCExitDecisionV2(
        **_exit_provenance(item, ledger_root_sha256),
        action=FamilyCExitActionV2.HOLD,
        reason=FamilyCExitReasonV2.HOLD,
        reasons=("NO_EXIT_CONDITION_MET",),
        invalidation="catch_h <= -0.50*g0 or catch_h >= 0.75*g0",
        interval_status=FamilyCIntervalStatusV2.COMPLETE,
        asset_move=asset_move,
        market_move=market_move,
        catch_h=catch_h,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _position_from_paper_admission(
    item: FamilyCEntryInputV2,
    decision: FamilyCEntryDecisionV2,
    *,
    paper_decision: PaperFokEntryDecisionV2,
    certificate: PaperFokFullFillCertificateV2,
    paper_registry: PaperFokDecisionRegistryV2,
) -> FamilyCPositionV2:
    if not decision.emitted_signal or decision.side is None:
        raise FamilyCContractError("position requires a SIGNAL decision")
    if not isinstance(paper_decision, PaperFokEntryDecisionV2):
        raise FamilyCContractError("paper_decision must be concrete PAPER evidence")
    if not isinstance(certificate, PaperFokFullFillCertificateV2):
        raise FamilyCContractError("certificate must be concrete PAPER evidence")
    if not isinstance(paper_registry, PaperFokDecisionRegistryV2):
        raise FamilyCContractError("paper_registry must be the concrete PAPER registry")
    if (
        paper_decision.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
        or not paper_decision.executed_full_quantity
    ):
        raise FamilyCContractError("zero, partial, rejected, or pending PAPER entry is not a fill")
    expected_paper_side = (
        PaperFokSideV2.BUY if decision.side is FamilyCSideV2.LONG else PaperFokSideV2.SELL
    )
    expected_decision_identity = (
        item.attempt_id,
        decision.event_id,
        item.target_symbol,
        item.venue,
        item.promoting_plan_sha256,
        item.bar_open_ms,
        item.bar_close_ms,
        item.decision_cutoff_ms,
        item.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2,
        expected_paper_side,
    )
    paper_identity = (
        paper_decision.attempt_id,
        paper_decision.signal_event_id,
        paper_decision.symbol,
        paper_decision.venue,
        paper_decision.promoting_plan_sha256,
        paper_decision.bar_open_ms,
        paper_decision.bar_close_ms,
        paper_decision.decision_cutoff_ms,
        paper_decision.target_venue_ms,
        paper_decision.side,
    )
    expected_certificate_identity = (
        item.attempt_id,
        decision.event_id,
        item.target_symbol,
        item.venue,
        item.promoting_plan_sha256,
        item.decision_cutoff_ms,
        item.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2,
        expected_paper_side,
    )
    certificate_identity = (
        certificate.attempt_id,
        certificate.signal_event_id,
        certificate.symbol,
        certificate.venue,
        certificate.promoting_plan_sha256,
        certificate.decision_cutoff_ms,
        certificate.target_venue_ms,
        certificate.side,
    )
    if (
        paper_identity != expected_decision_identity
        or certificate_identity != expected_certificate_identity
    ):
        raise FamilyCContractError("PAPER evidence identity differs from Family C signal")
    if not paper_registry.contains_exact_v2(paper_decision):
        raise FamilyCContractError("PAPER decision is absent from its registry checkpoint")
    checkpoint = paper_registry.terminal_checkpoint_v2()
    expected_certificate = issue_paper_fok_full_fill_certificate_v2(
        paper_decision,
        registry=paper_registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    if certificate != expected_certificate:
        raise FamilyCContractError("PAPER certificate differs from its sealed decision")
    if (
        paper_decision.filled_quantity is None
        or paper_decision.executable_vwap is None
        or paper_decision.executable_notional is None
        or paper_decision.requested_quantity != paper_decision.filled_quantity
        or certificate.filled_quantity != paper_decision.requested_quantity
        or certificate.executable_vwap != paper_decision.executable_vwap
        or certificate.executable_notional != paper_decision.executable_notional
    ):
        raise FamilyCContractError("PAPER requested, filled, VWAP, or notional evidence differs")
    assert decision.beta is not None
    assert decision.m3 is not None
    assert decision.r_i3 is not None
    assert decision.g0 is not None
    return FamilyCPositionV2(
        entry_event_id=decision.event_id,
        attempt_id=item.attempt_id,
        symbol=item.target_symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        source_root_sha256=item.source_root_sha256,
        universe_root_sha256=item.universe_root_sha256,
        feature_evidence_sha256=item.features.feature_evidence_sha256,
        entry_ledger_root_sha256=decision.episode_ledger_root_sha256,
        admission_evidence_sha256=certificate.certificate_sha256,
        paper_decision_event_id=paper_decision.event_id,
        paper_decision_payload_sha256=paper_decision.payload_sha256,
        paper_registry_root_sha256=checkpoint.replay_root_sha256,
        paper_registry_event_count=checkpoint.event_count,
        paper_registry_checkpoint_sha256=checkpoint.checkpoint_sha256,
        paper_requested_quantity=paper_decision.requested_quantity,
        paper_filled_quantity=paper_decision.filled_quantity,
        paper_executable_notional=paper_decision.executable_notional,
        entry_vwap=paper_decision.executable_vwap,
        side=decision.side,
        signal_bar_open_ms=item.bar_open_ms,
        beta=decision.beta,
        m3=decision.m3,
        r_i3=decision.r_i3,
        g0=decision.g0,
        entry_member_set=decision.entry_member_set,
        symbol_order=decision.symbol_order,
        entry_member_closes=item.features.current_closes,
        _factory_token=_POSITION_FACTORY_TOKEN,
    )


def _copy_exit_with_sticky_inconclusive(
    decision: FamilyCExitDecisionV2,
) -> FamilyCExitDecisionV2:
    return FamilyCExitDecisionV2(
        entry_event_id=decision.entry_event_id,
        attempt_id=decision.attempt_id,
        symbol=decision.symbol,
        venue=decision.venue,
        promoting_plan_sha256=decision.promoting_plan_sha256,
        source_root_sha256=decision.source_root_sha256,
        universe_root_sha256=decision.universe_root_sha256,
        episode_ledger_root_sha256=decision.episode_ledger_root_sha256,
        exit_source_root_sha256=decision.exit_source_root_sha256,
        bar_open_ms=decision.bar_open_ms,
        bar_close_ms=decision.bar_close_ms,
        decision_cutoff_ms=decision.decision_cutoff_ms,
        action=decision.action,
        reason=decision.reason,
        reasons=(
            *decision.reasons,
            "EPISODE_STICKY_MISSING_MASK_INCONCLUSIVE",
        ),
        invalidation=decision.invalidation,
        interval_status=FamilyCIntervalStatusV2.INCONCLUSIVE_DATA,
        asset_move=decision.asset_move,
        market_move=decision.market_move,
        catch_h=decision.catch_h,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _exit_provenance(
    item: FamilyCExitInputV2,
    ledger_root_sha256: str,
) -> _FamilyCExitProvenanceV2:
    return {
        "entry_event_id": item.position.entry_event_id,
        "attempt_id": item.position.attempt_id,
        "symbol": item.position.symbol,
        "venue": item.position.venue,
        "promoting_plan_sha256": item.position.promoting_plan_sha256,
        "source_root_sha256": item.position.source_root_sha256,
        "universe_root_sha256": item.position.universe_root_sha256,
        "episode_ledger_root_sha256": ledger_root_sha256,
        "exit_source_root_sha256": item.exit_source_root_sha256,
        "bar_open_ms": item.bar_open_ms,
        "bar_close_ms": item.bar_close_ms,
        "decision_cutoff_ms": item.decision_cutoff_ms,
    }


def _feature_not_ready(
    context: _FamilyCFeatureBuildContextV2,
    status: FamilyCFeatureStatusV2,
    reason: str,
    member_set: tuple[str, ...],
    prior_count: int,
) -> FamilyCFeatureSnapshotV2:
    return FamilyCFeatureSnapshotV2(
        venue=context.venue,
        promoting_plan_sha256=context.promoting_plan_sha256,
        source_root_sha256=context.source_root_sha256,
        universe_root_sha256=context.universe_root_sha256,
        panel_root_sha256=context.panel_root_sha256,
        bar_open_ms=context.bar_open_ms,
        bar_close_ms=context.bar_close_ms,
        decision_cutoff_ms=context.decision_cutoff_ms,
        latest_source_event_ms=context.latest_source_event_ms,
        latest_source_receipt_ms=context.latest_source_receipt_ms,
        current_closes=context.current_closes,
        status=status,
        reasons=(reason,),
        member_set=member_set,
        prior_observation_count=prior_count,
        _factory_token=_FEATURE_FACTORY_TOKEN,
    )


def _entry_status_for_feature_status(
    status: FamilyCFeatureStatusV2,
) -> FamilyCEntryStatusV2:
    mapping = {
        FamilyCFeatureStatusV2.FEATURE_NOT_READY_HISTORY: (
            FamilyCEntryStatusV2.FEATURE_NOT_READY_HISTORY
        ),
        FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_MARKET_VARIANCE: (
            FamilyCEntryStatusV2.FEATURE_NOT_READY_ZERO_MARKET_VARIANCE
        ),
        FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_SCALE: (
            FamilyCEntryStatusV2.FEATURE_NOT_READY_ZERO_SCALE
        ),
        FamilyCFeatureStatusV2.INCONCLUSIVE_CROSS_SECTION: (
            FamilyCEntryStatusV2.INCONCLUSIVE_CROSS_SECTION
        ),
        FamilyCFeatureStatusV2.DATA_INVALID: FamilyCEntryStatusV2.DATA_INVALID,
    }
    if status not in mapping:
        raise FamilyCContractError("READY feature status requires normal entry evaluation")
    return mapping[status]


def _entry_no_action(
    item: FamilyCEntryInputV2,
    ledger_root_sha256: str,
    status: FamilyCEntryStatusV2,
    reasons: tuple[str, ...],
    *,
    symbol_order: tuple[str, ...] = (),
    selected_rank: int | None = None,
) -> FamilyCEntryDecisionV2:
    return FamilyCEntryDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.target_symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        source_root_sha256=item.source_root_sha256,
        universe_root_sha256=item.universe_root_sha256,
        feature_evidence_sha256=item.features.feature_evidence_sha256,
        episode_ledger_root_sha256=ledger_root_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        status=status,
        side=None,
        reasons=reasons,
        invalidation=(
            "ACTIVE_POSITION_UNCHANGED"
            if status is FamilyCEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION
            else "NO_POSITION_NO_INVALIDATION"
        ),
        selected_rank=selected_rank,
        beta=None,
        m3=None,
        r_i3=None,
        g0=None,
        entry_member_set=item.features.member_set,
        symbol_order=symbol_order,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _exit(
    item: FamilyCExitInputV2,
    ledger_root_sha256: str,
    action: FamilyCExitActionV2,
    reason: FamilyCExitReasonV2,
    detail: str,
) -> FamilyCExitDecisionV2:
    return FamilyCExitDecisionV2(
        **_exit_provenance(item, ledger_root_sha256),
        action=action,
        reason=reason,
        reasons=(detail,),
        invalidation="POSITION_EXIT_REQUIRED",
        interval_status=FamilyCIntervalStatusV2.INCONCLUSIVE_DATA,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _exit_with_metrics(
    item: FamilyCExitInputV2,
    ledger_root_sha256: str,
    action: FamilyCExitActionV2,
    reason: FamilyCExitReasonV2,
    detail: str,
    asset_move: Decimal,
    market_move: Decimal,
    catch_h: Decimal,
) -> FamilyCExitDecisionV2:
    return FamilyCExitDecisionV2(
        **_exit_provenance(item, ledger_root_sha256),
        action=action,
        reason=reason,
        reasons=(detail,),
        invalidation="POSITION_EXIT_REQUIRED",
        interval_status=FamilyCIntervalStatusV2.COMPLETE,
        asset_move=asset_move,
        market_move=market_move,
        catch_h=catch_h,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _population_variance(values: tuple[Decimal, ...]) -> Decimal:
    with localcontext(protocol_decimal_context_v2()):
        count = Decimal(len(values))
        mean = sum(values, start=Decimal(0)) / count
        return (
            sum(
                ((value - mean) ** 2 for value in values),
                start=Decimal(0),
            )
            / count
        )


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise FamilyCContractError("member-complete median requires at least one value")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    with localcontext(protocol_decimal_context_v2()):
        return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _mad_decimal(values: tuple[Decimal, ...]) -> Decimal:
    location = _median_decimal(values)
    return _median_decimal(tuple(abs(value - location) for value in values))


def _clip_beta(beta_raw: Decimal) -> Decimal:
    return min(_BETA_MAX, max(_BETA_MIN, beta_raw))


def _unique_closes(
    closes: tuple[FamilyCSymbolCloseV2, ...],
    field_name: str,
) -> dict[str, Decimal]:
    if type(closes) is not tuple:
        raise FamilyCContractError(f"{field_name} must be an immutable tuple")
    if any(not isinstance(item, FamilyCSymbolCloseV2) for item in closes):
        raise FamilyCContractError(f"{field_name} contains an unsupported value")
    result = {item.symbol: item.close for item in closes}
    if len(result) != len(closes):
        raise FamilyCContractError(f"{field_name} cannot contain duplicate symbols")
    return result


def _normalized_member_set(symbols: tuple[str, ...]) -> tuple[str, ...]:
    if type(symbols) is not tuple or not symbols:
        raise FamilyCContractError("member_set must be a non-empty immutable tuple")
    for symbol in symbols:
        _validate_symbol(symbol)
    if len(set(symbols)) != len(symbols):
        raise FamilyCContractError("member_set cannot contain duplicate symbols")
    return tuple(sorted(symbols, key=_symbol_key))


def canonical_family_c_prior_universe_v2(
    universe: FamilyCPriorUniverseV2,
) -> bytes:
    if not isinstance(universe, FamilyCPriorUniverseV2):
        raise FamilyCContractError("universe must be FamilyCPriorUniverseV2")
    expected = hashlib.sha256(
        _UNIVERSE_ROOT_DOMAIN + canonical_json_line(_universe_document(universe))
    ).hexdigest()
    if universe.universe_root_sha256 != expected:
        raise FamilyCContractError("universe root differs from canonical lineage")
    return canonical_json_line(
        {
            **_universe_document(universe),
            "universe_root_sha256": universe.universe_root_sha256,
        }
    )


def canonical_family_c_candle_panel_v2(panel: FamilyCCandlePanelV2) -> bytes:
    if not isinstance(panel, FamilyCCandlePanelV2):
        raise FamilyCContractError("panel must be FamilyCCandlePanelV2")
    rebuilt = FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=panel.universe,
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=panel.candles,
    )
    if (
        panel.symbol_slice_sha256s != rebuilt.symbol_slice_sha256s
        or panel.panel_root_sha256 != rebuilt.panel_root_sha256
    ):
        raise FamilyCContractError("panel root differs from canonical candle slices")
    return canonical_json_line(
        {
            "current_bar_close_ms": panel.current_bar_close_ms,
            "current_bar_open_ms": panel.current_bar_open_ms,
            "decision_cutoff_ms": panel.decision_cutoff_ms,
            "panel_root_sha256": panel.panel_root_sha256,
            "promoting_plan_sha256": panel.promoting_plan_sha256,
            "schema_version": "r4b_family_c_candle_panel_evidence_v2",
            "source_root_sha256": panel.source_root_sha256,
            "symbol_slice_sha256s": [
                {"sha256": digest, "symbol": symbol}
                for symbol, digest in panel.symbol_slice_sha256s
            ],
            "universe_root_sha256": panel.universe.universe_root_sha256,
            "venue": panel.venue.value,
        }
    )


def canonical_family_c_feature_evidence_v2(
    evidence: FamilyCFeatureSnapshotV2,
) -> bytes:
    if not isinstance(evidence, FamilyCFeatureSnapshotV2):
        raise FamilyCContractError("evidence must be FamilyCFeatureSnapshotV2")
    expected = hashlib.sha256(
        _FEATURE_HASH_DOMAIN + canonical_json_line(_feature_document(evidence))
    ).hexdigest()
    if evidence.feature_evidence_sha256 != expected:
        raise FamilyCContractError("feature evidence hash differs from canonical payload")
    return canonical_json_line(
        {
            **_feature_document(evidence),
            "feature_evidence_sha256": evidence.feature_evidence_sha256,
        }
    )


def canonical_family_c_entry_decision_v2(
    decision: FamilyCEntryDecisionV2,
) -> bytes:
    if not isinstance(decision, FamilyCEntryDecisionV2):
        raise FamilyCContractError("decision must be FamilyCEntryDecisionV2")
    expected = hashlib.sha256(
        _ENTRY_PAYLOAD_DOMAIN
        + canonical_json_line(_entry_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise FamilyCContractError("entry payload hash differs from canonical decision")
    return canonical_json_line(_entry_decision_document(decision, include_payload_hash=True))


def parse_canonical_family_c_entry_decision_v2(
    payload: bytes,
) -> FamilyCEntryDecisionV2:
    """Parse an exact canonical Family C entry decision and rederive its seals."""

    if type(payload) is not bytes or not payload:
        raise FamilyCContractError("entry decision payload must be non-empty bytes")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FamilyCContractError("entry decision payload is invalid UTF-8 JSON") from error
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FamilyCContractError("entry decision payload must be canonical JSONL")
    decision = _family_c_decision_from_replay_document(document)
    if not isinstance(decision, FamilyCEntryDecisionV2):
        raise FamilyCContractError("entry decision payload contains an exit decision")
    if canonical_family_c_entry_decision_v2(decision) != payload:
        raise FamilyCContractError("entry decision payload does not rederive exactly")
    return decision


def canonical_family_c_exit_decision_v2(
    decision: FamilyCExitDecisionV2,
) -> bytes:
    if not isinstance(decision, FamilyCExitDecisionV2):
        raise FamilyCContractError("decision must be FamilyCExitDecisionV2")
    expected = hashlib.sha256(
        _EXIT_PAYLOAD_DOMAIN
        + canonical_json_line(_exit_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise FamilyCContractError("exit payload hash differs from canonical decision")
    return canonical_json_line(_exit_decision_document(decision, include_payload_hash=True))


def _entry_logical_event_id(item: FamilyCEntryInputV2) -> str:
    identity = {
        "attempt_id": item.attempt_id,
        "bar_open_ms": item.bar_open_ms,
        "family": "C",
        "promoting_plan_sha256": item.promoting_plan_sha256,
        "role": "ENTRY_DECISION",
        "rule_version": FAMILY_C_RULE_VERSION_V2,
        "symbol": item.target_symbol,
        "venue": item.venue.value,
    }
    return hashlib.sha256(_ENTRY_ID_DOMAIN + canonical_json_line(identity)).hexdigest()


def _exit_logical_event_id(item: FamilyCExitInputV2) -> str:
    identity = {
        "attempt_id": item.position.attempt_id,
        "bar_open_ms": item.bar_open_ms,
        "entry_event_id": item.position.entry_event_id,
        "family": "C",
        "promoting_plan_sha256": item.position.promoting_plan_sha256,
        "role": "EXIT_DECISION",
        "rule_version": FAMILY_C_RULE_VERSION_V2,
        "symbol": item.position.symbol,
        "venue": item.position.venue.value,
    }
    return hashlib.sha256(_EXIT_ID_DOMAIN + canonical_json_line(identity)).hexdigest()


def _entry_identity_document(
    decision: FamilyCEntryDecisionV2,
) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "family": "C",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "ENTRY_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _exit_identity_document(
    decision: FamilyCExitDecisionV2,
) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "entry_event_id": decision.entry_event_id,
        "family": "C",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "EXIT_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _entry_decision_document(
    decision: FamilyCEntryDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_entry_identity_document(decision),
        "bar_close_ms": decision.bar_close_ms,
        "beta": None if decision.beta is None else str(decision.beta),
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "entry_member_set": list(decision.entry_member_set),
        "episode_ledger_root_sha256": decision.episode_ledger_root_sha256,
        "event_id": decision.event_id,
        "feature_evidence_sha256": decision.feature_evidence_sha256,
        "g0": None if decision.g0 is None else str(decision.g0),
        "invalidation": decision.invalidation,
        "m3": None if decision.m3 is None else str(decision.m3),
        "r_i3": None if decision.r_i3 is None else str(decision.r_i3),
        "reasons": list(decision.reasons),
        "selected_rank": decision.selected_rank,
        "side": None if decision.side is None else decision.side.value,
        "source_root_sha256": decision.source_root_sha256,
        "status": decision.status.value,
        "symbol_order": list(decision.symbol_order),
        "universe_root_sha256": decision.universe_root_sha256,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _exit_decision_document(
    decision: FamilyCExitDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_exit_identity_document(decision),
        "action": decision.action.value,
        "asset_move": (None if decision.asset_move is None else str(decision.asset_move)),
        "bar_close_ms": decision.bar_close_ms,
        "catch_h": None if decision.catch_h is None else str(decision.catch_h),
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "episode_ledger_root_sha256": decision.episode_ledger_root_sha256,
        "event_id": decision.event_id,
        "exit_source_root_sha256": decision.exit_source_root_sha256,
        "interval_status": decision.interval_status.value,
        "invalidation": decision.invalidation,
        "market_move": (None if decision.market_move is None else str(decision.market_move)),
        "reason": decision.reason.value,
        "reasons": list(decision.reasons),
        "source_root_sha256": decision.source_root_sha256,
        "universe_root_sha256": decision.universe_root_sha256,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _entry_input_sha256(item: FamilyCEntryInputV2) -> str:
    return hashlib.sha256(
        _ENTRY_INPUT_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": item.attempt_id,
                "bar_close_ms": item.bar_close_ms,
                "bar_open_ms": item.bar_open_ms,
                "decision_cutoff_ms": item.decision_cutoff_ms,
                "feature_evidence_sha256": item.features.feature_evidence_sha256,
                "promoting_plan_sha256": item.promoting_plan_sha256,
                "source_root_sha256": item.source_root_sha256,
                "symbol": item.target_symbol,
                "universe_root_sha256": item.universe_root_sha256,
                "venue": item.venue.value,
            }
        )
    ).hexdigest()


def _exit_input_sha256(item: FamilyCExitInputV2) -> str:
    return hashlib.sha256(
        _EXIT_INPUT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": item.bar_close_ms,
                "bar_open_ms": item.bar_open_ms,
                "decision_cutoff_ms": item.decision_cutoff_ms,
                "entry_event_id": item.position.entry_event_id,
                "exit_source_root_sha256": item.exit_source_root_sha256,
                "mandatory_exit": (
                    None if item.mandatory_exit is None else item.mandatory_exit.value
                ),
                "member_moves": [
                    {"log_move": str(move.log_move), "symbol": move.symbol}
                    for move in sorted(
                        item.member_moves,
                        key=lambda value: _symbol_key(value.symbol),
                    )
                ],
                "position_sha256": _position_sha256(item.position),
                "promoting_plan_sha256": item.position.promoting_plan_sha256,
                "symbol": item.position.symbol,
                "venue": item.position.venue.value,
            }
        )
    ).hexdigest()


def _position_sha256(position: FamilyCPositionV2) -> str:
    return hashlib.sha256(
        _POSITION_PAYLOAD_DOMAIN
        + canonical_json_line(
            {
                "admission_evidence_sha256": position.admission_evidence_sha256,
                "attempt_id": position.attempt_id,
                "beta": str(position.beta),
                "entry_event_id": position.entry_event_id,
                "entry_ledger_root_sha256": position.entry_ledger_root_sha256,
                "entry_member_closes": [
                    {"close": str(item.close), "symbol": item.symbol}
                    for item in position.entry_member_closes
                ],
                "entry_member_set": list(position.entry_member_set),
                "entry_vwap": str(position.entry_vwap),
                "feature_evidence_sha256": position.feature_evidence_sha256,
                "g0": str(position.g0),
                "m3": str(position.m3),
                "paper_decision_event_id": position.paper_decision_event_id,
                "paper_decision_payload_sha256": (position.paper_decision_payload_sha256),
                "paper_executable_notional": str(position.paper_executable_notional),
                "paper_filled_quantity": str(position.paper_filled_quantity),
                "paper_registry_checkpoint_sha256": (position.paper_registry_checkpoint_sha256),
                "paper_registry_event_count": position.paper_registry_event_count,
                "paper_registry_root_sha256": position.paper_registry_root_sha256,
                "paper_requested_quantity": str(position.paper_requested_quantity),
                "promoting_plan_sha256": position.promoting_plan_sha256,
                "r_i3": str(position.r_i3),
                "schema_version": "r4b_family_c_paper_admitted_position_v2",
                "side": position.side.value,
                "signal_bar_open_ms": position.signal_bar_open_ms,
                "source_root_sha256": position.source_root_sha256,
                "symbol": position.symbol,
                "symbol_order": list(position.symbol_order),
                "universe_root_sha256": position.universe_root_sha256,
                "venue": position.venue.value,
            }
        )
    ).hexdigest()


def _family_c_exit_matches_position(
    decision: FamilyCExitDecisionV2,
    position: FamilyCPositionV2,
) -> bool:
    identity_matches = (
        decision.entry_event_id,
        decision.attempt_id,
        decision.symbol,
        decision.venue,
        decision.promoting_plan_sha256,
        decision.source_root_sha256,
        decision.universe_root_sha256,
    ) == (
        position.entry_event_id,
        position.attempt_id,
        position.symbol,
        position.venue,
        position.promoting_plan_sha256,
        position.source_root_sha256,
        position.universe_root_sha256,
    )
    if not identity_matches or not decision.exits_position:
        return identity_matches
    expected_action = (
        FamilyCExitActionV2.EXIT_LONG
        if position.side is FamilyCSideV2.LONG
        else FamilyCExitActionV2.EXIT_SHORT
    )
    return decision.action is expected_action


def _closed_candle_document(value: FamilyCClosedCandleV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close": str(value.close),
        "closed": value.closed,
        "event_time_ms": value.event_time_ms,
        "receipt_time_ms": value.receipt_time_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
    }


def _universe_document(value: FamilyCPriorUniverseV2) -> dict[str, object]:
    return {
        "effective_day_start_ms": value.effective_day_start_ms,
        "eligibility_cutoff_ms": value.eligibility_cutoff_ms,
        "members": list(value.members),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_version": "r4b_family_c_prior_universe_v2",
        "source_root_sha256": value.source_root_sha256,
        "venue": value.venue.value,
    }


def _feature_document(value: FamilyCFeatureSnapshotV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "breadth_count": value.breadth_count,
        "current_closes": [
            {"close": str(item.close), "symbol": item.symbol} for item in value.current_closes
        ],
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "m3_current": (None if value.m3_current is None else str(value.m3_current)),
        "member_set": list(value.member_set),
        "members": [
            {
                "beta": str(item.beta),
                "beta_raw": str(item.beta_raw),
                "current_three_bar_return": str(item.current_three_bar_return),
                "g0": str(item.g0),
                "lag_score": str(item.lag_score),
                "residual_scale": str(item.residual_scale),
                "symbol": item.symbol,
            }
            for item in value.members
        ],
        "panel_root_sha256": value.panel_root_sha256,
        "prior_observation_count": value.prior_observation_count,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "reasons": list(value.reasons),
        "schema_version": "r4b_family_c_feature_evidence_v2",
        "shock_scale": (None if value.shock_scale is None else str(value.shock_scale)),
        "shock_score": (None if value.shock_score is None else str(value.shock_score)),
        "source_root_sha256": value.source_root_sha256,
        "status": value.status.value,
        "universe_root_sha256": value.universe_root_sha256,
        "venue": value.venue.value,
    }


def _seal_feature_evidence(value: FamilyCFeatureSnapshotV2) -> None:
    digest = hashlib.sha256(
        _FEATURE_HASH_DOMAIN + canonical_json_line(_feature_document(value))
    ).hexdigest()
    object.__setattr__(value, "feature_evidence_sha256", digest)


def _validate_entry_decision_state(decision: FamilyCEntryDecisionV2) -> None:
    if not isinstance(decision.status, FamilyCEntryStatusV2):
        raise FamilyCContractError("status must be FamilyCEntryStatusV2")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    member_set = _normalized_member_set(decision.entry_member_set)
    object.__setattr__(decision, "entry_member_set", member_set)
    if type(decision.symbol_order) is not tuple:
        raise FamilyCContractError("symbol_order must be an immutable tuple")
    if decision.symbol_order:
        if len(set(decision.symbol_order)) != len(decision.symbol_order) or set(
            decision.symbol_order
        ) != set(member_set):
            raise FamilyCContractError("symbol_order must permute the entry member set")
        if (
            type(decision.selected_rank) is not int
            or not 1 <= decision.selected_rank <= len(decision.symbol_order)
            or decision.symbol_order[decision.selected_rank - 1] != decision.symbol
        ):
            raise FamilyCContractError("selected_rank differs from symbol_order")
    elif decision.selected_rank is not None:
        raise FamilyCContractError("selected_rank requires a frozen symbol_order")
    if decision.status is FamilyCEntryStatusV2.SIGNAL:
        if not isinstance(decision.side, FamilyCSideV2):
            raise FamilyCContractError("SIGNAL requires LONG or SHORT side")
        if not decision.symbol_order or decision.selected_rank is None:
            raise FamilyCContractError("SIGNAL requires rank and symbol order")
        if not all(
            _is_finite_decimal(value)
            for value in (decision.beta, decision.m3, decision.r_i3, decision.g0)
        ):
            raise FamilyCContractError("SIGNAL requires finite frozen metrics")
        assert decision.beta is not None
        assert decision.m3 is not None
        assert decision.r_i3 is not None
        assert decision.g0 is not None
        if not _BETA_MIN <= decision.beta <= _BETA_MAX or decision.g0 <= 0:
            raise FamilyCContractError("SIGNAL beta or g0 escapes its contract")
        expected_side = FamilyCSideV2.LONG if decision.m3 > 0 else FamilyCSideV2.SHORT
        if decision.m3 == 0 or decision.side is not expected_side:
            raise FamilyCContractError("SIGNAL side differs from m3 sign")
        with localcontext(protocol_decimal_context_v2()):
            expected_g0 = Decimal(_sign(decision.m3)) * (
                decision.beta * decision.m3 - decision.r_i3
            )
        if decision.g0 != expected_g0:
            raise FamilyCContractError("SIGNAL g0 differs from frozen formula")
        return
    if any(
        value is not None
        for value in (
            decision.side,
            decision.beta,
            decision.m3,
            decision.r_i3,
            decision.g0,
        )
    ):
        raise FamilyCContractError("non-signal decision cannot expose signal metrics")


def _validate_exit_decision_state(decision: FamilyCExitDecisionV2) -> None:
    if not isinstance(decision.action, FamilyCExitActionV2) or not isinstance(
        decision.reason,
        FamilyCExitReasonV2,
    ):
        raise FamilyCContractError("exit action and reason must use Family C enums")
    if not isinstance(decision.interval_status, FamilyCIntervalStatusV2):
        raise FamilyCContractError("interval_status must use Family C enum")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    hold_reasons = {
        FamilyCExitReasonV2.HOLD,
        FamilyCExitReasonV2.MISSING_MEMBER_INCONCLUSIVE,
    }
    if (decision.action is FamilyCExitActionV2.HOLD) != (decision.reason in hold_reasons):
        raise FamilyCContractError("exit HOLD action and reason disagree")
    metrics = (decision.asset_move, decision.market_move, decision.catch_h)
    present_count = sum(value is not None for value in metrics)
    if present_count not in (0, 3):
        raise FamilyCContractError("exit metrics must be all present or all absent")
    if present_count == 3 and not all(_is_finite_decimal(value) for value in metrics):
        raise FamilyCContractError("exit metrics must be finite Decimal")
    if decision.interval_status is FamilyCIntervalStatusV2.COMPLETE and present_count != 3:
        raise FamilyCContractError("COMPLETE exit decision requires all metrics")
    if (
        decision.reason
        in (
            FamilyCExitReasonV2.ADVERSE_WIDENING,
            FamilyCExitReasonV2.CATCHUP_COMPLETE,
        )
        and present_count != 3
    ):
        raise FamilyCContractError("metric exit reason requires frozen metrics")


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 32:
        raise FamilyCContractError("reasons must be a non-empty bounded tuple")
    for value in values:
        _validate_identity(value, "reason")


def _symbol_key(symbol: str) -> bytes:
    return symbol.encode("utf-8")


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _require_decimal(value: Decimal | None) -> Decimal:
    if value is None or not value.is_finite():
        raise FamilyCContractError("expected finite Decimal after readiness validation")
    return value


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0


def _validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 256:
        raise FamilyCContractError(f"{field_name} must be a bounded normalized identity")


def _validate_symbol(symbol: str) -> None:
    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise FamilyCContractError("symbol must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise FamilyCContractError(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FamilyCContractError(f"{field_name} must be a nonnegative integer")


def _validate_bar_times(bar_open_ms: int, bar_close_ms: int, decision_cutoff_ms: int) -> None:
    try:
        _decision_clock.validate_decision_bar_v2(
            bar_open_ms,
            bar_close_ms,
            decision_cutoff_ms,
        )
    except _decision_clock.DecisionClockContractErrorV2 as exc:
        raise FamilyCContractError(str(exc)) from exc
