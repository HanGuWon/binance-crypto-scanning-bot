"""Bounded historical numeric precomputation for repeated 5-minute anchors.

The caches in this module are source-neutral, historical-only accelerators.
They derive immutable return/flow series once, expose fixed-size causal windows
by arithmetic timestamp index, and delegate all directional arithmetic to the
existing frozen price, participation, and cross-sectional calculation owners.
They do not grant M0/M1/M2, live, probability, efficacy, or promotion authority.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException, localcontext
from typing import Final, Literal

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import ROBUST_Z_PRIOR_WINDOW_V2
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
    HistoricalCrossSectional7AssetCalculationV2,
    calculate_historical_cross_sectional_7asset_returns_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    ParticipationFlowBarValueV2,
    ParticipationFlowCalculationV2,
    build_participation_flow_bar_value_v2,
    calculate_participation_flow_v2,
    canonical_participation_flow_bar_value_v2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    PriceClosePathCalculationV2,
    calculate_price_return_series_v2,
)

HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2: Final = (
    "R4B_HISTORICAL_NUMERIC_PRECOMPUTE_V2.1.0_TARGET_SCOPED_NONPROMOTING"
)
HISTORICAL_NUMERIC_PRECOMPUTE_MIN_ROWS_V2: Final = (
    ROBUST_Z_PRIOR_WINDOW_V2 + 13
)
HISTORICAL_NUMERIC_PRECOMPUTE_MAX_ROWS_V2: Final = 1_000_000

_PRICE_RETURN_COUNT: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 1
_PARTICIPATION_BAR_COUNT: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 1
_CROSS_PRIOR_COUNT: Final = ROBUST_Z_PRIOR_WINDOW_V2
_CROSS_SOURCE_ROW_COUNT: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 4
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_R3_CACHE_DOMAIN: Final = b"R4B_HISTORICAL_R3_SERIES_CACHE_V2\0"
_TARGET_CACHE_DOMAIN: Final = b"R4B_HISTORICAL_TARGET_NUMERIC_CACHE_V2\0"
_CROSS_CACHE_DOMAIN: Final = b"R4B_HISTORICAL_TARGET_EXCLUDED_R3_CACHE_V2\0"
_CLOSE_SERIES_DOMAIN: Final = b"R4B_HISTORICAL_CLOSE_SERIES_V2\0"
_RETURN_1_SERIES_DOMAIN: Final = b"R4B_HISTORICAL_RETURN_1_SERIES_V2\0"
_RETURN_12_SERIES_DOMAIN: Final = b"R4B_HISTORICAL_RETURN_12_SERIES_V2\0"
_RETURN_3_SERIES_DOMAIN: Final = b"R4B_HISTORICAL_RETURN_3_SERIES_V2\0"
_MEDIAN_RETURN_3_SERIES_DOMAIN: Final = (
    b"R4B_HISTORICAL_EX_TARGET_MEDIAN_RETURN_3_SERIES_V2\0"
)
_FLOW_BAR_SERIES_DOMAIN: Final = b"R4B_HISTORICAL_FLOW_BAR_SERIES_V2\0"
_PRICE_SLICE_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_PRICE_NUMERIC_SLICE_V2\0"
_PARTICIPATION_SLICE_DOMAIN: Final = (
    b"R4B_HISTORICAL_CENSUS_PARTICIPATION_NUMERIC_SLICE_V2\0"
)
_PEER_PATH_SLICE_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_PATH_V2\0"
_PEER_INPUT_SLICE_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_INPUT_V2\0"
_R3_FACTORY_TOKEN: Final = object()
_TARGET_FACTORY_TOKEN: Final = object()
_CROSS_FACTORY_TOKEN: Final = object()
_TARGET_INPUT_FACTORY_TOKEN: Final = object()
_CROSS_INPUT_FACTORY_TOKEN: Final = object()


class HistoricalNumericPrecomputeContractErrorV2(ValueError):
    """Raised when a bounded historical numeric cache contract is broken."""


@dataclass(frozen=True, slots=True)
class HistoricalR3SeriesCacheV2:
    """One immutable dataset's three-bar returns without retaining candles."""

    symbol: str
    dataset_sha256: str
    manifest_sha256: str
    first_bar_open_ms: int
    final_bar_open_ms: int
    row_count: int
    returns_3: tuple[Decimal, ...] = field(repr=False)
    close_series_sha256: str
    _factory_token: InitVar[object | None] = None
    returns_3_sha256: str = field(init=False)
    cache_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
    )
    historical_only: Literal[True] = field(init=False, default=True)
    numeric_only: Literal[True] = field(init=False, default=True)
    live_authority: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _R3_FACTORY_TOKEN:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical R3 caches require their sealed factory"
            )
        returns_hash = _decimal_series_sha256(
            _RETURN_3_SERIES_DOMAIN,
            first_open_ms=self.first_return_open_ms,
            values=self.returns_3,
        )
        object.__setattr__(self, "returns_3_sha256", returns_hash)
        _validate_r3_cache(self, recompute_series_hash=False)
        object.__setattr__(
            self,
            "cache_sha256",
            hashlib.sha256(
                _R3_CACHE_DOMAIN + canonical_json_line(_r3_cache_document(self, False))
            ).hexdigest(),
        )

    @property
    def first_bar_close_ms(self) -> int:
        return self.first_bar_open_ms + FIVE_MINUTE_MS_V2 - 1

    @property
    def final_bar_close_ms(self) -> int:
        return self.final_bar_open_ms + FIVE_MINUTE_MS_V2 - 1

    @property
    def first_return_open_ms(self) -> int:
        return self.first_bar_open_ms + 3 * FIVE_MINUTE_MS_V2

    def return_3_at(self, bar_open_ms: int) -> Decimal:
        """Return one precomputed R3 value by O(1) arithmetic lookup."""

        return self.returns_3[_series_index(
            bar_open_ms=bar_open_ms,
            first_value_open_ms=self.first_return_open_ms,
            value_count=len(self.returns_3),
            label=f"{self.symbol} R3",
        )]


@dataclass(frozen=True, slots=True)
class HistoricalTargetNumericCacheV2:
    """Target-scoped R1/R12 and participation series for repeated anchors."""

    source_r3_cache: HistoricalR3SeriesCacheV2 = field(repr=False)
    returns_1: tuple[Decimal, ...] = field(repr=False)
    returns_12: tuple[Decimal, ...] = field(repr=False)
    participation_bars: tuple[ParticipationFlowBarValueV2, ...] = field(repr=False)
    _factory_token: InitVar[object | None] = None
    returns_1_sha256: str = field(init=False)
    returns_12_sha256: str = field(init=False)
    participation_bars_sha256: str = field(init=False)
    cache_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
    )
    historical_only: Literal[True] = field(init=False, default=True)
    target_scoped: Literal[True] = field(init=False, default=True)
    numeric_only: Literal[True] = field(init=False, default=True)
    live_authority: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _TARGET_FACTORY_TOKEN:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical target caches require their sealed factory"
            )
        object.__setattr__(
            self,
            "returns_1_sha256",
            _decimal_series_sha256(
                _RETURN_1_SERIES_DOMAIN,
                first_open_ms=self.first_return_1_open_ms,
                values=self.returns_1,
            ),
        )
        object.__setattr__(
            self,
            "returns_12_sha256",
            _decimal_series_sha256(
                _RETURN_12_SERIES_DOMAIN,
                first_open_ms=self.first_return_12_open_ms,
                values=self.returns_12,
            ),
        )
        object.__setattr__(
            self,
            "participation_bars_sha256",
            _flow_bar_series_sha256(self.participation_bars),
        )
        _validate_target_cache(self, recompute_component_hashes=False)
        object.__setattr__(
            self,
            "cache_sha256",
            hashlib.sha256(
                _TARGET_CACHE_DOMAIN
                + canonical_json_line(_target_cache_document(self, False))
            ).hexdigest(),
        )

    @property
    def symbol(self) -> str:
        return self.source_r3_cache.symbol

    @property
    def dataset_sha256(self) -> str:
        return self.source_r3_cache.dataset_sha256

    @property
    def manifest_sha256(self) -> str:
        return self.source_r3_cache.manifest_sha256

    @property
    def first_bar_open_ms(self) -> int:
        return self.source_r3_cache.first_bar_open_ms

    @property
    def final_bar_open_ms(self) -> int:
        return self.source_r3_cache.final_bar_open_ms

    @property
    def final_bar_close_ms(self) -> int:
        return self.source_r3_cache.final_bar_close_ms

    @property
    def row_count(self) -> int:
        return self.source_r3_cache.row_count

    @property
    def first_return_1_open_ms(self) -> int:
        return self.first_bar_open_ms + FIVE_MINUTE_MS_V2

    @property
    def first_return_12_open_ms(self) -> int:
        return self.first_bar_open_ms + 12 * FIVE_MINUTE_MS_V2

    def anchor_inputs_at(self, final_bar_open_ms: int) -> HistoricalTargetAnchorInputsV2:
        """Return fixed-size causal inputs using an O(1) timestamp index."""

        returns_1 = _fixed_series_window(
            values=self.returns_1,
            first_value_open_ms=self.first_return_1_open_ms,
            final_open_ms=final_bar_open_ms,
            count=_PRICE_RETURN_COUNT,
            label=f"{self.symbol} R1",
        )
        returns_12 = _fixed_series_window(
            values=self.returns_12,
            first_value_open_ms=self.first_return_12_open_ms,
            final_open_ms=final_bar_open_ms,
            count=_PRICE_RETURN_COUNT,
            label=f"{self.symbol} R12",
        )
        bars = _fixed_bar_window(
            values=self.participation_bars,
            first_value_open_ms=self.first_bar_open_ms,
            final_open_ms=final_bar_open_ms,
            count=_PARTICIPATION_BAR_COUNT,
            label=f"{self.symbol} participation",
        )
        final_close_ms = final_bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        price_slice_sha256 = _target_slice_sha256(
            domain=_PRICE_SLICE_DOMAIN,
            representation="PRICE_CLOSE_PATH_DATASET_ROOT_WINDOW",
            dataset_sha256=self.dataset_sha256,
            symbol=self.symbol,
            first_open_ms=final_bar_open_ms
            - (PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2,
            final_open_ms=final_bar_open_ms,
            row_count=PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
        )
        participation_slice_sha256 = _target_slice_sha256(
            domain=_PARTICIPATION_SLICE_DOMAIN,
            representation=(
                "ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY_DATASET_ROOT_WINDOW"
            ),
            dataset_sha256=self.dataset_sha256,
            symbol=self.symbol,
            first_open_ms=final_bar_open_ms
            - (_PARTICIPATION_BAR_COUNT - 1) * FIVE_MINUTE_MS_V2,
            final_open_ms=final_bar_open_ms,
            row_count=_PARTICIPATION_BAR_COUNT,
        )
        return HistoricalTargetAnchorInputsV2(
            symbol=self.symbol,
            final_bar_open_ms=final_bar_open_ms,
            final_bar_close_ms=final_close_ms,
            returns_1=returns_1,
            returns_12=returns_12,
            prior_participation_bars=bars[:-1],
            current_participation_bar=bars[-1],
            price_source_slice_sha256=price_slice_sha256,
            participation_source_slice_sha256=participation_slice_sha256,
            _factory_token=_TARGET_INPUT_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class HistoricalTargetAnchorInputsV2:
    """One exact target anchor's frozen numeric calculation inputs."""

    symbol: str
    final_bar_open_ms: int
    final_bar_close_ms: int
    returns_1: tuple[Decimal, ...] = field(repr=False)
    returns_12: tuple[Decimal, ...] = field(repr=False)
    prior_participation_bars: tuple[ParticipationFlowBarValueV2, ...] = field(
        repr=False
    )
    current_participation_bar: ParticipationFlowBarValueV2 = field(repr=False)
    price_source_slice_sha256: str
    participation_source_slice_sha256: str
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _TARGET_INPUT_FACTORY_TOKEN:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical target anchor inputs require their cache factory"
            )
        _validate_target_anchor_inputs(self)


@dataclass(frozen=True, slots=True)
class HistoricalTargetAnchorCalculationsV2:
    """Price and participation calculations for one cached target anchor."""

    inputs: HistoricalTargetAnchorInputsV2 = field(repr=False)
    price: PriceClosePathCalculationV2
    participation: ParticipationFlowCalculationV2


@dataclass(frozen=True, slots=True)
class HistoricalTargetExcludedMedianR3CacheV2:
    """Target-specific six-peer median R3 series over their exact overlap."""

    target_symbol: str
    peer_caches: tuple[HistoricalR3SeriesCacheV2, ...] = field(repr=False)
    first_return_open_ms: int
    final_return_open_ms: int
    median_returns_3: tuple[Decimal, ...] = field(repr=False)
    _factory_token: InitVar[object | None] = None
    peer_symbols: tuple[str, ...] = field(init=False)
    median_returns_3_sha256: str = field(init=False)
    cache_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
    )
    historical_only: Literal[True] = field(init=False, default=True)
    target_scoped: Literal[True] = field(init=False, default=True)
    target_returns_used: Literal[False] = field(init=False, default=False)
    numeric_only: Literal[True] = field(init=False, default=True)
    live_authority: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CROSS_FACTORY_TOKEN:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical target-excluded R3 caches require their sealed factory"
            )
        ordered = tuple(sorted(self.peer_caches, key=lambda value: value.symbol.encode()))
        object.__setattr__(self, "peer_caches", ordered)
        object.__setattr__(self, "peer_symbols", tuple(value.symbol for value in ordered))
        object.__setattr__(
            self,
            "median_returns_3_sha256",
            _decimal_series_sha256(
                _MEDIAN_RETURN_3_SERIES_DOMAIN,
                first_open_ms=self.first_return_open_ms,
                values=self.median_returns_3,
            ),
        )
        _validate_cross_cache(self, recompute_series_hash=False)
        object.__setattr__(
            self,
            "cache_sha256",
            hashlib.sha256(
                _CROSS_CACHE_DOMAIN
                + canonical_json_line(_cross_cache_document(self, False))
            ).hexdigest(),
        )

    @property
    def constituent_symbols(self) -> tuple[str, ...]:
        return tuple(sorted((self.target_symbol, *self.peer_symbols), key=str.encode))

    def anchor_inputs_at(self, final_bar_open_ms: int) -> HistoricalCrossAnchorInputsV2:
        """Return 8,640 prior medians plus the six current peer R3 values."""

        prior = _prior_series_window(
            values=self.median_returns_3,
            first_value_open_ms=self.first_return_open_ms,
            current_open_ms=final_bar_open_ms,
            prior_count=_CROSS_PRIOR_COUNT,
            label=f"{self.target_symbol} ex-target median R3",
        )
        current = tuple(
            cache.return_3_at(final_bar_open_ms) for cache in self.peer_caches
        )
        first_path_open_ms = final_bar_open_ms - (
            _CROSS_SOURCE_ROW_COUNT - 1
        ) * FIVE_MINUTE_MS_V2
        peer_path_sha256s = tuple(
            (
                cache.symbol,
                _cross_path_sha256(
                    dataset_sha256=cache.dataset_sha256,
                    manifest_sha256=cache.manifest_sha256,
                    symbol=cache.symbol,
                    first_open_ms=first_path_open_ms,
                    final_open_ms=final_bar_open_ms,
                    row_count=_CROSS_SOURCE_ROW_COUNT,
                ),
            )
            for cache in self.peer_caches
        )
        final_close_ms = final_bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        peer_input_sha256 = _cross_input_sha256(
            target_symbol=self.target_symbol,
            final_open_ms=final_bar_open_ms,
            peer_path_sha256s=peer_path_sha256s,
        )
        return HistoricalCrossAnchorInputsV2(
            target_symbol=self.target_symbol,
            peer_symbols=self.peer_symbols,
            final_bar_open_ms=final_bar_open_ms,
            final_bar_close_ms=final_close_ms,
            prior_market_median_returns_3=prior,
            current_peer_returns_3=current,
            peer_path_sha256s=peer_path_sha256s,
            peer_input_sha256=peer_input_sha256,
            _factory_token=_CROSS_INPUT_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class HistoricalCrossAnchorInputsV2:
    """One target-excluded anchor's exact numeric and provenance inputs."""

    target_symbol: str
    peer_symbols: tuple[str, ...]
    final_bar_open_ms: int
    final_bar_close_ms: int
    prior_market_median_returns_3: tuple[Decimal, ...] = field(repr=False)
    current_peer_returns_3: tuple[Decimal, ...] = field(repr=False)
    peer_path_sha256s: tuple[tuple[str, str], ...]
    peer_input_sha256: str
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CROSS_INPUT_FACTORY_TOKEN:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical cross anchor inputs require their cache factory"
            )
        _validate_cross_anchor_inputs(self)


@dataclass(frozen=True, slots=True)
class HistoricalCrossAnchorCalculationV2:
    """One cached target-excluded cross-sectional calculation."""

    inputs: HistoricalCrossAnchorInputsV2 = field(repr=False)
    calculation: HistoricalCrossSectional7AssetCalculationV2


def build_historical_r3_series_cache_v2(
    *,
    dataset_sha256: str,
    manifest_sha256: str,
    rows: tuple[Candle, ...],
) -> HistoricalR3SeriesCacheV2:
    """Precompute one verified dataset's R3 series exactly once."""

    symbol = _validate_source_rows(
        rows,
        dataset_sha256=dataset_sha256,
        manifest_sha256=manifest_sha256,
    )
    closes = tuple(row.close for row in rows)
    try:
        with localcontext(protocol_decimal_context_v2()):
            returns_3 = tuple(
                (closes[index] / closes[index - 3]).ln()
                for index in range(3, len(closes))
            )
    except DecimalException as exc:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical R3 precomputation exceeded Decimal protocol bounds"
        ) from exc
    return HistoricalR3SeriesCacheV2(
        symbol=symbol,
        dataset_sha256=dataset_sha256,
        manifest_sha256=manifest_sha256,
        first_bar_open_ms=rows[0].open_time_ms,
        final_bar_open_ms=rows[-1].open_time_ms,
        row_count=len(rows),
        returns_3=returns_3,
        close_series_sha256=_decimal_series_sha256(
            _CLOSE_SERIES_DOMAIN,
            first_open_ms=rows[0].open_time_ms,
            values=closes,
        ),
        _factory_token=_R3_FACTORY_TOKEN,
    )


def build_historical_target_numeric_cache_v2(
    *,
    source_r3_cache: HistoricalR3SeriesCacheV2,
    rows: tuple[Candle, ...],
) -> HistoricalTargetNumericCacheV2:
    """Precompute one target's R1/R12 and flow bars without retaining rows."""

    canonical_historical_r3_series_cache_v2(source_r3_cache)
    symbol = _validate_source_rows(
        rows,
        dataset_sha256=source_r3_cache.dataset_sha256,
        manifest_sha256=source_r3_cache.manifest_sha256,
    )
    if (
        symbol != source_r3_cache.symbol
        or rows[0].open_time_ms != source_r3_cache.first_bar_open_ms
        or rows[-1].open_time_ms != source_r3_cache.final_bar_open_ms
        or len(rows) != source_r3_cache.row_count
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target rows differ from their source R3 cache identity"
        )
    closes = tuple(row.close for row in rows)
    close_hash = _decimal_series_sha256(
        _CLOSE_SERIES_DOMAIN,
        first_open_ms=rows[0].open_time_ms,
        values=closes,
    )
    if close_hash != source_r3_cache.close_series_sha256:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target close series differs from its source R3 cache"
        )
    try:
        with localcontext(protocol_decimal_context_v2()):
            returns_1 = tuple(
                (closes[index] / closes[index - 1]).ln()
                for index in range(1, len(closes))
            )
            returns_12 = tuple(
                (closes[index] / closes[index - 12]).ln()
                for index in range(12, len(closes))
            )
            participation_bars = tuple(_flow_bar_from_candle(row) for row in rows)
    except DecimalException as exc:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical target precomputation exceeded Decimal protocol bounds"
        ) from exc
    return HistoricalTargetNumericCacheV2(
        source_r3_cache=source_r3_cache,
        returns_1=returns_1,
        returns_12=returns_12,
        participation_bars=participation_bars,
        _factory_token=_TARGET_FACTORY_TOKEN,
    )


def build_historical_target_excluded_median_r3_cache_v2(
    *,
    target_symbol: str,
    peer_caches: tuple[HistoricalR3SeriesCacheV2, ...],
) -> HistoricalTargetExcludedMedianR3CacheV2:
    """Precompute one target's exact six-peer median R3 overlap once."""

    _validate_symbol(target_symbol, "target_symbol")
    if type(peer_caches) is not tuple or len(peer_caches) != (
        HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded R3 cache requires exactly 6 immutable peer caches"
        )
    if any(type(value) is not HistoricalR3SeriesCacheV2 for value in peer_caches):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "peer_caches contains an unsupported cache value"
        )
    ordered = tuple(sorted(peer_caches, key=lambda value: value.symbol.encode()))
    for cache in ordered:
        canonical_historical_r3_series_cache_v2(cache)
    symbols = tuple(value.symbol for value in ordered)
    if len(set(symbols)) != len(symbols):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "peer_caches cannot contain duplicate symbols"
        )
    if target_symbol in symbols:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target returns must be structurally excluded from peer caches"
        )
    first_open_ms = max(value.first_return_open_ms for value in ordered)
    final_open_ms = min(value.final_bar_open_ms for value in ordered)
    if first_open_ms > final_open_ms:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "peer R3 caches have no common causal overlap"
        )
    count = (final_open_ms - first_open_ms) // FIVE_MINUTE_MS_V2 + 1
    if count < _CROSS_PRIOR_COUNT + 1:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "peer R3 overlap lacks 8,640 prior plus one current return"
        )
    try:
        with localcontext(protocol_decimal_context_v2()):
            medians = tuple(
                _median_decimal(
                    tuple(
                        cache.return_3_at(
                            first_open_ms + index * FIVE_MINUTE_MS_V2
                        )
                        for cache in ordered
                    )
                )
                for index in range(count)
            )
    except DecimalException as exc:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical peer-median precomputation exceeded Decimal protocol bounds"
        ) from exc
    return HistoricalTargetExcludedMedianR3CacheV2(
        target_symbol=target_symbol,
        peer_caches=ordered,
        first_return_open_ms=first_open_ms,
        final_return_open_ms=final_open_ms,
        median_returns_3=medians,
        _factory_token=_CROSS_FACTORY_TOKEN,
    )


def calculate_historical_target_anchor_v2(
    inputs: HistoricalTargetAnchorInputsV2,
) -> HistoricalTargetAnchorCalculationsV2:
    """Delegate one cached target anchor to the frozen calculation owners."""

    if type(inputs) is not HistoricalTargetAnchorInputsV2:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "inputs must be exact HistoricalTargetAnchorInputsV2"
        )
    _validate_target_anchor_inputs(inputs)
    return HistoricalTargetAnchorCalculationsV2(
        inputs=inputs,
        price=calculate_price_return_series_v2(inputs.returns_1, inputs.returns_12),
        participation=calculate_participation_flow_v2(
            current_bar=inputs.current_participation_bar,
            prior_bars=inputs.prior_participation_bars,
        ),
    )


def calculate_historical_cross_anchor_v2(
    inputs: HistoricalCrossAnchorInputsV2,
) -> HistoricalCrossAnchorCalculationV2:
    """Delegate one cached ex-target anchor to the frozen cross owner."""

    if type(inputs) is not HistoricalCrossAnchorInputsV2:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "inputs must be exact HistoricalCrossAnchorInputsV2"
        )
    _validate_cross_anchor_inputs(inputs)
    return HistoricalCrossAnchorCalculationV2(
        inputs=inputs,
        calculation=calculate_historical_cross_sectional_7asset_returns_v2(
            prior_market_median_returns_3=inputs.prior_market_median_returns_3,
            current_peer_returns_3=inputs.current_peer_returns_3,
        ),
    )


def canonical_historical_r3_series_cache_v2(
    value: HistoricalR3SeriesCacheV2,
) -> bytes:
    """Live-validate and serialize one compact R3 cache identity."""

    if type(value) is not HistoricalR3SeriesCacheV2:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "value must be exact HistoricalR3SeriesCacheV2"
        )
    _validate_r3_cache(value, recompute_series_hash=True)
    payload = canonical_json_line(_r3_cache_document(value, False))
    if value.cache_sha256 != hashlib.sha256(_R3_CACHE_DOMAIN + payload).hexdigest():
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical R3 cache hash differs from canonical content"
        )
    return canonical_json_line(_r3_cache_document(value, True))


def canonical_historical_target_numeric_cache_v2(
    value: HistoricalTargetNumericCacheV2,
) -> bytes:
    """Live-validate and serialize one target cache identity."""

    if type(value) is not HistoricalTargetNumericCacheV2:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "value must be exact HistoricalTargetNumericCacheV2"
        )
    _validate_target_cache(value, recompute_component_hashes=True)
    payload = canonical_json_line(_target_cache_document(value, False))
    if value.cache_sha256 != hashlib.sha256(_TARGET_CACHE_DOMAIN + payload).hexdigest():
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical target cache hash differs from canonical content"
        )
    return canonical_json_line(_target_cache_document(value, True))


def canonical_historical_target_excluded_median_r3_cache_v2(
    value: HistoricalTargetExcludedMedianR3CacheV2,
) -> bytes:
    """Live-validate and serialize one target-excluded median cache identity."""

    if type(value) is not HistoricalTargetExcludedMedianR3CacheV2:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "value must be exact HistoricalTargetExcludedMedianR3CacheV2"
        )
    _validate_cross_cache(value, recompute_series_hash=True)
    payload = canonical_json_line(_cross_cache_document(value, False))
    if value.cache_sha256 != hashlib.sha256(_CROSS_CACHE_DOMAIN + payload).hexdigest():
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical target-excluded cache hash differs from canonical content"
        )
    return canonical_json_line(_cross_cache_document(value, True))


def _flow_bar_from_candle(value: Candle) -> ParticipationFlowBarValueV2:
    with localcontext(protocol_decimal_context_v2()):
        total = +value.quote_volume
        signed = Decimal(2) * value.taker_buy_quote_volume - total
        share = signed / total if total > 0 else None
    return build_participation_flow_bar_value_v2(
        bar_open_ms=value.open_time_ms,
        bar_close_ms=value.close_time_ms,
        signed_normal_notional=signed,
        normal_notional=total,
        total_trade_notional=total,
        signed_share=share,
    )


def _validate_source_rows(
    rows: tuple[Candle, ...],
    *,
    dataset_sha256: str,
    manifest_sha256: str,
) -> str:
    _validate_sha256(dataset_sha256, "dataset_sha256")
    _validate_sha256(manifest_sha256, "manifest_sha256")
    if type(rows) is not tuple:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric source rows must be an immutable tuple"
        )
    if not HISTORICAL_NUMERIC_PRECOMPUTE_MIN_ROWS_V2 <= len(rows) <= (
        HISTORICAL_NUMERIC_PRECOMPUTE_MAX_ROWS_V2
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric source row count is outside [8,653, 1,000,000]"
        )
    if any(type(value) is not Candle for value in rows):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric source contains an unsupported row value"
        )
    first = rows[0]
    _validate_symbol(first.symbol, "source symbol")
    expected_open_ms = first.open_time_ms
    if type(expected_open_ms) is not int or expected_open_ms < 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric source first timestamp is invalid"
        )
    if expected_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric source must align to the 5m UTC grid"
        )
    for index, row in enumerate(rows):
        if (
            row.market is not Market.FUTURES
            or row.symbol != first.symbol
            or row.interval != "5m"
            or row.is_closed is not True
        ):
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical numeric source requires one closed USD-M 5m identity"
            )
        open_ms = expected_open_ms + index * FIVE_MINUTE_MS_V2
        if (
            row.open_time_ms != open_ms
            or row.close_time_ms != open_ms + FIVE_MINUTE_MS_V2 - 1
        ):
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical numeric source must be ordered and contiguous"
            )
        for number in (
            row.close,
            row.quote_volume,
            row.taker_buy_quote_volume,
        ):
            if type(number) is not Decimal or not number.is_finite():
                raise HistoricalNumericPrecomputeContractErrorV2(
                    "historical numeric source requires exact finite Decimal inputs"
                )
        if (
            row.close <= 0
            or row.quote_volume < 0
            or row.taker_buy_quote_volume < 0
            or row.taker_buy_quote_volume > row.quote_volume
        ):
            raise HistoricalNumericPrecomputeContractErrorV2(
                "historical numeric source price or flow input is outside bounds"
            )
    return first.symbol


def _validate_r3_cache(
    value: HistoricalR3SeriesCacheV2,
    *,
    recompute_series_hash: bool,
) -> None:
    _validate_symbol(value.symbol, "cache symbol")
    _validate_sha256(value.dataset_sha256, "dataset_sha256")
    _validate_sha256(value.manifest_sha256, "manifest_sha256")
    _validate_sha256(value.close_series_sha256, "close_series_sha256")
    if not HISTORICAL_NUMERIC_PRECOMPUTE_MIN_ROWS_V2 <= value.row_count <= (
        HISTORICAL_NUMERIC_PRECOMPUTE_MAX_ROWS_V2
    ):
        raise HistoricalNumericPrecomputeContractErrorV2("R3 cache row_count is outside bounds")
    if (
        type(value.first_bar_open_ms) is not int
        or value.first_bar_open_ms < 0
        or value.first_bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or value.final_bar_open_ms
        != value.first_bar_open_ms + (value.row_count - 1) * FIVE_MINUTE_MS_V2
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "R3 cache timestamp range differs from its row count"
        )
    if type(value.returns_3) is not tuple or len(value.returns_3) != value.row_count - 3:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "R3 cache return count differs from its source rows"
        )
    if any(not _is_finite_decimal(item) for item in value.returns_3):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "R3 cache requires exact finite Decimal returns"
        )
    _validate_non_authority_flags(value)
    if recompute_series_hash:
        expected = _decimal_series_sha256(
            _RETURN_3_SERIES_DOMAIN,
            first_open_ms=value.first_return_open_ms,
            values=value.returns_3,
        )
        if value.returns_3_sha256 != expected:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "R3 cache return hash differs from its immutable series"
            )


def _validate_target_cache(
    value: HistoricalTargetNumericCacheV2,
    *,
    recompute_component_hashes: bool,
) -> None:
    if recompute_component_hashes:
        canonical_historical_r3_series_cache_v2(value.source_r3_cache)
    else:
        _validate_r3_cache(value.source_r3_cache, recompute_series_hash=False)
    if len(value.returns_1) != value.row_count - 1 or len(value.returns_12) != value.row_count - 12:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target return series lengths differ from the source row count"
        )
    if any(
        not _is_finite_decimal(item)
        for series in (value.returns_1, value.returns_12)
        for item in series
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target cache requires exact finite Decimal returns"
        )
    if (
        type(value.participation_bars) is not tuple
        or len(value.participation_bars) != value.row_count
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target participation bar count differs from source rows"
        )
    expected_open_ms = value.first_bar_open_ms
    for index, bar in enumerate(value.participation_bars):
        if type(bar) is not ParticipationFlowBarValueV2:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target cache contains an unsupported participation value"
            )
        if (
            bar.bar_open_ms != expected_open_ms + index * FIVE_MINUTE_MS_V2
            or bar.bar_close_ms != bar.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        ):
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target participation bars are not ordered and contiguous"
            )
    _validate_non_authority_flags(value)
    if recompute_component_hashes:
        expected_1 = _decimal_series_sha256(
            _RETURN_1_SERIES_DOMAIN,
            first_open_ms=value.first_return_1_open_ms,
            values=value.returns_1,
        )
        expected_12 = _decimal_series_sha256(
            _RETURN_12_SERIES_DOMAIN,
            first_open_ms=value.first_return_12_open_ms,
            values=value.returns_12,
        )
        expected_flow = _flow_bar_series_sha256(value.participation_bars)
        if (
            value.returns_1_sha256 != expected_1
            or value.returns_12_sha256 != expected_12
            or value.participation_bars_sha256 != expected_flow
        ):
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target cache component hash differs from immutable series"
            )


def _validate_cross_cache(
    value: HistoricalTargetExcludedMedianR3CacheV2,
    *,
    recompute_series_hash: bool,
) -> None:
    _validate_symbol(value.target_symbol, "target_symbol")
    if type(value.peer_caches) is not tuple or len(value.peer_caches) != 6:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded cache requires exactly six peer caches"
        )
    if any(type(item) is not HistoricalR3SeriesCacheV2 for item in value.peer_caches):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded cache contains an unsupported peer cache"
        )
    for cache in value.peer_caches:
        if recompute_series_hash:
            canonical_historical_r3_series_cache_v2(cache)
        else:
            _validate_r3_cache(cache, recompute_series_hash=False)
    expected_symbols = tuple(cache.symbol for cache in value.peer_caches)
    if (
        value.peer_symbols != expected_symbols
        or expected_symbols != tuple(sorted(expected_symbols, key=str.encode))
        or len(set(expected_symbols)) != 6
        or value.target_symbol in expected_symbols
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded cache peer identity differs"
        )
    expected_first = max(cache.first_return_open_ms for cache in value.peer_caches)
    expected_final = min(cache.final_bar_open_ms for cache in value.peer_caches)
    expected_count = (expected_final - expected_first) // FIVE_MINUTE_MS_V2 + 1
    if (
        value.first_return_open_ms != expected_first
        or value.final_return_open_ms != expected_final
        or type(value.median_returns_3) is not tuple
        or len(value.median_returns_3) != expected_count
        or expected_count < _CROSS_PRIOR_COUNT + 1
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded median range differs from peer overlap"
        )
    if any(not _is_finite_decimal(item) for item in value.median_returns_3):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded cache requires finite Decimal medians"
        )
    _validate_non_authority_flags(value)
    if value.target_returns_used is not False:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target-excluded cache cannot use target returns"
        )
    if recompute_series_hash:
        expected_hash = _decimal_series_sha256(
            _MEDIAN_RETURN_3_SERIES_DOMAIN,
            first_open_ms=value.first_return_open_ms,
            values=value.median_returns_3,
        )
        if value.median_returns_3_sha256 != expected_hash:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target-excluded median hash differs from its immutable series"
            )


def _validate_target_anchor_inputs(value: HistoricalTargetAnchorInputsV2) -> None:
    _validate_symbol(value.symbol, "anchor symbol")
    _validate_slot(value.final_bar_open_ms, value.final_bar_close_ms)
    for digest, name in (
        (value.price_source_slice_sha256, "price_source_slice_sha256"),
        (
            value.participation_source_slice_sha256,
            "participation_source_slice_sha256",
        ),
    ):
        _validate_sha256(digest, name)
    if (
        type(value.returns_1) is not tuple
        or type(value.returns_12) is not tuple
        or len(value.returns_1) != _PRICE_RETURN_COUNT
        or len(value.returns_12) != _PRICE_RETURN_COUNT
        or any(
            not _is_finite_decimal(item)
            for series in (value.returns_1, value.returns_12)
            for item in series
        )
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target anchor requires exact finite 8,641-value return series"
        )
    if (
        type(value.prior_participation_bars) is not tuple
        or len(value.prior_participation_bars) != ROBUST_Z_PRIOR_WINDOW_V2
        or type(value.current_participation_bar) is not ParticipationFlowBarValueV2
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "target anchor requires 8,640 prior plus one current flow bar"
        )
    expected_first = value.final_bar_open_ms - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    bars = (*value.prior_participation_bars, value.current_participation_bar)
    for index, bar in enumerate(bars):
        if type(bar) is not ParticipationFlowBarValueV2:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target anchor contains an unsupported flow bar"
            )
        if bar.bar_open_ms != expected_first + index * FIVE_MINUTE_MS_V2:
            raise HistoricalNumericPrecomputeContractErrorV2(
                "target anchor flow bars are not exact contiguous slots"
            )


def _validate_cross_anchor_inputs(value: HistoricalCrossAnchorInputsV2) -> None:
    _validate_symbol(value.target_symbol, "target_symbol")
    _validate_slot(value.final_bar_open_ms, value.final_bar_close_ms)
    if (
        type(value.peer_symbols) is not tuple
        or len(value.peer_symbols) != 6
        or value.peer_symbols != tuple(sorted(value.peer_symbols, key=str.encode))
        or len(set(value.peer_symbols)) != 6
        or value.target_symbol in value.peer_symbols
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "cross anchor requires six sorted target-excluded peer symbols"
        )
    if (
        type(value.prior_market_median_returns_3) is not tuple
        or len(value.prior_market_median_returns_3) != _CROSS_PRIOR_COUNT
        or type(value.current_peer_returns_3) is not tuple
        or len(value.current_peer_returns_3) != 6
        or any(
            not _is_finite_decimal(item)
            for series in (
                value.prior_market_median_returns_3,
                value.current_peer_returns_3,
            )
            for item in series
        )
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "cross anchor requires exact finite 8,640-prior/six-current returns"
        )
    if (
        type(value.peer_path_sha256s) is not tuple
        or tuple(symbol for symbol, _ in value.peer_path_sha256s) != value.peer_symbols
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "cross anchor peer path hashes differ from peer symbols"
        )
    for symbol, digest in value.peer_path_sha256s:
        _validate_symbol(symbol, "peer path symbol")
        _validate_sha256(digest, "peer path sha256")
    _validate_sha256(value.peer_input_sha256, "peer_input_sha256")


def _validate_non_authority_flags(value: object) -> None:
    if (
        getattr(value, "rule_version", None)
        != HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2
        or getattr(value, "historical_only", None) is not True
        or getattr(value, "numeric_only", None) is not True
        or getattr(value, "live_authority", None) is not False
        or getattr(value, "promoting", None) is not False
        or getattr(value, "probability", None) is not False
        or getattr(value, "outcome_used", None) is not False
        or getattr(value, "data_through_ms", False) is not None
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "historical numeric cache authority or non-promotion flags differ"
        )


def _fixed_series_window(
    *,
    values: tuple[Decimal, ...],
    first_value_open_ms: int,
    final_open_ms: int,
    count: int,
    label: str,
) -> tuple[Decimal, ...]:
    final_index = _series_index(
        bar_open_ms=final_open_ms,
        first_value_open_ms=first_value_open_ms,
        value_count=len(values),
        label=label,
    )
    first_index = final_index - count + 1
    if first_index < 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} lacks the exact {count}-value causal window"
        )
    selected = values[first_index : final_index + 1]
    if len(selected) != count:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} returned an incomplete causal window"
        )
    return selected


def _prior_series_window(
    *,
    values: tuple[Decimal, ...],
    first_value_open_ms: int,
    current_open_ms: int,
    prior_count: int,
    label: str,
) -> tuple[Decimal, ...]:
    current_index = _series_index(
        bar_open_ms=current_open_ms,
        first_value_open_ms=first_value_open_ms,
        value_count=len(values),
        label=label,
    )
    first_index = current_index - prior_count
    if first_index < 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} lacks {prior_count} prior values"
        )
    selected = values[first_index:current_index]
    if len(selected) != prior_count:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} returned an incomplete prior window"
        )
    return selected


def _fixed_bar_window(
    *,
    values: tuple[ParticipationFlowBarValueV2, ...],
    first_value_open_ms: int,
    final_open_ms: int,
    count: int,
    label: str,
) -> tuple[ParticipationFlowBarValueV2, ...]:
    final_index = _series_index(
        bar_open_ms=final_open_ms,
        first_value_open_ms=first_value_open_ms,
        value_count=len(values),
        label=label,
    )
    first_index = final_index - count + 1
    if first_index < 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} lacks the exact {count}-bar causal window"
        )
    selected = values[first_index : final_index + 1]
    if len(selected) != count:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} returned an incomplete causal window"
        )
    return selected


def _series_index(
    *,
    bar_open_ms: int,
    first_value_open_ms: int,
    value_count: int,
    label: str,
) -> int:
    if type(bar_open_ms) is not int or bar_open_ms < 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} lookup timestamp must be a nonnegative integer"
        )
    difference = bar_open_ms - first_value_open_ms
    if difference < 0 or difference % FIVE_MINUTE_MS_V2 != 0:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} has no exact aligned value at the requested timestamp"
        )
    index = difference // FIVE_MINUTE_MS_V2
    if not 0 <= index < value_count:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{label} lookup lies outside the bounded cache"
        )
    return index


def _decimal_series_sha256(
    domain: bytes,
    *,
    first_open_ms: int,
    values: tuple[Decimal, ...],
) -> str:
    digest = hashlib.sha256(domain)
    digest.update(
        canonical_json_line(
            {
                "count": len(values),
                "first_open_ms": first_open_ms,
                "interval_ms": FIVE_MINUTE_MS_V2,
                "schema_version": "r4b_historical_decimal_series_v2",
            }
        )
    )
    for value in values:
        encoded = str(value).encode("ascii")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _flow_bar_series_sha256(
    values: tuple[ParticipationFlowBarValueV2, ...],
) -> str:
    digest = hashlib.sha256(_FLOW_BAR_SERIES_DOMAIN)
    first_open_ms = values[0].bar_open_ms if values else 0
    digest.update(
        canonical_json_line(
            {
                "count": len(values),
                "first_open_ms": first_open_ms,
                "interval_ms": FIVE_MINUTE_MS_V2,
                "schema_version": "r4b_historical_flow_bar_series_v2",
            }
        )
    )
    for value in values:
        canonical_participation_flow_bar_value_v2(value)
        digest.update(bytes.fromhex(value.bar_value_sha256))
    return digest.hexdigest()


def _target_slice_sha256(
    *,
    domain: bytes,
    representation: str,
    dataset_sha256: str,
    symbol: str,
    first_open_ms: int,
    final_open_ms: int,
    row_count: int,
) -> str:
    _validate_sha256(dataset_sha256, "target slice dataset_sha256")
    return hashlib.sha256(
        domain
        + canonical_json_line(
            {
                "dataset_sha256": dataset_sha256,
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "first_open_ms": first_open_ms,
                "historical_receipt_policy": (
                    "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                ),
                "interval": "5m",
                "market": "futures",
                "representation": representation,
                "row_count": row_count,
                "schema_version": "r4b_historical_numeric_dataset_root_slice_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _cross_path_sha256(
    *,
    dataset_sha256: str,
    manifest_sha256: str,
    symbol: str,
    first_open_ms: int,
    final_open_ms: int,
    row_count: int,
) -> str:
    _validate_sha256(dataset_sha256, "cross path dataset_sha256")
    _validate_sha256(manifest_sha256, "cross path manifest_sha256")
    return hashlib.sha256(
        _PEER_PATH_SLICE_DOMAIN
        + canonical_json_line(
            {
                "dataset_sha256": dataset_sha256,
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "first_open_ms": first_open_ms,
                "historical_receipt_policy": (
                    "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                ),
                "interval": "5m",
                "manifest_sha256": manifest_sha256,
                "market": "futures",
                "representation": "CROSS_CLOSE_PATH_DATASET_ROOT_WINDOW",
                "row_count": row_count,
                "schema_version": "r4b_historical_cross_numeric_peer_path_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _cross_input_sha256(
    *,
    target_symbol: str,
    final_open_ms: int,
    peer_path_sha256s: tuple[tuple[str, str], ...],
) -> str:
    ordered = tuple(
        sorted(peer_path_sha256s, key=lambda value: value[0].encode("utf-8"))
    )
    if (
        len(ordered) != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
        or target_symbol in {symbol for symbol, _digest in ordered}
        or len({symbol for symbol, _digest in ordered}) != len(ordered)
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "cross input requires six unique target-excluded peer roots"
        )
    for symbol, digest in ordered:
        _validate_symbol(symbol, "cross input peer symbol")
        _validate_sha256(digest, "cross input peer path SHA-256")
    return hashlib.sha256(
        _PEER_INPUT_SLICE_DOMAIN
        + canonical_json_line(
            {
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "historical_receipt_policy": (
                    "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                ),
                "interval": "5m",
                "market": "futures",
                "peer_paths": [
                    {"path_sha256": digest, "symbol": symbol}
                    for symbol, digest in ordered
                ],
                "representation": "TARGET_EXCLUDED_DATASET_ROOT_PEER_WINDOWS",
                "schema_version": "r4b_historical_cross_numeric_input_v2",
                "target_symbol": target_symbol,
            }
        )
    ).hexdigest()


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise HistoricalNumericPrecomputeContractErrorV2(
            "peer median requires at least one Decimal"
        )
    ordered = tuple(sorted(values))
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _r3_cache_document(
    value: HistoricalR3SeriesCacheV2,
    include_cache_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "close_series_sha256": value.close_series_sha256,
        "data_through_ms": value.data_through_ms,
        "dataset_sha256": value.dataset_sha256,
        "final_bar_close_ms": value.final_bar_close_ms,
        "final_bar_open_ms": value.final_bar_open_ms,
        "first_bar_close_ms": value.first_bar_close_ms,
        "first_bar_open_ms": value.first_bar_open_ms,
        "historical_only": value.historical_only,
        "live_authority": value.live_authority,
        "manifest_sha256": value.manifest_sha256,
        "numeric_only": value.numeric_only,
        "outcome_used": value.outcome_used,
        "probability": value.probability,
        "promoting": value.promoting,
        "returns_3_sha256": value.returns_3_sha256,
        "row_count": value.row_count,
        "rule_version": value.rule_version,
        "schema_version": "r4b_historical_r3_series_cache_v2",
        "symbol": value.symbol,
    }
    if include_cache_hash:
        document["cache_sha256"] = value.cache_sha256
    return document


def _target_cache_document(
    value: HistoricalTargetNumericCacheV2,
    include_cache_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "data_through_ms": value.data_through_ms,
        "dataset_sha256": value.dataset_sha256,
        "final_bar_close_ms": value.final_bar_close_ms,
        "final_bar_open_ms": value.final_bar_open_ms,
        "first_bar_open_ms": value.first_bar_open_ms,
        "historical_only": value.historical_only,
        "live_authority": value.live_authority,
        "manifest_sha256": value.manifest_sha256,
        "numeric_only": value.numeric_only,
        "outcome_used": value.outcome_used,
        "participation_bars_sha256": value.participation_bars_sha256,
        "probability": value.probability,
        "promoting": value.promoting,
        "returns_1_sha256": value.returns_1_sha256,
        "returns_12_sha256": value.returns_12_sha256,
        "row_count": value.row_count,
        "rule_version": value.rule_version,
        "schema_version": "r4b_historical_target_numeric_cache_v2",
        "source_r3_cache_sha256": value.source_r3_cache.cache_sha256,
        "symbol": value.symbol,
        "target_scoped": value.target_scoped,
    }
    if include_cache_hash:
        document["cache_sha256"] = value.cache_sha256
    return document


def _cross_cache_document(
    value: HistoricalTargetExcludedMedianR3CacheV2,
    include_cache_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "constituent_symbols": list(value.constituent_symbols),
        "data_through_ms": value.data_through_ms,
        "final_return_open_ms": value.final_return_open_ms,
        "first_return_open_ms": value.first_return_open_ms,
        "historical_only": value.historical_only,
        "live_authority": value.live_authority,
        "median_return_count": len(value.median_returns_3),
        "median_returns_3_sha256": value.median_returns_3_sha256,
        "numeric_only": value.numeric_only,
        "outcome_used": value.outcome_used,
        "peer_caches": [
            {"cache_sha256": cache.cache_sha256, "symbol": cache.symbol}
            for cache in value.peer_caches
        ],
        "peer_symbols": list(value.peer_symbols),
        "probability": value.probability,
        "promoting": value.promoting,
        "rule_version": value.rule_version,
        "schema_version": "r4b_historical_target_excluded_median_r3_cache_v2",
        "target_returns_used": value.target_returns_used,
        "target_scoped": value.target_scoped,
        "target_symbol": value.target_symbol,
    }
    if include_cache_hash:
        document["cache_sha256"] = value.cache_sha256
    return document


def _validate_slot(open_ms: int, close_ms: int) -> None:
    if (
        type(open_ms) is not int
        or open_ms < 0
        or open_ms % FIVE_MINUTE_MS_V2 != 0
        or type(close_ms) is not int
        or close_ms != open_ms + FIVE_MINUTE_MS_V2 - 1
    ):
        raise HistoricalNumericPrecomputeContractErrorV2(
            "anchor must be one exact aligned closed 5m slot"
        )


def _validate_symbol(value: str, name: str) -> None:
    if type(value) is not str or _SYMBOL_RE.fullmatch(value) is None:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{name} must be a normalized USDT symbol"
        )


def _validate_sha256(value: str, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalNumericPrecomputeContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()
