"""Outcome-blind D2 authority and deterministic derived-hourly contracts.

This module deliberately has no file loader, publisher, WAL, strategy runner,
or network boundary.  It can bind already known metadata and derive closed UTC
hours from caller-supplied, authenticated 5m candles only.  Native 1h inputs
are outside the D2 source policy.
"""

from __future__ import annotations

import hashlib
import re
from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field
from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DecimalException,
    Inexact,
    Rounded,
    localcontext,
)
from pathlib import PurePosixPath
from typing import Final

from signalbot.backtest.context import aggregate_closed_candles
from signalbot.backtest.d1_scefb_historical_development import (
    D1_HISTORICAL_ALIAS_BY_SYMBOL_V0,
    D1_HISTORICAL_DATA_START_MS_V0,
    D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
    D1_HISTORICAL_HOURLY_ROW_COUNT_V0,
    D1_HISTORICAL_RECEIPT_CONVENTION_V0,
    D1_HISTORICAL_UNIVERSE_V0,
    D1HistoricalDevelopmentContractErrorV0,
    D1HistoricalFundingFileBindingV0,
    D1HistoricalKlineManifestBindingV0,
    canonical_d1_historical_funding_authority_manifest_v0,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line

D2_HISTORICAL_PREREGISTRATION_SHA256_V0: Final = (
    "37640c48b386896edc333d83467cb89add0cedb95ffc3afa5e374bd1e580bca3"
)
D2_HISTORICAL_SOURCE_POLICY_V0: Final = "D2_SCEFB_DERIVED_1H_FROM_CLOSED_5M_V0"
D2_HISTORICAL_DERIVATION_POLICY_V0: Final = (
    "UTC_ALIGNED_EXACT_12_CLOSED_5M_TO_1H_NO_REPAIR_V0"
)

D1_PREDECESSOR_PREREGISTRATION_SHA256_V0: Final = (
    "af69c262282144432e6adbf1e01406c7334e37176dd83ce6f9666adc49b6899d"
)
D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0: Final = (
    "c33a77f4223dcf2b90fbf79853beb4818af105ccb65bf248daa273a3a4089f62"
)
D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0: Final = (
    "f22655f7a3327ed176c5bdcffb565914fe0807586338f688253208a7ea7cabd5"
)
D1_PREDECESSOR_FREEZE_SHA256_V0: Final = (
    "bdf6f495762371281a137c32d57066602578a47598303d2ce4830d5e977b161a"
)
D1_PREDECESSOR_START_RECORD_SHA256_V0: Final = (
    "1eb5d24f79c43bbdb80e7fdcb479a606fa92be6aa76e95c657f09509ecbe4c5d"
)
D1_PREDECESSOR_TERMINAL_FAILED_RECORD_SHA256_V0: Final = (
    "81948df00e0a11812d9088239712d145ba8ce0daa21fffefe4ab06573626b369"
)
D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0: Final = (
    "15988eec55f311cfc95273eca17848328a6fa24ab8b315f9c354c3e869a51e72"
)
D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0: Final = (
    "f44e4c38aefeb5542c8875e3625ab01e82cde1fd4ff7738e26684b9895a25592"
)

D2_HISTORICAL_FIXED_INPUT_PROJECTION_V0: Final = "D2_FIXED_D1_5M_FUNDING_PROJECTION_V0"
D2_HISTORICAL_FIXED_D1_INPUT_AUTHORITY_RELATIVE_PATH_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/input_authority.jsonl"
)
D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/funding_authority.jsonl"
)
D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0: Final = (
    "b128bf30c6f23141e638248e47352eee4b6532317e5c8379cc04a262228fb4e8"
)
D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0: Final = (
    (
        "BTCUSDT",
        "data/backtest/futures/BTC__BTCUSDT__5m.csv.gz.manifest.json",
        "065b1485f1c651955f10ed3fc772e5fe2373273317cc73e9c4121a10436225ff",
    ),
    (
        "ETHUSDT",
        "data/backtest/futures/ETH__ETHUSDT__5m.csv.gz.manifest.json",
        "adf38583f799767e0529eeaa1cfc8607e583fe27711f536360251b3c2bb9d3f3",
    ),
    (
        "BNBUSDT",
        "data/backtest/futures/BNB__BNBUSDT__5m.csv.gz.manifest.json",
        "e02e0ac41e0e7320cbe828857f3b5c9b147a99fee233edb6fb3f66e25532b575",
    ),
    (
        "SOLUSDT",
        "data/backtest/futures/SOL__SOLUSDT__5m.csv.gz.manifest.json",
        "956eaefb596283998b5765604be633026f1a08dfe72d5242bbf9dd7cdaf0f720",
    ),
    (
        "XRPUSDT",
        "data/backtest/futures/XRP__XRPUSDT__5m.csv.gz.manifest.json",
        "fc22aefefe4f0e7d98038631d16187d7bd7193b8cafb0132977f0004d1cf02d1",
    ),
    (
        "DOGEUSDT",
        "data/backtest/futures/DOGE__DOGEUSDT__5m.csv.gz.manifest.json",
        "efc7f8d4bd1f45f8eafbf6418cbe96d5ac9b5fdef4e47f6d85f8b3d477eb364a",
    ),
    (
        "ARBUSDT",
        "data/backtest/futures/ARB__ARBUSDT__5m.csv.gz.manifest.json",
        "b6832f296b93303b626d0f218c821800f4b3d8668b33f10ffb34a91d469049a8",
    ),
    (
        "OPUSDT",
        "data/backtest/futures/OP__OPUSDT__5m.csv.gz.manifest.json",
        "9d3e9a1891089dfd13fa1b3446f61c170eb01a23457bba3539194d20732abe8e",
    ),
    (
        "SUIUSDT",
        "data/backtest/futures/SUI__SUIUSDT__5m.csv.gz.manifest.json",
        "9ecd16409502b3582b9ea605969cd55b0a916aa85c2ff15a2234b37c0defbca1",
    ),
    (
        "WIFUSDT",
        "data/backtest/futures/WIF__WIFUSDT__5m.csv.gz.manifest.json",
        "885e80311daa9841fbde5d8935c469d4268f6c84030d7d21bcbe40ae9c23f303",
    ),
)
D2_HISTORICAL_FIXED_FUNDING_FILES_V0: Final = (
    (
        "BTCUSDT",
        "data/backtest/funding/BTC__BTCUSDT__5m.csv.gz",
        "51495fd4ffc163cd3b801b6981eeb07719216950d1c69687b0ed190c9bae5e46",
    ),
    (
        "ETHUSDT",
        "data/backtest/funding/ETH__ETHUSDT__5m.csv.gz",
        "18655396921527037f48e6c1bb38d14e75d08d4f755e4aa6d16e6c38db45c20b",
    ),
    (
        "BNBUSDT",
        "data/backtest/funding/BNB__BNBUSDT__5m.csv.gz",
        "bb0181f03a4f47d3d6f35837c483fdbe343cd69b331edbef064ee802d79117f7",
    ),
    (
        "SOLUSDT",
        "data/backtest/funding/SOL__SOLUSDT__5m.csv.gz",
        "7b430d47903156cabdacf3dc78d11604604df62326d1f729bd8834aed5e589ae",
    ),
    (
        "XRPUSDT",
        "data/backtest/funding/XRP__XRPUSDT__5m.csv.gz",
        "a7215a22243ea942fcc19e57355ad2dd62e56f823e4c55fd6517afc1558396d7",
    ),
    (
        "DOGEUSDT",
        "data/backtest/funding/DOGE__DOGEUSDT__5m.csv.gz",
        "177a8f253632160097dbf7dd6cfe4f833f94ef402eb7e2410d3476311d133c8a",
    ),
    (
        "ARBUSDT",
        "data/backtest/funding/ARB__ARBUSDT__5m.csv.gz",
        "7e1bd0d34a224f1ca212b8840b83bbfa1e6b1bb18f6366b3d73acf9d1c581022",
    ),
    (
        "OPUSDT",
        "data/backtest/funding/OP__OPUSDT__5m.csv.gz",
        "e3d92525490a781fc52463caabcfd897dff63b5b8d1a6a27258f26f6fe941c5e",
    ),
    (
        "SUIUSDT",
        "data/backtest/funding/SUI__SUIUSDT__5m.csv.gz",
        "c15e2b54f17b7e9cdd0e4e6dbd7ab4852838c51ef2943d38080a55eadc39b8bd",
    ),
    (
        "WIFUSDT",
        "data/backtest/funding/WIF__WIFUSDT__5m.csv.gz",
        "4536fe5bd694ab93283ee59270f5d5712cb3c67d2a7e351197d2776f2d7bbbd8",
    ),
)

_FIVE_MINUTE_MS: Final = 300_000
_HOUR_MS: Final = 3_600_000
_D2_INPUT_AUTHORITY_SCHEMA_V0: Final = "d2_scefb_derived_hourly_input_authority_v0"
_D2_DERIVED_HOURLY_MANIFEST_SCHEMA_V0: Final = "d2_scefb_derived_hourly_manifest_v0"
_INPUT_AUTHORITY_HASH_DOMAIN: Final = b"D2_SCEFB_DERIVED_HOURLY_INPUT_AUTHORITY_V0\0"
_DERIVED_HOUR_MEMBER_HASH_DOMAIN: Final = b"D2_SCEFB_DERIVED_HOUR_MEMBER_V0\0"
_DERIVED_HOUR_SEQUENCE_ROOT_DOMAIN: Final = b"D2_SCEFB_DERIVED_HOUR_SEQUENCE_ROOT_V0\0"
_DERIVED_HOUR_MANIFEST_HASH_DOMAIN: Final = b"D2_SCEFB_DERIVED_HOUR_MANIFEST_V0\0"
_INPUT_AUTHORITY_FACTORY_TOKEN: Final = object()
_DERIVED_MANIFEST_FACTORY_TOKEN: Final = object()
_DERIVED_PANEL_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_PATH_RE: Final = re.compile(r"^[A-Za-z0-9._/-]+$")
_D2_MAX_EXACT_DECIMAL_PRECISION_V0: Final = 256


def _fixed_input_projection_document_v0() -> dict[str, object]:
    return {
        "d1_input_authority_file": {
            "authority_sha256": D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0,
            "file_sha256": D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0,
            "relative_path": D2_HISTORICAL_FIXED_D1_INPUT_AUTHORITY_RELATIVE_PATH_V0,
        },
        "five_minute_manifests": [
            {
                "interval": "5m",
                "manifest_sha256": manifest_sha256,
                "relative_manifest_path": relative_path,
                "symbol": symbol,
            }
            for symbol, relative_path, manifest_sha256 in (
                D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
            )
        ],
        "funding_authority": {
            "files": [
                {
                    "relative_path": relative_path,
                    "sha256": sha256,
                    "symbol": symbol,
                }
                for symbol, relative_path, sha256 in D2_HISTORICAL_FIXED_FUNDING_FILES_V0
            ],
            "manifest_relative_path": (
                D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0
            ),
            "manifest_sha256": D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0,
        },
        "projection_version": D2_HISTORICAL_FIXED_INPUT_PROJECTION_V0,
    }


def canonical_d2_historical_fixed_input_projection_v0() -> bytes:
    """Return the canonical metadata-only D1 -> D2 fixed input projection."""

    return canonical_json_line(_fixed_input_projection_document_v0())


D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0: Final = hashlib.sha256(
    canonical_d2_historical_fixed_input_projection_v0()
).hexdigest()


def _source_policy_document_v0() -> dict[str, object]:
    return {
        "closed_source_candles_only": True,
        "derivation_policy_version": D2_HISTORICAL_DERIVATION_POLICY_V0,
        "derived_interval": "1h",
        "fixed_input_projection_sha256": (
            D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0
        ),
        "native_1h_authority_permitted": False,
        "preregistration_sha256": D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        "source_interval": "5m",
        "source_policy_version": D2_HISTORICAL_SOURCE_POLICY_V0,
    }


def canonical_d2_historical_source_policy_v0() -> bytes:
    """Return the canonical fixed D2 higher-timeframe source policy."""

    return canonical_json_line(_source_policy_document_v0())


D2_HISTORICAL_SOURCE_POLICY_SHA256_V0: Final = hashlib.sha256(
    canonical_d2_historical_source_policy_v0()
).hexdigest()


class D2HistoricalDerivedHourlyContractErrorV0(ValueError):
    """Raised when a D2 authority or derived-hourly invariant fails closed."""


@dataclass(frozen=True, slots=True)
class D2HistoricalPredecessorProvenanceV0:
    """Immutable identity of the terminal D1 attempt that selected D2 policy."""

    d1_preregistration_sha256: str = D1_PREDECESSOR_PREREGISTRATION_SHA256_V0
    d1_input_authority_sha256: str = D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0
    d1_input_authority_file_sha256: str = (
        D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0
    )
    d1_run_002_freeze_sha256: str = D1_PREDECESSOR_FREEZE_SHA256_V0
    d1_run_002_start_record_sha256: str = D1_PREDECESSOR_START_RECORD_SHA256_V0
    d1_run_002_terminal_failed_record_sha256: str = (
        D1_PREDECESSOR_TERMINAL_FAILED_RECORD_SHA256_V0
    )
    d1_failure_evidence_manifest_sha256: str = (
        D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0
    )
    d1_failure_evidence_archive_sha256: str = (
        D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0
    )

    def __post_init__(self) -> None:
        actual = tuple(getattr(self, name) for name, _ in _predecessor_fields_v0())
        expected = tuple(value for _, value in _predecessor_fields_v0())
        if actual != expected:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "D2 predecessor provenance must equal the fixed terminal D1 bindings"
            )


@dataclass(frozen=True, slots=True)
class D2HistoricalInputAuthorityV0:
    """Exact metadata-only D2 input authority; it cannot represent native 1h."""

    five_minute_manifests: tuple[D1HistoricalKlineManifestBindingV0, ...]
    funding_manifest_relative_path: str
    funding_manifest_sha256: str
    funding_files: tuple[D1HistoricalFundingFileBindingV0, ...]
    predecessor: D2HistoricalPredecessorProvenanceV0
    _factory_token: InitVar[object | None] = None
    authority_sha256: str = field(init=False)
    preregistration_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
    )
    schema_version: str = field(init=False, default=_D2_INPUT_AUTHORITY_SCHEMA_V0)
    source_policy_version: str = field(
        init=False,
        default=D2_HISTORICAL_SOURCE_POLICY_V0,
    )
    fixed_input_projection_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0,
    )
    source_policy_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_SOURCE_POLICY_SHA256_V0,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _INPUT_AUTHORITY_FACTORY_TOKEN:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "D2 input authority must be factory-created"
            )
        _validate_input_authority_members_v0(self)
        object.__setattr__(
            self,
            "authority_sha256",
            _hash_document(
                _INPUT_AUTHORITY_HASH_DOMAIN,
                _input_authority_document_v0(self, include_hash=False),
            ),
        )

    def five_minute_binding(self, symbol: str) -> D1HistoricalKlineManifestBindingV0:
        """Return the sole 5m binding for one member of the frozen universe."""

        _require_symbol(symbol)
        return self.five_minute_manifests[D1_HISTORICAL_UNIVERSE_V0.index(symbol)]


@dataclass(frozen=True, slots=True)
class D2DerivedHourlyManifestV0:
    """Canonical provenance for one deterministic derived-hour sequence."""

    symbol: str
    five_minute_manifest_sha256: str
    five_minute_compressed_data_sha256: str
    source_first_open_time_ms: int
    source_last_close_time_ms: int
    source_row_count: int
    derived_first_open_time_ms: int
    derived_last_close_time_ms: int
    derived_row_count: int
    ordered_canonical_sequence_root_sha256: str
    _factory_token: InitVar[object | None] = None
    manifest_sha256: str = field(init=False)
    derivation_policy_version: str = field(
        init=False,
        default=D2_HISTORICAL_DERIVATION_POLICY_V0,
    )
    historical_receipt_convention: str = field(
        init=False,
        default=D1_HISTORICAL_RECEIPT_CONVENTION_V0,
    )
    schema_version: str = field(
        init=False,
        default=_D2_DERIVED_HOURLY_MANIFEST_SCHEMA_V0,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _DERIVED_MANIFEST_FACTORY_TOKEN:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "derived-hour manifest must be created by the D2 derivation boundary"
            )
        _validate_derived_manifest_fields_v0(self)
        object.__setattr__(
            self,
            "manifest_sha256",
            _hash_document(
                _DERIVED_HOUR_MANIFEST_HASH_DOMAIN,
                _derived_manifest_document_v0(self, include_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D2DerivedHourlyPanelV0:
    """Immutable derived candles and their canonical provenance manifest."""

    candles: tuple[Candle, ...]
    manifest: D2DerivedHourlyManifestV0
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _DERIVED_PANEL_FACTORY_TOKEN:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "derived-hour panel must be created by the D2 derivation boundary"
            )
        validate_d2_derived_hourly_panel_v0(self)


def build_d2_historical_input_authority_v0(
    *,
    five_minute_manifests: Sequence[D1HistoricalKlineManifestBindingV0],
    funding_manifest_relative_path: str,
    funding_manifest_sha256: str,
    funding_files: Sequence[D1HistoricalFundingFileBindingV0],
) -> D2HistoricalInputAuthorityV0:
    """Bind D2 input metadata without opening any kline or funding outcome row."""

    return D2HistoricalInputAuthorityV0(
        five_minute_manifests=tuple(five_minute_manifests),
        funding_manifest_relative_path=funding_manifest_relative_path,
        funding_manifest_sha256=funding_manifest_sha256,
        funding_files=tuple(funding_files),
        predecessor=_fixed_predecessor_provenance_v0(),
        _factory_token=_INPUT_AUTHORITY_FACTORY_TOKEN,
    )


def canonical_d2_historical_input_authority_v0(
    authority: D2HistoricalInputAuthorityV0,
) -> bytes:
    """Revalidate and serialize one exact D2 authority as canonical JSONL."""

    if type(authority) is not D2HistoricalInputAuthorityV0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "authority must be exact D2HistoricalInputAuthorityV0"
        )
    _validate_input_authority_members_v0(authority)
    expected = _hash_document(
        _INPUT_AUTHORITY_HASH_DOMAIN,
        _input_authority_document_v0(authority, include_hash=False),
    )
    if authority.authority_sha256 != expected:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "D2 input authority hash differs from canonical content"
        )
    return canonical_json_line(_input_authority_document_v0(authority, include_hash=True))


def derive_d2_closed_hourly_v0(
    *,
    symbol: str,
    five_minute_candles: Sequence[Candle],
    five_minute_manifest_sha256: str,
    five_minute_compressed_data_sha256: str,
) -> D2DerivedHourlyPanelV0:
    """Derive the fixed 245,376 -> 20,448 production historical panel."""

    return _derive_d2_closed_hourly_for_contract_v0(
        symbol=symbol,
        five_minute_candles=five_minute_candles,
        five_minute_manifest_sha256=five_minute_manifest_sha256,
        five_minute_compressed_data_sha256=five_minute_compressed_data_sha256,
        expected_source_start_ms=D1_HISTORICAL_DATA_START_MS_V0,
        expected_source_end_ms_exclusive=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        expected_source_row_count=D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
        expected_derived_row_count=D1_HISTORICAL_HOURLY_ROW_COUNT_V0,
    )


def d2_latest_eligible_hour_close_ms_v0(signal_close_time_ms: int) -> int | None:
    """Return the latest complete UTC-hour close eligible at a closed 5m signal."""

    _require_nonnegative_int(signal_close_time_ms, "signal_close_time_ms")
    if (signal_close_time_ms + 1) % _FIVE_MINUTE_MS != 0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "signal close must be the exact final millisecond of a 5m slot"
        )
    candidate = ((signal_close_time_ms + 1) // _HOUR_MS) * _HOUR_MS - 1
    return candidate if candidate >= 0 else None


def d2_eligible_hour_end_index_v0(
    hourly_close_times_ms: Sequence[int],
    signal_close_time_ms: int,
) -> int:
    """Return the exclusive prefix index of hours eligible at a 5m close."""

    close_times = tuple(hourly_close_times_ms)
    previous: int | None = None
    for value in close_times:
        _require_nonnegative_int(value, "hourly close time")
        if (value + 1) % _HOUR_MS != 0:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "hourly close sequence contains a non-UTC-hour boundary"
            )
        if previous is not None and value <= previous:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "hourly close sequence must be strictly ordered and unique"
            )
        previous = value
    eligible_close = d2_latest_eligible_hour_close_ms_v0(signal_close_time_ms)
    if eligible_close is None:
        return 0
    return bisect_right(close_times, eligible_close)


def canonical_d2_derived_hour_v0(candle: Candle) -> bytes:
    """Serialize a derived hour, including fixed replay receipt semantics."""

    if type(candle) is not Candle:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived hour must be an exact Candle value"
        )
    _validate_derived_hour_v0(candle, expected_symbol=candle.symbol)
    return canonical_json_line(_derived_hour_document_v0(candle))


def canonical_d2_derived_hourly_manifest_v0(
    manifest: D2DerivedHourlyManifestV0,
) -> bytes:
    """Revalidate and serialize a D2 derived-hour manifest."""

    if type(manifest) is not D2DerivedHourlyManifestV0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "manifest must be exact D2DerivedHourlyManifestV0"
        )
    _validate_derived_manifest_fields_v0(manifest)
    expected = _hash_document(
        _DERIVED_HOUR_MANIFEST_HASH_DOMAIN,
        _derived_manifest_document_v0(manifest, include_hash=False),
    )
    if manifest.manifest_sha256 != expected:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived-hour manifest hash differs from canonical content"
        )
    return canonical_json_line(_derived_manifest_document_v0(manifest, include_hash=True))


def validate_d2_derived_hourly_panel_v0(panel: D2DerivedHourlyPanelV0) -> None:
    """Fail closed if a derived panel no longer matches its manifest."""

    if type(panel) is not D2DerivedHourlyPanelV0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "panel must be exact D2DerivedHourlyPanelV0"
        )
    manifest = panel.manifest
    canonical_d2_derived_hourly_manifest_v0(manifest)
    if type(panel.candles) is not tuple or any(
        type(candle) is not Candle for candle in panel.candles
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived candles must be an exact immutable Candle sequence"
        )
    if len(panel.candles) != manifest.derived_row_count:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived candle count differs from its manifest"
        )
    for index, candle in enumerate(panel.candles):
        _validate_derived_hour_v0(candle, expected_symbol=manifest.symbol)
        expected_open = manifest.derived_first_open_time_ms + index * _HOUR_MS
        if candle.open_time_ms != expected_open:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "derived candle sequence is not contiguous from its manifest boundary"
            )
    if (
        panel.candles[0].open_time_ms != manifest.derived_first_open_time_ms
        or panel.candles[-1].close_time_ms != manifest.derived_last_close_time_ms
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived candle boundaries differ from their manifest"
        )
    actual_root = _derived_hour_sequence_root_v0(panel.candles)
    if actual_root != manifest.ordered_canonical_sequence_root_sha256:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived candle sequence root differs from its manifest"
        )


def _derive_d2_closed_hourly_for_contract_v0(
    *,
    symbol: str,
    five_minute_candles: Sequence[Candle],
    five_minute_manifest_sha256: str,
    five_minute_compressed_data_sha256: str,
    expected_source_start_ms: int,
    expected_source_end_ms_exclusive: int,
    expected_source_row_count: int,
    expected_derived_row_count: int,
) -> D2DerivedHourlyPanelV0:
    """Parameterized pure contract helper used by small synthetic tests."""

    _require_symbol(symbol)
    _require_sha256(five_minute_manifest_sha256, "five_minute_manifest_sha256")
    _require_sha256(
        five_minute_compressed_data_sha256,
        "five_minute_compressed_data_sha256",
    )
    _validate_expected_panel_contract_v0(
        expected_source_start_ms=expected_source_start_ms,
        expected_source_end_ms_exclusive=expected_source_end_ms_exclusive,
        expected_source_row_count=expected_source_row_count,
        expected_derived_row_count=expected_derived_row_count,
    )
    source = tuple(five_minute_candles)
    if len(source) != expected_source_row_count:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source row count differs from the fixed derivation contract"
        )
    if any(type(candle) is not Candle for candle in source):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source must contain exact Candle values only"
        )
    for index, candle in enumerate(source):
        _validate_source_five_minute_v0(
            candle,
            expected_symbol=symbol,
            expected_open_ms=expected_source_start_ms + index * _FIVE_MINUTE_MS,
        )
    if (
        source[0].open_time_ms != expected_source_start_ms
        or source[-1].close_time_ms != expected_source_end_ms_exclusive - 1
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source candle boundaries differ from the fixed derivation contract"
        )

    exact_context = Context(
        prec=_d2_exact_decimal_precision_v0(source),
        rounding=ROUND_HALF_EVEN,
    )
    exact_context.traps[Inexact] = True
    exact_context.traps[Rounded] = True
    try:
        with localcontext(exact_context):
            derived = tuple(aggregate_closed_candles(source, "1h"))
    except DecimalException as error:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "closed 5m source cannot be aggregated exactly under the fixed Decimal context"
        ) from error
    except ValueError as error:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "closed 5m source could not be aggregated without repair"
        ) from error
    if len(derived) != expected_derived_row_count:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived hour count differs from the fixed derivation contract"
        )
    for index, candle in enumerate(derived):
        _validate_derived_hour_v0(candle, expected_symbol=symbol)
        if candle.open_time_ms != expected_source_start_ms + index * _HOUR_MS:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "derived hour sequence is not canonical and contiguous"
            )
        final_constituent = source[index * 12 + 11]
        if (
            candle.close_time_ms != final_constituent.close_time_ms
            or candle.close != final_constituent.close
        ):
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "derived hour close differs from its twelfth 5m constituent"
            )

    manifest = D2DerivedHourlyManifestV0(
        symbol=symbol,
        five_minute_manifest_sha256=five_minute_manifest_sha256,
        five_minute_compressed_data_sha256=five_minute_compressed_data_sha256,
        source_first_open_time_ms=source[0].open_time_ms,
        source_last_close_time_ms=source[-1].close_time_ms,
        source_row_count=len(source),
        derived_first_open_time_ms=derived[0].open_time_ms,
        derived_last_close_time_ms=derived[-1].close_time_ms,
        derived_row_count=len(derived),
        ordered_canonical_sequence_root_sha256=_derived_hour_sequence_root_v0(derived),
        _factory_token=_DERIVED_MANIFEST_FACTORY_TOKEN,
    )
    return D2DerivedHourlyPanelV0(
        candles=derived,
        manifest=manifest,
        _factory_token=_DERIVED_PANEL_FACTORY_TOKEN,
    )


def _d2_exact_decimal_precision_v0(source: Sequence[Candle]) -> int:
    """Bound an exact 12-row volume sum independently of caller Decimal state."""

    required_precision = 1
    for bucket_start in range(0, len(source), 12):
        bucket = source[bucket_start : bucket_start + 12]
        decimal_fields = (
            tuple(candle.volume for candle in bucket),
            tuple(candle.quote_volume for candle in bucket),
            tuple(candle.taker_buy_base_volume for candle in bucket),
            tuple(candle.taker_buy_quote_volume for candle in bucket),
        )
        for values in decimal_fields:
            nonzero_values = tuple(value for value in values if value != 0)
            if not nonzero_values:
                continue
            minimum_exponent = 0  # aggregate_closed_candles starts each sum at Decimal().
            maximum_adjusted_exponent: int | None = None
            for value in nonzero_values:
                decimal_tuple = value.as_tuple()
                exponent = decimal_tuple.exponent
                if type(exponent) is not int:
                    raise D2HistoricalDerivedHourlyContractErrorV0(
                        "D2 exact Decimal derivation requires finite source volumes"
                    )
                minimum_exponent = min(minimum_exponent, exponent)
                adjusted_exponent = len(decimal_tuple.digits) + exponent - 1
                maximum_adjusted_exponent = (
                    adjusted_exponent
                    if maximum_adjusted_exponent is None
                    else max(maximum_adjusted_exponent, adjusted_exponent)
                )
            if maximum_adjusted_exponent is None:
                continue
            carry_digits = len(str(len(nonzero_values)))
            required_precision = max(
                required_precision,
                maximum_adjusted_exponent
                - minimum_exponent
                + 1
                + carry_digits,
            )
    if required_precision > _D2_MAX_EXACT_DECIMAL_PRECISION_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source volumes exceed the fixed exact Decimal precision bound"
        )
    return required_precision


def _validate_expected_panel_contract_v0(
    *,
    expected_source_start_ms: int,
    expected_source_end_ms_exclusive: int,
    expected_source_row_count: int,
    expected_derived_row_count: int,
) -> None:
    _require_nonnegative_int(expected_source_start_ms, "expected_source_start_ms")
    _require_nonnegative_int(
        expected_source_end_ms_exclusive,
        "expected_source_end_ms_exclusive",
    )
    _require_positive_int(expected_source_row_count, "expected_source_row_count")
    _require_positive_int(expected_derived_row_count, "expected_derived_row_count")
    if (
        expected_source_start_ms % _HOUR_MS != 0
        or expected_source_end_ms_exclusive % _HOUR_MS != 0
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derivation boundaries must align to complete UTC hours"
        )
    if expected_source_row_count != expected_derived_row_count * 12:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derivation contract must be an exact 12:1 row ratio"
        )
    expected_span = expected_source_row_count * _FIVE_MINUTE_MS
    if expected_source_end_ms_exclusive - expected_source_start_ms != expected_span:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derivation boundaries do not match the declared source row count"
        )


def _validate_source_five_minute_v0(
    candle: Candle,
    *,
    expected_symbol: str,
    expected_open_ms: int,
) -> None:
    if (
        candle.market is not Market.FUTURES
        or candle.symbol != expected_symbol
        or candle.interval != "5m"
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source contains a mixed market, symbol, or interval"
        )
    if not candle.is_closed:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source contains an unclosed 5m candle"
        )
    if candle.open_time_ms != expected_open_ms:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source is gapped, duplicated, unordered, or slot-misaligned"
        )
    if candle.close_time_ms != candle.open_time_ms + _FIVE_MINUTE_MS - 1:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source 5m close time does not match its exact slot"
        )


def _validate_derived_hour_v0(candle: Candle, *, expected_symbol: str) -> None:
    _require_symbol(expected_symbol)
    if type(candle) is not Candle:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived hour must be an exact Candle value"
        )
    if (
        candle.market is not Market.FUTURES
        or candle.symbol != expected_symbol
        or candle.interval != "1h"
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived hour has a mixed market, symbol, or interval"
        )
    if (
        not candle.is_closed
        or candle.open_time_ms % _HOUR_MS != 0
        or candle.close_time_ms != candle.open_time_ms + _HOUR_MS - 1
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived hour is unclosed or not exactly UTC-aligned"
        )


def _validate_input_authority_members_v0(authority: D2HistoricalInputAuthorityV0) -> None:
    if type(authority.five_minute_manifests) is not tuple or any(
        type(value) is not D1HistoricalKlineManifestBindingV0
        for value in authority.five_minute_manifests
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "5m authority must contain exact immutable D1 manifest bindings"
        )
    actual_symbols = tuple(value.symbol for value in authority.five_minute_manifests)
    if actual_symbols != D1_HISTORICAL_UNIVERSE_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "5m authority must contain the exact ordered ten-symbol universe"
        )
    paths: list[str] = []
    for binding in authority.five_minute_manifests:
        if binding.interval != "5m":
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "native 1h bindings are forbidden by the D2 authority"
            )
        path = _require_relative_path(binding.relative_manifest_path, "5m manifest path")
        _reject_native_hour_path(path, "5m manifest path")
        expected_name = (
            f"{D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[binding.symbol]}__"
            f"{binding.symbol}__5m.csv.gz.manifest.json"
        )
        if PurePosixPath(path).name != expected_name:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "5m manifest path does not identify the fixed D1 sidecar"
            )
        _require_sha256(binding.manifest_sha256, "5m manifest sha256")
        paths.append(path)
    if len(set(paths)) != len(paths):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "5m authority contains duplicate manifest paths"
        )
    actual_five_minute_projection = tuple(
        (binding.symbol, binding.relative_manifest_path, binding.manifest_sha256)
        for binding in authority.five_minute_manifests
    )
    if actual_five_minute_projection != D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "5m authority differs from the fixed D1 metadata projection"
        )

    funding_path = _require_relative_path(
        authority.funding_manifest_relative_path,
        "funding manifest path",
    )
    _reject_native_hour_path(funding_path, "funding manifest path")
    if (
        funding_path != D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0
        or authority.funding_manifest_sha256
        != D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding manifest differs from the fixed D1 metadata projection"
        )
    if funding_path in set(paths):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding and 5m manifest paths must be distinct"
        )
    _require_sha256(authority.funding_manifest_sha256, "funding manifest sha256")
    if type(authority.funding_files) is not tuple or any(
        type(value) is not D1HistoricalFundingFileBindingV0 for value in authority.funding_files
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding authority must contain exact immutable D1 file bindings"
        )
    if tuple(value.symbol for value in authority.funding_files) != D1_HISTORICAL_UNIVERSE_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding authority must contain the exact ordered ten-symbol universe"
        )
    for binding in authority.funding_files:
        path = _require_relative_path(binding.relative_path, "funding file path")
        _reject_native_hour_path(path, "funding file path")
        expected_name = (
            f"{D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[binding.symbol]}__"
            f"{binding.symbol}__5m.csv.gz"
        )
        if PurePosixPath(path).name != expected_name:
            raise D2HistoricalDerivedHourlyContractErrorV0(
                "funding file path does not identify the fixed D1 source"
            )
        _require_sha256(binding.sha256, "funding file sha256")
    actual_funding_projection = tuple(
        (binding.symbol, binding.relative_path, binding.sha256)
        for binding in authority.funding_files
    )
    if actual_funding_projection != D2_HISTORICAL_FIXED_FUNDING_FILES_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding files differ from the fixed D1 metadata projection"
        )
    try:
        funding_raw = canonical_d1_historical_funding_authority_manifest_v0(
            authority.funding_files
        )
    except D1HistoricalDevelopmentContractErrorV0 as error:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding file bindings violate the inherited D1 authority contract"
        ) from error
    if hashlib.sha256(funding_raw).hexdigest() != authority.funding_manifest_sha256:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "funding file hashes do not match the bound funding authority hash"
        )
    if authority.predecessor != _fixed_predecessor_provenance_v0():
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "D2 predecessor provenance differs from the preregistered D1 terminal attempt"
        )
    if (
        authority.schema_version != _D2_INPUT_AUTHORITY_SCHEMA_V0
        or authority.source_policy_version != D2_HISTORICAL_SOURCE_POLICY_V0
        or authority.preregistration_sha256 != D2_HISTORICAL_PREREGISTRATION_SHA256_V0
        or authority.fixed_input_projection_sha256
        != D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0
        or authority.source_policy_sha256 != D2_HISTORICAL_SOURCE_POLICY_SHA256_V0
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "D2 authority protocol fields differ from the preregistration"
        )


def _validate_derived_manifest_fields_v0(manifest: D2DerivedHourlyManifestV0) -> None:
    _require_symbol(manifest.symbol)
    _require_sha256(manifest.five_minute_manifest_sha256, "5m manifest sha256")
    _require_sha256(
        manifest.five_minute_compressed_data_sha256,
        "5m compressed data sha256",
    )
    _require_sha256(
        manifest.ordered_canonical_sequence_root_sha256,
        "derived sequence root sha256",
    )
    _require_nonnegative_int(manifest.source_first_open_time_ms, "source first open")
    _require_nonnegative_int(manifest.source_last_close_time_ms, "source last close")
    _require_positive_int(manifest.source_row_count, "source row count")
    _require_nonnegative_int(manifest.derived_first_open_time_ms, "derived first open")
    _require_nonnegative_int(manifest.derived_last_close_time_ms, "derived last close")
    _require_positive_int(manifest.derived_row_count, "derived row count")
    if manifest.source_row_count != manifest.derived_row_count * 12:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived manifest row counts are not exactly 12:1"
        )
    if (
        manifest.source_first_open_time_ms != manifest.derived_first_open_time_ms
        or manifest.source_last_close_time_ms != manifest.derived_last_close_time_ms
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "source and derived manifest boundaries must be identical"
        )
    expected_last_close = (
        manifest.source_first_open_time_ms
        + manifest.source_row_count * _FIVE_MINUTE_MS
        - 1
    )
    if manifest.source_last_close_time_ms != expected_last_close:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived manifest boundaries do not match its row counts"
        )
    if (
        manifest.source_first_open_time_ms % _HOUR_MS != 0
        or (manifest.source_last_close_time_ms + 1) % _HOUR_MS != 0
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived manifest boundaries are not complete UTC hours"
        )
    if (
        manifest.derivation_policy_version != D2_HISTORICAL_DERIVATION_POLICY_V0
        or manifest.historical_receipt_convention != D1_HISTORICAL_RECEIPT_CONVENTION_V0
        or manifest.schema_version != _D2_DERIVED_HOURLY_MANIFEST_SCHEMA_V0
    ):
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "derived manifest protocol fields differ from the fixed D2 contract"
        )


def _input_authority_document_v0(
    authority: D2HistoricalInputAuthorityV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "five_minute_manifests": [
            {
                "interval": binding.interval,
                "manifest_sha256": binding.manifest_sha256,
                "relative_manifest_path": binding.relative_manifest_path,
                "symbol": binding.symbol,
            }
            for binding in authority.five_minute_manifests
        ],
        "funding_authority": {
            "files": [asdict(binding) for binding in authority.funding_files],
            "manifest_relative_path": authority.funding_manifest_relative_path,
            "manifest_sha256": authority.funding_manifest_sha256,
        },
        "predecessor": asdict(authority.predecessor),
        "fixed_input_projection_sha256": authority.fixed_input_projection_sha256,
        "preregistration_sha256": authority.preregistration_sha256,
        "schema_version": authority.schema_version,
        "source_policy_sha256": authority.source_policy_sha256,
        "source_policy_version": authority.source_policy_version,
    }
    if include_hash:
        document["authority_sha256"] = authority.authority_sha256
    return document


def _derived_manifest_document_v0(
    manifest: D2DerivedHourlyManifestV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "derivation_policy_version": manifest.derivation_policy_version,
        "derived_first_open_time_ms": manifest.derived_first_open_time_ms,
        "derived_last_close_time_ms": manifest.derived_last_close_time_ms,
        "derived_row_count": manifest.derived_row_count,
        "five_minute_compressed_data_sha256": manifest.five_minute_compressed_data_sha256,
        "five_minute_manifest_sha256": manifest.five_minute_manifest_sha256,
        "historical_receipt_convention": manifest.historical_receipt_convention,
        "ordered_canonical_sequence_root_sha256": (
            manifest.ordered_canonical_sequence_root_sha256
        ),
        "schema_version": manifest.schema_version,
        "source_first_open_time_ms": manifest.source_first_open_time_ms,
        "source_last_close_time_ms": manifest.source_last_close_time_ms,
        "source_row_count": manifest.source_row_count,
        "symbol": manifest.symbol,
    }
    if include_hash:
        document["manifest_sha256"] = manifest.manifest_sha256
    return document


def _derived_hour_document_v0(candle: Candle) -> dict[str, object]:
    return {
        "close": _decimal_text(candle.close),
        "close_time_ms": candle.close_time_ms,
        "data_through_ms": candle.close_time_ms,
        "high": _decimal_text(candle.high),
        "historical_proxy_receipt": True,
        "interval": candle.interval,
        "is_closed": candle.is_closed,
        "low": _decimal_text(candle.low),
        "market": candle.market.value,
        "open": _decimal_text(candle.open),
        "open_time_ms": candle.open_time_ms,
        "quote_volume": _decimal_text(candle.quote_volume),
        "receipt_ms": candle.close_time_ms,
        "symbol": candle.symbol,
        "taker_buy_base_volume": _decimal_text(candle.taker_buy_base_volume),
        "taker_buy_quote_volume": _decimal_text(candle.taker_buy_quote_volume),
        "trade_count": candle.trade_count,
        "volume": _decimal_text(candle.volume),
    }


def _derived_hour_sequence_root_v0(candles: tuple[Candle, ...]) -> str:
    root = hashlib.sha256(_DERIVED_HOUR_SEQUENCE_ROOT_DOMAIN + b"EMPTY").digest()
    for index, candle in enumerate(candles):
        raw = canonical_d2_derived_hour_v0(candle)
        member = hashlib.sha256(_DERIVED_HOUR_MEMBER_HASH_DOMAIN + raw).digest()
        root = hashlib.sha256(
            _DERIVED_HOUR_SEQUENCE_ROOT_DOMAIN
            + root
            + index.to_bytes(8, byteorder="big", signed=False)
            + member
        ).digest()
    return root.hex()


def _fixed_predecessor_provenance_v0() -> D2HistoricalPredecessorProvenanceV0:
    return D2HistoricalPredecessorProvenanceV0()


def _predecessor_fields_v0() -> tuple[tuple[str, str], ...]:
    return (
        ("d1_preregistration_sha256", D1_PREDECESSOR_PREREGISTRATION_SHA256_V0),
        ("d1_input_authority_sha256", D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0),
        (
            "d1_input_authority_file_sha256",
            D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0,
        ),
        ("d1_run_002_freeze_sha256", D1_PREDECESSOR_FREEZE_SHA256_V0),
        ("d1_run_002_start_record_sha256", D1_PREDECESSOR_START_RECORD_SHA256_V0),
        (
            "d1_run_002_terminal_failed_record_sha256",
            D1_PREDECESSOR_TERMINAL_FAILED_RECORD_SHA256_V0,
        ),
        (
            "d1_failure_evidence_manifest_sha256",
            D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0,
        ),
        (
            "d1_failure_evidence_archive_sha256",
            D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0,
        ),
    )


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _require_symbol(value: object) -> str:
    if not isinstance(value, str) or value not in D1_HISTORICAL_UNIVERSE_V0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            "symbol is outside the frozen D1/D2 universe"
        )
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
        raise D2HistoricalDerivedHourlyContractErrorV0(
            f"{label} must be a normalized relative POSIX path"
        )
    return value


def _reject_native_hour_path(value: str, label: str) -> None:
    if "1h" in value.lower():
        raise D2HistoricalDerivedHourlyContractErrorV0(
            f"{label} must not identify a native 1h source"
        )


def _require_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            f"{label} must be a nonnegative integer"
        )
    return value


def _require_positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise D2HistoricalDerivedHourlyContractErrorV0(
            f"{label} must be a positive integer"
        )
    return value
