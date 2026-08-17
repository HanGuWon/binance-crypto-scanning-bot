from __future__ import annotations

import hashlib
import json
from decimal import ROUND_DOWN, ROUND_UP, Decimal, localcontext

import pytest

from signalbot.backtest import d2_scefb_derived_hourly_historical as subject
from signalbot.backtest.d1_scefb_historical_development import (
    D1_HISTORICAL_UNIVERSE_V0,
    D1HistoricalFundingFileBindingV0,
    D1HistoricalKlineManifestBindingV0,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle

_MANIFEST_SHA = hashlib.sha256(b"synthetic-5m-manifest").hexdigest()
_DATA_SHA = hashlib.sha256(b"synthetic-compressed-5m-data").hexdigest()
_FUNDING_MANIFEST_PATH = subject.D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0


def _five_minute_candle(
    index: int,
    *,
    start_ms: int = 0,
    symbol: str = "BTCUSDT",
) -> Candle:
    open_time_ms = start_ms + index * 300_000
    price = Decimal(100 + index)
    return Candle(
        market=Market.FUTURES,
        symbol=symbol,
        interval="5m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 299_999,
        open=price,
        high=price + Decimal(1),
        low=price - Decimal(1),
        close=price + Decimal("0.5"),
        volume=Decimal(10),
        quote_volume=Decimal(100),
        trade_count=index + 1,
        taker_buy_base_volume=Decimal(4),
        taker_buy_quote_volume=Decimal(40),
        is_closed=True,
    )


def _small_panel(source: tuple[Candle, ...] | None = None) -> subject.D2DerivedHourlyPanelV0:
    candles = source or tuple(_five_minute_candle(index) for index in range(12))
    return subject._derive_d2_closed_hourly_for_contract_v0(
        symbol="BTCUSDT",
        five_minute_candles=candles,
        five_minute_manifest_sha256=_MANIFEST_SHA,
        five_minute_compressed_data_sha256=_DATA_SHA,
        expected_source_start_ms=0,
        expected_source_end_ms_exclusive=3_600_000,
        expected_source_row_count=12,
        expected_derived_row_count=1,
    )


def _five_minute_bindings() -> tuple[D1HistoricalKlineManifestBindingV0, ...]:
    return tuple(
        D1HistoricalKlineManifestBindingV0(
            symbol=symbol,
            interval="5m",
            relative_manifest_path=relative_path,
            manifest_sha256=manifest_sha256,
        )
        for symbol, relative_path, manifest_sha256 in (
            subject.D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
        )
    )


def _funding_bindings() -> tuple[D1HistoricalFundingFileBindingV0, ...]:
    return tuple(
        D1HistoricalFundingFileBindingV0(
            symbol=symbol,
            relative_path=relative_path,
            sha256=sha256,
        )
        for symbol, relative_path, sha256 in subject.D2_HISTORICAL_FIXED_FUNDING_FILES_V0
    )


def _authority(
    *,
    five_minute_manifests: tuple[D1HistoricalKlineManifestBindingV0, ...] | None = None,
    funding_manifest_relative_path: str = _FUNDING_MANIFEST_PATH,
    funding_manifest_sha256: str | None = None,
    funding_files: tuple[D1HistoricalFundingFileBindingV0, ...] | None = None,
) -> subject.D2HistoricalInputAuthorityV0:
    funding = funding_files or _funding_bindings()
    return subject.build_d2_historical_input_authority_v0(
        five_minute_manifests=five_minute_manifests or _five_minute_bindings(),
        funding_manifest_relative_path=funding_manifest_relative_path,
        funding_manifest_sha256=(
            funding_manifest_sha256
            or subject.D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0
        ),
        funding_files=funding,
    )


def test_d2_authority_is_deterministic_and_contains_only_ordered_5m_klines() -> None:
    first = _authority()
    second = _authority()

    first_raw = subject.canonical_d2_historical_input_authority_v0(first)
    document = json.loads(first_raw)

    assert first.authority_sha256 == second.authority_sha256
    assert first_raw == subject.canonical_d2_historical_input_authority_v0(second)
    assert first.preregistration_sha256 == subject.D2_HISTORICAL_PREREGISTRATION_SHA256_V0
    assert [item["symbol"] for item in document["five_minute_manifests"]] == list(
        D1_HISTORICAL_UNIVERSE_V0
    )
    assert {item["interval"] for item in document["five_minute_manifests"]} == {"5m"}
    assert all(
        "__1h" not in item["relative_manifest_path"].lower()
        for item in document["five_minute_manifests"]
    )
    assert "hourly_manifests" not in document
    assert len(document["funding_authority"]["files"]) == 10
    assert document["predecessor"]["d1_run_002_terminal_failed_record_sha256"] == (
        subject.D1_PREDECESSOR_TERMINAL_FAILED_RECORD_SHA256_V0
    )
    assert document["predecessor"]["d1_input_authority_sha256"] == (
        subject.D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0
    )
    assert document["predecessor"]["d1_input_authority_file_sha256"] == (
        subject.D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0
    )


def test_fixed_projection_and_source_policy_have_stable_canonical_digests() -> None:
    projection_raw = subject.canonical_d2_historical_fixed_input_projection_v0()
    policy_raw = subject.canonical_d2_historical_source_policy_v0()
    projection = json.loads(projection_raw)
    policy = json.loads(policy_raw)

    assert hashlib.sha256(projection_raw).hexdigest() == (
        subject.D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0
    )
    assert subject.D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0 == (
        "fa3f9c4c4ccfdf086348abe7f9277bf369531d18ac07b763d86ceb5727dc7472"
    )
    assert hashlib.sha256(policy_raw).hexdigest() == (
        subject.D2_HISTORICAL_SOURCE_POLICY_SHA256_V0
    )
    assert subject.D2_HISTORICAL_SOURCE_POLICY_SHA256_V0 == (
        "52a83f2a4e2e6c28a33ebfac7a0fa8726d80db0c93798088c9d92af2c3e79b19"
    )
    assert projection["d1_input_authority_file"]["authority_sha256"] == (
        subject.D1_PREDECESSOR_INPUT_AUTHORITY_SHA256_V0
    )
    assert projection["d1_input_authority_file"]["file_sha256"] == (
        subject.D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0
    )
    assert policy["fixed_input_projection_sha256"] == (
        subject.D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0
    )
    assert policy["native_1h_authority_permitted"] is False


def test_d2_authority_rejects_native_1h_binding_and_path() -> None:
    bindings = list(_five_minute_bindings())
    first = bindings[0]
    bindings[0] = D1HistoricalKlineManifestBindingV0(
        symbol=first.symbol,
        interval="1h",
        relative_manifest_path=first.relative_manifest_path.replace("__5m", "__1h"),
        manifest_sha256=first.manifest_sha256,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="native 1h"):
        _authority(five_minute_manifests=tuple(bindings))

    bindings[0] = D1HistoricalKlineManifestBindingV0(
        symbol=first.symbol,
        interval="5m",
        relative_manifest_path=first.relative_manifest_path.replace("__5m", "__1h"),
        manifest_sha256=first.manifest_sha256,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="native 1h"):
        _authority(five_minute_manifests=tuple(bindings))

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="native 1h"):
        _authority(funding_manifest_relative_path="artifacts/native-1h/funding.json")

    funding = list(_funding_bindings())
    funding_first = funding[0]
    funding[0] = D1HistoricalFundingFileBindingV0(
        symbol=funding_first.symbol,
        relative_path=funding_first.relative_path.replace("__5m", "__1h"),
        sha256=funding_first.sha256,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="native 1h"):
        _authority(funding_files=tuple(funding))


def test_d2_authority_binds_all_funding_files_and_detects_tamper() -> None:
    with pytest.raises(
        subject.D2HistoricalDerivedHourlyContractErrorV0,
        match="fixed D1 metadata projection",
    ):
        _authority(funding_manifest_sha256="0" * 64)

    authority = _authority()
    object.__setattr__(authority, "authority_sha256", "0" * 64)
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="hash differs"):
        subject.canonical_d2_historical_input_authority_v0(authority)


def test_public_authority_rejects_every_valid_format_path_or_hash_substitution() -> None:
    five = list(_five_minute_bindings())
    original_five = five[0]
    five[0] = D1HistoricalKlineManifestBindingV0(
        symbol=original_five.symbol,
        interval="5m",
        relative_manifest_path=original_five.relative_manifest_path,
        manifest_sha256="0" * 64,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(five_minute_manifests=tuple(five))

    five[0] = D1HistoricalKlineManifestBindingV0(
        symbol=original_five.symbol,
        interval="5m",
        relative_manifest_path=(
            f"alternate/{original_five.relative_manifest_path.rsplit('/', 1)[1]}"
        ),
        manifest_sha256=original_five.manifest_sha256,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(five_minute_manifests=tuple(five))

    funding = list(_funding_bindings())
    original_funding = funding[0]
    funding[0] = D1HistoricalFundingFileBindingV0(
        symbol=original_funding.symbol,
        relative_path=original_funding.relative_path,
        sha256="0" * 64,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(funding_files=tuple(funding))

    funding[0] = D1HistoricalFundingFileBindingV0(
        symbol=original_funding.symbol,
        relative_path=f"alternate/{original_funding.relative_path.rsplit('/', 1)[1]}",
        sha256=original_funding.sha256,
    )
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(funding_files=tuple(funding))

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(funding_manifest_relative_path="alternate/funding_authority.jsonl")
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="fixed D1"):
        _authority(funding_manifest_sha256="f" * 64)


def test_canonical_authority_rejects_predecessor_d1_authority_substitution() -> None:
    authority = _authority()
    object.__setattr__(
        authority.predecessor,
        "d1_input_authority_sha256",
        "0" * 64,
    )

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="predecessor"):
        subject.canonical_d2_historical_input_authority_v0(authority)


def test_exact_twelve_to_one_aggregation_and_replay_timestamps() -> None:
    panel = _small_panel()
    hour = panel.candles[0]

    assert hour.market is Market.FUTURES
    assert hour.symbol == "BTCUSDT"
    assert hour.interval == "1h"
    assert hour.open_time_ms == 0
    assert hour.close_time_ms == 3_599_999
    assert hour.open == Decimal(100)
    assert hour.high == Decimal(112)
    assert hour.low == Decimal(99)
    assert hour.close == Decimal("111.5")
    assert hour.volume == Decimal(120)
    assert hour.quote_volume == Decimal(1_200)
    assert hour.trade_count == 78
    assert hour.taker_buy_base_volume == Decimal(48)
    assert hour.taker_buy_quote_volume == Decimal(480)
    assert panel.manifest.source_row_count == 12
    assert panel.manifest.derived_row_count == 1
    assert panel.manifest.source_first_open_time_ms == 0
    assert panel.manifest.source_last_close_time_ms == 3_599_999
    assert panel.manifest.derived_first_open_time_ms == 0
    assert panel.manifest.derived_last_close_time_ms == 3_599_999
    assert panel.manifest.five_minute_manifest_sha256 == _MANIFEST_SHA
    assert panel.manifest.five_minute_compressed_data_sha256 == _DATA_SHA

    replay_document = json.loads(subject.canonical_d2_derived_hour_v0(hour))
    assert replay_document["data_through_ms"] == hour.close_time_ms
    assert replay_document["receipt_ms"] == hour.close_time_ms
    assert replay_document["historical_proxy_receipt"] is True
    assert subject.canonical_d2_derived_hourly_manifest_v0(panel.manifest)


def test_derived_hashes_are_deterministic_and_bind_candle_content() -> None:
    first = _small_panel()
    second = _small_panel()
    assert first.manifest == second.manifest
    assert first.manifest.manifest_sha256 == second.manifest.manifest_sha256
    assert (
        first.manifest.ordered_canonical_sequence_root_sha256
        == second.manifest.ordered_canonical_sequence_root_sha256
    )

    changed = list(_five_minute_candle(index) for index in range(12))
    changed[-1] = changed[-1].model_copy(update={"close": Decimal("111.25")})
    changed_panel = _small_panel(tuple(changed))
    assert (
        changed_panel.manifest.ordered_canonical_sequence_root_sha256
        != first.manifest.ordered_canonical_sequence_root_sha256
    )
    assert changed_panel.manifest.manifest_sha256 != first.manifest.manifest_sha256

    object.__setattr__(first.manifest, "manifest_sha256", "0" * 64)
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="hash differs"):
        subject.canonical_d2_derived_hourly_manifest_v0(first.manifest)


def test_derived_decimal_sums_and_hashes_ignore_ambient_context() -> None:
    volume = Decimal("123456789.123456789")
    quote_volume = Decimal("987654321.987654321")
    source = tuple(
        _five_minute_candle(index).model_copy(
            update={
                "quote_volume": quote_volume,
                "taker_buy_base_volume": volume,
                "taker_buy_quote_volume": quote_volume,
                "volume": volume,
            }
        )
        for index in range(12)
    )

    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        low_precision = _small_panel(source)
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_UP
        high_precision = _small_panel(source)

    assert low_precision.candles[0].volume == Decimal("1481481469.481481468")
    assert subject.canonical_d2_derived_hour_v0(
        low_precision.candles[0]
    ) == subject.canonical_d2_derived_hour_v0(high_precision.candles[0])
    assert low_precision.manifest == high_precision.manifest
    assert (
        low_precision.manifest.manifest_sha256
        == high_precision.manifest.manifest_sha256
    )


def test_derived_decimal_sum_fails_closed_above_exact_precision_bound() -> None:
    source = [_five_minute_candle(index) for index in range(12)]
    source[0] = source[0].model_copy(
        update={
            "quote_volume": Decimal("1E+300"),
            "taker_buy_base_volume": Decimal(0),
            "taker_buy_quote_volume": Decimal(0),
            "volume": Decimal("1E+300"),
        }
    )
    source[1] = source[1].model_copy(
        update={
            "quote_volume": Decimal(1),
            "taker_buy_base_volume": Decimal(0),
            "taker_buy_quote_volume": Decimal(0),
            "volume": Decimal(1),
        }
    )

    with pytest.raises(
        subject.D2HistoricalDerivedHourlyContractErrorV0,
        match="fixed exact Decimal precision",
    ):
        _small_panel(tuple(source))


def test_panel_validation_detects_post_construction_candle_tamper() -> None:
    panel = _small_panel()
    changed = panel.candles[0].model_copy(update={"close": Decimal("111.25")})
    object.__setattr__(panel, "candles", (changed,))

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="sequence root"):
        subject.validate_d2_derived_hourly_panel_v0(panel)


@pytest.mark.parametrize(
    "replacement_index,replacement,expected_message",
    (
        (5, _five_minute_candle(6), "gapped, duplicated"),
        (5, _five_minute_candle(4), "gapped, duplicated"),
        (
            5,
            _five_minute_candle(5).model_copy(update={"is_closed": False}),
            "unclosed",
        ),
        (
            5,
            _five_minute_candle(5).model_copy(update={"close_time_ms": 1_800_000}),
            "close time",
        ),
        (
            5,
            _five_minute_candle(5, symbol="ETHUSDT"),
            "mixed market, symbol, or interval",
        ),
        (
            5,
            _five_minute_candle(5).model_copy(update={"market": Market.SPOT}),
            "mixed market, symbol, or interval",
        ),
        (
            5,
            _five_minute_candle(5).model_copy(update={"interval": "1h"}),
            "mixed market, symbol, or interval",
        ),
    ),
)
def test_derivation_rejects_gap_duplicate_unclosed_misalignment_and_mixed_source(
    replacement_index: int,
    replacement: Candle,
    expected_message: str,
) -> None:
    source = [_five_minute_candle(index) for index in range(12)]
    source[replacement_index] = replacement

    with pytest.raises(
        subject.D2HistoricalDerivedHourlyContractErrorV0,
        match=expected_message,
    ):
        _small_panel(tuple(source))


def test_derivation_rejects_partial_hour_wrong_boundary_and_public_small_panel() -> None:
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="row count"):
        _small_panel(tuple(_five_minute_candle(index) for index in range(11)))

    shifted = tuple(_five_minute_candle(index, start_ms=300_000) for index in range(12))
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="UTC hours"):
        subject._derive_d2_closed_hourly_for_contract_v0(
            symbol="BTCUSDT",
            five_minute_candles=shifted,
            five_minute_manifest_sha256=_MANIFEST_SHA,
            five_minute_compressed_data_sha256=_DATA_SHA,
            expected_source_start_ms=300_000,
            expected_source_end_ms_exclusive=3_900_000,
            expected_source_row_count=12,
            expected_derived_row_count=1,
        )

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="row count"):
        subject.derive_d2_closed_hourly_v0(
            symbol="BTCUSDT",
            five_minute_candles=tuple(_five_minute_candle(index) for index in range(12)),
            five_minute_manifest_sha256=_MANIFEST_SHA,
            five_minute_compressed_data_sha256=_DATA_SHA,
        )


def test_hour_eligibility_includes_just_closed_hour_only_at_its_final_5m_close() -> None:
    close_times = (3_599_999, 7_199_999)

    assert subject.d2_latest_eligible_hour_close_ms_v0(3_299_999) is None
    assert subject.d2_eligible_hour_end_index_v0(close_times, 3_299_999) == 0
    assert subject.d2_latest_eligible_hour_close_ms_v0(3_599_999) == 3_599_999
    assert subject.d2_eligible_hour_end_index_v0(close_times, 3_599_999) == 1
    assert subject.d2_latest_eligible_hour_close_ms_v0(3_899_999) == 3_599_999
    assert subject.d2_eligible_hour_end_index_v0(close_times, 3_899_999) == 1
    assert subject.d2_eligible_hour_end_index_v0(close_times, 7_199_999) == 2

    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="5m slot"):
        subject.d2_latest_eligible_hour_close_ms_v0(3_600_000)
    with pytest.raises(subject.D2HistoricalDerivedHourlyContractErrorV0, match="strictly ordered"):
        subject.d2_eligible_hour_end_index_v0((3_599_999, 3_599_999), 3_599_999)
