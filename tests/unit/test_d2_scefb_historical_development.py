from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
from dataclasses import replace
from decimal import ROUND_DOWN, Decimal, getcontext, localcontext
from pathlib import Path

import pytest

from signalbot.backtest import d1_scefb_historical_attempt_wal as d1_wal
from signalbot.backtest import d1_scefb_historical_development as d1
from signalbot.backtest import d2_scefb_derived_hourly_historical as d2_source
from signalbot.backtest import d2_scefb_historical_development as subject
from signalbot.backtest.d1_scefb_historical_attempt_wal import (
    D1AttemptWalBindingsV0,
    D1AttemptWalPrefixV0,
    D1OutcomeAccessGrantV0,
)
from signalbot.backtest.downstream_code_freeze import (
    DownstreamCodeFreezeAuthorityV1,
    create_downstream_code_freeze_v1,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy import d1_scefb as d1_strategy

_RUN_ID = "d2-synthetic-run"
_RUN_STARTED_MS = 10_000


def _authority() -> d2_source.D2HistoricalInputAuthorityV0:
    five = tuple(
        d1.D1HistoricalKlineManifestBindingV0(
            symbol=symbol,
            interval="5m",
            relative_manifest_path=relative_path,
            manifest_sha256=manifest_sha256,
        )
        for symbol, relative_path, manifest_sha256 in (
            d2_source.D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
        )
    )
    funding = tuple(
        d1.D1HistoricalFundingFileBindingV0(
            symbol=symbol,
            relative_path=relative_path,
            sha256=sha256,
        )
        for symbol, relative_path, sha256 in (
            d2_source.D2_HISTORICAL_FIXED_FUNDING_FILES_V0
        )
    )
    return d2_source.build_d2_historical_input_authority_v0(
        five_minute_manifests=five,
        funding_manifest_relative_path=(
            d2_source.D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0
        ),
        funding_manifest_sha256=(
            d2_source.D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0
        ),
        funding_files=funding,
    )


def _freeze(
    authority: d2_source.D2HistoricalInputAuthorityV0,
) -> subject.D2HistoricalDevelopmentFreezeV0:
    return subject.D2HistoricalDevelopmentFreezeV0(
        manifest_sha256="a" * 64,
        manifest_created_at_ms=1,
        input_authority_sha256=authority.authority_sha256,
        frozen_file_count=100,
        _factory_token=subject._FREEZE_FACTORY_TOKEN,
    )


def _five_minute_candles(symbol: str) -> tuple[Candle, ...]:
    values: list[Candle] = []
    for index in range(12):
        open_ms = index * 300_000
        price = Decimal(100 + index)
        values.append(
            Candle(
                market=Market.FUTURES,
                symbol=symbol,
                interval="5m",
                open_time_ms=open_ms,
                close_time_ms=open_ms + 299_999,
                open=price,
                high=price + 1,
                low=price - 1,
                close=price + Decimal("0.5"),
                volume=Decimal(10),
                quote_volume=Decimal(1_000),
                trade_count=index + 1,
                taker_buy_base_volume=Decimal(4),
                taker_buy_quote_volume=Decimal(400),
                is_closed=True,
            )
        )
    return tuple(values)


def _causal_core_candles(
    symbol: str,
    *,
    mutate_future: bool,
) -> tuple[Candle, ...]:
    """Build 252 complete hours; only the final, post-decision hour may differ."""

    values: list[Candle] = []
    for index in range(252 * 12):
        open_ms = index * 300_000
        open_price = close = Decimal("100")
        high = Decimal("101")
        low = Decimal("99")
        quote_volume = Decimal("100000")
        taker_buy_quote_volume = Decimal("50000")
        if symbol == "BTCUSDT":
            if index == 250 * 12:
                high = Decimal("101.7")
                low = Decimal("98.3")
                close = Decimal("101.5")
                quote_volume = Decimal("500000")
                taker_buy_quote_volume = Decimal("350000")
            elif index == 250 * 12 + 2:
                open_price = Decimal("101.5")
                high = Decimal("107")
                low = Decimal("101.3")
                close = Decimal("106")
            elif index == 250 * 12 + 4:
                open_price = close = Decimal("106")
                high = Decimal("106.2")
                low = Decimal("105.8")
            elif index == 250 * 12 + 6:
                open_price = Decimal("106")
                high = Decimal("108.2")
                low = Decimal("104")
                close = Decimal("108")
                quote_volume = Decimal("500000")
                taker_buy_quote_volume = Decimal("350000")
            elif index == 250 * 12 + 11:
                open_price = Decimal("108")
                high = Decimal("109.7")
                low = Decimal("105")
                close = Decimal("109.5")
                quote_volume = Decimal("500000")
                taker_buy_quote_volume = Decimal("350000")
            elif mutate_future and index >= 251 * 12:
                offset = Decimal(index - 251 * 12)
                open_price = close = Decimal("120") + offset
                high = close + Decimal("1")
                low = close - Decimal("1")
                quote_volume = Decimal("900000") + offset
                taker_buy_quote_volume = Decimal("600000") + offset
        values.append(
            Candle(
                market=Market.FUTURES,
                symbol=symbol,
                interval="5m",
                open_time_ms=open_ms,
                close_time_ms=open_ms + 299_999,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=Decimal("10"),
                quote_volume=quote_volume,
                trade_count=index + 1,
                taker_buy_base_volume=Decimal("4"),
                taker_buy_quote_volume=taker_buy_quote_volume,
                is_closed=True,
            )
        )
    return tuple(values)


def _causal_core_data_sha256(candles: tuple[Candle, ...]) -> str:
    document = [
        {
            "close": str(value.close),
            "close_time_ms": value.close_time_ms,
            "high": str(value.high),
            "low": str(value.low),
            "open": str(value.open),
            "open_time_ms": value.open_time_ms,
            "quote_volume": str(value.quote_volume),
            "taker_buy_quote_volume": str(value.taker_buy_quote_volume),
        }
        for value in candles
    ]
    return hashlib.sha256(canonical_json_line({"candles": document})).hexdigest()


def _causal_core_inputs(
    authority: d2_source.D2HistoricalInputAuthorityV0,
    *,
    mutate_future: bool,
) -> tuple[
    tuple[d1.D1HistoricalReplaySymbolInputV0, ...],
    tuple[d2_source.D2DerivedHourlyManifestV0, ...],
]:
    inputs: list[d1.D1HistoricalReplaySymbolInputV0] = []
    manifests: list[d2_source.D2DerivedHourlyManifestV0] = []
    for symbol in d1.D1_HISTORICAL_UNIVERSE_V0:
        candles = _causal_core_candles(
            symbol,
            mutate_future=mutate_future and symbol == "BTCUSDT",
        )
        data_sha256 = _causal_core_data_sha256(candles)
        five = d1.D1HistoricalAuthenticatedFiveMinuteV0(
            symbol=symbol,
            manifest_sha256=authority.five_minute_binding(symbol).manifest_sha256,
            data_sha256=data_sha256,
            candles=candles,
            _factory_token=d1._AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN,
        )
        funding_binding = authority.funding_files[
            d1.D1_HISTORICAL_UNIVERSE_V0.index(symbol)
        ]
        funding = d1.D1HistoricalAuthenticatedFundingV0(
            symbol=symbol,
            file_sha256=funding_binding.sha256,
            start_time_ms=0,
            end_time_ms=0,
            points=(),
            exact_standard_8h_development_coverage=True,
            _factory_token=d1._AUTHENTICATED_FUNDING_FACTORY_TOKEN,
        )
        panel = d2_source._derive_d2_closed_hourly_for_contract_v0(
            symbol=symbol,
            five_minute_candles=candles,
            five_minute_manifest_sha256=five.manifest_sha256,
            five_minute_compressed_data_sha256=data_sha256,
            expected_source_start_ms=0,
            expected_source_end_ms_exclusive=252 * 3_600_000,
            expected_source_row_count=252 * 12,
            expected_derived_row_count=252,
        )
        proof = subject._build_replay_proof_v0(
            input_authority=authority,
            five=five,
            funding=funding,
            derived_hourly=panel,
        )
        assert (
            proof.replay_input.source_root_policy
            == d1.D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0
        )
        inputs.append(proof.replay_input)
        manifests.append(panel.manifest)
    return tuple(inputs), tuple(manifests)


def _small_panel(
    *,
    symbol: str,
    manifest_sha256: str,
    data_sha256: str,
) -> d2_source.D2DerivedHourlyPanelV0:
    return d2_source._derive_d2_closed_hourly_for_contract_v0(
        symbol=symbol,
        five_minute_candles=_five_minute_candles(symbol),
        five_minute_manifest_sha256=manifest_sha256,
        five_minute_compressed_data_sha256=data_sha256,
        expected_source_start_ms=0,
        expected_source_end_ms_exclusive=3_600_000,
        expected_source_row_count=12,
        expected_derived_row_count=1,
    )


def _manifests(
    authority: d2_source.D2HistoricalInputAuthorityV0,
) -> tuple[d2_source.D2DerivedHourlyManifestV0, ...]:
    return tuple(
        d2_source.D2DerivedHourlyManifestV0(
            symbol=symbol,
            five_minute_manifest_sha256=(
                authority.five_minute_binding(symbol).manifest_sha256
            ),
            five_minute_compressed_data_sha256=hashlib.sha256(
                f"data:{symbol}".encode()
            ).hexdigest(),
            source_first_open_time_ms=d1.D1_HISTORICAL_DATA_START_MS_V0,
            source_last_close_time_ms=d1.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
            source_row_count=d1.D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
            derived_first_open_time_ms=d1.D1_HISTORICAL_DATA_START_MS_V0,
            derived_last_close_time_ms=d1.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
            derived_row_count=d1.D1_HISTORICAL_HOURLY_ROW_COUNT_V0,
            ordered_canonical_sequence_root_sha256=hashlib.sha256(
                f"derived:{symbol}".encode()
            ).hexdigest(),
            _factory_token=d2_source._DERIVED_MANIFEST_FACTORY_TOKEN,
        )
        for symbol in d1.D1_HISTORICAL_UNIVERSE_V0
    )


def _empty_summary() -> d1.D1HistoricalDevelopmentSummaryV0:
    return d1._summarize_development_v0(
        episodes=(),
        censors=(),
        counters=d1._RunCountersV0(),
        funding_coverage_status_by_symbol=tuple(
            (
                symbol,
                d1.D1HistoricalFundingCoverageStatusV0.EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE.value,
            )
            for symbol in d1.D1_HISTORICAL_UNIVERSE_V0
        ),
    )


def _empty_core() -> d1.D1HistoricalReplayCoreResultV0:
    return d1.D1HistoricalReplayCoreResultV0(
        episodes=(),
        censors=(),
        summary=_empty_summary(),
        _factory_token=d1._REPLAY_CORE_RESULT_FACTORY_TOKEN,
    )


def _bindings(
    authority: d2_source.D2HistoricalInputAuthorityV0,
    freeze: subject.D2HistoricalDevelopmentFreezeV0,
    *,
    input_authority_file_sha256: str | None = None,
) -> D1AttemptWalBindingsV0:
    authority_raw = d2_source.canonical_d2_historical_input_authority_v0(authority)
    return D1AttemptWalBindingsV0(
        run_id=_RUN_ID,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        input_authority_sha256=authority.authority_sha256,
        input_authority_file_sha256=(
            input_authority_file_sha256 or hashlib.sha256(authority_raw).hexdigest()
        ),
        funding_authority_file_sha256=authority.funding_manifest_sha256,
        preregistration_sha256=d2_source.D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        output_path_sha256="b" * 64,
    )


def _reproduction_bindings(
    authority: d2_source.D2HistoricalInputAuthorityV0,
    freeze: subject.D2HistoricalDevelopmentFreezeV0,
) -> D1AttemptWalBindingsV0:
    return D1AttemptWalBindingsV0(
        run_id=subject.D2_HISTORICAL_REPRODUCTION_RUN_ID_V0,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        input_authority_sha256=authority.authority_sha256,
        input_authority_file_sha256=hashlib.sha256(
            d2_source.canonical_d2_historical_input_authority_v0(authority)
        ).hexdigest(),
        funding_authority_file_sha256=authority.funding_manifest_sha256,
        preregistration_sha256=d2_source.D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        output_path_sha256=hashlib.sha256(
            subject._D2_REPRODUCTION_OUTPUT_PATH_HASH_DOMAIN
            + subject.D2_HISTORICAL_REPRODUCTION_OUTPUT_RELATIVE_PATH_V0.encode("utf-8")
        ).hexdigest(),
    )


def _grant(bindings: D1AttemptWalBindingsV0) -> D1OutcomeAccessGrantV0:
    grant = object.__new__(D1OutcomeAccessGrantV0)
    start_sha256 = "c" * 64
    object.__setattr__(grant, "_start_record_sha256", start_sha256)
    object.__setattr__(
        grant,
        "_start_prefix",
        D1AttemptWalPrefixV0(
            record_count=2,
            complete_bytes=100,
            last_record_sha256=start_sha256,
            prefix_sha256="d" * 64,
        ),
    )
    object.__setattr__(grant, "_bindings", bindings)
    object.__setattr__(grant, "_attempt_directory_sha256", "e" * 64)
    object.__setattr__(grant, "_start_seal_sha256", "f" * 64)
    object.__setattr__(grant, "_consume_lock", threading.Lock())
    object.__setattr__(grant, "_consumed", False)
    object.__setattr__(grant, "_mint_process_id", os.getpid())
    return grant


def _result_bundle() -> tuple[
    subject.D2HistoricalDevelopmentResultV0,
    d2_source.D2HistoricalInputAuthorityV0,
    subject.D2HistoricalDevelopmentFreezeV0,
]:
    authority = _authority()
    freeze = _freeze(authority)
    bindings = _bindings(authority, freeze)
    result = subject.D2HistoricalDevelopmentResultV0(
        run_id=_RUN_ID,
        run_started_at_ms=_RUN_STARTED_MS,
        start_record_sha256="c" * 64,
        attempt_directory_sha256="e" * 64,
        attempt_bindings_sha256=bindings.bindings_sha256,
        input_authority_sha256=authority.authority_sha256,
        input_authority_file_sha256=bindings.input_authority_file_sha256,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        code_freeze_receipt_sha256=freeze.receipt_sha256,
        derived_hourly_manifests=_manifests(authority),
        episodes=(),
        censors=(),
        summary=_empty_summary(),
        _factory_token=subject._RESULT_FACTORY_TOKEN,
    )
    return result, authority, freeze


def _authenticated_funding(
    authority: d2_source.D2HistoricalInputAuthorityV0,
    *,
    substitute_first: bool = False,
) -> tuple[d1.D1HistoricalAuthenticatedFundingV0, ...]:
    return tuple(
        d1.D1HistoricalAuthenticatedFundingV0(
            symbol=binding.symbol,
            file_sha256=(
                "0" * 64 if substitute_first and index == 0 else binding.sha256
            ),
            start_time_ms=0,
            end_time_ms=0,
            points=(),
            exact_standard_8h_development_coverage=True,
            _factory_token=d1._AUTHENTICATED_FUNDING_FACTORY_TOKEN,
        )
        for index, binding in enumerate(authority.funding_files)
    )


def _artifact_test_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fresh(value: str | Path) -> Path:
        target = Path(value).absolute()
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() or target.is_symlink():
            raise d1.D1HistoricalDevelopmentContractErrorV0("target already exists")
        return target

    def publish(*, staging: Path, target: Path) -> None:
        staging.rename(target)

    monkeypatch.setattr(subject, "_fresh_artifact_target", fresh)
    monkeypatch.setattr(subject, "_fsync_directory_if_supported_v0", lambda _path: None)
    monkeypatch.setattr(subject, "_publish_staging_no_replace", publish)


def test_freeze_policy_is_exact_sorted_and_binds_a0_a1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    upstream = subject.d2_historical_development_freeze_upstream_v0(
        authority.authority_sha256
    )
    file_sha256 = {
        path: "9" * 64 for path in subject.D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
    }
    file_sha256.update(
        {
            subject._D2_PREREGISTRATION_RELATIVE_PATH: (
                d2_source.D2_HISTORICAL_PREREGISTRATION_SHA256_V0
            ),
            subject._D2_OPERATOR_AMENDMENT_RELATIVE_PATH: (
                subject.D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0
            ),
            subject._D2_OPERATOR_CORRECTION_A1_RELATIVE_PATH: (
                subject.D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0
            ),
            subject._D1_ECONOMIC_PREREGISTRATION_RELATIVE_PATH: (
                d2_source.D1_PREDECESSOR_PREREGISTRATION_SHA256_V0
            ),
            subject._D1_PREDECESSOR_FREEZE_RELATIVE_PATH: (
                d2_source.D1_PREDECESSOR_FREEZE_SHA256_V0
            ),
            subject._D1_FAILURE_EVIDENCE_MANIFEST_RELATIVE_PATH: (
                d2_source.D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0
            ),
            subject._D2_RUNNER_RELATIVE_PATH: "1" * 64,
            subject._D2_AUTHORITY_RELATIVE_PATH: "2" * 64,
            subject._D1_RULE_RELATIVE_PATH: "3" * 64,
        }
    )
    downstream = DownstreamCodeFreezeAuthorityV1(
        manifest_path=Path("freeze.json"),
        manifest_sha256="a" * 64,
        created_at_utc="2026-07-21T00:00:00+00:00",
        purpose=subject.D2_DEVELOPMENT_FREEZE_PURPOSE_V0,
        include_trees=subject.D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
        include_files=subject.D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
        included_suffixes=subject.D2_DEVELOPMENT_FREEZE_SUFFIXES_V0,
        upstream_sha256=upstream,
        file_sha256=file_sha256,
        file_size_bytes={name: 1 for name in file_sha256},
    )
    monkeypatch.setattr(subject, "load_downstream_code_freeze_v1", lambda *_a, **_k: downstream)

    loaded = subject.load_d2_historical_development_freeze_v0(
        "unused",
        workspace_root="unused",
        expected_manifest_sha256="a" * 64,
        input_authority=authority,
    )

    assert subject.D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0 == tuple(
        sorted(subject.D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0)
    )
    assert "d2_operator_correction_a1" in upstream
    assert loaded.operator_amendment_sha256 == (
        subject.D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0
    )
    assert loaded.operator_correction_a1_sha256 == (
        subject.D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0
    )

    monkeypatch.setattr(
        subject,
        "load_downstream_code_freeze_v1",
        lambda *_a, **_k: replace(downstream, purpose="SUBSTITUTED"),
    )
    with pytest.raises(subject.D2HistoricalDevelopmentContractErrorV0, match="exact D2"):
        subject.load_d2_historical_development_freeze_v0(
            "unused",
            workspace_root="unused",
            expected_manifest_sha256="a" * 64,
            input_authority=authority,
        )


def test_real_generic_freeze_round_trip_matches_canonical_d2_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    explicit_paths = {
        "docs/d1-prereg.md": b"d1 prereg\n",
        "docs/d2-a0.md": b"d2 a0\n",
        "docs/d2-a1.md": b"d2 a1\n",
        "docs/d2-prereg.md": b"d2 prereg\n",
        "evidence/failure.jsonl": b'{"failure":true}\n',
        "evidence/freeze.json": b'{"freeze":2}\n',
    }
    tree_paths = {
        "src/authority.py": b"AUTHORITY = 1\n",
        "src/rule.py": b"RULE = 1\n",
        "src/runner.py": b"RUNNER = 1\n",
    }
    for relative, raw in {**explicit_paths, **tree_paths}.items():
        path = tmp_path.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    include_files = tuple(sorted(explicit_paths))
    hashes = {
        relative: hashlib.sha256(raw).hexdigest()
        for relative, raw in {**explicit_paths, **tree_paths}.items()
    }
    patches: tuple[tuple[str, object], ...] = (
        ("D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0", ("src",)),
        ("D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0", include_files),
        ("D2_DEVELOPMENT_FREEZE_SUFFIXES_V0", (".py",)),
        ("_D2_PREREGISTRATION_RELATIVE_PATH", "docs/d2-prereg.md"),
        ("_D2_OPERATOR_AMENDMENT_RELATIVE_PATH", "docs/d2-a0.md"),
        ("_D2_OPERATOR_CORRECTION_A1_RELATIVE_PATH", "docs/d2-a1.md"),
        ("_D1_ECONOMIC_PREREGISTRATION_RELATIVE_PATH", "docs/d1-prereg.md"),
        ("_D1_PREDECESSOR_FREEZE_RELATIVE_PATH", "evidence/freeze.json"),
        ("_D1_FAILURE_EVIDENCE_MANIFEST_RELATIVE_PATH", "evidence/failure.jsonl"),
        ("_D2_RUNNER_RELATIVE_PATH", "src/runner.py"),
        ("_D2_AUTHORITY_RELATIVE_PATH", "src/authority.py"),
        ("_D1_RULE_RELATIVE_PATH", "src/rule.py"),
        ("D2_HISTORICAL_PREREGISTRATION_SHA256_V0", hashes["docs/d2-prereg.md"]),
        ("D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0", hashes["docs/d2-a0.md"]),
        ("D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0", hashes["docs/d2-a1.md"]),
        ("D1_PREDECESSOR_PREREGISTRATION_SHA256_V0", hashes["docs/d1-prereg.md"]),
        ("D1_PREDECESSOR_FREEZE_SHA256_V0", hashes["evidence/freeze.json"]),
        (
            "D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0",
            hashes["evidence/failure.jsonl"],
        ),
    )
    for name, value in patches:
        monkeypatch.setattr(subject, name, value)
    upstream = subject.d2_historical_development_freeze_upstream_v0(
        authority.authority_sha256
    )
    manifest_path = tmp_path / "artifacts/freeze.json"
    generic = create_downstream_code_freeze_v1(
        workspace_root=tmp_path,
        manifest_path=manifest_path,
        purpose=subject.D2_DEVELOPMENT_FREEZE_PURPOSE_V0,
        include_trees=("src",),
        include_files=include_files,
        included_suffixes=(".py",),
        upstream_sha256=upstream,
    )

    loaded = subject.load_d2_historical_development_freeze_v0(
        manifest_path,
        workspace_root=tmp_path,
        expected_manifest_sha256=generic.manifest_sha256,
        input_authority=authority,
    )

    assert loaded.manifest_sha256 == generic.manifest_sha256
    assert loaded.frozen_file_count == len(explicit_paths) + len(tree_paths)


def test_run_consumes_grant_before_any_file_loader_and_never_requests_native_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    freeze = _freeze(authority)
    grant = _grant(_bindings(authority, freeze))
    observed_bindings: list[d1.D1HistoricalKlineManifestBindingV0] = []
    captured_inputs: tuple[d1.D1HistoricalReplaySymbolInputV0, ...] = ()

    def funding_loader(**_kwargs: object) -> tuple[d1.D1HistoricalAuthenticatedFundingV0, ...]:
        assert grant.consumed
        return _authenticated_funding(authority)

    def five_loader(
        *,
        data_root: str | Path,
        binding: d1.D1HistoricalKlineManifestBindingV0,
    ) -> d1.D1HistoricalAuthenticatedFiveMinuteV0:
        assert grant.consumed
        assert data_root == "unused"
        observed_bindings.append(binding)
        return d1.D1HistoricalAuthenticatedFiveMinuteV0(
            symbol=binding.symbol,
            manifest_sha256=binding.manifest_sha256,
            data_sha256=hashlib.sha256(f"data:{binding.symbol}".encode()).hexdigest(),
            candles=_five_minute_candles(binding.symbol),
            _factory_token=d1._AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN,
        )

    def derive(**kwargs: object) -> d2_source.D2DerivedHourlyPanelV0:
        return _small_panel(
            symbol=str(kwargs["symbol"]),
            manifest_sha256=str(kwargs["five_minute_manifest_sha256"]),
            data_sha256=str(kwargs["five_minute_compressed_data_sha256"]),
        )

    def core_runner(
        *,
        symbol_inputs: object,
        run_id: str,
        decision_start_ms: int,
        decision_end_ms: int,
    ) -> d1.D1HistoricalReplayCoreResultV0:
        nonlocal captured_inputs
        expected_context = protocol_decimal_context_v2()
        assert getcontext().prec == expected_context.prec
        assert getcontext().rounding == expected_context.rounding
        assert getcontext().traps == expected_context.traps
        captured_inputs = tuple(symbol_inputs)  # type: ignore[arg-type]
        assert run_id == _RUN_ID
        assert decision_start_ms == d1.D1_HISTORICAL_DEVELOPMENT_START_MS_V0
        assert decision_end_ms == d1.D1_HISTORICAL_DEVELOPMENT_END_MS_V0
        return _empty_core()

    monkeypatch.setattr(
        subject,
        "load_d1_historical_authenticated_funding_bindings_v0",
        funding_loader,
    )
    monkeypatch.setattr(subject, "load_d1_historical_authenticated_five_minute_v0", five_loader)
    monkeypatch.setattr(subject, "derive_d2_closed_hourly_v0", derive)
    monkeypatch.setattr(subject, "run_d1_historical_replay_core_v0", core_runner)
    monkeypatch.setattr(subject, "_validate_production_derived_manifests_v0", lambda _value: None)

    with localcontext() as ambient:
        ambient.prec = 6
        ambient.rounding = ROUND_DOWN
        result = subject.run_d2_historical_development_v0(
            data_root="unused",
            input_authority=authority,
            code_freeze=freeze,
            outcome_access_grant=grant,
            run_id=_RUN_ID,
            run_started_at_ms=_RUN_STARTED_MS,
        )

    assert grant.consumed
    assert tuple(value.symbol for value in captured_inputs) == d1.D1_HISTORICAL_UNIVERSE_V0
    assert all(value.interval == "5m" for value in observed_bindings)
    assert all("1h" not in value.relative_manifest_path.lower() for value in observed_bindings)
    assert all(
        value.higher_timeframe_source_sha256
        == result.derived_hourly_manifests[index].manifest_sha256
        for index, value in enumerate(captured_inputs)
    )
    assert len({value.source_root_sha256 for value in captured_inputs}) == 10
    assert all(
        value.source_root_policy
        == d1.D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0
        for value in captured_inputs
    )


def test_d2_used_rows_core_is_causal_and_future_invariant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise D2 proof construction through the real public D1 replay core."""

    authority = _authority()
    original_entry = d1.evaluate_d1_entry_v0
    original_exit = d1.evaluate_d1_exit_v0
    watched_open_times = (250 * 3_600_000, 250 * 3_600_000 + 6 * 300_000, 251 * 3_600_000 - 300_000)
    signal_open_times = {watched_open_times[0], watched_open_times[2]}
    active_variant = ""
    entry_observations: dict[
        str,
        dict[int, tuple[int, tuple[int, ...], bytes]],
    ] = {"base": {}, "future": {}}
    exit_observations: dict[str, list[bytes]] = {"base": [], "future": []}

    def forced_long_signal(
        item: d1_strategy.D1EntryInputV0,
    ) -> d1_strategy.D1EntryDecisionV0:
        assert len(item.prior_bars) == d1_strategy.D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0
        assert len(item.hourly_bars) == d1_strategy.D1_HOURLY_BAR_COUNT_V0
        prior: list[d1_strategy.D1FiveMinuteBarV0] = []
        for index, source_bar in enumerate(item.prior_bars):
            if index == 0:
                width = Decimal("1")
            elif index - 1 < 216:
                width = Decimal("1")
            elif index - 1 < 276:
                width = Decimal("2")
            else:
                width = Decimal("1")
            imbalance = Decimal("-0.05") if index % 2 else Decimal("0.05")
            quote_volume = Decimal("200000")
            prior.append(
                d1_strategy.build_d1_five_minute_bar_v0(
                    open_ms=source_bar.open_ms,
                    open_price=Decimal("100"),
                    high_price=Decimal("100") + width / Decimal(2),
                    low_price=Decimal("100") - width / Decimal(2),
                    close_price=Decimal("100"),
                    quote_volume=quote_volume,
                    taker_buy_quote_volume=(
                        quote_volume * (Decimal(1) + imbalance) / Decimal(2)
                    ),
                    data_through_ms=source_bar.close_ms,
                    receipt_ms=source_bar.close_ms,
                )
            )
        current = d1_strategy.build_d1_five_minute_bar_v0(
            open_ms=item.current_bar.open_ms,
            open_price=Decimal("100"),
            high_price=Decimal("101.7"),
            low_price=Decimal("98.3"),
            close_price=Decimal("101.5"),
            quote_volume=Decimal("500000"),
            taker_buy_quote_volume=Decimal("350000"),
            data_through_ms=item.current_bar.close_ms,
            receipt_ms=item.current_bar.close_ms,
        )
        hourly = tuple(
            d1_strategy.build_d1_hourly_bar_v0(
                open_ms=value.open_ms,
                close_price=Decimal("100") + Decimal(index) / Decimal(10),
                data_through_ms=value.close_ms,
                receipt_ms=value.close_ms,
            )
            for index, value in enumerate(item.hourly_bars)
        )
        decision = original_entry(
            d1_strategy.build_d1_entry_input_v0(
                attempt_id=item.attempt_id,
                symbol=item.symbol,
                venue=item.venue,
                source_root_sha256=item.source_root_sha256,
                prior_bars=tuple(prior),
                current_bar=current,
                hourly_bars=hourly,
                required_fields_complete=item.required_fields_complete,
            )
        )
        assert decision.status is d1_strategy.D1EntryStatusV0.SIGNAL
        return decision

    def entry_wrapper(
        item: d1_strategy.D1EntryInputV0,
    ) -> d1_strategy.D1EntryDecisionV0:
        decision = (
            forced_long_signal(item)
            if item.symbol == "BTCUSDT" and item.current_bar.open_ms in signal_open_times
            else original_entry(item)
        )
        if item.symbol == "BTCUSDT" and item.current_bar.open_ms in watched_open_times:
            entry_observations[active_variant][item.current_bar.open_ms] = (
                item.hourly_bars[-1].open_ms,
                tuple(value.open_ms for value in item.hourly_bars),
                d1_strategy.canonical_d1_entry_decision_v0(decision),
            )
        return decision

    def exit_wrapper(
        item: d1_strategy.D1ExitInputV0,
    ) -> d1_strategy.D1ExitDecisionV0:
        decision = original_exit(item)
        if item.position.symbol == "BTCUSDT":
            exit_observations[active_variant].append(
                d1_strategy.canonical_d1_exit_decision_v0(decision)
            )
        return decision

    monkeypatch.setattr(d1, "evaluate_d1_entry_v0", entry_wrapper)
    monkeypatch.setattr(d1, "evaluate_d1_exit_v0", exit_wrapper)

    inputs_by_variant: dict[str, tuple[d1.D1HistoricalReplaySymbolInputV0, ...]] = {}
    manifests_by_variant: dict[
        str,
        tuple[d2_source.D2DerivedHourlyManifestV0, ...],
    ] = {}
    core_by_variant: dict[str, d1.D1HistoricalReplayCoreResultV0] = {}
    for variant, mutate_future in (("base", False), ("future", True)):
        active_variant = variant
        inputs, manifests = _causal_core_inputs(
            authority,
            mutate_future=mutate_future,
        )
        inputs_by_variant[variant] = inputs
        manifests_by_variant[variant] = manifests
        core_by_variant[variant] = d1.run_d1_historical_replay_core_v0(
            symbol_inputs=inputs,
            run_id="d2-used-rows-causal-core",
            decision_start_ms=250 * 3_600_000,
            decision_end_ms=251 * 3_600_000,
        )

    base_input = inputs_by_variant["base"][0]
    future_input = inputs_by_variant["future"][0]
    assert base_input.source_root_sha256 != future_input.source_root_sha256
    assert manifests_by_variant["base"][0].manifest_sha256 != (
        manifests_by_variant["future"][0].manifest_sha256
    )
    assert base_input.hourly[:-1] == future_input.hourly[:-1]
    assert base_input.hourly[-1] != future_input.hourly[-1]

    mid_open = watched_open_times[1]
    hour_end_open = watched_open_times[2]
    for variant in ("base", "future"):
        mid_last_hour, mid_hours, _mid_decision = entry_observations[variant][mid_open]
        end_last_hour, end_hours, _end_decision = entry_observations[variant][hour_end_open]
        assert mid_last_hour == 249 * 3_600_000
        assert 250 * 3_600_000 not in mid_hours
        assert end_last_hour == 250 * 3_600_000
        assert 251 * 3_600_000 not in end_hours

    assert entry_observations["base"] == entry_observations["future"]
    assert exit_observations["base"] == exit_observations["future"]
    assert exit_observations["base"]

    base_core = core_by_variant["base"]
    future_core = core_by_variant["future"]
    assert len(base_core.episodes) == len(future_core.episodes) == 1
    assert len(base_core.censors) == len(future_core.censors) == 1
    assert tuple(
        d1.canonical_d1_historical_censor_v0(value) for value in base_core.censors
    ) == tuple(
        d1.canonical_d1_historical_censor_v0(value) for value in future_core.censors
    )

    def economic_fields(value: d1.D1HistoricalEpisodeV0) -> tuple[object, ...]:
        return (
            value.statistical_unit_id,
            value.symbol,
            value.side,
            value.signal_event_id,
            value.signal_payload_sha256,
            value.signal_bar_open_ms,
            value.signal_decision_cutoff_ms,
            value.entry_reference_time_ms,
            value.entry_reference_price,
            value.entry_executable_price,
            value.exit_observation_open_ms,
            value.exit_observation_close_ms,
            value.exit_decision_event_id,
            value.exit_decision_payload_sha256,
            value.exit_reason,
            value.exit_reference_time_ms,
            value.exit_reference_price,
            value.exit_executable_price,
            value.funding_event_count,
            value.funding_evaluable,
            value.funding_inconclusive_reason,
            value.projections,
        )

    assert economic_fields(base_core.episodes[0]) == economic_fields(future_core.episodes[0])
    assert d1.canonical_d1_historical_episode_v0(base_core.episodes[0]) != (
        d1.canonical_d1_historical_episode_v0(future_core.episodes[0])
    )
    assert d1.canonical_d1_historical_summary_v0(base_core.summary) == (
        d1.canonical_d1_historical_summary_v0(future_core.summary)
    )


def test_grant_binding_failure_occurs_before_loader_and_does_not_consume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    freeze = _freeze(authority)
    grant = _grant(
        _bindings(
            authority,
            freeze,
            input_authority_file_sha256="0" * 64,
        )
    )
    monkeypatch.setattr(
        subject,
        "load_d1_historical_authenticated_funding_bindings_v0",
        lambda **_kwargs: pytest.fail("outcome loader was called before grant validation"),
    )

    with pytest.raises(subject.D2HistoricalDevelopmentContractErrorV0, match="grant differs"):
        subject.run_d2_historical_development_v0(
            data_root="unused",
            input_authority=authority,
            code_freeze=freeze,
            outcome_access_grant=grant,
            run_id=_RUN_ID,
            run_started_at_ms=_RUN_STARTED_MS,
        )
    assert not grant.consumed


@pytest.mark.parametrize("substitution", ["five", "funding"])
def test_valid_type_substituted_loader_return_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    substitution: str,
) -> None:
    authority = _authority()
    freeze = _freeze(authority)
    grant = _grant(_bindings(authority, freeze))

    monkeypatch.setattr(
        subject,
        "load_d1_historical_authenticated_funding_bindings_v0",
        lambda **_kwargs: _authenticated_funding(
            authority,
            substitute_first=substitution == "funding",
        ),
    )

    def five_loader(
        *,
        data_root: str | Path,
        binding: d1.D1HistoricalKlineManifestBindingV0,
    ) -> d1.D1HistoricalAuthenticatedFiveMinuteV0:
        del data_root
        digest = "0" * 64 if substitution == "five" and binding.symbol == "BTCUSDT" else (
            binding.manifest_sha256
        )
        return d1.D1HistoricalAuthenticatedFiveMinuteV0(
            symbol=binding.symbol,
            manifest_sha256=digest,
            data_sha256=hashlib.sha256(f"data:{binding.symbol}".encode()).hexdigest(),
            candles=_five_minute_candles(binding.symbol),
            _factory_token=d1._AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN,
        )

    monkeypatch.setattr(subject, "load_d1_historical_authenticated_five_minute_v0", five_loader)
    monkeypatch.setattr(
        subject,
        "derive_d2_closed_hourly_v0",
        lambda **kwargs: _small_panel(
            symbol=str(kwargs["symbol"]),
            manifest_sha256=str(kwargs["five_minute_manifest_sha256"]),
            data_sha256=str(kwargs["five_minute_compressed_data_sha256"]),
        ),
    )
    monkeypatch.setattr(
        subject,
        "run_d1_historical_replay_core_v0",
        lambda **kwargs: (tuple(kwargs["symbol_inputs"]), _empty_core())[1],
    )

    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="authenticated input identity",
    ):
        subject.run_d2_historical_development_v0(
            data_root="unused",
            input_authority=authority,
            code_freeze=freeze,
            outcome_access_grant=grant,
            run_id=_RUN_ID,
            run_started_at_ms=_RUN_STARTED_MS,
        )
    assert grant.consumed


def test_result_is_distinct_d2_and_forces_every_nonclaim() -> None:
    result, _authority_value, _freeze_value = _result_bundle()
    raw = subject.canonical_d2_historical_development_result_v0(result)
    document = json.loads(raw)

    assert document["schema_version"] == "d2_scefb_historical_development_result_v0"
    assert document["rule_version"] == subject.D2_HISTORICAL_DEVELOPMENT_RULE_V0
    assert document["operator_correction_a1_sha256"] == (
        subject.D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0
    )
    assert document["source_policy_version"] == d2_source.D2_HISTORICAL_SOURCE_POLICY_V0
    for name in (
        "historical_bbo_available",
        "paper_fill_claim",
        "execution_conclusive",
        "probability_claim",
        "efficacy_claim",
        "promoting",
        "prospective",
        "production_order_placement",
    ):
        assert document[name] is False
    assert b'd1_historical_development_result_v0' not in raw


def test_result_rejects_self_consistent_nonproduction_derived_manifests() -> None:
    result, authority, _freeze_value = _result_bundle()
    small = tuple(
        _small_panel(
            symbol=symbol,
            manifest_sha256=authority.five_minute_binding(symbol).manifest_sha256,
            data_sha256=hashlib.sha256(f"small:{symbol}".encode()).hexdigest(),
        ).manifest
        for symbol in d1.D1_HISTORICAL_UNIVERSE_V0
    )

    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="production authority",
    ):
        subject.D2HistoricalDevelopmentResultV0(
            run_id=result.run_id,
            run_started_at_ms=result.run_started_at_ms,
            start_record_sha256=result.start_record_sha256,
            attempt_directory_sha256=result.attempt_directory_sha256,
            attempt_bindings_sha256=result.attempt_bindings_sha256,
            input_authority_sha256=result.input_authority_sha256,
            input_authority_file_sha256=result.input_authority_file_sha256,
            code_freeze_manifest_sha256=result.code_freeze_manifest_sha256,
            code_freeze_receipt_sha256=result.code_freeze_receipt_sha256,
            derived_hourly_manifests=small,
            episodes=result.episodes,
            censors=result.censors,
            summary=result.summary,
            _factory_token=subject._RESULT_FACTORY_TOKEN,
        )


def test_completed_reproduction_is_one_use_read_only_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _artifact_test_boundary(monkeypatch)
    authority = _authority()
    freeze = _freeze(authority)
    bindings = _reproduction_bindings(authority, freeze)
    attempt_path = tmp_path.joinpath(
        *subject.D2_HISTORICAL_REPRODUCTION_ATTEMPT_RELATIVE_PATH_V0.split("/")
    ).resolve()
    attempt_path.parent.mkdir(parents=True)
    armed = d1_wal.create_armed_wal_v0(
        attempt_dir=attempt_path,
        bindings=bindings,
        armed_at_ms=1_000,
    )
    started = d1_wal.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    )
    core = _empty_core()
    result = subject.D2HistoricalDevelopmentResultV0(
        run_id=bindings.run_id,
        run_started_at_ms=2_000,
        start_record_sha256=started.snapshot.records[1].record_sha256,
        attempt_directory_sha256=started.snapshot.records[1].attempt_directory_sha256,
        attempt_bindings_sha256=bindings.bindings_sha256,
        input_authority_sha256=authority.authority_sha256,
        input_authority_file_sha256=bindings.input_authority_file_sha256,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        code_freeze_receipt_sha256=freeze.receipt_sha256,
        derived_hourly_manifests=_manifests(authority),
        episodes=core.episodes,
        censors=core.censors,
        summary=core.summary,
        _factory_token=subject._RESULT_FACTORY_TOKEN,
    )
    output_dir = tmp_path.joinpath(
        *subject.D2_HISTORICAL_REPRODUCTION_OUTPUT_RELATIVE_PATH_V0.split("/")
    ).resolve()
    artifacts = subject.write_d2_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=output_dir,
    )
    completed = d1_wal.append_terminal_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=3_000,
        result_sha256=result.result_sha256,
        artifact_manifest_sha256=artifacts.manifest_sha256,
    )
    captured: dict[str, subject.D2CompletedReproductionGrantV0] = {}
    mode = {"value": "good"}
    original_grant_loader = subject._load_d2_completed_reproduction_grant_v0

    def capture_grant(**kwargs: object) -> subject.D2CompletedReproductionGrantV0:
        grant = original_grant_loader(**kwargs)  # type: ignore[arg-type]
        captured["grant"] = grant
        return grant

    def replay_inputs(**kwargs: object) -> tuple[()]:
        assert captured["grant"].consumed
        manifests = kwargs["derived_manifests"]
        assert isinstance(manifests, list)
        replay_manifests = list(_manifests(authority))
        if mode["value"] == "raw-mutation":
            original = replay_manifests[0]
            replay_manifests[0] = d2_source.D2DerivedHourlyManifestV0(
                symbol=original.symbol,
                five_minute_manifest_sha256=original.five_minute_manifest_sha256,
                five_minute_compressed_data_sha256="0" * 64,
                source_first_open_time_ms=original.source_first_open_time_ms,
                source_last_close_time_ms=original.source_last_close_time_ms,
                source_row_count=original.source_row_count,
                derived_first_open_time_ms=original.derived_first_open_time_ms,
                derived_last_close_time_ms=original.derived_last_close_time_ms,
                derived_row_count=original.derived_row_count,
                ordered_canonical_sequence_root_sha256="1" * 64,
                _factory_token=d2_source._DERIVED_MANIFEST_FACTORY_TOKEN,
            )
        manifests.extend(replay_manifests)
        return ()

    def replay_core(**kwargs: object) -> d1.D1HistoricalReplayCoreResultV0:
        expected_context = protocol_decimal_context_v2()
        assert getcontext().prec == expected_context.prec
        assert getcontext().rounding == expected_context.rounding
        assert getcontext().traps == expected_context.traps
        assert tuple(kwargs["symbol_inputs"]) == ()  # type: ignore[arg-type]
        if mode["value"] == "output-swap":
            original_output = tmp_path / "byte-identical-original"
            output_dir.rename(original_output)
            shutil.copytree(original_output, output_dir)
        elif mode["value"] == "wal-override":
            d1_wal.append_terminal_v0(
                attempt_dir=completed.attempt_dir,
                expected_prefix=completed.prefix,
                state="AMBIGUOUS_OUTPUT",
                terminal_at_ms=4_000,
                detail_code="CONCURRENT_POST_COMPLETION_AMBIGUITY",
                result_sha256=result.result_sha256,
                artifact_manifest_sha256=artifacts.manifest_sha256,
            )
        return core

    monkeypatch.setattr(subject, "_load_d2_completed_reproduction_grant_v0", capture_grant)
    monkeypatch.setattr(subject, "_iter_authenticated_d2_replay_inputs_v0", replay_inputs)
    monkeypatch.setattr(subject, "run_d1_historical_replay_core_v0", replay_core)
    attempt_before = {path.name: path.read_bytes() for path in armed.attempt_dir.iterdir()}
    output_before = {path.name: path.read_bytes() for path in output_dir.iterdir()}

    def forbidden_write(*_args: object, **_kwargs: object) -> None:
        pytest.fail("reproduction invoked a publication write boundary")

    monkeypatch.setattr(subject, "_write_bounded_artifact_file", forbidden_write)
    monkeypatch.setattr(subject, "_publish_staging_no_replace", forbidden_write)

    with localcontext() as ambient:
        ambient.prec = 6
        ambient.rounding = ROUND_DOWN
        receipt = subject.reproduce_d2_historical_published_artifact_bundle_v0(
            data_root=tmp_path,
            attempt_dir=armed.attempt_dir,
            output_dir=output_dir,
            expected_attempt_bindings=bindings,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
        )

    assert receipt.completed_record_sha256 == completed.records[2].record_sha256
    assert receipt.result_sha256 == result.result_sha256
    assert receipt.artifact_manifest_sha256 == artifacts.manifest_sha256
    assert receipt.raw_replay_performed
    for name in (
        "published_artifacts_modified",
        "historical_bbo_available",
        "paper_fill_claim",
        "execution_conclusive",
        "probability_claim",
        "efficacy_claim",
        "promoting",
        "prospective",
        "production_order_placement",
    ):
        assert getattr(receipt, name) is False
    assert {path.name: path.read_bytes() for path in armed.attempt_dir.iterdir()} == attempt_before
    assert {path.name: path.read_bytes() for path in output_dir.iterdir()} == output_before
    successful_grant = captured["grant"]
    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="already consumed",
    ):
        successful_grant._consume_once_v0(lambda: None)

    mode["value"] = "raw-mutation"
    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="raw D2 reproduction result",
    ):
        subject.reproduce_d2_historical_published_artifact_bundle_v0(
            data_root=tmp_path,
            attempt_dir=armed.attempt_dir,
            output_dir=output_dir,
            expected_attempt_bindings=bindings,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
        )

    mode["value"] = "output-swap"
    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="stable published bundle",
    ):
        subject.reproduce_d2_historical_published_artifact_bundle_v0(
            data_root=tmp_path,
            attempt_dir=armed.attempt_dir,
            output_dir=output_dir,
            expected_attempt_bindings=bindings,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
        )

    mode["value"] = "wal-override"
    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="exact untorn",
    ):
        subject.reproduce_d2_historical_published_artifact_bundle_v0(
            data_root=tmp_path,
            attempt_dir=armed.attempt_dir,
            output_dir=output_dir,
            expected_attempt_bindings=bindings,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
        )


def test_reproduction_rejects_noncompleted_or_torn_wal_before_raw_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    freeze = _freeze(authority)
    bindings = _reproduction_bindings(authority, freeze)

    def armed_at(name: str) -> d1_wal.D1AttemptWalSnapshotV0:
        return d1_wal.create_armed_wal_v0(
            attempt_dir=(tmp_path / name).resolve(),
            bindings=bindings,
            armed_at_ms=1_000,
        )

    armed = armed_at("armed")
    failed_armed = armed_at("failed")
    failed_started = d1_wal.append_started_v0(
        attempt_dir=failed_armed.attempt_dir,
        expected_prefix=failed_armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    failed = d1_wal.append_terminal_v0(
        attempt_dir=failed_started.attempt_dir,
        expected_prefix=failed_started.prefix,
        state="FAILED",
        terminal_at_ms=3_000,
        detail_code="TEST_FAILURE",
    )
    ambiguous_armed = armed_at("ambiguous")
    ambiguous_started = d1_wal.append_started_v0(
        attempt_dir=ambiguous_armed.attempt_dir,
        expected_prefix=ambiguous_armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    ambiguous = d1_wal.append_terminal_v0(
        attempt_dir=ambiguous_started.attempt_dir,
        expected_prefix=ambiguous_started.prefix,
        state="AMBIGUOUS_OUTPUT",
        terminal_at_ms=3_000,
        detail_code="TEST_AMBIGUITY",
    )
    torn_armed = armed_at("torn")
    torn_started = d1_wal.append_started_v0(
        attempt_dir=torn_armed.attempt_dir,
        expected_prefix=torn_armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    with torn_started.wal_path.open("ab", buffering=0) as handle:
        handle.write(b"D1")
        handle.flush()
        os.fsync(handle.fileno())
    torn = d1_wal.load_attempt_wal_v0(
        torn_started.attempt_dir,
        expected_bindings=bindings,
    )
    extra_armed = armed_at("extra")
    extra_started = d1_wal.append_started_v0(
        attempt_dir=extra_armed.attempt_dir,
        expected_prefix=extra_armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    complete = d1_wal.append_terminal_v0(
        attempt_dir=extra_started.attempt_dir,
        expected_prefix=extra_started.prefix,
        state="COMPLETED",
        terminal_at_ms=3_000,
        result_sha256="1" * 64,
        artifact_manifest_sha256="2" * 64,
    )
    extra = d1_wal.append_terminal_v0(
        attempt_dir=complete.attempt_dir,
        expected_prefix=complete.prefix,
        state="AMBIGUOUS_OUTPUT",
        terminal_at_ms=4_000,
        detail_code="POST_COMPLETION_AMBIGUITY",
        result_sha256="1" * 64,
        artifact_manifest_sha256="2" * 64,
    )
    raw_loader_calls = 0

    def forbidden_raw_loader(**_kwargs: object) -> tuple[()]:
        nonlocal raw_loader_calls
        raw_loader_calls += 1
        return ()

    monkeypatch.setattr(subject, "_iter_authenticated_d2_replay_inputs_v0", forbidden_raw_loader)
    for wrong_bindings in (
        replace(bindings, run_id="wrong-d2-run"),
        replace(bindings, output_path_sha256="0" * 64),
    ):
        with pytest.raises(
            subject.D2HistoricalDevelopmentContractErrorV0,
            match="bindings differ",
        ):
            subject._load_d2_completed_reproduction_grant_v0(
                attempt_dir=armed.attempt_dir,
                expected_attempt_bindings=wrong_bindings,
                expected_input_authority=authority,
                expected_code_freeze=freeze,
            )
    for snapshot in (armed, failed_started, failed, ambiguous, torn, extra):
        monkeypatch.setattr(
            subject,
            "load_attempt_wal_v0",
            lambda *_a, snapshot=snapshot, **_k: snapshot,
        )
        with pytest.raises(
            subject.D2HistoricalDevelopmentContractErrorV0,
            match="exact untorn",
        ):
            subject._load_d2_completed_reproduction_grant_v0(
                attempt_dir=snapshot.attempt_dir,
                expected_attempt_bindings=bindings,
                expected_input_authority=authority,
                expected_code_freeze=freeze,
            )
    assert raw_loader_calls == 0


def test_artifacts_are_deterministic_restart_verifiable_tamper_evident_and_no_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _artifact_test_boundary(monkeypatch)
    result, authority, freeze = _result_bundle()
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"

    first = subject.write_d2_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=first_dir,
    )
    second = subject.write_d2_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=second_dir,
    )

    assert first.manifest_sha256 == second.manifest_sha256
    assert first.output_file_sha256 == second.output_file_sha256
    verified = subject.verify_d2_historical_published_artifact_bundle_v0(
        output_dir=first_dir,
        expected_result_sha256=result.result_sha256,
        expected_manifest_sha256=first.manifest_sha256,
        expected_input_authority=authority,
        expected_code_freeze=freeze,
        expected_run_id=result.run_id,
        expected_run_started_at_ms=result.run_started_at_ms,
        expected_start_record_sha256=result.start_record_sha256,
        expected_attempt_directory_sha256=result.attempt_directory_sha256,
        expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
    )
    assert verified.result_sha256 == result.result_sha256
    assert verified.artifact_manifest_sha256 == first.manifest_sha256

    total_bytes = sum(path.stat().st_size for path in first_dir.iterdir())
    with monkeypatch.context() as aggregate_patch:
        aggregate_patch.setattr(
            subject,
            "D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0",
            total_bytes - 1,
        )
        with pytest.raises(
            subject.D2HistoricalDevelopmentContractErrorV0,
            match="aggregate byte cap",
        ):
            subject.verify_d2_historical_published_artifact_bundle_v0(
                output_dir=first_dir,
                expected_result_sha256=result.result_sha256,
                expected_manifest_sha256=first.manifest_sha256,
                expected_input_authority=authority,
                expected_code_freeze=freeze,
                expected_run_id=result.run_id,
                expected_run_started_at_ms=result.run_started_at_ms,
                expected_start_record_sha256=result.start_record_sha256,
                expected_attempt_directory_sha256=result.attempt_directory_sha256,
                expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
            )

    original_revalidate = subject._revalidate_published_artifacts_v0

    def inject_member_after_final_read(**kwargs: object) -> None:
        original_revalidate(**kwargs)  # type: ignore[arg-type]
        (second_dir / "injected.jsonl").write_bytes(b"{}\n")

    with monkeypatch.context() as membership_patch:
        membership_patch.setattr(
            subject,
            "_revalidate_published_artifacts_v0",
            inject_member_after_final_read,
        )
        with pytest.raises(
            subject.D2HistoricalDevelopmentContractErrorV0,
            match="membership differs",
        ):
            subject.verify_d2_historical_published_artifact_bundle_v0(
                output_dir=second_dir,
                expected_result_sha256=result.result_sha256,
                expected_manifest_sha256=second.manifest_sha256,
                expected_input_authority=authority,
                expected_code_freeze=freeze,
                expected_run_id=result.run_id,
                expected_run_started_at_ms=result.run_started_at_ms,
                expected_start_record_sha256=result.start_record_sha256,
                expected_attempt_directory_sha256=result.attempt_directory_sha256,
                expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
            )

    before = {path.name: path.read_bytes() for path in first_dir.iterdir()}
    with pytest.raises(subject.D2HistoricalDevelopmentContractErrorV0, match="fresh"):
        subject.write_d2_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=first_dir,
        )
    assert {path.name: path.read_bytes() for path in first_dir.iterdir()} == before

    report_path = first_dir / "report.md"
    report_path.write_bytes(report_path.read_bytes() + b"tamper\n")
    manifest_path = first_dir / "manifest.jsonl"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["outputs"]["report.md"] = {
        "sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
        "size_bytes": len(report_path.read_bytes()),
    }
    coherent_manifest = canonical_json_line(manifest)
    manifest_path.write_bytes(coherent_manifest)
    with pytest.raises(subject.D2HistoricalDevelopmentContractErrorV0, match="report differs"):
        subject.verify_d2_historical_published_artifact_bundle_v0(
            output_dir=first_dir,
            expected_result_sha256=result.result_sha256,
            expected_manifest_sha256=hashlib.sha256(coherent_manifest).hexdigest(),
            expected_input_authority=authority,
            expected_code_freeze=freeze,
            expected_run_id=result.run_id,
            expected_run_started_at_ms=result.run_started_at_ms,
            expected_start_record_sha256=result.start_record_sha256,
            expected_attempt_directory_sha256=result.attempt_directory_sha256,
            expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
        )


def test_independent_verifier_rejects_reparse_directory_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, authority, freeze = _result_bundle()
    monkeypatch.setattr(
        subject,
        "_require_real_artifact_directory_v0",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            d1.D1HistoricalArtifactDurabilityErrorV0("reparse")
        ),
    )
    with pytest.raises(subject.D2HistoricalDevelopmentContractErrorV0, match="non-reparse"):
        subject.verify_d2_historical_published_artifact_bundle_v0(
            output_dir=tmp_path,
            expected_result_sha256=result.result_sha256,
            expected_manifest_sha256="0" * 64,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
            expected_run_id=result.run_id,
            expected_run_started_at_ms=result.run_started_at_ms,
            expected_start_record_sha256=result.start_record_sha256,
            expected_attempt_directory_sha256=result.attempt_directory_sha256,
            expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
        )


def test_independent_verifier_rejects_reparse_ancestor_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, authority, freeze = _result_bundle()
    target = tmp_path / "ancestor" / "bundle"
    target.mkdir(parents=True)
    original = subject._require_real_artifact_directory_v0
    observed: list[Path] = []

    def simulated_reparse(path: Path, label: str) -> os.stat_result:
        observed.append(path)
        if path == target.parent:
            raise d1.D1HistoricalArtifactDurabilityErrorV0("simulated ancestor junction")
        return original(path, label)

    monkeypatch.setattr(subject, "_require_real_artifact_directory_v0", simulated_reparse)
    with pytest.raises(
        subject.D2HistoricalDevelopmentContractErrorV0,
        match="only real non-reparse",
    ):
        subject.verify_d2_historical_published_artifact_bundle_v0(
            output_dir=target,
            expected_result_sha256=result.result_sha256,
            expected_manifest_sha256="0" * 64,
            expected_input_authority=authority,
            expected_code_freeze=freeze,
            expected_run_id=result.run_id,
            expected_run_started_at_ms=result.run_started_at_ms,
            expected_start_record_sha256=result.start_record_sha256,
            expected_attempt_directory_sha256=result.attempt_directory_sha256,
            expected_attempt_bindings_sha256=result.attempt_bindings_sha256,
        )
    assert target.parent in observed
