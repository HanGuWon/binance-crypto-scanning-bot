from datetime import UTC, datetime
from pathlib import Path

import pytest

from signalbot.backtest.config import (
    BacktestAsset,
    BacktestSpec,
    BacktestSplit,
    load_backtest_spec,
)
from signalbot.domain.enums import Direction
from signalbot.prospective.directional_candidates import (
    FixtureReplayRow,
    load_preregistration,
    replay_fixture,
)
from signalbot.prospective.directional_validation import (
    _base_spec_for_directional_research,
    _data_authority,
)

CONFIG_PATH = Path("config/research.futures-bidirectional.v1.yaml")


def _base_spec() -> BacktestSpec:
    return BacktestSpec(
        protocol_version="base",
        rule_version="base",
        interval="5m",
        data_start=datetime(2024, 1, 1, tzinfo=UTC),
        evaluation_start=datetime(2024, 2, 1, tzinfo=UTC),
        evaluation_end=datetime(2024, 3, 1, tzinfo=UTC),
        assets=[
            BacktestAsset(
                asset="BTC",
                cohort="anchor",
                spot_symbol="BTCUSDT",
                futures_symbol="BTCUSDT",
            )
        ],
        splits=[
            BacktestSplit(
                name="development",
                start=datetime(2024, 2, 1, tzinfo=UTC),
                end=datetime(2024, 3, 1, tzinfo=UTC),
            )
        ],
    )


def test_directional_research_scope_is_opt_in_and_keeps_existing_default() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    base = load_backtest_spec("config/backtest.5m.r2-c0-corrected.yaml")

    assert base.direction_scope == "market_default"
    research = _base_spec_for_directional_research(base, preregistration)
    assert research.direction_scope == "futures_bidirectional"
    assert research.candidate_policy == "strict_pit_htf_diagnostic"


def test_data_authority_hash_changes_when_an_input_manifest_changes(tmp_path: Path) -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    spec = _base_spec()
    first = tmp_path / "one.manifest.json"
    second = tmp_path / "two.manifest.json"
    first.write_text("one\n", encoding="utf-8")
    second.write_text("two\n", encoding="utf-8")

    first_hash, _ = _data_authority(
        preregistration=preregistration,
        spec=spec,
        paths={"one": first},
    )
    second_hash, _ = _data_authority(
        preregistration=preregistration,
        spec=spec,
        paths={"one": second},
    )

    assert first_hash != second_hash


def test_fixture_replay_rejects_unclosed_or_same_bar_rows() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    row = FixtureReplayRow(
        opportunity_id="op-1",
        candidate_version=preregistration.candidate_version,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        decision_time_ms=100,
        candle_close_time_ms=100,
        higher_timeframe_time_ms=99,
        entry_open_time_ms=101,
        entry_price=100,
        exit_price=101,
        funding_return=0,
    )
    assert replay_fixture([row, row], preregistration=preregistration).unique_rows == 1

    with pytest.raises(ValueError, match="unclosed"):
        replay_fixture(
            [row.model_copy(update={"candle_closed": False})],
            preregistration=preregistration,
        )
    with pytest.raises(ValueError, match="next bar open"):
        replay_fixture(
            [row.model_copy(update={"entry_open_time_ms": 100})],
            preregistration=preregistration,
        )
