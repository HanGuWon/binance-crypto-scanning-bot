import pytest

from signalbot.prospective.directional_candidates import (
    FixtureReplayRow,
    load_preregistration,
    replay_fixture,
)


def _row(**updates: object) -> FixtureReplayRow:
    values: dict[str, object] = {
        "opportunity_id": "opp-1",
        "candidate_version": "futures-bidirectional-v1",
        "symbol": "BTCUSDT",
        "direction": "long",
        "decision_time_ms": 300_000,
        "candle_close_time_ms": 300_000,
        "higher_timeframe_time_ms": 299_999,
        "entry_open_time_ms": 600_000,
        "entry_price": 100.0,
        "exit_price": 101.0,
        "funding_return": 0.0,
    }
    values.update(updates)
    return FixtureReplayRow.model_validate(values)


def test_replay_is_idempotent_for_exact_duplicate_rows() -> None:
    preregistration = load_preregistration("config/research.futures-bidirectional.v1.yaml")
    receipt = replay_fixture([_row(), _row()], preregistration=preregistration)

    assert receipt.input_rows == 2
    assert receipt.unique_rows == 1
    assert receipt.exact_duplicates_removed == 1


@pytest.mark.parametrize(
    "updates",
    [
        {"candle_closed": False},
        {"higher_timeframe_time_ms": 300_000},
        {"entry_open_time_ms": 300_000},
        {"candle_close_time_ms": 299_999},
    ],
)
def test_replay_rejects_open_future_or_optimistic_rows(updates: dict[str, object]) -> None:
    preregistration = load_preregistration("config/research.futures-bidirectional.v1.yaml")
    row = _row(**updates)

    with pytest.raises(ValueError):
        replay_fixture([row], preregistration=preregistration)


def test_replay_rejects_changed_payload_for_same_opportunity_id() -> None:
    preregistration = load_preregistration("config/research.futures-bidirectional.v1.yaml")
    first = _row()
    changed = _row(exit_price=102.0)

    with pytest.raises(ValueError, match="changed payload"):
        replay_fixture([first, changed], preregistration=preregistration)
