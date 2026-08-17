from pathlib import Path

import pytest

from signalbot.backtest.engine import FundingRate
from signalbot.backtest.funding import (
    FundingDataset,
    FundingValidationError,
    funding_sha256,
    read_funding_csv,
    verify_funding_dataset,
    write_funding_csv,
)


def test_funding_dataset_round_trip_is_deterministic(tmp_path: Path) -> None:
    dataset = FundingDataset(
        "BTCUSDT",
        0,
        10,
        (FundingRate(1, 0.0001, 100.0), FundingRate(9, -0.0002, None)),
    )
    first = write_funding_csv(dataset, tmp_path / "one.csv.gz")
    second = write_funding_csv(dataset, tmp_path / "two.csv.gz")
    assert first.read_bytes() == second.read_bytes()
    assert funding_sha256(first) == funding_sha256(second)
    assert read_funding_csv(first) == dataset
    assert (
        verify_funding_dataset(
            first,
            expected_symbol="btcusdt",
            expected_start_time_ms=0,
            expected_end_time_ms=10,
        )
        == dataset
    )


def test_funding_reuse_rejects_stale_identity_or_range(tmp_path: Path) -> None:
    dataset = FundingDataset(
        "BTCUSDT",
        0,
        10,
        (FundingRate(1, 0.0001, 100.0), FundingRate(9, -0.0002, None)),
    )
    path = write_funding_csv(dataset, tmp_path / "funding.csv.gz")

    with pytest.raises(FundingValidationError, match="symbol/request range"):
        verify_funding_dataset(
            path,
            expected_symbol="ETHUSDT",
            expected_start_time_ms=0,
            expected_end_time_ms=10,
        )
    with pytest.raises(FundingValidationError, match="symbol/request range"):
        verify_funding_dataset(
            path,
            expected_symbol="BTCUSDT",
            expected_start_time_ms=0,
            expected_end_time_ms=11,
        )


def test_funding_dataset_rejects_out_of_range_or_duplicate_events() -> None:
    with pytest.raises(FundingValidationError, match="outside"):
        FundingDataset("BTCUSDT", 0, 10, (FundingRate(11, 0.0001, None),))
    with pytest.raises(FundingValidationError, match="strictly ordered"):
        FundingDataset(
            "BTCUSDT",
            0,
            10,
            (FundingRate(1, 0.0001, None), FundingRate(1, 0.0002, None)),
        )
