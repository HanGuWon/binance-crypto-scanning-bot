import pytest
from pydantic import ValidationError

from signalbot.config import VolumeFeatureSettings


@pytest.mark.parametrize(
    "update",
    [
        {"taker_short_bars": 4},
        {"taker_long_bars": 10},
        {"vpci_short_window": 6},
        {"vpci_long_window": 21},
        {"vpci_atr_window": 14},
        {"vpci_signal_window": 4},
        {"vpci_slope_lag": 2},
    ],
)
def test_frozen_volume_windows_reject_mislabeled_provenance(
    update: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        VolumeFeatureSettings.model_validate(update)
