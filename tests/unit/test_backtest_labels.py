import math

import pytest

from signalbot.backtest.labels import KlineProxyOutcomeLabel, classify_kline_proxy_outcome


@pytest.mark.parametrize(
    ("long_net_return", "short_net_return", "edge_margin", "expected"),
    [
        (0.01, -0.02, 0.0, KlineProxyOutcomeLabel.LONG),
        (0.02, 0.01, 0.0, KlineProxyOutcomeLabel.LONG),
        (0.011, 0.012, 0.01, KlineProxyOutcomeLabel.SHORT),
        (-0.01, 0.02, 0.0, KlineProxyOutcomeLabel.SHORT),
    ],
)
def test_classify_kline_proxy_outcome_selects_strictly_better_profitable_direction(
    long_net_return: float,
    short_net_return: float,
    edge_margin: float,
    expected: KlineProxyOutcomeLabel,
) -> None:
    assert (
        classify_kline_proxy_outcome(long_net_return, short_net_return, edge_margin) == expected
    )


@pytest.mark.parametrize(
    ("long_net_return", "short_net_return", "edge_margin"),
    [
        (0.0, -0.01, 0.0),
        (0.01, -0.01, 0.01),
        (-0.01, -0.02, 0.0),
        (0.01, 0.01, 0.0),
        (0.02, 0.02, 0.01),
    ],
)
def test_classify_kline_proxy_outcome_returns_flat_at_boundaries(
    long_net_return: float,
    short_net_return: float,
    edge_margin: float,
) -> None:
    assert (
        classify_kline_proxy_outcome(long_net_return, short_net_return, edge_margin)
        is KlineProxyOutcomeLabel.FLAT
    )


@pytest.mark.parametrize(
    ("long_net_return", "short_net_return", "edge_margin"),
    [
        (math.nan, 0.0, 0.0),
        (0.0, math.inf, 0.0),
        (0.0, -math.inf, 0.0),
        (0.0, 0.0, math.nan),
        (0.0, 0.0, math.inf),
        (0.0, 0.0, -0.001),
    ],
)
def test_classify_kline_proxy_outcome_rejects_invalid_inputs(
    long_net_return: float,
    short_net_return: float,
    edge_margin: float,
) -> None:
    with pytest.raises(ValueError):
        classify_kline_proxy_outcome(long_net_return, short_net_return, edge_margin)
