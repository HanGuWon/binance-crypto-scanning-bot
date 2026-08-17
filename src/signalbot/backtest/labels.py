from __future__ import annotations

import math
from enum import StrEnum


class KlineProxyOutcomeLabel(StrEnum):
    LONG = "KLINE_PROXY_LONG"
    FLAT = "KLINE_PROXY_FLAT"
    SHORT = "KLINE_PROXY_SHORT"


def classify_kline_proxy_outcome(
    long_net_return: float,
    short_net_return: float,
    edge_margin: float = 0.0,
) -> KlineProxyOutcomeLabel:
    """Classify a kline-proxy outcome from cost-adjusted directional returns."""
    if not all(math.isfinite(value) for value in (long_net_return, short_net_return, edge_margin)):
        raise ValueError("returns and edge margin must be finite")
    if edge_margin < 0.0:
        raise ValueError("edge margin must be non-negative")

    if long_net_return > edge_margin and long_net_return > short_net_return:
        return KlineProxyOutcomeLabel.LONG
    if short_net_return > edge_margin and short_net_return > long_net_return:
        return KlineProxyOutcomeLabel.SHORT
    return KlineProxyOutcomeLabel.FLAT
