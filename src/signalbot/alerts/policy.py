from __future__ import annotations

from signalbot.config import AlertSettings
from signalbot.domain.enums import SignalFamily, SignalStage
from signalbot.domain.models import SignalDecision

_MARKET_WATCH_FAMILIES = {
    SignalFamily.TOP_GAINER,
    SignalFamily.BOLLINGER_UPPER_TOUCH,
    SignalFamily.BOLLINGER_LOWER_TOUCH,
}


def should_deliver_discord(decision: SignalDecision, settings: AlertSettings) -> bool:
    """Return whether a durable signal should become a Discord delivery intent.

    ``focused`` keeps research/state transitions in SQLite while limiting the
    operator channel to sparse market-watch events, actual directional confirms,
    and paper technical exits.
    """

    if settings.delivery_mode == "all":
        return True
    if decision.family in _MARKET_WATCH_FAMILIES:
        return True
    if decision.family is SignalFamily.TECHNICAL_EXIT:
        return decision.stage is SignalStage.CONFIRMED
    if decision.family in {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK}:
        return False
    return decision.stage is SignalStage.CONFIRMED
