from enum import StrEnum


class Market(StrEnum):
    SPOT = "spot"
    FUTURES = "futures"


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"
    RISK_UP = "risk_up"
    RISK_DOWN = "risk_down"


class SignalFamily(StrEnum):
    SQUEEZE_LONG = "squeeze_long"
    SQUEEZE_SHORT = "squeeze_short"
    BREAKOUT_LONG = "breakout_long"
    BREAKDOWN_SHORT = "breakdown_short"
    PULLBACK_LONG = "pullback_long"
    PULLBACK_SHORT = "pullback_short"
    EXHAUSTION_SHORT = "exhaustion_short"
    CAPITULATION_LONG = "capitulation_long"
    PUMP_RISK = "pump_risk"
    CRASH_RISK = "crash_risk"
    TOP_GAINER = "top_gainer"
    BOLLINGER_UPPER_TOUCH = "bollinger_upper_touch"
    BOLLINGER_LOWER_TOUCH = "bollinger_lower_touch"
    WATCH_TREND_UP = "watch_trend_up"
    WATCH_TREND_DOWN = "watch_trend_down"
    RANGE_LONG = "range_long"
    RANGE_SHORT = "range_short"
    TECHNICAL_EXIT = "technical_exit"


class SignalStage(StrEnum):
    IDLE = "idle"
    WATCH = "watch"
    SETUP = "setup"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
