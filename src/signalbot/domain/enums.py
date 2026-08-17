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
    TECHNICAL_EXIT = "technical_exit"


class SignalStage(StrEnum):
    IDLE = "idle"
    WATCH = "watch"
    SETUP = "setup"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
