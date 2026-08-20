from __future__ import annotations

import math
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction, SignalFamily
from signalbot.domain.models import MarketRegime, MiniTicker, RuleEvaluation


@dataclass(frozen=True, slots=True)
class PricePoint:
    event_time_ms: int
    price: float


class AnomalyDetector:
    def __init__(self, settings: SignalSettings) -> None:
        self.settings = settings
        self._points: dict[tuple[object, str], deque[PricePoint]] = defaultdict(
            lambda: deque(maxlen=settings.anomaly_history_points)
        )

    def update(
        self, ticker: MiniTicker, allowed_symbols: frozenset[str], regime: MarketRegime
    ) -> tuple[RuleEvaluation, ...]:
        """Evaluate both risk families after one valid, in-order observation.

        Returning explicit zero-score evaluations is intentional: the signal state
        machine needs a valid idle observation to clear a prior intrabar warning.
        Rejected symbols, invalid prices, and out-of-order observations return no
        evaluations and therefore cannot accidentally reset live state.

        Liquidity semantics (documented fail-closed contract):
        - a *valid* quote_volume below the configured floor is real evidence of
          insufficient liquidity and therefore emits the idle (score 0) families,
          which is what allows a previously liquidity-qualified warning to clear;
        - a *missing* (``None``) quote_volume is unusable evidence: liquidity
          qualification cannot be evaluated, so no evaluation is returned and any
          prior warning is preserved. Absence of data never clears a warning.
        """

        if ticker.symbol not in allowed_symbols or ticker.close <= 0:
            return ()
        liquidity_floor = self.settings.anomaly_min_quote_volume_usdt
        if liquidity_floor > 0:
            if ticker.quote_volume is None:
                return ()
            if float(ticker.quote_volume) < liquidity_floor:
                return self._with_idle_families(
                    ticker,
                    regime,
                    None,
                    reason=(
                        "below anomaly liquidity floor "
                        f"({float(ticker.quote_volume):.0f} < "
                        f"{liquidity_floor:.0f} USDT)"
                    ),
                )
        key = (ticker.market, ticker.symbol)
        points = self._points[key]
        point = PricePoint(ticker.event_time_ms, float(ticker.close))
        if points and point.event_time_ms < points[-1].event_time_ms:
            return ()
        if points and point.event_time_ms == points[-1].event_time_ms:
            points[-1] = point
        else:
            points.append(point)
        cutoff = ticker.event_time_ms - max(
            self.settings.anomaly_horizon_seconds * 20_000, 600_000
        )
        while points and points[0].event_time_ms < cutoff:
            points.popleft()
        triggered: RuleEvaluation | None = None
        if len(points) < self.settings.anomaly_min_points:
            return self._with_idle_families(ticker, regime, triggered)

        target = ticker.event_time_ms - self.settings.anomaly_horizon_seconds * 1000
        anchor = min(points, key=lambda candidate: abs(candidate.event_time_ms - target))
        if anchor.event_time_ms <= target + 5_000 and anchor.price > 0:
            horizon_return = point.price / anchor.price - 1
            values = list(points)
            incremental = [
                math.log(current.price / previous.price)
                for previous, current in pairwise(values)
                if previous.price > 0 and current.price > 0
            ]
            enough_returns = len(incremental) >= self.settings.anomaly_min_points - 1
            if (
                abs(horizon_return) >= self.settings.anomaly_min_absolute_return
                and enough_returns
            ):
                median = statistics.median(incremental)
                mad = statistics.median(abs(value - median) for value in incremental)
                sigma = max(1.4826 * mad, 1e-9)
                robust_z = abs(math.log(point.price / anchor.price)) / (
                    sigma * math.sqrt(max(1.0, self.settings.anomaly_horizon_seconds))
                )
                if robust_z >= self.settings.anomaly_robust_zscore:
                    triggered = self._triggered(
                        ticker, regime, horizon_return, robust_z
                    )
        return self._with_idle_families(ticker, regime, triggered)

    def retain_symbols(self, allowed_symbols: frozenset[str]) -> int:
        """Prune price histories outside the bounded surveillance universe."""

        allowed = frozenset(symbol.upper() for symbol in allowed_symbols)
        stale = [key for key in self._points if key[1] not in allowed]
        for key in stale:
            del self._points[key]
        return len(stale)

    def _with_idle_families(
        self,
        ticker: MiniTicker,
        regime: MarketRegime,
        triggered: RuleEvaluation | None,
        *,
        reason: str = "valid intrabar observation; anomaly conditions absent",
    ) -> tuple[RuleEvaluation, RuleEvaluation]:
        by_family = {triggered.family: triggered} if triggered is not None else {}
        return (
            by_family.get(SignalFamily.PUMP_RISK)
            or self._idle(
                ticker, regime, SignalFamily.PUMP_RISK, Direction.RISK_UP, reason
            ),
            by_family.get(SignalFamily.CRASH_RISK)
            or self._idle(
                ticker, regime, SignalFamily.CRASH_RISK, Direction.RISK_DOWN, reason
            ),
        )

    def _idle(
        self,
        ticker: MiniTicker,
        regime: MarketRegime,
        family: SignalFamily,
        direction: Direction,
        reason: str,
    ) -> RuleEvaluation:
        return RuleEvaluation(
            market=ticker.market,
            symbol=ticker.symbol,
            family=family,
            direction=direction,
            timeframe=f"{self.settings.anomaly_horizon_seconds}s",
            event_time_ms=ticker.event_time_ms,
            score=0,
            triggered=False,
            price=ticker.close,
            reasons=(reason,),
            regime=regime,
            metadata={"intrabar": True, "idle_evaluation": True},
        )

    def _triggered(
        self,
        ticker: MiniTicker,
        regime: MarketRegime,
        horizon_return: float,
        robust_z: float,
    ) -> RuleEvaluation:
        upward = horizon_return > 0
        family = SignalFamily.PUMP_RISK if upward else SignalFamily.CRASH_RISK
        direction = Direction.RISK_UP if upward else Direction.RISK_DOWN
        score = min(
            100,
            int(
                70
                + min(15, robust_z - self.settings.anomaly_robust_zscore)
                + min(15, abs(horizon_return) * 500)
            ),
        )
        return RuleEvaluation(
            market=ticker.market,
            symbol=ticker.symbol,
            family=family,
            direction=direction,
            timeframe=f"{self.settings.anomaly_horizon_seconds}s",
            event_time_ms=ticker.event_time_ms,
            score=score,
            triggered=True,
            price=Decimal(str(ticker.close)),
            reasons=(
                f"{self.settings.anomaly_horizon_seconds}s return {horizon_return:+.2%}",
                f"robust anomaly z-score {robust_z:.2f}",
                "intrabar all-market mini-ticker warning",
            ),
            regime=regime,
            metadata={"return": horizon_return, "robust_zscore": robust_z, "intrabar": True},
        )
