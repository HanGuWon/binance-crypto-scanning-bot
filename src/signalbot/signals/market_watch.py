from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal

from signalbot.config import MarketWatchSettings
from signalbot.domain.enums import Direction, SignalFamily
from signalbot.domain.models import (
    Candle,
    FeatureSnapshot,
    GainerWatchState,
    Instrument,
    RuleEvaluation,
)


@dataclass(frozen=True, slots=True)
class PriorityGainer:
    rank: int | None
    symbol: str
    price_change_percent_24h: float
    quote_volume_24h_usdt: float
    expires_at_ms: int | None = None
    peak_quote_volume_usdt: float | None = None


def classify_trend(feature: FeatureSnapshot) -> tuple[str, tuple[str, ...]]:
    """Classify the closed-candle trend from independent price/momentum evidence."""

    bullish = (
        feature.price > feature.ema20 > feature.ema50,
        feature.ema20_slope_atr > 0,
        feature.macd_histogram > 0,
        feature.rsi >= 55,
    )
    bearish = (
        feature.price < feature.ema20 < feature.ema50,
        feature.ema20_slope_atr < 0,
        feature.macd_histogram < 0,
        feature.rsi <= 45,
    )
    bullish_votes = sum(bullish)
    bearish_votes = sum(bearish)
    if bullish_votes >= 3 and bullish_votes > bearish_votes:
        label = "상승 추세"
    elif bearish_votes >= 3 and bearish_votes > bullish_votes:
        label = "하락 추세"
    else:
        label = "혼조/횡보"
    strength = "강함" if feature.adx >= 25 else "약함"
    evidence = (
        f"EMA20/50 {feature.ema20:.8g}/{feature.ema50:.8g}",
        f"EMA20 slope {feature.ema20_slope_atr:+.2f} ATR/3봉",
        f"MACD hist {feature.macd_histogram:+.6g}",
        f"RSI14 {feature.rsi:.1f}",
        f"ADX14 {feature.adx:.1f} ({strength})",
    )
    return label, evidence


class MarketWatchEngine:
    """Generate sparse, closed-candle mover and Bollinger-touch observations."""

    def __init__(self, settings: MarketWatchSettings) -> None:
        self.settings = settings
        self._priority: dict[str, PriorityGainer] = {}
        self._retained: dict[str, PriorityGainer] = {}
        self._snapshots: deque[tuple[int, dict[str, PriorityGainer]]] = deque(maxlen=4)

    @property
    def priority_symbols(self) -> frozenset[str]:
        return frozenset(self._priority)

    def set_priority_gainers(
        self, instruments: tuple[Instrument, ...], retained: tuple[Instrument, ...] = (),
        states: tuple[GainerWatchState, ...] = (), observed_ms: int = 0,
    ) -> tuple[frozenset[str], frozenset[str]]:
        previous = frozenset(self._priority)
        self._priority = {
            item.symbol: PriorityGainer(
                rank=rank,
                symbol=item.symbol,
                price_change_percent_24h=item.price_change_percent_24h,
                quote_volume_24h_usdt=float(item.quote_volume),
            )
            for rank, item in enumerate(instruments[: self.settings.top_n], start=1)
        }
        current = frozenset(self._priority)
        by_symbol = {state.symbol: state for state in states}
        self._retained = {
            item.symbol: PriorityGainer(
                rank=None, symbol=item.symbol,
                price_change_percent_24h=item.price_change_percent_24h,
                quote_volume_24h_usdt=float(item.quote_volume),
                expires_at_ms=by_symbol[item.symbol].expires_at_ms,
                peak_quote_volume_usdt=by_symbol[item.symbol].peak_quote_volume_usdt,
            )
            for item in retained[:self.settings.max_retained_symbols]
            if item.symbol not in current
        }
        self._snapshots.append((observed_ms, {**self._priority, **self._retained}))
        return previous - current, current - previous

    def monitored_symbols(self, now_ms: int) -> frozenset[str]:
        return frozenset(self._priority) | frozenset(
            symbol for symbol, item in self._retained.items()
            if item.expires_at_ms is not None and now_ms < item.expires_at_ms
        )

    def evaluate(
        self, candle: Candle, feature: FeatureSnapshot, history: list[Candle] | None = None,
        *, now_ms: int | None = None,
    ) -> tuple[RuleEvaluation, ...]:
        if (
            not self.settings.enabled or candle.market is not self.settings.market
            or not candle.is_closed
        ):
            return ()
        if (
            feature.event_time_ms != candle.close_time_ms or feature.market is not candle.market
            or feature.symbol != candle.symbol or feature.interval != candle.interval
        ):
            return ()
        if candle.symbol not in (self._priority.keys() | self._retained.keys()):
            return ()
        snapshot = next(
            (items for observed_ms, items in reversed(self._snapshots)
             if observed_ms <= candle.close_time_ms),
            {},
        )
        gainer = snapshot.get(candle.symbol)
        if gainer is None:
            return ()
        effective_now = max(candle.close_time_ms, now_ms or 0)
        current_gainer = self._priority.get(candle.symbol) or self._retained[candle.symbol]
        active = (
            gainer.quote_volume_24h_usdt > self.settings.min_quote_volume_usdt
            # Current liquidity is an operational notification gate, never historical evidence.
            and current_gainer.quote_volume_24h_usdt > self.settings.min_quote_volume_usdt
            and (gainer.expires_at_ms is None or effective_now < gainer.expires_at_ms)
        )
        trend_label, trend_evidence = classify_trend(feature)
        common = {
            "informational_only": True,
            "event_only": True,
            "event_cooldown_seconds": self.settings.event_cooldown_seconds,
            "market_watch": True,
            "trend_label": trend_label,
            "trend_evidence": trend_evidence,
            "gainer_rank": gainer.rank,
            "retained_gainer": gainer.rank is None,
            "watch_expires_at_ms": gainer.expires_at_ms,
            "peak_quote_volume_usdt": gainer.peak_quote_volume_usdt,
            "price_change_percent_24h": gainer.price_change_percent_24h,
            "quote_volume_24h_usdt": gainer.quote_volume_24h_usdt,
            "bollinger_middle": feature.bollinger_middle,
            "bollinger_upper": feature.bollinger_upper,
            "bollinger_lower": feature.bollinger_lower,
            "candle_high": float(candle.high),
            "candle_low": float(candle.low),
            "candle_close": float(candle.close),
            "ema20": feature.ema20,
            "ema50": feature.ema50,
            "ema20_slope_atr": feature.ema20_slope_atr,
            "macd_histogram": feature.macd_histogram,
            "rsi": feature.rsi,
            "adx": feature.adx,
            "taker_buy_ratio": (
                feature.taker_buy_ratio if feature.closed_kline_flow_available else None
            ),
            "flow_interval": candle.interval,
        }
        top = RuleEvaluation(
            market=candle.market,
            symbol=candle.symbol,
            family=SignalFamily.TOP_GAINER,
            direction=Direction.RISK_UP,
            timeframe=candle.interval,
            event_time_ms=candle.close_time_ms,
            score=100 if active and gainer.rank is not None else 0,
            triggered=active and gainer.rank is not None,
            eligible=False,
            price=Decimal(candle.close),
            reasons=(
                f"Binance 24h 상승률 #{gainer.rank}: {gainer.price_change_percent_24h:+.2f}%",
                f"24h 거래대금 {gainer.quote_volume_24h_usdt / 1_000_000:.1f}M USDT",
                f"폐봉 기술 추세: {trend_label}",
            ),
            regime=feature.regime,
            metadata={
                **common,
                "market_watch_kind": "top_gainer",
                "invalidation_condition": (
                    f"24h 상승 순위 {self.settings.top_n}위 밖 또는 "
                    f"24h 거래대금 {self.settings.min_quote_volume_usdt / 1_000_000:.0f}M USDT 이하"
                ),
            },
        )
        evaluations: list[RuleEvaluation] = [top]
        upper = feature.bollinger_upper
        lower = feature.bollinger_lower
        if upper is not None:
            upper_touch = float(candle.high) >= upper
            evaluations.append(
                self._bollinger_evaluation(
                    candle,
                    feature,
                    common,
                    family=SignalFamily.BOLLINGER_UPPER_TOUCH,
                    direction=Direction.RISK_UP,
                    touched=active and upper_touch,
                    band=upper,
                    trend_label=trend_label,
                )
            )
        if lower is not None:
            lower_touch = float(candle.low) <= lower
            evaluations.append(
                self._bollinger_evaluation(
                    candle,
                    feature,
                    common,
                    family=SignalFamily.BOLLINGER_LOWER_TOUCH,
                    direction=Direction.RISK_DOWN,
                    touched=active and lower_touch,
                    band=lower,
                    trend_label=trend_label,
                )
            )
        if self.settings.context_alerts_enabled:
            evaluations.extend(self._context_evaluations(
                candle, feature, common, active, history or [], trend_label,
            ))
        concurrent = tuple(item.family.value for item in evaluations if item.triggered)
        evaluations = [item.model_copy(update={
            "metadata": {**item.metadata, "concurrent_watch_events": concurrent},
        }) for item in evaluations]
        return tuple(evaluations)

    def _context_evaluations(
        self, candle: Candle, feature: FeatureSnapshot, common: dict[str, object],
        active: bool, history: list[Candle], trend_label: str,
    ) -> list[RuleEvaluation]:
        result: list[RuleEvaluation] = []
        for family, direction, label in (
            (SignalFamily.WATCH_TREND_UP, Direction.RISK_UP, "상승 추세"),
            (SignalFamily.WATCH_TREND_DOWN, Direction.RISK_DOWN, "하락 추세"),
        ):
            result.append(self._observation(
                candle, feature, common, family, direction,
                active and feature.adx >= 25 and trend_label == label,
                (f"폐봉 추세 {label}; ADX {feature.adx:.1f}",),
                f"추세가 {label}에서 혼조/반대 방향으로 바뀌거나 ADX가 25 미만",
            ))
        prior = [
            item for item in history
            if item.is_closed and item.market is candle.market and item.symbol == candle.symbol
            and item.interval == candle.interval and item.close_time_ms < candle.open_time_ms
        ][-20:]
        contiguous = len(prior) == 20 and all(
            left.close_time_ms + 1 == right.open_time_ms
            for left, right in zip(prior, [*prior[1:], candle], strict=True)
        )
        low = min((float(item.low) for item in prior), default=feature.recent_low)
        high = max((float(item.high) for item in prior), default=feature.recent_high)
        width = high - low
        midpoint = (high + low) / 2
        range_valid = (
            active and contiguous and feature.atr > 0 and low > 0 and width >= 2 * feature.atr
            and feature.adx < 20 and abs(feature.ema20_slope_atr) <= 0.25
            and feature.efficiency_ratio_20 is not None and feature.efficiency_ratio_20 <= 0.30
            and feature.closed_kline_flow_available
            and low < float(candle.close) < high
        )
        bottom = low + width * 0.20
        top = high - width * 0.20
        # Repeated edge tests distinguish oscillation from an isolated wide impulse.
        range_valid = range_valid and (
            sum(float(item.low) <= bottom for item in prior) >= 2
            and sum(float(item.high) >= top for item in prior) >= 2
        )
        ambiguous = float(candle.low) <= bottom and float(candle.high) >= top
        range_common = {
            **common, "range_low": low, "range_high": high, "range_target": midpoint,
            "range_round_trip_cost_bps": self.settings.range_round_trip_cost_bps,
        }
        for family, direction in (
            (SignalFamily.RANGE_LONG, Direction.LONG),
            (SignalFamily.RANGE_SHORT, Direction.SHORT),
        ):
            long = direction is Direction.LONG
            rejection = (
                float(candle.low) <= bottom and bottom < float(candle.close) <= low + width * 0.35
                and candle.close > candle.open and feature.taker_buy_ratio >= 0.55
                if long else
                float(candle.high) >= top and high - width * 0.35 <= float(candle.close) < top
                and candle.close < candle.open and feature.taker_buy_ratio <= 0.45
            )
            headroom_bps = abs(midpoint - float(candle.close)) / float(candle.close) * 10_000
            triggered = (
                range_valid and not ambiguous and rejection
                and headroom_bps >= 2 * self.settings.range_round_trip_cost_bps
            )
            invalidation = low - 0.25 * feature.atr if long else high + 0.25 * feature.atr
            result.append(self._observation(
                candle, feature, range_common, family, direction, triggered,
                (
                    f"이전 20폐봉 박스 {low:.8g} ~ {high:.8g}; 중간 목표 {midpoint:.8g}",
                    f"경계 재진입 {'양봉' if long else '음봉'} + "
                    f"주도 매수 {feature.taker_buy_ratio:.1%}",
                    f"ADX {feature.adx:.1f}; ER20 {feature.efficiency_ratio_20}",
                    f"중간 목표 여유 {headroom_bps:.1f}bps; 추정 왕복 비용 "
                    f"{self.settings.range_round_trip_cost_bps:.1f}bps "
                    "(실제 체결 비용은 다를 수 있음)",
                ),
                f"박스 경계 이탈 또는 횡보·체결 압력 조건 해제; 무효화 {invalidation:.8g}",
                Decimal(str(invalidation)) if invalidation > 0 else None,
            ))
        return result

    @staticmethod
    def _observation(
        candle: Candle, feature: FeatureSnapshot, common: dict[str, object],
        family: SignalFamily, direction: Direction, triggered: bool,
        reasons: tuple[str, ...], invalidation_condition: str,
        invalidation: Decimal | None = None,
    ) -> RuleEvaluation:
        return RuleEvaluation(
            market=candle.market, symbol=candle.symbol, family=family, direction=direction,
            timeframe=candle.interval, event_time_ms=candle.close_time_ms,
            score=100 if triggered else 0, triggered=triggered, eligible=False,
            price=candle.close, invalidation=invalidation, reasons=reasons,
            regime=feature.regime,
            metadata={**common, "market_watch_kind": family.value,
                      "invalidation_condition": invalidation_condition},
        )

    @staticmethod
    def _bollinger_evaluation(
        candle: Candle,
        feature: FeatureSnapshot,
        common: dict[str, object],
        *,
        family: SignalFamily,
        direction: Direction,
        touched: bool,
        band: float,
        trend_label: str,
    ) -> RuleEvaluation:
        upper = family is SignalFamily.BOLLINGER_UPPER_TOUCH
        side = "상단" if upper else "하단"
        observed = float(candle.high if upper else candle.low)
        reasons = (
            (
                f"Bollinger(20, 2 stdev) {side} {band:.8g}; "
                f"봉 {'고가' if upper else '저가'} {observed:.8g}",
                f"폐봉 기술 추세: {trend_label}",
            )
            if touched
            else (f"Bollinger {side} 비접촉; 이벤트 재무장",)
        )
        return RuleEvaluation(
            market=candle.market,
            symbol=candle.symbol,
            family=family,
            direction=direction,
            timeframe=candle.interval,
            event_time_ms=candle.close_time_ms,
            score=100 if touched else 0,
            triggered=touched,
            eligible=False,
            price=Decimal(candle.close),
            reasons=reasons,
            regime=feature.regime,
            metadata={
                **common,
                "market_watch_kind": "bollinger_upper_touch" if upper else "bollinger_lower_touch",
                "band_value": band,
                "touched": touched,
                "invalidation_condition": (
                    "다음 폐봉 고가가 상단 밴드 아래로 복귀"
                    if upper
                    else "다음 폐봉 저가가 하단 밴드 위로 복귀"
                ),
            },
        )
