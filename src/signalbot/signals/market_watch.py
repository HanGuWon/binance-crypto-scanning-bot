from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from signalbot.config import MarketWatchSettings
from signalbot.domain.enums import Direction, SignalFamily
from signalbot.domain.models import Candle, FeatureSnapshot, Instrument, RuleEvaluation


@dataclass(frozen=True, slots=True)
class PriorityGainer:
    rank: int
    symbol: str
    price_change_percent_24h: float
    quote_volume_24h_usdt: float


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

    @property
    def priority_symbols(self) -> frozenset[str]:
        return frozenset(self._priority)

    def set_priority_gainers(
        self, instruments: tuple[Instrument, ...]
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
        return previous - current, current - previous

    def evaluate(self, candle: Candle, feature: FeatureSnapshot) -> tuple[RuleEvaluation, ...]:
        if not self.settings.enabled or candle.market is not self.settings.market:
            return ()
        gainer = self._priority.get(candle.symbol)
        if gainer is None:
            return ()
        trend_label, trend_evidence = classify_trend(feature)
        common = {
            "informational_only": True,
            "event_only": True,
            "event_cooldown_seconds": self.settings.event_cooldown_seconds,
            "market_watch": True,
            "trend_label": trend_label,
            "trend_evidence": trend_evidence,
            "gainer_rank": gainer.rank,
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
        }
        top = RuleEvaluation(
            market=candle.market,
            symbol=candle.symbol,
            family=SignalFamily.TOP_GAINER,
            direction=Direction.RISK_UP,
            timeframe=candle.interval,
            event_time_ms=candle.close_time_ms,
            score=100,
            triggered=True,
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
                    touched=upper_touch,
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
                    touched=lower_touch,
                    band=lower,
                    trend_label=trend_label,
                )
            )
        return tuple(evaluations)

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
