from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from math import isfinite
from typing import Any, Literal
from zoneinfo import ZoneInfo

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import (
    DIRECTIONAL_DIAGNOSTICS_METADATA_KEY,
    DirectionalDiagnostics,
    DirectionalSetupScore,
    SignalDecision,
)

KST = ZoneInfo("Asia/Seoul")
DISCORD_EMBED_TOTAL_LIMIT = 6_000
DISCORD_EMBED_TITLE_LIMIT = 256
DISCORD_EMBED_DESCRIPTION_LIMIT = 4_096
DISCORD_EMBED_FIELDS_LIMIT = 25
DISCORD_EMBED_FIELD_NAME_LIMIT = 256
DISCORD_EMBED_FIELD_VALUE_LIMIT = 1_024
DISCORD_EMBED_FOOTER_LIMIT = 2_048
DISCORD_USERNAME_LIMIT = 80


def _truncate_text(value: object, limit: int) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    if limit <= 1:
        return "…"[:limit]
    return f"{text[: limit - 1]}…"


def _embed_text_length(embed: dict[str, Any]) -> int:
    footer = embed.get("footer")
    footer_text = footer.get("text", "") if isinstance(footer, dict) else ""
    fields = embed.get("fields")
    field_text = 0
    if isinstance(fields, list):
        field_text = sum(
            len(str(field.get("name", ""))) + len(str(field.get("value", "")))
            for field in fields
            if isinstance(field, dict)
        )
    return (
        len(str(embed.get("title", "")))
        + len(str(embed.get("description", "")))
        + len(str(footer_text))
        + field_text
    )


def _fit_discord_embed(embed: dict[str, Any]) -> dict[str, Any]:
    """Enforce Discord component limits and the shared 6,000-character budget."""

    embed["title"] = _truncate_text(embed.get("title", ""), DISCORD_EMBED_TITLE_LIMIT)
    embed["description"] = _truncate_text(
        embed.get("description", ""), DISCORD_EMBED_DESCRIPTION_LIMIT
    )
    footer = embed.get("footer")
    if isinstance(footer, dict):
        footer["text"] = _truncate_text(footer.get("text", ""), DISCORD_EMBED_FOOTER_LIMIT)

    raw_fields = embed.get("fields")
    fields: list[dict[str, Any]] = []
    if isinstance(raw_fields, list):
        for raw_field in raw_fields[:DISCORD_EMBED_FIELDS_LIMIT]:
            if not isinstance(raw_field, dict):
                continue
            fields.append(
                {
                    **raw_field,
                    "name": _truncate_text(
                        raw_field.get("name", ""), DISCORD_EMBED_FIELD_NAME_LIMIT
                    ),
                    "value": _truncate_text(
                        raw_field.get("value", ""), DISCORD_EMBED_FIELD_VALUE_LIMIT
                    ),
                }
            )
    embed["fields"] = fields

    low_priority = {
        "Evidence": 0,
        "Gate status": 1,
        "Market regime": 2,
        "Time": 3,
        "Regime provenance": 4,
        "Paper timing": 5,
        "Exit model": 6,
    }
    value_order = sorted(
        range(len(fields)),
        key=lambda index: (low_priority.get(str(fields[index]["name"]), 100), -index),
    )
    for index in value_order:
        excess = _embed_text_length(embed) - DISCORD_EMBED_TOTAL_LIMIT
        if excess <= 0:
            break
        current = str(fields[index]["value"])
        fields[index]["value"] = _truncate_text(current, max(1, len(current) - excess))

    for key in ("description", "title"):
        excess = _embed_text_length(embed) - DISCORD_EMBED_TOTAL_LIMIT
        if excess <= 0:
            break
        current = str(embed.get(key, ""))
        embed[key] = _truncate_text(current, max(1, len(current) - excess))

    if isinstance(footer, dict):
        excess = _embed_text_length(embed) - DISCORD_EMBED_TOTAL_LIMIT
        if excess > 0:
            current = str(footer.get("text", ""))
            footer["text"] = _truncate_text(current, max(1, len(current) - excess))

    for field in reversed(fields):
        excess = _embed_text_length(embed) - DISCORD_EMBED_TOTAL_LIMIT
        if excess <= 0:
            break
        current = str(field["name"])
        field["name"] = _truncate_text(current, max(1, len(current) - excess))

    if _embed_text_length(embed) > DISCORD_EMBED_TOTAL_LIMIT:  # pragma: no cover
        raise ValueError("Discord embed could not be reduced to the platform budget")
    return embed


def _times(timestamp_ms: int) -> str:
    utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    kst = utc.astimezone(KST)
    return f"UTC {utc:%Y-%m-%d %H:%M:%S} · KST {kst:%Y-%m-%d %H:%M:%S}"


def _color(d: SignalDecision) -> int:
    if d.stage is SignalStage.INVALIDATED:
        return 0x808080
    if d.family is SignalFamily.TOP_GAINER:
        return 0x2ECC71
    if d.family is SignalFamily.BOLLINGER_UPPER_TOUCH:
        return 0xF1C40F
    if d.family is SignalFamily.BOLLINGER_LOWER_TOUCH:
        return 0xE74C3C
    if d.metadata.get("informational_only") is True:
        return 0x3498DB
    if d.family is SignalFamily.TECHNICAL_EXIT:
        return 0xF1C40F
    if d.family in {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK}:
        return 0xF39C12
    if d.stage is not SignalStage.CONFIRMED:
        return 0x3498DB
    return 0x2ECC71 if d.direction in {Direction.LONG, Direction.RISK_UP} else 0xE74C3C


def _recommendation_summary(d: SignalDecision) -> str:
    """Translate the existing decision state into a direct user-facing recommendation.

    This is presentation-only. It does not create a new score, probability,
    gate, or trading rule. Only a CONFIRMED directional decision is rendered
    as an entry candidate. WATCH/SETUP/INVALIDATED, research-only, and rapid
    risk events remain explicit no-entry recommendations.
    """

    if d.family is SignalFamily.TOP_GAINER:
        rank = d.metadata.get("gainer_rank", "?")
        trend = d.metadata.get("trend_label", "추세 판정 없음")
        return f"🚀 Binance 24h 급상승 #{rank} · {trend}"
    if d.family is SignalFamily.BOLLINGER_UPPER_TOUCH:
        trend = d.metadata.get("trend_label", "추세 판정 없음")
        return f"📈 Bollinger 상단 터치 · {trend}"
    if d.family is SignalFamily.BOLLINGER_LOWER_TOUCH:
        trend = d.metadata.get("trend_label", "추세 판정 없음")
        return f"📉 Bollinger 하단 터치 · {trend}"
    if d.family in {SignalFamily.WATCH_TREND_UP, SignalFamily.WATCH_TREND_DOWN}:
        trend = d.metadata.get("trend_label", "추세 판정 없음")
        return f"🔎 추세 관찰 · {trend}"
    if d.family in {SignalFamily.RANGE_LONG, SignalFamily.RANGE_SHORT}:
        side = "LONG 매수" if d.family is SignalFamily.RANGE_LONG else "SHORT 매도"
        return f"🔎 박스권 {side} 후보 · 검증 전 관찰 신호"
    if d.family is SignalFamily.TECHNICAL_EXIT:
        if d.market is Market.SPOT:
            return "🟠 추천: 보유 포지션 정리 검토 · 신규 매수 보류"
        side = "LONG" if d.direction is Direction.LONG else "SHORT"
        return f"🟠 추천: 기존 {side} 정리 검토 · 신규 진입 보류"
    if d.family is SignalFamily.PUMP_RISK:
        return "⚠️ 추천: 진입 보류 · 단기 급등 위험"
    if d.family is SignalFamily.CRASH_RISK:
        return "⚠️ 추천: 진입 보류 · 단기 급락 위험"
    if d.stage is SignalStage.INVALIDATED:
        return "⏸️ 추천: 진입 보류 · 직전 진입 조건 무효화"
    if d.metadata.get("informational_only") is True:
        bias = "상승 조건 관찰 중" if d.direction is Direction.LONG else "하락 조건 관찰 중"
        return f"⏸️ 추천: 진입 보류 · {bias}"
    if d.stage is not SignalStage.CONFIRMED:
        bias = "상승 조건 형성 중" if d.direction is Direction.LONG else "하락 조건 형성 중"
        return f"⏸️ 추천: 진입 보류 · {bias}"
    if d.direction is Direction.LONG:
        action = "매수 후보" if d.market is Market.SPOT else "LONG 후보"
        return f"🟢 추천: 상승 예상 · {action}"
    if d.direction is Direction.SHORT:
        action = "신규 매수 보류" if d.market is Market.SPOT else "SHORT 후보"
        return f"🔴 추천: 하락 예상 · {action}"
    return "⏸️ 추천: 진입 보류"


_FAMILY_LABELS = {
    SignalFamily.SQUEEZE_LONG: "상승 스퀴즈 대기",
    SignalFamily.SQUEEZE_SHORT: "하락 스퀴즈 대기",
    SignalFamily.BREAKOUT_LONG: "상방 돌파",
    SignalFamily.BREAKDOWN_SHORT: "하방 이탈",
    SignalFamily.PULLBACK_LONG: "상승 추세 눌림·재개",
    SignalFamily.PULLBACK_SHORT: "하락 추세 반등·재개",
    SignalFamily.EXHAUSTION_SHORT: "과열 소진",
    SignalFamily.CAPITULATION_LONG: "투매 반등",
}


def _market_watch_fields(d: SignalDecision) -> list[dict[str, object]]:
    if d.metadata.get("market_watch") is not True:
        return []
    rank = d.metadata.get("gainer_rank")
    change = d.metadata.get("price_change_percent_24h")
    quote_volume = d.metadata.get("quote_volume_24h_usdt")
    trend = d.metadata.get("trend_label", "unknown")
    trend_evidence = d.metadata.get("trend_evidence", ())
    if isinstance(trend_evidence, (list, tuple)):
        trend_lines = [str(item) for item in trend_evidence]
    else:
        trend_lines = [str(trend_evidence)]
    rendered_change = (
        _percent(float(change), signed=True) if isinstance(change, (int, float)) else "n/a"
    )
    rendered_volume = (
        _compact_number(float(quote_volume) / 1_000_000)
        if isinstance(quote_volume, (int, float))
        else "n/a"
    )
    ranking = (
        f"24h 상승 순위 #{rank} · 상승률 {rendered_change}\n"
        f"24h quote volume {rendered_volume}M USDT"
    )
    if d.metadata.get("retained_gainer") is True:
        peak = d.metadata.get("peak_quote_volume_usdt")
        expiry = d.metadata.get("watch_expires_at_ms")
        peak_text = (
            f"{float(peak) / 1_000_000_000:.2f}B" if isinstance(peak, (int, float)) else "n/a"
        )
        expiry_text = _times(expiry) if isinstance(expiry, int) else "n/a"
        ranking = (
            f"이전 급상승 종목 유지 추적 · 현재 상위 5개 밖\n"
            f"관측 최대 거래대금 {peak_text} USDT · 현재 {rendered_volume}M USDT\n"
            f"추적 만료: {expiry_text}"
        )
    bands = (
        "Bollinger(20, 2 stdev) "
        f"하단 {_compact_number(d.metadata.get('bollinger_lower'))} · "
        f"중심 {_compact_number(d.metadata.get('bollinger_middle'))} · "
        f"상단 {_compact_number(d.metadata.get('bollinger_upper'))}\n"
        f"봉 L/H/C {_compact_number(d.metadata.get('candle_low'))} / "
        f"{_compact_number(d.metadata.get('candle_high'))} / "
        f"{_compact_number(d.metadata.get('candle_close'))}"
    )
    analysis = "\n".join((f"판정: **{trend}**", *trend_lines))
    ma_context = _ma_context(d)
    if ma_context is not None:
        positions = {"above": "위", "below": "아래", "equal": "일치", "unavailable": "자료 없음"}
        analysis += (
            f"\n{ma_context['summary']}\n종가 위치: "
            f"EMA20 {positions[ma_context['price_vs_ema20']]} · "
            f"EMA50 {positions[ma_context['price_vs_ema50']]} · "
            f"BB 중심 SMA20 {positions[ma_context['price_vs_sma20']]}"
        )
        if ma_context.get("band_interpretation"):
            analysis += f"\n{ma_context['band_interpretation']}"
    fields: list[dict[str, object]] = [
        {"name": "급상승 우선순위", "value": ranking[:1024], "inline": False},
        {"name": "Bollinger 위치", "value": bands[:1024], "inline": False},
        {"name": "폐봉 기술 추세", "value": analysis[:1024], "inline": False},
    ]
    ratio = d.metadata.get("taker_buy_ratio")
    pressure = "데이터 없음"
    if isinstance(ratio, (int, float)):
        label = "매수 체결 우세" if ratio >= 0.55 else "매도 체결 우세" if ratio <= 0.45 else "균형"
        pressure = f"{d.metadata.get('flow_interval')} 폐봉 주도 매수 {ratio:.1%} · {label}"
    fields.append({
        "name": "매수·매도 압력", "value": pressure + "\n체결 방향 기준 (호가 잔량 아님)",
        "inline": False,
    })
    simultaneous = d.metadata.get("concurrent_watch_events")
    if isinstance(simultaneous, (list, tuple)):
        labels = {
            "top_gainer": "급상승 상위 5개",
            "bollinger_upper_touch": "볼린저 상단 접촉",
            "bollinger_lower_touch": "볼린저 하단 접촉",
            "watch_trend_up": "상승 추세", "watch_trend_down": "하락 추세",
            "range_long": "박스권 LONG 매수 후보", "range_short": "박스권 SHORT 매도 후보",
        }
        fields.append({
            "name": "같은 폐봉에서 관측한 사건",
            "value": " · ".join(labels.get(str(item), str(item)) for item in simultaneous)[:1024],
            "inline": False,
        })
    if d.family in {SignalFamily.RANGE_LONG, SignalFamily.RANGE_SHORT}:
        fields.append({
            "name": "박스권 후보 기준",
            "value": (
                f"범위 {d.metadata.get('range_low')} ~ {d.metadata.get('range_high')}\n"
                f"중간 목표 {d.metadata.get('range_target')}\n"
                f"추정 왕복 비용 {d.metadata.get('range_round_trip_cost_bps')}bps · 검증 전 후보"
            ), "inline": False,
        })
    return fields


def _compact_number(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    if abs(value) < 1e-12:
        return "0"
    magnitude = abs(value)
    if magnitude >= 1_000:
        rendered = f"{value:,.2f}"
    elif magnitude >= 1:
        rendered = f"{value:.4f}"
    elif magnitude >= 0.01:
        rendered = f"{value:.6f}"
    else:
        rendered = f"{value:.8f}"
    rendered = rendered.rstrip("0").rstrip(".")
    if signed and value > 0:
        return f"+{rendered}"
    return rendered


def _percent(value: float | None, *, signed: bool = False, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.{digits}f}%"


def _percent_distance(value: float, reference: float) -> float | None:
    if abs(reference) < 1e-12:
        return None
    return (value / reference - 1) * 100


def _setup_line(label: str, setup: DirectionalSetupScore) -> str:
    if setup.raw_score == 0:
        return f"**{label} 0/100** · 충족 규칙 없음"
    family = _FAMILY_LABELS.get(setup.family, setup.family.value)
    state = "트리거 충족" if setup.triggered else "트리거 대기"
    gate = "게이트 통과" if setup.eligible else "게이트 보류"
    adjusted = (
        ""
        if setup.decision_score == setup.raw_score
        else f" · 게이트 반영 {setup.decision_score}/100"
    )
    return f"**{label} {setup.raw_score}/100** · {family} · {state} · {gate}{adjusted}"


def _directional_fields(d: SignalDecision) -> list[dict[str, object]]:
    if d.metadata.get("market_watch") is True:
        return []
    raw = d.metadata.get(DIRECTIONAL_DIAGNOSTICS_METADATA_KEY)
    if raw is None:
        return []
    diagnostics = DirectionalDiagnostics.model_validate(raw)
    feature = diagnostics.feature
    if (
        feature.market is not d.market
        or feature.symbol != d.symbol
        or feature.interval != d.timeframe
        or feature.event_time_ms != d.event_time_ms
    ):
        raise ValueError("directional diagnostics do not match the signal decision")
    if Decimal(str(round(feature.price, 12))) != d.price:
        raise ValueError("directional diagnostics price does not match the signal decision")

    if d.market is Market.SPOT:
        long_label = "매수·상승 근거"
        short_label = "하락·신규매수 보류 근거"
    else:
        long_label = "LONG 근거"
        short_label = "SHORT 근거"
    score_value = "\n".join(
        (
            _setup_line(long_label, diagnostics.long),
            _setup_line(short_label, diagnostics.short),
            "각 방향의 대표 규칙 강도이며 진입 여부는 추천 요약과 게이트 상태를 따릅니다.",
        )
    )

    price_vs_ema20 = _percent_distance(feature.price, feature.ema20)
    ema20_vs_ema50 = _percent_distance(feature.ema20, feature.ema50)
    macd_delta = _compact_number(
        feature.macd_histogram - feature.macd_histogram_previous,
        signed=True,
    )
    trend_value = "\n".join(
        (
            (
                f"가격 {_compact_number(feature.price)} · EMA20 대비 "
                f"{_percent(price_vs_ema20, signed=True)}"
            ),
            (
                "EMA9/20/50/200 "
                f"{_compact_number(feature.ema9)} / {_compact_number(feature.ema20)} / "
                f"{_compact_number(feature.ema50)} / {_compact_number(feature.ema200)}"
            ),
            (
                f"EMA20-EMA50 {_percent(ema20_vs_ema50, signed=True)} · "
                f"EMA20 기울기 {_compact_number(feature.ema20_slope_atr, signed=True)} ATR/3봉"
            ),
            (
                f"RSI14 {feature.rsi:.1f} (Δ {feature.rsi - feature.rsi_previous:+.1f}) · "
                f"ADX14 {feature.adx:.1f}"
            ),
            (f"MACD hist {_compact_number(feature.macd_histogram, signed=True)} (Δ {macd_delta})"),
            (
                f"직전 20봉 범위 {_compact_number(feature.recent_low)} - "
                f"{_compact_number(feature.recent_high)}"
            ),
        )
    )

    if feature.closed_kline_flow_available:
        flow = (
            f"Taker buy {feature.taker_buy_ratio:.1%} · "
            f"imbalance {_compact_number(feature.taker_imbalance, signed=True)} · "
            f"CVD {_compact_number(feature.cvd_pressure, signed=True)}"
        )
    else:
        flow = "Taker buy / imbalance / CVD n/a (폐봉 수급 데이터 없음)"
    spread_source = "proxy" if feature.spread_is_proxy else "observed BBO"
    book_age = "n/a" if feature.book_age_ms is None else f"{feature.book_age_ms}ms"
    risk_lines = [
        (
            f"ATR14 {_compact_number(feature.atr)} ({feature.atr_percent:.2f}%) · "
            f"BB폭 {feature.bollinger_width:.2%} (백분위 {feature.bollinger_width_percentile:.1f}%)"
        ),
        (
            f"상대거래량 {feature.relative_volume:.2f}x · 거래량 z {feature.volume_zscore:+.2f} · "
            f"체결수 z {feature.trade_count_zscore:+.2f}"
        ),
        flow,
        (
            f"Spread {_compact_number(feature.spread_bps)} bps ({spread_source}) · "
            f"호가 age {book_age}"
        ),
    ]
    if feature.funding_rate is not None or feature.funding_zscore is not None:
        funding = None if feature.funding_rate is None else feature.funding_rate * 100
        risk_lines.append(
            f"Funding {_percent(funding, signed=True, digits=4)} · "
            f"z {_compact_number(feature.funding_zscore, signed=True)}"
        )
    risk_lines.append(f"데이터 완전성 {feature.data_completeness:.0%} · 폐봉 기준")

    structure = feature.chart_structure
    pullback_depth = None if structure.pullback_depth is None else structure.pullback_depth * 100
    pullback_duration = (
        "n/a" if structure.pullback_duration_bars is None else str(structure.pullback_duration_bars)
    )
    support_distance = _compact_number(structure.price_minus_support_atr, signed=True)
    resistance_distance = _compact_number(structure.resistance_minus_price_atr, signed=True)
    swing_value = "\n".join(
        (
            (
                f"구조 {structure.state} · 확정 high/low "
                f"{structure.qualified_high_count}/{structure.qualified_low_count}"
            ),
            (
                "Swing H "
                f"{_compact_number(structure.previous_swing_high)} → "
                f"{_compact_number(structure.latest_swing_high)} "
                f"(Δ {_compact_number(structure.swing_high_change_atr, signed=True)} ATR)"
            ),
            (
                "Swing L "
                f"{_compact_number(structure.previous_swing_low)} → "
                f"{_compact_number(structure.latest_swing_low)} "
                f"(Δ {_compact_number(structure.swing_low_change_atr, signed=True)} ATR)"
            ),
            (
                "2-anchor 연구용 선: support "
                f"{_compact_number(structure.projected_support)} "
                f"(가격-선 {support_distance} ATR) · "
                f"resistance {_compact_number(structure.projected_resistance)} "
                f"(선-가격 {resistance_distance} ATR)"
            ),
            (
                f"눌림 {structure.pullback_direction}/{structure.pullback_status} · "
                f"impulse {_compact_number(structure.impulse_size_atr)} ATR · "
                f"depth {_percent(pullback_depth)} · duration {pullback_duration}봉"
            ),
            (
                f"EMA20 거리 {_compact_number(feature.ema20_distance_atr, signed=True)} ATR · "
                f"confluence {_compact_number(structure.confluence_distance_atr)} ATR · "
                f"회복종가 {'yes' if structure.recovery_confirmed else 'no'}"
            ),
            "피벗/선은 t-1 고정 · 2오른쪽봉 확정 · 임계값은 연구용 시드",
        )
    )

    return [
        {
            "name": "상승·하락 근거 강도",
            "value": score_value[:1024],
            "inline": False,
        },
        {
            "name": "추세·모멘텀 실제값",
            "value": trend_value[:1024],
            "inline": False,
        },
        {
            "name": "변동성·수급·체결 실제값",
            "value": "\n".join(risk_lines)[:1024],
            "inline": False,
        },
        {
            "name": "차트 구조·눌림목 실제값",
            "value": swing_value[:1024],
            "inline": False,
        },
    ]


def _finite_number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and isfinite(value) else None


def _ma_context(d: SignalDecision) -> dict[str, Any] | None:
    context = d.metadata.get("ma_context")
    if (
        isinstance(context, dict)
        and context.get("version") == "bb-ma-context-v1"
        and context.get("as_of_ms") == d.event_time_ms
        and context.get("timeframe") == d.timeframe
        and all(isinstance(context.get(key), str) for key in (
            "summary", "alignment", "band_setup", "band_interpretation", "band_check", "reference",
            "price_vs_ema20", "price_vs_ema50", "price_vs_sma20",
        ))
        and context.get("alignment") in ("up", "down", "mixed", "unavailable")
        and context.get("reference") in ("", "ema20", "ema50")
        and all(context.get(key) in ("above", "below", "equal", "unavailable") for key in (
            "price_vs_ema20", "price_vs_ema50", "price_vs_sma20",
        ))
    ):
        return context
    return None


def _simple_pressure(d: SignalDecision) -> str:
    pressure = d.metadata.get("minute_pressure")
    if isinstance(pressure, dict) and pressure.get("available") is True:
        ratio = _finite_number(pressure.get("buy_ratio"))
        change = _finite_number(pressure.get("price_change_percent"))
        last = _finite_number(pressure.get("last_buy_ratio"))
        start, end = pressure.get("start_ms"), pressure.get("end_ms")
        lag = pressure.get("lag_ms")
        if (
            ratio is not None
            and 0 <= ratio <= 1
            and change is not None
            and last is not None
            and 0 <= last <= 1
            and isinstance(start, int)
            and isinstance(end, int)
            and isinstance(lag, int)
            and pressure.get("market") == d.market.value
            and pressure.get("symbol") == d.symbol
            and pressure.get("as_of_ms") == d.event_time_ms
            and pressure.get("version") == "closed-minute-pressure-v1"
            and end - start + 1 == 180_000
            and start <= end <= d.event_time_ms
            and 0 <= lag <= 60_000
            and d.event_time_ms - end == lag
        ):
            start_time = datetime.fromtimestamp(start / 1000, tz=UTC).astimezone(KST)
            end_time = datetime.fromtimestamp(end / 1000, tz=UTC).astimezone(KST)
            delay = "" if lag == 0 else " · 1분 전까지의 자료"
            return (
                f"{pressure.get('summary', '체결 방향 확인 중')}\n"
                f"최근 3분 매수 {ratio:.0%} · 마지막 1분 {last:.0%}\n"
                f"같은 3분 가격 {change:+.2f}% · "
                f"KST {start_time:%H:%M}-{end_time:%H:%M}{delay}"
            )
    ratio = _finite_number(d.metadata.get("taker_buy_ratio"))
    if ratio is not None and 0 <= ratio <= 1:
        label = (
            "매수 체결 우세"
            if ratio >= 0.55
            else "매도 체결 우세"
            if ratio <= 0.45
            else "체결 균형"
        )
        prefix = "1분 자료 부족 · " if pressure is not None else ""
        interval = str(d.metadata.get("flow_interval", d.timeframe))
        interval = interval.replace("m", "분").replace("h", "시간")
        return f"{prefix}{interval}봉 기준 {label}\n매수 비중 {ratio:.0%} (체결 기준)"
    return "체결 자료 부족 — 매수·매도 우세를 판단할 수 없습니다."


def _readable_reason(reason: str) -> str:
    labels = {
        "closed above range": "봉이 범위 상단 위에서 마감",
        "closed below range": "봉이 범위 하단 아래에서 마감",
        "positive and improving MACD histogram": "상승 동력이 강화됨",
        "negative and weakening MACD histogram": "하락 동력이 강화됨",
        "EMA20 above EMA50": "단기 추세가 중기 추세보다 위에 있음",
        "EMA20 below EMA50": "단기 추세가 중기 추세보다 아래에 있음",
        "15m trend confirms direction": "15분봉 추세도 같은 방향",
        "1h price confirms direction": "1시간봉 가격도 같은 방향",
        "all independent entry gates passed": "진입 조건 검사를 통과",
    }
    text = labels.get(reason, reason)
    breakout = re.fullmatch(r"closed (above|below) (\d+)-bar (high|low)", reason)
    volume = re.fullmatch(r"relative volume ([\d.]+)x", reason)
    if breakout:
        boundary = "고점 위" if breakout[1] == "above" else "저점 아래"
        text = f"최근 {breakout[2]}개 봉의 {boundary}에서 마감"
    elif volume:
        text = f"거래량이 비교 기준의 {volume[1]}배"
    return _truncate_text(text, 160)


def _simple_payload(d: SignalDecision, username: str) -> dict[str, object]:
    watch = d.metadata.get("market_watch") is True
    paper = d.metadata.get("paper_only") is True
    recommendation = _recommendation_summary(d)
    fields: list[dict[str, object]] = []
    if watch:
        names = {
            SignalFamily.TOP_GAINER: "급상승 종목 관찰",
            SignalFamily.BOLLINGER_UPPER_TOUCH: "볼린저 상단 접촉",
            SignalFamily.BOLLINGER_LOWER_TOUCH: "볼린저 하단 접촉",
            SignalFamily.WATCH_TREND_UP: "상승 추세 관찰",
            SignalFamily.WATCH_TREND_DOWN: "하락 추세 관찰",
            SignalFamily.RANGE_LONG: "박스권 매수 후보",
            SignalFamily.RANGE_SHORT: "박스권 숏 후보",
        }
        recommendation = names.get(d.family, "시장 관찰")
        trend = str(d.metadata.get("trend_label", "추세 자료 부족"))
        change = _finite_number(d.metadata.get("price_change_percent_24h"))
        volume = _finite_number(d.metadata.get("quote_volume_24h_usdt"))
        rank = d.metadata.get("gainer_rank")
        member = (
            "이전 급등 종목 · 유지 추적"
            if d.metadata.get("retained_gainer")
            else f"상승률 {rank}위"
            if isinstance(rank, int)
            else "현재 급등 종목"
        )
        situation = f"{trend} · {member}\n기준 가격 {d.price} USDT"
        if change is not None:
            situation += f" · 24시간 {change:+.2f}%"
        if volume is not None:
            situation += f"\n24시간 거래대금 {volume / 100_000_000:.2f}억 USDT"
        ma_context = _ma_context(d)
        if ma_context is not None:
            situation += f"\n{ma_context['summary']}"
        fields.extend(
            [
                {"name": "시장 상황", "value": situation, "inline": False},
                {"name": "매수·매도 힘", "value": _simple_pressure(d), "inline": False},
            ]
        )
        condition = str(d.metadata.get("invalidation_condition", "조건 자료 부족"))
        if d.family in {SignalFamily.RANGE_LONG, SignalFamily.RANGE_SHORT}:
            target = _finite_number(d.metadata.get("range_target"))
            condition = (
                f"관찰 목표 {_compact_number(target)} USDT\n"
                f"후보 무효화 가격 {d.invalidation} USDT\n{condition}"
            )
            description = "범위 경계에서 되돌아온 **검증 전 후보**입니다."
        else:
            description = "**관찰 알림** · 진입 확정 신호가 아닙니다."
            if d.family in {SignalFamily.BOLLINGER_UPPER_TOUCH, SignalFamily.BOLLINGER_LOWER_TOUCH}:
                side = "상단" if d.family is SignalFamily.BOLLINGER_UPPER_TOUCH else "하단"
                band = _finite_number(
                    d.metadata.get(f"bollinger_{'upper' if side == '상단' else 'lower'}")
                )
                condition = f"접촉한 {side} {_compact_number(band)} USDT\n조건 해제: {condition}"
                description += " 밴드 접촉만으로 반전을 판단하지 않습니다."
            else:
                condition = f"조건 해제: {condition}"
        if ma_context is not None and ma_context.get("band_interpretation"):
            condition += f"\n{ma_context['band_interpretation']}"
            reference = ma_context.get("reference")
            line = _finite_number(d.metadata.get(reference)) if isinstance(reference, str) else None
            if line is not None:
                period = "20" if reference == "ema20" else "50"
                condition += f"\n추세 확인선 EMA{period} {_compact_number(line)} USDT"
            condition += f"\n다음 폐봉: {ma_context.get('band_check', '방향 재확인')}"
        fields.append({"name": "확인할 조건", "value": condition, "inline": False})
        simultaneous = d.metadata.get("concurrent_watch_events")
        if isinstance(simultaneous, (list, tuple)):
            bands = {
                "bollinger_upper_touch": "볼린저 상단 접촉",
                "bollinger_lower_touch": "볼린저 하단 접촉",
            }
            other_bands = [
                label for family, label in bands.items()
                if family in simultaneous and family != d.family.value
            ]
            if other_bands:
                description += " 함께 감지: " + " · ".join(other_bands)
        expiry = d.metadata.get("watch_expires_at_ms")
        if d.metadata.get("retained_gainer") is True and isinstance(expiry, int):
            fields.append({"name": "추적 기한", "value": _times(expiry), "inline": False})
    else:
        description = (
            "모의 포지션 알림 · 실제 거래소 주문 없음"
            if paper
            else "규칙 조건에 따른 후보·위험 알림"
        )
        if not paper and d.metadata.get("informational_only") is True:
            description = "검증 전 관찰 신호 · 진입 보류"
        family = _FAMILY_LABELS.get(d.family, "시장 위험")
        reasons = f"{family} 조건을 관찰했습니다."
        if d.family is SignalFamily.TECHNICAL_EXIT:
            reasons = (
                "모의 포지션의 종료 조건이 충족됐습니다."
                if paper
                else "기존 포지션의 종료 조건을 관찰했습니다."
            )
            if paper:
                side = "매수" if d.direction is Direction.LONG else "숏"
                recommendation = f"모의 {side} 포지션 종료"
        elif d.family in {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK}:
            reasons = "짧은 구간의 가격 변동이 급격합니다. 추격 진입에 주의하세요."
        elif d.stage is SignalStage.INVALIDATED:
            reasons = "직전 후보의 조건이 유지되지 않습니다."
        elif d.gate is not None:
            reasons += (
                " 조건 검사를 통과했습니다." if d.gate.passed else " 조건 검사에서 보류됐습니다."
            )
        if d.reasons:
            reasons = "\n".join(_readable_reason(reason) for reason in d.reasons[:2])
        fields = [
            {"name": "가격과 기준", "value": f"{d.price} USDT · {d.timeframe}", "inline": False},
            {"name": "알림 이유", "value": reasons, "inline": False},
            {
                "name": "조건 무효화",
                "value": str(
                    d.invalidation or d.metadata.get("invalidation_condition", "자료 없음")
                ),
                "inline": False,
            },
        ]
        if paper:
            exit_labels = {
                "initial_stop": "처음 정한 손절 조건에 도달",
                "trailing_stop": "추적 손절 조건에 도달",
                "opposite_signal": "반대 방향 신호 감지",
                "trend_failure": "추세 유지 조건이 깨짐",
                "time_exit": "최대 관찰 기간 종료",
                "split_boundary": "모의 분석 구간 종료",
                "data_gap": "봉 누락으로 추적 중단",
                "end_of_data": "분석 자료 구간 종료",
            }
            fields[2] = {
                "name": "모의 종료 이유",
                "value": exit_labels.get(str(d.metadata.get("exit_reason")), "종료 조건 관찰"),
                "inline": False,
            }
            fill = d.metadata.get("fill_time_ms")
            if isinstance(fill, int):
                fields.append({"name": "모의 체결 시각", "value": _times(fill), "inline": False})
    basis = (
        "실시간 위험 관찰"
        if d.family in {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK}
        else f"{d.timeframe} 폐봉"
    )
    embed: dict[str, Any] = {
        "title": f"{d.symbol} · {recommendation}",
        "description": description,
        "color": _color(d),
        "fields": fields,
        "footer": {
            "text": (
                f"{basis} · {_times(d.event_time_ms)}\n"
                f"event {d.event_id} · rules {d.rule_version}"
            )
        },
        "timestamp": datetime.fromtimestamp(d.event_time_ms / 1000, tz=UTC).isoformat(),
    }
    return {
        "username": _truncate_text(
            username.strip() or "Binance Signal Bot", DISCORD_USERNAME_LIMIT
        ),
        "allowed_mentions": {"parse": []},
        "embeds": [_fit_discord_embed(embed)],
    }


def build_discord_payload(
    d: SignalDecision, username: str, *, message_style: Literal["simple", "detailed"] = "detailed",
) -> dict[str, object]:
    d = SignalDecision.model_validate(d.model_dump(mode="python", warnings="none"))
    if message_style not in {"simple", "detailed"}:
        raise ValueError("unknown Discord message style")
    if d.metadata.get("informational_only") is True and d.stage is SignalStage.CONFIRMED:
        raise ValueError("informational-only decisions cannot be CONFIRMED")
    if message_style == "simple":
        return _simple_payload(d, username)
    reasons = "\n".join(f"• {reason}" for reason in d.reasons[:10]) or "• No details"
    invalidation = (
        str(d.invalidation)
        if d.invalidation is not None
        else str(d.metadata.get("invalidation_condition", "n/a"))
    )
    fields = [
        {"name": "Price", "value": str(d.price), "inline": True},
        {"name": "Timeframe", "value": d.timeframe, "inline": True},
        {"name": "Invalidation", "value": invalidation, "inline": True},
        *_market_watch_fields(d),
        *_directional_fields(d),
        {"name": "Evidence", "value": reasons[:1024], "inline": False},
        {
            "name": "Market regime",
            "value": (
                f"{d.regime.label}; BTC {d.regime.btc_trend}; breadth {d.regime.breadth_ratio:.0%}"
            ),
            "inline": False,
        },
        {"name": "Time", "value": _times(d.event_time_ms), "inline": False},
    ]
    paper_exit = d.family is SignalFamily.TECHNICAL_EXIT and d.metadata.get("paper_only") is True
    informational_only = d.metadata.get("informational_only") is True
    if informational_only and d.stage is SignalStage.CONFIRMED:
        raise ValueError("informational-only decisions cannot be CONFIRMED")
    recommendation = _recommendation_summary(d)
    status_line = (
        "PAPER 포지션 종료 추적"
        if paper_exit
        else f"상태: {d.stage.value.upper()} · 근거 강도: {d.score}/100"
    )
    if d.metadata.get("market_watch") is True:
        status_line = "폐봉 조건 관찰 · 수익률·정확도 검증 전"
    if paper_exit:
        fill_time_ms = d.metadata.get("fill_time_ms")
        observed_at_ms = d.metadata.get("observed_at_closed_candle_ms")
        regime_observed_at_ms = d.metadata.get("regime_observed_at_ms")
        regime_context_source = d.metadata.get("regime_context_source")
        timing = "unavailable"
        if isinstance(fill_time_ms, int) and isinstance(observed_at_ms, int):
            timing = (
                f"Paper fill: {_times(fill_time_ms)}\n"
                f"Alert observation (closed candle): {_times(observed_at_ms)}"
            )
        regime_timing = "unavailable"
        if isinstance(regime_observed_at_ms, int) and isinstance(regime_context_source, str):
            regime_timing = f"{regime_context_source}: {_times(regime_observed_at_ms)}"
        fields.insert(
            0,
            {
                "name": "Execution scope",
                "value": (
                    "PAPER alert only — no exchange order was placed. "
                    "Position state is in memory and is not restored after restart."
                ),
                "inline": False,
            },
        )
        fields.insert(
            1,
            {
                "name": "Exit model",
                "value": (
                    f"{d.metadata.get('exit_reason', 'unknown')} · "
                    f"{d.metadata.get('execution_model', 'unknown')}\n"
                    f"Entry event: {d.metadata.get('entry_event_id', 'unknown')}"
                )[:1024],
                "inline": False,
            },
        )
        fields.insert(
            2,
            {
                "name": "Paper timing",
                "value": timing[:1024],
                "inline": False,
            },
        )
        fields.insert(
            3,
            {
                "name": "Regime provenance",
                "value": regime_timing[:1024],
                "inline": False,
            },
        )
    if d.gate is not None:
        gate = d.gate
        fields.insert(
            3,
            {
                "name": "Independent gates",
                "value": (
                    f"Trend {gate.trend_score} | Participation {gate.participation_score} | "
                    f"Crowding risk {gate.crowding_risk_score} | "
                    f"Execution {gate.execution_score} | Completeness {gate.completeness_score}"
                )[:1024],
                "inline": False,
            },
        )
        quality = [*(f"FAIL: {item}" for item in gate.failures)]
        if gate.proxy_fields:
            quality.append(f"Proxy fields: {', '.join(gate.proxy_fields)}")
        fields.insert(
            4,
            {
                "name": "Gate status",
                "value": ("PASS" if gate.passed else "\n".join(quality))[:1024],
                "inline": False,
            },
        )
    embed: dict[str, Any] = {
        "title": f"{d.symbol} · {recommendation}",
        "description": f"**{status_line}**",
        "color": _color(d),
        "fields": fields,
        "footer": {"text": f"event {d.event_id} · rules {d.rule_version}"},
        "timestamp": datetime.fromtimestamp(d.event_time_ms / 1000, tz=UTC).isoformat(),
    }
    return {
        "username": _truncate_text(
            username.strip() or "Binance Signal Bot", DISCORD_USERNAME_LIMIT
        ),
        "allowed_mentions": {"parse": []},
        "embeds": [_fit_discord_embed(embed)],
    }
