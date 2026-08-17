from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
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
        footer["text"] = _truncate_text(
            footer.get("text", ""), DISCORD_EMBED_FOOTER_LIMIT
        )

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
    if d.metadata.get("informational_only") is True:
        return 0x3498DB
    if d.family is SignalFamily.TECHNICAL_EXIT:
        return 0xF1C40F
    return 0x2ECC71 if d.direction in {Direction.LONG, Direction.RISK_UP} else 0xE74C3C


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
    return (
        f"**{label} {setup.raw_score}/100** · {family} · {state} · {gate}{adjusted}"
    )


def _directional_fields(d: SignalDecision) -> list[dict[str, object]]:
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
            "서로 다른 규칙 점수의 차이는 방향 우세량으로 계산하지 않습니다.",
            "방향별 규칙 중 대표 원점수이며 합산값이나 확률이 아닙니다.",
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
            (
                f"MACD hist {_compact_number(feature.macd_histogram, signed=True)} "
                f"(Δ {macd_delta})"
            ),
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
        funding = (
            None if feature.funding_rate is None else feature.funding_rate * 100
        )
        risk_lines.append(
            f"Funding {_percent(funding, signed=True, digits=4)} · "
            f"z {_compact_number(feature.funding_zscore, signed=True)}"
        )
    risk_lines.append(f"데이터 완전성 {feature.data_completeness:.0%} · 폐봉 기준")

    structure = feature.chart_structure
    pullback_depth = (
        None if structure.pullback_depth is None else structure.pullback_depth * 100
    )
    pullback_duration = (
        "n/a"
        if structure.pullback_duration_bars is None
        else str(structure.pullback_duration_bars)
    )
    support_distance = _compact_number(
        structure.price_minus_support_atr, signed=True
    )
    resistance_distance = _compact_number(
        structure.resistance_minus_price_atr, signed=True
    )
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
            "name": "방향별 최고 설정 점수 · 확률 아님",
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


def build_discord_payload(d: SignalDecision, username: str) -> dict[str, object]:
    d = SignalDecision.model_validate(d.model_dump(mode="python", warnings="none"))
    reasons = "\n".join(f"• {reason}" for reason in d.reasons[:10]) or "• No details"
    invalidation = str(d.invalidation) if d.invalidation is not None else "n/a"
    fields = [
        {"name": "Price", "value": str(d.price), "inline": True},
        {"name": "Timeframe", "value": d.timeframe, "inline": True},
        {"name": "Invalidation", "value": invalidation, "inline": True},
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
    paper_exit = (
        d.family is SignalFamily.TECHNICAL_EXIT
        and d.metadata.get("paper_only") is True
    )
    informational_only = d.metadata.get("informational_only") is True
    if informational_only and d.stage is SignalStage.CONFIRMED:
        raise ValueError("informational-only decisions cannot be CONFIRMED")
    headline = (
        f"INFORMATION_ONLY · {d.stage.value.upper()}"
        if informational_only
        else f"{d.action_label} · {d.stage.value.upper()}"
    )
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
        if isinstance(regime_observed_at_ms, int) and isinstance(
            regime_context_source, str
        ):
            regime_timing = (
                f"{regime_context_source}: {_times(regime_observed_at_ms)}"
            )
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
        "title": f"{d.symbol} · {d.family.value}",
        "description": (
            f"**{headline}**\n"
            + (
                "**PAPER ONLY · NO ORDER PLACED**"
                if paper_exit
                else (
                    f"정보용 SETUP · 진입 승인 아님\nRule strength: **{d.score}/100** "
                    "(not a probability)"
                    if informational_only
                    else f"Rule strength: **{d.score}/100** (not a probability)"
                )
            )
        ),
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
