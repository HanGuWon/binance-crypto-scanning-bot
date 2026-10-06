from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from conftest import make_decision
from signalbot.alerts.embeds import (
    DEFAULT_VALIDATION_NOTICE,
    DISCORD_EMBED_TOTAL_LIMIT,
    PRESENTATION_VERSION,
    VALIDATION_FIELD_NAME,
    build_discord_payload,
)
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings, Settings, SignalSettings
from signalbot.data.anomaly import ROBUST_Z_DISPLAY_CAP, AnomalyDetector
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import (
    GateEvaluation,
    MarketRegime,
    MiniTicker,
    RuleEvaluation,
)
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime

ALLOWED_DIRECTIONS: dict[SignalFamily, tuple[Direction, ...]] = {
    SignalFamily.SQUEEZE_LONG: (Direction.LONG,),
    SignalFamily.SQUEEZE_SHORT: (Direction.SHORT,),
    SignalFamily.BREAKOUT_LONG: (Direction.LONG,),
    SignalFamily.BREAKDOWN_SHORT: (Direction.SHORT,),
    SignalFamily.PULLBACK_LONG: (Direction.LONG,),
    SignalFamily.PULLBACK_SHORT: (Direction.SHORT,),
    SignalFamily.EXHAUSTION_SHORT: (Direction.SHORT,),
    SignalFamily.CAPITULATION_LONG: (Direction.LONG,),
    SignalFamily.PUMP_RISK: (Direction.RISK_UP,),
    SignalFamily.CRASH_RISK: (Direction.RISK_DOWN,),
    SignalFamily.TECHNICAL_EXIT: (Direction.LONG, Direction.SHORT),
}


def _reachable_decisions() -> list[Any]:
    decisions = []
    for family, directions in ALLOWED_DIRECTIONS.items():
        for direction in directions:
            for market in (Market.SPOT, Market.FUTURES):
                for stage in SignalStage:
                    if stage is SignalStage.IDLE:
                        continue
                    metadata: dict[str, object] = {}
                    if family is SignalFamily.TECHNICAL_EXIT:
                        metadata = {"paper_only": True}
                    try:
                        decisions.append(
                            make_decision(
                                market=market,
                                family=family,
                                direction=direction,
                                stage=stage,
                                metadata=metadata,
                            )
                        )
                    except ValidationError:
                        continue  # combination is not constructible, hence not reachable
    return decisions


def _embed(decision: Any, **kwargs: Any) -> dict[str, Any]:
    payload = build_discord_payload(decision, "Test Bot", **kwargs)
    return payload["embeds"][0]  # type: ignore[index, return-value]


def _field(embed: dict[str, Any], name: str) -> str:
    return next(f["value"] for f in embed["fields"] if f["name"] == name)


CONFIRMED_TITLES = [
    (Market.SPOT, Direction.LONG, SignalFamily.BREAKOUT_LONG,
     "[SPOT] BTCUSDT · 🟢 규칙 트리거 · 매수 검토 후보 (미검증 규칙)"),
    (Market.FUTURES, Direction.LONG, SignalFamily.BREAKOUT_LONG,
     "[USDⓈ-M] BTCUSDT · 🟢 규칙 트리거 · LONG 검토 후보 (미검증 규칙)"),
    (Market.FUTURES, Direction.SHORT, SignalFamily.BREAKDOWN_SHORT,
     "[USDⓈ-M] BTCUSDT · 🔴 규칙 트리거 · SHORT 검토 후보 (미검증 규칙)"),
    (Market.SPOT, Direction.SHORT, SignalFamily.BREAKDOWN_SHORT,
     "[SPOT] BTCUSDT · 🔴 규칙 트리거 · 신규 매수 보류 (미검증 규칙)"),
]  # fmt: skip


@pytest.mark.parametrize(("market", "direction", "family", "title"), CONFIRMED_TITLES)
def test_confirmed_titles_golden(
    market: Market, direction: Direction, family: SignalFamily, title: str
) -> None:
    decision = make_decision(market=market, direction=direction, family=family)
    assert _embed(decision)["title"] == title


GOLDEN_OTHER = [
    (dict(stage=SignalStage.SETUP), "⏸️ 추천: 진입 보류 · 상승 조건 형성 중"),
    (dict(stage=SignalStage.WATCH), "⏸️ 추천: 진입 보류 · 상승 조건 형성 중"),
    (dict(stage=SignalStage.INVALIDATED), "⏸️ 추천: 진입 보류 · 직전 진입 조건 무효화"),
    (dict(stage=SignalStage.WATCH, direction=Direction.SHORT, family=SignalFamily.PULLBACK_SHORT),
     "⏸️ 추천: 진입 보류 · 하락 조건 형성 중"),
    (dict(stage=SignalStage.WATCH, family=SignalFamily.PUMP_RISK, direction=Direction.RISK_UP),
     "⚠️ 추천: 진입 보류 · 단기 급등 위험"),
    (dict(stage=SignalStage.WATCH, family=SignalFamily.CRASH_RISK, direction=Direction.RISK_DOWN),
     "⚠️ 추천: 진입 보류 · 단기 급락 위험"),
    (dict(stage=SignalStage.SETUP, metadata={"informational_only": True}),
     "⏸️ 추천: 진입 보류 · 상승 조건 관찰 중"),
]  # fmt: skip


@pytest.mark.parametrize(("updates", "suffix"), GOLDEN_OTHER)
def test_non_confirmed_titles_golden(updates: dict[str, Any], suffix: str) -> None:
    decision = make_decision(market=Market.FUTURES, **updates)
    assert _embed(decision)["title"] == f"[USDⓈ-M] BTCUSDT · {suffix}"


def test_technical_exit_titles_golden() -> None:
    spot = make_decision(
        market=Market.SPOT, family=SignalFamily.TECHNICAL_EXIT, direction=Direction.LONG,
        metadata={"paper_only": True},
    )  # fmt: skip
    assert _embed(spot)["title"] == (
        "[SPOT] BTCUSDT · 🟠 추천: 보유 포지션 정리 검토 · 신규 매수 보류"
    )
    futures = make_decision(
        market=Market.FUTURES, family=SignalFamily.TECHNICAL_EXIT, direction=Direction.SHORT,
        metadata={"paper_only": True},
    )  # fmt: skip
    assert _embed(futures)["title"] == (
        "[USDⓈ-M] BTCUSDT · 🟠 추천: 기존 SHORT 정리 검토 · 신규 진입 보류"
    )


def test_every_reachable_family_market_stage_combination_is_clean() -> None:
    decisions = _reachable_decisions()
    assert len(decisions) >= 60  # sanity: the matrix really was enumerated
    for decision in decisions:
        embed = _embed(decision)
        label = f"{decision.family}/{decision.market}/{decision.stage}"
        assert "예상" not in embed["title"], label
        assert embed["title"].startswith(
            "[SPOT] " if decision.market is Market.SPOT else "[USDⓈ-M] "
        ), label
        assert _field(embed, VALIDATION_FIELD_NAME) == DEFAULT_VALIDATION_NOTICE, label
        assert embed["footer"]["text"].endswith(f"view v{PRESENTATION_VERSION}"), label


def test_no_title_or_description_anywhere_contains_the_prediction_word() -> None:
    for decision in _reachable_decisions():
        embed = _embed(decision)
        assert "예상" not in embed["title"]
        assert "예상" not in embed["description"]


def test_spot_and_futures_payloads_are_distinguishable_by_title_not_only_footer() -> None:
    kwargs: dict[str, Any] = dict(
        family=SignalFamily.PUMP_RISK, direction=Direction.RISK_UP, stage=SignalStage.WATCH
    )
    spot = _embed(make_decision(market=Market.SPOT, **kwargs))
    futures = _embed(make_decision(market=Market.FUTURES, **kwargs))
    assert spot["title"] != futures["title"]


def test_validation_notice_default_custom_and_blank_fallback() -> None:
    decision = make_decision()
    assert _field(_embed(decision), VALIDATION_FIELD_NAME) == DEFAULT_VALIDATION_NOTICE
    assert _field(_embed(decision, validation_notice="  커스텀 고지 "), VALIDATION_FIELD_NAME) == (
        "커스텀 고지"
    )
    assert _field(_embed(decision, validation_notice="   "), VALIDATION_FIELD_NAME) == (
        DEFAULT_VALIDATION_NOTICE
    )
    assert "기대수익·확률 아님" in DEFAULT_VALIDATION_NOTICE


def test_validation_notice_setting_default_bounds_and_dump_exclusion() -> None:
    settings = AlertSettings()
    assert settings.validation_notice == DEFAULT_VALIDATION_NOTICE
    assert "validation_notice" not in settings.model_dump()
    assert AlertSettings(validation_notice="x" * 300).validation_notice == "x" * 300
    with pytest.raises(ValidationError):
        AlertSettings(validation_notice="x" * 301)
    with pytest.raises(ValidationError):
        AlertSettings(validation_notice="   ")


def _gate() -> GateEvaluation:
    return GateEvaluation(
        trend_score=100,
        participation_score=100,
        crowding_risk_score=0,
        execution_score=75,
        completeness_score=100,
        passed=True,
    )


def test_gate_field_renders_na_for_unevaluated_gates_under_r2() -> None:
    decision = make_decision(
        gate=_gate(),
        metadata={
            "entry_policy": "r2_pit_htf_exec",
            "unevaluated_gates": ["participation", "crowding"],
        },
    )
    value = _field(_embed(decision), "Independent gates")
    assert "Participation N/A(정책 미사용)" in value
    assert "Crowding risk N/A(정책 미사용)" in value
    assert "Trend 100" in value and "Execution 75" in value and "Completeness 100" in value


def test_gate_field_falls_back_to_entry_policy_when_list_missing() -> None:
    decision = make_decision(gate=_gate(), metadata={"entry_policy": "r2_pit_htf_exec"})
    value = _field(_embed(decision), "Independent gates")
    assert value.count("N/A(정책 미사용)") == 2


def test_gate_field_is_numeric_under_legacy_gates_and_without_metadata() -> None:
    legacy = make_decision(
        gate=_gate(), metadata={"entry_policy": "legacy_gates", "unevaluated_gates": []}
    )
    value = _field(_embed(legacy), "Independent gates")
    assert "Participation 100" in value and "Crowding risk 0" in value
    assert "N/A" not in value
    unrecorded = _field(_embed(make_decision(gate=_gate())), "Independent gates")
    assert "N/A" not in unrecorded


def test_legacy_policy_with_disabled_participation_gate_shows_only_that_one_as_na() -> None:
    decision = make_decision(gate=_gate(), metadata={"unevaluated_gates": ["participation"]})
    value = _field(_embed(decision), "Independent gates")
    assert "Participation N/A(정책 미사용)" in value
    assert "Crowding risk 0" in value


def _runtime(signals: dict[str, Any]) -> MarketRuntime:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    settings = Settings.model_validate({"signals": signals})
    return MarketRuntime(
        Market.FUTURES, settings, repo, ReplayClock(1_000), lambda decision: asyncio.sleep(0)
    )


@pytest.mark.parametrize(
    ("signals", "policy", "unevaluated"),
    [
        (
            {
                "entry_policy": "r2_pit_htf_exec",
                "confirmation_mode": "explicit_trigger",
                "gate_enabled": True,
            },
            "r2_pit_htf_exec",
            ["participation", "crowding"],
        ),
        (
            {"gate_enabled": True, "gate_use_participation": True, "gate_use_crowding": True},
            "legacy_gates",
            [],
        ),
        (
            {"gate_enabled": True, "gate_use_participation": False, "gate_use_crowding": True},
            "legacy_gates",
            ["participation"],
        ),
    ],
)
def test_runtime_records_policy_metadata_at_creation_without_touching_the_gate(
    signals: dict[str, Any], policy: str, unevaluated: list[str]
) -> None:
    runtime = _runtime(signals)
    decision = make_decision(gate=_gate())
    enriched = runtime._with_presentation_metadata(decision)
    assert enriched.metadata["entry_policy"] == policy
    assert enriched.metadata["unevaluated_gates"] == unevaluated
    assert enriched.gate == decision.gate
    assert (enriched.event_id, enriched.score, enriched.stage) == (
        decision.event_id,
        decision.score,
        decision.stage,
    )
    json.dumps(enriched.metadata)


def test_runtime_leaves_ungated_and_already_annotated_decisions_alone() -> None:
    runtime = _runtime({"gate_enabled": True})
    ungated = make_decision()
    assert runtime._with_presentation_metadata(ungated) is ungated
    annotated = make_decision(gate=_gate(), metadata={"entry_policy": "legacy_gates"})
    assert runtime._with_presentation_metadata(annotated) is annotated


def test_published_decision_row_carries_the_policy_metadata() -> None:
    runtime = _runtime({"gate_enabled": True})
    decision = make_decision(gate=_gate())
    persisted = asyncio.run(runtime._publish_decision(decision))
    assert persisted is not None
    stored = runtime.repository.recent_signals(limit=1)[0]
    assert stored.metadata["entry_policy"] == "legacy_gates"
    assert stored.metadata["unevaluated_gates"] == []


def test_input_availability_label_replaces_data_completeness() -> None:
    from test_discord import directional_metadata

    decision = make_decision(metadata=directional_metadata())
    embed = _embed(decision)
    text = json.dumps(embed, ensure_ascii=False)
    assert "입력 가용성(체결흐름·호가)" in text
    assert "데이터 완전성" not in text


def test_embed_budget_holds_for_the_longest_case_and_keeps_the_validation_field() -> None:
    gate = GateEvaluation(
        trend_score=0,
        participation_score=0,
        crowding_risk_score=100,
        execution_score=0,
        completeness_score=0,
        passed=False,
        failures=tuple("failure-" + "x" * 1_000 for _ in range(5)),
    )
    from test_discord import directional_metadata

    metadata = directional_metadata()
    metadata.update({"entry_policy": "r2_pit_htf_exec", "unevaluated_gates": ["participation"]})
    decision = make_decision(
        reasons=tuple("evidence-" + "x" * 1_000 for _ in range(10)),
        gate=gate,
        regime=MarketRegime(label="x" * 2_000, btc_trend="x" * 2_000, breadth_ratio=0.5),
        rule_version="r" * 64,
        metadata=metadata,
    )
    embed = _embed(decision)
    total = (
        len(embed["title"])
        + len(embed["description"])
        + len(embed["footer"]["text"])
        + sum(len(f["name"]) + len(f["value"]) for f in embed["fields"])
    )
    assert total <= DISCORD_EMBED_TOTAL_LIMIT
    assert _field(embed, VALIDATION_FIELD_NAME)
    assert embed["title"].startswith("[USDⓈ-M] ")


# --------------------------------------------------------------------------
# Anomaly display (presentation only)
# --------------------------------------------------------------------------


def _ticker(index: int, price: float) -> MiniTicker:
    return MiniTicker(
        market=Market.SPOT,
        symbol="TESTUSDT",
        event_time_ms=index * 1_000,
        close=Decimal(str(price)),
    )


def _settings() -> SignalSettings:
    return SignalSettings(
        anomaly_horizon_seconds=10,
        anomaly_min_absolute_return=0.01,
        anomaly_robust_zscore=2,
        anomaly_min_points=5,
        anomaly_history_points=50,
    )


def _trigger(prices: list[float]) -> RuleEvaluation:
    detector = AnomalyDetector(_settings())
    result = None
    for index, price in enumerate(prices):
        for item in detector.update(_ticker(index, price), frozenset({"TESTUSDT"}), MarketRegime()):
            if item.triggered:
                result = item
    assert result is not None
    return result


def test_flat_history_mad_zero_shows_not_computable_and_records_the_flag() -> None:
    result = _trigger([100.0] * 10 + [103.0])
    assert result.metadata["sigma_floor_hit"] is True
    assert result.metadata["robust_zscore"] > 1_000_000  # detection value is untouched
    reason = " ".join(result.reasons)
    assert "robust z 산출 불가(MAD=0)" in reason
    assert "z-score" not in reason
    assert result.family is SignalFamily.PUMP_RISK


def test_normal_sigma_shows_the_numeric_value() -> None:
    prices = [100 + (0.001 if i % 2 else 0) for i in range(10)] + [103]
    result = _trigger(prices)
    assert result.metadata["sigma_floor_hit"] is False
    expected = f"robust anomaly z-score {result.metadata['robust_zscore']:.2f}"
    assert expected in " ".join(result.reasons)


@pytest.mark.parametrize(
    ("value", "floor", "expected"),
    [
        (2_898_063.8, True, "robust z 산출 불가(MAD=0)"),
        (5.0, True, "robust z 산출 불가(MAD=0)"),
        (999.99, False, "robust anomaly z-score 999.99"),
        (1000.0, False, "robust anomaly z-score ≥1000"),
        (1_000_000.0, False, "robust anomaly z-score ≥1000"),
        (12.345, False, "robust anomaly z-score 12.35"),
    ],
)
def test_robust_z_text_boundaries(value: float, floor: bool, expected: str) -> None:
    assert ROBUST_Z_DISPLAY_CAP == 1000
    assert AnomalyDetector._robust_z_text(value, floor) == expected
