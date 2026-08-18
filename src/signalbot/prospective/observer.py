from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from signalbot.config import Settings
from signalbot.domain.models import (
    ComparatorCandidate,
    FeatureSnapshot,
)
from signalbot.persistence.repository import SqlRepository
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.shadow_policy import shadow_policy_identity


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def shadow_opportunity_id(
    *,
    campaign_id: str,
    market: str,
    symbol: str,
    decision_time_ms: int,
    primary_interval: str,
) -> str:
    """Policy-neutral, deterministic opportunity identity.

    Independent of whether R2 or the shadow successor passes, so one raw C0
    close maps to exactly one common opportunity for the campaign.
    """

    return _sha256("|".join([
        campaign_id,
        market,
        symbol,
        str(decision_time_ms),
        primary_interval,
    ]))


def shadow_observation_id(
    *,
    opportunity_id: str,
    policy_sha256: str,
    schema_version: str,
) -> str:
    """Policy-specific observer identity derived from the common opportunity."""

    return _sha256("|".join([opportunity_id, policy_sha256, schema_version]))


def shadow_config_sha256(settings: Settings) -> str:
    """Deterministic config digest for the frozen observation inputs."""

    canonical = {
        "rule_version": settings.rule_version,
        "primary_interval": settings.binance.primary_interval,
        "markets": sorted(item.value for item in settings.binance.markets),
        "intervals": sorted(settings.binance.intervals),
        "confirmation_mode": settings.signals.confirmation_mode,
        "shadow": settings.shadow.model_dump(mode="json"),
        "shared": {
            "relative_volume_threshold": settings.signals.relative_volume_threshold,
            "book_maximum_age_ms": settings.signals.book_maximum_age_ms,
            "maximum_spread_bps": settings.signals.maximum_spread_bps,
            "execution_notional_usdt": settings.signals.execution_notional_usdt,
        },
    }
    return _sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")))


def _coverage_cell_key(market: str, decision_close_ms: int) -> tuple[str, int]:
    return (market, decision_close_ms)


class ShadowObserver:
    """Live prospective shadow observer feeding evidence persistence only.

    The observer never emits a production decision, never enters the
    ``SignalStateMachine``, never touches PAPER lifecycles, and never causes a
    Discord delivery. For each raw C0 5m close it writes one durable comparator
    observation; it also maintains a per-close coverage ledger whose invariant
    (raw C0 count == comparator rows persisted) proves there are no silent
    observation holes for a completed close.
    """

    def __init__(
        self,
        settings: Settings,
        engine: SignalRuleEngine,
        repository: SqlRepository,
        *,
        campaign_id: str,
        created_at_ms: int,
    ) -> None:
        if not settings.shadow.observation_enabled:
            raise ValueError("shadow observer requires observation_enabled")
        self.settings = settings
        self.engine = engine
        self.repository = repository
        self.campaign_id = campaign_id
        self.created_at_ms = created_at_ms
        self.policy_sha256 = shadow_policy_identity(
            settings.shadow, settings.signals
        )
        self.config_sha256 = shadow_config_sha256(settings)
        self.schema_version = settings.shadow.observation_schema_version
        # (market, decision_close_ms) -> mutable cell counters
        self._pending: dict[tuple[str, int], dict[str, Any]] = defaultdict(dict)
        self._finalized: set[tuple[str, int]] = set()

    def observe(
        self,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
        tradable_symbols: frozenset[str],
    ) -> ComparatorCandidate | None:
        candidate = self.engine.evaluate_comparator(feature, contexts)
        if candidate is None:
            return None

        key = _coverage_cell_key(
            feature.market.value, candidate.decision_time_ms
        )
        self._begin_cell_if_needed(key, feature, candidate, tradable_symbols)
        cell = self._pending[key]

        mature = cell.setdefault("mature", 0)
        cell["mature"] = mature + 1
        if {"15m", "1h"}.issubset(contexts):
            cell["htf_ready"] = cell.get("htf_ready", 0) + 1
        if feature.spread_bps is not None and (
            feature.book_age_ms is None
            or feature.book_age_ms <= self.settings.signals.book_maximum_age_ms
        ):
            cell["fresh_bbo"] = cell.get("fresh_bbo", 0) + 1

        if candidate.raw_c0_triggered:
            self._persist_observation(candidate, feature, contexts)
            cell["raw_c0"] = cell.get("raw_c0", 0) + 1
            cell["comparator_rows"] = cell.get("comparator_rows", 0) + 1

        self._finalize_older_cells(key)
        return candidate

    def _begin_cell_if_needed(
        self,
        key: tuple[str, int],
        feature: FeatureSnapshot,
        candidate: ComparatorCandidate,
        tradable_symbols: frozenset[str],
    ) -> None:
        cell = self._pending[key]
        if "expected" in cell:
            return
        universe_hash = _sha256(
            "|".join(sorted(symbol.upper() for symbol in tradable_symbols))
        )
        cell.update(
            {
                "market": feature.market.value,
                "decision_close_ms": candidate.decision_time_ms,
                "primary_interval": feature.interval,
                "expected": len(tradable_symbols),
                "universe_hash": universe_hash,
                "mature": 0,
                "htf_ready": 0,
                "fresh_bbo": 0,
                "raw_c0": 0,
                "comparator_rows": 0,
                "missing": [],
            }
        )

    def _finalize_older_cells(self, this_key: tuple[str, int]) -> None:
        market, close = this_key
        for pending_key in list(self._pending):
            pending_market, pending_close = pending_key
            if pending_market != market:
                continue
            if pending_close >= close:
                continue
            if pending_key in self._finalized or pending_key == this_key:
                continue
            self._write_coverage(pending_key)

    def _persist_observation(
        self,
        candidate: ComparatorCandidate,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> bool:
        opportunity_id = shadow_opportunity_id(
            campaign_id=self.campaign_id,
            market=candidate.market.value,
            symbol=candidate.symbol,
            decision_time_ms=candidate.decision_time_ms,
            primary_interval=candidate.primary_interval,
        )
        observation_id = shadow_observation_id(
            opportunity_id=opportunity_id,
            policy_sha256=self.policy_sha256,
            schema_version=self.schema_version,
        )
        payload = build_observation_payload(
            self, candidate, feature, contexts, opportunity_id
        )
        return self.repository.save_shadow_observation(
            observation_id=observation_id,
            campaign_id=self.campaign_id,
            opportunity_id=opportunity_id,
            market=candidate.market.value,
            symbol=candidate.symbol,
            family=candidate.family.value,
            direction=candidate.direction.value,
            decision_time_ms=candidate.decision_time_ms,
            primary_interval=candidate.primary_interval,
            payload=payload,
            policy_sha256=self.policy_sha256,
            created_at_ms=self.created_at_ms,
        )

    def _write_coverage(self, key: tuple[str, int]) -> None:
        cell = self._pending.pop(key, None)
        if cell is None:
            return
        self._finalized.add(key)
        raw_c0 = cell.get("raw_c0", 0)
        comparator_rows = cell.get("comparator_rows", 0)
        mature = cell.get("mature", 0)
        expected = cell.get("expected", 0)
        complete = mature == expected and raw_c0 == comparator_rows
        missing = ["some tradable symbols did not yield a mature 5m close"] if (
            mature < expected
        ) else []
        if raw_c0 != comparator_rows:
            missing.append(
                "raw C0 opportunity count does not match comparator rows persisted"
            )
        self.repository.save_shadow_coverage(
            campaign_id=self.campaign_id,
            market=cell["market"],
            decision_close_ms=cell["decision_close_ms"],
            primary_interval=cell["primary_interval"],
            expected_tradable_count=expected,
            tradable_universe_hash=cell["universe_hash"],
            mature_count=mature,
            htf_ready_count=cell.get("htf_ready", 0),
            fresh_bbo_count=cell.get("fresh_bbo", 0),
            raw_c0_count=raw_c0,
            comparator_rows=comparator_rows,
            complete=complete,
            failures=missing,
            created_at_ms=self.created_at_ms,
        )

    def flush(self) -> None:
        """Finalize any remaining coverage cells (safe on graceful shutdown)."""

        for key in list(self._pending):
            if key in self._finalized:
                continue
            self._write_coverage(key)


def build_observation_payload(
    observer: ShadowObserver,
    candidate: ComparatorCandidate,
    feature: FeatureSnapshot,
    contexts: Mapping[str, FeatureSnapshot],
    opportunity_id: str,
) -> dict[str, Any]:
    """Deterministic serializable payload capturing causal input, strict-prior
    context, execution evidence, and both policy results for one opportunity."""

    c15 = contexts.get("15m")
    c1 = contexts.get("1h")
    signals = observer.settings.signals
    execution_available = feature.spread_bps is not None and (
        feature.book_age_ms is None
        or feature.book_age_ms <= signals.book_maximum_age_ms
    )
    return {
        "provenance": {
            "policy_name": "shadow_er_context",
            "policy_version": observer.settings.shadow.policy_version,
            "policy_sha256": observer.policy_sha256,
            "rule_version": observer.settings.rule_version,
            "config_sha256": observer.config_sha256,
            "observation_schema_version": observer.schema_version,
        },
        "common_causal_input": {
            "event_time_ms": feature.event_time_ms,
            "price": feature.price,
            "previous_close": feature.previous_close,
            "recent_high": feature.recent_high,
            "recent_low": feature.recent_low,
            "ema20": feature.ema20,
            "ema50": feature.ema50,
            "macd_histogram": feature.macd_histogram,
            "macd_histogram_previous": feature.macd_histogram_previous,
            "adx": feature.adx,
            "atr": feature.atr,
            "atr_percent": feature.atr_percent,
            "efficiency_ratio_20": feature.efficiency_ratio_20,
            "relative_volume": feature.relative_volume,
            "btc_trend": feature.regime.btc_trend,
            "breadth_ratio": feature.regime.breadth_ratio,
            "data_completeness": feature.data_completeness,
        },
        "strict_prior_context": {
            "15m_event_time_ms": c15.event_time_ms if c15 is not None else None,
            "15m_close": c15.price if c15 is not None else None,
            "15m_ema20": c15.ema20 if c15 is not None else None,
            "15m_ema50": c15.ema50 if c15 is not None else None,
            "1h_event_time_ms": c1.event_time_ms if c1 is not None else None,
            "1h_close": c1.price if c1 is not None else None,
            "1h_ema20": c1.ema20 if c1 is not None else None,
            "1h_ema50": c1.ema50 if c1 is not None else None,
        },
        "execution_evidence": {
            "spread_bps": feature.spread_bps,
            "spread_is_proxy": feature.spread_is_proxy,
            "book_age_ms": feature.book_age_ms,
            "bid_quote_capacity": feature.bid_quote_capacity,
            "ask_quote_capacity": feature.ask_quote_capacity,
            "execution_available": execution_available,
        },
        "incumbent_r2": {
            "raw_c0_triggered": candidate.raw_c0_triggered,
            "raw_score": candidate.raw_score,
            "r2_passed": candidate.r2_passed,
            "r2_failures": list(candidate.r2_failures),
        },
        "shadow": {
            "gate_passed": candidate.shadow_passed,
            "failures": list(candidate.shadow_failures),
            "informational_only": True,
            "opportunity_id": opportunity_id,
        },
    }
