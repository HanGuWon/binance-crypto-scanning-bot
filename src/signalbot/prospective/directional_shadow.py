"""Evidence-only live observer for the preregistered Futures pair."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, cast

from signalbot.clock import Clock
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import FeatureSnapshot
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.observer import shadow_observation_id
from signalbot.prospective.source_freeze import default_source_root, freeze_source
from signalbot.signals.gates import evaluate_strict_prior_htf
from signalbot.signals.rules import SignalRuleEngine

SCHEMA_VERSION = "directional_shadow_observation_v1"
_FAMILIES = {SignalFamily.BREAKOUT_LONG, SignalFamily.BREAKDOWN_SHORT}
_DIRECTIONS = {Direction.LONG, Direction.SHORT}


def _sha256_payload(payload: object) -> str:
    encoded = json.dumps(
        _json_ready(payload),
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, (Direction, Market, SignalFamily)):
        return value.value
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"unsupported evidence payload type: {type(value).__name__}")


class DirectionalShadowObserver:
    """Persist raw successor observations without entering production state."""

    def __init__(
        self,
        settings: Settings,
        repository: SqlRepository,
        *,
        clock: Clock,
    ) -> None:
        policy = settings.shadow
        if not policy.directional_observation_enabled:
            raise ValueError("directional observer requires directional_observation_enabled")
        campaign_id = policy.directional_campaign_id
        source_identity = policy.directional_source_identity
        created_at_ms = policy.directional_campaign_created_at_ms
        activation_ms = policy.directional_activation_ms
        if campaign_id is None or source_identity is None or created_at_ms is None:
            raise ValueError("directional observer requires campaign provenance")
        if activation_ms is None:
            raise ValueError("directional observer requires activation_ms")
        running_source = freeze_source(default_source_root())
        if running_source.source_identity != source_identity:
            raise RuntimeError("directional observer source freeze mismatch")
        self.settings = settings
        self.repository = repository
        self.clock = clock
        self.campaign_id = campaign_id
        self.activation_ms = activation_ms
        self.directional_symbols = frozenset(policy.directional_symbols)
        self.universe_sha256 = _sha256_payload(sorted(self.directional_symbols))
        effective_signals = settings.signals.model_copy(
            update={"gate_enabled": False, "entry_policy": "legacy_gates"}
        )
        self.signal_config_sha256 = _sha256_payload(
            effective_signals.model_dump(mode="json")
        )
        strict_prior_htf_contract = {
            "required": True,
            "primary_interval": settings.binance.primary_interval,
            "higher_timeframes": ["15m", "1h"],
            "closed_candles_only": True,
            "selection": "strict_prior",
        }
        self.policy_sha256 = _sha256_payload(
            {
                "candidate_version": policy.directional_candidate_version,
                "schema_version": SCHEMA_VERSION,
                "rule_version": settings.rule_version,
                "source_identity": source_identity,
                "preregistration_sha256": policy.directional_preregistration_sha256,
                "universe_sha256": self.universe_sha256,
                "signal_config_sha256": self.signal_config_sha256,
                "strict_prior_htf": strict_prior_htf_contract,
                "families": sorted(item.value for item in _FAMILIES),
                "directions": sorted(item.value for item in _DIRECTIONS),
            }
        )
        self.config_sha256 = _sha256_payload(
            {
                "rule_version": settings.rule_version,
                "candidate_version": policy.directional_candidate_version,
                "source_identity": source_identity,
                "preregistration_sha256": policy.directional_preregistration_sha256,
                "universe_sha256": self.universe_sha256,
                "directional_symbols": sorted(self.directional_symbols),
                "signal_config_sha256": self.signal_config_sha256,
                "signal_settings": effective_signals.model_dump(mode="json"),
                "strict_prior_htf": strict_prior_htf_contract,
                "primary_interval": settings.binance.primary_interval,
                "markets": [Market.FUTURES.value],
                "families": sorted(item.value for item in _FAMILIES),
                "directions": sorted(item.value for item in _DIRECTIONS),
            }
        )
        self.engine = SignalRuleEngine(effective_signals)
        self.repository.register_shadow_campaign(
            campaign_schema_version="shadow_campaign_v1",
            campaign_id=campaign_id,
            campaign_mode="prospective",
            source_identity=source_identity,
            rule_version=settings.rule_version,
            policy_name="futures_bidirectional_successor",
            policy_version=policy.directional_candidate_version,
            policy_sha256=self.policy_sha256,
            config_sha256=self.config_sha256,
            observation_schema_version=SCHEMA_VERSION,
            primary_interval=settings.binance.primary_interval,
            markets=[Market.FUTURES.value],
            families={Market.FUTURES.value: sorted(item.value for item in _FAMILIES)},
            activation_ms=activation_ms,
            created_at_ms=created_at_ms,
            status="REGISTERED",
        )
        campaign = repository.get_shadow_campaign(campaign_id)
        if campaign is None:
            raise RuntimeError("directional shadow campaign could not be reloaded")
        manifest_sha256 = campaign.get("manifest_sha256")
        if not isinstance(manifest_sha256, str) or not manifest_sha256:
            raise RuntimeError("directional shadow campaign is missing manifest_sha256")
        self.campaign_manifest_sha256 = manifest_sha256

    def observe(
        self,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> int:
        if (
            feature.market is not Market.FUTURES
            or feature.symbol not in self.directional_symbols
            or feature.event_time_ms < self.activation_ms
        ):
            return 0
        count = 0
        for evaluation in self.engine.evaluate(feature, contexts):
            if (
                not evaluation.triggered
                or evaluation.family not in _FAMILIES
                or evaluation.direction not in _DIRECTIONS
            ):
                continue
            htf = evaluate_strict_prior_htf(feature, evaluation.direction, contexts)
            opportunity_id = _sha256_payload(
                [
                    self.campaign_id,
                    feature.market.value,
                    feature.symbol,
                    evaluation.family.value,
                    evaluation.event_time_ms,
                ]
            )
            observation_id = shadow_observation_id(
                opportunity_id=opportunity_id,
                policy_sha256=self.policy_sha256,
                schema_version=SCHEMA_VERSION,
            )
            payload: dict[str, Any] = {
                "provenance": {
                    "campaign_id": self.campaign_id,
                    "campaign_manifest_sha256": self.campaign_manifest_sha256,
                    "source_identity": self.settings.shadow.directional_source_identity,
                    "candidate_version": self.settings.shadow.directional_candidate_version,
                    "rule_version": self.settings.rule_version,
                    "config_sha256": self.config_sha256,
                    "policy_sha256": self.policy_sha256,
                    "preregistration_sha256": (
                        self.settings.shadow.directional_preregistration_sha256
                    ),
                    "universe_sha256": self.universe_sha256,
                    "directional_symbols": sorted(self.directional_symbols),
                    "signal_config_sha256": self.signal_config_sha256,
                    "schema_version": SCHEMA_VERSION,
                    "activation_ms": self.activation_ms,
                },
                "causal_input": {
                    "market": feature.market.value,
                    "symbol": feature.symbol,
                    "interval": feature.interval,
                    "event_time_ms": feature.event_time_ms,
                    "price": feature.price,
                    "candle_closed": True,
                    "spread_is_proxy": feature.spread_is_proxy,
                },
                "candidate": {
                    "family": evaluation.family.value,
                    "direction": evaluation.direction.value,
                    "triggered": evaluation.triggered,
                    "raw_score": evaluation.score,
                    "reasons": list(evaluation.reasons),
                    "invalidation": evaluation.invalidation,
                },
                "strict_prior_htf": {
                    "accepted": htf.accepted,
                    "failures": list(htf.failures),
                    "context_event_times_ms": {
                        key: value.event_time_ms for key, value in contexts.items()
                    },
                },
                "informational_only": True,
                "production_entry": False,
                "order_placement": False,
                "discord_delivery": False,
            }
            payload = cast(dict[str, Any], _json_ready(payload))
            if self.repository.save_shadow_observation(
                observation_id=observation_id,
                campaign_id=self.campaign_id,
                opportunity_id=opportunity_id,
                market=feature.market.value,
                symbol=feature.symbol,
                family=evaluation.family.value,
                direction=evaluation.direction.value,
                decision_time_ms=evaluation.event_time_ms,
                primary_interval=feature.interval,
                payload=payload,
                policy_sha256=self.policy_sha256,
                campaign_manifest_sha256=self.campaign_manifest_sha256,
                created_at_ms=self.clock.now_ms(),
            ):
                count += 1
        return count

    def flush(self) -> None:
        """The immutable observation table is durable per close; no finalizer needed."""
