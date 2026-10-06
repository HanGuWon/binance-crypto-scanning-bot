from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from position_guardian.alert_recovery import GuardianAlertRecovery, recover_guardian_alerts
from position_guardian.alerts import GUARDIAN_ALERT_SCHEMA_VERSION
from position_guardian.config import GuardianAlertSettings
from position_guardian.persistence.repository import GuardianRepository
from signalbot.clock import Clock

_ALERT_CODES = {
    "WOULD_UPDATE_STOP",
    "STALE_CONTEXT",
    "MANUAL_SIZE_INCREASE",
    "SIDE_FLIP",
    "PROTECTION_MISSING",
    "RECONCILIATION_UNCERTAIN",
}
_SEVERITIES = {"INFO", "WARNING", "CRITICAL"}
_DETAIL_KEYS = {
    "context_id",
    "intent_id",
    "policy_state",
    "previous_stop",
    "proposed_stop",
    "effective_from_next_candle",
    "reason",
    "state",
    "observed_quantity",
}
_TOP_LEVEL_KEYS = {
    "schema_version",
    "alert_id",
    "code",
    "severity",
    "event_time_ms",
    "symbol",
    "position_side",
    "adoption_generation",
    "message",
    "details",
    "exchange_write_calls",
}
GUARDIAN_DISCORD_USERNAME_V1 = "Position Guardian"


@dataclass(frozen=True, slots=True)
class GuardianDeliveryResult:
    status: str
    attempts: int
    response_code: int | None = None
    message_id: str | None = None
    detail_code: str | None = None


@dataclass(frozen=True, slots=True)
class GuardianStartupDispatchResult:
    recovery: GuardianAlertRecovery
    deliveries: tuple[GuardianDeliveryResult, ...]
    exchange_write_calls: int = 0


def build_guardian_discord_payload(
    payload_json: str,
    *,
    expected_alert_id: str,
) -> dict[str, object]:
    """Render only the frozen sanitized Guardian alert schema for Discord."""

    try:
        raw: Any = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("persisted Guardian alert payload is invalid JSON") from exc
    if not isinstance(raw, dict) or set(raw) != _TOP_LEVEL_KEYS:
        raise ValueError("persisted Guardian alert payload violates the frozen schema")
    if raw.get("schema_version") != GUARDIAN_ALERT_SCHEMA_VERSION:
        raise ValueError("persisted Guardian alert schema version is unsupported")

    alert_id = raw.get("alert_id")
    if (
        not isinstance(alert_id, str)
        or len(alert_id) != 64
        or any(character not in "0123456789abcdef" for character in alert_id)
    ):
        raise ValueError("persisted Guardian alert_id is invalid")
    if alert_id != expected_alert_id:
        raise ValueError("persisted Guardian alert_id does not match its outbox row")
    code = raw.get("code")
    severity = raw.get("severity")
    if code not in _ALERT_CODES or severity not in _SEVERITIES:
        raise ValueError("persisted Guardian alert code/severity is unsupported")
    event_time_ms = raw.get("event_time_ms")
    generation = raw.get("adoption_generation")
    if (
        not isinstance(event_time_ms, int)
        or isinstance(event_time_ms, bool)
        or event_time_ms < 0
        or not isinstance(generation, int)
        or isinstance(generation, bool)
        or generation < 1
    ):
        raise ValueError("persisted Guardian alert numeric fields are invalid")
    symbol = raw.get("symbol")
    side = raw.get("position_side")
    message = raw.get("message")
    if not isinstance(symbol, str) or not symbol or len(symbol) > 32:
        raise ValueError("persisted Guardian symbol is invalid")
    if side not in {"LONG", "SHORT"}:
        raise ValueError("persisted Guardian position side is invalid")
    if not isinstance(message, str) or not message.strip() or len(message) > 4_096:
        raise ValueError("persisted Guardian message is invalid")
    if raw.get("exchange_write_calls") != 0:
        raise ValueError("Guardian Discord transport requires zero exchange writes")

    details = raw.get("details")
    if not isinstance(details, dict) or any(key not in _DETAIL_KEYS for key in details):
        raise ValueError("persisted Guardian details violate the frozen allowlist")
    rendered_fields: list[dict[str, object]] = [
        {"name": "Generation", "value": str(generation), "inline": True},
        {"name": "Event time (UTC ms)", "value": str(event_time_ms), "inline": True},
    ]
    for key, value in sorted(details.items()):
        if not isinstance(key, str) or not isinstance(value, str) or len(value) > 1_024:
            raise ValueError("persisted Guardian detail value is invalid")
        rendered_fields.append({"name": key, "value": value or "-", "inline": False})
    if len(rendered_fields) > 25:
        raise ValueError("persisted Guardian alert has too many Discord fields")

    title = f"[Guardian][{severity}] {code} · {symbol} {side}"
    footer = f"alert_id={alert_id}"
    total_text = (
        len(title)
        + len(message)
        + len(footer)
        + sum(len(str(field["name"])) + len(str(field["value"])) for field in rendered_fields)
    )
    if len(title) > 256 or len(footer) > 2_048 or total_text > 6_000:
        raise ValueError("persisted Guardian alert exceeds Discord embed text limits")

    return {
        "username": GUARDIAN_DISCORD_USERNAME_V1,
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": f"[Guardian][{severity}] {code} · {symbol} {side}",
                "description": message,
                "fields": rendered_fields,
                "footer": {"text": f"alert_id={alert_id}"},
            }
        ],
    }


class GuardianDiscordNotifier:
    """L60-07 bounded Discord transport over the durable Guardian outbox."""

    def __init__(
        self,
        settings: GuardianAlertSettings,
        repository: GuardianRepository,
        clock: Clock,
        client: httpx.AsyncClient | None = None,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.clock = clock
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=settings.timeout_seconds)
        self._sleep = sleep
        self._startup_recovered = False

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def recover_startup(self) -> GuardianAlertRecovery:
        recovery = recover_guardian_alerts(
            self.repository,
            now_ms=self.clock.now_ms(),
            maximum_active_items=self.settings.outbox_max_active_items,
        )
        self._startup_recovered = True
        return recovery

    async def startup_and_dispatch(self) -> GuardianStartupDispatchResult:
        recovery = self.recover_startup()
        deliveries = tuple(await self.dispatch_pending(self.settings.batch_limit))
        return GuardianStartupDispatchResult(recovery=recovery, deliveries=deliveries)

    async def deliver_alert(self, alert_id: str) -> GuardianDeliveryResult:
        self._require_startup_recovery()
        if not self.settings.discord_enabled or self.settings.discord_webhook_url is None:
            return self._current_result(alert_id, fallback="disabled")
        webhook = self.settings.discord_webhook_url.get_secret_value()

        while True:
            claimed = self.repository.claim_guardian_alert(alert_id, self.clock.now_ms())
            if claimed is None:
                return self._current_result(alert_id)
            attempt = claimed.attempts
            if hashlib.sha256(claimed.payload_json.encode()).hexdigest() != claimed.payload_sha256:
                self._mark_required(
                    alert_id,
                    "dead",
                    detail_code="PAYLOAD_HASH_MISMATCH",
                )
                return GuardianDeliveryResult(
                    "dead",
                    attempt,
                    detail_code="PAYLOAD_HASH_MISMATCH",
                )
            try:
                payload = build_guardian_discord_payload(
                    claimed.payload_json,
                    expected_alert_id=claimed.alert_id,
                )
            except ValueError:
                self._mark_required(
                    alert_id,
                    "dead",
                    detail_code="INVALID_PERSISTED_PAYLOAD",
                )
                return GuardianDeliveryResult(
                    "dead",
                    attempt,
                    detail_code="INVALID_PERSISTED_PAYLOAD",
                )

            try:
                response = await self._client.post(
                    webhook,
                    params={"wait": "true"},
                    json=payload,
                )
            except httpx.HTTPError:
                self._mark_required(
                    alert_id,
                    "uncertain",
                    detail_code="TRANSPORT_OUTCOME_UNKNOWN",
                )
                return GuardianDeliveryResult(
                    "uncertain",
                    attempt,
                    detail_code="TRANSPORT_OUTCOME_UNKNOWN",
                )

            status_code = response.status_code
            if 200 <= status_code < 300:
                message_id = self._message_id(response)
                if message_id is not None:
                    self._mark_required(
                        alert_id,
                        "delivered",
                        response_code=status_code,
                        message_id=message_id,
                        detail_code="DISCORD_MESSAGE_CONFIRMED",
                    )
                    return GuardianDeliveryResult(
                        "sent",
                        attempt,
                        response_code=status_code,
                        message_id=message_id,
                        detail_code="DISCORD_MESSAGE_CONFIRMED",
                    )
                self._mark_required(
                    alert_id,
                    "uncertain",
                    response_code=status_code,
                    detail_code="SUCCESS_NO_MESSAGE_ID",
                )
                return GuardianDeliveryResult(
                    "uncertain",
                    attempt,
                    response_code=status_code,
                    detail_code="SUCCESS_NO_MESSAGE_ID",
                )

            if status_code == 429:
                if attempt < self.settings.max_attempts:
                    self._mark_required(
                        alert_id,
                        "pending",
                        response_code=status_code,
                        detail_code="RATE_LIMITED",
                    )
                    await self._sleep(self._retry_after(response))
                    continue
                self._mark_required(
                    alert_id,
                    "dead",
                    response_code=status_code,
                    detail_code="RATE_LIMIT_EXHAUSTED",
                )
                return GuardianDeliveryResult(
                    "dead",
                    attempt,
                    response_code=status_code,
                    detail_code="RATE_LIMIT_EXHAUSTED",
                )

            if status_code >= 500:
                self._mark_required(
                    alert_id,
                    "uncertain",
                    response_code=status_code,
                    detail_code="SERVER_OUTCOME_AMBIGUOUS",
                )
                return GuardianDeliveryResult(
                    "uncertain",
                    attempt,
                    response_code=status_code,
                    detail_code="SERVER_OUTCOME_AMBIGUOUS",
                )

            detail_code = f"HTTP_{status_code}"
            self._mark_required(
                alert_id,
                "dead",
                response_code=status_code,
                detail_code=detail_code,
            )
            return GuardianDeliveryResult(
                "dead",
                attempt,
                response_code=status_code,
                detail_code=detail_code,
            )

    async def dispatch_pending(self, limit: int | None = None) -> list[GuardianDeliveryResult]:
        self._require_startup_recovery()
        if not self.settings.discord_enabled or self.settings.discord_webhook_url is None:
            return []
        batch_limit = self.settings.batch_limit if limit is None else limit
        if batch_limit < 1 or batch_limit > self.settings.batch_limit:
            raise ValueError("Guardian dispatch limit exceeds the configured bounded batch")
        results: list[GuardianDeliveryResult] = []
        for item in self.repository.pending_guardian_alerts(batch_limit):
            results.append(await self.deliver_alert(item.alert_id))
        return results

    async def run_dispatch_loop(
        self,
        stop_event: asyncio.Event,
        *,
        idle_seconds: float = 1.0,
    ) -> None:
        self._require_startup_recovery()
        if idle_seconds <= 0:
            raise ValueError("idle_seconds must be positive")
        while not stop_event.is_set():
            results = await self.dispatch_pending(self.settings.batch_limit)
            if len(results) >= self.settings.batch_limit:
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=idle_seconds)
            except TimeoutError:
                continue

    def _require_startup_recovery(self) -> None:
        if not self._startup_recovered:
            raise RuntimeError("Guardian alert recovery must run before dispatch")

    def _current_result(
        self,
        alert_id: str,
        *,
        fallback: str = "missing",
    ) -> GuardianDeliveryResult:
        item = self.repository.get_guardian_alert_outbox(alert_id)
        if item is None:
            return GuardianDeliveryResult(fallback, 0, detail_code="OUTBOX_ITEM_MISSING")
        status = {
            "delivered": "duplicate",
            "sending": "in_flight",
        }.get(item.status, item.status)
        return GuardianDeliveryResult(
            status,
            item.attempts,
            item.response_code,
            item.message_id,
            item.detail_code,
        )

    def _mark_required(
        self,
        alert_id: str,
        status: str,
        *,
        response_code: int | None = None,
        message_id: str | None = None,
        detail_code: str | None = None,
    ) -> None:
        if not self.repository.mark_guardian_alert(
            alert_id,
            status,
            self.clock.now_ms(),
            response_code=response_code,
            message_id=message_id,
            detail_code=detail_code,
        ):
            raise RuntimeError("Guardian alert outbox transition lost its sending CAS")

    @staticmethod
    def _message_id(response: httpx.Response) -> str | None:
        try:
            payload: Any = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        value = payload.get("id")
        return value if isinstance(value, str) and value else None

    def _retry_after(self, response: httpx.Response) -> float:
        try:
            payload: Any = response.json()
            value = float(payload.get("retry_after", 1)) if isinstance(payload, dict) else 1
        except (ValueError, TypeError):
            value = 1
        if not math.isfinite(value):
            value = 1
        return min(max(value, 0.05), self.settings.retry_after_max_seconds)
