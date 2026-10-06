from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import Clock
from signalbot.config import AlertSettings
from signalbot.domain.enums import SignalFamily
from signalbot.domain.models import SignalDecision
from signalbot.persistence.repository import SqlRepository

LOGGER = logging.getLogger(__name__)

_RISK_FAMILIES = frozenset({SignalFamily.PUMP_RISK.value, SignalFamily.CRASH_RISK.value})
_MAX_INLINE_RETRY_SECONDS = 30.0
_MAX_EMBARGO_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str
    attempts: int
    response_code: int | None = None
    detail: str | None = None


class DiscordNotifier:
    def __init__(
        self,
        settings: AlertSettings,
        repository: SqlRepository,
        clock: Clock,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.clock = clock
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=settings.timeout_seconds)
        # In-memory only: a Discord 429 means the message was not processed, so the
        # item stays pending and the whole notifier pauses until this deadline.
        self._embargo_until_ms = 0

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def send(self, d: SignalDecision) -> DeliveryResult:
        delivery_enabled = (
            self.settings.discord_enabled and self.settings.discord_webhook_url is not None
        )
        payload = build_discord_payload(d, self.settings.discord_username)
        created = self.repository.save_signal_and_enqueue(
            d,
            payload,
            self.clock.now_ms(),
            delivery_enabled=delivery_enabled,
            maximum_active_items=self.settings.outbox_max_active_items,
        )
        item = self.repository.get_outbox(d.event_id)
        if item is None:
            raise RuntimeError(f"outbox item {d.event_id} was not persisted")
        if item.status == "disabled":
            LOGGER.info(
                "Discord disabled; signal persisted only",
                extra={"event_id": d.event_id, "symbol": d.symbol},
            )
            if created:
                self.repository.record_alert(d.event_id, 0, "disabled", self.clock.now_ms())
            return DeliveryResult("disabled", 0)
        if item.status == "delivered":
            return DeliveryResult("duplicate", item.attempts, item.response_code)
        if item.status == "uncertain":
            return DeliveryResult("uncertain", item.attempts, item.response_code, item.detail)
        if item.status == "dead":
            return DeliveryResult("dead", item.attempts, item.response_code, item.detail)
        if item.status == "sending":
            return DeliveryResult("in_flight", item.attempts, item.response_code, item.detail)
        return await self.deliver_event(d.event_id)

    async def deliver_event(self, event_id: str) -> DeliveryResult:
        """Deliver one pending intent without blindly replaying ambiguous requests.

        Discord is called with ``wait=true`` so a successful delivery must return
        a message ID. A transport failure or a success response without that ID is
        quarantined as ``uncertain``; retrying it could create a duplicate message.
        """

        if self.settings.discord_webhook_url is None:
            item = self.repository.get_outbox(event_id)
            attempts = 0 if item is None else item.attempts
            return DeliveryResult("disabled", attempts)
        webhook = self.settings.discord_webhook_url.get_secret_value()
        while True:
            expired = self._expire_if_stale(event_id)
            if expired is not None:
                return expired
            claimed = self.repository.claim_outbox(event_id, self.clock.now_ms())
            if claimed is None:
                return self._current_result(event_id)
            attempt = claimed.attempts
            payload_value = json.loads(claimed.payload_json)
            if not isinstance(payload_value, dict):
                detail = "persisted Discord payload is not a JSON object"
                self.repository.mark_outbox(
                    event_id, "dead", self.clock.now_ms(), detail=detail
                )
                self.repository.record_alert(
                    event_id, attempt, "dead", self.clock.now_ms(), detail=detail
                )
                return DeliveryResult("dead", attempt, detail=detail)
            try:
                response = await self._client.post(
                    webhook,
                    params={"wait": "true"},
                    json=payload_value,
                )
            except httpx.HTTPError as exc:
                detail = f"Discord delivery outcome unknown after {type(exc).__name__}"
                self.repository.mark_outbox(
                    event_id,
                    "uncertain",
                    self.clock.now_ms(),
                    detail=detail,
                )
                self.repository.record_alert(
                    event_id,
                    attempt,
                    "uncertain",
                    self.clock.now_ms(),
                    detail=detail,
                )
                return DeliveryResult("uncertain", attempt, detail=detail)

            status_code = response.status_code
            if 200 <= status_code < 300:
                message_id = self._message_id(response)
                if message_id is not None:
                    self.repository.mark_outbox(
                        event_id,
                        "delivered",
                        self.clock.now_ms(),
                        response_code=status_code,
                        message_id=message_id,
                    )
                    self.repository.record_alert(
                        event_id, attempt, "sent", self.clock.now_ms(), status_code
                    )
                    return DeliveryResult("sent", attempt, status_code)
                detail = "Discord success response lacked a message ID; outcome is ambiguous"
                self.repository.mark_outbox(
                    event_id,
                    "uncertain",
                    self.clock.now_ms(),
                    response_code=status_code,
                    detail=detail,
                )
                self.repository.record_alert(
                    event_id,
                    attempt,
                    "uncertain",
                    self.clock.now_ms(),
                    status_code,
                    detail,
                )
                return DeliveryResult("uncertain", attempt, status_code, detail)

            detail = f"Discord HTTP {status_code}: {response.text[:300]}"
            if status_code == 429:
                # The message was not processed, so it is safe to keep it pending.
                # It is never marked dead for rate limiting; delivery-age expiry
                # bounds how long it can wait.
                self.repository.mark_outbox(
                    event_id,
                    "pending",
                    self.clock.now_ms(),
                    response_code=status_code,
                    detail=detail,
                )
                self.repository.record_alert(
                    event_id,
                    attempt,
                    "rate_limited",
                    self.clock.now_ms(),
                    status_code,
                    detail,
                )
                if attempt < self.settings.max_attempts:
                    await asyncio.sleep(self._retry_after(response))
                    continue
                embargo_seconds = self._retry_after(response, maximum=_MAX_EMBARGO_SECONDS)
                self._embargo_until_ms = self.clock.now_ms() + int(embargo_seconds * 1000)
                LOGGER.warning(
                    "Discord rate limit persisted; pausing outbox delivery",
                    extra={"event_id": event_id, "attempt": attempt},
                )
                return DeliveryResult("rate_limited", attempt, status_code, detail)

            if status_code >= 500:
                uncertain_detail = f"{detail}; server-side delivery outcome is ambiguous"
                self.repository.mark_outbox(
                    event_id,
                    "uncertain",
                    self.clock.now_ms(),
                    response_code=status_code,
                    detail=uncertain_detail,
                )
                self.repository.record_alert(
                    event_id,
                    attempt,
                    "uncertain",
                    self.clock.now_ms(),
                    status_code,
                    uncertain_detail,
                )
                return DeliveryResult(
                    "uncertain", attempt, status_code, uncertain_detail
                )

            self.repository.mark_outbox(
                event_id,
                "dead",
                self.clock.now_ms(),
                response_code=status_code,
                detail=detail,
            )
            self.repository.record_alert(
                event_id,
                attempt,
                "dead",
                self.clock.now_ms(),
                status_code,
                detail,
            )
            LOGGER.error(
                "Discord delivery permanently failed",
                extra={"event_id": event_id, "status_code": status_code},
            )
            return DeliveryResult("dead", attempt, status_code, detail)

    async def dispatch_pending(self, limit: int = 100) -> list[DeliveryResult]:
        """Drain a bounded batch of durable pending notifications."""

        results: list[DeliveryResult] = []
        for item in self.repository.pending_outbox(limit):
            expired = self._expire_if_stale(item.event_id)
            if expired is not None:
                results.append(expired)
                continue
            if self._embargoed():
                continue
            results.append(await self.deliver_event(item.event_id))
        return results

    def _embargoed(self) -> bool:
        return self.clock.now_ms() < self._embargo_until_ms

    def _delivery_limit_ms(self, family: str) -> int:
        limit_seconds = self.settings.max_delivery_delay_seconds
        if family in _RISK_FAMILIES:
            limit_seconds = min(limit_seconds, self.settings.risk_max_delivery_delay_seconds)
        return limit_seconds * 1000

    def _expire_if_stale(self, event_id: str) -> DeliveryResult | None:
        """Terminally expire a pending item whose signal is too old to be useful.

        The class (risk vs. other) comes from the stored signal row. An expired
        item is never sent and does not count toward the active outbox limit.
        """

        meta = self.repository.signal_delivery_meta(event_id)
        if meta is None:
            return None
        family, event_time_ms = meta
        now_ms = self.clock.now_ms()
        age_ms = now_ms - event_time_ms
        limit_ms = self._delivery_limit_ms(family)
        if age_ms <= limit_ms:
            return None
        detail = (
            f"expired before delivery: age {age_ms} ms exceeds limit {limit_ms} ms "
            f"for {family}"
        )
        if not self.repository.mark_outbox(
            event_id, "expired", now_ms, detail=detail, expected_status="pending"
        ):
            return None
        self.repository.append_alert_audit(event_id, "expired", now_ms, detail=detail)
        LOGGER.warning(
            "stale Discord alert expired without delivery",
            extra={"event_id": event_id},
        )
        return self._current_result(event_id)

    async def run_dispatch_loop(
        self,
        stop_event: asyncio.Event,
        *,
        batch_limit: int = 100,
        idle_seconds: float = 1.0,
        error_backoff_initial_seconds: float = 1.0,
        error_backoff_max_seconds: float = 60.0,
    ) -> None:
        """Continuously drain bounded outbox batches until stop or cancellation.

        A failing batch (for example a transient database error) is logged at
        ERROR and retried with exponential backoff capped at
        ``error_backoff_max_seconds``; the wait is interrupted by ``stop_event``.
        An item that was claimed as ``sending`` when the error hit is not
        retried by this loop; it is quarantined as ``uncertain`` by
        ``recover_inflight`` on the next restart (existing behavior).
        ``asyncio.CancelledError`` is never caught.
        """

        if (
            batch_limit < 1
            or idle_seconds <= 0
            or error_backoff_initial_seconds <= 0
            or error_backoff_max_seconds < error_backoff_initial_seconds
        ):
            raise ValueError("outbox dispatch limits must be positive")
        consecutive_failures = 0
        while not stop_event.is_set():
            try:
                results = await self.dispatch_pending(batch_limit)
            except Exception:
                consecutive_failures += 1
                delay = min(
                    error_backoff_initial_seconds * 2 ** min(consecutive_failures - 1, 16),
                    error_backoff_max_seconds,
                )
                LOGGER.error(
                    "Discord outbox dispatch batch failed; backing off",
                    extra={"attempt": consecutive_failures},
                    exc_info=True,
                )
                wait_seconds = delay
            else:
                if consecutive_failures:
                    LOGGER.info(
                        "Discord outbox dispatch recovered",
                        extra={"attempt": consecutive_failures},
                    )
                consecutive_failures = 0
                if len(results) >= batch_limit:
                    continue
                wait_seconds = idle_seconds
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            except TimeoutError:
                continue

    def recover_inflight(self) -> int:
        """Quarantine requests interrupted while the HTTP outcome was unknown."""

        return self.repository.mark_inflight_uncertain(self.clock.now_ms())

    def _current_result(self, event_id: str) -> DeliveryResult:
        item = self.repository.get_outbox(event_id)
        if item is None:
            return DeliveryResult("missing", 0, detail="outbox item does not exist")
        status = {
            "delivered": "duplicate",
            "sending": "in_flight",
        }.get(item.status, item.status)
        return DeliveryResult(status, item.attempts, item.response_code, item.detail)

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

    @staticmethod
    def _retry_after(
        response: httpx.Response, maximum: float = _MAX_INLINE_RETRY_SECONDS
    ) -> float:
        try:
            payload: Any = response.json()
            value = float(payload.get("retry_after", 1)) if isinstance(payload, dict) else 1
        except (ValueError, TypeError):
            value = 1
        return min(max(value, 0.05), maximum)
