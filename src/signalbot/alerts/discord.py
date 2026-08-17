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
from signalbot.domain.models import SignalDecision
from signalbot.persistence.repository import SqlRepository

LOGGER = logging.getLogger(__name__)


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
            if status_code == 429 and attempt < self.settings.max_attempts:
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
                await asyncio.sleep(self._retry_after(response))
                continue

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
            results.append(await self.deliver_event(item.event_id))
        return results

    async def run_dispatch_loop(
        self,
        stop_event: asyncio.Event,
        *,
        batch_limit: int = 100,
        idle_seconds: float = 1.0,
    ) -> None:
        """Continuously drain bounded outbox batches until cancellation."""

        if batch_limit < 1 or idle_seconds <= 0:
            raise ValueError("outbox dispatch limits must be positive")
        while not stop_event.is_set():
            results = await self.dispatch_pending(batch_limit)
            if len(results) >= batch_limit:
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=idle_seconds)
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
    def _retry_after(response: httpx.Response) -> float:
        try:
            payload: Any = response.json()
            value = float(payload.get("retry_after", 1)) if isinstance(payload, dict) else 1
        except (ValueError, TypeError):
            value = 1
        return min(max(value, 0.05), 30)
