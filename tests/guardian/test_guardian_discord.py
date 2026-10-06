from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import update
from sqlalchemy.orm import Session

from position_guardian.config import GuardianAlertSettings, GuardianCredentials, GuardianSettings
from position_guardian.discord import (
    GuardianDiscordNotifier,
)
from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.models import GuardianAlertOutboxRow
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.reconcile import ReconciliationRequest, reconcile_once
from position_guardian.runtime import GuardianRuntimeOwner
from signalbot.clock import ReplayClock


def _identity() -> ManagedPositionIdentity:
    return ManagedPositionIdentity("private-account-sentinel", "BTCUSDT", "LONG", 7)


def _candidate() -> AdoptionCandidate:
    return AdoptionCandidate(
        identity=_identity(),
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        source_update_time_ms=1000,
        original_risk_stop=Decimal("59000"),
        protection_floor=Decimal("60500"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference("open_order", 11, Decimal("59000")),
    )


def _position(*, update_time_ms: int = 2000) -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amount=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        unrealized_profit=Decimal("10"),
        update_time_ms=update_time_ms,
    )


def _settings(
    *,
    max_attempts: int = 3,
    retry_after_max_seconds: float = 30,
) -> GuardianAlertSettings:
    return GuardianAlertSettings(
        discord_enabled=True,
        discord_webhook_url=SecretStr("https://discord.test/webhook"),
        max_attempts=max_attempts,
        retry_after_max_seconds=retry_after_max_seconds,
        batch_limit=10,
        outbox_max_active_items=100,
    )


def _guardian_settings(*, discord_enabled: bool = True) -> GuardianSettings:
    return GuardianSettings(
        account_alias="private-account-sentinel",
        credentials=GuardianCredentials(
            api_key=SecretStr("key"),
            api_secret=SecretStr("secret"),
        ),
        alerts=(
            _settings()
            if discord_enabled
            else GuardianAlertSettings(discord_enabled=False)
        ),
    )


def _create_alert(
    repository: GuardianRepository,
    *,
    event_id: str = "snapshot-alert",
    delivery_mode: str = "discord_v1",
) -> str:
    assert repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )
    result = reconcile_once(
        repository,
        ReconciliationRequest(
            identity=_identity(),
            position=_position(),
            snapshot_event_id=event_id,
            event_time_ms=3000,
            created_at_ms=3001,
            protective_order_confirmed=False,
            alert_delivery_mode=delivery_mode,  # type: ignore[arg-type]
        ),
    )
    assert result.alerts == ("PROTECTIVE_ORDER_MISSING",)
    rows = repository.list_guardian_alert_outbox()
    assert len(rows) == 1
    return rows[0].alert_id


@pytest.mark.asyncio
async def test_pre_l60_07_source_remains_disabled_after_transport_is_enabled() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"id": "should-not-send"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository, delivery_mode="disabled")
        assert repository.get_guardian_alert_outbox(alert_id).status == "disabled"  # type: ignore[union-attr]
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            recovery = notifier.recover_startup()
            deliveries = await notifier.dispatch_pending()
            assert recovery.alerts_inserted == 0
            assert deliveries == []
            assert requests == 0
            assert repository.get_guardian_alert_outbox(alert_id).status == "disabled"  # type: ignore[union-attr]
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_startup_disables_forged_pending_row_without_source_authority() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"id": "must-not-send"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository, delivery_mode="disabled")
        with Session(repository._engine) as session, session.begin():  # type: ignore[attr-defined]
            session.execute(
                update(GuardianAlertOutboxRow)
                .where(GuardianAlertOutboxRow.alert_id == alert_id)
                .values(status="pending")
            )
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            recovery = notifier.recover_startup()
            assert recovery.unauthorized_pending_disabled == 1
            assert await notifier.dispatch_pending() == []
            assert requests == 0
            item = repository.get_guardian_alert_outbox(alert_id)
            assert item is not None
            assert item.status == "disabled"
            assert item.detail_code == "SOURCE_NOT_DELIVERY_AUTHORIZED"
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_startup_recovery_is_required_before_any_dispatch() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"id": "discord-message-1"})
        )
    )
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            with pytest.raises(RuntimeError, match="recovery must run before dispatch"):
                await notifier.deliver_alert(alert_id)
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "sent"
        finally:
            await client.aclose()


def test_runtime_owner_binds_configured_delivery_authority_to_new_sources() -> None:
    with GuardianRepository("sqlite://") as repository:
        assert repository.record_adoption(
            event_id="adopt-1",
            event_time_ms=1000,
            created_at_ms=1001,
            candidate=_candidate(),
        )
        owner = GuardianRuntimeOwner(_guardian_settings(), repository)
        result = owner.reconcile(
            ReconciliationRequest(
                identity=_identity(),
                position=_position(),
                snapshot_event_id="snapshot-owner",
                event_time_ms=3000,
                created_at_ms=3001,
                protective_order_confirmed=False,
            )
        )
        assert result.alerts == ("PROTECTIVE_ORDER_MISSING",)
        outbox = repository.list_guardian_alert_outbox()
        assert len(outbox) == 1
        assert outbox[0].status == "pending"


def test_runtime_owner_rejects_identity_from_a_different_account_alias() -> None:
    with GuardianRepository("sqlite://") as repository:
        owner = GuardianRuntimeOwner(_guardian_settings(), repository)
        foreign = ManagedPositionIdentity("other-account", "BTCUSDT", "LONG", 7)
        with pytest.raises(ValueError, match="account_alias"):
            owner.reconcile(
                ReconciliationRequest(
                    identity=foreign,
                    position=_position(),
                    snapshot_event_id="foreign",
                    event_time_ms=3000,
                    created_at_ms=3001,
                    protective_order_confirmed=False,
                )
            )


@pytest.mark.asyncio
async def test_successful_delivery_is_idempotent_sanitized_and_audited() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.content)
        rendered = json.dumps(body, sort_keys=True)
        assert request.url.params["wait"] == "true"
        assert body["allowed_mentions"] == {"parse": []}
        assert "private-account-sentinel" not in rendered
        assert "position_ref" not in rendered
        assert "PROTECTION_MISSING" in rendered
        return httpx.Response(200, json={"id": "discord-message-1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            first = await notifier.deliver_alert(alert_id)
            second = await notifier.deliver_alert(alert_id)
            assert first.status == "sent"
            assert first.message_id == "discord-message-1"
            assert second.status == "duplicate"
            assert len(requests) == 1
            outbox = repository.get_guardian_alert_outbox(alert_id)
            assert outbox is not None
            assert outbox.status == "delivered"
            assert outbox.attempts == 1
            attempts = repository.list_guardian_alert_attempts(alert_id)
            assert [(item.attempt, item.status) for item in attempts] == [(1, "delivered")]
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_success_without_message_id_is_uncertain_and_never_blindly_retried() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            first = await notifier.deliver_alert(alert_id)
            second = await notifier.deliver_alert(alert_id)
            assert first.status == second.status == "uncertain"
            assert first.detail_code == "SUCCESS_NO_MESSAGE_ID"
            assert requests == 1
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_transport_error_is_uncertain_without_retry() -> None:
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise httpx.ReadTimeout("ambiguous", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "uncertain"
            assert result.detail_code == "TRANSPORT_OUTCOME_UNKNOWN"
            assert requests == 1
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_server_error_is_uncertain_without_retry() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503, text="must-not-be-persisted")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "uncertain"
            assert result.response_code == 503
            assert result.detail_code == "SERVER_OUTCOME_AMBIGUOUS"
            assert requests == 1
            attempts = repository.list_guardian_alert_attempts(alert_id)
            assert attempts[0].detail_code == "SERVER_OUTCOME_AMBIGUOUS"
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_definitive_client_error_is_dead() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(400, text="secret-ish response"))
    )
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "dead"
            assert result.detail_code == "HTTP_400"
            assert "secret-ish" not in repository.get_guardian_alert_outbox(alert_id).detail_code  # type: ignore[operator,union-attr]
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_429_is_the_only_automatic_retry_and_is_bounded() -> None:
    requests = 0
    sleeps: list[float] = []

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(429, json={"retry_after": 999})
        return httpx.Response(200, json={"id": "discord-message-2"})

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(
            _settings(max_attempts=2, retry_after_max_seconds=0.25),
            repository,
            ReplayClock(9000),
            client,
            sleep=fake_sleep,
        )
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "sent"
            assert result.attempts == 2
            assert requests == 2
            assert sleeps == [0.25]
            attempts = repository.list_guardian_alert_attempts(alert_id)
            assert [(item.attempt, item.status) for item in attempts] == [
                (1, "rate_limited"),
                (2, "delivered"),
            ]
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_429_retry_budget_exhaustion_is_dead() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(429, json={"retry_after": 0})

    async def no_sleep(_: float) -> None:
        return None

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        notifier = GuardianDiscordNotifier(
            _settings(max_attempts=2),
            repository,
            ReplayClock(9000),
            client,
            sleep=no_sleep,
        )
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "dead"
            assert result.attempts == 2
            assert result.detail_code == "RATE_LIMIT_EXHAUSTED"
            assert requests == 2
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_restart_quarantines_inflight_before_dispatch() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"id": "must-not-send"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        alert_id = _create_alert(repository)
        claimed = repository.claim_guardian_alert(alert_id, 8000)
        assert claimed is not None and claimed.status == "sending"
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            recovery = notifier.recover_startup()
            assert recovery.inflight_quarantined == 1
            assert await notifier.dispatch_pending() == []
            assert requests == 0
            item = repository.get_guardian_alert_outbox(alert_id)
            assert item is not None
            assert item.status == "uncertain"
            assert item.detail_code == "PROCESS_RESTART_IN_FLIGHT"
            attempts = repository.list_guardian_alert_attempts(alert_id)
            assert [(entry.attempt, entry.status) for entry in attempts] == [(1, "uncertain")]
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_invalid_persisted_payload_fails_dead_without_network() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"id": "must-not-send"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with GuardianRepository("sqlite://") as repository:
        assert repository.record_adoption(
            event_id="adopt-1",
            event_time_ms=1000,
            created_at_ms=1001,
            candidate=_candidate(),
        )
        source_id = "manual-discord-source"
        assert repository.record_shadow_alert_source(
            event_id=source_id,
            event_time_ms=2000,
            created_at_ms=2001,
            identity=_identity(),
            payload={
                "schema_version": "non-projecting-source",
                "reason": "NONE",
                "delivery_mode": "discord_v1",
            },
        )
        alert_id = "a" * 64
        assert repository.enqueue_guardian_alert(
            alert_id=alert_id,
            source_event_id=source_id,
            payload={"not": "a guardian alert"},
            created_at_ms=2001,
        )
        notifier = GuardianDiscordNotifier(_settings(), repository, ReplayClock(9000), client)
        try:
            notifier.recover_startup()
            result = await notifier.deliver_alert(alert_id)
            assert result.status == "dead"
            assert result.detail_code == "INVALID_PERSISTED_PAYLOAD"
            assert requests == 0
        finally:
            await client.aclose()
