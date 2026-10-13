"""Keyless USD-M request admission and bounded retry decisions."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)


class RestCircuitOpen(RuntimeError):
    """A venue ban or exhausted shared IP budget prevents further requests."""


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    delay_seconds: float


class RestWeightBudget:
    """Conservative rolling-minute local admission with shared IP header observations.

    No scheduling or HTTP is performed here. Callers must serialize admission
    and observation to avoid concurrent over-admission.
    """

    def __init__(self, *, local_limit: int = 600, shared_limit: int = 2400) -> None:
        if not 0 < local_limit <= 600 or shared_limit < local_limit:
            raise ValueError("invalid REST weight limits")
        self.local_limit = local_limit
        self.shared_limit = shared_limit
        self._uses: deque[tuple[float, int]] = deque()
        self._local_used = 0
        self._shared_used = 0
        self._shared_observed_at = -1.0
        self._banned = False
        self._summary_bucket: int | None = None
        self._summary_requests = 0
        self._summary_weight = 0
        self._last_used_weight_1m: int | None = None

    @property
    def banned(self) -> bool:
        return self._banned

    def admit(self, *, weight: int, now: float) -> bool:
        if self._banned:
            raise RestCircuitOpen("REST venue circuit is open")
        if type(weight) is not int or weight <= 0 or now < 0:
            raise ValueError("positive weight and monotonic time required")
        while self._uses and now - self._uses[0][0] >= 60:
            _, old_weight = self._uses.popleft()
            self._local_used -= old_weight
        if now - self._shared_observed_at >= 60:
            self._shared_used = 0
        if (self._local_used + weight > self.local_limit
                or self._shared_used + weight > self.shared_limit):
            return False
        self._uses.append((now, weight))
        self._local_used += weight
        self._shared_used += weight
        return True

    def record_request(self, *, weight: int, now_ms: int) -> None:
        if type(weight) is not int or weight < 0 or now_ms < 0:
            raise ValueError("non-negative request weight and UTC milliseconds required")
        self._advance_summary_bucket(now_ms // 60_000)
        self._summary_requests += 1
        self._summary_weight += weight

    def flush_minute(self, *, now_ms: int) -> None:
        if now_ms < 0:
            raise ValueError("UTC milliseconds must be non-negative")
        if self._summary_bucket is None:
            return
        self._advance_summary_bucket(now_ms // 60_000)

    def _advance_summary_bucket(self, bucket: int) -> None:
        if self._summary_bucket is None:
            self._summary_bucket = bucket
            return
        if bucket <= self._summary_bucket:
            return
        self._log_summary()
        self._summary_bucket = bucket
        self._summary_requests = 0
        self._summary_weight = 0
        self._last_used_weight_1m = None

    def _log_summary(self) -> None:
        LOGGER.info(
            "REST minute summary requests=%d weight=%d "
            "last_x_mbx_used_weight_1m=%s",
            self._summary_requests,
            self._summary_weight,
            self._last_used_weight_1m,
        )

    def observe(self, *, status: int | None, headers: Mapping[str, str], now: float,
                now_ms: int | None = None) -> None:
        if now_ms is not None:
            if now_ms < 0:
                raise ValueError("UTC milliseconds must be non-negative")
            self._advance_summary_bucket(now_ms // 60_000)
        if status == 418:
            self._banned = True
        for key, value in headers.items():
            if key.lower() in {"x-mbx-used-weight-1m", "x-mbx-used-weight"}:
                try:
                    used = int(value)
                except ValueError:
                    self._banned = True
                    raise RestCircuitOpen("invalid shared used-weight header") from None
                if used < 0:
                    self._banned = True
                    raise RestCircuitOpen("negative shared used-weight header")
                self._shared_used = max(self._shared_used, used)
                self._shared_observed_at = now
                if key.lower() == "x-mbx-used-weight-1m":
                    self._last_used_weight_1m = used

    def retry(self, *, status: int | None, attempt: int,
              headers: Mapping[str, str]) -> RetryDecision:
        if self._banned or status == 418:
            self._banned = True
            raise RestCircuitOpen("venue ban; no retry")
        if status == 429:
            if attempt >= 3:
                self._banned = True
                raise RestCircuitOpen("HTTP 429 retry exhaustion; collector must stop")
            raw = next((v for k, v in headers.items() if k.lower() == "retry-after"), "1")
            try:
                delay = float(raw)
            except ValueError:
                delay = 60.0
            return RetryDecision(True, min(120.0, max(1.0, delay)))
        if attempt >= 3:
            return RetryDecision(False, 0)
        if status is None or 500 <= status < 600:
            return RetryDecision(True, float(min(30, 2 ** (attempt - 1))))
        return RetryDecision(False, 0)
