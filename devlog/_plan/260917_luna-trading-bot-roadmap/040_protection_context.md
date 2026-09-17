# 040 — 닫힌 봉 기반 보호 context

## 목표

private position을 scanner에 노출하지 않고도 Guardian이 stop 후보를 계산할 수 있도록, 공개 데이터에서만 만들어지는 versioned `ProtectionContext`를 발행한다.

## Task L40-01 — ProtectionContext domain contract

**선행 조건:** L10-02

**읽을 파일**

- `src/signalbot/domain/models.py`
- `src/signalbot/signals/positions.py`
- `src/signalbot/signals/position_management.py`
- `src/signalbot/indicators/`
- `docs/SIGNAL_SPEC.md`

**허용 변경**

- NEW `src/signalbot/signals/protection_context.py`
- NEW `tests/unit/test_protection_context.py`
- MODIFY `src/signalbot/signals/__init__.py` if exports are used there

**필드**

- market, symbol, primary interval
- closed candle open/close time
- close, ATR, confirmed swing support/resistance when available
- trend state and consecutive trend-failure count input
- data completeness and context freshness
- source decision clock ID, context version, deterministic context ID

**불변식**

- closed candle만 허용한다.
- higher-timeframe values는 strict-prior mature snapshot만 허용한다.
- 가격은 양수, timestamp는 UTC ms, ID는 payload에서 결정한다.
- unavailable structure를 임의 값으로 채우지 않는다.

**완료 조건**

- positive, open-candle negative, stale-HTF negative, no-structure boundary, deterministic ID test가 있다.

## Task L40-02 — context builder 연결

**선행 조건:** L40-01

**허용 변경**

- MODIFY the existing feature/scanner owner selected after symbol tracing
- MODIFY `src/signalbot/scanner.py` only at the closed-candle decision boundary
- NEW focused integration tests

**범위**

- recommendation과 같은 decision clock에서 context를 만든다.
- context 생성 실패가 signal을 변조하지 않는다. 단, Guardian용 context는 발행하지 않고 오류 지표를 남긴다.
- reconnect duplicate와 gap replay가 같은 ID를 만든다.

**완료 조건**

- feature 계산을 복제하지 않고 기존 owner를 compose한다.
- scanner의 private import와 account symbol 개념은 0건이다.

## Task L40-03 — bounded persistence와 read-only surface

**선행 조건:** L40-02

**허용 변경**

- MODIFY `src/signalbot/persistence/models.py`
- MODIFY `src/signalbot/persistence/repository.py`
- MODIFY `src/signalbot/api/app.py`
- MODIFY repository/API tests

**범위**

- immutable context event와 symbol별 latest pointer를 저장한다.
- Guardian이 `after_context_id` 또는 closed-candle cursor로 안전하게 poll할 수 있게 한다.
- 한 응답과 DB retention을 bounded하게 한다.
- 동일 ID payload conflict를 fail closed한다.

**실패 분기**

- API/DB가 unavailable이면 Guardian은 마지막 확인 stop을 유지하고 새 intent를 만들지 않는다.
- latest context가 configured age를 넘으면 `STALE_CONTEXT`로 취급한다.

**검증**

```powershell
uv run pytest -q tests/unit/test_protection_context.py tests/unit/test_repository.py
uv run ruff check src/signalbot/signals/protection_context.py src/signalbot/persistence src/signalbot/api
uv run pyright src/signalbot/signals/protection_context.py src/signalbot/persistence src/signalbot/api
```

## Task L40-04 — context compatibility contract fixture

**선행 조건:** L40-03

**허용 변경**

- NEW `tests/fixtures/protection_context/`
- NEW `tests/contract/test_protection_context_contract.py`
- MODIFY `docs/TRADING_EVENT_CONTRACTS.md`

**범위**

- current schema fixture와 unknown-field/old-version behavior를 고정한다.
- Guardian이 지원하지 않는 major version을 거부하도록 소비자 계약을 명시한다.

## Phase gate

fully closed candle, restart, duplicate replay, stale context가 모두 검증될 때까지 Guardian 정책 계산을 연결하지 않는다.
