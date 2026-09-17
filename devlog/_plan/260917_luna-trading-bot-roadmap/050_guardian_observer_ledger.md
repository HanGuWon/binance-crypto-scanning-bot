# 050 — Guardian 관측 서비스와 재시작 원장

## 목표

별도 process가 private account를 읽어 사용자가 명시한 수동 USD-M Futures 포지션만 관측한다. 이 단계는 exchange write를 하지 않는다.

## Task L50-01 — 별도 package와 CLI scaffold

**선행 조건:** L10-03, L40-04

**허용 변경**

- NEW `src/position_guardian/__init__.py`
- NEW `src/position_guardian/cli.py`
- NEW `src/position_guardian/config.py`
- NEW `src/position_guardian/runtime.py`
- NEW `tests/guardian/test_guardian_config.py`
- MODIFY `pyproject.toml`
- MODIFY `.env.example`

**설계**

- entry point: `position-guardian`
- default mode: `observe`
- 별도 database URL, API key env names, account alias를 사용한다.
- `signalbot`은 `position_guardian`을 import하지 않는다.
- config validation은 secret 값을 repr/log에 노출하지 않는다.

**완료 조건**

- secret 누락 observe/read mode, write-capable mode의 필수 설정, invalid mode, URL redaction test가 있다.
- `position-guardian validate-config`와 `position-guardian run --dry-run`이 네트워크 없이 끝난다.

## Task L50-02 — private read client protocol과 recorded fixtures

**선행 조건:** L50-01

**중요:** 구현 시점의 Binance 공식 USD-M 문서를 다시 확인하고 확인 날짜와 링크를 `docs/POSITION_GUARDIAN_BINANCE_CONTRACT.md`에 기록한다.

**허용 변경**

- NEW `src/position_guardian/exchange/protocol.py`
- NEW `src/position_guardian/exchange/binance_read.py`
- NEW `src/position_guardian/exchange/signing.py`
- NEW `tests/fixtures/binance_private/`
- NEW `tests/guardian/test_binance_read.py`
- NEW `docs/POSITION_GUARDIAN_BINANCE_CONTRACT.md`

**범위**

- server time, account position, position mode, open conditional/protective orders, symbol filters를 read한다.
- retry는 bounded하고 cancellation path가 있다.
- HTTP unit test는 recorded JSON만 사용한다.
- signing canonicalization은 golden vector로 검증한다.

**실패 분기**

- clock skew, 429/418, auth error, malformed number, unknown mode를 terminal/temporary로 구분한다.
- 응답 해석이 불명확하면 position을 관리 대상으로 만들지 않는다.

## Task L50-03 — opt-in managed position identity

**선행 조건:** L50-02

**허용 변경**

- NEW `src/position_guardian/domain.py`
- NEW `src/position_guardian/adoption.py`
- NEW `tests/guardian/test_adoption.py`

**identity**

- account alias + symbol + position side + adoption generation
- user allowlist와 현재 exchange snapshot이 일치해야 adoption candidate가 된다.
- `original_risk_stop`과 `protection_floor`를 구분한다. 이미 큰 이익인 포지션은 protection floor가 entry를 넘어설 수 있다.
- live adoption에는 exchange에서 확인된 active stop 또는 사용자가 명시한 protection floor가 있어야 한다. 원래 진입 stop을 모르면 `original_risk_stop`은 unknown으로 보존한다.
- side flip, zero quantity, 여러 보호 주문 충돌은 자동 인수하지 않는다.

**완료 조건**

- one-way long/short, Hedge LONG/SHORT, no stop, duplicate stop, partial position, flipped side, 이미 이익인 long의 entry 위 floor와 short의 entry 아래 floor test가 있다.
- 신규 포지션 생성 API는 존재하지 않는다.

## Task L50-04 — restart-safe Guardian ledger

**선행 조건:** L50-03

**허용 변경**

- NEW `src/position_guardian/persistence/models.py`
- NEW `src/position_guardian/persistence/repository.py`
- NEW `tests/guardian/test_guardian_repository.py`

**기록**

- observed account snapshots
- adoption/release state
- highest/lowest since adoption
- active protection identity
- planned intent와 execution receipt
- reconciliation cursor와 uncertainty state

**불변식**

- append-only event와 current projection을 구분한다.
- same event no-op, same ID conflict hard failure다.
- state가 없는 재시작은 exchange를 다시 읽기 전 아무것도 관리하지 않는다.

## Task L50-05 — read-only reconciliation loop

**선행 조건:** L50-04

**허용 변경**

- NEW `src/position_guardian/reconcile.py`
- MODIFY `src/position_guardian/runtime.py`
- NEW `tests/guardian/test_reconcile_readonly.py`

**state**

`DISCOVERED -> ADOPTABLE -> OBSERVED -> MANAGED_SHADOW -> DEGRADED | RELEASED | CLOSED`

**필수 시나리오**

- manual partial close: quantity projection 축소
- manual add: 자동 보호 수량 확대 금지, operator attention
- side flip: 이전 관리 종료, 새 generation은 재승인
- exchange order missing: shadow에서는 alert만
- duplicate account event와 REST poll race: ingest cursor로 deterministic 처리

## Task L50-06 — private event stream과 REST 재동기화

**선행 조건:** L50-05

**허용 변경**

- NEW `src/position_guardian/exchange/binance_user_stream.py`
- MODIFY `src/position_guardian/reconcile.py`
- NEW `tests/guardian/test_binance_user_stream.py`
- NEW recorded stream fixtures
- MODIFY `docs/POSITION_GUARDIAN_BINANCE_CONTRACT.md`

**범위**

- 구현 시점 공식 문서에 맞는 private account/order event stream을 사용한다.
- startup과 reconnect 직후 REST snapshot을 권위 있는 재동기화 기준으로 삼는다.
- event queue, dedupe cache, reconnect backoff는 bounded하다.
- stream gap, listen/session expiry, out-of-order event는 DEGRADED로 보내고 REST reconcile 전 write eligibility를 주지 않는다.

**조건부 경로**

testnet이나 현재 계정에서 private stream 계약을 신뢰할 수 없으면 bounded REST polling을 명시적 degraded mode로 유지한다. degraded mode의 poll 간격보다 빠른 stop 갱신을 약속하지 않는다.

**완료 조건**

- disconnect/reconnect, duplicate, reordered event, REST race, queue overflow test가 있다.
- 여전히 exchange write call은 0이다.

## Phase gate

24시간 read-only soak 또는 deterministic replay에서 write call count가 정확히 0이고, restart/partial close/side flip/stream reconnect reconciliation test가 통과해야 060으로 간다.
