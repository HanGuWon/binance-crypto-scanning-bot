# 070 — Guardian stop-only testnet 실행

## 목표

Binance USD-M testnet에서 기존 포지션의 보호 stop만 안전하게 관리한다. 신규 진입, 증액, 반전 주문은 API surface 수준에서 금지한다.

## Task L70-01 — 최신 주문 계약 freeze

**선행 조건:** 060 phase gate

**허용 변경**

- MODIFY `docs/POSITION_GUARDIAN_BINANCE_CONTRACT.md`
- NEW `tests/fixtures/binance_private/testnet_contract/`
- NEW `tests/guardian/test_binance_order_contract.py`

**조사 항목**

- conditional/algo order endpoint와 query endpoint
- One-way/Hedge mode의 `reduceOnly`, `closePosition`, `positionSide`, quantity 조합
- client order ID 길이/문자, tick/step/min notional, trigger source, rate limits
- cancel/query/new 순서와 duplicate semantics
- canonical testnet hosts, account/network identity proof, account alias, capability-token format

**완료 조건**

- 공식 문서 확인 날짜와 fixture가 일치한다.
- 불명확한 mode 조합은 unsupported로 잠긴다.
- testnet mode는 production host/account를 hard reject하는 contract를 가진다.

## Task L70-02 — capability-limited write protocol

**선행 조건:** L70-01

**허용 변경**

- NEW `src/position_guardian/exchange/stop_writer.py`
- NEW `src/position_guardian/exchange/binance_stop_writer.py`
- NEW `tests/guardian/test_stop_writer.py`

**API 제한과 runtime gate**

- accepted command는 create protective stop, verified replace, confirmed position closure/release 뒤의 cleanup뿐이다. 보호 주문 단독 취소는 표현할 수 없다.
- BUY/SELL market entry나 position-increasing quantity를 표현할 type가 없어야 한다.
- current account snapshot보다 큰 보호 quantity는 거부한다.
- client identity는 intent ID에서 deterministic하게 파생한다.
- 매 write 전 endpoint identity, testnet account alias, non-expired capability token, arm mode를 재검증한다. 하나라도 다르면 네트워크 call 전에 중단한다.
- 매 write 직전에 fresh REST/account-order reconciliation을 완료하고, position side/quantity/active orders가 intent와 일치하며 Guardian state가 non-degraded임을 증명한다. stale snapshot이나 stream gap 상태에서는 쓰지 않는다.

## Task L70-03 — timeout-safe execution state machine

**선행 조건:** L70-02

**허용 변경**

- NEW `src/position_guardian/execution.py`
- MODIFY `src/position_guardian/persistence/`
- NEW `tests/guardian/test_stop_execution.py`

**state**

`PREPARED -> SUBMITTING -> ACKNOWLEDGED | UNCERTAIN -> RECONCILED -> ACTIVE | FAILED`

**필수 동작**

- timeout 뒤 같은 주문을 바로 다시 보내지 않고 client ID/order query로 실제 접수 여부를 확인한다.
- 동일 execution receipt는 idempotent하다.
- payload conflict는 중단한다.
- response가 성공이어도 order/account snapshot에서 보호 상태가 확인되기 전 `ACTIVE`가 아니다.

## Task L70-04 — 보호 공백을 다루는 replace protocol

**선행 조건:** L70-03

**허용 변경**

- NEW `src/position_guardian/replace.py`
- NEW `tests/guardian/test_stop_replace.py`
- MODIFY `docs/GUARDIAN_STOP_POLICY.md`

**조건부 경로**

- exchange가 두 보호 주문을 안전하게 겹칠 수 있으면 새 주문 확인 후 옛 주문을 취소한다.
- 겹침이 불가능하면 공식 계약과 testnet 증거에 맞는 cancel/replace protocol을 사용하고 unprotected duration을 측정한다.
- 어느 방식도 안전성을 증명하지 못하면 amend를 하지 않고 기존 stop을 유지한다.
- campaign 전에 `max_replace_unprotected_ms`와 bounded reconcile cadence를 고정한다. `cancel succeeded / new uncertain`이 deadline을 넘으면 새 갱신을 모두 중지하고, 기존 intent의 query/recreate recovery만 제한 횟수로 수행한 뒤 operator escalation한다.

**필수 시나리오**

- cancel 성공/new timeout
- new accepted/cancel timeout
- old stop fills during replace
- manual partial close during replace
- duplicate open stops after restart
- price already beyond proposed stop

## Task L70-05 — testnet qualification harness와 receipt schema

**선행 조건:** L70-04

**허용 변경**

- NEW `tools/qualify_guardian_testnet.py`
- NEW `docs/GUARDIAN_TESTNET_RUNBOOK.md`
- NEW receipt schema/tests under `tests/guardian/`

**범위**

- scenario registry, preflight, redacted event capture, terminal receipt builder를 구현한다.
- external testnet call 없이 fixture로 runner state machine을 검증한다.

## Task L70-06 — deterministic fault fixture qualification

**선행 조건:** L70-05

**허용 변경**

- NEW/modify recorded testnet fixtures
- NEW `tests/guardian/test_guardian_testnet_qualification.py`

**시나리오**

- one-way long/short
- Hedge mode supported side matrix
- monotonic repeated trail
- process kill/restart
- network loss before/after ACK
- manual partial close/full close
- stale context and emergency stop-new-writes
- production endpoint/account rejection
- unprotected deadline breach and recovery exhaustion

**완료 조건**

- 모든 fixture rerun이 byte-stable terminal receipt를 만든다.
- 신규 exposure delta와 standalone protection cancellation이 0이다.

## Task L70-07 — external testnet campaign execution

**선행 조건:** L70-06 PASS, operator가 testnet credential을 실행 환경에 준비함

**허용 변경**

- source 수정 금지
- NEW machine-local campaign artifacts/receipts only

**범위**

- runbook의 one-way/Hedge supported matrix, restart, timeout, partial/full close를 testnet에서 실행한다.
- 실행 중 contract drift가 보이면 campaign을 중단하고 L70-01의 새 version으로 돌아간다.

## Task L70-08 — testnet receipt adjudication

**선행 조건:** L70-07 terminal receipt

**허용 변경**

- NEW versioned qualification report and independent review
- source/config 수정 금지

**완료 조건**

- receipt는 order IDs, intent IDs, before/after snapshots, uncertainty resolution, unprotected duration을 secret 없이 기록한다.
- 신규 exposure delta가 0임을 계산한다.
- unresolved uncertainty, deadline breach, unsupported account mode가 하나라도 있으면 FAIL이다.

## Phase gate

L70-08 PASS가 Guardian testnet qualification receipt다. testnet은 주문 정합성만 검증한다. live 보호 구현은 사용자에게 이 receipt와 실패 시나리오 결과를 보여주고 별도 명시 승인을 받은 뒤 시작한다.
