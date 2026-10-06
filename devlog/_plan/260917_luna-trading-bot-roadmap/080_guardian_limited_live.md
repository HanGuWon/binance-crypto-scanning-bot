# 080 — 지정 포지션의 제한적 live 보호

## 진입 조건

이 phase는 자동 실행 대상이 아니다. 사용자가 070의 구체적인 testnet 결과를 검토한 뒤 “live protection 구현/활성화”를 명시적으로 승인해야 시작한다.

## 목표

한 계정의 allowlist된 소수 포지션에 stop-only Guardian을 단계적으로 적용한다. 새 진입과 position increase 기능은 계속 금지한다.

## Task L80-01 — 2단계 arming과 capability manifest

**선행 조건:** explicit user authorization, L70-08 PASS

**허용 변경**

- MODIFY `src/position_guardian/config.py`
- NEW `src/position_guardian/arming.py`
- NEW `tests/guardian/test_arming.py`
- NEW `docs/GUARDIAN_LIVE_RUNBOOK.md`

**arming**

- config의 `live_protection`과 짧은 수명의 operator-generated arm artifact가 둘 다 필요하다.
- artifact는 account alias, exact symbols/sides, max quantity, expiry, contract/policy hash를 묶는다.
- 재시작 후 만료된 arm artifact는 자동 연장되지 않는다.

## Task L80-02 — 분리된 kill switches

**선행 조건:** L80-01

**허용 변경**

- NEW `src/position_guardian/controls.py`
- MODIFY `src/position_guardian/runtime.py`
- NEW `tests/guardian/test_guardian_controls.py`

**controls**

- stop new intents
- stop exchange writes
- release one managed position
- shutdown process

이미 확인된 보호 주문을 kill switch가 자동 취소하지 않는다. 취소는 별도 명시 action과 현재 account reconciliation을 요구한다.

## Task L80-03 — canary 운영

**선행 조건:** L80-02

**허용 변경**

- NEW `tools/guardian_live_preflight.py`
- NEW `tools/guardian_live_status.py`
- MODIFY `docs/GUARDIAN_LIVE_RUNBOOK.md`

**순서**

1. 한 계정, 한 symbol/side, 한 포지션.
2. observe와 shadow 결과를 live candidate와 나란히 확인.
3. 최초 live amend 1회를 operator가 확인.
4. 정해진 관측 수와 restart drill 전에는 범위를 늘리지 않음.

**stop 조건**

- unexpected exposure delta
- unresolved uncertain order
- stale account/context beyond limit
- position mode drift
- duplicate protective order conflict
- DB integrity failure

## Task L80-04 — limited-live qualification report

**선행 조건:** L80-03 canary 완료

**허용 변경**

- NEW `tools/qualify_guardian_live.py`
- NEW versioned receipt/report schema and tests

**지표**

- intended/confirmed stop updates
- failed/uncertain/reconciled counts
- maximum unprotected duration
- stale-context suspension duration
- manual intervention count
- exposure increase violations, required value 0

## Phase gate

Guardian live pass는 auto-entry를 승인하지 않는다. Guardian은 계속 지정 포지션 stop-only 서비스로 남는다.
