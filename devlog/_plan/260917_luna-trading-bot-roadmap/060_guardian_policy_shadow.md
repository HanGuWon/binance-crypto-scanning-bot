# 060 — Guardian 정책과 주문 없는 shadow 검증

## 목표

“잠깐 눌림인지 추세 종료인지 맞힌다”는 표현을, 측정 가능한 허용 되돌림 정책으로 바꾼다. 같은 진입·수량에 여러 exit policy를 적용하고, exchange write 없이 `StopUpdateIntent`만 만든다.

## Task L60-01 — policy state model 확장

**선행 조건:** L50 phase gate including L50-06

**읽을 파일**

- `src/signalbot/signals/positions.py`
- `src/signalbot/signals/position_management.py`
- `tests/unit/test_position_exits.py`
- `tests/unit/test_position_management.py`

**허용 변경**

- MODIFY `src/signalbot/signals/position_management.py`
- MODIFY `tests/unit/test_position_management.py`
- NEW `docs/GUARDIAN_STOP_POLICY.md`

**정책 상태**

- `INITIAL_RISK`: 최초 stop 유지
- `TREND_PROGRESS`: ATR/structure 여유 유지
- `PROFIT_PROTECTION`: break-even보다 큰 비용 여유를 둔 이익 보호
- `TREND_WEAKENING`: 확정 구조 훼손과 모멘텀 약화가 겹칠 때만 trail 축소
- `STALE_OR_UNCERTAIN`: 새 갱신 금지

**adoption model 보정**

- 현재 `ManagedPositionSnapshot.initial_stop`은 long에서 entry 아래, short에서 entry 위를 요구한다. 수익 중인 수동 포지션 인수를 위해 `original_risk_stop`, `protection_floor`, `active_stop`을 분리한다.
- original risk가 알려졌을 때만 R 기반 activation을 사용한다. 모르면 R을 추정하지 않고 ATR/confirmed structure 정책만 사용한다.
- 새 stop은 long에서 `max(active_stop, protection_floor, candidate)`, short에서 `min(active_stop, protection_floor, candidate)` 제약을 만족해야 한다.

**불변식**

- long stop은 내려가지 않고 short stop은 올라가지 않는다.
- current price를 넘는 candidate는 정상 amend가 아니다.
- 한 closed candle의 정보로 만든 stop은 같은 candle 내부에 소급 적용하지 않는다.
- 구조 값이 없으면 ATR baseline으로 명시적으로 fallback하거나 intent를 생략한다.

## Task L60-02 — policy selection preregistration

**선행 조건:** L60-01

**허용 변경**

- NEW `docs/GUARDIAN_POLICY_SELECTION_PREREGISTRATION.md`
- NEW versioned machine-readable policy-selection contract under `config/`
- NEW contract identity test

**결과 열람 전 고정할 값**

- primary metric와 tie-break 순서
- maximum drawdown/tail-loss 악화 한도
- maximum premature-stop rate와 MFE giveback
- maximum stop-update frequency와 modeled operational cost
- minimum sample/regime coverage와 bootstrap validity
- same-bar ambiguity와 gap-through 처리

어떤 후보도 hard guardrail을 통과하지 못하면 `NO POLICY PROMOTION`이 결과다. 기존 ATR baseline보다 나쁜 후보를 “적응형”이라는 이유로 고르지 않는다.

**완료 조건**

- exact contract hash와 review receipt가 outcome computation 전에 존재한다.
- threshold 수정은 새 policy-selection version으로만 가능하다.

## Task L60-03 — 동일 진입 exit-policy 비교 harness

**선행 조건:** L60-02

**허용 변경**

- NEW `src/signalbot/backtest/guardian_policy.py`
- NEW `tests/unit/test_guardian_policy_backtest.py`
- MODIFY `src/signalbot/cli.py`
- NEW `docs/GUARDIAN_POLICY_EVALUATION.md`

**비교 정책**

- initial stop only
- current delayed ATR trail
- confirmed swing + ATR trail
- weakening-sensitive adaptive trail

**지표**

- after-cost return, drawdown, MFE giveback, premature stop frequency
- stop update count, gap-through slippage, same-bar ambiguity count
- long/short, volatility/regime, time-of-day strata

**완료 조건**

- 모든 정책은 같은 entry/time/quantity/cost rows를 사용한다.
- target/stop same-bar collision은 win으로 처리하지 않는다.
- 이미 크게 오른 포지션만 골라 cohort를 만들지 않는다.

## Task L60-04 — Guardian context consumer와 shadow intent

**선행 조건:** L60-03, L40-04, L50 phase gate including L50-06

**허용 변경**

- NEW `src/position_guardian/context_client.py`
- NEW `src/position_guardian/planner.py`
- MODIFY `src/position_guardian/runtime.py`
- NEW `tests/guardian/test_shadow_planner.py`

**범위**

- context major version/freshness/closed-candle cursor를 검증한다.
- managed snapshot과 context로 순수 planner를 호출한다.
- intent를 ledger에 저장하지만 exchange write는 하지 않는다.
- 같은 snapshot+context rerun은 같은 intent ID를 만든다.

## Task L60-05 — shadow alerts와 운영 report

**선행 조건:** L60-04

**허용 변경**

- NEW `src/position_guardian/alerts.py`
- NEW `src/position_guardian/report.py`
- NEW `tests/guardian/test_guardian_alerts.py`
- MODIFY `docs/OPERATIONS.md`

**알림**

- would-update stop, stale context, manual size increase, side flip, protection missing, reconciliation uncertain.
- account balance나 credential을 메시지에 넣지 않는다.

## Task L60-06 — restart/fault replay

**선행 조건:** L60-05

**허용 변경**

- NEW `tests/guardian/test_shadow_restart_replay.py`
- NEW `tests/fixtures/guardian_scenarios/`

**시나리오**

- intent persist 직후 crash
- context fetch timeout
- account snapshot stale
- duplicate REST response
- partial close between two contexts
- scanner/API outage and recovery

## Task L60-07 — external shadow policy campaign

**선행 조건:** L60-06 PASS, L60-02 contract hash 확인

**허용 변경**

- source/config/threshold 수정 금지
- NEW campaign checkpoints, run manifest, terminal receipt only

**범위**

- 동일 adoption cohort에 preregistered policy를 실행한다.
- bounded batch/worker/memory와 deterministic resume를 사용한다.
- 결과를 보고 threshold나 cohort를 바꾸지 않는다.

## Task L60-08 — policy selection adjudication

**선행 조건:** L60-07 terminal receipt

**허용 변경**

- NEW versioned policy comparison report, selection receipt, independent review
- source/config/threshold 수정 금지

**완료 조건**

- L60-02의 primary metric, hard guardrail, sample/regime, cost 조건을 그대로 적용한다.
- exact policy/config/data/code hashes를 묶는다.
- 통과 후보가 없으면 `NO POLICY PROMOTION`으로 닫는다.

## Phase gate

L60-08 selection receipt가 있어야 한다. L60-02 contract로만 후보를 고르고 유리한 사례만 보고 threshold를 바꾸지 않는다. 주문 call이 0인 shadow campaign과 restart replay가 통과해야 070으로 간다.
