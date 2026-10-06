# 100 — 한국 밤·새벽 초소액 자동 진입 paper 단계

## 목표

한국 시간 운영 창에서만 활성화되는, risk-budgeted paper entry를 만든다. 시간대 자체는 alpha가 아니며 canonical recommendation과 독립 승격 근거가 필요하다.

## Task L100-01 — session policy와 시간대 분석

**선행 조건:** L30 phase gate, full L35 phase gate through L35-09, L60-08 policy-selection receipt, L70-08 Guardian testnet qualification, L90-07 parity PASS

L35 promotion/wiring receipt와 L90-07 parity receipt의 candidate/config hashes가 byte-for-byte 같아야 한다. 서로 다른 version의 PASS 두 개를 조합하면 fail closed한다.

**허용 변경**

- NEW `integrations/freqtrade/auto_entry/session_policy.py`
- NEW `integrations/freqtrade/auto_entry/tests/test_session_policy.py`
- NEW `integrations/freqtrade/AUTO_ENTRY_PROTOCOL.md`

**범위**

- 저장과 비교는 UTC ms, 표시와 운영 창 계산은 `Asia/Seoul`이다.
- DST가 없는 KST라도 UTC 날짜 rollover를 테스트한다.
- 운영 창 밖 recommendation은 기록하되 entry하지 않는다.
- 전체 시간과 야간 subset 성능을 같이 보고 cherry-pick을 막는다.

## Task L100-02 — pure risk budget sizing

**선행 조건:** L100-01

**허용 변경**

- NEW `integrations/freqtrade/auto_entry/risk.py`
- NEW `integrations/freqtrade/auto_entry/tests/test_risk.py`

**계산**

`quantity = allowed_loss / (stop_distance + fee_and_slippage_allowance_per_unit)`

그 뒤 tick/step/min-notional, leverage, gross exposure, max concurrent positions, correlated-symbol cap, daily loss cap을 적용한다.

**불변식**

- min order 때문에 allowed loss를 넘으면 skip한다.
- stop이 없거나 방향상 잘못됐으면 skip한다.
- DCA, martingale, averaging down은 없다.
- quantity rounding이 risk를 늘리면 아래로 내리거나 skip한다.

**완료 조건**

- long/short, zero/near-zero stop distance, min notional, rounding, fee shock, daily cap boundary property test가 있다.

## Task L100-03 — recommendation admission gate

**선행 조건:** L100-02, full L35 phase gate through L35-09, exact L60-08 selected policy

**허용 변경**

- NEW `integrations/freqtrade/auto_entry/admission.py`
- NEW `integrations/freqtrade/auto_entry/tests/test_admission.py`

**필수 gate**

- unexpired `ENTRY_CANDIDATE`
- allowed market/symbol/direction
- promotion-eligible strategy version
- fresh protection context와 valid initial stop
- session window
- candidate contract가 요구하는 fresh spread/liquidity/execution-quality evidence
- portfolio risk budget

`PUMP_RISK`, `CRASH_RISK`, informational pullback, score만 높은 SETUP은 entry하지 않는다.

execution-quality evidence가 unavailable/stale하거나 threshold가 candidate contract와 일치하지 않으면 `NO_ENTRY`다. 시장별 threshold는 admission 결과를 보기 전에 versioned contract로 고정한다.

## Task L100-04 — Freqtrade paper strategy

**선행 조건:** L100-03, L90-07

**허용 변경**

- NEW `integrations/freqtrade/user_data/strategies/SignalbotOvernightPaperStrategy.py`
- NEW `integrations/freqtrade/config-auto-entry-dryrun.json`
- NEW sidecar strategy tests
- MODIFY `integrations/freqtrade/README.md`

**범위**

- `dry_run=true`, static allowlist, tiny dry-run wallet, bounded open trades.
- initial stop과 custom trailing policy를 explicit version으로 묶는다.
- recommendation ID와 Freqtrade trade ID를 연결한다.

## Task L100-05 — paper ledger와 비교 report

**선행 조건:** L100-04

**허용 변경**

- NEW `integrations/freqtrade/auto_entry/report.py`
- NEW `integrations/freqtrade/auto_entry/tests/test_report.py`
- NEW `integrations/freqtrade/AUTO_ENTRY_PAPER_RUNBOOK.md`

**지표**

- admitted/rejected reason census
- after-cost return, drawdown, tail loss, MFE/MAE, giveback
- overnight vs all-hours reference
- expected vs actual dry-run fill difference
- Guardian-style stop update behavior

이 task는 report builder와 deterministic fixture를 구현하며 실제 campaign verdict를 내리지 않는다.

## Task L100-06 — external overnight paper campaign

**선행 조건:** L100-05 PASS, exact candidate/policy/session/risk contract hashes 확인

**허용 변경**

- source/config/threshold 수정 금지
- NEW campaign checkpoints, run manifest, terminal receipt only

**범위**

- preregistered minimum sample/regime window까지 paper/dry-run을 실행한다.
- restart continuity, rejected-reason census, execution-quality availability를 함께 기록한다.

## Task L100-07 — paper campaign adjudication

**선행 조건:** L100-06 terminal receipt

**허용 변경**

- NEW versioned paper qualification report and independent review
- source/config/threshold 수정 금지

**완료 조건**

- sample/regime, after-cost, tail risk, risk-cap violation, restart continuity 조건을 preregistration 그대로 판정한다.
- exact L35/L60/L90/session/risk hashes를 하나의 receipt에 묶는다.
- 기준 미달이면 `NO AUTO-ENTRY PROMOTION`으로 닫는다.

## Phase gate

L100-07 PASS가 필요하다. 기간만 채우면 통과하지 않는다. 사전 등록한 최소 표본, 여러 시장 국면, 비용 민감도, risk-cap 위반 0, restart continuity가 필요하다. 기준 미달이면 전략을 탈락시키며 threshold를 뒤늦게 낮추지 않는다.
