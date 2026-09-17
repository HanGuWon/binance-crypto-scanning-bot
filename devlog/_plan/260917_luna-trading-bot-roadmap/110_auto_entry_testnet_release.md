# 110 — 초소액 자동 진입 testnet과 production 전 문턱

## 목표

자동 진입 전용 계정/서브계정에서 Freqtrade testnet 실행과 운영 안전성을 검증한다. production entry는 이 phase의 산출물이 아니며 별도 사용자 승인이 필요하다.

## Task L110-01 — account ownership contract

**선행 조건:** 100 phase gate PASS

**허용 변경**

- NEW `integrations/freqtrade/AUTO_ENTRY_ACCOUNT_OWNERSHIP.md`
- MODIFY `docs/TRADING_CAPABILITY_MATRIX.md`

**고정 사항**

- Guardian manual-position account와 auto-entry account/subaccount를 분리한다.
- 한 자동 계정의 포지션·주문은 Freqtrade만 소유한다.
- 수동 거래를 같은 account에 섞지 않는다.
- API key는 withdrawal 권한 없이 필요한 최소 trade 권한만 가진다.

## Task L110-02 — testnet config와 preflight

**선행 조건:** L110-01

**허용 변경**

- NEW `integrations/freqtrade/config-auto-entry-testnet.example.json`
- NEW `integrations/freqtrade/tools/auto_entry_preflight.py`
- NEW related tests
- MODIFY `.env.example`

**preflight**

- endpoint/network identity
- account mode/leverage/margin settings
- exact allowlist
- min quantity/notional/tick
- risk caps and dry-run false only on testnet
- no secret in generated receipt

## Task L110-03 — testnet qualification harness와 stop deadline contract

**선행 조건:** L110-02

**허용 변경**

- NEW `integrations/freqtrade/AUTO_ENTRY_TESTNET_RUNBOOK.md`
- NEW `integrations/freqtrade/tools/qualify_auto_entry_testnet.py`
- NEW receipt schema/tests

**결과 열람 전 고정**

- entry acknowledgement부터 protective stop ACTIVE까지의 exact maximum milliseconds
- deadline 전 reconcile cadence와 retry cap
- deadline breach 때 emergency close/cancel-new-entry/operator escalation 순서
- zero-breach promotion rule

**범위**

- external testnet call 없이 scenario registry와 terminal receipt builder를 구현한다.

## Task L110-04 — deterministic execution fixture qualification

**선행 조건:** L110-03

**허용 변경**

- NEW/modify recorded testnet fixtures and qualification tests

**시나리오**

- one long, one short
- rejected stale recommendation
- minimum-order skip
- initial stop placement
- trailing update
- partial fill, cancel, restart
- daily loss kill switch
- network timeout and reconciliation

**완료 조건**

- fixture에서 설정 외 symbol/size의 entry가 0건이다.
- stop deadline breach가 emergency action과 terminal FAIL을 만든다.
- rerun receipt가 byte-stable이다.

## Task L110-05 — external auto-entry testnet campaign

**선행 조건:** L110-04 PASS, operator가 전용 testnet credential을 준비함

**허용 변경**

- source/config 수정 금지
- NEW machine-local campaign outputs and terminal receipt only

**범위**

- L110-03 scenario registry를 testnet에서 실행한다.
- 설정 외 symbol/size의 entry, stop deadline breach, risk-cap breach가 보이면 즉시 new-entry를 중단한다.

## Task L110-06 — testnet receipt adjudication

**선행 조건:** L110-05 terminal receipt

**허용 변경**

- NEW versioned qualification report and independent review
- source/config 수정 금지

**완료 조건**

- testnet에서 설정 외 symbol/size의 entry가 0건이다.
- stop deadline breach가 0건이다.
- unresolved execution uncertainty가 있으면 campaign은 fail이다.
- exact strategy/config/account-mode/risk-contract hashes가 묶인다.

## Task L110-07 — production proposal 생성기

**선행 조건:** L110-06 PASS

**허용 변경**

- NEW `integrations/freqtrade/tools/build_auto_entry_promotion_proposal.py`
- NEW proposal schema/tests

**proposal 내용**

- exact strategy/config/code/data hashes
- eligible symbols/session/risk caps
- paper/prospective/testnet evidence references
- known limitations와 rollback
- 첫 canary의 최대 허용 손실과 기간/표본 stop conditions

proposal은 주문을 활성화하지 않는다.

## Production gate

사용자가 proposal을 보고 production auto-entry를 명시 승인하기 전 live config/API key 연결/주문은 금지한다. 승인 뒤에도 한 symbol, 한 open trade, 가장 작은 risk budget부터 별도 구현 작업으로 시작한다.
