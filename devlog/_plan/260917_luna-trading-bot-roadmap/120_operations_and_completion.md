# 120 — 운영, 장애 복구, 최종 완료 판정

## 목표

추천 scanner, Guardian, Freqtrade sidecar가 서로의 권한을 침범하지 않고 운영·중단·복구되는지 통합 검증한다.

## Task L120-01 — deployment topology와 secret isolation

**선행 조건:** recommendation complete, Guardian target mode complete, auto-entry target mode complete

**허용 변경**

- MODIFY `docker-compose.yml` or deployment owners only after current deployment convention inspection
- NEW `deployment/position-guardian/` if this path is current authority
- MODIFY `docs/OPERATIONS.md`
- NEW `docs/TRADING_SERVICES_RUNBOOK.md`

**범위**

- process별 service user, env, DB, network egress, restart policy를 분리한다.
- scanner에는 private credential이 없다.
- Guardian key와 auto-entry key는 서로 다르다.
- active DB/order ledger를 Google Drive 동기화 경로에서 실행하지 않는다.

## Task L120-02 — observability와 alert routing

**선행 조건:** L120-01

**허용 변경**

- MODIFY existing observability owners
- NEW service-specific health tests
- MODIFY `docs/TRADING_SERVICES_RUNBOOK.md`

**health dimensions**

- scanner data freshness/reconnect/gap/outbox
- recommendation projection/delivery/expiry
- Guardian account/context freshness/reconcile/order uncertainty/protection status
- Freqtrade heartbeat/open position/stop presence/risk budget

operational health와 strategy efficacy를 별도 지표로 둔다.

## Task L120-03 — backup, restore, disaster drill

**선행 조건:** L120-02

**허용 변경**

- NEW `tools/trading_state_backup.py`
- NEW `tools/trading_state_restore_verify.py`
- NEW tests with temporary DBs
- MODIFY runbook

**범위**

- config hash와 ledger snapshot을 secret 없이 백업한다.
- restore는 exchange reconciliation 전 write를 금지한다.
- corrupted/missing backup, newer schema, partial restore를 fail closed한다.

## Task L120-04 — composite failure campaign

**선행 조건:** L120-03

**허용 변경**

- NEW `tests/system/test_trading_services_failures.py`
- NEW `tests/fixtures/system_trading/`
- NEW deterministic campaign receipt

**시나리오**

- scanner down / Guardian up
- Guardian down / exchange stop remains
- DB unavailable
- duplicate/reordered public and private events
- Discord unavailable
- clock jump
- process restart during order uncertainty
- manual full close while all services recover

**완료 조건**

- duplicate recommendation/order가 없다.
- unknown state에서 exposure가 증가하지 않는다.
- recovery 전 stale recommendation/intent를 실행하지 않는다.

## Task L120-05 — 최종 검증과 문서 동기화

**선행 조건:** L120-04

**허용 변경**

- MODIFY `README.md`
- MODIFY `docs/ARCHITECTURE.md`
- MODIFY `docs/SIGNAL_SPEC.md`
- MODIFY operations/runbooks
- 기록용 completion receipt

**명령**

```powershell
uv run ruff check .
uv run pyright
uv run pytest -q
python -m compileall -q src tests
```

sidecar는 pin된 Freqtrade 환경에서 별도 test/lookahead/recursive 검증을 실행하고 버전과 exit code를 receipt에 남긴다.

## 완료 행렬

| Capability | 완료 조건 | 독립 승격 |
|---|---|---|
| 추천 | canonical LONG/SHORT/NO_ENTRY, persistence/API/Discord, closed-candle/dedupe 검증 | 주문 권한 없음 |
| Guardian shadow | opt-in position, restart ledger, deterministic intents, write 0건 | testnet만 다음 |
| Guardian testnet | timeout reconciliation, replace safety, exposure increase 0 | live 보호는 별도 승인 |
| Guardian live | 지정 포지션 canary, uncertainty 0, bounded unprotected duration | auto-entry 권한 없음 |
| Freqtrade parity | lookahead/recursive PASS, mismatch 설명 | dry-run만 다음 |
| Auto paper | risk cap 위반 0, cost/regime evidence, restart continuity | testnet만 다음 |
| Auto testnet | account ownership, stop presence, uncertainty 0 | production은 별도 승인 |

## 최종 stop condition

요청한 시스템은 추천 기능과 Guardian의 선택된 운영 단계가 실제로 작동하고, auto-entry가 승인된 단계까지 검증됐을 때 완성으로 판정한다. 연구 결과가 기준을 통과하지 못하면 기능을 억지로 활성화하지 않고 `NO PROMOTION`을 정상적인 최종 결과로 남긴다.
