# 090 — Freqtrade 비교·parity·dry-run

## 목표

Freqtrade를 BOT-2의 실행 주인으로 바꾸지 않고, 전략 비교와 자동 진입 실험을 빠르게 반복하는 sidecar로 사용한다. 차이를 숨기지 않고 측정한다.

## Task L90-00 — Google Drive cold archive staging

**선행 조건:** L35-02의 exact candidate/config version receipt와 data manifest가 고정됨

**읽을 파일**

- `BOT2_COLD_STORAGE_CAMPAIGN_CLOSURE_V1.md`
- `BOT2_STORAGE_TIER_POLICY_V1.md`
- `docs/STORAGE_RETENTION_POLICY_V2.md`

**허용 변경**

- NEW `tools/stage_research_data.py`
- NEW `tests/unit/test_research_data_staging.py`
- NEW `docs/RESEARCH_DATA_STAGING.md`

**흐름**

`experiment manifest -> required partition list -> local cache census -> missing-only Drive restore -> payload hash verification -> read-only analysis -> bounded cache eviction`

**불변식**

- 전체 Drive를 다시 동기화하지 않는다.
- 원본 hash, 변환 code version, timezone, symbol mapping, gap rule을 staging receipt에 기록한다.
- active SQLite/order ledger/model runtime을 Drive mount에서 실행하지 않는다.
- worker 수는 CPU 수가 아니라 실측 memory envelope로 제한한다.

**완료 조건**

- already-local, missing remote, hash mismatch, interrupted restore, cache limit boundary test가 있다.
- 실제 remote restore는 별도 운영 실행이며 unit test는 fake manifest/fixture만 사용한다.

## Task L90-01 — canonical export dataset

**선행 조건:** L20-03, L35-02 exact candidate receipt, L90-00

**읽을 파일**

- `integrations/freqtrade/README.md`
- `integrations/freqtrade/user_data/strategies/SignalParityFuturesStrategy.py`
- `src/signalbot/backtest/`
- `docs/BACKTEST_SPEC.md`

**허용 변경**

- NEW `src/signalbot/backtest/freqtrade_export.py`
- NEW `tools/export_freqtrade_parity.py`
- NEW `tests/unit/test_freqtrade_export.py`
- NEW `integrations/freqtrade/PARITY_CONTRACT.md`

**export**

- exact closed candle OHLCV와 strict-prior context
- canonical recommendation/action/blocker
- source data hash, config hash, code/rule version
- next-open execution marker와 explicit unavailable fields

**완료 조건**

- future row/open candle가 export되지 않는다.
- symbol/time keys가 unique하고 rerun bytes/hash가 deterministic하다.
- Drive 원본은 필요 단위만 복원하며 active DB를 Drive 경로에서 실행하지 않는다.

## Task L90-02 — indicator/signal parity comparator

**선행 조건:** L90-01

**허용 변경**

- NEW `integrations/freqtrade/tools/compare_signalbot_parity.py`
- NEW `integrations/freqtrade/tests/test_parity_comparator.py`
- MODIFY `integrations/freqtrade/PARITY_CONTRACT.md`

**비교 층**

- candle alignment
- EMA/Wilder warmup 차이
- feature availability
- raw trigger
- gate pass/fail
- final recommendation
- modeled entry/exit/cost

**완료 조건**

- mismatch를 `EXPECTED_MODEL_DIFFERENCE`, `DATA_ALIGNMENT_ERROR`, `IMPLEMENTATION_BUG`, `UNAVAILABLE_EVIDENCE`로 분류한다.
- 합의하지 않은 tolerance로 mismatch를 지우지 않는다.

## Task L90-03 — sidecar strategy adapter

**선행 조건:** L90-02

**허용 변경**

- MODIFY `integrations/freqtrade/user_data/strategies/SignalParityFuturesStrategy.py`
- NEW focused sidecar tests
- MODIFY static validation config only

**범위**

- 가능한 범위에서 canonical closed-candle semantics와 맞춘다.
- scanner에만 있는 aggTrade/BBO/receipt evidence는 proxy로 꾸미지 않고 unavailable로 둔다.
- `dry_run`은 true로 고정한다.

## Task L90-04 — parity campaign harness와 receipt schema

**선행 조건:** L90-03

**허용 변경**

- NEW `integrations/freqtrade/run_parity_campaign.ps1`
- NEW `integrations/freqtrade/PARITY_RUNBOOK.md`
- NEW deterministic receipt schema/tests

**범위**

- backtest/lookahead/recursive/comparator 단계와 terminal status를 orchestration한다.
- external Freqtrade run 없이 fake command results로 receipt builder를 검증한다.

## Task L90-05 — deterministic sidecar fixture validation

**선행 조건:** L90-04

**허용 변경**

- NEW/modify `integrations/freqtrade/tests/` fixtures and harness tests

**검증**

- command failure/timeout, malformed result, lookahead finding, recursive instability, mismatch census를 fixture로 재생한다.
- 같은 inputs는 byte-stable receipt를 만든다.

## Task L90-06 — external Freqtrade campaign execution

**선행 조건:** L90-05 PASS, pin된 Freqtrade version 확인

**허용 변경**

- source/config 수정 금지
- NEW campaign output/terminal receipt only

**검증**

- Freqtrade backtesting
- lookahead-analysis
- recursive-analysis
- BOT-2 comparator
- fees, adverse slippage, funding sensitivity

**실패 분기**

lookahead, recursive instability, unexplained decision mismatch가 있으면 자동 진입 후보로 승격하지 않는다.

## Task L90-07 — parity receipt adjudication

**선행 조건:** L90-06 terminal receipt

**허용 변경**

- NEW versioned parity report and independent review
- source/config 수정 금지

**완료 조건**

- candidate/config/data/Freqtrade/BOT-2 hashes가 모두 terminal receipt에 묶인다.
- unexplained mismatch, lookahead, recursive instability가 하나라도 있으면 FAIL이다.

## Task L90-08 — FreqAI 격리 판정

**선행 조건:** L90-07

**허용 변경**

- MODIFY `integrations/freqtrade/README.md`
- NEW `integrations/freqtrade/FREQAI_PROMOTION_CHECKLIST.md`

**범위**

- FreqAI는 challenger 연구로만 둔다.
- data leakage, label timing, walk-forward, feature availability, prospective shadow receipt가 모두 있어야 deterministic baseline과 비교한다.
- model score를 확률이나 auto-entry 승인으로 바로 연결하지 않는다.

## Phase gate

L90-07 PASS 뒤에만 Freqtrade dry-run 결과를 100의 후보 입력으로 쓸 수 있다. 수동 Guardian 계좌를 공유하지 않는다.
