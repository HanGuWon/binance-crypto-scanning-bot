# 035 — 실제 Futures LONG/SHORT 후보 검증과 승격

## 목표

추천 UI가 양방향 단어를 표시하는 것과, 실제로 검증된 Futures LONG/SHORT 후보가 존재하는 것을 구분한다. 현재 frozen policy의 actionability 범위를 먼저 감사하고, 없는 방향은 기존 Phase-R/Phase-S 연구 규칙을 지키는 successor candidate로 검증한다.

## Task L35-01 — 현재 actionable coverage audit

**선행 조건:** L20-02

**읽을 파일**

- `config/settings.example.yaml`
- `config/prospective.*.yaml`
- `docs/SIGNAL_SPEC.md`
- `docs/DIRECTIONAL_SIGNAL_ALGORITHM_REVIEW_AND_RESEARCH_2026-09-02.md`
- `src/signalbot/signals/`
- `src/signalbot/prospective/`
- `src/signalbot/r4b_v2/strategy/`

**허용 변경**

- NEW `docs/DIRECTIONAL_CANDIDATE_COVERAGE.md`
- MODIFY `docs/TRADING_CAPABILITY_MATRIX.md`

**범위**

- Spot LONG/exit-warning, Futures LONG/SHORT, informational-only family를 표로 구분한다.
- 각 셀에 live rule version, retrospective evidence, prospective evidence, promotion status, blocker를 적는다.
- synthetic unit-test support를 live promotion으로 오인하지 않는다.

**완료 조건**

- 현재 frozen R2의 Spot breakout LONG과 Futures breakdown SHORT 범위가 정확히 드러난다.
- Futures LONG 또는 다른 family가 승격되지 않았다면 `UNSUPPORTED/NO_ENTRY`로 표시한다.

## Task L35-02 — successor candidate preregistration

**선행 조건:** L35-01

**허용 변경**

- NEW versioned preregistration under `docs/` using the existing research naming convention
- NEW successor config under `config/` only; existing Phase-R/Phase-S config는 수정 금지
- NEW contract tests for config identity

**범위**

- 현재 코드에 이미 있는 causal family 중 LONG/SHORT 양쪽 후보를 선택한다.
- trigger, gates, invalidation, holding/exit policy, cost model, universe, split, primary metric, multiplicity, minimum evidence를 결과 열람 전에 고정한다.
- mirrored rule이라는 이유만으로 대칭 성능을 가정하지 않는다.

**실패 분기**

기존 Phase-S의 첫 열람 계약이 아직 닫혀 있거나 successor 입력 권위가 불명확하면 새 threshold를 만들지 않고 `WAITING_FOR_AUTHORITY`를 남긴다.

## Task L35-03 — historical authority manifest와 runner schema

**선행 조건:** L35-02

**허용 변경**

- compose existing Phase-R campaign owners; frozen WP0-WP6와 과거 revision 수정 금지
- NEW successor revision manifest/runner schema/tests under the current campaign convention

**범위**

- raw authority, point-in-time universe, native 5m, strict-prior HTF, funding, next-open, technical exit의 identity와 input schema를 기존 owner에 묶는다.
- exact BBO/receipt age/intrabar ordering unavailable은 명시하고 full live gate를 역사 proxy로 통과시키지 않는다.
- 결과를 읽지 않고 run manifest, trial registry, receipt schema를 만든다.

**완료 조건**

- 결과 열람 전에 config/code/data/trial registry hash가 고정된다.

## Task L35-04 — deterministic fixture replay와 parity

**선행 조건:** L35-03

**허용 변경**

- NEW successor replay adapter/tests/recorded small fixtures only
- external broad campaign 실행 금지

**범위**

- signal/non-signal common opportunity panel, dedup, next-open, funding, long/short exit semantics를 작은 fixture로 검증한다.
- selected-signal parity와 rerun hash를 검증한다.

**완료 조건**

- fixture row/hash/dedup/parity receipt가 있고 unexplained mismatch가 0이다.
- future row, unclosed HTF, same-bar optimistic ordering test가 통과한다.

## Task L35-05 — external historical campaign execution

**선행 조건:** L35-04 PASS, data/staging preflight PASS

**허용 변경**

- source/config 수정 금지
- NEW revision-owned campaign checkpoints, run manifest, terminal receipt only

**범위**

- bounded workers/queue/memory admission으로 broad replay를 실행한다.
- valid checkpoint를 재사용하고 full panel을 RAM에 유지하지 않는다.
- 중단·재개·중복 unit을 deterministic하게 처리한다.

## Task L35-06 — cost-aware analysis와 historical adjudication

**선행 조건:** L35-05 terminal receipt PASS

**허용 변경**

- NEW successor analysis revision only
- 기존 frozen result와 Phase-S result 수정 금지

**분석**

- after-cost return, drawdown, tail loss, MFE/MAE, concentration
- six baselines/matched random where preregistered
- temporal/regime replication, bootstrap/FDR/HAC as frozen contract requires
- LONG/SHORT를 별도 보고하고 합산 수치로 약한 방향을 숨기지 않는다.

**완료 조건**

- 결과가 나쁘면 `NO QUALIFIED CANDIDATE`가 정상 verdict다.
- threshold를 결과에 맞춰 바꾸지 않는다. 변경 후보는 새 version/새 trial로만 간다.
- independent review와 result hash를 포함한 historical adjudication receipt를 발행한다.

## Task L35-07 — prospective shadow activation

**선행 조건:** L35-06에서 historical screen 통과, independent review PASS

**허용 변경**

- NEW prospective successor config/campaign manifest
- MODIFY prospective observer only through versioned composition points
- NEW parity/durability tests
- 기존 Phase-S campaign identity와 data 수정 금지

**범위**

- 주문과 Discord promotion 없이 raw opportunity, gate, recommendation candidate, outcome eligibility를 기록한다.
- closed evidence reader, source/config identity, dedupe, restart continuity를 유지한다.

## Task L35-08 — forward evaluation과 promotion receipt

**선행 조건:** preregistered prospective gate가 실제로 열림

**허용 변경**

- NEW evaluation/report/review revision
- result를 보기 전 계약 수정 금지

**완료 조건**

- sample/regime/censor/cost/data-quality 조건을 모두 증명한다.
- efficacy와 delivery/operational health를 별도로 판정한다.
- 방향별 verdict가 `PROMOTE`, `CONTINUE_OBSERVING`, `REJECT` 중 하나다.

## Task L35-09 — promoted rule wiring

**선행 조건:** L35-08의 exact candidate가 `PROMOTE`

**허용 변경**

- NEW rule version/config; 기존 frozen version 수정 금지
- MODIFY scanner rule registry/config selection
- MODIFY recommendation evidence binding
- positive, negative, boundary, anti-lookahead, replay tests

**범위**

- promotion receipt의 exact hashes와 같은 candidate만 활성화한다.
- default config 변경은 별도 운영 review 뒤 수행한다.
- 추천 활성화만 하며 주문 권한은 추가하지 않는다.

## Phase gate

Futures LONG과 SHORT는 각각 promotion receipt가 있어야 `ENTRY_CANDIDATE`로 운영 노출한다. 한 방향만 통과하면 다른 방향은 계속 `NO_ENTRY/UNSUPPORTED`로 남긴다. Freqtrade 결과만으로 이 gate를 대신할 수 없다.
