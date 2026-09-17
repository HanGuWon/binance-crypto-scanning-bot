# 020 — LONG/SHORT/NO_ENTRY 추천 코어

## 목표

현재 `SignalDecision`과 rule evaluation을 거래 명령으로 바꾸지 않고, 사람이 읽고 다른 서비스가 소비할 수 있는 canonical recommendation으로 투영한다.

## Task L20-01 — recommendation domain model

**선행 조건:** L10-02

**읽을 파일**

- `src/signalbot/domain/models.py`
- `src/signalbot/domain/enums.py`
- `src/signalbot/signals/state_machine.py`
- `docs/SIGNAL_SPEC.md`
- `tests/unit/test_state_machine.py`

**허용 변경**

- NEW `src/signalbot/recommendations/__init__.py`
- NEW `src/signalbot/recommendations/models.py`
- NEW `tests/unit/test_recommendation_models.py`

**모델**

- action: `LONG`, `SHORT`, `NO_ENTRY`
- recommendation kind: `ENTRY_CANDIDATE`, `EXIT_WARNING`, `RISK_WARNING`, `HOLD`
- score kind를 고정해 probability 오해를 막는다.
- `NO_ENTRY`는 blocker가 최소 하나 있어야 한다.
- Futures와 Spot action mapping을 validation한다.
- event ID는 source decision과 projection version을 묶어 deterministic하게 만든다.

**완료 조건**

- positive: Futures CONFIRMED long/short가 올바른 recommendation이 된다.
- negative: Spot short-direction 분석이 spot short entry가 되지 않는다.
- boundary: invalidation 없음, expiry 경계, 동일 입력 재실행, 잘못된 action/kind 조합을 검증한다.

**검증**

```powershell
uv run pytest -q tests/unit/test_recommendation_models.py
uv run ruff check src/signalbot/recommendations tests/unit/test_recommendation_models.py
uv run pyright src/signalbot/recommendations tests/unit/test_recommendation_models.py
```

## Task L20-02 — deterministic projector

**선행 조건:** L20-01

**허용 변경**

- NEW `src/signalbot/recommendations/projector.py`
- NEW `tests/unit/test_recommendation_projector.py`
- MODIFY `src/signalbot/recommendations/__init__.py`

**행동 규칙**

- `CONFIRMED` directional decision만 entry candidate가 된다.
- `WATCH`, `SETUP`, gate failure, informational pullback은 `NO_ENTRY`다.
- `PUMP_RISK`/`CRASH_RISK`는 risk warning이며 entry가 아니다.
- reasons와 failed gates를 각각 보존한다.
- invalidation이 방향상 잘못됐으면 entry candidate를 fail closed `NO_ENTRY`로 내린다.
- decision time보다 오래된 higher-timeframe context는 projection하지 않는다.

**완료 조건**

- LONG/SHORT/NO_ENTRY positive, negative, threshold boundary가 있다.
- open candle이나 future context가 recommendation까지 도달하지 않는다.
- 같은 decision batch의 정렬과 ID가 실행 순서와 무관하다.

## Task L20-03 — ranking과 표시 만료

**선행 조건:** L20-02

**허용 변경**

- NEW `src/signalbot/recommendations/ranking.py`
- NEW `tests/unit/test_recommendation_ranking.py`

**범위**

- 한 decision clock에서 market별 top long/top short를 고른다.
- rank는 서로 다른 rule family score를 뺄셈한 directional edge가 아니다.
- stable tie-break는 actionability, evidence tier, rule strength, symbol 순서다.
- 추천 expiry 뒤에는 `EXPIRED` view만 내고 과거 추천을 재사용하지 않는다.
- `NO_ENTRY` 전체를 Discord로 쏟지 않고 API/query용 latest view로 유지한다.

**완료 조건**

- tie, empty candidate, all NO_ENTRY, stale recommendation 경계가 deterministic하다.
- ranking이 raw score를 성공 확률로 바꾸지 않는다.

## Task L20-04 — evidence annotation

**선행 조건:** L20-03, 현재 research governance 문서 확인

**허용 변경**

- NEW `src/signalbot/recommendations/evidence.py`
- NEW `tests/unit/test_recommendation_evidence.py`
- MODIFY `docs/RESEARCH_GOVERNANCE.md`

**범위**

- evidence tier를 `UNVALIDATED`, `HISTORICAL_ONLY`, `PROSPECTIVE_SHADOW`, `PROMOTION_ELIGIBLE`처럼 명시하되 기존 연구 authority를 우회하지 않는다.
- tier source에는 campaign ID, contract hash, sample/count availability만 넣는다.
- Phase-S 결과를 열지 않고도 표시 가능한 metadata와 결과 열람이 필요한 metadata를 구분한다.

**실패 분기**

권위 있는 promotion receipt를 증명할 수 없으면 `UNVALIDATED` 또는 `HISTORICAL_ONLY`를 유지한다. 알 수 없는 상태를 상위 tier로 추정하지 않는다.

## Phase gate

recommendation model/projector/ranking focused tests가 통과하고 기존 `SignalDecision` payload가 바뀌지 않아야 030으로 간다.
