# 030 — 추천 저장, API, Discord

## 목표

canonical recommendation을 재시작 후에도 조회할 수 있고, Discord에서 LONG/SHORT/진입 보류를 오해 없이 보여준다.

## Task L30-01 — recommendation persistence

**선행 조건:** L20-04

**읽을 파일**

- `src/signalbot/persistence/models.py`
- `src/signalbot/persistence/repository.py`
- `tests/unit/test_repository.py`

**허용 변경**

- MODIFY `src/signalbot/persistence/models.py`
- MODIFY `src/signalbot/persistence/repository.py`
- MODIFY `tests/unit/test_repository.py`
- NEW migration file only if the repository's current migration convention requires it

**범위**

- immutable recommendation event와 symbol/action별 latest pointer를 분리한다.
- 동일 ID+동일 bytes는 no-op, 동일 ID+다른 payload는 hard conflict다.
- retention/pruning은 bounded하며 promotion evidence에 필요한 immutable row를 지우지 않는다.
- schema version과 projection version을 저장한다.

**완료 조건**

- restart 조회, duplicate replay, conflict, latest ordering, pruning boundary test가 있다.
- signal/outbox transaction의 기존 atomicity를 깨지 않는다.

## Task L30-02 — scanner integration

**선행 조건:** L30-01

**허용 변경**

- MODIFY `src/signalbot/scanner.py`
- MODIFY `src/signalbot/app.py`
- MODIFY relevant scanner/runtime tests

**범위**

- fully closed primary candle decision 뒤 projector를 호출한다.
- persistence 실패 시 recommendation 전달을 하지 않는다.
- gap recovery와 replay가 같은 recommendation ID를 만든다.
- intrabar risk warning은 별도 kind로 유지한다.

**완료 조건**

- open candle, duplicate reconnect, gap replay, universe rotation, persistence failure test가 통과한다.
- scanner에서 private API import가 0건이다.

## Task L30-03 — read-only API와 CLI query

**선행 조건:** L30-02

**허용 변경**

- MODIFY `src/signalbot/api/app.py`
- MODIFY `src/signalbot/cli.py`
- NEW/modify API and CLI tests
- MODIFY `README.md`

**API 결과**

- latest actionable long/short
- symbol별 latest recommendation와 blockers
- UTC/KST 표시값
- evidence tier와 score semantics

**완료 조건**

- pagination/limit은 bounded하다.
- expired recommendation은 actionable query에서 제외된다.
- API가 secret이나 private account 정보를 반환하지 않는다.

## Task L30-04 — Discord recommendation card

**선행 조건:** L30-02

**허용 변경**

- MODIFY `src/signalbot/alerts/discord.py`
- MODIFY `tests/unit/test_discord.py`
- MODIFY `docs/SIGNAL_SPEC.md`

**표시 규칙**

- 제목: LONG 후보, SHORT 후보, 진입 보류, 위험 경고를 구분한다.
- 근거, blocker, invalidation, 만료, UTC/KST, rule version, evidence tier를 담는다.
- score에는 `확률 아님` 표시를 유지한다.
- `NO_ENTRY`는 상태 변화나 명시 query summary에서만 보내 spam을 막는다.

**완료 조건**

- Discord embed limit, Unicode, 긴 reasons truncation, UTC 날짜 rollover, webhook ambiguous response test가 통과한다.
- 기존 durable outbox semantics를 재사용한다.

## Task L30-05 — recommendation 운영 지표

**선행 조건:** L30-03, L30-04

**허용 변경**

- MODIFY existing observability/report owner
- NEW focused tests
- MODIFY `docs/OPERATIONS.md`

**지표**

- recommendation count/action, NO_ENTRY blocker 분포, expired-before-delivery, projection conflict, delivery status, decision-to-alert latency.
- 성능 수익률과 운영 전달률을 같은 pass 조건으로 섞지 않는다.

## Phase gate

추천 단계 완료는 주문 권한을 주지 않는다. full repository verification이 통과하고, 실제 Discord payload를 dry-run snapshot으로 사람이 읽을 수 있어야 한다.
