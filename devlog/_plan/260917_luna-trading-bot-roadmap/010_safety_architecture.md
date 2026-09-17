# 010 — 안전 경계와 서비스 계약

## 목표

구현 전에 scanner, Guardian, Freqtrade의 권한과 데이터 흐름을 문서·설정 계약으로 고정한다. 이 단계는 주문 코드를 만들지 않는다.

## Task L10-01 — 현재 capability matrix 고정

**읽을 파일**

- `AGENTS.md`
- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/SIGNAL_SPEC.md`
- `src/signalbot/signals/positions.py`
- `src/signalbot/signals/position_management.py`
- `integrations/freqtrade/README.md`

**허용 변경**

- NEW `docs/TRADING_CAPABILITY_MATRIX.md`
- MODIFY `docs/ARCHITECTURE.md`
- MODIFY `README.md`

**내용**

- process별 public read, private read, stop write, entry write 권한을 표로 만든다.
- scanner=`public read only`, Guardian=`private read + stop-only`, Freqtrade auto=`dedicated account entry/exit`로 고정한다.
- Spot exit, Futures short, risk warning, entry recommendation의 의미를 분리한다.
- credential 소유자, DB 소유자, kill switch 소유자, 장애 시 동작을 기록한다.

**완료 조건**

- 문서 어디에도 scanner가 주문을 배치한다고 적혀 있지 않다.
- Guardian은 지정 포지션만 인수하며 신규 exposure를 만들 수 없다고 명시한다.
- Freqtrade는 수동 포지션을 인수하지 않는다고 명시한다.

**검증**

```powershell
git diff --check
rg -n "scanner|Guardian|Freqtrade|private|stop-only|dedicated" README.md docs/TRADING_CAPABILITY_MATRIX.md docs/ARCHITECTURE.md
```

## Task L10-02 — cross-service schema와 시간 계약 설계

**선행 조건:** L10-01

**허용 변경**

- NEW `docs/TRADING_EVENT_CONTRACTS.md`
- MODIFY `docs/SIGNAL_SPEC.md`

**고정할 계약**

- `RecommendationEnvelope`: action, market, symbol, decision time, expiry, reasons, blockers, invalidation, evidence strength, evidence tier, rule version, event ID.
- `ProtectionContext`: closed candle time, close, ATR, confirmed structure boundary, trend status, data completeness, source event ID, context version.
- `ManagedPositionSnapshot`: account alias, symbol, position side, quantity, entry, initial/active stop, extrema, observation cursor.
- `StopUpdateIntent`: deterministic ID, prior/proposed stop, quantity/mode semantics, source context, reason, policy version.
- `ExecutionReceipt`: request identity, exchange identity, terminal/uncertain status, before/after account snapshots.
- 모든 timestamp는 UTC Unix ms로 저장하고 출력에서 UTC/KST를 함께 표시한다.

**조건부 경로**

- exchange ID가 없는 timeout은 `FAILED`가 아니라 `UNCERTAIN`이다.
- source context가 open candle이거나 오래됐으면 intent를 만들지 않는다.
- 동일 ID에 다른 payload가 오면 hard conflict다.

**완료 조건**

- 각 schema의 identity 구성과 idempotency 규칙이 예제로 설명된다.
- nullable field가 “모름”인지 “해당 없음”인지 구분된다.
- price/quantity는 exchange boundary에서 `Decimal`이라는 규칙이 적힌다.

## Task L10-03 — 설정·비밀·모드 위협 모델

**선행 조건:** L10-02

**허용 변경**

- NEW `docs/POSITION_GUARDIAN_THREAT_MODEL.md`
- NEW `docs/TRADING_MODE_PROMOTION.md`

**설계할 모드**

`observe -> shadow -> testnet -> live_protection`과 `backtest -> dry_run -> testnet -> live_entry`를 별도 상태축으로 둔다. 기본값은 각각 `observe`, `backtest`다.

**필수 위협**

- leaked secret, clock skew, replay, duplicate order, timeout-after-accept, partial fill, manual partial close, manual size increase, side flip, Hedge/One-way mismatch, stale context, DB loss, scanner outage, exchange outage, min-notional/tick mismatch.

**완료 조건**

- 각 위협에 detect, fail behavior, recovery owner가 있다.
- live mode를 켜는 설정 한 개만으로는 주문이 활성화되지 않는 2단계 arming을 정한다.
- 로그에 API secret/signature가 들어가지 않는 redaction 규칙이 있다.

## Phase gate

L10-01~03 문서 리뷰가 끝나기 전 `position_guardian` package나 private Binance adapter를 만들지 않는다.
