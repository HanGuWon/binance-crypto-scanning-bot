독립 감사 결론

최종 manifest 기준 첨부물은 31개이며, Python source 17개, tests 8개, YAML 2개, R1 산출물 2개, 패키지 문서 2개다. CONTEXT_PACKAGE.md에 다시 포함된 나머지 30개 파일도 실제 파일과 대조했으며, terminal newline 차이를 제외하고 30/30개가 일치했다.

최종 판정은 다음과 같다.

대상	판정
R1 G2/G4 활성화	기각 유지
R1의 완전 재현성·VERIFIED 주장	현 첨부물로 인증 불가
현재 live alert runtime	배포 차단
제안한 R2 구현	필수 engineering gate 충족을 조건으로 승인
Binance 생산 주문	영구 금지

R1에서 보고된 여섯 조합—C0/G2/G4 × Spot-long/Futures-short—은 모두 비용 전 평균 gross, 비용 후 평균 net, 총손익이 음수이고 PF도 1 미만이다 (artifacts/backtest/2026-07-15-volume-r1/final_report_ko.md:108-135). 따라서 발견된 재현성 및 분할 결함은 G2/G4를 되살릴 근거가 아니다. 다만 R1을 완결된 과학적 재현 패키지라고 부르거나 현재 live gate stack의 검증으로 사용하는 것도 타당하지 않다.

1. 감사 범위와 검증 상태

확인된 강점은 다음과 같다.

Candle.validate_exchange_fields()는 finite/positive OHLC, volume, taker-buy≤total, 시간 및 OHLC 일관성을 강하게 검증한다 (src/signalbot/domain/models.py:31-80).

closed_kline_flow()는 quote volume을 denominator로 고정하고, quote volume 0을 중립값으로 대체하지 않는다 (data/microstructure.py:26-58).

live funding은 strict-prior, freshness, 최소 history를 요구한다 (data/funding.py:FundingRateTracker.snapshot:137-175).

backtest HTF context는 완전한 aggregate candle과 strict < decision_time 조회를 사용한다 (backtest/context.py:12-111).

현재 event_id는 아래 공식으로 결정론적으로 생성된다 (signals/state_machine.py:_decision:83-95).

SHA256(
  market|symbol|family|timeframe|stage|event_time_ms|rule_version
)[:24]

반면 첨부물에는 clock.py, candle store, Binance payload parser, repository, Discord sender, tests/conftest.py, uv.lock이 없다. R1이 참조하는 24개 원시 입력, G2/G4 spec, run manifests, trades.csv, opportunities.csv, comparison JSON, plan, feature contract도 없다. 따라서 전체 배포 경로와 R1 A/B byte identity는 인증할 수 없다.

독립 실행 결과는 다음과 같다.

전체 Python syntax compilation: 성공

독립 실행 가능한 state-machine tests: 7 passed

전체 pytest collection: 7개 test 수집, 7개 파일 collection error

R1 보고서의 176 passed는 현 패키지로 재현 불가

첨부된 가시 source/YAML에서는 order/private/auth/signing endpoint 패턴을 발견하지 못함

verdict.json의 overall result, criteria, fail reasons는 서로 내부 일관적

settings YAML과 C0 YAML hash는 R1 보고서 값과 일치

따라서 “가시 핵심 코드에는 주문 실행이 없다”까지는 확인되지만, 누락된 전체 repository와 deployment wiring까지 포함해 생산 주문 부재를 인증할 수는 없다.

2. 현재 live 전략에서 발견된 핵심 결함
BLOCKER 1 — 동일 데이터가 도착순서에 따라 다른 판단을 생성한다

MarketRuntime._handle_candle()은 symbol별 5분봉이 도착할 때마다 즉시 breadth를 갱신하고 해당 symbol을 평가한다 (runtime.py:130-148). MarketRegimeEngine은 현재까지 도착한 symbol의 최신 방향만 평균한다 (regime/market.py:14-40). BTC 1시간봉도 도착 즉시 current regime에 반영된다 (runtime.py:140-143).

독립 재현에서는 같은 close timestamp의 상승봉과 하락봉을 처리하는 순서만 바꿨을 때 첫 평가의 breadth가 다음처럼 달라졌다.

AAA → BBB: 1.0, 이후 0.5
BBB → AAA: 0.0, 이후 0.5

최종 상태는 같아도 첫 alert의 context가 다르다. 같은 시각의 BTC 1h 봉이 먼저 도착하면 동일 시각 5m 판단에 섞일 가능성도 있다.

이는 backtest의 timestamp-aligned regime과도 다르다 (backtest/engine.py:build_market_regimes:264-311). R2에서는 symbol arrival 즉시 평가를 폐기하고, strict-prior immutable context와 canonical phase ordering을 사용하는 EventTimeCoordinator가 필요하다.

BLOCKER 2 — DB 저장과 Discord 전송 사이에 atomicity가 없다

현재 _process() 순서는 다음과 같다 (runtime.py:255-260).

state transition
→ repository.save_signal()
→ await decision_handler()

따라서 DB 저장 후 Discord sender가 실패하면 signal은 이미 dedupe되어 재전송되지 않을 수 있다. 반대로 Discord가 수락했지만 응답 전에 timeout이 나면 자동 재시도는 중복을 만들 수 있다.

Discord 공식 문서상 wait=true는 서버 확인을 기다리고 생성된 message body를 반환한다. wait=false에서는 message가 저장되지 않아도 오류가 반환되지 않을 수 있다. 공식 Execute Webhook query parameter에는 요청 idempotency key가 제시되어 있지 않으므로, 로컬 DB와 Discord를 포함한 외부 exactly-once를 주장해서는 안 된다. 
Documentation - Discord
+1

R2는 같은 DB transaction에서 decision과 outbox를 저장해야 한다.

save_decision_and_enqueue(event_id, fingerprint, payload)

Dispatcher는 PENDING → SENDING → DELIVERED / UNCERTAIN / DEAD 상태를 유지하고, Discord 요청에는 wait=true를 사용해 반환 message ID를 저장해야 한다. 요청 body가 전송된 뒤 timeout이 난 모호한 경우에는 blind retry하지 않고 UNCERTAIN으로 두는 것이 중복 금지 조건에 맞다.

BLOCKER 3 — R1 technical trade가 split boundary를 넘는다

기회 label은 split start embargo와 horizon crossing을 차단한다 (backtest/engine.py:_build_opportunity:616-680). 그러나 실제 technical position loop는 data gap에서만 position, pending entry, state machine을 reset한다 (run_symbol:401-425). split 변경을 감지하는 코드는 없다.

Trade의 split은 entry time만으로 지정된다 (_close_trade:937). 독립 probe에서는 development 종료 직전에 진입하고 validation에서 종료한 포지션이 development로 귀속되었다.

따라서 R1 보고서의 “split boundary purge” 서술은 opportunity label에는 적용되지만 technical trade ledger에는 적용되지 않는다 (final_report_ko.md:332-340). 모든 보고된 split 성과가 음수이므로 최종 기각 방향은 바뀌지 않지만, split별 trade 표를 독립적인 clean segment 결과로 해석해서는 안 된다.

BLOCKER 4 — R1 결과 패키지가 완전 재현 가능하지 않다

R1 보고서는 다음을 주장한다.

A/B 산출물 byte identity

24개 원시 입력 hash 일치

50,000회 shared bootstrap

176 tests 통과

그러나 이를 검증할 raw inputs, output files, manifests, lockfile, comparison JSON, G2/G4 specs가 없다 (final_report_ko.md:279-304,323-352,367-376).

또한 첨부 C0 YAML은 bootstrap.samples: 2000인데 보고서는 주 분석을 50,000회라고 서술한다 (config/backtest.5m.volume-c0.yaml:57; final_report_ko.md:167-191). 별도 runner override였을 수 있지만 effective manifest가 없으므로 provenance gap으로 남는다.

3. 추가 HIGH 결함
Anomaly 재무장 실패

AnomalyDetector.update()는 정상 상태에서 IDLE/CLEAR evaluation을 만들지 않고 None만 반환한다 (data/anomaly.py:31-67). State machine은 같은 family가 이미 CONFIRMED이면 동일 stage evaluation을 무시한다 (state_machine.py:48-55).

독립 probe에서 첫 anomaly 이후 정상화와 두 번째 독립 anomaly가 있었지만 다음 결과가 나왔다.

JSON
{
  "anomaly_evaluations": 6,
  "emitted_decisions": 1,
  "final_stage": "confirmed"
}

R2 anomaly는 일반 score state가 아니라 CONFIRMED → CLEAR/EXPIRED → REARMED incident lifecycle로 구현해야 한다.

Key-space가 bounded하지 않다

각 deque에는 maxlen이 있지만 symbol key dictionary에는 prune이 없다.

anomaly _points: data/anomaly.py:24-26

flow _trades, _last_trade_ids: data/microstructure.py:61-82

book _books: data/microstructure.py:110-139

regime _breadth: regime/market.py:11-16

state _states: signals/state_machine.py:23-27

runtime feature maps: runtime.py:67-70,217-223

set_surveillance_symbols()도 active set만 바꾼다 (runtime.py:74-75). Funding tracker는 old symbol을 제거하지 않고 maximum symbol 수에 도달하면 exception을 낸다 (data/funding.py:107-131).

1,000개 symbol rotation probe에서 위 map들이 모두 1,000 key까지 증가했고 funding capacity exception이 재현되었다. R2는 UniverseCoordinator.reconcile(generation, active_symbols)에서 모든 store를 원자적으로 prune해야 한다.

Bootstrap/gap replay가 정상 stream과 동등하지 않다

bootstrap()은 candle을 bulk insert한 뒤 각 key의 최신 feature만 한 번 계산한다 (runtime.py:77-81). Gap recovery도 missing candles를 bulk insert하지만 각 candle의 regime와 feature를 순차 replay하지 않는다 (runtime.py:150-197).

R2에서는 bootstrap과 gap recovery를 같은 event-time handler에 emit_alerts=false로 순서대로 재주입하고, uninterrupted replay와 최종 state/decision hash가 같아야 한다.

Active universe와 clock 계약이 불완전하다

Surveillance set 검사는 anomaly에만 적용된다. Candle, book, trade는 active-symbol 검증 없이 상태를 변경한다 (runtime.py:97-148).

Spot bookTicker의 공식 schema는 update ID와 BBO를 제공하지만 exchange event timestamp를 제공하지 않는다. 반면 diff-depth stream은 event time과 first/final update IDs를 제공한다. 따라서 timestamp가 없는 bookTicker에 local time을 넣고 exchange event time처럼 취급해서는 안 된다. 
바이낸스 개발자 센터
+1

R2의 event envelope에는 다음이 분리되어야 한다.

exchange_event_time_ms: nullable
object_time_ms
receipt_time_ms
stream_session_id
stream_sequence
source_update_id
raw_payload_sha256
universe_generation
Live/backtest funding 계약이 다르다

Live는 minimum 20 prior observations, 설정된 30일 lookback, 9시간 freshness를 사용한다. Backtest는 30일을 hard-code하고 history 2개부터 z-score를 계산하며 freshness 검사가 없다 (backtest/engine.py:799-840).

USDⓈ-M funding history 자체는 공개 endpoint인 GET /fapi/v1/fundingRate로 받을 수 있으며, 공식 문서에는 limit과 ascending-order 계약이 명시되어 있다. 
바이낸스 개발자 센터
+1

R2에서는 하나의 pure PIT funding calculator를 live/replay/backtest가 공유해야 하며 funding은 confirmatory gate가 아니라 covariate로만 기록하는 것이 안전하다.

4. 현재 live와 R1 전략의 관계

현재 example 설정은 다음을 사용한다.

Spot와 Futures

primary 5m

trend, participation, crowding, execution, completeness gates 활성

strict explicit trigger

volume feature none

reversal interval [1h, 4h]

하지만 runtime은 primary 5m이 아닌 candle에서 rule evaluation 전에 return한다 (runtime.py:140-148). Reversal family는 1h/4h에서만 허용되므로 현재 설정에서는 사실상 비활성이다 (rules.py:295-304,346-355).

더 중요한 점은 R1 C0/G2/G4가 participation, crowding, HTF gate를 모두 끄고 historical spread proxy를 사용했다는 것이다 (config/backtest.5m.volume-c0.yaml:10-22). 따라서 R1은 현재 live 전체 gate policy를 검증한 실험이 아니다.

R1 판정은 다음과 같이 제한해서 사용해야 한다.

G2/G4 volume eligibility 채택: 기각

현재 live gate stack의 유효성: 미검증

2024-07-01~2026-07-01 데이터: R2 efficacy에 재사용 금지

해당 기간의 용도: regression fixture와 negative control

5. 제안 R2 전략
Benchmark — C0_FROZEN

R1 raw trigger를 그대로 동결한다.

Spot long

closed 5m close crosses above prior 20 closed-bar high
AND EMA20 > EMA50
AND MACD histogram > 0 and improving
AND ADX14 >= 20

USDⓈ-M futures short

정확한 방향 반대다.

Candidate — R2_PIT_HTF_EXEC

Raw trigger는 C0와 동일하며, 추가 조건은 보상 없는 Boolean AND다.

C0 raw trigger
AND strict-prior 15m trend aligned
AND strict-prior 1h trend aligned
AND public BBO age <= 2,000ms
AND spread <= 15bp
AND 100 USDT quote/depth executable

HTF 조건은 다음과 같다.

long:  close > EMA20 > EMA50 on both 15m and 1h
short: close < EMA20 < EMA50 on both 15m and 1h

2초와 15bp는 현재 live 설정에 이미 존재하는 값을 그대로 사용한다. 결과를 본 뒤 새 임계값을 탐색하지 않는다.

다음 항목은 confirmatory gate에서 제외한다.

G2/G4 volume features

participation score

crowding/funding z-score

breadth/BTC regime

squeeze/reversal

anomaly

이 값들은 모두 PIT covariate로 기록하되 R2 결과를 본 뒤 gate로 승격하지 않는다.

6. Closed candle 및 공개 데이터 계약

Binance Spot kline stream은 현재 candle을 반복 갱신하고 payload에 closed 여부 x를 포함한다. 따라서 x=true만 확정 candle로 받아야 한다. all-market miniTicker는 변경된 symbol만 배열에 포함하므로 각 symbol에 대해 고정 1Hz 표본이라고 가정할 수 없다. 
바이낸스 개발자 센터
+2
바이낸스 개발자 센터
+2

현재 USDⓈ-M all-market stream 문서는 changed-only 동작과 merged UM/CM schema의 market discriminator를 설명한다. R2 parser는 해당 schema가 제공될 때 USDⓈ-M인 st=1을 명시적으로 검증해야 한다. 
바이낸스 개발자 센터

Binance 공식 public-data repository는 daily/monthly 파일과 .CHECKSUM을 제공한다. Spot 파일 timestamp는 2025-01-01부터 microseconds이므로 source/date에 따른 단위 정규화와 range validation이 필수다. 
GitHub
+1

R2의 핵심 시간 규칙은 다음과 같다.

decision time = closed 5m candle close time t
HTF/global context allowed only when context.close_time < t
same-close 15m/1h context is forbidden
partial breadth denominator is forbidden
duplicate same payload = no-op
same key/time different payload = fatal conflict
7. Paper execution 정의

어떤 경우에도 Binance order endpoint를 호출하지 않는다. Signal efficacy는 공개 quote 기반 hypothetical outcome으로만 측정한다.

notional: 100 USDT
entry: first valid public quote at/after decision persisted time + 1 second
exit: first valid opposite quote at/after entry + 60 minutes
Spot long: enter ask, exit bid
Futures short: enter bid, exit ask

Top-of-book quantity가 100 USDT에 부족하면 public depth를 walk하거나 non_executable로 기록한다. Stale/missing quote는 0 return이나 mid-price로 대체하지 않는다. Futures holding 중 실제 발생한 funding은 paper P&L 비용에는 포함할 수 있지만, signal feature에는 strict-prior 값만 허용한다.

8. 사전등록형 prospective 실험
기간과 universe

Manifest에 code, config, dependency lock, schema, seed, source hash를 기록하고 외부 timestamp를 받은 다음 첫 00:00 UTC가 T0

정확히 180 calendar days

성과에 따른 기간 연장 금지

전일 완전 종료 UTC day quote volume으로 시장별 top 20

listing age ≥90일

신규 편입 후 closed 5m 210개 warm-up

universe 교체 시 generation 기반 atomic prune

Common opportunity panel

모든 C0_FROZEN raw trigger가 공통 panel이다.

C0 contribution:
    모든 raw trigger의 60분 net return

R2 contribution:
    candidate accept이면 같은 net return
    reject이면 policy contribution 0

R2 conditional return:
    accept된 opportunity만의 net return

Abstention 효과와 선택된 alert 자체의 기대값을 혼동하지 않기 위해 방향별로 두 co-primary endpoint를 둔다.

Candidate accepted conditional mean net return

Common-panel policy contribution uplift R2 − C0

방향별 검정은 intersection-union test로 구성해 두 endpoint p-value의 최대값을 composite p-value로 사용한다. Spot-long과 Futures-short 두 composite hypothesis에는 Holm step-down을 적용해 one-sided FWER 0.05를 통제한다. Holm의 원 논문은 true hypothesis 조합과 무관한 1종 오류 보호를 목적으로 한 순차 기각법을 제시한다. 
JSTOR

의존성 보존 추론

개별 alert를 iid로 bootstrap하지 않는다. UTC day별로 accepted sum/count, C0/R2 contribution, asset contribution을 묶고 모든 asset과 market을 같은 날짜 block으로 재표본한다.

method: circular moving block bootstrap
primary block: 7 UTC days
fixed sensitivities: 14, 28 days
replicates: 50,000
seed: 20260716

관측 추정치를 θ̂, block replicate를 θ*라 할 때:

θ̂ <= 0  → p = 1

θ̂ > 0   → p =
(1 + count[(θ* - θ̂) >= θ̂]) / (B + 1)

Accepted denominator가 0인 replicate는 invalid이며, invalid 비율이 0.1%를 넘으면 해당 방향은 inconclusive다. Künsch의 원 논문은 dependent stationary observations에 block-based bootstrap을 확장한다. 
Project Euclid

최소 정보량과 통과 조건

방향별 최소 정보량:

accepted opportunities >= 500
valid UTC days >= 120
candidate coverage >= 10%

미달 시 FAIL이 아니라 INCONCLUSIVE다.

모든 통과 조건:

두 co-primary endpoint의 Holm-adjusted lower bound > 0
accepted mean net return >= 5bp
fixed-notional cumulative paper P&L > 0
profit factor > 1.05
largest positive-asset contribution <= 35%
positive-contribution assets >= 6
closed/PIT/determinism/duplicate operational gates 통과

중간 efficacy peeking, 손실 종목 제거, 유리한 horizon 또는 block length 선택, missing-value 대체, threshold 변경은 금지한다. 변경이 필요하면 새 protocol version으로 처음부터 다시 시작한다.

9. 필수 release gate

다음 중 하나라도 실패하면 R2 live 배포를 차단해야 한다.

같은 canonical event set을 무작위 순서로 재생해 decision/outbox byte hash가 동일

open candle에서 decision 0건

same-close HTF context 사용 0건

duplicate는 no-op, conflicting correction은 fatal

old universe generation event 거절

10,000회 universe rotation 후 모든 key count가 선언 상한 이하

anomaly confirm→clear→second confirm 재무장

uninterrupted, bootstrap, gap-recovery replay 결과 동일

streaming/batch indicator parity

split crossing trade 0

DB/Discord crash-injection matrix 통과

Discord wait=true와 반환 message ID 저장

API key, signer, private/account/order client 및 endpoint 0

outbox/raw-event memory와 disk hard quota

dependency lock 및 preregistration manifest 존재

산출물

R2_independent_audit_ko.md

proposed_r2_preregistered_spec.yaml

reproduce_findings_output.json

verification_summary.json

전체 감사 번들 ZIP

감사 번들 SHA-256은 14a9fcdbdaac96a9759557aecde1a08a3e5539c132a79f0d4c5a020af3fcc2d2다.
