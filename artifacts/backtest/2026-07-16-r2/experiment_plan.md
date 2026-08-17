# R2 strict-PIT HTF 5분봉 소급 진단 사전계획

Protocol: `r2_retrospective_screen_v1`  
상태: 결과 확인 전 동결  
동결 시각: 2026-07-16T10:59:24+09:00  
대상 기간의 지위: 이미 노출된 retrospective regression / negative control  
금지: 이 결과를 untouched OOS, 확증된 edge, 코인 추천, 실거래 승인으로 표현

## 1. 질문과 범위

R1의 C0 가격 돌파 trigger를 바꾸지 않은 상태에서 strict-prior 15m·1h 추세 정렬이 비용 후 60분 에피소드와 현재 동결된 기술적 종료 성과를 개선하는지 진단한다. Binance 주문 endpoint는 호출하지 않는다.

Full 후보 `R2_PIT_HTF_EXEC`에 필요한 판단 후 실제 BBO, 호가 수량, depth, receipt time이 없으므로 실행 조건은 소급 검정하지 않는다. Full 후보의 현재 판정은 결과와 무관하게 `INCONCLUSIVE_NO_HISTORICAL_BBO`다.

## 2. 고정 데이터

- 시장/방향: Binance Spot long, Binance USDⓈ-M Futures short. 방향은 합치지 않는다.
- 자산: BTC, ETH, BNB, SOL, XRP, DOGE, SUI, WIF의 고정 8자산.
- 주기: 완전히 닫힌 5m candle만 사용.
- 원시 자료: 2024-03-01 이후 16개 kline stream과 8개 futures funding stream.
- 평가: `[2024-07-01, 2026-07-01)`.
- 노출 segment: development `[2024-07-01,2025-03-01)`, validation `[2025-03-01,2025-11-01)`, retrospective_test `[2025-11-01,2026-07-01)`.
- 이 8자산은 동적 과거 top-N이 아니라 survivor-selected 고정 panel이다.
- 5m feature는 연속 source bar 210개, 15m/1h context는 각 완전 aggregate bar 210개가 있어야 한다.

## 3. 동결 entry 정책

### C0_CORRECTED

Long은 closed 5m close가 직전 20개 완전봉 high를 새로 상향 돌파하고, EMA20 > EMA50, MACD histogram > 0이면서 직전보다 개선, ADX14 >= 20일 때다. Short는 정확한 방향 반대다. 진입 기준가는 다음 연속 5m 봉 open이다.

Squeeze, RSI reversal, G2/G4 volume, participation, crowding/funding gate, breadth/BTC regime, anomaly는 entry 선택에 쓰지 않는다.

### H1_STRICT_PRIOR_HTF

C0 trigger와 아래 Boolean 조건의 AND다.

- Long: strict-prior 15m와 1h가 모두 `close > EMA20 > EMA50`.
- Short: strict-prior 15m와 1h가 모두 `close < EMA20 < EMA50`.
- 각 context의 `close_time < 5m decision_time`이어야 한다. same-close 사용은 금지한다.
- 하나라도 없거나 immature면 reject다.
- 기존 보상형 trend score를 사용하지 않는다.

## 4. 동결 exit 정책과 4셀 행렬

| Entry | F60 고정 에피소드 | T72 기술적 종료 |
|---|---|---|
| C0 | 비혼입 신호 기준선 | 현재 lifecycle 기준선 |
| H1 | HTF entry filter 단일 효과 | 배포 조합의 보조 기술통계 |

F60은 다음 open 진입, decision 뒤 12번째 5m 봉 close 종료다. T72는 현재 정책을 동결한다: trigger 구조 초기 stop, 1R 후 2ATR trailing, 3개 연속 trend failure, eligible 반대 돌파, 최대 72 closed bars. trailing은 close 뒤 갱신되어 같은 봉에 적용되지 않는다. 파라미터 탐색은 하지 않는다.

Primary는 모든 공통 raw trigger의 독립 episode다. 한 종목 한 포지션 순차 portfolio replay는 secondary다.

## 5. 공통 panel과 split 격리

모든 셀은 동일 C0 raw-trigger ID panel을 사용한다. 다음 open이 split 시작 + 72×5m 이상이고, t+72 close가 같은 split 끝보다 이르며, 전체 경로가 연속인 기회만 primary panel에 포함한다. feature warm-up은 split 전 과거를 사용할 수 있지만 position, pending entry/exit, state machine은 split을 넘지 않는다.

거절된 H1 기회는 policy contribution 0으로 포함한다. 결측 실행 자료를 0으로 대체하지 않는다.

## 6. 고정 비용 계약

- 명목금액: 기회마다 100 USDT.
- 수수료/side: Spot 10bp, Futures 5bp.
- adverse execution/side: Spot anchor/major 5bp, volatile 10bp; Futures anchor/major 3bp, volatile 8bp.
- 이 adverse concession은 관측되지 않은 half-spread, impact, latency를 포함하는 kline proxy다.
- 11.25bp historical spread proxy를 추가 비용으로 이중 차감하지 않는다.
- 0×와 2× slippage 고정 민감도를 함께 산출한다.
- Futures funding은 `entry_time < funding_time < exit_time`인 실제 settled event만 P&L에 포함한다. entry feature funding은 동일 live PIT 최소이력·lookback·freshness 계약을 따른다.

## 7. 사전 estimand

방향 s의 공통 panel P에서 F60 net return을 R_i, H1 수락을 I_i라 한다.

- `theta_C0 = mean(R_i)`.
- `theta_H1 = mean(I_i * R_i)`; reject contribution은 0.
- `mu_H1 = sum(I_i R_i) / sum(I_i)`.
- `delta_entry = theta_H1 - theta_C0`.
- 공통 stop-openable set X에서 `mu_T = mean(T72 net)`.
- `delta_exit = mean(T72 net - F60 net)` on X.

H1 entry composite는 `mu_H1 > 0` AND `delta_entry > 0`; T72 exit composite는 `mu_T > 0` AND `delta_exit > 0`이다. Entry/Exit × Spot/Futures의 네 composite만 primary family로 둔다. H1×T72는 secondary다.

## 8. 추론과 다중비교

- 모든 자산·시장을 같은 UTC 날짜 block으로 함께 재표본한다.
- circular moving-block bootstrap: 7일 primary, 14/28일 고정 민감도.
- 50,000 replicates, seed `20260716`.
- endpoint one-sided p-value는 사전 공식으로 계산하고 composite는 두 p-value의 max다.
- 네 composite에 Holm step-down으로 one-sided FWER 0.05를 통제한다.
- lower bound는 centered-bootstrap error의 one-sided basic bound로 고정한다.

## 9. 정보량과 판정

다음이면 FAIL이 아니라 INCONCLUSIVE다: accepted/common-stop-openable <500, valid UTC days <120, H1 coverage <10%, feature availability <99%, invalid bootstrap replicate >0.1%.

정보량이 충분한 retrospective screen의 모든 통과 조건:

1. 관련 composite의 두 Holm-adjusted one-sided lower bound >0.
2. accepted/technical mean net >=5bp.
3. fixed-notional cumulative net P&L >0.
4. PF >1.05.
5. 2× slippage point estimate >=0.
6. 양의 기여 자산 >=6/8.
7. 최대 양의 자산 기여 집중도 <=35%.
8. closed/PIT/split/common-ID/A-B/funding-cost provenance gate 통과.

모두 통과해도 명칭은 `RETROSPECTIVE_SCREEN_PASS`이며 live 배포나 full R2 승인이 아니다.

## 10. 결과 확인 전 동결한 engineering 수정

- 연구 confirmation policy가 live 설정을 따르게 한다.
- backtest funding을 live PIT owner와 일치시킨다.
- split 전후 position/pending/state 오염을 제거한다.
- 반대 signal 종료는 triggered+eligible일 때만 인정한다.
- strict HTF는 별도 Boolean predicate와 별도 opportunity 필드로 기록한다.
- F60 fee/slippage/funding/net 필드를 raw opportunity에 기록한다.
- comparator가 입력·코드·설정·plan·출력 hash를 직접 검증한다.

결과를 본 뒤 임계값, 종목, 방향, horizon, 비용, block, exclusion을 바꾸지 않는다.

## 11. 통계적 오류 사전 점검 (11/11)

1. Simpson's Paradox: pooled 방향과 자산/segment 방향을 함께 보고한다.
2. Ecological Fallacy: panel 평균을 개별 코인의 기대수익으로 일반화하지 않는다.
3. Berkson's Paradox: 거래대금/상장기간/고정 8자산 선택의 조건부 표본 편향을 명시한다.
4. Collider Bias: 성과를 본 뒤 gate/covariate를 통제변수로 추가하지 않는다.
5. Base Rate Neglect: 수락률·원시 trigger 수·거절 contribution을 함께 보고한다.
6. Regression to the Mean: 변동성 극단 종목의 후속 정상화를 alpha로 해석하지 않는다.
7. Survivorship Bias: 현재 8자산 고정 panel의 생존편향을 명시한다.
8. Look-Elsewhere Effect: 네 composite 외 결과를 primary로 승격하지 않는다.
9. Garden of Forking Paths: 이 문서로 entry/exit/cost/split/inference를 사전 고정한다.
10. Correlation != Causation: 역사적 연관을 전략이 수익을 ‘개선했다’는 인과로 표현하지 않는다.
11. Reverse Causality: HTF 정렬과 이후 수익의 시간 순서는 지키되 인과 메커니즘을 확정하지 않는다.

## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: plan
- Origin Date: 2026-07-16T10:59:24+09:00
- Verification Status: UNVERIFIED
- Version Label: code_plan_v1

