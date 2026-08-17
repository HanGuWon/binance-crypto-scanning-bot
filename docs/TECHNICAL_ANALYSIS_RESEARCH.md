# 기술적 분석 알림 시스템 조사·구현 결정

조사 기준일: 2026-07-18. 이 문서는 거래·주문 기능이 아니라 공개 Binance
데이터를 이용한 설명 가능한 Discord 알림의 근거와 한계를 기록한다. 사용자가
제공한 ChatGPT 프로젝트의 `기술적 분석 방법 분석`, `기술적 지표 설명`,
`가상화폐 투자 봇 분석` 대화도 검토했으며, 최종 구현 판단은 아래 공식 문서와
1차 연구로 다시 확인했다.

## 결론

기술지표 수를 늘려 다수결하는 방식은 채택하지 않는다. EMA 정렬, MACD,
RSI, 가격/EMA 거리는 대체로 같은 종가에서 파생되므로 서로 독립된 네 표가
아니다. 시스템은 다음 역할을 분리한다.

1. 추세·모멘텀: EMA20/50, EMA 기울기, ADX, MACD, RSI 변화.
2. 가격 구조: 직전 Donchian 범위와 미래정보 없는 확정 swing high/low.
3. 눌림목: 기존 구조, ATR 정규화 impulse/depth, 구조 미붕괴, 회복 종가.
4. 변동성: ATR%, Bollinger 폭·백분위. 방향표가 아니라 상태·위험 문맥.
5. 참여도: 현재 봉을 제외한 상대거래량, 거래량/체결수 z-score.
6. 공격적 체결: taker-buy 비율, imbalance, CVD. 주로 확인·체결 문맥.
7. 유동성·실행: 실제 BBO spread, age, top-of-book 수용량.
8. 파생 문맥: funding과 향후 OI/basis. 단독 방향표가 아니라 crowding 문맥.

Discord의 방향 점수는 각 방향에서 가장 강한 기존 규칙의 원점수다. 상관된
규칙 점수를 합산하지 않으며, 확률도 아니다. 독립 Trend/Participation/Crowding/
Execution/Completeness 게이트는 별도 줄로 표시한다.

## Binance에서 얻는 값과 로컬 계산 값

Binance는 EMA, RSI, MACD, ADX, ATR, Bollinger Band, 추세선, 지지·저항,
눌림목을 완성된 지표로 주지 않는다. Spot `/api/v3/klines`와 USDⓈ-M
`/fapi/v1/klines`가 OHLC, base/quote volume, 거래 수, taker-buy volume을
제공하며 지표는 폐봉 뒤 로컬에서 계산한다. [Spot 공식 market data](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market),
[USDⓈ-M 공식 market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).

WebSocket kline은 `k.x=true`인 경우에만 의사결정한다. Futures는 현재
`/market`(kline/aggTrade/markPrice)과 `/public`(bookTicker/depth) 라우트를
구분해야 한다. 이 저장소의 endpoint plan은 이미 두 라우트를 분리한다.
[Spot WebSocket](https://developers.binance.com/en/docs/products/spot/web-socket-streams),
[Futures 연결 규약](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect).

USDⓈ-M의 funding, current/historical OI, global long/short account ratio,
taker buy/sell ratio는 공개 REST로 얻을 수 있다. 다만 historical OI와 ratio
통계는 최근 약 30일 범위이고, top-trader account/position ratio는 현재 공식
catalog상 API key가 필요한 `MARKET_DATA`다. 따라서 무키 V1에서 funding은
표시하되 OI·ratio는 충분한 point-in-time 수집·결측 처리·ablation 전까지
방향 점수에 넣지 않는다. [Futures 공식 REST catalog](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).

## 인과적 차트 구조 계약

- 모든 신호는 폐봉 `t`에서 계산한다.
- 기준 pivot/line 상태는 `t-1`에 고정한다.
- pivot은 좌 2봉·우 2봉 fractal이며 `i+2`가 끝나야 확정된다. 진행 중인
  ZigZag leg는 사용하지 않는다.
- pivot prominence가 당시 ATR의 0.75배 이상인 후보만 쓰고 100봉 뒤
  만료한다.
- 최근 두 high와 low의 변화가 각각 0.10 ATR보다 크면 HH/HL 또는 LH/LL로
  분류한다. 그보다 작은 차이는 mixed로 처리한다.
- 두 anchor 투영선은 Discord 진단값일 뿐 고신뢰 추세선이나 진입 조건으로
  부르지 않는다. slope, 가격과의 ATR 거리, break 여부를 그대로 보여준다.
- bullish pullback은 확정 HH/HL, 2 ATR 이상 impulse, 20–60% depth,
  12봉 이하, 구조 미붕괴, 눌림 극값 직전의 EMA20·ATR 기준 0.25 ATR 이내
  접점, 종가의 직전 high 회복을 모두 요구한다. impulse의 ATR 기준은 끝
  pivot이 확정된 시점에 고정하여 이후 변동성으로 과거 impulse를 다시
  판정하지 않는다. bearish는 정확한 mirror다.
- 위 숫자는 OOS로 검증된 보편값이 아니다. 그래서 기본 설정에서도
  `informational` WATCH/SETUP만 가능하고 `CONFIRMED` 진입은 불가능하다.
- 공용 feature/structure API도 사용 prefix에 미확정 봉이 있으면 즉시
  실패하고, NaN/Inf 지표값은 모델 경계에서 거부한다.

Pivot/ZigZag의 오른쪽 window와 repaint 위험은 [Stock Indicators Pivots](https://python.stockindicators.dev/indicators/Pivots/)와
[ZigZag](https://python.stockindicators.dev/indicators/ZigZag/)의 공개 계약과
일치한다. Lo–Mamaysky–Wang은 차트 패턴의 기계적 formalization 가능성을
보였지만 보편 수익을 증명한 것은 아니다. [NBER 원문](https://www.nber.org/papers/w7613).

## 연구가 지지하는 범위와 충돌

- 암호화폐 추세·모멘텀과 기술 규칙에 유의한 결과를 보고한 연구가 있다.
  [Liu–Tsyvinski](https://doi.org/10.1093/rfs/hhaa113),
  [Hudson–Urquhart](https://doi.org/10.1007/s10479-019-03357-1).
- 그러나 비용, 다중검정, 시기 외 표본을 엄격히 적용하면 결과가 약해지거나
  사라진 연구도 있다. [Anghel](https://doi.org/10.1016/j.frl.2020.101655),
  [Deprez–Frömmel](https://doi.org/10.1016/j.iref.2024.05.003).
- 거래량은 가격 이외의 참여 정보가 될 수 있지만 임의의 `1.5x` 같은 임계값을
  보편적으로 검증하지 않는다. [Balcilar et al.](https://doi.org/10.1016/j.econmod.2017.03.019).
- perpetual funding은 가격 괴리와 carry/crowding을 이해하는 데 유용하지만
  funding 부호가 곧 다음 수익 방향이라는 뜻은 아니다.
  [He et al.](https://arxiv.org/abs/2212.06888).
- Fibonacci zone이 임의의 non-Fibonacci zone보다 우월하다는 근거는 약해
  점수에 넣지 않는다. [Tsinaslanidis et al.](https://doi.org/10.1016/j.eswa.2021.115893).
- 수천 규칙 중 사후 승자를 고르면 우연한 승자를 찾기 쉽다. 모든 승격은
  chronological walk-forward, 비용·슬리피지·funding, untouched holdout,
  multiple-testing 통제를 요구한다. [Sullivan–Timmermann–White](https://doi.org/10.1111/0022-1082.00163),
  [White's Reality Check](https://doi.org/10.1111/1468-0262.00152).

## 확률로 승격하는 조건

`72/100` 규칙 점수를 `상승확률 72%`로 바꾸지 않는다. 확률은 자산,
timeframe, 예측 horizon, 수수료·슬리피지를 넘는 목표수익 label을 먼저
고정한 뒤 시간순 OOS에서 별도 calibration해야 한다. Brier score, log loss,
reliability curve와 coverage를 통과하기 전에는 Discord에 확률을 표시하지
않는다. [Niculescu-Mizil–Caruana](https://doi.org/10.1145/1102351.1102430),
[Gneiting–Raftery](https://doi.org/10.1198/016214506000001437).

## 다음 검증 순서

1. 현재 price/volume 구조를 B0으로 고정한다.
2. causal structure/pullback은 shadow event만 수집한다.
3. OI, basis, public global ratio, order-book depth는 각각 한 정보군씩 추가해
   ablation한다. 결측을 0으로 바꾸지 않는다.
4. 자산·시장·regime별 coverage, false-alert rate, MFE/MAE, 비용 후 수익,
   calibration을 기간순으로 비교한다.
5. untouched prospective 표본을 통과한 규칙만 새 rule version에서 승격한다.

이 절차는 정확도를 보장하지 않는다. 다만 repaint, 중복투표, 미래정보,
데이터 스누핑 때문에 겉보기 정확도가 부풀어 오르는 경로를 차단한다.
