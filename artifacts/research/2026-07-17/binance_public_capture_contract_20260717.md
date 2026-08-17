# Binance Public Prospective Capture Contract

- 문서 ID: `BINANCE-PUBLIC-CAPTURE-CONTRACT-20260717`
- 확인일: 2026-07-17
- 대상: Binance Spot 공개 market data 및 USDⓈ-M Futures 공개 market data
- 목적: R4B Family A/B의 prospective 검정을 위한 인과적으로 감사 가능한 원자료 수집 계약
- 범위 제외: API key, user data stream, private endpoint, 계정/포지션 조회, 주문 생성·취소·체결
- 변경 범위: 연구 문서만 작성. scanner 코드와 테스트는 변경하지 않음.

## 1. 증거 등급과 해석 규칙

이 문서는 다음 두 종류를 명시적으로 구분한다.

- **[OFFICIAL]**: 2026-07-17에 확인한 Binance 공식 개발자 문서, Binance 공식 GitHub 문서 또는 공개 공식 endpoint가 명시하는 계약.
- **[DESIGN]**: R4B 인과성, 운영 안정성, gap 감사와 canary 판정을 위해 이 프로젝트가 채택할 보수적 구현 제안. Binance가 보장하는 SLA가 아니다.

문서에 수치로 제안된 polling cadence, coverage threshold, queue threshold, resync 제한, 7일 canary 기간은 모두 **[DESIGN]**이다. 반대로 WebSocket URL, payload field, sequence ID, heartbeat, stream 수, REST endpoint와 request weight는 별도 표시가 없는 한 **[OFFICIAL]**이다.

## 2. 핵심 결론

1. USDⓈ-M Futures는 2026-04-23 이후 공개 stream도 traffic class에 맞춰 `/public`과 `/market`으로 나눠야 한다. unrouted 연결은 public stream만 전달하고 aggTrade, kline, markPrice 같은 market stream은 전달하지 않는다.
2. Family A/B의 causal availability time은 Binance payload의 `E` 또는 `T`가 아니라 실제 local receipt time이다.
3. Spot `bookTicker`에는 `E` 또는 `T`가 없고 REST snapshot과 원자적으로 이어 붙이는 공식 절차도 없다. 감사 가능한 BBO는 diff depth + REST snapshot local book에서 파생한다.
4. `/fapi/v1/openInterest`는 event stream이 아니라 REST sampled state다. payload `time`으로 availability를 과거에 소급하지 않고 response receipt 이후부터만 사용한다.
5. Canary 통과는 캡처 인프라 적합성만 뜻한다. Family A/B의 경제적 유효성이나 수익성을 입증하지 않는다.

## 3. 공식 공개 URL

### 3.1 Spot

**[OFFICIAL]**

| 용도 | URL | 비고 |
|---|---|---|
| 공개 REST market-data-only | `https://data-api.binance.vision` | API key 불필요, user data 없음 |
| 공개 WebSocket market-data-only | `wss://data-stream.binance.vision:443` | API key 불필요, user data 없음 |
| 일반 Spot WebSocket | `wss://stream.binance.com:443` 또는 `:9443` | 본 계약은 market-data-only 경로를 우선 사용 |

Raw와 combined 형식:

```text
/ws/<streamName>
/stream?streams=<streamName1>/<streamName2>/...
```

Combined message envelope:

```json
{"stream":"<streamName>","data":{}}
```

stream symbol은 소문자다. timestamp 기본 단위는 milliseconds다. `timeUnit=MICROSECOND`를 URL에 추가할 수 있지만 본 계약은 이를 생략하고 모든 저장 timestamp를 Unix milliseconds UTC로 정규화한다.

예:

```text
wss://data-stream.binance.vision:443/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms/btcusdt@aggTrade/btcusdt@kline_5m
```

### 3.2 USDⓈ-M Futures

**[OFFICIAL]**

Root는 `wss://fstream.binance.com`이며 공개 수집에 필요한 routed endpoint는 다음 둘이다.

| Route | URL | 본 계약의 stream |
|---|---|---|
| high-frequency public | `wss://fstream.binance.com/public` | bookTicker, partial/diff depth |
| regular market | `wss://fstream.binance.com/market` | aggTrade, markPrice, kline |

Private route는 존재하지만 이 계약의 범위 밖이며 연결하거나 구현하지 않는다.

예:

```text
wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms
wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@kline_5m/btcusdt@markPrice@1s
```

2026-04-23 이후 unrouted URL은 public traffic만 전달한다. 예를 들어 unrouted `@depth`는 계속 동작할 수 있지만 unrouted `@markPrice`는 push되지 않는다.

## 4. 연결 수명, heartbeat와 subscription 제한

### 4.1 공식 계약

| 항목 | Spot | USDⓈ-M Futures |
|---|---:|---:|
| 최대 연결 수명 | 24시간 | 24시간 |
| 서버 ping | 20초마다 | 3분마다 |
| pong deadline | 1분 | 10분 |
| client→server message 제한 | 5/s | 10/s |
| 한 연결의 최대 streams | 1,024 | 1,024 |
| connection attempts | IP당 300/5분 | 현행 Connect 문서에 별도 수치 미기재 |

Spot의 5/s에는 PING, PONG, JSON control message가 포함된다. 받은 ping payload를 그대로 복사한 pong을 가능한 빨리 보내야 한다. unsolicited pong은 허용되지만 server ping 응답을 대신하지 못한다.

USDⓈ-M도 server ping마다 즉시 pong을 반환한다. 문서에 없는 connection-attempt 제한을 임의로 Futures에 적용하거나 수치로 주장하지 않는다.

Spot은 2026년에 `serverShutdown` event가 추가됐다. raw payload는 `e=serverShutdown`, `E=event time`이고 combined stream 이름은 `!serverShutdown`이다. 고정된 사전 예고시간은 보장되지 않으므로 받는 즉시 새 연결을 준비한다.

### 4.2 구현 제안

**[DESIGN]**

- 24시간 직전에 proactive rotation하고 구·신 연결을 짧게 overlap한다.
- overlap 중복은 deterministic key로 제거한다.
- reconnect마다 subscription을 재생성하고 depth는 반드시 snapshot부터 재동기화한다.
- reconnect는 capped exponential backoff + jitter를 사용하며 무제한 retry loop를 금지한다.
- SUBSCRIBE `id`는 venue 차이와 Spot 문서 내부의 signed/unsigned 표현 불일치를 피하도록 nonnegative integer로 고정한다.
- 고정된 stream set은 가능하면 URL combined subscription으로 연결해 control-message budget을 줄인다.

## 5. Stream payload timing과 ID 계약

### 5.1 요약

| Stream | Spot | USDⓈ-M Futures | 인과·ID상 주의점 |
|---|---|---|---|
| `bookTicker` | real-time; `u,s,b,B,a,A`; `E/T` 없음 | real-time; `u,E,T,s,b,B,a,A` | Spot BBO는 exchange-time latency를 계산할 수 없음 |
| diff `depth` | 1,000ms 또는 `@100ms`; `E,U,u,b,a` | 기본 250ms, `@500ms`, `@100ms`; `E,T,U,u,pu,b,a` | quantity는 absolute quantity, 0은 level 삭제 |
| `aggTrade` | real-time; `E,a,f,l,T,m,p,q` | 100ms aggregation; 같은 핵심 field | `a` aggregate ID, `f/l` 구성 trade ID |
| `kline_5m` | 2,000ms | 250ms | `k.t/T`, `f/L`, `x`; decision에는 `x=true`만 |
| `markPrice@1s` | 없음 | 1초, suffix 없으면 3초 | `T`는 transaction time이 아니라 next funding time |

### 5.2 BBO/bookTicker

**[OFFICIAL]**

Spot:

```text
u  order-book update ID
s  symbol
b/B best bid price/quantity
a/A best ask price/quantity
```

Spot bookTicker는 `E` 또는 `T`를 제공하지 않는다. `u`를 diff-depth의 snapshot bridge로 사용하라는 공식 알고리즘도 없으며 Spot REST bookTicker에는 update ID가 없다.

USDⓈ-M:

```text
e=bookTicker
u  update ID
E  event time
T  transaction time
s,b,B,a,A
```

Futures REST/WS 일반 book에는 Retail Price Improvement orders가 보이지 않거나 구분되지 않을 수 있으므로 executable-book 연구에서 이를 명시한다.

### 5.3 Diff depth

**[OFFICIAL]**

Spot:

```text
e=depthUpdate
E event time
s symbol
U first update ID in event
u final update ID in event
b/a changed bid/ask levels
```

USDⓈ-M은 여기에 `T=transaction time`, `pu=previous event final update ID`가 추가된다.

각 `[price, quantity]`의 quantity는 변화량이 아니라 그 가격 level의 새 absolute quantity다. quantity 0은 삭제다. 없는 level을 삭제하는 event가 오는 것은 정상일 수 있다.

### 5.4 Aggregate trade

**[OFFICIAL]**

```text
E event time
a aggregate trade ID
p price
q quantity
f/l first/last constituent trade ID
T trade time
m buyer is market maker
```

`m=true`이면 buyer가 maker이므로 aggressor는 seller로 해석할 수 있다. USDⓈ-M aggregation은 market trades만 포함하고 insurance-fund/ADL trades를 제외한다.

문서는 `a`의 전역 범위나 모든 ID의 엄격한 연속성을 보장하지 않는다.

**[DESIGN]**

- dedup key는 `(venue, symbol, a)`.
- `a` regression 또는 같은 key의 내용 불일치는 integrity error.
- positive ID skip은 missing-data suspicion으로 기록하지만 packet loss라고 단정하지 않는다.
- optional REST recovery는 원래 live completeness를 소급 복원한 것으로 취급하지 않고 `source=rest_recovery`와 실제 receipt time을 남긴다.

### 5.5 5m kline

**[OFFICIAL]**

```text
outer: e, E, s
k.t/k.T: candle start/close time
k.i: interval
k.f/k.L: first/last trade ID
k.o/c/h/l/v/n/q/V/Q
k.x: closed flag
```

**[DESIGN]**

- stable key는 `(venue,symbol,interval,k.t)`.
- `x=false` update는 raw research observation으로 저장할 수 있으나 candle signal과 decision에는 절대 사용하지 않는다.
- `x=true`가 없는 경우 REST로 검산·복구할 수 있지만 original live gap은 유지한다.

### 5.6 Mark price

**[OFFICIAL]**

USDⓈ-M `@markPrice@1s`의 핵심 field:

```text
E event time
s symbol
p mark price
i index price
P estimated settlement price
r funding rate
T next funding time
```

여기서 `T`는 transaction time이 아니다. generic field parser가 모든 `T`를 같은 의미로 취급하면 안 된다.

## 6. REST order-book snapshot과 sequencing

### 6.1 Spot 공식 절차

Snapshot:

```text
GET https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=5000
```

Weight:

| limit | weight |
|---:|---:|
| 1–100 | 5 |
| 101–500 | 25 |
| 501–1,000 | 50 |
| 1,001–5,000 | 250 |

**[OFFICIAL]**

1. diff-depth WS를 먼저 열고 event를 buffer하며 첫 `U`를 기록한다.
2. REST snapshot을 얻는다.
3. snapshot `lastUpdateId < first U`이면 snapshot을 다시 얻는다.
4. buffered event 중 `u <= lastUpdateId`를 폐기한다.
5. snapshot을 local book과 local update ID로 설치한다.
6. 이후 `u < local_id`인 event는 오래된 event로 무시한다.
7. `U > local_id + 1`이면 gap이므로 local book을 폐기하고 처음부터 재시작한다.
8. 정상 적용 후 `local_id=u`로 갱신한다.

공식 문서에는 첫 processed event가 snapshot `lastUpdateId`를 `[U,u]`에 포함해야 한다는 문장과 이후 `U > local_id+1`만 gap이라는 규칙이 함께 있어 `U=local_id+1`에서 표현상 off-by-one 불일치가 있다.

**[DESIGN — 공식 문구의 보수적 reconciliation]**

첫 bridge는 다음을 모두 만족할 때만 허용한다.

```text
u > local_id
U <= local_id + 1 <= u
```

이 식은 Binance 공식 문구의 verbatim 보장이 아니라 두 공식 규칙을 fail-closed하게 조정한 프로젝트 구현 제안이다. 불만족이면 resnapshot한다.

### 6.2 USDⓈ-M 공식 절차

Snapshot:

```text
GET https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000
```

Weight:

| limit | weight |
|---:|---:|
| 5, 10, 20, 50 | 2 |
| 100 | 5 |
| 500 | 10 |
| 1,000 | 20 |

Snapshot은 `lastUpdateId`, `E=message output time`, `T=transaction time`, bids, asks를 반환한다.

**[OFFICIAL]**

1. `wss://fstream.binance.com/public/...@depth`를 먼저 열고 buffer한다.
2. snapshot을 얻는다.
3. `u < snapshot.lastUpdateId`인 event를 버린다.
4. 첫 processed event는 `U <= lastUpdateId`와 `u >= lastUpdateId`를 모두 만족해야 한다.
5. 그다음부터 모든 event에서 `pu == previous.u`여야 한다.
6. 불일치하면 snapshot 단계부터 다시 시작한다.

두 venue 모두 snapshot 최대 level 밖의 수량은 이후 해당 level이 변하기 전까지 알 수 없으므로 local book을 거래소 full book이라고 주장하지 않는다.

## 7. 공개 REST endpoint와 rate contract

### 7.1 Spot

Base: `https://data-api.binance.vision`

| Endpoint | 공식 weight | 용도 |
|---|---:|---|
| `/api/v3/time` | 1 | `serverTime` |
| `/api/v3/exchangeInfo` | 20 | 현재 symbol/status/filter/rateLimits |
| `/api/v3/depth` | limit별 5/25/50/250 | depth snapshot |
| `/api/v3/aggTrades` | 4 | bounded recovery/검산 |
| `/api/v3/klines` | 2 | closed candle 검산 |
| `/api/v3/ticker/bookTicker`, single | 2 | 보조 BBO |
| `/api/v3/ticker/bookTicker`, symbols/all | 4 | 보조 BBO |

2026-07-17 공식 `exchangeInfo` 공개 응답에서 확인한 현행 IP limit:

```text
REQUEST_WEIGHT: 6000 / minute / IP
RAW_REQUESTS: 300000 / 5 minutes / IP
```

이 수치는 startup에 `exchangeInfo.rateLimits`를 다시 읽어 동적으로 확인하며 영구 상수로 가정하지 않는다. `X-MBX-USED-WEIGHT-*`를 저장한다. 429에서는 `Retry-After` 동안 중단하고 반복 위반에 따른 418 ban을 피한다.

Spot status parser는 `TRADING`, `END_OF_DAY`, `HALT`, `BREAK`, `CANCEL_ONLY`와 future unknown enum을 수용하되 signal eligibility는 `TRADING`만 허용한다. `exchangeInfo`는 현재 snapshot이지 historical status archive가 아니다.

### 7.2 USDⓈ-M Futures

Base: `https://fapi.binance.com`

| Endpoint | 공식 limit/weight | 핵심 field·주의점 |
|---|---|---|
| `/fapi/v1/time` | weight 1 | `serverTime` |
| `/fapi/v1/exchangeInfo` | weight 1 | 현재 trading rules/status; 문서 응답 예시 REQUEST_WEIGHT 2,400/min |
| `/fapi/v1/openInterest?symbol=` | weight 1 | `openInterest,symbol,time`; `time`은 transaction time |
| `/futures/data/openInterestHist` | weight 0, 별도 1,000 requests/5min/IP | `period=5m` 가능, 최신 1개월만 |
| `/fapi/v1/premiumIndex?symbol=` | symbol 있음 1, 없음 10 | mark/index/settle/funding/nextFunding/time |
| `/fapi/v1/premiumIndexKlines` | limit별 1/2/5/10, max 1,500 | 5m premium-index bar |
| `/fapi/v1/fundingRate` | `/fundingInfo`와 합산 500/5min/IP | 최근 funding history, ascending |
| `/fapi/v1/fundingInfo` | weight 0, 위 shared cap | 조정된 cap/floor/interval symbol만 반환 |
| `/fapi/v1/depth` | limit별 2/5/10/20 | snapshot |
| `/fapi/v1/ticker/bookTicker` | single 2, symbol 생략 5 | `time`은 transaction time; RPI 제외 |

`fundingInfo`는 전체 symbol의 완전한 funding schedule이 아니다. `/fapi/v1/fundingRate` 문서는 별도 숫자 weight보다 `/fundingInfo`와 공유하는 500/5min/IP 제한을 명시하므로 그 shared cap을 limiter에 반영한다.

## 8. Polling cadence

다음은 모두 **[DESIGN]**이며 Binance SLA 또는 공식 권장 빈도가 아니다.

| Endpoint | 3-symbol canary cadence | 근거 |
|---|---:|---|
| Spot `/api/v3/time` | 30초 | local clock offset/RTT sample |
| Spot `/api/v3/exchangeInfo` | 60초, hash-on-change | status/filter/rate-limit change capture |
| Futures `/fapi/v1/time` | 30초 | local clock offset/RTT sample |
| Futures `/fapi/v1/exchangeInfo` | 60초, hash-on-change | symbol/status/rule change capture |
| `/fapi/v1/openInterest` | symbol당 5초 | Family A의 sampled OI state |
| `/futures/data/openInterestHist?period=5m` | 각 5m bar 종료 후 지연 호출 | 검산용, primary causal sample 아님 |
| `/fapi/v1/premiumIndex?symbol=` | symbol당 30초 | WS markPrice가 primary, REST cross-check |
| `/fapi/v1/fundingRate` | `nextFundingTime+15s` 1회, bounded retry 1회 | 실제 funding 확정 확인 |
| `/fapi/v1/fundingInfo` | 5분 또는 exchangeInfo hash change 시 | adjustment capture |
| depth snapshot | startup/reconnect/gap 시만 | 정상 sequence 동안 반복 polling 금지 |

3 symbols 기준 정상 반복 부하 추정:

```text
Spot: time 2 + exchangeInfo 20 ≈ 22 weight/min
Futures: time 2 + exchangeInfo 1 + OI 36 + premiumIndex 6 ≈ 45 weight/min
Startup snapshot: Spot limit=1000 → 150 total; Futures limit=1000 → 60 total
```

Spot 20 symbols를 limit 5,000으로 동시에 resnapshot하면 weight 5,000이므로 reconnect storm에서는 snapshot을 stagger한다.

## 9. Receipt-time capture contract

### 9.1 WebSocket 필수 field

**[DESIGN]**

socket read가 완료된 즉시, JSON parsing이나 queue enqueue 전에 다음 immutable envelope를 생성한다.

```text
capture_schema_version
venue
route                       # spot | um_public | um_market
stream_name
symbol
session_id
connection_generation
frame_ordinal
recv_wall_utc_ms
recv_monotonic_ns
raw_length
raw_sha256
exchange_E
exchange_T
exchange_T_semantics
U
u
pu
aggregate_trade_id
first_trade_id
last_trade_id
kline_open_ms
kline_close_ms
kline_closed
clock_sample_id
enqueue_wall_utc_ms
persist_wall_utc_ms
```

Raw bytes를 먼저 내구성 있게 보존하고 parsing 실패 payload는 quarantine한다. parser가 알지 못하는 새 field도 raw record에 남는다.

### 9.2 REST 필수 field

```text
request_send_wall_utc_ms
request_send_monotonic_ns
response_first_byte_wall_utc_ms
response_complete_wall_utc_ms
response_complete_monotonic_ns
endpoint_and_query_hash
HTTP status
X-MBX-USED-WEIGHT-*
Retry-After
response_raw_length
response_raw_sha256
server_or_data_timestamp
clock_sample_id
```

### 9.3 Causal availability 규칙

**[DESIGN]**

- cutoff `τ`의 feature는 `recv_monotonic_ns <= τ` 또는 REST `response_complete_monotonic_ns <= τ`인 record만 사용한다.
- 서로 다른 socket의 event를 `E`만으로 global ordering하지 않는다.
- OI는 response complete부터만 사용한다. payload `time`으로 과거에 소급하지 않는다.
- `recv-E`, `recv-T`는 clock health가 정상이고 stream별 `T` 의미가 확인된 경우에만 latency metric으로 계산한다.
- cross-connection join은 receipt-time watermark를 사용한다.
- 늦게 수신된 과거 `E` event로 이미 생성된 prospective feature를 소급 변경하지 않는다.
- wall clock은 UTC epoch 기록용, monotonic clock은 순서·duration 판정용이다.

## 10. Family A/B 최소 관측 계약

### 10.1 Family A: crowding → deleveraging

필수:

- USDⓈ-M aggTrade flow
- sequence-valid Futures depth와 그로부터 파생한 BBO
- 5초 sampled current OI와 receipt interval
- mark price, premium/index, funding state
- closed 5m kline
- exchangeInfo status와 rule version

OI는 event가 아니므로 연속 변화가 아니라 두 receipt-bounded sample 사이의 state difference로 정의한다. poll gap을 가로지르는 OI change는 invalid다.

### 10.2 Family B: flow × liquidity depletion/absorption

필수:

- Spot과 USDⓈ-M aggTrade
- sequence-valid L2 diff depth
- local book에서 파생한 BBO/spread/depth imbalance
- trade aggressor side, price, quantity, aggregate/trade IDs
- 모든 stream의 receipt time과 connection generation

Spot bookTicker 단독은 reconnect gap과 exchange timestamp를 감사할 수 없어 executable-net 또는 causal BBO의 primary source로 쓰지 않는다.

## 11. Gap, staleness와 invalidation

다음은 **[DESIGN]**이다.

1. reconnect마다 새 `connection_generation`을 발급하고 이전 연결과 자동 연속이라고 가정하지 않는다.
2. Futures depth에서 `pu != previous.u`, Spot에서 `U > local_u+1`, 첫 snapshot bridge 실패 시 첫 의심 event부터 성공한 resnapshot/bridge까지 order book을 invalid 처리한다.
3. invalid interval은 forward-fill하지 않고 모든 depth-dependent feature와 signal eligibility를 차단한다.
4. Spot bookTicker reconnect 구간은 unobservable로 표시하거나 sequence-valid local depth BBO만 사용한다.
5. aggTrade ID regression 또는 같은 ID의 내용 불일치는 integrity error다. positive skip은 missing-data flag이며, 공식 연속성 보장이 없으므로 packet loss 확정으로 쓰지 않는다.
6. closed kline이 예정 close 이후 tolerance 내 도착하지 않으면 gap을 기록한다. REST recovery는 별도 source로 저장하고 original prospective gap을 지우지 않는다.
7. markPrice는 예정 cadence의 3배 이상 event가 없으면 stale이며 새 event가 올 때까지 invalid다.
8. OI poll 실패·지연은 보간하지 않고 그 gap을 가로지르는 OI delta를 무효화한다.
9. queue drop, disk failure, checksum failure, parse failure, raw counter 불일치, system-clock step이 발생하면 signal eligibility를 freeze한다.
10. `exchangeInfo.status != TRADING` 또는 unknown status면 해당 symbol을 fail-closed한다.
11. REST 429는 `Retry-After`까지 중단한다. rate-limit recovery를 busy retry하지 않는다.

## 12. 최소 3-symbol canary

### 12.1 구성

**[DESIGN]**

Symbols:

```text
BTCUSDT
ETHUSDT
SOLUSDT
```

Connections:

1. Spot combined 한 개: symbol별 `bookTicker`, `depth@100ms`, `aggTrade`, `kline_5m` = 12 streams.
2. USDⓈ-M `/public` 한 개: symbol별 `bookTicker`, `depth@100ms` = 6 streams.
3. USDⓈ-M `/market` 한 개: symbol별 `aggTrade`, `kline_5m`, `markPrice@1s` = 9 streams.

Phases:

1. 24시간 schema/throughput smoke.
2. 수정 이후 7일 연속 acceptance run.
3. 각 connection에 natural 24h rollover 최소 1회와 controlled forced disconnect 최소 1회.
4. acceptance 통과 후에만 장기 prospective Family A/B collection 시작.

### 12.2 Acceptance 기준

모두 **[DESIGN]**이며 사전 고정한다.

- subscription acknowledgement 100%.
- raw producer/frame counter와 persisted record counter 완전 일치, silent drop 0.
- depth gap 100% 탐지 및 explicit invalidation/resnapshot.
- invalid depth sequence가 feature 또는 signal에 사용된 건 0.
- stream valid-time coverage ≥99.9%.
- depth-valid coverage ≥99.5%.
- closed 5m candle은 dedup 후 `(venue,symbol,interval,open_time)`당 정확히 하나이거나 explicit gap/recovery flag.
- queue drop 0, peak occupancy <70%, 모든 queue/cache가 bounded.
- monotonic clock regression 0.
- receipt timestamp가 parse/enqueue/persist timestamp보다 항상 앞섬.
- reconnect overlap에서도 deterministic dedup 결과가 동일.
- REST 사용량이 확인된 공식 budget의 25% 미만.
- HTTP 429/418 0.
- restart 후 raw segment hash/checksum verification 100%.
- API key, user/private stream, account endpoint, order path 0.

### 12.3 Stop과 quarantine 기준

모두 **[DESIGN]**이다.

Hard fail:

- silent raw loss.
- invalid depth를 valid로 사용.
- receipt timestamp를 queue 또는 parsing 뒤에서 생성.
- checksum/disk corruption.
- unmarked queue overflow/drop.
- monotonic clock regression 또는 wall-clock step을 숨김.
- private/user/order/API-key path가 capture process에 포함됨.
- HTTP 418 또는 반복 rate-limit/IP ban.

Symbol quarantine:

- depth resync 3회 연속 실패.
- rolling 1시간 depth-invalid 비율 >1%.
- symbol status가 `TRADING`이 아님.

Connection stop:

- capped reconnect cycle 3회 연속 실패 시 uncontrolled loop 대신 상태를 `DOWN`으로 전환.
- 잘못된 routed URL로 필수 market stream이 오지 않으면 해당 run을 무효화.

Canary restart:

- schema, timestamp placement, sequencing, gap 또는 invalidation rule처럼 outcome eligibility에 영향을 주는 변경 후에는 7일 acceptance를 처음부터 다시 수행.

## 13. Promotion boundary

Canary는 다음만 입증한다.

- public-only collection이 공식 route와 rate limit을 지킨다.
- 원자료 receipt time과 sequence continuity를 감사할 수 있다.
- missingness가 숨겨지지 않고 invalid interval로 전파된다.
- closed-candle 및 no-look-ahead invariant를 지킨다.

Canary는 alpha, hit rate, Sharpe, executable net return 또는 실제 수익을 입증하지 않는다. Family A/B efficacy는 이 계약으로 수집한 별도의 장기 prospective 표본, 사전 고정 episode/execution 정의, 비용·slippage 모델과 독립 holdout으로 검정해야 한다.

## 14. 공식 출처와 확인일

모든 URL은 2026-07-17에 확인했다. USDⓈ-M Connect, migration, local-book 페이지는 페이지 표시상 2026-07-16 수정본이다.

### USDⓈ-M Futures

- Connect 및 routed URLs: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
- Base URL split/migration: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- Live subscribe/unsubscribe: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams
- Local order book: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Aggregate trade stream: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Aggregate-Trade-Streams
- Individual book ticker stream: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- Diff depth stream: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Diff-Book-Depth-Streams
- Kline stream: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Kline-Candlestick-Streams
- Mark price stream: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Mark-Price-Stream
- Consolidated REST market data: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data

### Spot

- Market-data-only endpoints: https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
- WebSocket streams, payloads, limits, local book: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- REST API and weights: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
- Changelog: https://github.com/binance/binance-spot-api-docs/blob/master/CHANGELOG.md
- Enums: https://github.com/binance/binance-spot-api-docs/blob/master/enums.md
- Public live exchangeInfo used to verify current rateLimits: https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT

## 15. Material Passport

- Evidence type: Binance official developer documentation, Binance official GitHub documentation, official public market-data endpoint.
- Retrieval/verification date: 2026-07-17.
- Intended use: prospective public-data capture engineering contract and canary gate.
- Known documentation ambiguity: Spot first depth bridge off-by-one wording; this document labels its conservative reconciliation as **[DESIGN]**.
- Known field-semantic hazard: `T` is stream-dependent; markPrice `T` is next funding time.
- Known availability hazard: Spot bookTicker has no `E/T`; OI is REST sampled state.
- Scope guarantee: no secret, API key, private/user stream or order execution is required by this contract.
