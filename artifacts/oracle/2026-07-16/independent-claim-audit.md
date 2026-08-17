# GPT-5.6 Sol Pro 검토 독립 대조

검토 대화: https://chatgpt.com/c/6a582a76-0684-83ee-98d9-d4013d2f0bb9  
세션 ID: `01KXM6BHYY5JJ4N5DTJ66616ZT`  
원문 정규화본: `gpt-5.6-sol-pro-r2-review.md`  
원본 transcript SHA-256: `519ae0366b9e80e330550d28afc347634f9fe012197387a47242449299eea95f`

## 판정

| Pro 주장 | 독립 판정 | 전체 로컬 저장소 기준 근거 |
|---|---|---|
| 같은 close 시각의 심볼 도착 순서가 live regime 문맥을 바꾼다 | CONFIRMED | `MarketRuntime._handle_candle()`이 각 심볼 도착 즉시 breadth를 갱신한 뒤 평가한다. AAA→BBB 첫 breadth 1.0, BBB→AAA 첫 breadth 0.0을 재현했다. |
| signal DB 저장과 Discord 전송이 비원자적이다 | CONFIRMED | `save_signal()` commit 뒤 `decision_handler()`를 호출한다. DB commit 후 crash는 누락, 수신 후 timeout retry는 중복 가능성이 있다. |
| technical trade가 split 경계를 넘는다 | CONFIRMED | position loop는 gap에서만 reset한다. C0/G2에 2025-03-01 경계를 넘는 거래가 각각 1건 존재한다. |
| R1 전체 재현성을 인증할 수 없다 | PARTIAL | 축소 업로드 번들에는 자료가 없었지만 전체 workspace에는 원시 입력 24개, 6개 A/B run, 세 spec, plan, lockfile이 있다. 24/24 입력 hash와 C0/G2/G4 핵심 A/B 산출물 hash가 일치한다. comparator 자체 argv/input-hash manifest는 부족하다. |
| 2,000회와 50,000회 bootstrap이 모순이다 | REFUTED | plan과 ledger가 per-run 기술통계 2,000회와 common-panel comparator 50,000회를 분리한다. CLI comparator 기본값도 50,000이다. |
| anomaly가 정상화 뒤 재무장되지 않는다 | CONFIRMED | detector는 정상 tick에서 IDLE/CLEAR를 내지 않아 state가 CONFIRMED에 고착된다. |
| symbol-key 공간이 prune되지 않는다 | CONFIRMED | candle/anomaly/flow/book/regime/state/runtime feature map은 key prune이 없다. funding은 capacity exception만 있고 퇴출 심볼 제거가 없다. |
| bootstrap/gap recovery가 정상 stream과 동등하지 않다 | CONFIRMED | 둘 다 bulk insert 후 최신 feature만 갱신하거나 누락 봉을 lifecycle에 순차 재생하지 않는다. |
| live/backtest funding 계약이 다르다 | CONFIRMED | live는 최소 20 prior, 설정 lookback, 9h freshness이고 backtest는 prior 2, 30d hard-code, freshness 없음이다. |
| `[1h,4h]` RSI reversal은 live에서 dead path다 | CONFIRMED | runtime은 primary 5m이 아닌 candle을 rule evaluation 전에 반환한다. |
| 과거 자료로 BBO/depth 실행 후보를 검증할 수 있다 | REFUTED | `data/backtest`에는 kline과 funding만 있으며 historical BBO/depth/receipt-time 파일은 0개다. |

## 적용 원칙

Pro의 코드 결함 지적은 독립 확인된 항목만 수정한다. 제한된 업로드 범위 때문에 생긴 R1 재현성 오판은 채택하지 않는다. `R2_PIT_HTF_EXEC`의 BBO 부분은 소급 proxy로 대체하지 않고 prospective-only로 남긴다.

## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-07-16T10:59:24+09:00
- Verification Status: VERIFIED
- Version Label: validation_v1

