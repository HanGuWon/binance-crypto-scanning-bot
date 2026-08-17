# Wilder 지표 구현 감사 (재튜닝 없음)

## 결론

현재 `src/signalbot/indicators/core.py`의 True Range, ATR, RSI, 방향성 움직임(DM),
DX 및 ADX 계산식은 Wilder 원문의 정의와 일치한다. 전략의 기간, 임계값, 점수,
진입/종료 규칙을 바꿀 근거는 발견하지 못했고 어떤 재튜닝도 하지 않았다.

단, `adx_series`의 짧은 입력 길이 guard에는 인과성 결함이 하나 있었다. 현재 구현의
인덱싱 규약에서는 첫 ADX가 `2 * period - 2` 인덱스에서 이미 계산 가능하지만,
함수는 캔들이 하나 더 들어올 때까지 전부 `None`을 반환했다. 그 결과 동일 prefix가
나중 호출에서 과거 인덱스에 소급해 채워졌다. 최소 입력 길이를 `2 * period - 1`로
바꾸고 `period <= 0`을 명시적으로 거부해, 최초로 인과적으로 이용 가능한 ADX만
즉시 노출하도록 고쳤다. 이후 ADX 값, 전략 임계값, R3 신호 파라미터에는 변화가 없다.

## 검토 범위와 원문 위치

- 원문: J. Welles Wilder Jr., *New Concepts in Technical Trading Systems*,
  Trend Research, 1978. 사용자가 제공한 130쪽 스캔본을 이미지로 렌더링해 확인했다.
- True Range와 14기간 평활: PDF 23-24쪽(책 인쇄면 21-22쪽).
- Directional Movement, inside/outside day 및 평활: PDF 37-41쪽
  (책 인쇄면 36-40쪽).
- RSI 공식, 최초 평균 및 재귀 평활: PDF 66쪽과 68쪽
  (책 인쇄면 65쪽과 67쪽).
- RSI 해석과 단일 도구의 한계: PDF 69쪽과 71쪽
  (책 인쇄면 68쪽과 70쪽).

이 PDF는 텍스트 레이어가 없는 스캔본이므로 검색 텍스트에 의존하지 않고 위 페이지를
직접 시각 검토했다. 페이지 번호는 PDF 파일의 물리 페이지와 책에 인쇄된 페이지를
함께 적었다.

## 수식별 대조

| 항목 | Wilder 원문 정의 | 현재 구현 | 판정 |
|---|---|---|---|
| True Range | `max(H-L, abs(H-C_prev), abs(L-C_prev))` | `true_ranges`가 같은 세 거리를 계산한다. 최초 캔들은 선행 종가가 없어 `H-L`을 사용한다. | 일치 |
| ATR/Wilder 평활 | 최초 기간의 산술평균 후 `(이전 평균 * (n-1) + 현재값) / n` | `wilder_series`와 `atr_series`가 동일하다. | 일치 |
| RSI | `100 - 100 / (1 + 평균상승/평균하락)`, 최초 n개 변화량 평균 후 같은 Wilder 재귀 갱신 | `rsi_series`가 최초 seed와 이후 gain/loss를 각각 같은 방식으로 갱신한다. | 일치 |
| +DM/-DM | 둘 중 더 큰 양의 이동만 인정하고, outside day도 큰 쪽 하나만 사용하며 inside day는 둘 다 0 | `adx_series`의 `up > down and up > 0`, `down > up and down > 0` 조건과 같다. | 일치 |
| DX/ADX | `DX = 100 * abs(+DI - -DI) / (+DI + -DI)`, 최초 ADX는 n개 DX 평균, 이후 Wilder 평활 | 동일한 비율과 seed/재귀 갱신을 사용한다. 합계 대신 동일 배율의 평균을 쓰므로 DI와 DX 비율은 변하지 않는다. | 일치 |

## 독립 경계 테스트

`tests/unit/test_indicators_wilder.py`에 다음 손계산 기준을 고정했다.

1. 갭 상승/하락에서 단순 고저폭이 아니라 세 거리 중 최대값을 고르는 True Range.
2. ATR의 최초 SMA seed와 다음 캔들의 Wilder 재귀값.
3. RSI의 최초 gain/loss 평균, 다음 재귀값, 완전 횡보(50), 연속 상승(100),
   연속 하락(0) 경계.
4. outside day에서 큰 DM 한쪽만 남고 inside day에서 양쪽이 0이 되는 period=2
   ADX 손계산 예제.
5. 첫 ADX가 이용 가능한 `2 * period - 2` 인덱스의 prefix 안정성과
   비양수 period의 명시적 unavailable 처리.

RSI가 완전히 횡보할 때 원문의 분수는 `0/0`으로 정의되지 않는다. 구현의 50은 중립값을
택한 명시적 경계 규약이며, 상승만 있는 경우 100 및 하락만 있는 경우 0과 함께 테스트로
고정했다.

## 전략 적용 판단

원문의 예시는 1970년대 일봉 원자재를 대상으로 한다. 따라서 14기간, RSI 70/30,
ADX/ADXR 해석 등을 Binance 5분봉의 새 진입 규칙이나 임계값으로 직접 이전할 수 없다.
특히 원문도 RSI를 가격 차트와 함께 쓰는 입력 중 하나로 설명하고 어떤 단일 도구도 항상
정답을 내지 않는다고 명시한다. 이번 반영 범위는 계산 정확성과 인과적 가용성 검증뿐이며,
R3의 사전 고정된 전략과 백테스트 판정 기준은 그대로 유지한다.
