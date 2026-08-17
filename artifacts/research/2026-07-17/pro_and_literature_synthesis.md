# PRO·원전·문헌 조언의 R3 반영 판정

작성 시점: 2026-07-17 KST

## 증거 범위

- GPT 5.6 SOL PRO의 R2 코드·결과 감사:
  <https://chatgpt.com/g/g-p-69a9f92954288191a063fd1eea40b983-gasanghwapye-teureiding/c/6a58ea02-1f94-83e8-876a-7a8666a87371>
- 별도 Oracle/PRO 감사:
  <https://chatgpt.com/g/g-p-69a9f92954288191a063fd1eea40b983-gasanghwapye-teureiding/c/6a58f2ac-6ee4-83e8-af82-a45533d46408>
- Wilder 원전 PRO 연구:
  <https://chatgpt.com/g/g-p-69a9f92954288191a063fd1eea40b983/c/6a58fcce-fc18-83ee-8a37-f27d88524693>
- 사용자 제공 Wilder 스캔본의 로컬 시각 감사:
  `artifacts/research/2026-07-17/wilder_indicator_audit.md`
- 사용자 제공 문헌 요약과 그 안의 원 논문은 DOI·출판사 원문을 기준으로 별도 감사했다.

PRO의 답변은 조언이며 실험 결과가 아니다. 특히 Wilder PRO 답변은 업로드된 스캔의
페이지를 직접 읽지 못하고 별도 OCR본으로 재구성했다고 명시한다. 수식과 페이지 판정은
사용자 스캔을 직접 렌더링한 로컬 감사가 우선한다.

## 즉시 반영

| 항목 | 반영 | 근거와 범위 |
|---|---|---|
| Wilder TR/ATR/RSI/DM/DX/ADX 공식 검증 | 반영 | 사용자 스캔과 현재 구현을 직접 대조했다. |
| ADX prefix 인과성 | 수정 | 첫 계산 가능 ADX가 한 봉 늦게 노출되고 미래 호출에서 과거 인덱스가 소급 채워지는 guard 결함만 수정했다. 기간·임계값·후속 값은 불변이다. |
| 완결 캔들·strict-prior HTF | 유지·강화 | 같은 시각의 타 종목 종가와 미완성 1h 정보를 배제한다. |
| BBO 신선도 경계 | 수정 | `r2_pit_htf_exec`는 BBO 나이가 0 미만이거나 설정 상한을 넘으면 거부한다. |
| 비용 인지 15/30/60분 결과 | 반영 | 다음 연속 5분봉 시가 진입, 3/6/12번째 종가 종료, 양방향 비용 후 수익과 LONG/FLAT/SHORT 라벨을 저장한다. |
| 기회·거래 provenance | 반영 | 결정적 opportunity ID, 이유, 무효화 가격, 분할, 종료 이유, 명시적 split containment를 기록한다. |
| 의존성 보존 불확실성 | 반영 | 양 시장·전 자산을 함께 보존하는 UTC 일 단위 circular moving-block bootstrap을 R3 최종 분석에 사용한다. |

## 이미 구현된 보조 기능과 활성화 경계

- ATR은 변동성 정규화와 기술적 trailing stop에 쓰이며 trailing stop은 진입 후 위험을
  다시 넓히지 않는다. 백테스트와 별도로 런타임 Discord에도 완결봉 기반 PAPER 종료
  경보가 연결됐다. 예제 설정에서는 활성화되지만 주문을 내지 않으며, 메모리 전용이라
  프로세스 재시작 뒤 기존 PAPER 포지션을 복구하지 않는다는 한계를 알림에 표시한다.
- 거래량 참여도, taker delta, normalized VPCI는 계산·검증 가능한 연구 기능으로 남아
  있다. R1에서 G2/G4가 비용 전후 모두 실패했으므로 활성 진입 gate로 복귀시키지 않는다.
- RSI와 divergence 정보는 피처로 존재하지만 R3의 5분 진입 규칙에서는 reversal family를
  비활성화한다. 사용자가 대화 중 든 RSI 예시는 독립 진입 근거로 확대 해석하지 않는다.
- Spot의 SHORT 결과 라벨은 하락/신규 long 보류/보유분 exit 경고의 연구 의미다. Spot
  신규 short 주문을 뜻하지 않는다.

## 이번 R3에서 보류

다음 항목은 합리적인 새 가설일 수 있지만, 이미 노출된 표본에 한꺼번에 추가하면
사후선택이 된다. R3 결과를 본 뒤 조용히 끼워 넣지 않는다.

- PSAR, ADXR, Commodity Selection Index식 종목 랭킹
- trend/range/shock 전략 라우터와 RSI range module
- ADX rolling-percentile 또는 새로운 ADX/ADXR 임계값
- Wilder의 3 ATR 등 일봉 원자재 상수를 5분 crypto에 직접 이식
- ATR 기반 수량 산정, portfolio heat, leverage 또는 계좌 위험 예산
- Logistic Regression, Random Forest, LightGBM/XGBoost, 앙상블과 확률 임계값
- 15/30/60분 중 가장 좋아 보이는 horizon의 사후 승격

이들은 새 사전등록, 새 데이터 창, point-in-time universe와 별도 다중시도 ledger가 있을
때만 실험 후보가 된다.

## 프로젝트 경계 때문에 기각

Wilder PRO 답변에는 exchange-native stop, OMS, 주문 ID, 부분 체결, 실제 포지션 조정,
live canary와 Binance/Alpaca 주문 경로가 포함돼 있다. 이는 일반적인 자동매매 설계
조언일 뿐 현재 프로젝트의 허가 범위가 아니다.

- production 주문, 서명, API key, 계좌·잔고·포지션 endpoint는 추가하지 않는다.
- 실제 수량 산정이나 leverage 제어도 추가하지 않는다.
- 현재 코드는 공개 Binance 시장데이터, 연구용 가상 체결, Discord 경보까지만 다룬다.

## 문헌 주장 교정

- Jaquart 등의 52.9~54.1% 전체 정확도와 57.5~59.5% 고확신 하위집합은 특정 일간
  cross-sectional 설계의 결과이지, crypto 5분봉 전체에 적용되는 보편 정확도 범위가
  아니다.
- Jaquart의 1~60분 연구는 짧은 보유기간의 비용 위험을 강하게 보여 주지만, 모든 5분
  전략이 반드시 실패한다는 정리는 아니다.
- Hafid 등의 92.4%는 같은 시점 이동평균 규칙 라벨 재현에 가까우며 미래 수익 방향
  92.4%를 뜻하지 않는다.
- Dashtaki 등은 0.1% commission을 넣었으므로 비용을 전혀 쓰지 않았다는 비판은 틀리다.
  다만 split·scaling 순서가 모호하고 spread/slippage/시점 BBO가 없다.
- Deprez·Frömmel의 대규모 규칙 연구는 단순 규칙을 연구할 가치는 뒷받침하지만, 이
  프로젝트의 C0/H1을 구제하지 않는다. R1/R2에서 C0/H1/G2/G4의 비용 후 기대값은
  음수였고 0배 modeled slippage에서도 음수였다.

## R3 해석 경계

R3는 2024-07-01~2026-07-01의 이미 노출된 8자산 표본을 인과·출처 결함을 고쳐 다시
기술하는 진단이다. 성공하더라도 untouched OOS, 실제 BBO 실행 가능성 또는 배포 승인이
되지 않는다. 결과 상태는 data integrity, kline-proxy efficacy, execution validity,
generalization, deployment의 다섯 축으로 분리한다.

## 핵심 주장-원문 식별자

| 이 문서에서 감사한 주장 | 원문 |
|---|---|
| 일간 전체/고확신 하위집합 정확도와 abstention | Jaquart et al. (2022), *Machine learning for cryptocurrency market prediction and trading*, DOI [10.1016/j.jfds.2022.12.001](https://doi.org/10.1016/j.jfds.2022.12.001) |
| 1~60분 예측과 거래비용 후 성과 소멸 | Jaquart et al. (2021), *Short-term bitcoin market prediction via machine learning*, DOI [10.1016/j.jfds.2021.03.001](https://doi.org/10.1016/j.jfds.2021.03.001) |
| 데이터 스누핑·마찰·위험요인 통제 후 희소한 유의성 | Anghel (2021), *A reality check on trading rule performance in the cryptocurrency market*, DOI [10.1016/j.frl.2020.101655](https://doi.org/10.1016/j.frl.2020.101655) |
| 75,360개 단순 규칙, 다중검정 및 비용 민감도 | Deprez & Frömmel (2024), *Are simple technical trading rules profitable in bitcoin markets?*, DOI [10.1016/j.iref.2024.05.003](https://doi.org/10.1016/j.iref.2024.05.003) |
| 92.4%가 미래수익이 아닌 MA 규칙 라벨 재현이라는 감사 | Hafid et al., *Predicting Bitcoin Market Trends with Enhanced Technical Indicator Integration*, [arXiv:2410.06935](https://arxiv.org/abs/2410.06935) |
| 0.1% commission은 포함하지만 split·spread 재현성이 불충분하다는 감사 | Dashtaki et al., *A Multisource Fusion Framework for Cryptocurrency Price Movement Prediction*, [arXiv:2409.18895](https://arxiv.org/abs/2409.18895) |
| 캔들 이미지/패턴의 제한적 추가 가치 | Duong et al., *Investigating Market Strength Prediction with CNNs on Candlestick Chart Images*, [arXiv:2501.12239](https://arxiv.org/abs/2501.12239) |

제목·식별자는 위 DOI 또는 arXiv 원문을 가리킨다. 수치 해석은 본문의 교정 범위를 따르며,
이 참고문헌 자체가 R3 표본에 대한 외부 검증이나 5분봉 수익성 증명을 뜻하지 않는다.
