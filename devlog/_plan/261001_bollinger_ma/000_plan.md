# 볼린저 접촉과 이동평균선 해석

분류: C4 OCI release. 기존 f8aa851의 간결 알림에 이평 조건을 함께 해석한다.
기존 EMA20/50 추세 투표는 있지만, 볼린저 터치는 고가/저가로 독립 감지한다.
투표의 종합 추세와 엄격한 이평 배열을 구분해야 한다. 기존 알림 후보·빈도·ID,
진입 규칙·모의 포지션·주문 기능은 변경하지 않는다.

읽기전용 architect 조사와 제안: /root/algorithm_research_audit.
기존 market_watch.py owner에 순수 context helper와 primitive metadata를 추가하고
embeds.py의 기존 필드에 짧게 표시한다. 새 점수·이평 기간·데이터 수집은 불필요하다.
상단+상승 배열인데 EMA20 아래 마감하는 분기와 대칭, 양쪽 밴드 접촉 우선 처리를
추가하라는 제안을 수용했다. 자세한 변경은 010_implementation.md.

연구: 2026-10-01 원문 확인. John Bollinger의 규칙 6·7·8은 터치 자체는 역방향
진입 신호가 아니며 추세 속 밴드 이동/외부 종가의 연장을 설명한다. 규칙 4는 관련
지표의 중복 확신을 경계하고 12는 전통적 중심선이 SMA임을 설명한다.
https://www.bollingerbands.com/bollinger-band-rules
Fidelity EMA 설명은 이평 방향과 눌림/반등의 지지·저항 관찰, EMA의 민감도와
후행성을 설명한다. 이 자료는 Binance 5분봉 EMA20/50의 수익성을 검증하지 않는다.
https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/ema

구현은 위 원칙을 적용한 설명 규칙이다. EMA20/50을 독립적인 확률 점수로 더하지
않고 기존 기간을 재사용한다. SMA20 중심선을 EMA20으로 바꾸지 않는다.
신규 골든/데드크로스 발생이나 추세 반전 확정은 단일 snapshot으로 주장하지 않는다.

합격: 배열과 종가 관계·동률·기울기0·양방향·양쪽 접촉·중심선 누락·기존 종합label
충돌·인과성·이벤트ID/발송수 불변 테스트, 기존 간결3필드 유지와 detailed 원본 호환.
독립 A/B 감사, Ruff/Pyright/전체pytest/compileall 후 SHA 고정 archive로 staging,
DB/설정 backup과 원자 release 교체. 새 폐봉 metadata와 실제 Discord 전송 또는
자연 신호 부재 시 표시검증 범위를 구분한다. f8aa851과 기존 설정으로 복구 가능.

같은 architect reflection ALIGNED. invalid context는 추가 설명만 생략하고 기존
알림 유지할 것을 명시했고 실제 formatter 음성 테스트에 반영했다. 독립 A/B 감사
PASS, 차단 결함0. 신규·관련47개 테스트 통과(1.18s). B의 보완 의견을 따라 종가
위치 계산은 snapshot.price에 의존하지 않고 실제 closed candle.close를 사용한다.
최종 전체검증과 immutable OCI release를 진행한다.
