# OCI 급등 종목 유지 추적 계획 · 2026-09-30

목표: 현재 상위 5개 알림을 유지하고, 그 안에서 24시간 거래대금 10억 USDT를 기록한 종목은 마지막 상위 5개 관측 뒤 48시간 추적한다. 상승·하락 추세, 볼린저 접촉, 박스권 매수·매도 후보를 알린다. 자동 주문은 범위 밖이다.

## 근거와 적용 범위

- [Bollinger 공식 규칙](https://www.bollingerbands.com/bollinger-band-rules): 밴드 접촉만으로 역방향 매매를 결정하지 않는다. 추세 중 밴드 추종과 반전을 구분한다.
- [Momentum and liquidity in cryptocurrencies](https://arxiv.org/abs/1904.00890): 유동성별 모멘텀 차이를 연구했다. 5분봉 선물의 수익성이나 10억/48시간 기준을 검증한 연구는 아니다.
- [Periodicity in Cryptocurrency Volatility and Liquidity](https://arxiv.org/abs/2109.12142): 변동성과 유동성의 시간 패턴 연구. 다음 개선은 시간대별 거래량 기준선과 체결 압력에 대한 가격 반응을 검증하는 것이다.
- [Pump, Dump, and then What?](https://arxiv.org/abs/2309.06608): 조직적 펌프의 후속 위험 연구. 정상 급등 종목 전체에 결과를 일반화하지 않는다.

큰 과거 거래대금은 추적 자격이며 진입 근거가 아니다. 이번 수치는 검증 전 관찰 규칙이다. 1분 체결 흐름·가격 반응, L2 호가 변화, 수수료·펀딩·슬리피지를 포함한 전진 검증은 후속 연구로 남긴다.

## 변경 지도와 합격 조건

1. domain/models 및 persistence/models/repository: 유지 추적 자격과 알림 에피소드 저장. >=1B 경계, 정확히 48h 만료, 재시작, cap10, 상장 폐지, pending outbox 중복 및 실패 트랜잭션 테스트. cap 퇴출 시 자격 종료. 시각은 UTC Unix ms.
2. universe/scanner/runtime: 성공한 REST snapshot -> typed callback -> 자격 갱신 -> 별도 candle-only 감시 집합. 기존 tradable/R2/PAPER는 그대로. PAPER 회전 보류 시 감시 집합 변경도 적용. bootstrap은 알림을 발생시키지 않는다. REST 실패 중에도 평가 시각으로 만료한다.
3. market_watch: 기존 BB/현재 순위 유지, 신규 양방향 추세 관찰 및 박스권 후보. 이전 20봉 범위, ADX<20, ER<=.30, EMA slope abs<=.25ATR/3봉, 폭>=2ATR, 양쪽 경계 왕복 봉 제외, 체결 주도 매수>=55%/<=45%, 양봉/음봉 및 경계 재진입. 중간 목표까지 거리>=추정 왕복26bps의 2배. 모두 폐봉, 정보성, eligible=False. 각 family는 false 관측으로 재무장한다.
4. enum -> domain direction validation -> decision JSON -> persisted signal/outbox -> formatter/focused policy/PAPER 제외까지 소비 경로 점검. 확률처럼 보이는 점수 대신 조건 관찰 표시. 추적 만료, 과거 최대/현재 거래대금, 추세, 체결 방향, 박스 목표/무효화 포함.
5. config/settings.oci-focused.yaml 활성화, README 계약 갱신. 기존 기본 설정은 비활성. 새 규칙 양성·음성·경계/불완전·미래 데이터 검증.
6. 전체 Ruff/Pyright/pytest/compileall 통과 후 로컬 commit으로 고정. SHA256 확인한 불변 release를 OCI에 staging, signalbot 사용자와 실제 환경에서 config/dry-run 확인. SQLite/config backup, atomic symlink switch, service 및 다음 폐봉/DB 처리 확인. 이전 release+config로 5분 이내 rollback.

## 검토 기록

C4 배포 작업. FSM/별도 goal은 시작하지 않는다. 기존 cxc-dev/dev-devops/vibe-coding-guardrails 기준을 적용한다.
Architect `/root/algorithm_research_audit` 제안: 감시 종목을 tradable과 분리하고 durable episode로 지속 알림 중복을 막는다. 수용. Selector에는 DB 대신 typed callback을 주입한다는 수정안에 ALIGNED 답변. cap 퇴출 자격 종료, 실패 중 TTL, 양방향 false 평가, 거래 비용 표기와 PAPER 보류 경로를 합격 조건에 반영했다.
Independent A audit `/root/audit_alert_focus`: PASS. 순위 스냅샷 최대4개 중 폐봉보다 먼저 관측한 최신 자료만 사용, 목록 이탈 시 durable episode 재무장/쿨다운 유지, 실제 폐봉 수급 가용성 확인을 필수 조건으로 수용했다. 기준 HEAD 8450522. 변경 전 관련 31개 테스트 통과 (1.34s).
구현 중 보강: 이전 20봉 양쪽 경계 2회 이상 접촉 및 봉 연속성 검사. 같은 종목/폐봉은 후보>밴드>추세>순위 우선으로 신규 market-watch 알림 1개만 배달하되 나머지 이벤트도 저장한다. PAPER가 거래 패널 회전을 막아도 새로운 top5는 별도 candle-only 패널에서 분석한다.
운영 기준 확인: OCI /opt/signalbot/releases/8450522, service active, NRestarts=0, root 여유17GB. 검증 중 기존 서비스를 유지한다.

구현 재감사: `/root/audit_alert_focus` GO-WITH-FIXES, blockers=0. 불완전 REST snapshot 거부, 오래된 intent 복원 범위 제한, 현재 유동성 배달 조건, 동시 사건 표시, 이탈 시각 장벽을 수정하고 관련 회귀 테스트를 추가했다. 마지막 표현 제안도 수용했다. 변경 전 원본 테스트는 수정하지 않았다. 통합 검증에서 발견한 선택 인자 전달 문제는 비시장 알림의 기존 저장 호출 계약을 보존하는 방식으로 수정했다. 관련 단위·통합 69개 테스트 통과 (2.82s). 전체 검증은 최종 코드 고정 후 실행한다.
