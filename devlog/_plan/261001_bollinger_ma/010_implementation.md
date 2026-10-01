# 변경 단위와 검증

1. src/signalbot/signals/market_watch.py MODIFY. private 순수 helper는 candle의
   high/low 접촉 및 FeatureSnapshot만 사용한다. EMA20>EMA50 & slope>0이면 up,
   반대이면 down, 동률/0/배열-기울기 불일치는 mixed. 비유한 필수값은 unavailable.
   종가 vs EMA20/50/SMA20는 above/equal/below/unavailable. ma_context에 버전,
   as_of_ms/timeframe, 배열/위치, 짧은 summary, band_setup/해석/확인 조건을 저장.
   both touch는 먼저 방향 보류. upper+up: 종가>=EMA20면 상승 지속 관찰, 아래면
   상승근거 약화. lower+up: 종가>=EMA50면 눌림 반등 관찰, 아래면 상승근거 약화.
   lower+down/upper+down은 대칭. mixed는 방향판단 보류. equality는 위치 equal로
   보존하며 매매 확정으로 표현하지 않는다. 누락 밴드는 접촉으로 판단하지 않는다.
   모든 watch의 common metadata에 추가하되 triggered/score/eligible/이유/ID 유지.
2. src/signalbot/alerts/embeds.py MODIFY. ma_context 버전·시각·timeframe이 해당
   decision과 일치하면 시장상황에 배열 요약 한줄. BB조건 필드에 접촉+MA 해석 및
   확인선 숫자/다음 폐봉 확인 조건을 표시. BB 사건이 묶여도 해당 context를 표시.
   기존 밴드 조건해제와 후보 무효화는 남긴다. simple 필드수3(유지종목4) 유지.
   detailed market-watch는 추가 관계와 SMA20/EMA 구분을 기존 추세 필드에 표시.
   예전 payload/metadata에 context 없으면 과거 출력 유지. raw JSON원본 저장 유지.
3. tests/unit/test_bollinger_ma_context.py NEW. 공개 engine.evaluate와 실제 formatter,
   state_machine/repository 경로로 결과를 검증. up/down continuation·pullback·약화,
   EMA equality/기울기0/불일치·SMA20≠EMA20/누락·양쪽 touch·정확 MA 경계,
   종합상승인데 이평mixed, 미래/미완성 입력 제외, metadata만 달라도 기존 event IDs
   및 trigger/eligible/score/Discord 발송수 불변, 간결필드수/예전 metadata fallback.
4. docs/OPERATIONS.md MODIFY. 분석 조건·5분의20/50봉 의미와 후행성·교차발생이
   아닌 배열·중심 SMA유지·설명용조건/확정신호아님·새알림증가없음을 기록.
5. ignored var/deploy-ma-20261001에 실제 formatter 기반 사례 Markdown/rollout 기록.
   새 source SHA의 full gates만 배포하고 staged 공개자료 확인은 Discord를 보내지
   않는다. uncertain outbox 재발송/기존 DB복원은 하지 않는다.

실행: uv run ruff check .; uv run pyright; uv run pytest -q;
uv run python -m compileall -q src tests. 전체검사 전 관련 unit suites를 한번 실행한다.
