# 알림 가독성 + 1분 체결·가격 반응

분류 C4 (OCI release). 목표는 Discord에서 상황과 수급을 빠르게 이해하는 것이다. 사용자 순서: 간결한 메시지 먼저, 기존 1분 자료를 이용한 설명 보강 다음. 단일 배포 단위이며 매매 규칙·대상·알림 빈도·event ID는 변경하지 않는다. 자동 주문, L2 호가벽/흡수 확정, 수익률 주장 제외.

Design read: 한국어로 휴대폰에서 읽는 시장 관찰 알림. 제목에 사건, 본문에 시장 상황·체결 우세·가격 반응·해제/무효화 조건. 표준 Discord embed와 색상 유지, 필드 최대4개, raw indicator 나열 제거. variance2, motion0, densityD3. 별도 이미지/웹 UI가 필요 없는 기능성 텍스트 표면이라 시각 컨셉 생성 생략.

설계 제안: `/root/algorithm_research_audit` 읽기전용 조사. 기존 CandleStore와 closed_kline_flow를 재사용한 최근3개 1분 폐봉 snapshot, 최대60초 lag, 가중 거래대금 비율·동일구간 가격반응·명시적 unavailable, 신규 이벤트 없음. 수용. detail mode는 과거 직접 formatter API 호환을 위해 helper 기본 detailed, 운영 AlertSettings 기본 simple로 두고 모든 실제 생성 경로에 전달한다.

같은 architect reflection ALIGNED. 혼합 series는 제외하고 선택한3봉 중복은거부, ±0.05% 가격변화경계는 보합으로명시. 계획과요구사항 일치. 변경전 관련87개테스트 통과(2.29s), 명시파일들이 formatter/runtime/microstructure를 실제import/호출하는검증이다.

독립 `/root/audit_alert_focus` A 감사 PASS. metadata는 JSON 기본형만 저장하고, 실제 4필드/문자제한·원본 outbox 유지 확인을 합격조건에 포함했다. 구현: 메시지 축약 먼저 완료하고, 기존1분자료 snapshot을 추가했다. 관련119개 테스트 통과(2.36s), 정적 타입 오류0. 구현 후 독립 검토와 전체검증 진행.

구현 파일·검증은 010_implementation.md. 기존 runtime/Discord outbox·SQLite 저장을 사용한다. 원본 이유·지표·근거는 SignalDecision JSON에 남긴다. 기술적 판단과 설명용 pressure의 버전을 분리해 기존 rule_version과 중복 방지를 유지한다. 전체 Ruff/Pyright/pytest/compileall, 독립 검토, 로컬 commit 고정, OCI staging/설정검사, DB·설정 백업과 release 교체, 실제 폐봉과 Discord delivery 확인. 기존 b92a981+설정으로 복구 가능하게 준비한다.

최종 독립 B 감사: 일반 방향 신호의 원본 이유가 빠지는 결함을 수정했다. 한국어로 풀어 쓴 이유 최대2개, 숫자 보존, 미지원 문자열 길이제한과 원본 decision 불변 테스트를 추가. 재감사 PASS (추가 차단 없음), 새 테스트34개 통과(1.00s). 최종 Ruff 통과, Pyright 오류0, compileall 성공; 전체 pytest는 진행중이며 성공 전 운영 교체하지 않는다.
