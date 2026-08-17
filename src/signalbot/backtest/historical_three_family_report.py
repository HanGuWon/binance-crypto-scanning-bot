"""Pure Korean report renderer for the historical three-family experiment."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import ROUND_HALF_UP, Decimal, localcontext
from fractions import Fraction
from typing import Final, cast

from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
)
from signalbot.backtest.historical_three_family_te0 import (
    HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
)
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HistoricalExactRationalV2,
    HistoricalThreeFamilyBootstrapBucketV2,
    HistoricalThreeFamilyBootstrapComparisonV2,
    HistoricalThreeFamilyBootstrapEndpointV2,
    HistoricalThreeFamilyBootstrapErrorV2,
    HistoricalThreeFamilyBootstrapMetricV2,
    HistoricalThreeFamilyBootstrapV2,
    canonical_historical_three_family_bootstrap_v2,
)

HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_KO_REPORT_V1_FROZEN"
)

_SHA256_RE_LENGTH: Final = 64


class HistoricalThreeFamilyReportErrorV2(ValueError):
    """Raised when report inputs do not reconcile to one historical run."""


def render_historical_three_family_report_ko_v2(
    *,
    census_results: Mapping[str, object],
    fixed_horizon_results: Mapping[str, object],
    bootstrap: HistoricalThreeFamilyBootstrapV2,
    te0_results: Mapping[str, object],
) -> str:
    """Render one deterministic Korean Markdown report without file or network I/O."""

    census = _validate_census(census_results)
    outcomes = _validate_outcomes(fixed_horizon_results)
    te0 = _validate_te0(te0_results)
    try:
        canonical_historical_three_family_bootstrap_v2(bootstrap)
    except HistoricalThreeFamilyBootstrapErrorV2 as exc:
        raise HistoricalThreeFamilyReportErrorV2(
            "bootstrap result failed canonical validation"
        ) from exc
    _reconcile_documents(census, outcomes, bootstrap, te0)

    topology = _mapping(census.get("topology_analysis"), "census topology_analysis")
    reconciliation = _mapping(
        topology.get("admission_reconciliation"),
        "census admission_reconciliation",
    )
    census_complete = _boolean(census.get("census_complete"), "census_complete")
    authenticated = _integer(census.get("authenticated_anchors"), "authenticated_anchors")
    consensus_rows = _integer(census.get("consensus_rows"), "consensus_rows")
    admitted = _integer(
        reconciliation.get("source_admitted_rows"),
        "source_admitted_rows",
    )
    clean = _integer(
        reconciliation.get("clean_primary_audit_eligible_rows"),
        "clean_primary_audit_eligible_rows",
    )
    conflicted = _integer(
        reconciliation.get("conflicted_comparator_eligible_rows"),
        "conflicted_comparator_eligible_rows",
    )
    outcome_rows = _integer(outcomes.get("outcome_rows"), "outcome_rows")
    evaluable_outcomes = _integer(
        outcomes.get("evaluable_outcomes"),
        "evaluable_outcomes",
    )
    admitted_events = _integer(outcomes.get("admitted_events"), "admitted_events")
    te0_rows = _integer(te0.get("result_rows"), "TE0 result_rows")
    te0_evaluable = _integer(te0.get("evaluable_rows"), "TE0 evaluable_rows")
    positive, comparable = _positive_primary_mean_contrasts(bootstrap)

    lines = [
        "# 3개 증거군 역사적 백테스트 감사 보고서",
        "",
        f"> 보고서 계약: `{HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2}`",
        "",
        "## 한눈에 보는 결론",
        "",
        (
            "이번 결과는 이미 노출된 과거 구간의 기술적 진단입니다. "
            "수익 확률, 효능 검증, 실거래 승격 또는 주문 지시가 아닙니다."
        ),
        (
            f"현재 3/3-깨끗한 2+중립 평균 순수익 차이는 평가 가능한 "
            f"방향/기간 셀 {comparable}개 중 {positive}개에서 양수였습니다. "
            "이 부호 개수에는 다중검정 보정이나 전향 검증 의미가 없습니다."
        ),
        "",
        "## 실행 및 대사 상태",
        "",
        f"- 센서스 상태: {'완전 실행' if census_complete else '진단 제한 실행(미완료)'}",
        f"- 인증 anchor: {authenticated:,}개; consensus 행: {consensus_rows:,}개",
        f"- 원래 clean/broad 입장 허용: {admitted:,}개; clean 대사: {clean:,}개",
        f"- 별도 conflicted 2대1 후보: {conflicted:,}개(깨끗한 2+중립과 합산 금지)",
        (
            f"- 고정기간: 이벤트 {admitted_events:,}개, 결과 {outcome_rows:,}행, "
            f"평가 가능 {evaluable_outcomes:,}행"
        ),
        f"- TE0: 결과/제외 {te0_rows:,}행, 평가 가능 {te0_evaluable:,}행",
        (
            f"- 부트스트랩: 7 UTC일 circular block, {bootstrap.samples:,}회, "
            f"seed {bootstrap.seed}, 공유 schedule `{bootstrap.shared_draw_schedule_sha256}`"
        ),
        "",
        "## 기존 V1A 실패 기준선",
        "",
        (
            "기존 V1A 검증 60분 결과는 선택 704/3,115건, 평균 순수익 -35.62bp, "
            "전체 기준선 -34.75bp, uplift -0.87bp, profit factor 0.452, "
            "엄격 적중률 34.66%였습니다. gross는 +0.23bp였지만 비용 drag가 "
            "35.85bp였습니다. 따라서 단순 지표 점수 증가는 개선으로 간주할 수 없습니다."
        ),
        "",
        "## 증거 폭별 고정기간 결과",
        "",
        "| 기간 | 방향 | 버킷 | 평가/전체 | 평균 net(bp) | 적중률 | PF | 판정 |",
        "|---:|:---:|:---|---:|---:|---:|---:|:---|",
    ]
    for cell in bootstrap.cells:
        if (
            cell.bucket is HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1
            and bootstrap.conflicted_outcome_protocol_version is None
        ):
            continue
        mean = _cell_endpoint(
            cell.endpoints,
            HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
        )
        hit = _cell_endpoint(
            cell.endpoints,
            HistoricalThreeFamilyBootstrapMetricV2.STRICT_AFTER_COST_HIT_RATE_MICROS,
        )
        profit_factor = _cell_endpoint(
            cell.endpoints,
            HistoricalThreeFamilyBootstrapMetricV2.PROFIT_FACTOR_MICROS,
        )
        lines.append(
            "| "
            f"{cell.horizon_bars * 5}m | {cell.side.value} | {cell.bucket.value} | "
            f"{cell.evaluable:,}/{cell.events:,} | "
            f"{_format_scaled(mean.point_estimate, divisor=100, places=2)} | "
            f"{_format_scaled(hit.point_estimate, divisor=10_000, places=2, suffix='%')} | "
            f"{_format_scaled(profit_factor.point_estimate, divisor=1_000_000, places=3)} | "
            f"{cell.feasibility.value} |"
        )

    lines.extend(
        [
            "",
            "## 3/3 대비 사전 선언 비교",
            "",
            (
                "| 비교 | 기간 | 방향 | n(3/3/비교군) | 평균 차이(bp) | "
                "95% 구간(bp) | 단측 p(진단) | 판정 |"
            ),
            "|:---|---:|:---:|---:|---:|:---:|---:|:---|",
        ]
    )
    for contrast in bootstrap.contrasts:
        mean = _cell_endpoint(
            contrast.endpoints,
            HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
        )
        interval = mean.two_sided_percentile_95_interval
        interval_text = (
            "NA"
            if interval is None
            else (
                f"[{_format_scaled(interval[0], divisor=100, places=2)}, "
                f"{_format_scaled(interval[1], divisor=100, places=2)}]"
            )
        )
        lines.append(
            "| "
            f"{contrast.comparison.value} | {contrast.horizon_bars * 5}m | "
            f"{contrast.side.value} | {contrast.broad_evaluable:,}/"
            f"{contrast.comparator_evaluable:,} | "
            f"{_format_scaled(mean.point_estimate, divisor=100, places=2)} | "
            f"{interval_text} | "
            f"{_format_scaled(mean.null_centered_one_sided_p_value, divisor=1, places=4)} | "
            f"{contrast.feasibility.value} |"
        )

    lines.extend(
        [
            "",
            "## 비용 귀속",
            "",
            (
                "각 평가 가능 행은 `gross - total_cost + funding = net`을 정확한 "
                "정수 micro-return으로 대사합니다. 아래 표는 비용 companion이 "
                "완전 제공된 경우에만 채워집니다."
            ),
            "",
            (
                "| 기간 | 방향 | 버킷 | gross(bp) | 슬리피지(bp) | 수수료(bp) | "
                "펀딩(bp) | 총비용(bp) | net(bp) | gross 양수→net 비양수 |"
            ),
            "|---:|:---:|:---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    cost_rows = 0
    for cell in bootstrap.cells:
        cost = cell.cost_attribution
        if cost is None or cost.events == 0:
            continue
        cost_rows += 1
        lines.append(
            "| "
            f"{cell.horizon_bars * 5}m | {cell.side.value} | {cell.bucket.value} | "
            f"{_format_micros(cost.mean_gross_directional_return_micros)} | "
            f"{_format_micros(cost.mean_slippage_return_micros)} | "
            f"{_format_micros(cost.mean_fee_return_micros)} | "
            f"{_format_micros(cost.mean_funding_return_micros)} | "
            f"{_format_micros(cost.mean_total_cost_micros)} | "
            f"{_format_micros(cost.mean_net_return_micros)} | "
            f"{cost.gross_to_net_hit_loss_count:,} |"
        )
    if cost_rows == 0:
        lines.append("| - | - | 비용 companion 미제공 | NA | NA | NA | NA | NA | NA | NA |")

    exclusion_counts = _mapping(te0.get("exclusion_counts"), "TE0 exclusion_counts")
    exit_counts = _mapping(te0.get("exit_reason_counts"), "TE0 exit_reason_counts")
    lines.extend(
        [
            "",
            "## TE0 기술적 종료",
            "",
            (
                "TE0는 다음 연속 bar 시가 진입, 원래 구조적 무효화 stop, 1R 이후 "
                "2ATR trail, 3개 연속 종가 trend failure, 최대 72개 bar를 적용한 "
                "겹치는 반사실 결과입니다. 반대 신호 종료는 사용하지 않습니다."
            ),
            f"- 종료 사유: {_format_counts(exit_counts)}",
            f"- 제외 사유: {_format_counts(exclusion_counts)}",
            "",
            "## 해석 제한",
            "",
            "- 모든 역사 구간은 이미 노출되어 있어 bootstrap 구간과 p값도 진단값일 뿐입니다.",
            "- 30건 미만 셀과 빈 셀은 명시적으로 `INCONCLUSIVE`이며 다른 버킷과 합치지 않습니다.",
            "- 가격 계열 RSI·MACD·EMA 등은 한 가격 증거군 안의 상관된 변환이지 독립 표가 아닙니다.",
            (
                "- 참여도는 kline 기반 all-trades-normal 역사 proxy이며 "
                "exact aggTrade M1과 동등하지 않습니다."
            ),
            (
                "- 횡단면은 고정 7자산 target-excluded 역사 proxy이며 "
                "live 최소-universe 보증이 아닙니다."
            ),
            "- 역사 BBO·depth·latency·impact가 없으므로 실제 체결 가능성을 입증하지 않습니다.",
            (
                "- 고정기간 결과는 서로 겹칠 수 있어 합산 수익이나 최대낙폭을 "
                "포트폴리오 성과로 부르지 않습니다."
            ),
            (
                "- 다중검정, 전향 PAPER/BBO 재현, 확률 calibration, 실거래 승격은 "
                "아직 완료되지 않았습니다."
            ),
            "- 이 스캐너는 알림 전용이며 주문 배치 기능을 구현하거나 암시하지 않습니다.",
            "",
        ]
    )
    return "\n".join(lines)


def historical_three_family_report_sha256_v2(markdown: str) -> str:
    """Hash an LF-only deterministic report after validating its text boundary."""

    if not isinstance(markdown, str) or not markdown.endswith("\n") or "\r" in markdown:
        raise HistoricalThreeFamilyReportErrorV2(
            "report must be LF-only text with one trailing newline"
        )
    return hashlib.sha256(markdown.encode("utf-8")).hexdigest()


def _validate_census(value: Mapping[str, object]) -> Mapping[str, object]:
    document = _mapping(value, "census results")
    if document.get("protocol") != HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2:
        raise HistoricalThreeFamilyReportErrorV2("census protocol is not frozen")
    _fixed_false_claims(document, "census")
    if document.get("outcome_data_read") is not False:
        raise HistoricalThreeFamilyReportErrorV2("census must remain outcome blind")
    return document


def _validate_outcomes(value: Mapping[str, object]) -> Mapping[str, object]:
    document = _mapping(value, "fixed-horizon results")
    if document.get("protocol") != HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2:
        raise HistoricalThreeFamilyReportErrorV2("fixed-horizon protocol is not frozen")
    _fixed_false_claims(document, "fixed-horizon")
    if (
        document.get("bootstrap_included") is not False
        or document.get("multiplicity_claim") is not False
        or document.get("order_placement") is not False
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            "fixed-horizon report claims differ from the frozen boundary"
        )
    return document


def _validate_te0(value: Mapping[str, object]) -> Mapping[str, object]:
    document = _mapping(value, "TE0 results")
    if document.get("protocol") != HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2:
        raise HistoricalThreeFamilyReportErrorV2("TE0 protocol is not frozen")
    _fixed_false_claims(document, "TE0")
    if (
        document.get("opposite_signal_evaluated") is not False
        or document.get("order_placement") is not False
        or document.get("portfolio_equity_claim") is not False
        or document.get("drawdown_claim") is not False
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            "TE0 report claims differ from the frozen boundary"
        )
    return document


def _fixed_false_claims(document: Mapping[str, object], label: str) -> None:
    if (
        document.get("historical_only") is not True
        or document.get("probability") is not False
        or document.get("probability_calibrated") is not False
        or document.get("promoting") is not False
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            f"{label} historical/probability/promotion claims differ"
        )


def _reconcile_documents(
    census: Mapping[str, object],
    outcomes: Mapping[str, object],
    bootstrap: HistoricalThreeFamilyBootstrapV2,
    te0: Mapping[str, object],
) -> None:
    execution_hashes = {
        _sha256(census.get("execution_contract_sha256"), "census execution contract"),
        _sha256(outcomes.get("execution_contract_sha256"), "outcome execution contract"),
        _sha256(te0.get("execution_contract_sha256"), "TE0 execution contract"),
        bootstrap.execution_contract_sha256,
    }
    if len(execution_hashes) != 1:
        raise HistoricalThreeFamilyReportErrorV2(
            "report inputs do not share one execution/cost contract"
        )
    for label in ("census_manifest_sha256", "consensus_sha256"):
        if _sha256(outcomes.get(label), f"outcome {label}") != _sha256(
            te0.get(label), f"TE0 {label}"
        ):
            raise HistoricalThreeFamilyReportErrorV2(
                f"outcome and TE0 {label} values differ"
            )
    if _integer(outcomes.get("census_rows"), "outcome census_rows") != _integer(
        te0.get("census_rows"), "TE0 census_rows"
    ):
        raise HistoricalThreeFamilyReportErrorV2("outcome and TE0 census counts differ")
    if _integer(census.get("consensus_rows"), "census consensus_rows") != _integer(
        outcomes.get("census_rows"), "outcome census_rows"
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            "census and downstream consensus row counts differ"
        )
    admitted_events = _integer(outcomes.get("admitted_events"), "outcome admitted_events")
    if admitted_events != _integer(te0.get("admitted_events"), "TE0 admitted_events"):
        raise HistoricalThreeFamilyReportErrorV2("outcome and TE0 admitted counts differ")
    if admitted_events != bootstrap.primary_event_count:
        raise HistoricalThreeFamilyReportErrorV2(
            "bootstrap primary event count differs from fixed-horizon results"
        )
    if _integer(outcomes.get("outcome_rows"), "outcome_rows") != (
        bootstrap.primary_outcome_count
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            "bootstrap primary outcome count differs from fixed-horizon results"
        )


def _positive_primary_mean_contrasts(
    bootstrap: HistoricalThreeFamilyBootstrapV2,
) -> tuple[int, int]:
    values: list[Fraction] = []
    for contrast in bootstrap.contrasts:
        if contrast.comparison is not (
            HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL
        ):
            continue
        endpoint = _cell_endpoint(
            contrast.endpoints,
            HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
        )
        if endpoint.point_estimate is not None:
            values.append(endpoint.point_estimate.as_fraction())
    return sum(value > 0 for value in values), len(values)


def _cell_endpoint(
    endpoints: tuple[HistoricalThreeFamilyBootstrapEndpointV2, ...],
    metric: HistoricalThreeFamilyBootstrapMetricV2,
) -> HistoricalThreeFamilyBootstrapEndpointV2:
    for value in endpoints:
        if getattr(value, "metric", None) is metric:
            return value
    raise HistoricalThreeFamilyReportErrorV2(f"bootstrap endpoint {metric.value} is missing")


def _format_scaled(
    value: HistoricalExactRationalV2 | None,
    *,
    divisor: int,
    places: int,
    suffix: str = "",
) -> str:
    if value is None:
        return "NA"
    fraction = value.as_fraction()
    with localcontext() as context:
        context.prec = 40
        context.rounding = ROUND_HALF_UP
        decimal = Decimal(fraction.numerator) / Decimal(fraction.denominator) / Decimal(divisor)
        return f"{decimal:.{places}f}{suffix}"


def _format_micros(value: int | None) -> str:
    if value is None:
        return "NA"
    return _format_scaled(HistoricalExactRationalV2(str(value), "1"), divisor=100, places=2)


def _format_counts(value: Mapping[str, object]) -> str:
    if not value:
        return "없음"
    documents: list[str] = []
    for key in sorted(value):
        documents.append(f"{key}={_integer(value[key], f'count {key}'):,}")
    return ", ".join(documents)


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise HistoricalThreeFamilyReportErrorV2(f"{label} must be a string-key mapping")
    return cast(Mapping[str, object], value)


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise HistoricalThreeFamilyReportErrorV2(f"{label} must be a nonnegative integer")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise HistoricalThreeFamilyReportErrorV2(f"{label} must be Boolean")
    return value


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_RE_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise HistoricalThreeFamilyReportErrorV2(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value
