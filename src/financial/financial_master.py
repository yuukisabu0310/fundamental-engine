"""
FinancialMaster
Normalizer出力からBS/PL/CFの生Factを統合し、financial-dataset用の構造を生成する。

出力するのは財務諸表に記載された不可逆なFactのみ。
Derived指標（ROE, ROA, マージン, 成長率, EPS等）はvaluation-engineの責務であり、
このモジュールでは一切算出しない。

会計定義の明示・period保持。EPSは再計算可能なためFactレイクに含めない。
valuation-engineで net_income / total_number_of_issued_shares から算出する。
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _resolve_equity(bs: dict[str, Any]) -> float | None:
    """Equity統合。優先順位: shareholders_equity > equity > net_assets。"""
    for key in ("shareholders_equity", "equity", "net_assets"):
        v = bs.get(key)
        if v is not None and isinstance(v, (int, float)):
            return float(v)
    return None


def _resolve_interest_bearing_debt(bs: dict[str, Any]) -> float | None:
    """InterestBearingDebt。XBRLタグが存在する場合のみ返す（内訳合算は行わない）。"""
    v = bs.get("interest_bearing_debt")
    if v is not None and isinstance(v, (int, float)):
        return float(v)
    return None


def _safe_float(value: Any) -> float | None:
    """None安全にfloatへ変換。"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    """None安全にintへ変換。"""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_facts(
    pl: dict[str, Any],
    bs: dict[str, Any],
    cf: dict[str, Any],
) -> dict[str, float | int | None]:
    """
    単年分のPL/BS/CFから財務Factのみを抽出する。
    値が取得できなかった項目は None として保持する。
    """
    return {
        "total_assets": _safe_float(bs.get("total_assets")),
        "equity": _resolve_equity(bs),
        "interest_bearing_debt": _resolve_interest_bearing_debt(bs),
        "net_sales": _safe_float(pl.get("net_sales")),
        "operating_income": _safe_float(pl.get("operating_income")),
        "net_income_attributable_to_parent": _safe_float(pl.get("profit_loss")),
        "total_number_of_issued_shares": _safe_int(bs.get("total_number_of_issued_shares")),
    }


class FinancialMaster:
    """
    Normalizer出力を受け取り、BS/PL/CFの生Factを統合する。
    Derived指標は算出しない。Normalizerには影響しない。
    """

    def __init__(self, normalized_data: dict[str, Any]) -> None:
        self._data = normalized_data

    def compute(self) -> dict[str, Any]:
        """
        current_year / prior_year それぞれの Fact を抽出して返す。
        有効なFactが存在しない年度はキー自体を出力しない。
        メタデータ（accounting_standard, consolidation_type）をパススルーする。
        """
        current = self._data.get("current_year") or {}
        prior = self._data.get("prior_year") or {}

        current_facts = _extract_facts(
            current.get("pl") or {}, current.get("bs") or {}, current.get("cf") or {},
        )
        prior_facts = _extract_facts(
            prior.get("pl") or {}, prior.get("bs") or {}, prior.get("cf") or {},
        )

        result: dict[str, Any] = {
            "doc_id": self._data.get("doc_id", ""),
            "security_code": self._data.get("security_code"),
            "fiscal_year_end": self._data.get("fiscal_year_end"),
            "report_type": self._data.get("report_type"),
            "consolidation_type": self._data.get("consolidation_type"),
            "accounting_standard": self._data.get("accounting_standard"),
        }

        current_has_data = any(v is not None for v in current_facts.values())
        prior_has_data = any(v is not None for v in prior_facts.values())

        if current_has_data:
            year_block: dict[str, Any] = {"metrics": current_facts}
            current_period = current.get("period")
            if current_period:
                year_block["period"] = current_period
            result["current_year"] = year_block

        if prior_has_data:
            year_block = {"metrics": prior_facts}
            prior_period = prior.get("period")
            if prior_period:
                year_block["period"] = prior_period
            result["prior_year"] = year_block

        current_count = sum(1 for v in current_facts.values() if v is not None)
        prior_count = sum(1 for v in prior_facts.values() if v is not None)
        logger.info("FinancialMaster compute: doc_id=%s, current=%d facts, prior=%d facts",
                     result["doc_id"], current_count, prior_count)
        return result
