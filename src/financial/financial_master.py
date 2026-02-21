"""
FinancialMaster
Normalizer出力からBS/PL/CFの生Factを統合し、financial-dataset用の構造を生成する。

出力するのは財務諸表に記載された不可逆なFactのみ。
Derived指標（ROE, ROA, マージン, 成長率等）はvaluation-engineの責務であり、
このモジュールでは一切算出しない。
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


def _extract_facts(
    pl: dict[str, Any],
    bs: dict[str, Any],
    cf: dict[str, Any],
) -> dict[str, float]:
    """
    単年分のPL/BS/CFから財務Factのみを抽出する。
    値がNoneの項目は出力しない（null出力禁止）。
    """
    candidates: dict[str, float | None] = {
        "equity": _resolve_equity(bs),
        "interest_bearing_debt": _resolve_interest_bearing_debt(bs),
        "total_assets": _safe_float(bs.get("total_assets")),
        "net_sales": _safe_float(pl.get("net_sales")),
        "operating_income": _safe_float(pl.get("operating_income")),
        "profit_loss": _safe_float(pl.get("profit_loss")),
        "earnings_per_share": _safe_float(pl.get("earnings_per_share")),
    }

    return {k: v for k, v in candidates.items() if v is not None}


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
        }

        if current_facts:
            result["current_year"] = {"metrics": current_facts}
        if prior_facts:
            result["prior_year"] = {"metrics": prior_facts}

        logger.info("FinancialMaster compute: doc_id=%s, current=%d facts, prior=%d facts",
                     result["doc_id"], len(current_facts), len(prior_facts))
        return result
