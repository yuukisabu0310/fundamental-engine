"""
FinancialMaster
Normalizer出力からBS/PL/CF/配当の生Factを統合し、financial-dataset用の構造を生成する。

出力するのは財務諸表に記載された不可逆なFactのみ。
Derived指標（ROE, ROA, マージン, 成長率, EPS等）はvaluation-engineの責務であり、
このモジュールでは一切算出しない。

EPSは再計算可能なためFactレイクに含めない。
有利子負債は構成要素を生データで保存し、合算はvaluation-engineで行う。

優先順位解決ルールは config/canonical_keys.yaml から読み込む。
"""
import logging
from typing import Any

try:
    from src.config_loader import get_fact_keys, get_normalizer_key_mapping, get_resolution_rules
except ModuleNotFoundError:
    from config_loader import get_fact_keys, get_normalizer_key_mapping, get_resolution_rules

logger = logging.getLogger(__name__)

_RESOLUTION_RULES = get_resolution_rules()
_NORMALIZER_KEY_MAP = get_normalizer_key_mapping()
_FACT_KEYS = get_fact_keys()


def _resolve_by_priority(bs: dict[str, Any], candidates: list[str]) -> float | None:
    """候補キーを優先順位で走査し、最初に有効な値を返す。"""
    for key in candidates:
        v = bs.get(key)
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
    dividend: dict[str, Any],
) -> dict[str, float | int | None]:
    """
    単年分のPL/BS/CF/配当から財務Factのみを抽出する。
    値が取得できなかった項目は None として保持する。

    resolution ルールが定義されているキーは、複数候補から優先順位で解決する。
    normalizer_key マッピングが定義されているキーは、normalizer 出力キーから変換する。
    """
    all_sources = {**pl, **bs, **cf, **dividend}

    result: dict[str, float | int | None] = {}
    for fact_key in _FACT_KEYS:
        if fact_key in _RESOLUTION_RULES:
            result[fact_key] = _resolve_by_priority(all_sources, _RESOLUTION_RULES[fact_key])
            continue

        source_key = fact_key
        for nk, ck in _NORMALIZER_KEY_MAP.items():
            if ck == fact_key:
                source_key = nk
                break

        raw_value = all_sources.get(source_key)
        if fact_key == "total_number_of_issued_shares":
            result[fact_key] = _safe_int(raw_value)
        elif fact_key == "dividends_per_share":
            result[fact_key] = _safe_float(raw_value)
        else:
            result[fact_key] = _safe_float(raw_value)

    return result


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
            current.get("pl") or {}, current.get("bs") or {},
            current.get("cf") or {}, current.get("dividend") or {},
        )
        prior_facts = _extract_facts(
            prior.get("pl") or {}, prior.get("bs") or {},
            prior.get("cf") or {}, prior.get("dividend") or {},
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
