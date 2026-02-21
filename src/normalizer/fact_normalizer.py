"""
FactNormalizer
事実の正規化専用レイヤー。タグ→標準キー変換・current/prior分類・型変換・連結優先のみ。
補完・再構成・推測は行わず、FinancialMasterで処理する。
"""
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# PL（duration）: タグ部分一致 -> 出力キー（数値はint）
PL_TAGS = [
    ("NetSales", "net_sales"),
    ("OperatingIncome", "operating_income"),
    ("OrdinaryIncome", "ordinary_income"),
    ("ProfitLoss", "profit_loss"),
]

# PL EPS（duration）: 複数タグで取得、数値はfloat、unitRefは制限しない
PL_EPS_TAGS = [
    ("BasicEarningsLossPerShareSummaryOfBusinessResults", "earnings_per_share"),
    ("BasicEarningsPerShareSummaryOfBusinessResults", "earnings_per_share"),
    ("BasicEarningsPerShare", "earnings_per_share"),
    ("EarningsPerShare", "earnings_per_share"),
    ("DilutedEarningsPerShareSummaryOfBusinessResults", "diluted_earnings_per_share"),
    ("DilutedEarningsPerShare", "diluted_earnings_per_share"),
]

# BS（instant）: タグ部分一致 -> 出力キー（補完・再構成は行わない）
BS_TAGS = [
    ("TotalAssets", "total_assets"),
    ("NetAssets", "net_assets"),
    ("Equity", "equity"),
    ("ShareholdersEquity", "shareholders_equity"),
    ("CurrentAssets", "current_assets"),
    ("CurrentLiabilities", "current_liabilities"),
    ("NoncurrentLiabilities", "noncurrent_liabilities"),
    ("InterestBearingDebt", "interest_bearing_debt"),
    ("TotalNumberOfIssuedSharesSummaryOfBusinessResults", "shares_outstanding"),
    ("IssuedSharesTotalNumberOfSharesEtc", "shares_outstanding"),
    ("NumberOfIssuedSharesAsOfFilingDateTotalNumberOfSharesEtc", "shares_outstanding"),
]

# CF（duration）: タグ部分一致 -> 出力キー（EDINETは NetCashProvidedByUsedIn*SummaryOfBusinessResults 等）
CF_TAGS = [
    ("NetCashProvidedByUsedInOperatingActivities", "net_cash_provided_by_operating_activities"),
    ("NetCashProvidedByUsedInInvestingActivities", "net_cash_used_in_investing_activities"),
    ("NetCashProvidedByUsedInFinancingActivities", "net_cash_provided_by_financing_activities"),
]

# DEI（非数値）: タグ部分一致 -> 出力キー
DEI_TAGS = [
    ("SecurityCodeDEI", "security_code"),
    ("CompanyName", "company_name"),
    ("AccountingStandardsDEI", "accounting_standard"),
    ("WhetherConsolidatedFinancialStatementsArePrepared", "is_consolidated_dei"),
    ("CurrentPeriodEndDateDEI", "current_period_end_date"),
    ("CurrentFiscalYearEndDateDEI", "current_fiscal_year_end_date"),
]


def _current_and_prior_year_ends(context_map: dict[str, dict[str, Any]]) -> tuple[str | None, str | None]:
    """context_mapからcurrent_year_end, prior_year_endを算出（durationのend_dateのみ対象）。"""
    end_dates: list[str] = []
    for ctx in context_map.values():
        if ctx.get("type") == "duration" and ctx.get("end_date"):
            end_dates.append(ctx["end_date"])
    if not end_dates:
        return None, None
    sorted_dates = sorted(set(end_dates), reverse=True)
    current_year_end = sorted_dates[0]
    prior_year_end = None
    try:
        current_dt = datetime.strptime(current_year_end, "%Y-%m-%d")
        prior_dt = current_dt.replace(year=current_dt.year - 1)
        for d in sorted_dates:
            try:
                if datetime.strptime(d, "%Y-%m-%d").year == prior_dt.year:
                    prior_year_end = d
                    break
            except ValueError:
                continue
    except ValueError:
        logger.warning("日付解析失敗: %s", current_year_end)
    return current_year_end, prior_year_end


def _tag_matches(tag: str, keyword: str) -> bool:
    """タグがキーワードを部分一致で含むか。ローカル名で判定（prefix:local の local 部分）。"""
    local = tag.split(":")[-1] if ":" in tag else tag
    return keyword in local


def _is_consolidated_context(context_ref: str) -> bool:
    """contextRefが連結か。NonConsolidatedを含む場合は単体。"""
    return "NonConsolidated" not in context_ref


def _parse_numeric_value(value: str) -> int | None:
    """文字列を数値に変換。空はNone、例外時もNone。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_float_value(value: str) -> float | None:
    """文字列をfloatに変換。空はNone、例外時もNone。EPS用。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_consolidated_dei(value: str) -> bool:
    """WhetherConsolidatedの値をboolに。"""
    if value is None:
        return False
    v = str(value).strip().lower()
    if v in ("true", "1", "yes", "有"):
        return True
    return False


class FactNormalizer:
    """
    パース済みXBRLとcontext_mapから、投資分析用の標準構造を生成する。
    """

    def __init__(self, parsed_data: dict[str, Any], context_map: dict[str, dict[str, Any]]) -> None:
        """
        Args:
            parsed_data: XBRLParser.parse() の戻り値（doc_id, taxonomy_version, facts）
            context_map: ContextResolver.build_context_map() の戻り値
        """
        self._parsed = parsed_data
        self._context_map = context_map
        self._current_year_end: str | None = None
        self._prior_year_end: str | None = None
        self._compute_year_ends()

    def _compute_year_ends(self) -> None:
        """context_mapから当期・前期の基準日を算出。"""
        self._current_year_end, self._prior_year_end = _current_and_prior_year_ends(self._context_map)
        if self._current_year_end:
            logger.debug("current_year_end: %s", self._current_year_end)
        if self._prior_year_end:
            logger.debug("prior_year_end: %s", self._prior_year_end)

    def _fact_context_info(self, context_ref: str) -> dict[str, Any]:
        """contextRefから type / is_current_year / is_prior_year を返す。"""
        ctx = self._context_map.get(context_ref, {})
        t = ctx.get("type", "")
        is_current = False
        is_prior = False
        if t == "duration":
            end = ctx.get("end_date", "")
            is_current = end == self._current_year_end if self._current_year_end else False
            is_prior = end == self._prior_year_end if self._prior_year_end else False
        elif t == "instant":
            date = ctx.get("date", "")
            is_current = date == self._current_year_end if self._current_year_end else False
            is_prior = date == self._prior_year_end if self._prior_year_end else False
        return {"type": t, "is_current_year": is_current, "is_prior_year": is_prior}

    def _pick_duration_facts(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        is_current: bool,
    ) -> dict[str, int | None]:
        """duration系のfactから、連結優先・同一タグは最初の1件でPL/CF用辞書を構築。"""
        out: dict[str, int | None] = {}
        for keyword, key in tag_keywords:
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                info = self._fact_context_info(f.get("contextRef", ""))
                if info["type"] != "duration":
                    continue
                if is_current and not info["is_current_year"]:
                    continue
                if not is_current and not info["is_prior_year"]:
                    continue
                if _is_consolidated_context(f.get("contextRef", "")):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            # 連結優先
            chosen = consolidated_candidates[0] if consolidated_candidates else (non_consolidated_candidates[0] if non_consolidated_candidates else None)
            if chosen is not None:
                out[key] = _parse_numeric_value(chosen.get("value"))
            else:
                out[key] = None
        return out

    def _pick_duration_facts_eps(
        self,
        facts: list[dict[str, str]],
        is_current: bool,
    ) -> dict[str, float | None]:
        """duration系のfactからEPSのみ取得。値はfloat。unitRefは制限しない。"""
        out: dict[str, float | None] = {"earnings_per_share": None, "diluted_earnings_per_share": None}
        for keyword, key in PL_EPS_TAGS:
            if out.get(key) is not None:
                continue
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                info = self._fact_context_info(f.get("contextRef", ""))
                if info["type"] != "duration":
                    continue
                if is_current and not info["is_current_year"]:
                    continue
                if not is_current and not info["is_prior_year"]:
                    continue
                if _is_consolidated_context(f.get("contextRef", "")):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            chosen = consolidated_candidates[0] if consolidated_candidates else (non_consolidated_candidates[0] if non_consolidated_candidates else None)
            if chosen is not None:
                out[key] = _parse_float_value(chosen.get("value"))
        return out

    def _extract_pl(
        self,
        facts: list[dict[str, str]],
        is_current: bool,
    ) -> dict[str, int | float | None]:
        """PL抽出。通常項目はint、EPSはfloat。"""
        pl_int = self._pick_duration_facts(facts, PL_TAGS, is_current=is_current)
        pl_eps = self._pick_duration_facts_eps(facts, is_current=is_current)
        pl_int["earnings_per_share"] = pl_eps.get("earnings_per_share")
        pl_int["diluted_earnings_per_share"] = pl_eps.get("diluted_earnings_per_share")
        return pl_int

    def _extract_bs(
        self,
        facts: list[dict[str, str]],
        is_current: bool,
    ) -> dict[str, int | None]:
        """BS抽出。タグから取得した値をそのまま格納。補完・再構成は行わない。"""
        return self._pick_instant_facts(facts, BS_TAGS, is_current=is_current)

    def _pick_instant_facts(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        is_current: bool,
    ) -> dict[str, int | None]:
        """instant系のfactから、BS用辞書を構築。dateがcurrent_year_end/prior_year_endと一致するもの。"""
        out: dict[str, int | None] = {}
        target_date = self._current_year_end if is_current else self._prior_year_end
        if not target_date:
            for _, key in tag_keywords:
                out[key] = None
            return out
        for keyword, key in tag_keywords:
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                ctx = self._context_map.get(f.get("contextRef", ""), {})
                if ctx.get("type") != "instant":
                    continue
                if ctx.get("date") != target_date:
                    continue
                if _is_consolidated_context(f.get("contextRef", "")):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            chosen = consolidated_candidates[0] if consolidated_candidates else (non_consolidated_candidates[0] if non_consolidated_candidates else None)
            if chosen is not None:
                out[key] = _parse_numeric_value(chosen.get("value"))
            else:
                out[key] = None
        return out

    def _pick_dei(self, facts: list[dict[str, str]]) -> dict[str, Any]:
        """DEIタグから security_code, company_name, accounting_standard, is_consolidated, fiscal_year_end を取得。連結優先。"""
        result: dict[str, Any] = {
            "security_code": None,
            "company_name": None,
            "accounting_standard": None,
            "is_consolidated": True,
            "fiscal_year_end": None,
        }
        for keyword, key in DEI_TAGS:
            consolidated_f: dict[str, str] | None = None
            non_consolidated_f: dict[str, str] | None = None
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                if _is_consolidated_context(f.get("contextRef", "")):
                    consolidated_f = f
                    break
                else:
                    if non_consolidated_f is None:
                        non_consolidated_f = f
            chosen = consolidated_f or non_consolidated_f
            if chosen is None:
                # デバッグログ: タグが見つからない場合
                if key == "security_code":
                    logger.warning(
                        "SecurityCodeDEI タグが見つかりませんでした。"
                        "doc_id=%s, 検索キーワード=%s",
                        self._parsed.get("doc_id", "unknown"),
                        keyword
                    )
                continue
            if key == "is_consolidated_dei":
                result["is_consolidated"] = _parse_consolidated_dei(chosen.get("value", ""))
            elif key == "security_code":
                result["security_code"] = (chosen.get("value") or "").strip() or None
                if result["security_code"]:
                    logger.info(
                        "security_code を抽出しました: %s (doc_id=%s)",
                        result["security_code"],
                        self._parsed.get("doc_id", "unknown")
                    )
            elif key == "company_name":
                result["company_name"] = (chosen.get("value") or "").strip() or None
            elif key == "accounting_standard":
                result["accounting_standard"] = (chosen.get("value") or "").strip() or None
            elif key in ("current_period_end_date", "current_fiscal_year_end_date"):
                # fiscal_year_end を優先順位で取得（CurrentFiscalYearEndDateDEI > CurrentPeriodEndDateDEI）
                value = (chosen.get("value") or "").strip()
                if value and result["fiscal_year_end"] is None:
                    result["fiscal_year_end"] = value
                elif key == "current_fiscal_year_end_date" and value:
                    # CurrentFiscalYearEndDateDEI の方が優先
                    result["fiscal_year_end"] = value
        
        # security_code が None の場合、警告を出力
        if result["security_code"] is None:
            logger.warning(
                "security_code が抽出できませんでした。doc_id=%s",
                self._parsed.get("doc_id", "unknown")
            )
        return result

    def _detect_report_type(self) -> str:
        """
        書類種別を判定。
        有価証券報告書 → "annual"
        四半期報告書 → "quarterly"
        判定不能 → "unknown"
        """
        # 暫定的に "annual" を返す（有価証券報告書が最も一般的）
        # 将来的には、XBRLParser からファイル名を取得して判定する
        return "annual"

    def _build_period(self, is_current: bool) -> dict[str, str] | None:
        """duration contextからperiod(start/end)を構築する。"""
        target_end = self._current_year_end if is_current else self._prior_year_end
        if not target_end:
            return None
        for ctx in self._context_map.values():
            if ctx.get("type") == "duration" and ctx.get("end_date") == target_end:
                start = ctx.get("start_date")
                if start:
                    return {"start": start, "end": target_end}
        return None

    def normalize(self) -> dict[str, Any]:
        """
        正規化結果を返す。
        current_year / prior_year それぞれに pl, bs, cf, period を持つ構造。
        """
        facts = self._parsed.get("facts") or []
        dei = self._pick_dei(facts)

        current_pl = self._extract_pl(facts, is_current=True)
        prior_pl = self._extract_pl(facts, is_current=False)
        current_bs = self._extract_bs(facts, is_current=True)
        prior_bs = self._extract_bs(facts, is_current=False)
        current_cf = self._pick_duration_facts(facts, CF_TAGS, is_current=True)
        prior_cf = self._pick_duration_facts(facts, CF_TAGS, is_current=False)
        current_period = self._build_period(is_current=True)
        prior_period = self._build_period(is_current=False)

        report_type = self._detect_report_type()

        consolidation_type = "consolidated" if dei["is_consolidated"] else "non_consolidated"

        result: dict[str, Any] = {
            "doc_id": self._parsed.get("doc_id", ""),
            "security_code": dei["security_code"],
            "company_name": dei["company_name"],
            "accounting_standard": dei["accounting_standard"],
            "is_consolidated": dei["is_consolidated"],
            "consolidation_type": consolidation_type,
            "fiscal_year_end": dei["fiscal_year_end"],
            "report_type": report_type,
            "current_year": {
                "pl": current_pl,
                "bs": current_bs,
                "cf": current_cf,
            },
            "prior_year": {
                "pl": prior_pl,
                "bs": prior_bs,
                "cf": prior_cf,
            },
        }

        if current_period:
            result["current_year"]["period"] = current_period
        if prior_period:
            result["prior_year"]["period"] = prior_period

        return result
