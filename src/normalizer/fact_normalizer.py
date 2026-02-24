"""
FactNormalizer
事実の正規化専用レイヤー。タグ→標準キー変換・current/prior分類・型変換・連結優先のみ。
補完・再構成・推測は行わず、FinancialMasterで処理する。

タグ→キーのマッピングは config/taxonomy_mapping.yaml から読み込む（ハードコード排除）。
"""
import logging
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from src.config_loader import load_taxonomy_mapping
except ModuleNotFoundError:
    from config_loader import load_taxonomy_mapping

logger = logging.getLogger(__name__)

_mapping = load_taxonomy_mapping()

PL_TAGS: list[tuple[str, str]] = _mapping["pl"]
BS_TAGS: list[tuple[str, str]] = _mapping["bs"]
CF_TAGS: list[tuple[str, str]] = _mapping["cf"]
DIVIDEND_TAGS: list[tuple[str, str]] = _mapping["dividend"]
SHARES_TAGS: list[tuple[str, str]] = _mapping["shares"]
DEI_TAGS: list[tuple[str, str]] = _mapping["dei"]

_BS_ANCHOR_KEYWORDS = ("TotalAssets", "LiabilitiesAndNetAssets", "NetAssets")


# ---------------------------------------------------------------------------
# ユーティリティ関数
# ---------------------------------------------------------------------------

def _current_and_prior_year_ends(
    context_map: dict[str, dict[str, Any]],
) -> tuple[str | None, str | None]:
    """context_map の duration end_date から current_year_end / prior_year_end を算出する。"""
    end_dates: list[str] = []
    for ctx in context_map.values():
        if ctx.get("type") == "duration" and ctx.get("end_date"):
            end_dates.append(ctx["end_date"])
    if not end_dates:
        return None, None

    sorted_dates = sorted(set(end_dates), reverse=True)
    current_year_end = sorted_dates[0]
    prior_year_end: str | None = None
    try:
        current_dt = datetime.strptime(current_year_end, "%Y-%m-%d")
        prior_year = current_dt.year - 1
        for d in sorted_dates:
            try:
                if datetime.strptime(d, "%Y-%m-%d").year == prior_year:
                    prior_year_end = d
                    break
            except ValueError:
                continue
    except ValueError:
        logger.warning("日付解析失敗: %s", current_year_end)
    return current_year_end, prior_year_end


def _tag_local_name(tag: str) -> str:
    """タグからローカル名を取得する（prefix:local → local）。"""
    return tag.split(":")[-1] if ":" in tag else tag


def _tag_matches(tag: str, keyword: str) -> bool:
    """タグのローカル名が keyword と完全一致するか判定する。

    部分一致は誤爆リスクが高いため禁止。正確性を優先する。
    """
    return _tag_local_name(tag) == keyword


def _is_consolidated_context(context_ref: str) -> bool:
    """contextRef が連結コンテキストか判定する。NonConsolidated を含む場合は単体。"""
    return "NonConsolidated" not in context_ref


def _has_member_dimension(context_ref: str) -> bool:
    """contextRef にセグメント/メンバー dimension が含まれるか判定する。

    セグメント情報（ReportableSegmentsMember 等）を除外するために使用。
    NonConsolidatedMember は連結/単体区分なので除外対象外。
    """
    if "Member" not in context_ref:
        return False
    if context_ref.endswith("_NonConsolidatedMember"):
        return False
    for part in context_ref.split("_")[1:]:
        if "Member" in part and part != "NonConsolidatedMember":
            return True
    return False


def _parse_numeric_value(value: str | None) -> int | None:
    """文字列を int に変換する。単位変換は行わない。

    XBRL の decimals 属性は精度指標であり単位変換には使わない（XBRL仕様）。
    EDINET の主要財務指標は円単位で統一されているため値をそのまま使用する。
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_float_value(value: str | None) -> float | None:
    """文字列を float に変換する。配当等の小数値用。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_consolidated_dei(value: str | None) -> bool:
    """WhetherConsolidated の値を bool に変換する。"""
    if value is None:
        return False
    v = str(value).strip().lower()
    return v in ("true", "1", "yes", "有")


# ---------------------------------------------------------------------------
# FactNormalizer 本体
# ---------------------------------------------------------------------------

class FactNormalizer:
    """パース済み XBRL と context_map から、投資分析用の標準構造を生成する。"""

    def __init__(
        self,
        parsed_data: dict[str, Any],
        context_map: dict[str, dict[str, Any]],
    ) -> None:
        self._parsed = parsed_data
        self._context_map = context_map
        self._current_year_end: str | None = None
        self._prior_year_end: str | None = None
        self._compute_year_ends()

    # ------------------------------------------------------------------
    # 初期化
    # ------------------------------------------------------------------

    def _compute_year_ends(self) -> None:
        """context_map から当期・前期の基準日を算出する。"""
        self._current_year_end, self._prior_year_end = _current_and_prior_year_ends(
            self._context_map,
        )
        if self._current_year_end:
            logger.debug("current_year_end: %s", self._current_year_end)
        if self._prior_year_end:
            logger.debug("prior_year_end: %s", self._prior_year_end)

    # ------------------------------------------------------------------
    # fact ピッカー共通
    # ------------------------------------------------------------------

    def _fact_context_info(self, context_ref: str) -> dict[str, Any]:
        """contextRef から type / is_current_year / is_prior_year を返す。"""
        ctx = self._context_map.get(context_ref, {})
        ctx_type = ctx.get("type", "")
        is_current = False
        is_prior = False
        if ctx_type == "duration":
            end = ctx.get("end_date", "")
            is_current = (end == self._current_year_end) if self._current_year_end else False
            is_prior = (end == self._prior_year_end) if self._prior_year_end else False
        elif ctx_type == "instant":
            date = ctx.get("date", "")
            is_current = (date == self._current_year_end) if self._current_year_end else False
            is_prior = (date == self._prior_year_end) if self._prior_year_end else False
        return {"type": ctx_type, "is_current_year": is_current, "is_prior_year": is_prior}

    def _choose_fact(
        self,
        consolidated: list[dict[str, str]],
        non_consolidated: list[dict[str, str]],
        *,
        consolidated_only: bool,
    ) -> dict[str, str] | None:
        """連結/単体候補から1つ選択する。consolidated_only 時は連結のみ。"""
        if consolidated_only:
            return consolidated[0] if consolidated else None
        return consolidated[0] if consolidated else (
            non_consolidated[0] if non_consolidated else None
        )

    # ------------------------------------------------------------------
    # duration fact ピッカー
    # ------------------------------------------------------------------

    def _pick_duration_facts(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        is_current: bool,
        *,
        consolidated_only: bool = False,
    ) -> dict[str, int | None]:
        """duration 系 fact から PL/CF 用辞書を構築する。

        同一 output key に複数 keyword がある場合、先頭マッチ優先。
        consolidated_only=True で個別フォールバックを抑止し IFRS 値混入を防止。
        xsi:nil fact は None を返すが同一キーへの後続フォールバックを抑止する。
        """
        out: dict[str, int | None] = {}
        resolved: set[str] = set()
        for keyword, key in tag_keywords:
            if key in resolved:
                continue
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                ctx_ref = f.get("contextRef", "")
                if _has_member_dimension(ctx_ref):
                    continue
                info = self._fact_context_info(ctx_ref)
                if info["type"] != "duration":
                    continue
                if is_current and not info["is_current_year"]:
                    continue
                if not is_current and not info["is_prior_year"]:
                    continue
                if _is_consolidated_context(ctx_ref):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            chosen = self._choose_fact(
                consolidated_candidates, non_consolidated_candidates,
                consolidated_only=consolidated_only,
            )
            if chosen is not None:
                parsed = _parse_numeric_value(chosen.get("value"))
                out[key] = parsed
                if parsed is not None or chosen.get("is_nil", False):
                    resolved.add(key)
            elif key not in out:
                out[key] = None
        return out

    def _pick_duration_facts_allow_non_consolidated(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        is_current: bool,
    ) -> dict[str, float | None]:
        """配当等の個別ベース項目用。連結で見つからなければ個別からも取得する。値は float。"""
        out: dict[str, float | None] = {}
        resolved: set[str] = set()
        for keyword, key in tag_keywords:
            if key in resolved:
                continue
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                ctx_ref = f.get("contextRef", "")
                if _has_member_dimension(ctx_ref):
                    continue
                info = self._fact_context_info(ctx_ref)
                if info["type"] != "duration":
                    continue
                if is_current and not info["is_current_year"]:
                    continue
                if not is_current and not info["is_prior_year"]:
                    continue
                if _is_consolidated_context(ctx_ref):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            chosen = self._choose_fact(
                consolidated_candidates, non_consolidated_candidates,
                consolidated_only=False,
            )
            if chosen is not None:
                parsed = _parse_float_value(chosen.get("value"))
                out[key] = parsed
                if parsed is not None or chosen.get("is_nil", False):
                    resolved.add(key)
            elif key not in out:
                out[key] = None
        return out

    # ------------------------------------------------------------------
    # instant fact ピッカー
    # ------------------------------------------------------------------

    def _pick_instant_facts(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        is_current: bool,
        *,
        consolidated_only: bool = False,
    ) -> dict[str, int | None]:
        """instant 系 fact から BS 用辞書を構築する。

        対象日付は current_year_end / prior_year_end。
        xsi:nil fact は None を返すが同一キーへの後続フォールバックを抑止する。
        """
        target_date = self._current_year_end if is_current else self._prior_year_end
        return self._pick_instant_facts_by_date(
            facts, tag_keywords, target_date, consolidated_only=consolidated_only,
        )

    def _pick_instant_facts_by_date(
        self,
        facts: list[dict[str, str]],
        tag_keywords: list[tuple[str, str]],
        target_date: str | None,
        *,
        consolidated_only: bool = False,
    ) -> dict[str, int | None]:
        """指定日付の instant fact を取得する共通実装。"""
        out: dict[str, int | None] = {}
        if not target_date:
            for _, key in tag_keywords:
                if key not in out:
                    out[key] = None
            return out

        resolved: set[str] = set()
        for keyword, key in tag_keywords:
            if key in resolved:
                continue
            consolidated_candidates: list[dict[str, str]] = []
            non_consolidated_candidates: list[dict[str, str]] = []
            for f in facts:
                if not _tag_matches(f.get("tag", ""), keyword):
                    continue
                ctx_ref = f.get("contextRef", "")
                if _has_member_dimension(ctx_ref):
                    continue
                ctx = self._context_map.get(ctx_ref, {})
                if ctx.get("type") != "instant":
                    continue
                if ctx.get("date") != target_date:
                    continue
                if _is_consolidated_context(ctx_ref):
                    consolidated_candidates.append(f)
                else:
                    non_consolidated_candidates.append(f)
            chosen = self._choose_fact(
                consolidated_candidates, non_consolidated_candidates,
                consolidated_only=consolidated_only,
            )
            if chosen is not None:
                parsed = _parse_numeric_value(chosen.get("value"))
                out[key] = parsed
                if parsed is not None or chosen.get("is_nil", False):
                    resolved.add(key)
            elif key not in out:
                out[key] = None
        return out

    # ------------------------------------------------------------------
    # BS 抽出 + アンカー方式
    # ------------------------------------------------------------------

    def _extract_bs(
        self,
        facts: list[dict[str, str]],
        is_current: bool,
        *,
        consolidated_only: bool = False,
    ) -> dict[str, int | None]:
        """BS 抽出。

        BS本表アンカー方式: duration 由来の target_date で total_assets が取れない場合、
        アンカータグ (TotalAssets 等) の実際の instant 日付を検出して再試行する。
        変則決算期や投資法人等で duration end_date と BS instant 日付がずれるケースに対応。
        """
        out = self._pick_instant_facts(
            facts, BS_TAGS, is_current=is_current, consolidated_only=consolidated_only,
        )
        if out.get("total_assets") is not None:
            return out

        target_date = self._current_year_end if is_current else self._prior_year_end
        anchor_date = self._find_bs_anchor_date(facts, target_date, consolidated_only)
        if anchor_date and anchor_date != target_date:
            logger.info(
                "BS anchor fallback: target=%s -> anchor=%s (is_current=%s)",
                target_date, anchor_date, is_current,
            )
            fallback = self._pick_instant_facts_by_date(
                facts, BS_TAGS, anchor_date, consolidated_only=consolidated_only,
            )
            for key, val in fallback.items():
                if out.get(key) is None and val is not None:
                    out[key] = val
        return out

    def _find_bs_anchor_date(
        self,
        facts: list[dict[str, str]],
        reference_date: str | None,
        consolidated_only: bool,
    ) -> str | None:
        """BS 本表の代表的タグが存在する instant 日付を検出する。

        reference_date と異なる日付が見つかった場合にフォールバック先として返す。
        """
        date_counts: Counter[str] = Counter()
        for f in facts:
            local = _tag_local_name(f.get("tag", ""))
            if not any(kw in local for kw in _BS_ANCHOR_KEYWORDS):
                continue
            ctx_ref = f.get("contextRef", "")
            if _has_member_dimension(ctx_ref):
                continue
            if consolidated_only and not _is_consolidated_context(ctx_ref):
                continue
            ctx = self._context_map.get(ctx_ref, {})
            if ctx.get("type") != "instant":
                continue
            val = (f.get("value") or "").strip()
            if not val or f.get("is_nil", False):
                continue
            date = ctx.get("date", "")
            if date:
                date_counts[date] += 1

        if not date_counts:
            return None
        return date_counts.most_common(1)[0][0]

    # ------------------------------------------------------------------
    # PL 抽出
    # ------------------------------------------------------------------

    def _extract_pl(
        self,
        facts: list[dict[str, str]],
        is_current: bool,
        *,
        consolidated_only: bool = False,
    ) -> dict[str, int | None]:
        """PL 抽出。EPS は再計算可能なため抽出しない（valuation-engine で算出）。"""
        return self._pick_duration_facts(
            facts, PL_TAGS, is_current=is_current, consolidated_only=consolidated_only,
        )

    # ------------------------------------------------------------------
    # DEI 抽出
    # ------------------------------------------------------------------

    def _pick_dei(self, facts: list[dict[str, str]]) -> dict[str, Any]:
        """DEI タグから security_code, company_name 等のメタ情報を取得する。連結優先。"""
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
                elif non_consolidated_f is None:
                    non_consolidated_f = f
            chosen = consolidated_f or non_consolidated_f
            if chosen is None:
                if key == "security_code":
                    logger.warning(
                        "SecurityCodeDEI タグが見つかりませんでした。doc_id=%s, 検索キーワード=%s",
                        self._parsed.get("doc_id", "unknown"), keyword,
                    )
                continue
            if key == "is_consolidated_dei":
                result["is_consolidated"] = _parse_consolidated_dei(chosen.get("value", ""))
            elif key == "security_code":
                result["security_code"] = (chosen.get("value") or "").strip() or None
                if result["security_code"]:
                    logger.info(
                        "security_code を抽出しました: %s (doc_id=%s)",
                        result["security_code"], self._parsed.get("doc_id", "unknown"),
                    )
            elif key == "company_name":
                result["company_name"] = (chosen.get("value") or "").strip() or None
            elif key == "accounting_standard":
                result["accounting_standard"] = (chosen.get("value") or "").strip() or None
            elif key in ("current_period_end_date", "current_fiscal_year_end_date"):
                value = (chosen.get("value") or "").strip()
                if value and result["fiscal_year_end"] is None:
                    result["fiscal_year_end"] = value
                elif key == "current_fiscal_year_end_date" and value:
                    result["fiscal_year_end"] = value

        if result["security_code"] is None:
            logger.warning(
                "security_code が抽出できませんでした。doc_id=%s",
                self._parsed.get("doc_id", "unknown"),
            )
        return result

    # ------------------------------------------------------------------
    # レポートタイプ / 期間
    # ------------------------------------------------------------------

    def _detect_report_type(self) -> str:
        """書類種別を判定する。現状は有価証券報告書のみ対応。"""
        return "annual"

    def _build_period(self, is_current: bool) -> dict[str, str] | None:
        """duration context から period (start/end) を構築する。"""
        target_end = self._current_year_end if is_current else self._prior_year_end
        if not target_end:
            return None
        for ctx in self._context_map.values():
            if ctx.get("type") == "duration" and ctx.get("end_date") == target_end:
                start = ctx.get("start_date")
                if start:
                    return {"start": start, "end": target_end}
        return None

    # ------------------------------------------------------------------
    # メインエントリーポイント
    # ------------------------------------------------------------------

    def normalize(self) -> dict[str, Any]:
        """正規化結果を返す。

        current_year / prior_year それぞれに pl, bs, cf, dividend, period を持つ構造。
        """
        facts = self._parsed.get("facts") or []
        dei = self._pick_dei(facts)

        consol_only = dei["is_consolidated"]

        current_pl = self._extract_pl(facts, is_current=True, consolidated_only=consol_only)
        prior_pl = self._extract_pl(facts, is_current=False, consolidated_only=consol_only)
        current_bs = self._extract_bs(facts, is_current=True, consolidated_only=consol_only)
        prior_bs = self._extract_bs(facts, is_current=False, consolidated_only=consol_only)
        current_cf = self._pick_duration_facts(
            facts, CF_TAGS, is_current=True, consolidated_only=consol_only,
        )
        prior_cf = self._pick_duration_facts(
            facts, CF_TAGS, is_current=False, consolidated_only=consol_only,
        )
        current_dividend = self._pick_duration_facts_allow_non_consolidated(
            facts, DIVIDEND_TAGS, is_current=True,
        )
        prior_dividend = self._pick_duration_facts_allow_non_consolidated(
            facts, DIVIDEND_TAGS, is_current=False,
        )
        current_shares = self._pick_instant_facts(facts, SHARES_TAGS, is_current=True)
        prior_shares = self._pick_instant_facts(facts, SHARES_TAGS, is_current=False)
        current_period = self._build_period(is_current=True)
        prior_period = self._build_period(is_current=False)

        report_type = self._detect_report_type()
        consolidation_type = "consolidated" if dei["is_consolidated"] else "non_consolidated"

        current_bs.update(current_shares)
        prior_bs.update(prior_shares)

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
                "dividend": current_dividend,
            },
            "prior_year": {
                "pl": prior_pl,
                "bs": prior_bs,
                "cf": prior_cf,
                "dividend": prior_dividend,
            },
        }

        if current_period:
            result["current_year"]["period"] = current_period
        if prior_period:
            result["prior_year"]["period"] = prior_period

        return result
