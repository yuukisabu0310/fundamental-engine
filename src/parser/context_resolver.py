"""
ContextResolver
XBRLのcontextノードを解析し、factに期間情報を付与する。
"""
import logging
from datetime import datetime
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)

# XBRLインスタンス名前空間
XBRLI_NS = "http://www.xbrl.org/2003/instance"


class ContextResolver:
    """
    XBRLのcontextノードを解析し、factに期間情報を付与するリゾルバー。
    """

    def __init__(self, xbrl_root: etree._Element) -> None:
        """
        Args:
            xbrl_root: lxmlでパース済みのXBRLルート要素。
        """
        self._root = xbrl_root
        self._context_map: dict[str, dict[str, Any]] | None = None
        self._current_year_end: str | None = None
        self._prior_year_end: str | None = None

    def build_context_map(self) -> dict[str, dict[str, Any]]:
        """
        contextノードを解析し、contextRef -> context情報のマップを構築する。

        Returns:
            contextRefをキーとする辞書。各値は以下の形式：
            - duration: {"type": "duration", "start_date": "...", "end_date": "..."}
            - instant: {"type": "instant", "date": "..."}
        """
        if self._context_map is not None:
            return self._context_map

        context_map: dict[str, dict[str, Any]] = {}
        end_dates: list[str] = []

        # xbrli:context 要素をすべて取得
        for context_elem in self._root.iter():
            if etree.QName(context_elem).namespace != XBRLI_NS:
                continue
            if etree.QName(context_elem).localname != "context":
                continue

            context_id = context_elem.get("id")
            if not context_id:
                continue

            # xbrli:period を取得
            period_elem = context_elem.find(f"{{{XBRLI_NS}}}period")
            if period_elem is None:
                continue

            # instant か duration かを判定
            instant_elem = period_elem.find(f"{{{XBRLI_NS}}}instant")
            start_date_elem = period_elem.find(f"{{{XBRLI_NS}}}startDate")
            end_date_elem = period_elem.find(f"{{{XBRLI_NS}}}endDate")

            if instant_elem is not None and instant_elem.text:
                # instant型
                date = instant_elem.text.strip()
                context_map[context_id] = {
                    "type": "instant",
                    "date": date,
                }
                # instantの日付はend_datesに含めない（durationのend_dateのみを対象）
            elif start_date_elem is not None and end_date_elem is not None:
                # duration型
                start_date = start_date_elem.text.strip() if start_date_elem.text else ""
                end_date = end_date_elem.text.strip() if end_date_elem.text else ""
                if start_date and end_date:
                    context_map[context_id] = {
                        "type": "duration",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                    end_dates.append(end_date)

        # 最も新しいend_dateをcurrent_year、その1年前をprior_yearとして判定
        if end_dates:
            # 日付文字列をソートして最新を取得
            sorted_dates = sorted(set(end_dates), reverse=True)
            self._current_year_end = sorted_dates[0]

            # prior_yearはcurrent_yearの1年前の日付を探す
            # 年のみを考慮して簡易的に判定（より正確には日付計算が必要だが、ここでは簡易実装）
            try:
                current_dt = datetime.strptime(self._current_year_end, "%Y-%m-%d")
                # 1年前の同じ日付を計算
                prior_dt = current_dt.replace(year=current_dt.year - 1)
                prior_year_str = prior_dt.strftime("%Y-%m-%d")
                # 実際に存在する日付の中で最も近いものを探す
                for date in sorted_dates:
                    try:
                        date_dt = datetime.strptime(date, "%Y-%m-%d")
                        if date_dt.year == prior_dt.year:
                            self._prior_year_end = date
                            break
                    except ValueError:
                        continue
            except ValueError:
                logger.warning("日付の解析に失敗しました: %s", self._current_year_end)

        self._context_map = context_map
        logger.info("context_map構築完了: %d件", len(context_map))
        if self._current_year_end:
            logger.info("current_year_end: %s", self._current_year_end)
        if self._prior_year_end:
            logger.info("prior_year_end: %s", self._prior_year_end)

        return context_map

    def enrich_fact(self, fact: dict[str, str]) -> dict[str, Any]:
        """
        factにcontext情報を付与する。

        Args:
            fact: XBRLParser.parse()で取得したfact辞書。
                必須キー: contextRef

        Returns:
            context情報が付与されたfact辞書。
            形式:
            {
                "tag": "...",
                "value": "...",
                "unit": "...",  # unitRefから
                "context": {
                    "type": "duration" | "instant",
                    "start_date": "...",  # durationの場合
                    "end_date": "...",    # durationの場合
                    "date": "...",         # instantの場合
                    "is_current_year": bool,
                    "is_prior_year": bool,
                }
            }
        """
        if self._context_map is None:
            self.build_context_map()

        context_ref = fact.get("contextRef", "")
        context_info = self._context_map.get(context_ref, {})

        enriched: dict[str, Any] = {
            "tag": fact.get("tag", ""),
            "value": fact.get("value", ""),
            "unit": fact.get("unitRef", ""),
        }

        context_type = context_info.get("type", "")
        is_current_year = False
        is_prior_year = False

        if context_type == "duration":
            end_date = context_info.get("end_date", "")
            enriched["context"] = {
                "type": "duration",
                "start_date": context_info.get("start_date", ""),
                "end_date": end_date,
                "is_current_year": end_date == self._current_year_end if self._current_year_end else False,
                "is_prior_year": end_date == self._prior_year_end if self._prior_year_end else False,
            }
            is_current_year = enriched["context"]["is_current_year"]
            is_prior_year = enriched["context"]["is_prior_year"]
        elif context_type == "instant":
            date = context_info.get("date", "")
            enriched["context"] = {
                "type": "instant",
                "date": date,
                "is_current_year": date == self._current_year_end if self._current_year_end else False,
                "is_prior_year": date == self._prior_year_end if self._prior_year_end else False,
            }
            is_current_year = enriched["context"]["is_current_year"]
            is_prior_year = enriched["context"]["is_prior_year"]
        else:
            # contextが見つからない場合
            enriched["context"] = {
                "type": "unknown",
                "is_current_year": False,
                "is_prior_year": False,
            }

        return enriched
