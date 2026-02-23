"""
ContextResolver
XBRLのcontextノードを解析し、contextRef → 期間情報のマップを構築する。

責務:
  - XBRLインスタンスの xbrli:context を走査
  - instant / duration を判定し context_map を返す
  - 日付解決（current_year / prior_year 判定）は FactNormalizer の責務
"""
import logging
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)

XBRLI_NS = "http://www.xbrl.org/2003/instance"


class ContextResolver:
    """
    XBRLインスタンスの xbrli:context を解析し、
    contextRef をキーとする context_map を構築する。

    context_map の各値:
      - duration: {"type": "duration", "start_date": "...", "end_date": "..."}
      - instant:  {"type": "instant", "date": "..."}
    """

    def __init__(self, xbrl_root: etree._Element) -> None:
        self._root = xbrl_root
        self._context_map: dict[str, dict[str, Any]] | None = None

    def build_context_map(self) -> dict[str, dict[str, Any]]:
        """
        contextノードを解析し、contextRef -> context情報のマップを構築する。

        Returns:
            contextRef をキーとする辞書。キャッシュされ、2回目以降はキャッシュを返す。
        """
        if self._context_map is not None:
            return self._context_map

        context_map: dict[str, dict[str, Any]] = {}

        for context_elem in self._root.iter():
            if etree.QName(context_elem).namespace != XBRLI_NS:
                continue
            if etree.QName(context_elem).localname != "context":
                continue

            context_id = context_elem.get("id")
            if not context_id:
                continue

            period_elem = context_elem.find(f"{{{XBRLI_NS}}}period")
            if period_elem is None:
                continue

            instant_elem = period_elem.find(f"{{{XBRLI_NS}}}instant")
            start_date_elem = period_elem.find(f"{{{XBRLI_NS}}}startDate")
            end_date_elem = period_elem.find(f"{{{XBRLI_NS}}}endDate")

            if instant_elem is not None and instant_elem.text:
                context_map[context_id] = {
                    "type": "instant",
                    "date": instant_elem.text.strip(),
                }
            elif start_date_elem is not None and end_date_elem is not None:
                start_date = start_date_elem.text.strip() if start_date_elem.text else ""
                end_date = end_date_elem.text.strip() if end_date_elem.text else ""
                if start_date and end_date:
                    context_map[context_id] = {
                        "type": "duration",
                        "start_date": start_date,
                        "end_date": end_date,
                    }

        self._context_map = context_map
        logger.debug("context_map構築完了: %d件", len(context_map))
        return context_map
