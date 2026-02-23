"""
XBRLパーサー
生fact抽出基盤。正規化・財務指標計算は行わない。
"""
import re
import logging
from pathlib import Path
from typing import Any

from lxml import etree

logger = logging.getLogger(__name__)

# 除外する名前空間URI
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_XSI_NIL_ATTR = f"{{{XSI_NS}}}nil"
# 除外する要素のローカル名
EXCLUDED_LOCAL_NAMES = frozenset(("context", "unit", "schemaRef"))
# taxonomy_version 抽出用の日付パターン（YYYY-MM-DD）
TAXONOMY_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _ns_to_prefix_map(root: etree._Element) -> dict[str, str]:
    """ルート要素の名前空間宣言から ns_uri -> prefix のマップを構築する。"""
    nsmap = root.nsmap
    if nsmap is None:
        return {}
    return {v or "": k or "" for k, v in nsmap.items()}


def _qname_for_element(element: etree._Element, ns_to_prefix: dict[str, str]) -> str:
    """要素のQName（prefix:localname）を返す。"""
    tag = element.tag
    if not isinstance(tag, str) or tag[0] != "{":
        return tag or ""
    ns_uri, _, local = tag[1:].partition("}")
    prefix = ns_to_prefix.get(ns_uri, "")
    if prefix:
        return f"{prefix}:{local}"
    return local


def _get_text(element: etree._Element) -> str:
    """要素のテキストを返す（XBRLのfactは通常要素直下のテキストのみ）。"""
    return (element.text or "").strip()


class XBRLParser:
    """
    XBRLインスタンスから doc_id / taxonomy_version / facts を抽出するパーサー。
    """

    def __init__(self, xbrl_path: Path) -> None:
        """
        Args:
            xbrl_path: XBRLファイルのパス。
        """
        self._path = Path(xbrl_path)
        if not self._path.is_file():
            raise FileNotFoundError(f"XBRL file not found: {self._path}")
        self._root: etree._Element | None = None

    def parse(self) -> dict[str, Any]:
        """
        XBRLをパースし、doc_id / taxonomy_version / facts を返す。

        Returns:
            doc_id: ファイルパスから取得したドキュメントID（例: S100VUAT）
            taxonomy_version: schemaRef から抽出した日付（YYYY-MM-DD）
            facts: 各factの tag, contextRef, unitRef, decimals, value, is_nil のリスト
        """
        doc_id = self._path.parent.name
        taxonomy_version = ""
        facts: list[dict[str, str]] = []

        parser = etree.XMLParser(recover=False, remove_blank_text=False)
        try:
            tree = etree.parse(str(self._path), parser=parser)
        except etree.XMLSyntaxError as e:
            logger.exception("XBRLのパースに失敗しました: %s", self._path)
            raise

        root = tree.getroot()
        self._root = root
        ns_to_prefix = _ns_to_prefix_map(root)

        # schemaRef から taxonomy_version（YYYY-MM-DD）を抽出
        # link:schemaRef の xlink:href に含まれる日付のうち最初のものを使用
        xlink_href = f"{{{XLINK_NS}}}href"
        for elem in root.iter():
            if etree.QName(elem).localname == "schemaRef":
                href = elem.get(xlink_href)
                if href:
                    m = TAXONOMY_DATE_PATTERN.search(href)
                    if m:
                        taxonomy_version = m.group(1)
                        break
                break

        # contextRef を持つ要素を fact として収集（link/xlink/context/unit/schemaRef は除外）
        for elem in root.iter():
            context_ref = elem.get("contextRef")
            if context_ref is None:
                continue

            tag_qname = elem.tag
            if not isinstance(tag_qname, str):
                continue
            if tag_qname.startswith("{"):
                ns_uri = tag_qname[1 : tag_qname.index("}")]
                local = etree.QName(elem).localname
            else:
                ns_uri = ""
                local = tag_qname

            if ns_uri in (LINK_NS, XLINK_NS):
                continue
            if local in EXCLUDED_LOCAL_NAMES:
                continue

            tag = _qname_for_element(elem, ns_to_prefix)
            unit_ref = elem.get("unitRef") or ""
            decimals = elem.get("decimals", "")
            is_nil = elem.get(_XSI_NIL_ATTR, "").lower() == "true"
            value = "" if is_nil else _get_text(elem)

            facts.append({
                "tag": tag,
                "contextRef": context_ref,
                "unitRef": unit_ref,
                "decimals": decimals,
                "value": value,
                "is_nil": is_nil,
            })

        return {
            "doc_id": doc_id,
            "taxonomy_version": taxonomy_version,
            "facts": facts,
        }

    @property
    def root(self) -> etree._Element:
        """
        パース済みのXBRLルート要素を返す。
        parse()を実行後にアクセス可能。
        """
        if self._root is None:
            raise RuntimeError("parse()を先に実行してください")
        return self._root
