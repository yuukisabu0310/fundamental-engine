"""
設定ファイルローダー。
config/ ディレクトリの YAML をロードし、アプリケーション全体に提供する。
ハードコードされたタグリストや定数を排除し、YAML 駆動のアーキテクチャを実現する。
"""
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _load_yaml(filename: str) -> dict[str, Any]:
    """config/ 配下の YAML ファイルをロードする。"""
    path = _CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"設定ファイルの形式が不正です: {path}")
    return data


@lru_cache(maxsize=1)
def load_taxonomy_mapping() -> dict[str, list[tuple[str, str]]]:
    """
    taxonomy_mapping.yaml をロードし、カテゴリ別のタグリストを返す。

    Returns:
        {
            "pl": [(tag, key), ...],
            "bs": [(tag, key), ...],
            "cf": [(tag, key), ...],
            "dividend": [(tag, key), ...],
            "dei": [(tag, key), ...],
        }
    """
    raw = _load_yaml("taxonomy_mapping.yaml")
    result: dict[str, list[tuple[str, str]]] = {}
    for category in ("pl", "bs", "cf", "dividend", "shares", "dei"):
        entries = raw.get(category, [])
        tag_list: list[tuple[str, str]] = []
        for entry in entries:
            tag = entry.get("tag", "")
            key = entry.get("key", "")
            if tag and key:
                tag_list.append((tag, key))
        result[category] = tag_list
        logger.debug("taxonomy_mapping: %s -> %d entries", category, len(tag_list))
    return result


@lru_cache(maxsize=1)
def load_canonical_keys() -> dict[str, Any]:
    """
    canonical_keys.yaml をロードする。

    Returns:
        全設定データ（fact_keys, derived_keys, accounting_standard_mapping 等）
    """
    return _load_yaml("canonical_keys.yaml")


@lru_cache(maxsize=1)
def get_fact_keys() -> frozenset[str]:
    """financial-dataset に保存する Fact キーの集合を返す。"""
    config = load_canonical_keys()
    return frozenset(config.get("fact_keys", {}).keys())


@lru_cache(maxsize=1)
def get_derived_keys() -> frozenset[str]:
    """再計算可能（保存しない）キーの集合を返す。"""
    config = load_canonical_keys()
    return frozenset(config.get("derived_keys", []))


@lru_cache(maxsize=1)
def get_resolution_rules() -> dict[str, list[str]]:
    """
    同一概念の優先順位解決ルールを返す。

    Returns:
        {"equity": ["shareholders_equity", "equity_attributable_to_owners", ...], ...}
    """
    config = load_canonical_keys()
    rules: dict[str, list[str]] = {}
    for key, props in config.get("fact_keys", {}).items():
        if isinstance(props, dict) and "resolution" in props:
            rules[key] = props["resolution"]
    return rules


@lru_cache(maxsize=1)
def get_normalizer_key_mapping() -> dict[str, str]:
    """
    normalizer の出力キーと canonical キーのマッピングを返す。
    normalizer_key が定義されている場合のみ含む。

    Returns:
        {"profit_loss": "net_income_attributable_to_parent", ...}
    """
    config = load_canonical_keys()
    mapping: dict[str, str] = {}
    for key, props in config.get("fact_keys", {}).items():
        if isinstance(props, dict) and "normalizer_key" in props:
            mapping[props["normalizer_key"]] = key
    return mapping


@lru_cache(maxsize=1)
def get_accounting_standard_mapping() -> dict[str, str]:
    """会計基準の表記ゆれ→正規化マッピングを返す。"""
    config = load_canonical_keys()
    return config.get("accounting_standard_mapping", {})


@lru_cache(maxsize=1)
def get_valid_accounting_standards() -> frozenset[str]:
    """有効な会計基準名の集合を返す。"""
    config = load_canonical_keys()
    return frozenset(config.get("valid_accounting_standards", []))
