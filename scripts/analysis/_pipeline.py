"""
分析スクリプト共通のパイプラインユーティリティ。

analysis/ 配下のスクリプトから共有される:
  - XBRL パイプライン実行
  - 証券コード正規化
  - 報告書様式コード推定
  - XBRLファイル収集
"""
import logging
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer
from financial.financial_master import FinancialMaster
from config_loader import get_fact_keys, get_derived_keys
from constants import SKIP_FILENAME_PATTERNS

logger = logging.getLogger(__name__)

FACT_KEYS = get_fact_keys()
DERIVED_KEYS = get_derived_keys()

XBRL_BASE_DIR = PROJECT_ROOT / "data" / "edinet" / "raw_xbrl"


def normalize_code(raw: Any) -> str:
    """EDINET 証券コードを4桁に正規化する。5桁末尾0なら削除。"""
    s = str(raw).strip()
    if len(s) == 5 and s.endswith("0"):
        return s[:4]
    return s


def check_form_code(filename: str) -> str:
    """XBRL ファイル名から報告書様式コードを推定する。"""
    parts = filename.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return "unknown"


def run_pipeline(
    xbrl_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], FactNormalizer, dict[str, Any], dict[str, Any]]:
    """XBRL ファイルを完全パイプラインで処理する。

    Returns:
        (parsed, context_map, normalizer, normalized, master_result)
    Raises:
        Exception: パイプラインの任意のステップで失敗した場合
    """
    parser = XBRLParser(xbrl_path)
    parsed = parser.parse()
    resolver = ContextResolver(parser.root)
    ctx_map = resolver.build_context_map()
    normalizer = FactNormalizer(parsed, ctx_map)
    normalized = normalizer.normalize()
    master = FinancialMaster(normalized)
    result = master.compute()
    return parsed, ctx_map, normalizer, normalized, result


def collect_xbrl_files(base_dir: Path | None = None) -> list[Path]:
    """XBRL ファイルを再帰収集し、スキップ対象を除外して返す。"""
    root = base_dir or XBRL_BASE_DIR
    files: list[Path] = []
    for f in root.rglob("*.xbrl"):
        if not any(pat in f.name.lower() for pat in SKIP_FILENAME_PATTERNS):
            files.append(f)
    return sorted(files)
