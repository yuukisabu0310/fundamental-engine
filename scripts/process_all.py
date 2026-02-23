"""
パーサーパイプライン全体を実行するエントリーポイント。
XBRLファイルをパースしてJSON出力まで実行する。

使用例:
    python scripts/process_all.py
"""
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)

if "DATASET_PATH" not in os.environ:
    sys.stderr.write("ERROR: DATASET_PATH 環境変数が設定されていません。\n")
    sys.exit(1)

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer
from financial.financial_master import FinancialMaster
from output.json_exporter import JSONExporter
from constants import SKIP_FILENAME_PATTERNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    xbrl_base_dir = project_root / "data" / "edinet" / "raw_xbrl"

    if not xbrl_base_dir.exists():
        logger.warning("XBRLディレクトリが存在しません: %s", xbrl_base_dir)
        return

    xbrl_files = list(xbrl_base_dir.rglob("*.xbrl"))
    logger.info("XBRL検索ディレクトリ: %s", xbrl_base_dir)
    logger.info("XBRL ファイル数: %d", len(xbrl_files))

    if not xbrl_files:
        logger.warning("XBRLファイルが見つかりません: %s", xbrl_base_dir)
        return

    for xbrl_path in xbrl_files:
        try:
            name_lower = xbrl_path.name.lower()
            if any(pattern in name_lower for pattern in SKIP_FILENAME_PATTERNS):
                logger.debug("SKIP: %s (処理対象外)", xbrl_path.name)
                continue

            logger.info("Processing: %s", xbrl_path.name)

            parser = XBRLParser(xbrl_path)
            parsed_data = parser.parse()
            resolver = ContextResolver(parser.root)
            context_map = resolver.build_context_map()
            normalizer = FactNormalizer(parsed_data, context_map)
            normalized_data = normalizer.normalize()

            security_code = normalized_data.get("security_code")
            fiscal_year_end = normalized_data.get("fiscal_year_end")
            if security_code is None or fiscal_year_end is None:
                logger.debug(
                    "SKIP: 必須項目欠損 (security_code=%s, fiscal_year_end=%s)",
                    security_code, fiscal_year_end,
                )
                continue

            master = FinancialMaster(normalized_data)
            financial_data = master.compute()

            exporter = JSONExporter()
            json_path = exporter.export(financial_data)
            logger.info("Saved: %s", json_path)

        except ValueError as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ("security_code", "fiscal_year_end", "data_version", "unknown")):
                logger.debug("SKIP: %s - %s", xbrl_path.name, e)
                continue
            logger.error("Failed: %s - %s", xbrl_path.name, e)
        except Exception as e:
            logger.error("Failed: %s - %s", xbrl_path.name, e, exc_info=True)

    logger.info("Processing completed")


if __name__ == "__main__":
    main()
