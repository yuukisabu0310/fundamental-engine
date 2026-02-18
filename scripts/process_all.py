"""
パーサーパイプライン全体を実行するエントリーポイント。
XBRLファイルをパースしてJSON出力まで実行する。

使用例:
    python scripts/process_all.py
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# .env ファイルを読み込む（DATASET_PATH用）
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)

# DATASET_PATH が設定されていない場合はエラー
if "DATASET_PATH" not in os.environ:
    print("ERROR: DATASET_PATH 環境変数が設定されていません。")
    sys.exit(1)

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer
from financial.financial_master import FinancialMaster
from market.market_integrator import MarketIntegrator
from valuation.valuation_engine import ValuationEngine
from output.json_exporter import JSONExporter
from constants import SKIP_FILENAME_PATTERNS

if __name__ == "__main__":
    # データディレクトリからXBRLファイルを検索
    xbrl_base_dir = project_root / "data" / "edinet" / "raw_xbrl"
    
    if not xbrl_base_dir.exists():
        print(f"WARNING: XBRLディレクトリが存在しません: {xbrl_base_dir}")
        print("XBRLファイルがダウンロードされていないため、処理をスキップします。")
        sys.exit(0)

    # すべてのXBRLファイルを再帰的に検索（サブディレクトリを含む）
    # rglobは再帰的に検索するが、パターンを**/*.xbrlのように明示的に指定
    xbrl_files = list(xbrl_base_dir.rglob("*.xbrl"))
    
    # デバッグ用: 検索されたファイル数を表示
    print(f"Searching for XBRL files in: {xbrl_base_dir}")
    print(f"Found {len(xbrl_files)} XBRL files")
    
    if not xbrl_files:
        print(f"WARNING: XBRLファイルが見つかりません: {xbrl_base_dir}")
        print("処理するXBRLファイルがないため、処理をスキップします。")
        # デバッグ用: ディレクトリ構造を確認
        if xbrl_base_dir.exists():
            print(f"Directory exists. Contents:")
            try:
                for item in xbrl_base_dir.iterdir():
                    print(f"  - {item.name} ({'dir' if item.is_dir() else 'file'})")
            except Exception as e:
                print(f"  Error listing directory: {e}")
        sys.exit(0)
    
    # 各XBRLファイルを処理
    for xbrl_path in xbrl_files:
        try:
            # 処理対象外の書類（大量保有報告書等）を早期スキップ
            name_lower = xbrl_path.name.lower()
            if any(pattern in name_lower for pattern in SKIP_FILENAME_PATTERNS):
                print(f"\nSKIP: {xbrl_path.name} - 処理対象外の書類（ファイル名にスキップパターンが含まれます）")
                continue

            print(f"\nProcessing: {xbrl_path.name}")

            parser = XBRLParser(xbrl_path)
            parsed_data = parser.parse()
            resolver = ContextResolver(parser.root)
            context_map = resolver.build_context_map()
            normalizer = FactNormalizer(parsed_data, context_map)
            normalized_data = normalizer.normalize()

            # 必須項目検証：有価証券報告書・四半期報告書以外はスキップ
            security_code = normalized_data.get("security_code")
            fiscal_year_end = normalized_data.get("fiscal_year_end")
            if security_code is None or fiscal_year_end is None:
                print(
                    f"  SKIP: 有価証券報告書・四半期報告書ではないか、必須項目が欠損 "
                    f"(security_code={security_code}, fiscal_year_end={fiscal_year_end})"
                )
                continue

            master = FinancialMaster(normalized_data)
            financial_data = master.compute()

            # security_code のフォールバック処理（5桁→4桁）
            # 将来的にマーケットデータAPIを統合する際に使用
            security_code = financial_data.get("security_code")
            if security_code and isinstance(security_code, str) and security_code.isdigit():
                if len(security_code) == 5:
                    # 5桁の場合は4桁にフォールバック
                    four_digit_code = security_code[:4]
                    print(f"  security_code: {security_code} -> 4桁フォールバック: {four_digit_code}")
                    # 将来的にマーケットデータ取得時に使用
                    # 現在はマーケットデータが空のため、この処理は実行されない

            # マーケットデータは暫定的に空（実際の運用では外部APIから取得）
            market_data = {
                "stock_price": None,
                "shares_outstanding": None,
                "dividend_per_share": None,
            }
            
            integrator = MarketIntegrator(financial_data, market_data)
            integrated_data = integrator.integrate()

            engine = ValuationEngine(integrated_data)
            result = engine.evaluate()

            # JSON エクスポート
            exporter = JSONExporter()
            json_path = exporter.export(result)
            print(f"  -> Saved: {json_path}")
            
        except ValueError as e:
            # バリデーションエラー（security_code/fiscal_year_end/data_version）はスキップして継続
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ("security_code", "fiscal_year_end", "data_version", "unknown")):
                print(f"  SKIP: {xbrl_path.name} - {e}")
                continue
            print(f"ERROR: Failed to process {xbrl_path.name}: {e}")
            continue
        except Exception as e:
            print(f"ERROR: Failed to process {xbrl_path.name}: {e}")
            import traceback
            traceback.print_exc()
            continue

    print("\nProcessing completed")
