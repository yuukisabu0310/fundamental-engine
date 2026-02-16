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

if __name__ == "__main__":
    # データディレクトリからXBRLファイルを検索
    xbrl_base_dir = project_root / "data" / "edinet" / "raw_xbrl"
    
    if not xbrl_base_dir.exists():
        print(f"ERROR: XBRLディレクトリが存在しません: {xbrl_base_dir}")
        sys.exit(1)

    # すべてのXBRLファイルを検索（jpcrp030000-asr-*.xbrl）
    xbrl_files = list(xbrl_base_dir.rglob("jpcrp030000-asr-*.xbrl"))
    
    if not xbrl_files:
        print(f"WARNING: XBRLファイルが見つかりません: {xbrl_base_dir}")
        sys.exit(0)

    print(f"Found {len(xbrl_files)} XBRL files")
    
    # 各XBRLファイルを処理
    for xbrl_path in xbrl_files:
        try:
            print(f"\nProcessing: {xbrl_path.name}")
            
            parser = XBRLParser(xbrl_path)
            parsed_data = parser.parse()
            resolver = ContextResolver(parser.root)
            context_map = resolver.build_context_map()
            normalizer = FactNormalizer(parsed_data, context_map)
            normalized_data = normalizer.normalize()
            master = FinancialMaster(normalized_data)
            financial_data = master.compute()

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
            
        except Exception as e:
            print(f"ERROR: Failed to process {xbrl_path.name}: {e}")
            continue

    print("\nProcessing completed")
