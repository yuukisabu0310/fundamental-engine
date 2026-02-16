"""
Phase5 ValuationEngine 動作確認用スクリプト。
main.py に影響を与えない。プロジェクトルートから実行すること。

使用例:
    python scripts/test_valuation_engine.py
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

# .env が存在しない場合、または DATASET_PATH が設定されていない場合はデフォルト値を設定
if "DATASET_PATH" not in os.environ:
    os.environ["DATASET_PATH"] = "./financial-dataset"

# 環境変数設定後にインポート
from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer
from financial.financial_master import FinancialMaster
from market.market_integrator import MarketIntegrator
from valuation.valuation_engine import ValuationEngine
from output.json_exporter import JSONExporter

if __name__ == "__main__":
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print("XBRLファイルが見つかりません:", xbrl_path)
        sys.exit(1)

    parser = XBRLParser(xbrl_path)
    parsed_data = parser.parse()
    resolver = ContextResolver(parser.root)
    context_map = resolver.build_context_map()
    normalizer = FactNormalizer(parsed_data, context_map)
    normalized_data = normalizer.normalize()
    master = FinancialMaster(normalized_data)
    financial_data = master.compute()

    market_data = {
        "stock_price": 2500.0,
        "shares_outstanding": 5000000,
        "dividend_per_share": 50.0,
    }
    integrator = MarketIntegrator(financial_data, market_data)
    integrated_data = integrator.integrate()

    engine = ValuationEngine(integrated_data)
    result = engine.evaluate()

    # JSON エクスポート
    exporter = JSONExporter()
    json_path = exporter.export(result)

    print("=" * 60)
    print("ValuationEngine 出力")
    print("=" * 60)
    print("doc_id:", result["doc_id"])
    print("JSON保存パス:", json_path)

    v = result["current_year"].get("valuation") or {}
    print("\n--- Valuation ---")
    print("PER:", v.get("per"))
    print("PBR:", v.get("pbr"))
    print("PSR:", v.get("psr"))
    print("PEG:", v.get("peg"))
    print("Dividend Yield:", v.get("dividend_yield"))

    print("\n--- 検証 (stock_price=2500, eps=199.68 -> PER 約12.52) ---")
    eps = (result["current_year"].get("metrics") or {}).get("earnings_per_share")
    print("EPS:", eps)
    if v.get("per") is not None:
        print("PER 約12.52 一致:", abs((v["per"] or 0) - 12.52) < 0.1)
