"""
JSONExporter 動作確認用スクリプト。
Fact-only出力・Derived除去・null除去・空prior_year省略を検証する。

使用例:
    python scripts/test_json_export.py
"""
import json
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
    os.environ["DATASET_PATH"] = "./financial-dataset"

from output.json_exporter import JSONExporter, DERIVED_KEYS

DERIVED_AND_MARKET_KEYS = DERIVED_KEYS | {
    "stock_price", "shares_outstanding", "dividend_per_share", "market_cap",
}

if __name__ == "__main__":
    # FinancialMaster のFact-only出力を模倣（Derived/nullは含まない）
    dummy_financial_dict = {
        "doc_id": "S100W67S",
        "security_code": "4827",
        "fiscal_year_end": "2025-03-31",
        "report_type": "annual",
        "current_year": {
            "metrics": {
                "equity": 5805695000.0,
                "total_assets": 30554571000.0,
                "net_sales": 16094118000.0,
                "operating_income": 1461488000.0,
                "profit_loss": 828459000.0,
                "earnings_per_share": 199.68,
            },
        },
        "prior_year": {
            "metrics": {
                "equity": 5018725000.0,
                "total_assets": 28546264000.0,
                "net_sales": 13409224000.0,
                "operating_income": 1331316000.0,
                "profit_loss": 743129000.0,
                "earnings_per_share": 179.11,
            },
        },
    }

    exporter = JSONExporter()
    output_path = exporter.export(dummy_financial_dict)

    print("=" * 60)
    print("JSONExporter Fact-only テスト")
    print("=" * 60)
    print(f"保存パス: {output_path}")

    path_obj = Path(output_path)
    if not path_obj.exists():
        print("[NG] ファイルが存在しません")
        sys.exit(1)

    with open(path_obj, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    print(json.dumps(loaded, indent=2, ensure_ascii=False))

    current_year = loaded.get("current_year", {})
    prior_year = loaded.get("prior_year", {})
    current_metrics = current_year.get("metrics", {})
    prior_metrics = prior_year.get("metrics", {})

    checks = []
    checks.append(("schema_version == 2.0", loaded.get("schema_version") == "2.0"))
    checks.append(("current_year.metrics 存在", bool(current_metrics)))
    checks.append(("prior_year.metrics 存在", bool(prior_metrics)))
    checks.append(("market セクション不在", "market" not in current_year))
    checks.append(("valuation セクション不在", "valuation" not in current_year))

    all_keys = set(current_metrics.keys()) | set(prior_metrics.keys())
    leaked = all_keys & DERIVED_AND_MARKET_KEYS
    checks.append(("Derived/Market キー混入なし", len(leaked) == 0))

    all_values = list(current_metrics.values()) + list(prior_metrics.values())
    has_null = any(v is None for v in all_values)
    checks.append(("null値なし", not has_null))

    # prior_yearが空の場合のテスト
    dummy_no_prior = {
        "doc_id": "TEST_NO_PRIOR",
        "security_code": "9999",
        "fiscal_year_end": "2025-03-31",
        "report_type": "annual",
        "current_year": {"metrics": {"equity": 100.0}},
        "prior_year": {"metrics": {}},
    }
    path2 = exporter.export(dummy_no_prior)
    with open(path2, "r", encoding="utf-8") as f:
        loaded2 = json.load(f)
    checks.append(("空prior_yearは省略", "prior_year" not in loaded2))

    print("\n--- 検証結果 ---")
    all_ok = True
    for name, result in checks:
        status = "[OK]" if result else "[NG]"
        print(f"{status} {name}")
        if not result:
            all_ok = False

    if all_ok:
        print("\n[OK] すべての検証が成功しました（Fact-only / null除去 / Derived除去）")
    else:
        print("\n[NG] 一部の検証が失敗しました")
        if leaked:
            print(f"  禁止キー検出: {leaked}")
        sys.exit(1)

    # テスト用ファイルの後始末
    test_path = Path(path2)
    if test_path.exists():
        test_path.unlink()
        parent = test_path.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
