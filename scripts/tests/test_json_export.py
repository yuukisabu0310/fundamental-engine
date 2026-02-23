"""
JSONExporter 動作確認用スクリプト。
Fact-only・period保持・会計定義明示・有利子負債構成項目を検証する。
EPSは再計算可能なためFactレイクに含めない。

使用例:
    python scripts/test_json_export.py
"""
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)

if "DATASET_PATH" not in os.environ:
    os.environ["DATASET_PATH"] = "./financial-dataset"

from output.json_exporter import (
    JSONExporter, DERIVED_KEYS, FACT_KEYS, SCHEMA_VERSION,
    normalize_security_code,
)

PROHIBITED_KEYS = DERIVED_KEYS | {
    "stock_price", "shares_outstanding_market", "market_cap",
}

if __name__ == "__main__":
    dummy_financial_dict = {
        "doc_id": "S100XL6L",
        "security_code": "27340",
        "fiscal_year_end": "2025-11-30",
        "report_type": "annual",
        "consolidation_type": "consolidated",
        "accounting_standard": "Japan GAAP",
        "current_year": {
            "period": {"start": "2024-12-01", "end": "2025-11-30"},
            "metrics": {
                "total_assets": 218345000000.0,
                "equity": 81630000000.0,
                "net_sales": 251533000000.0,
                "operating_income": 7381000000.0,
                "ordinary_income": 7200000000.0,
                "net_income_attributable_to_parent": 5870000000.0,
                "total_number_of_issued_shares": 64200000,
                "cash_and_equivalents": 15000000000.0,
                "operating_cash_flow": 8500000000.0,
                "depreciation": 3200000000.0,
                "dividends_per_share": 50.0,
                "short_term_borrowings": 5000000000.0,
                "current_portion_of_long_term_borrowings": 2000000000.0,
                "commercial_papers": None,
                "current_portion_of_bonds": None,
                "short_term_lease_obligations": 100000000.0,
                "bonds_payable": 10000000000.0,
                "long_term_borrowings": 15000000000.0,
                "long_term_lease_obligations": 300000000.0,
                "lease_obligations": None,
            },
        },
        "prior_year": {
            "period": {"start": "2023-12-01", "end": "2024-11-30"},
            "metrics": {
                "total_assets": 200000000000.0,
                "equity": 75000000000.0,
                "net_sales": 230000000000.0,
                "operating_income": 6500000000.0,
                "ordinary_income": 6300000000.0,
                "net_income_attributable_to_parent": 5000000000.0,
                "total_number_of_issued_shares": 64200000,
                "cash_and_equivalents": 14000000000.0,
                "operating_cash_flow": 7500000000.0,
                "depreciation": 3000000000.0,
                "dividends_per_share": 45.0,
                "short_term_borrowings": 4000000000.0,
                "current_portion_of_long_term_borrowings": 1500000000.0,
                "commercial_papers": None,
                "current_portion_of_bonds": None,
                "short_term_lease_obligations": 80000000.0,
                "bonds_payable": 10000000000.0,
                "long_term_borrowings": 16000000000.0,
                "long_term_lease_obligations": 250000000.0,
                "lease_obligations": None,
            },
        },
    }

    exporter = JSONExporter()
    output_path = exporter.export(dummy_financial_dict)

    print("=" * 60)
    print(f"JSONExporter schema {SCHEMA_VERSION} テスト")
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

    checks.append((f"schema_version == {SCHEMA_VERSION}", loaded.get("schema_version") == SCHEMA_VERSION))
    checks.append(("consolidation_type 存在", loaded.get("consolidation_type") == "consolidated"))
    checks.append(("accounting_standard 正規化", loaded.get("accounting_standard") == "JGAAP"))
    checks.append(("currency == JPY", loaded.get("currency") == "JPY"))
    checks.append(("unit == JPY", loaded.get("unit") == "JPY"))
    checks.append(("security_code 正規化 (27340→2734)", loaded.get("security_code") == "2734"))
    checks.append(("ファイル名 正規化", Path(output_path).stem == "2734"))

    checks.append(("current_year.metrics 存在", bool(current_metrics)))
    checks.append(("prior_year.metrics 存在", bool(prior_metrics)))
    checks.append(("current_year.period 存在", "period" in current_year))
    checks.append(("prior_year.period 存在", "period" in prior_year))

    # 基礎財務項目
    checks.append(("total_assets 存在", "total_assets" in current_metrics))
    checks.append(("equity 存在", "equity" in current_metrics))
    checks.append(("net_sales 存在", "net_sales" in current_metrics))
    checks.append(("operating_income 存在", "operating_income" in current_metrics))
    checks.append(("ordinary_income 存在", "ordinary_income" in current_metrics))
    checks.append(("net_income_attributable_to_parent 存在",
                    "net_income_attributable_to_parent" in current_metrics))
    checks.append(("total_number_of_issued_shares 存在",
                    "total_number_of_issued_shares" in current_metrics))

    # 分析用追加項目
    checks.append(("cash_and_equivalents 存在", "cash_and_equivalents" in current_metrics))
    checks.append(("operating_cash_flow 存在", "operating_cash_flow" in current_metrics))
    checks.append(("depreciation 存在", "depreciation" in current_metrics))
    checks.append(("dividends_per_share 存在", "dividends_per_share" in current_metrics))

    # 有利子負債構成
    checks.append(("short_term_borrowings 存在", "short_term_borrowings" in current_metrics))
    checks.append(("bonds_payable 存在", "bonds_payable" in current_metrics))
    checks.append(("long_term_borrowings 存在", "long_term_borrowings" in current_metrics))
    checks.append(("lease_obligations null出力", current_metrics.get("lease_obligations") is None))

    # 禁止キー
    checks.append(("EPSは含まない", "earnings_per_share_basic" not in current_metrics))
    checks.append(("旧 interest_bearing_debt 不在", "interest_bearing_debt" not in current_metrics))
    checks.append(("旧キー profit_loss 不在", "profit_loss" not in current_metrics))
    checks.append(("旧キー earnings_per_share 不在", "earnings_per_share" not in current_metrics))

    checks.append(("market セクション不在", "market" not in current_year))
    checks.append(("valuation セクション不在", "valuation" not in current_year))

    all_keys = set(current_metrics.keys()) | set(prior_metrics.keys())
    leaked = all_keys & PROHIBITED_KEYS
    checks.append(("Derived/Market キー混入なし", len(leaked) == 0))

    # security_code 正規化ロジックテスト
    sc_cases = [
        ("48270", "4827"),
        ("4827", "4827"),
        ("00100", "0010"),
        ("12345", "12345"),
        ("100", "100"),
    ]
    sc_ok = all(normalize_security_code(r) == e for r, e in sc_cases)
    checks.append(("security_code正規化ロジック", sc_ok))

    # 空prior_year省略テスト
    dummy_no_prior = {
        "doc_id": "TEST_NO_PRIOR",
        "security_code": "9999",
        "fiscal_year_end": "2025-03-31",
        "report_type": "annual",
        "consolidation_type": "consolidated",
        "current_year": {
            "metrics": {"total_assets": 100.0, "equity": 50.0, "net_sales": 200.0,
                        "operating_income": 10.0, "net_income_attributable_to_parent": 5.0,
                        "total_number_of_issued_shares": 5},
        },
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
        print(f"\n[OK] すべての検証が成功しました（schema {SCHEMA_VERSION}）")
    else:
        print("\n[NG] 一部の検証が失敗しました")
        if leaked:
            print(f"  禁止キー検出: {leaked}")
        sys.exit(1)

    # テスト用ファイルの後始末
    for p in [path2]:
        test_path = Path(p)
        if test_path.exists():
            test_path.unlink()
            parent = test_path.parent
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
