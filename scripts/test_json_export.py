"""
JSONExporter 動作確認用スクリプト。
Fact-only・EPS分離・period保持・会計定義明示・EPS整合チェックを検証する。

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

from output.json_exporter import (
    JSONExporter, DERIVED_KEYS, FACT_KEYS, SCHEMA_VERSION,
    normalize_security_code,
)

PROHIBITED_KEYS = DERIVED_KEYS | {
    "stock_price", "shares_outstanding_market", "dividend_per_share", "market_cap",
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
                "net_income_attributable_to_parent": 5870000000.0,
                "earnings_per_share_basic": 91.44,
                "shares_outstanding": 64200000,
            },
        },
        "prior_year": {
            "period": {"start": "2023-12-01", "end": "2024-11-30"},
            "metrics": {
                "total_assets": 200000000000.0,
                "equity": 75000000000.0,
                "net_sales": 230000000000.0,
                "operating_income": 6500000000.0,
                "net_income_attributable_to_parent": 5000000000.0,
                "earnings_per_share_basic": 77.88,
                "shares_outstanding": 64200000,
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

    checks.append(("net_income_attributable_to_parent 存在",
                    "net_income_attributable_to_parent" in current_metrics))
    checks.append(("earnings_per_share_basic 存在",
                    "earnings_per_share_basic" in current_metrics))
    checks.append(("shares_outstanding 存在",
                    "shares_outstanding" in current_metrics))

    checks.append(("旧キー profit_loss 不在", "profit_loss" not in current_metrics))
    checks.append(("旧キー earnings_per_share 不在", "earnings_per_share" not in current_metrics))
    checks.append(("旧キー fiscal_year_end 不在", "fiscal_year_end" not in loaded))

    checks.append(("market セクション不在", "market" not in current_year))
    checks.append(("valuation セクション不在", "valuation" not in current_year))

    all_keys = set(current_metrics.keys()) | set(prior_metrics.keys())
    leaked = all_keys & PROHIBITED_KEYS
    checks.append(("Derived/Market キー混入なし", len(leaked) == 0))

    all_values = list(current_metrics.values()) + list(prior_metrics.values())
    has_null = any(v is None for v in all_values)
    checks.append(("null値なし", not has_null))

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
                        "earnings_per_share_basic": 1.0, "shares_outstanding": 5},
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
