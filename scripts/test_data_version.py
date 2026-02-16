"""
Phase5.2 data_version 決算期ベース生成のテストスクリプト。
"""
import json
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
else:
    # .env が存在しない場合はデフォルト値を設定
    if "DATASET_PATH" not in os.environ:
        os.environ["DATASET_PATH"] = "./financial-dataset"

from output.json_exporter import JSONExporter

if __name__ == "__main__":
    exporter = JSONExporter()

    # テストケース1: Annual → 2025FY
    print("=" * 60)
    print("テストケース1: Annual → 2025FY")
    print("=" * 60)
    result1 = exporter._generate_data_version("2025-12-31", "annual")
    print(f"fiscal_year_end: 2025-12-31, report_type: annual")
    print(f"結果: {result1}")
    assert result1 == "2025FY", f"期待値: 2025FY, 実際: {result1}"
    print("[OK] テストケース1 成功\n")

    # テストケース2: Quarterly → 2025Q3
    print("=" * 60)
    print("テストケース2: Quarterly → 2025Q3")
    print("=" * 60)
    result2 = exporter._generate_data_version("2025-09-30", "quarterly")
    print(f"fiscal_year_end: 2025-09-30, report_type: quarterly")
    print(f"結果: {result2}")
    assert result2 == "2025Q3", f"期待値: 2025Q3, 実際: {result2}"
    print("[OK] テストケース2 成功\n")

    # テストケース3: Quarterly → 2025Q1 (3月)
    print("=" * 60)
    print("テストケース3: Quarterly → 2025Q1 (3月)")
    print("=" * 60)
    result3 = exporter._generate_data_version("2025-03-31", "quarterly")
    print(f"fiscal_year_end: 2025-03-31, report_type: quarterly")
    print(f"結果: {result3}")
    assert result3 == "2025Q1", f"期待値: 2025Q1, 実際: {result3}"
    print("[OK] テストケース3 成功\n")

    # テストケース4: Quarterly → 2025Q2 (6月)
    print("=" * 60)
    print("テストケース4: Quarterly → 2025Q2 (6月)")
    print("=" * 60)
    result4 = exporter._generate_data_version("2025-06-30", "quarterly")
    print(f"fiscal_year_end: 2025-06-30, report_type: quarterly")
    print(f"結果: {result4}")
    assert result4 == "2025Q2", f"期待値: 2025Q2, 実際: {result4}"
    print("[OK] テストケース4 成功\n")

    # テストケース5: Quarterly → 2025Q4 (12月)
    print("=" * 60)
    print("テストケース5: Quarterly → 2025Q4 (12月)")
    print("=" * 60)
    result5 = exporter._generate_data_version("2025-12-31", "quarterly")
    print(f"fiscal_year_end: 2025-12-31, report_type: quarterly")
    print(f"結果: {result5}")
    assert result5 == "2025Q4", f"期待値: 2025Q4, 実際: {result5}"
    print("[OK] テストケース5 成功\n")

    # テストケース6: None → UNKNOWN
    print("=" * 60)
    print("テストケース6: None → UNKNOWN")
    print("=" * 60)
    result6 = exporter._generate_data_version(None, None)
    print(f"fiscal_year_end: None, report_type: None")
    print(f"結果: {result6}")
    assert result6 == "UNKNOWN", f"期待値: UNKNOWN, 実際: {result6}"
    print("[OK] テストケース6 成功\n")

    # テストケース7: 空文字列 → UNKNOWN
    print("=" * 60)
    print("テストケース7: 空文字列 → UNKNOWN")
    print("=" * 60)
    result7 = exporter._generate_data_version("", "annual")
    print(f"fiscal_year_end: '', report_type: annual")
    print(f"結果: {result7}")
    assert result7 == "UNKNOWN", f"期待値: UNKNOWN, 実際: {result7}"
    print("[OK] テストケース7 成功\n")

    # テストケース8: unknown report_type → FY形式
    print("=" * 60)
    print("テストケース8: unknown report_type → FY形式")
    print("=" * 60)
    result8 = exporter._generate_data_version("2025-03-31", "unknown")
    print(f"fiscal_year_end: 2025-03-31, report_type: unknown")
    print(f"結果: {result8}")
    assert result8 == "2025FY", f"期待値: 2025FY, 実際: {result8}"
    print("[OK] テストケース8 成功\n")

    # 実際のパイプラインでの動作確認
    print("=" * 60)
    print("実際のパイプラインでの動作確認")
    print("=" * 60)
    dummy_valuation_dict = {
        "doc_id": "S100W67S",
        "security_code": "4827",
        "fiscal_year_end": "2025-03-31",
        "report_type": "annual",
        "current_year": {
            "metrics": {
                "equity": 5805695000.0,
                "net_sales": 16094118000.0,
                "earnings_per_share": 199.68,
                "roe": 0.1426976,
                "eps_growth": 0.11481234,
            },
            "market": {
                "stock_price": 2500.0,
                "shares_outstanding": 5000000,
                "market_cap": 12500000000.0,
                "dividend_per_share": 50.0,
            },
            "valuation": {
                "per": 12.520032051282051,
                "pbr": 2.1530583332400344,
                "psr": 0.7766812695172236,
                "peg": 1.0901618574162026,
                "dividend_yield": 0.020000123,
            },
        },
        "prior_year": {
            "metrics": {
                "equity": 5018725000.0,
                "net_sales": 13409224000.0,
                "earnings_per_share": 179.11,
                "roe": 0.1481234,
            },
        },
    }

    output_path = exporter.export(dummy_valuation_dict)
    print(f"保存パス: {output_path}")

    # JSON 読み込み確認
    with open(output_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    print("\n--- JSON 構造確認 ---")
    print(f"schema_version: {loaded.get('schema_version')}")
    print(f"engine_version: {loaded.get('engine_version')}")
    print(f"data_version: {loaded.get('data_version')}")
    print(f"generated_at: {loaded.get('generated_at')}")
    print(f"fiscal_year_end: {loaded.get('fiscal_year_end')}")
    print(f"report_type: {loaded.get('report_type')}")

    # 検証
    checks = []
    checks.append(("data_version が FY形式", loaded.get("data_version") == "2025FY"))
    checks.append(("fiscal_year_end 存在", loaded.get("fiscal_year_end") == "2025-03-31"))
    checks.append(("report_type 存在", loaded.get("report_type") == "annual"))
    checks.append(("generated_at 存在", loaded.get("generated_at") is not None))

    print("\n--- 検証結果 ---")
    all_ok = True
    for name, result in checks:
        status = "[OK]" if result else "[NG]"
        print(f"{status} {name}: {result}")
        if not result:
            all_ok = False

    if all_ok:
        print("\n[OK] すべてのテストが成功しました")
    else:
        print("\n[NG] 一部のテストが失敗しました")
        sys.exit(1)
