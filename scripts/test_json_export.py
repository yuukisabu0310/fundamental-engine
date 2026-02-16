"""
Phase5 JSONExporter 動作確認用スクリプト。
main.py に影響を与えない。プロジェクトルートから実行すること。

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
    # ダミーデータ（ValuationEngine の出力形式）
    # 数値精度テスト用に、丸めが必要な値を含める
    dummy_valuation_dict = {
        "doc_id": "S100W67S",
        "security_code": "4827",
        "current_year": {
            "metrics": {
                "equity": 5805695000.0,
                "net_sales": 16094118000.0,
                "earnings_per_share": 199.68,
                "roe": 0.1426976,  # 小数4桁に丸めるべき値
                "eps_growth": 0.11481234,  # 小数4桁に丸めるべき値
            },
            "market": {
                "stock_price": 2500.0,
                "shares_outstanding": 5000000,
                "market_cap": 12500000000.0,
                "dividend_per_share": 50.0,
            },
            "valuation": {
                "per": 12.520032051282051,  # 小数2桁に丸めるべき値
                "pbr": 2.1530583332400344,  # 小数2桁に丸めるべき値
                "psr": 0.7766812695172236,  # 小数2桁に丸めるべき値
                "peg": 1.0901618574162026,  # 小数2桁に丸めるべき値
                "dividend_yield": 0.020000123,  # 小数4桁に丸めるべき値
            },
        },
        "prior_year": {
            "metrics": {
                "equity": 5018725000.0,
                "net_sales": 13409224000.0,
                "earnings_per_share": 179.11,
                "roe": 0.1481234,  # 小数4桁に丸めるべき値
            },
        },
    }

    exporter = JSONExporter()
    output_path = exporter.export(dummy_valuation_dict)

    print("=" * 60)
    print("JSONExporter テスト結果")
    print("=" * 60)
    print(f"保存パス: {output_path}")

    # ファイル存在確認
    path_obj = Path(output_path)
    if path_obj.exists():
        print("[OK] ファイルが存在します")
    else:
        print("[NG] ファイルが存在しません")
        sys.exit(1)

    # JSON 読み込み確認
    with open(path_obj, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    print("\n--- JSON 構造確認 ---")
    print(f"schema_version: {loaded.get('schema_version')}")
    print(f"engine_version: {loaded.get('engine_version')}")
    print(f"data_version: {loaded.get('data_version')}")
    print(f"generated_at: {loaded.get('generated_at')}")
    print(f"doc_id: {loaded.get('doc_id')}")
    print(f"security_code: {loaded.get('security_code')}")

    # 数値精度確認
    current_metrics = loaded.get("current_year", {}).get("metrics", {})
    current_valuation = loaded.get("current_year", {}).get("valuation", {})
    prior_metrics = loaded.get("prior_year", {}).get("metrics", {})

    print("\n--- 数値精度確認 ---")
    roe = current_metrics.get("roe")
    eps_growth = current_metrics.get("eps_growth")
    per = current_valuation.get("per")
    pbr = current_valuation.get("pbr")
    dividend_yield = current_valuation.get("dividend_yield")

    print(f"roe: {roe} (期待: 小数4桁)")
    print(f"eps_growth: {eps_growth} (期待: 小数4桁)")
    print(f"per: {per} (期待: 小数2桁)")
    print(f"pbr: {pbr} (期待: 小数2桁)")
    print(f"dividend_yield: {dividend_yield} (期待: 小数4桁)")

    # 検証
    checks = []
    checks.append(("schema_version 存在", loaded.get("schema_version") == "1.0"))
    checks.append(("engine_version 存在", loaded.get("engine_version") is not None))
    checks.append(("data_version 存在", loaded.get("data_version") is not None))
    checks.append(("generated_at 存在", loaded.get("generated_at") is not None))
    checks.append(("current_year 存在", "current_year" in loaded))
    checks.append(("current_year.valuation 存在", "valuation" in loaded.get("current_year", {})))
    checks.append(("prior_year 存在", "prior_year" in loaded))

    # 数値精度検証
    def count_decimal_places(value):
        """小数部の桁数をカウント"""
        if value is None:
            return None
        s = str(value)
        if "." not in s:
            return 0
        return len(s.split(".")[1])

    # roe が小数4桁以下か確認（0.1426976 → 0.1427 になるはず）
    if roe is not None:
        roe_decimal = count_decimal_places(roe)
        checks.append(("roe が小数4桁以下", roe_decimal is not None and roe_decimal <= 4))
        checks.append(("roe が正しく丸められている", abs(roe - 0.1427) < 0.0001))

    # per が小数2桁以下か確認（12.520032 → 12.52 になるはず）
    if per is not None:
        per_decimal = count_decimal_places(per)
        checks.append(("per が小数2桁以下", per_decimal is not None and per_decimal <= 2))
        checks.append(("per が正しく丸められている", abs(per - 12.52) < 0.01))

    print("\n--- 検証結果 ---")
    all_ok = True
    for name, result in checks:
        status = "[OK]" if result else "[NG]"
        print(f"{status} {name}: {result}")
        if not result:
            all_ok = False

    if all_ok:
        print("\n[OK] すべての検証が成功しました")
    else:
        print("\n[NG] 一部の検証が失敗しました")
        sys.exit(1)
