"""
Phase6 DatasetManifestGenerator 動作確認用スクリプト（型安全・拡張可能設計）。
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

from output.manifest_generator import DatasetManifestGenerator

if __name__ == "__main__":
    print("=" * 60)
    print("DatasetManifestGenerator テスト（型安全・拡張可能設計）")
    print("=" * 60)

    generator = DatasetManifestGenerator("financial-dataset")
    manifest_path = generator.save()

    print(f"\n保存パス: {manifest_path}")

    # ファイル存在確認
    path_obj = Path(manifest_path)
    if path_obj.exists():
        print("[OK] ファイルが存在します")
    else:
        print("[NG] ファイルが存在しません")
        sys.exit(1)

    # JSON 読み込み確認
    with open(path_obj, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    print("\n--- Manifest 構造確認 ---")
    print(f"schema_version: {manifest.get('schema_version')}")
    print(f"engine_version: {manifest.get('engine_version')}")
    print(f"generated_at: {manifest.get('generated_at')}")
    print(f"latest_annual: {manifest.get('latest_annual')}")
    print(f"latest_quarterly: {manifest.get('latest_quarterly')}")

    annual_periods = manifest.get("annual_periods", [])
    quarterly_periods = manifest.get("quarterly_periods", [])
    record_counts = manifest.get("record_counts", {})
    annual_counts = record_counts.get("annual", {})
    quarterly_counts = record_counts.get("quarterly", {})

    print(f"\n--- Record Counts ---")
    print(f"Annual periods: {len(annual_periods)}")
    if annual_periods:
        print("Annual counts:")
        for period in annual_periods:
            count = annual_counts.get(period, 0)
            print(f"  {period}: {count}件")

    print(f"\nQuarterly periods: {len(quarterly_periods)}")
    if quarterly_periods:
        print("Quarterly counts:")
        for period in quarterly_periods:
            count = quarterly_counts.get(period, 0)
            print(f"  {period}: {count}件")

    # 型安全性検証
    print("\n--- 型安全性検証 ---")
    checks = []

    # schema_version と engine_version
    checks.append(("schema_version 存在", manifest.get("schema_version") == "1.0"))
    checks.append(("engine_version 存在", manifest.get("engine_version") is not None))
    checks.append(("generated_at 存在", manifest.get("generated_at") is not None))

    # 型チェック: annual_periods は常に list
    is_annual_list = isinstance(annual_periods, list)
    checks.append(("annual_periods が list", is_annual_list))
    if not is_annual_list:
        print(f"[NG] annual_periods の型が不正: {type(annual_periods)}")

    # 型チェック: quarterly_periods は常に list
    is_quarterly_list = isinstance(quarterly_periods, list)
    checks.append(("quarterly_periods が list", is_quarterly_list))
    if not is_quarterly_list:
        print(f"[NG] quarterly_periods の型が不正: {type(quarterly_periods)}")

    # 型チェック: record_counts.annual は常に dict
    is_annual_dict = isinstance(annual_counts, dict)
    checks.append(("record_counts.annual が dict", is_annual_dict))
    if not is_annual_dict:
        print(f"[NG] record_counts.annual の型が不正: {type(annual_counts)}")

    # 型チェック: record_counts.quarterly は常に dict（null禁止）
    is_quarterly_dict = isinstance(quarterly_counts, dict)
    checks.append(("record_counts.quarterly が dict（null禁止）", is_quarterly_dict))
    if not is_quarterly_dict:
        print(f"[NG] record_counts.quarterly の型が不正: {type(quarterly_counts)}")

    # latest_annual の検証
    if annual_periods:
        latest_annual = manifest.get("latest_annual")
        sorted_annual = sorted(annual_periods, reverse=True)
        checks.append(
            (
                "latest_annual が正しい",
                latest_annual == sorted_annual[0] if sorted_annual else latest_annual is None,
            )
        )
    else:
        checks.append(("latest_annual が None", manifest.get("latest_annual") is None))

    # latest_quarterly の検証
    if quarterly_periods:
        latest_quarterly = manifest.get("latest_quarterly")
        sorted_quarterly = sorted(quarterly_periods, reverse=True)
        checks.append(
            (
                "latest_quarterly が正しい",
                latest_quarterly == sorted_quarterly[0] if sorted_quarterly else latest_quarterly is None,
            )
        )
    else:
        checks.append(("latest_quarterly が None", manifest.get("latest_quarterly") is None))

    # 降順ソート確認
    if len(annual_periods) > 1:
        is_sorted_desc = annual_periods == sorted(annual_periods, reverse=True)
        checks.append(("annual_periods が降順ソート", is_sorted_desc))

    if len(quarterly_periods) > 1:
        is_sorted_desc = quarterly_periods == sorted(quarterly_periods, reverse=True)
        checks.append(("quarterly_periods が降順ソート", is_sorted_desc))

    print("\n--- 検証結果 ---")
    all_ok = True
    for name, result in checks:
        status = "[OK]" if result else "[NG]"
        print(f"{status} {name}: {result}")
        if not result:
            all_ok = False

    if all_ok:
        print("\n[OK] すべてのテストが成功しました（型安全・拡張可能設計）")
    else:
        print("\n[NG] 一部のテストが失敗しました")
        sys.exit(1)
