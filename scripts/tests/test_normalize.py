"""
FactNormalizer 動作確認用スクリプト。
main.py に影響を与えない。プロジェクトルートから実行すること。

使用例:
    python scripts/test_normalize.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer

if __name__ == "__main__":
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print(f"XBRLファイルが見つかりません: {xbrl_path}")
        sys.exit(1)

    parser = XBRLParser(xbrl_path)
    parsed_data = parser.parse()
    resolver = ContextResolver(parser.root)
    context_map = resolver.build_context_map()

    normalizer = FactNormalizer(parsed_data, context_map)
    result = normalizer.normalize()

    print("=" * 60)
    print("正規化結果サマリ")
    print("=" * 60)
    print("doc_id:", result["doc_id"])
    print("security_code:", result["security_code"])
    print("company_name:", result["company_name"])
    print("accounting_standard:", result["accounting_standard"])
    print("is_consolidated:", result["is_consolidated"])

    print("\n" + "=" * 60)
    print("current_year.pl")
    print("=" * 60)
    for k, v in result["current_year"]["pl"].items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("prior_year.pl")
    print("=" * 60)
    for k, v in result["prior_year"]["pl"].items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("current_year.bs")
    print("=" * 60)
    for k, v in result["current_year"]["bs"].items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("prior_year.bs")
    print("=" * 60)
    for k, v in result["prior_year"]["bs"].items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("current_year.cf")
    print("=" * 60)
    for k, v in result["current_year"]["cf"].items():
        print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("prior_year.cf")
    print("=" * 60)
    for k, v in result["prior_year"]["cf"].items():
        print(f"  {k}: {v}")

    # 完成条件の確認
    print("\n" + "=" * 60)
    print("完成条件チェック")
    print("=" * 60)
    checks = [
        ("current_year.pl.net_sales が存在する", result["current_year"]["pl"].get("net_sales") is not None),
        ("prior_year.pl.net_sales が存在する", result["prior_year"]["pl"].get("net_sales") is not None),
        ("BSデータが入る", any(v is not None for v in result["current_year"]["bs"].values())),
        ("CFデータが入る", any(v is not None for v in result["current_year"]["cf"].values())),
        ("security_code が取得できる", result["security_code"] is not None),
        ("is_consolidated が正しく判定される", isinstance(result["is_consolidated"], bool)),
    ]
    for desc, ok in checks:
        print(f"  {'OK' if ok else 'NG'}: {desc}")
