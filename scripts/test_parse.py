"""
Phase2 Step1 パーサー動作確認用スクリプト。
main.py に影響を与えない。プロジェクトルートから実行すること。

使用例:
    python scripts/test_parse.py
"""
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

from parser.xbrl_parser import XBRLParser

if __name__ == "__main__":
    # 実在するXBRLの例（環境に合わせてパスを変更可能）
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print(f"XBRLファイルが見つかりません: {xbrl_path}")
        print("data/edinet/raw_xbrl/ 以下にXBRLを配置するか、パスを変更してください。")
        sys.exit(1)

    parser = XBRLParser(xbrl_path)
    data = parser.parse()

    print("doc_id:", data["doc_id"])
    print("taxonomy_version:", data["taxonomy_version"])
    print("facts count:", len(data["facts"]))
    print("facts (all):")
    for i, f in enumerate(data["facts"]):
        print(f"  {i + 1}. {f}")
