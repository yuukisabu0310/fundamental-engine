"""
ContextResolver 動作確認用スクリプト。
context_map の構築結果を検証する。

使用例:
    python scripts/tests/test_context.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver

if __name__ == "__main__":
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print(f"XBRLファイルが見つかりません: {xbrl_path}")
        sys.exit(1)

    parser = XBRLParser(xbrl_path)
    data = parser.parse()

    resolver = ContextResolver(parser.root)
    context_map = resolver.build_context_map()

    print("=" * 60)
    print("Context Map (最初の10件):")
    print("=" * 60)
    for i, (context_id, context_info) in enumerate(list(context_map.items())[:10]):
        print(f"{i + 1}. {context_id}: {context_info}")

    print("\n" + "=" * 60)
    print("CurrentYearDuration関連のcontext:")
    print("=" * 60)
    for context_id, context_info in context_map.items():
        if "CurrentYearDuration" in context_id:
            print(f"  {context_id}: {context_info}")

    print("\n" + "=" * 60)
    print("統計情報:")
    print("=" * 60)
    print(f"総context数: {len(context_map)}")
    duration_count = sum(1 for c in context_map.values() if c.get("type") == "duration")
    instant_count = sum(1 for c in context_map.values() if c.get("type") == "instant")
    print(f"duration: {duration_count}件")
    print(f"instant: {instant_count}件")
