"""
ContextResolver 動作確認用スクリプト。
main.py に影響を与えない。プロジェクトルートから実行すること。

使用例:
    python scripts/test_context.py
"""
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver

if __name__ == "__main__":
    # 実在するXBRLの例（環境に合わせてパスを変更可能）
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print(f"XBRLファイルが見つかりません: {xbrl_path}")
        print("data/edinet/raw_xbrl/ 以下にXBRLを配置するか、パスを変更してください。")
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

    # CurrentYearDurationの存在確認
    print("\n" + "=" * 60)
    print("CurrentYearDuration関連のcontext:")
    print("=" * 60)
    for context_id, context_info in context_map.items():
        if "CurrentYearDuration" in context_id:
            print(f"  {context_id}: {context_info}")

    print("\n" + "=" * 60)
    print("Enriched Fact (サンプル):")
    print("=" * 60)
    if data["facts"]:
        # CurrentYearDurationのfactを探す
        current_year_fact = None
        for fact in data["facts"]:
            if fact.get("contextRef") == "CurrentYearDuration":
                current_year_fact = fact
                break
        
        # Prior1YearDurationのfactを探す
        prior_year_fact = None
        for fact in data["facts"]:
            if fact.get("contextRef") == "Prior1YearDuration":
                prior_year_fact = fact
                break
        
        # CurrentYearDurationのfactを表示
        if current_year_fact:
            print("CurrentYearDuration:")
            print(f"  元のfact: {current_year_fact}")
            enriched_current = resolver.enrich_fact(current_year_fact)
            print(f"  context: {enriched_current['context']}")
        
        # Prior1YearDurationのfactを表示
        if prior_year_fact:
            print("\nPrior1YearDuration:")
            print(f"  元のfact: {prior_year_fact}")
            enriched_prior = resolver.enrich_fact(prior_year_fact)
            print(f"  context: {enriched_prior['context']}")
        
        # 見つからない場合は最初のfactを表示
        if not current_year_fact and not prior_year_fact:
            sample_fact = data["facts"][0]
            print("元のfact:")
            print(f"  {sample_fact}")
            enriched = resolver.enrich_fact(sample_fact)
            print("\nenrich後:")
            print(f"  tag: {enriched['tag']}")
            print(f"  value: {enriched['value']}")
            print(f"  unit: {enriched['unit']}")
            print(f"  context: {enriched['context']}")

    print("\n" + "=" * 60)
    print("統計情報:")
    print("=" * 60)
    print(f"総context数: {len(context_map)}")
    duration_count = sum(1 for c in context_map.values() if c.get("type") == "duration")
    instant_count = sum(1 for c in context_map.values() if c.get("type") == "instant")
    print(f"duration: {duration_count}件")
    print(f"instant: {instant_count}件")
