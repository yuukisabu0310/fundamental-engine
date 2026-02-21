"""
FinancialMaster 動作確認用スクリプト。
Fact-only出力（会計定義明示・EPS分離・period保持）を検証する。

使用例:
    python scripts/test_financial_master.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

from parser.xbrl_parser import XBRLParser
from parser.context_resolver import ContextResolver
from normalizer.fact_normalizer import FactNormalizer
from financial.financial_master import FinancialMaster

FACT_KEYS = {
    "total_assets", "equity", "interest_bearing_debt",
    "net_sales", "operating_income",
    "net_income_attributable_to_parent",
    "earnings_per_share_basic", "earnings_per_share_diluted",
    "shares_outstanding",
}

DERIVED_KEYS = {
    "roe", "roa", "roic", "operating_margin", "net_margin",
    "equity_ratio", "de_ratio",
    "sales_growth", "profit_growth", "eps_growth",
    "free_cash_flow", "cagr",
    "profit_loss", "earnings_per_share",
}

if __name__ == "__main__":
    xbrl_path = project_root / "data/edinet/raw_xbrl/2025/S100W67S/jpcrp030000-asr-001_E05325-000_2025-03-31_01_2025-06-25.xbrl"

    if not xbrl_path.exists():
        print("XBRLファイルが見つかりません:", xbrl_path)
        sys.exit(1)

    parser = XBRLParser(xbrl_path)
    parsed_data = parser.parse()
    resolver = ContextResolver(parser.root)
    context_map = resolver.build_context_map()
    normalizer = FactNormalizer(parsed_data, context_map)
    normalized_data = normalizer.normalize()

    master = FinancialMaster(normalized_data)
    result = master.compute()

    print("=" * 60)
    print("FinancialMaster Fact-only 出力")
    print("=" * 60)
    print("doc_id:", result.get("doc_id"))
    print("security_code:", result.get("security_code"))
    print("fiscal_year_end:", result.get("fiscal_year_end"))
    print("report_type:", result.get("report_type"))
    print("consolidation_type:", result.get("consolidation_type"))
    print("accounting_standard:", result.get("accounting_standard"))

    current = result.get("current_year", {})
    prior = result.get("prior_year", {})
    current_metrics = current.get("metrics", {})
    prior_metrics = prior.get("metrics", {})

    print("\n--- current_year.period ---")
    print(current.get("period", "なし"))

    print("\n--- current_year.metrics ---")
    for k, v in current_metrics.items():
        print(f"  {k}: {v}")

    if prior_metrics:
        print("\n--- prior_year.period ---")
        print(prior.get("period", "なし"))
        print("\n--- prior_year.metrics ---")
        for k, v in prior_metrics.items():
            print(f"  {k}: {v}")
    else:
        print("\n--- prior_year: なし ---")

    print("\n--- 検証結果 ---")
    checks = []

    checks.append(("current_year.metrics が存在", bool(current_metrics)))
    checks.append(("consolidation_type が存在", result.get("consolidation_type") is not None))
    checks.append(("accounting_standard が存在", result.get("accounting_standard") is not None))

    all_keys = set(current_metrics.keys()) | set(prior_metrics.keys())
    leaked = all_keys & DERIVED_KEYS
    checks.append(("Derived指標が混入していない", len(leaked) == 0))

    unknown_keys = all_keys - FACT_KEYS
    checks.append(("未定義キーが混入していない", len(unknown_keys) == 0))

    all_values = list(current_metrics.values()) + list(prior_metrics.values())
    has_null = any(v is None for v in all_values)
    checks.append(("null値なし", not has_null))

    checks.append(("net_income_attributable_to_parent 存在",
                    "net_income_attributable_to_parent" in current_metrics))
    checks.append(("earnings_per_share_basic 存在",
                    "earnings_per_share_basic" in current_metrics))
    checks.append(("旧キー profit_loss 不在", "profit_loss" not in current_metrics))
    checks.append(("旧キー earnings_per_share 不在", "earnings_per_share" not in current_metrics))

    checks.append(("current_year.period 存在", "period" in current))

    all_ok = True
    for name, ok in checks:
        status = "[OK]" if ok else "[NG]"
        print(f"{status} {name}")
        if not ok:
            all_ok = False

    if leaked:
        print(f"\n  Derived混入キー: {leaked}")
    if unknown_keys:
        print(f"\n  未定義キー: {unknown_keys}")

    if all_ok:
        print("\n[OK] すべての検証が成功しました（Fact-only）")
    else:
        print("\n[NG] 一部の検証が失敗しました")
        sys.exit(1)
