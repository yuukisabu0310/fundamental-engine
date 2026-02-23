"""
NULL理由4分類スクリプト。

全XBRLデータを処理し、canonical_factの各NULLを以下に分類する：
  1. 経済実態NULL  … 当該企業にその経済事象が存在しない
  2. 会計基準差NULL … 会計基準・業種特性により当該概念が存在しない
  3. 空値NULL      … タグは存在するが値が xsi:nil="true" / 空文字
  4. 取得失敗NULL  … データは存在するはずだがパイプラインが取得できていない

日付認識: current_year_end に一致するコンテキストの fact のみ対象。

使用例:
    python scripts/analysis/classify_null_reasons.py
"""
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path

from _pipeline import (
    PROJECT_ROOT,
    FACT_KEYS,
    collect_xbrl_files,
    normalize_code,
    check_form_code,
    run_pipeline,
)

logging.basicConfig(level=logging.WARNING)

# =========================================================================
# 会計基準差 NULL の静的ルール
# =========================================================================
ACCOUNTING_STD_NULL_RULES: dict[str, list[tuple]] = {
    "ordinary_income": [
        (lambda meta: meta["acct_std"] in ("IFRS", "US-GAAP"),
         "IFRSに経常利益概念なし"),
    ],
    "net_sales": [
        (lambda meta: meta["is_bank"],
         "銀行PL構造に売上高概念なし（資金運用収益等を使用）"),
    ],
    "operating_income": [
        (lambda meta: meta["is_bank"],
         "銀行PL構造に営業利益概念なし（業務粗利益等を使用）"),
    ],
}

# =========================================================================
# 取得失敗 NULL 判定用: canonical key → XBRL タグパターン
# =========================================================================
EXTENDED_TAG_HINTS: dict[str, list[str]] = {
    "net_sales": [
        "NetSales", "Revenue", "OperatingRevenue", "GrossOperatingRevenue",
        "OperatingIncomeSPF", "RentalRevenueOfRealEstateAndOther",
    ],
    "operating_income": ["OperatingIncome", "OperatingProfit"],
    "ordinary_income": ["OrdinaryIncome", "OrdinaryProfit"],
    "total_assets": ["TotalAssets", "Assets", "TotalAssetsSPF"],
    "net_income_attributable_to_parent": ["ProfitLossAttributableToOwnersOfParent"],
    "total_number_of_issued_shares": [
        "TotalNumberOfIssuedShares", "IssuedShares",
        "NumberOfIssuedShares", "TotalUnitsIssued",
    ],
    "cash_and_equivalents": ["CashAndCashEquivalents", "CashAndDeposits"],
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities",
    ],
    "depreciation": ["Depreciation", "DepreciationAndAmortization"],
    "dividends_per_share": [
        "DividendPaidPerShare", "DividendPerShare", "DistributionPerUnit",
    ],
    "short_term_borrowings": [
        "ShortTermBorrowings", "ShortTermLoansPayable", "BorrowingsCL",
    ],
    "current_portion_of_long_term_borrowings": [
        "CurrentPortionOfLongTermLoans", "CurrentPortionOfLongTermBorrowings",
    ],
    "commercial_papers": ["CommercialPaper"],
    "current_portion_of_bonds": ["CurrentPortionOfBonds", "BondsPayableCL"],
    "bonds_payable": ["BondsPayable"],
    "long_term_borrowings": [
        "LongTermLoansPayable", "LongTermBorrowings", "BorrowingsNCL",
    ],
    "short_term_lease_obligations": [
        "LeaseObligationsCL", "ShortTermLease", "LeaseLiabilitiesCL",
    ],
    "long_term_lease_obligations": [
        "LeaseObligationsNCL", "LongTermLease", "LeaseLiabilitiesNCL",
    ],
    "lease_obligations": [],
    "equity": [
        "ShareholdersEquity", "NetAssets",
        "EquityAttributableToOwnersOfParent", "TotalEquity",
    ],
}

BANK_INDICATOR_TAGS = [
    "InterestIncome", "InterestExpense",
    "TrustFees", "FeesAndCommissions",
    "OrdinaryRevenue", "OrdinaryExpense",
    "FundOperationRevenue", "FundRaisingCost",
    "FundProfitOrLoss",
]

BS_DEBT_KEYS = {
    "short_term_borrowings", "current_portion_of_long_term_borrowings",
    "commercial_papers", "current_portion_of_bonds", "bonds_payable",
    "long_term_borrowings", "short_term_lease_obligations",
    "long_term_lease_obligations", "lease_obligations",
}

ACCT_STD_NORMALIZE = {
    "Japan GAAP": "JGAAP", "日本基準": "JGAAP", "JGAAP": "JGAAP",
    "IFRS": "IFRS", "US GAAP": "US-GAAP", "US-GAAP": "US-GAAP",
}


# =========================================================================
# ヘルパー関数
# =========================================================================

def _tag_local(tag: str) -> str:
    return tag.split(":")[-1] if ":" in tag else tag


def _has_tag_in_facts(facts: list[dict], patterns: list[str]) -> list[str]:
    """raw facts に patterns のいずれかを含むタグが存在するか。見つかったパターンを返す。"""
    found: list[str] = []
    for pat in patterns:
        for f in facts:
            if pat in _tag_local(f.get("tag", "")):
                found.append(pat)
                break
    return found


def _get_context_date(ctx_ref: str, context_map: dict) -> str:
    """コンテキストから該当日付を取得。"""
    ctx = context_map.get(ctx_ref, {})
    if ctx.get("type") == "instant":
        return ctx.get("date", "")
    elif ctx.get("type") == "duration":
        return ctx.get("end_date", "")
    return ""


def _find_matching_facts_detail_dated(
    facts: list[dict],
    patterns: list[str],
    context_map: dict,
    target_date: str | None,
) -> tuple[bool, bool, bool]:
    """日付認識版: target_date のコンテキストのみ対象で fact の存在と値を判定する。"""
    if not target_date:
        return _find_matching_facts_detail(facts, patterns)

    tag_exists = False
    has_value = False
    for f in facts:
        local = _tag_local(f.get("tag", ""))
        for pat in patterns:
            if pat in local:
                ctx_date = _get_context_date(f.get("contextRef", ""), context_map)
                if ctx_date != target_date:
                    break
                tag_exists = True
                val = (f.get("value") or "").strip()
                if val and not f.get("is_nil", False):
                    has_value = True
                break
    return tag_exists, has_value, (tag_exists and not has_value)


def _find_matching_facts_detail(
    facts: list[dict], patterns: list[str],
) -> tuple[bool, bool, bool]:
    """patterns を含むタグの存在・値の状態を返す。"""
    tag_exists = False
    has_value = False
    for f in facts:
        local = _tag_local(f.get("tag", ""))
        for pat in patterns:
            if pat in local:
                tag_exists = True
                val = (f.get("value") or "").strip()
                if val and not f.get("is_nil", False):
                    has_value = True
                break
    return tag_exists, has_value, (tag_exists and not has_value)


def _has_tag_in_consolidated_context_dated(
    facts: list[dict],
    patterns: list[str],
    context_map: dict,
    target_date: str | None,
) -> tuple[bool, bool, bool]:
    """日付認識版: 連結/個別コンテキストでのタグ存在判定。"""
    if not target_date:
        return _has_tag_in_consolidated_context(facts, patterns)

    in_consol = False
    in_non_consol = False
    consol_has_value = False
    for f in facts:
        local = _tag_local(f.get("tag", ""))
        ctx_ref = f.get("contextRef", "")
        for pat in patterns:
            if pat in local:
                ctx_date = _get_context_date(ctx_ref, context_map)
                if ctx_date != target_date:
                    break
                val = (f.get("value") or "").strip()
                is_nil = f.get("is_nil", False)
                if "NonConsolidated" in ctx_ref:
                    in_non_consol = True
                else:
                    in_consol = True
                    if val and not is_nil:
                        consol_has_value = True
                break
    return in_consol, in_non_consol, (in_consol and not consol_has_value)


def _has_tag_in_consolidated_context(
    facts: list[dict], patterns: list[str],
) -> tuple[bool, bool, bool]:
    """連結/個別コンテキストでのタグ存在判定。"""
    in_consol = False
    in_non_consol = False
    consol_has_value = False
    for f in facts:
        local = _tag_local(f.get("tag", ""))
        ctx_ref = f.get("contextRef", "")
        for pat in patterns:
            if pat in local:
                val = (f.get("value") or "").strip()
                is_nil = f.get("is_nil", False)
                if "NonConsolidated" in ctx_ref:
                    in_non_consol = True
                else:
                    in_consol = True
                    if val and not is_nil:
                        consol_has_value = True
                break
    return in_consol, in_non_consol, (in_consol and not consol_has_value)


def _detect_bank(facts: list[dict]) -> bool:
    return len(_has_tag_in_facts(facts, BANK_INDICATOR_TAGS)) >= 2


def _detect_reit(filename: str) -> bool:
    return "jpsps" in filename.lower()


# =========================================================================
# パイプライン実行
# =========================================================================

def process_xbrl(xbrl_path: Path) -> dict | None:
    """1ファイルを処理し、分類に必要な情報を返す。"""
    try:
        parsed, ctx_map, normalizer, _normalized, result = run_pipeline(xbrl_path)
        return {
            "xbrl_path": str(xbrl_path),
            "xbrl_filename": xbrl_path.name,
            "security_code": normalize_code(result.get("security_code", "")),
            "accounting_standard": result.get("accounting_standard"),
            "consolidation_type": result.get("consolidation_type"),
            "current_metrics": result.get("current_year", {}).get("metrics", {}),
            "raw_facts": parsed.get("facts", []),
            "form_code": check_form_code(xbrl_path.name),
            "context_map": ctx_map,
            "current_year_end": normalizer._current_year_end,
        }
    except Exception as e:
        return {"xbrl_path": str(xbrl_path), "error": str(e)}


# =========================================================================
# NULL 分類
# =========================================================================

def classify_nulls(result: dict) -> dict[str, list[tuple[str, str]]]:
    """1つの処理結果について NULL を4分類する。

    日付認識: current_year_end に一致するコンテキストの fact のみ対象とし、
    前期・前々期にのみ存在するタグを誤って「取得失敗」に分類しない。
    """
    metrics = result.get("current_metrics", {})
    raw_facts = result.get("raw_facts", [])
    context_map = result.get("context_map", {})
    current_year_end = result.get("current_year_end")
    raw_std = result.get("accounting_standard") or ""
    acct_std = ACCT_STD_NORMALIZE.get(raw_std, raw_std)
    is_bank = _detect_bank(raw_facts)
    is_reit = _detect_reit(result.get("xbrl_filename", ""))
    consol = result.get("consolidation_type", "")

    meta = {"acct_std": acct_std, "is_bank": is_bank, "is_reit": is_reit, "consol": consol}
    classification: dict[str, list[tuple[str, str]]] = {
        "経済実態": [], "会計基準差": [], "空値": [], "取得失敗": [],
    }

    for key, value in metrics.items():
        if value is not None:
            continue

        if key in ACCOUNTING_STD_NULL_RULES:
            matched = False
            for cond_fn, reason in ACCOUNTING_STD_NULL_RULES[key]:
                if cond_fn(meta):
                    classification["会計基準差"].append((key, reason))
                    matched = True
                    break
            if matched:
                continue

        hints = EXTENDED_TAG_HINTS.get(key, [])
        if hints:
            if key in BS_DEBT_KEYS and consol == "consolidated":
                in_consol, in_non_consol, consol_all_nil = (
                    _has_tag_in_consolidated_context_dated(
                        raw_facts, hints, context_map, current_year_end,
                    )
                )
                if in_consol:
                    if consol_all_nil:
                        classification["空値"].append(
                            (key, "当期連結コンテキストにタグあり (xsi:nil/空値)"),
                        )
                    else:
                        classification["取得失敗"].append(
                            (key, "当期連結コンテキストに値ありタグが存在"),
                        )
                    continue
                elif in_non_consol and not in_consol:
                    classification["経済実態"].append(
                        (key, "連結BSに該当タグなし (個別BSのみ存在)"),
                    )
                    continue
            else:
                tag_exists, has_value, all_nil = _find_matching_facts_detail_dated(
                    raw_facts, hints, context_map, current_year_end,
                )
                if tag_exists:
                    if all_nil:
                        classification["空値"].append((key, "当期タグあり (xsi:nil/空値)"))
                    else:
                        classification["取得失敗"].append(
                            (key, "当期コンテキストに値ありタグが存在"),
                        )
                    continue

        if key == "lease_obligations":
            short_lease = metrics.get("short_term_lease_obligations")
            long_lease = metrics.get("long_term_lease_obligations")
            if short_lease is not None or long_lease is not None:
                classification["経済実態"].append((key, "CL/NCL分割済み（相互排他構造）"))
                continue

        classification["経済実態"].append((key, "raw XBRLに該当タグなし"))

    return classification


# =========================================================================
# レポート出力
# =========================================================================

def main() -> list[dict]:
    xbrl_files = collect_xbrl_files()

    print("=" * 90)
    print("  NULL理由4分類レポート")
    print("=" * 90)
    print(f"\n対象XBRLファイル数: {len(xbrl_files)}")

    results: list[dict] = []
    errors: list[dict] = []
    for xf in xbrl_files:
        r = process_xbrl(xf)
        if r and "error" in r:
            errors.append(r)
        elif r:
            results.append(r)

    print(f"処理成功: {len(results)} / 処理失敗: {len(errors)}")

    global_counts: dict[str, Counter] = {
        "経済実態": Counter(), "会計基準差": Counter(), "空値": Counter(), "取得失敗": Counter(),
    }
    per_key_total_null: Counter = Counter()
    total_companies = 0
    per_company_details: list[dict] = []

    for r in results:
        metrics = r.get("current_metrics", {})
        if not metrics:
            continue
        total_companies += 1
        cls = classify_nulls(r)
        per_company_details.append({
            "security_code": r["security_code"],
            "acct_std": ACCT_STD_NORMALIZE.get(r.get("accounting_standard", ""), ""),
            "form_code": r.get("form_code", ""),
            "classification": cls,
        })
        for category, items in cls.items():
            for key, _reason in items:
                global_counts[category][key] += 1
                per_key_total_null[key] += 1

    # --- 1. キー別 NULL 分類サマリー ---
    print(f"\n{'=' * 90}")
    print(f"  1. Canonical Key 別 NULL 分類 (全{total_companies}社)")
    print(f"{'=' * 90}")
    all_null_keys = sorted(per_key_total_null.keys(), key=lambda k: -per_key_total_null[k])
    header = f"{'canonical_key':<45} {'合計':>5} {'経済実態':>6} {'基準差':>6} {'空値':>6} {'取得失敗':>6} {'率%':>6}"
    print(f"\n{header}")
    print("-" * 100)
    for key in all_null_keys:
        total_null = per_key_total_null[key]
        eco = global_counts["経済実態"][key]
        std = global_counts["会計基準差"][key]
        nil_empty = global_counts["空値"][key]
        fail = global_counts["取得失敗"][key]
        rate = total_null / total_companies * 100 if total_companies > 0 else 0
        print(f"  {key:<43} {total_null:>5} {eco:>6} {std:>6} {nil_empty:>6} {fail:>6} {rate:>5.1f}%")

    # --- 2. カテゴリ別集計 ---
    for category in ("経済実態", "会計基準差", "空値", "取得失敗"):
        print(f"\n{'=' * 90}")
        print(f"  2-{['経済実態','会計基準差','空値','取得失敗'].index(category)+1}. {category}NULL 詳細")
        print(f"{'=' * 90}")
        counts = global_counts[category]
        if not counts:
            print("  (該当なし)")
            continue
        for key, cnt in counts.most_common():
            rate = cnt / total_companies * 100
            print(f"  {key:<43} {cnt:>5}社 ({rate:>5.1f}%)")

    # --- 3. 空値NULL代表例 ---
    print(f"\n{'=' * 90}")
    print(f"  3. 空値NULL 代表例 (xsi:nil / 空値 - 正常欠損)")
    print(f"{'=' * 90}")
    nil_examples: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for d in per_company_details:
        for key, reason in d["classification"]["空値"]:
            if len(nil_examples[key]) < 3:
                nil_examples[key].append((d["security_code"], reason))
    if not nil_examples:
        print("  (空値NULLなし)")
    else:
        for key in sorted(nil_examples.keys()):
            cnt = global_counts["空値"][key]
            rate = cnt / total_companies * 100
            print(f"\n  [{key}] {cnt}社 ({rate:.1f}%)")
            for sc, reason in nil_examples[key]:
                print(f"    - {sc}: {reason}")

    # --- 4. 取得失敗NULL代表例 ---
    print(f"\n{'=' * 90}")
    print(f"  4. 取得失敗NULL 代表例 (taxonomy_mapping.yaml 改善候補)")
    print(f"{'=' * 90}")
    fail_examples: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for d in per_company_details:
        for key, reason in d["classification"]["取得失敗"]:
            if len(fail_examples[key]) < 3:
                fail_examples[key].append((d["security_code"], reason))
    if not fail_examples:
        print("  (取得失敗NULLなし)")
    else:
        for key in sorted(fail_examples.keys()):
            cnt = global_counts["取得失敗"][key]
            rate = cnt / total_companies * 100
            print(f"\n  [{key}] {cnt}社 ({rate:.1f}%)")
            for sc, reason in fail_examples[key]:
                print(f"    - {sc}: {reason}")

    # --- 5. 会計基準差NULL内訳 ---
    print(f"\n{'=' * 90}")
    print(f"  5. 会計基準差NULL 内訳")
    print(f"{'=' * 90}")
    std_reasons: dict[str, Counter] = defaultdict(Counter)
    for d in per_company_details:
        for key, reason in d["classification"]["会計基準差"]:
            std_reasons[key][reason] += 1
    if not std_reasons:
        print("  (該当なし)")
    else:
        for key, reason_counts in sorted(std_reasons.items()):
            print(f"\n  [{key}]")
            for reason, cnt in reason_counts.most_common():
                print(f"    - {reason}: {cnt}社")

    # --- 6. 全体サマリー ---
    print(f"\n{'=' * 90}")
    print(f"  6. 全体サマリー")
    print(f"{'=' * 90}")
    total_eco = sum(global_counts["経済実態"].values())
    total_std = sum(global_counts["会計基準差"].values())
    total_nil = sum(global_counts["空値"].values())
    total_fail = sum(global_counts["取得失敗"].values())
    grand_total = total_eco + total_std + total_nil + total_fail
    print(f"\n  全NULL件数:      {grand_total}")
    if grand_total:
        print(f"  経済実態NULL:    {total_eco} ({total_eco/grand_total*100:.1f}%)")
        print(f"  会計基準差NULL:  {total_std} ({total_std/grand_total*100:.1f}%)")
        print(f"  空値NULL:        {total_nil} ({total_nil/grand_total*100:.1f}%)")
        print(f"  取得失敗NULL:    {total_fail} ({total_fail/grand_total*100:.1f}%)")
    if total_fail > 0:
        print(f"\n  [WARN] 取得失敗NULLが存在します。taxonomy_mapping.yaml の拡張を検討してください。")
    elif total_nil > 0:
        print(f"\n  [OK] 取得失敗NULLなし。空値NULL {total_nil}件は xsi:nil/空値による正常欠損です。")
    else:
        print(f"\n  [OK] 取得失敗NULL・空値NULLなし。カバレッジは十分です。")
    print()
    return per_company_details


if __name__ == "__main__":
    main()
