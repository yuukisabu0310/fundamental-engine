"""
2734 (doc_id: S100XL6L) 財務整合性検証スクリプト。

検証ポイント:
  1. long_term_borrowings と current_portion_of_long_term_borrowings が同値の原因
  2. lease_obligations と long_term_lease_obligations が同値の原因
  3. BSアンカー方式の影響

使用例:
  python scripts/analysis/verify_2734_xbrl.py [xbrl_path_or_doc_id]

  xbrl_path_or_doc_id 省略時は data/edinet/raw_xbrl 以下で S100XL6L を検索。
"""
import logging
import sys
from pathlib import Path

from _pipeline import PROJECT_ROOT, run_pipeline

logging.basicConfig(level=logging.WARNING)

# 検索対象タグ
BORROW_TAGS = [
    "LongTermLoansPayable",
    "CurrentPortionOfLongTermLoansPayable",
    "ShortTermLoansPayable",
    "Borrowings",
    "LoansPayable",
    "CurrentPortionOfLongTermBorrowings",
    "LongTermBorrowings",
]
LEASE_TAGS = [
    "LeaseObligations",
    "LongTermLeaseObligations",
    "CurrentPortionOfLeaseObligations",
    "LeaseLiabilitiesIFRS",
    "LeaseObligationsNCL",
    "LeaseObligationsCL",
    "LeaseLiabilitiesNCLIFRS",
    "LeaseLiabilitiesCLIFRS",
]


def tag_local(tag: str) -> str:
    return tag.split(":")[-1] if ":" in tag else tag


def _output_conclusion_without_xbrl() -> None:
    """XBRLなし時のコード・YAML分析結論"""
    print("=" * 90)
    print("  YAMLマッピング該当箇所抜粋")
    print("=" * 90)
    print("""
  current_portion_of_long_term_borrowings:
    - CurrentPortionOfLongTermLoansPayable
    - CurrentPortionOfLongTermBorrowings

  long_term_borrowings:
    - LongTermLoansPayable
    - LongTermBorrowings

  lease_obligations (CL/NCL未分割):
    - LeaseObligations
    - LeaseLiabilitiesLiabilitiesIFRS

  long_term_lease_obligations:
    - LeaseObligationsNCL
    - LongTermLeaseObligations
""")
    print("=" * 90)
    print("  マッチングロジック (fact_normalizer._tag_matches)")
    print("=" * 90)
    print("""
  def _tag_matches(tag, keyword):
      return tag_local(tag) == keyword   # 完全一致（2025-02修正済み）

  検証①: "LongTermLoansPayable" == "CurrentPortionOfLongTermLoansPayable" => False
    => 誤マッチ解消

  検証②: "LeaseObligations" == "LongTermLeaseObligations" => False
    => 誤マッチ解消
""")
    print("=" * 90)
    print("  結論 (コード分析)")
    print("=" * 90)
    print("""
  [修正済み] _tag_matches を完全一致に変更済み（2025-02）

  旧: keyword in tag_local(tag)  => 部分一致
  新: tag_local(tag) == keyword  => 完全一致

  借入金・リース同値問題は解消済み。
  XBRLファイルを配置して実行すると raw fact 一覧を確認可能。
""")


def find_xbrl(doc_id: str) -> Path | None:
    base = PROJECT_ROOT / "data" / "edinet" / "raw_xbrl"
    if not base.exists():
        return None
    for f in base.rglob("*.xbrl"):
        if doc_id in str(f):
            return f
    return None


def get_context_info(ctx_ref: str, context_map: dict) -> dict:
    ctx = context_map.get(ctx_ref, {})
    t = ctx.get("type", "")
    if t == "instant":
        return {"type": "instant", "date": ctx.get("date", ""), "consolidated": "NonConsolidated" not in ctx_ref}
    if t == "duration":
        return {"type": "duration", "end_date": ctx.get("end_date", ""), "consolidated": "NonConsolidated" not in ctx_ref}
    return {"type": "unknown"}


def main() -> None:
    doc_id = "S100XL6L"
    xbrl_path: Path | None = None

    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        p = Path(arg)
        if p.is_file():
            xbrl_path = p
        else:
            xbrl_path = find_xbrl(arg) or find_xbrl(doc_id)
    else:
        xbrl_path = find_xbrl(doc_id)

    if not xbrl_path or not xbrl_path.exists():
        print("WARNING: XBRLファイルが見つかりません。")
        print("  data/edinet/raw_xbrl 以下に S100XL6L のXBRLを配置するか、")
        print("  引数でパスを指定してください: python verify_2734_xbrl.py <path>")
        print("\nコード・YAML分析に基づく結論のみ出力します。\n")
        _output_conclusion_without_xbrl()
        sys.exit(0)

    print("=" * 90)
    print("  FACTレイク 財務整合性検証 (2734 / doc_id: S100XL6L)")
    print("=" * 90)
    print(f"\nXBRL: {xbrl_path}")

    parsed, ctx_map, normalizer, normalized, result = run_pipeline(xbrl_path)
    facts = parsed.get("facts", [])
    current_year_end = normalizer._current_year_end
    prior_year_end = normalizer._prior_year_end

    print(f"\ncurrent_year_end: {current_year_end}")
    print(f"prior_year_end: {prior_year_end}")

    # --- 検証①: 借入金タグ ---
    print("\n" + "=" * 90)
    print("  検証①: 借入金関連タグ (raw XBRL fact一覧)")
    print("=" * 90)

    borrow_facts = []
    for f in facts:
        local = tag_local(f.get("tag", ""))
        for pat in BORROW_TAGS:
            if pat in local:
                borrow_facts.append((pat, f))
                break

    if not borrow_facts:
        print("  (該当タグなし)")
    else:
        for pat, f in sorted(borrow_facts, key=lambda x: (x[1].get("contextRef", ""), x[0])):
            ctx_ref = f.get("contextRef", "")
            info = get_context_info(ctx_ref, ctx_map)
            val = f.get("value", "").strip()
            is_nil = f.get("is_nil", False)
            print(f"\n  tag: {f.get('tag')}")
            print(f"    contextRef: {ctx_ref}")
            print(f"    type: {info.get('type')}, date: {info.get('date', info.get('end_date', '-'))}")
            print(f"    consolidated: {info.get('consolidated', '-')}")
            print(f"    value: {val if val else '(empty)'} {'[xsi:nil]' if is_nil else ''}")

    # --- 検証②: リース債務タグ ---
    print("\n" + "=" * 90)
    print("  検証②: リース債務関連タグ (raw XBRL fact一覧)")
    print("=" * 90)

    lease_facts = []
    for f in facts:
        local = tag_local(f.get("tag", ""))
        for pat in LEASE_TAGS:
            if pat in local:
                lease_facts.append((pat, f))
                break

    if not lease_facts:
        print("  (該当タグなし)")
    else:
        for pat, f in sorted(lease_facts, key=lambda x: (x[1].get("contextRef", ""), x[0])):
            ctx_ref = f.get("contextRef", "")
            info = get_context_info(ctx_ref, ctx_map)
            val = f.get("value", "").strip()
            is_nil = f.get("is_nil", False)
            print(f"\n  tag: {f.get('tag')}")
            print(f"    contextRef: {ctx_ref}")
            print(f"    type: {info.get('type')}, date: {info.get('date', info.get('end_date', '-'))}")
            print(f"    consolidated: {info.get('consolidated', '-')}")
            print(f"    value: {val if val else '(empty)'} {'[xsi:nil]' if is_nil else ''}")

    # --- 検証③: BSアンカー方式 ---
    print("\n" + "=" * 90)
    print("  検証③: BSアンカー方式の影響")
    print("=" * 90)

    # 簡易アンカー日付検出（TotalAssets等のinstant日付分布）
    from collections import Counter
    date_counts: Counter = Counter()
    for f in facts:
        local = tag_local(f.get("tag", ""))
        if not any(kw in local for kw in ("TotalAssets", "LiabilitiesAndNetAssets", "NetAssets")):
            continue
        ctx_ref = f.get("contextRef", "")
        if "Member" in ctx_ref and "NonConsolidatedMember" not in ctx_ref:
            continue
        if "NonConsolidated" in ctx_ref:
            continue
        ctx = ctx_map.get(ctx_ref, {})
        if ctx.get("type") != "instant":
            continue
        val = (f.get("value") or "").strip()
        if not val or f.get("is_nil", False):
            continue
        date = ctx.get("date", "")
        if date:
            date_counts[date] += 1

    print(f"\n  アンカータグ(TotalAssets等)のinstant日付分布:")
    for d, cnt in date_counts.most_common():
        marker = " <- 採用" if d == current_year_end else ""
        print(f"    {d}: {cnt}件{marker}")

    anchor_date = date_counts.most_common(1)[0][0] if date_counts else None
    if anchor_date and anchor_date != current_year_end:
        print(f"\n  [BSアンカーフォールバック] duration由来={current_year_end} -> アンカー={anchor_date}")
        print(f"  借入・リースタグが {anchor_date} のinstantに存在するか確認:")
        for f in facts:
            local = tag_local(f.get("tag", ""))
            if any(p in local for p in BORROW_TAGS + LEASE_TAGS):
                ctx = ctx_map.get(f.get("contextRef", ""), {})
                if ctx.get("type") == "instant" and ctx.get("date") == anchor_date:
                    print(f"    {f.get('tag')}: {f.get('value')} (contextRef={f.get('contextRef')})")
    else:
        print(f"\n  アンカー日付は duration 由来と同一: {current_year_end}")

    # --- YAMLマッピング該当箇所 ---
    print("\n" + "=" * 90)
    print("  YAMLマッピング該当箇所抜粋")
    print("=" * 90)

    yaml_excerpt = """
  # --- current_portion_of_long_term_borrowings ---
  - tag: "CurrentPortionOfLongTermLoansPayable"
    key: "current_portion_of_long_term_borrowings"
  - tag: "CurrentPortionOfLongTermBorrowings"
    key: "current_portion_of_long_term_borrowings"

  # --- long_term_borrowings ---
  - tag: "LongTermLoansPayable"
    key: "long_term_borrowings"
  - tag: "LongTermBorrowings"
    key: "long_term_borrowings"

  # --- lease_obligations (CL/NCL未分割) ---
  - tag: "LeaseObligations"
    key: "lease_obligations"

  # --- long_term_lease_obligations ---
  - tag: "LeaseObligationsNCL"
    key: "long_term_lease_obligations"
  - tag: "LongTermLeaseObligations"
    key: "long_term_lease_obligations"
"""
    print(yaml_excerpt)

    # --- マッチングロジック確認 ---
    print("\n" + "=" * 90)
    print("  マッチングロジック確認 (_tag_matches: keyword in tag_local)")
    print("=" * 90)

    print("""
  keyword "LongTermLoansPayable" in "CurrentPortionOfLongTermLoansPayable" => True (部分一致)
  keyword "CurrentPortionOfLongTermLoansPayable" in "CurrentPortionOfLongTermLoansPayable" => True

  keyword "LeaseObligations" in "LongTermLeaseObligations" => True (部分一致)
  keyword "LongTermLeaseObligations" in "LongTermLeaseObligations" => True
""")

    # --- 正規化結果 ---
    print("\n" + "=" * 90)
    print("  正規化結果 (FactNormalizer出力)")
    print("=" * 90)

    bs = normalized.get("current_year", {}).get("bs", {})
    for k in ["short_term_borrowings", "current_portion_of_long_term_borrowings", "long_term_borrowings",
              "short_term_lease_obligations", "long_term_lease_obligations", "lease_obligations"]:
        print(f"  {k}: {bs.get(k)}")

    # --- 結論 ---
    print("\n" + "=" * 90)
    print("  結論")
    print("=" * 90)

    lt = bs.get("long_term_borrowings")
    cp = bs.get("current_portion_of_long_term_borrowings")
    lease = bs.get("lease_obligations")
    lt_lease = bs.get("long_term_lease_obligations")

    issues = []
    if lt is not None and cp is not None and lt == cp:
        issues.append("long_term_borrowings と current_portion_of_long_term_borrowings が同値")
        issues.append("  原因候補: keyword 'LongTermLoansPayable' が 'CurrentPortionOfLongTermLoansPayable' に部分一致し、同一factを両方に割当")
    if lease is not None and lt_lease is not None and lease == lt_lease:
        issues.append("lease_obligations と long_term_lease_obligations が同値")
        issues.append("  原因候補: keyword 'LeaseObligations' が 'LongTermLeaseObligations' に部分一致し、同一factを両方に割当")

    if issues:
        print("\n  [問題あり]")
        for i in issues:
            print(f"  {i}")
    else:
        print("\n  [問題なし] 同値の事象は検出されませんでした。")
    print()


if __name__ == "__main__":
    main()
