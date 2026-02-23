"""
FACTレイク検証スクリプト。
全XBRLデータを処理し、設計整合性・会計基準対応・業種差異吸収を検証する。

使用例:
    python scripts/analysis/verify_fact_lake.py
"""
import logging
import re
import sys
from collections import Counter
from pathlib import Path

from _pipeline import (
    PROJECT_ROOT,
    FACT_KEYS,
    DERIVED_KEYS,
    collect_xbrl_files,
    normalize_code,
    check_form_code,
    run_pipeline,
)

logging.basicConfig(level=logging.WARNING)

TARGET_CODES = {
    "8951", "8952", "3281", "8964",
    "9984", "6758", "4063",
    "8306", "8316",
    "1436", "3064", "3558",
}

TARGET_LABELS = {
    "8951": "日本ビルファンド (REIT)",
    "8952": "ジャパンリアルエステイト (REIT)",
    "3281": "GLP投資法人 (REIT)",
    "8964": "フロンティア不動産 (REIT)",
    "9984": "ソフトバンクG (IFRS)",
    "6758": "ソニーG (IFRS)",
    "4063": "信越化学 (IFRS)",
    "8306": "三菱UFJFG (銀行)",
    "8316": "三井住友FG (銀行)",
    "1436": "フィット (小型)",
    "3064": "MonotaRO (小型)",
    "3558": "ロコンド (小型)",
}


def process_xbrl(xbrl_path: Path) -> dict:
    """1ファイルを処理し検証に必要な情報を返す。"""
    try:
        parsed, _ctx_map, _normalizer, normalized, result = run_pipeline(xbrl_path)
        return {
            "xbrl_path": str(xbrl_path),
            "xbrl_filename": xbrl_path.name,
            "doc_id": result.get("doc_id"),
            "security_code": normalize_code(result.get("security_code", "")),
            "security_code_raw": result.get("security_code"),
            "accounting_standard": result.get("accounting_standard"),
            "consolidation_type": result.get("consolidation_type"),
            "report_type": result.get("report_type"),
            "fiscal_year_end": result.get("fiscal_year_end"),
            "taxonomy_version": parsed.get("taxonomy_version"),
            "current_metrics": result.get("current_year", {}).get("metrics", {}),
            "prior_metrics": result.get("prior_year", {}).get("metrics", {}),
            "current_period": result.get("current_year", {}).get("period"),
            "normalized_raw": normalized,
        }
    except Exception as e:
        return {"xbrl_path": str(xbrl_path), "error": str(e)}


def analyze_null_rate(metrics: dict) -> dict:
    """NULL 率を計算する。"""
    if not metrics:
        return {}
    total = len(metrics)
    null_items = {k: v for k, v in metrics.items() if v is None}
    return {
        "total_keys": total,
        "null_count": len(null_items),
        "null_rate": len(null_items) / total if total > 0 else 0,
        "null_keys": sorted(null_items.keys()),
    }


def main() -> tuple[list[dict], list[dict]]:
    xbrl_files = collect_xbrl_files()

    print("=" * 80)
    print("  FACTレイク検証レポート")
    print("=" * 80)
    print(f"\n対象XBRLファイル数: {len(xbrl_files)}")

    results: list[dict] = []
    errors: list[dict] = []
    for xf in xbrl_files:
        r = process_xbrl(xf)
        if "error" in r:
            errors.append(r)
        else:
            results.append(r)

    print(f"処理成功: {len(results)}")
    print(f"処理失敗: {len(errors)}")

    # === 1. 全体統計 ===
    print(f"\n{'=' * 80}")
    print(f"  1. 全体統計")
    print(f"{'=' * 80}")

    form_codes: Counter = Counter()
    acct_standards: Counter = Counter()
    consol_types: Counter = Counter()
    found_targets: dict[str, dict] = {}

    for r in results:
        form_codes[check_form_code(r["xbrl_filename"])] += 1
        acct_standards[r.get("accounting_standard") or "N/A"] += 1
        consol_types[r.get("consolidation_type") or "N/A"] += 1
        sc = r.get("security_code", "")
        if sc in TARGET_CODES:
            found_targets[sc] = r

    print("\n--- 様式コード分布 ---")
    for fc, cnt in form_codes.most_common():
        print(f"  {fc}: {cnt} 件")
    print("\n--- 会計基準分布 ---")
    for std, cnt in acct_standards.most_common():
        print(f"  {std}: {cnt} 件")
    print("\n--- 連結区分分布 ---")
    for ct, cnt in consol_types.most_common():
        print(f"  {ct}: {cnt} 件")
    print("\n--- 対象銘柄の存在確認 ---")
    for code in sorted(TARGET_CODES):
        status = "FOUND" if code in found_targets else "NOT FOUND"
        print(f"  {code} {TARGET_LABELS.get(code, '')}: [{status}]")

    # === 2. IFRS企業検証 ===
    ifrs_results = [
        r for r in results
        if r.get("accounting_standard") in ("IFRS", "International Financial Reporting Standards")
    ]
    print(f"\n{'=' * 80}")
    print(f"  2. IFRS企業検証 ({len(ifrs_results)} 件)")
    print(f"{'=' * 80}")
    for r in ifrs_results:
        cm = r.get("current_metrics", {})
        null_info = analyze_null_rate(cm)
        print(f"\n--- {r['security_code']} (doc_id: {r['doc_id']}) ---")
        print(f"  accounting_standard: {r['accounting_standard']}")
        print(f"  taxonomy_version: {r.get('taxonomy_version')}")
        print(f"  form_code: {check_form_code(r['xbrl_filename'])}")
        print(f"  consolidation_type: {r['consolidation_type']}")
        print(f"  canonical_fact件数: {null_info.get('total_keys', 0)}")
        print(f"  NULL率: {null_info.get('null_rate', 0):.1%}")
        print(f"  ordinary_income: {cm.get('ordinary_income')} (IFRS -> null expected)")

    # === 3. JGAAP企業サンプル検証 ===
    jgaap_results = [
        r for r in results
        if r.get("accounting_standard") not in ("IFRS", "International Financial Reporting Standards", None)
    ]
    print(f"\n{'=' * 80}")
    print(f"  3. JGAAP企業検証 ({len(jgaap_results)} 件)")
    print(f"{'=' * 80}")
    for r in jgaap_results[:5]:
        cm = r.get("current_metrics", {})
        null_info = analyze_null_rate(cm)
        print(f"\n--- {r['security_code']} (doc_id: {r['doc_id']}) ---")
        print(f"  accounting_standard: {r['accounting_standard']}")
        print(f"  NULL率: {null_info.get('null_rate', 0):.1%}")

    # === 4. NULL率全体分析 ===
    print(f"\n{'=' * 80}")
    print(f"  4. NULL率全体分析")
    print(f"{'=' * 80}")
    null_counter: Counter = Counter()
    total_processed = 0
    for r in results:
        cm = r.get("current_metrics", {})
        if cm:
            total_processed += 1
            for k, v in cm.items():
                if v is None:
                    null_counter[k] += 1
    print(f"\n全{total_processed}件中のNULL率上位:")
    for key, cnt in null_counter.most_common(15):
        rate = cnt / total_processed if total_processed > 0 else 0
        print(f"  {key}: {cnt}/{total_processed} ({rate:.1%})")

    # === 5. 設計整合性チェック ===
    print(f"\n{'=' * 80}")
    print(f"  5. 設計整合性チェック")
    print(f"{'=' * 80}")
    checks: list[tuple[str, bool, str]] = []

    all_metric_keys: set[str] = set()
    for r in results:
        all_metric_keys.update(r.get("current_metrics", {}).keys())
        all_metric_keys.update(r.get("prior_metrics", {}).keys())

    leaked_derived = all_metric_keys & DERIVED_KEYS
    checks.append(("Derived指標が混入していない", len(leaked_derived) == 0,
                    f"混入: {leaked_derived}" if leaked_derived else ""))
    unknown_keys = all_metric_keys - FACT_KEYS
    checks.append(("未定義キーが混入していない", len(unknown_keys) == 0,
                    f"未定義: {unknown_keys}" if unknown_keys else ""))

    ifrs_ordinary_companies = [
        r.get("security_code", "?")
        for r in ifrs_results
        if r.get("current_metrics", {}).get("ordinary_income") is not None
    ]
    checks.append((
        "IFRS企業のordinary_income非混入",
        len(ifrs_ordinary_companies) <= len(ifrs_results) * 0.1,
        f"{len(ifrs_ordinary_companies)}/{len(ifrs_results)}件に値あり" if ifrs_ordinary_companies else "",
    ))

    key_sets = {frozenset(r.get("current_metrics", {}).keys()) for r in results}
    checks.append(("全企業が同一スキーマ", len(key_sets) <= 1,
                    f"異なるスキーマ数: {len(key_sets)}" if len(key_sets) > 1 else ""))

    for name, ok, detail in checks:
        print(f"  {'[OK]' if ok else '[NG]'} {name}")
        if detail:
            print(f"        {detail}")

    # === 6. コードベース設計検証 ===
    print(f"\n{'=' * 80}")
    print(f"  6. コードベース設計検証（様式/業種/会計基準依存の有無）")
    print(f"{'=' * 80}")
    src_dir = PROJECT_ROOT / "src"
    code_checks: list[tuple[str, str]] = []
    for py_file in src_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        fname = py_file.relative_to(PROJECT_ROOT)
        for pattern, desc in [
            ("if.*form_code", "form_code分岐"),
            ("if.*industry", "業種分岐"),
            ("if.*bank", "銀行特別処理"),
            ("if.*reit", "REIT特別処理"),
            ("if.*accounting_standard.*==", "会計基準条件分岐"),
            ("jpsps", "投資法人様式参照"),
            ("jpigp", "IFRS様式ハードコード"),
        ]:
            for m in re.findall(f".*{pattern}.*", content, re.IGNORECASE):
                stripped = m.strip()
                if not stripped.startswith("#") and not stripped.startswith('"""'):
                    code_checks.append((f"{fname}: {desc}", stripped[:120]))
    if not code_checks:
        for label in ["様式依存分岐", "業種依存分岐", "会計基準条件分岐", "REIT特別処理", "銀行特別処理"]:
            print(f"  [OK] {label}なし")
    else:
        for desc, line in code_checks:
            print(f"  [WARN] {desc}\n         {line}")

    # === エラー一覧 ===
    if errors:
        print(f"\n{'=' * 80}")
        print(f"  7. 処理エラー ({len(errors)} 件)")
        print(f"{'=' * 80}")
        for e in errors[:10]:
            print(f"  {Path(e['xbrl_path']).name}: {e['error'][:100]}")

    # === 最終判定 ===
    print(f"\n{'=' * 80}")
    print(f"  最終判定")
    print(f"{'=' * 80}")
    all_ok = all(ok for _, ok, _ in checks) and len(code_checks) == 0
    print(f"  {'[PASS] 設計整合性に問題なし' if all_ok else '[WARN] 一部検証項目に注意事項あり'}")
    print()
    return results, errors


if __name__ == "__main__":
    main()
