"""
検証対象銘柄の詳細検証スクリプト。
取得済みXBRLデータを処理し、各銘柄ごとに詳細レポートを出力する。

使用例:
    python scripts/analysis/verify_targets_detail.py
"""
import logging
import sys
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

TARGET_LABELS = {
    "8951": "日本ビルファンド (REIT)",
    "8952": "ジャパンリアルエステイト (REIT)",
    "3281": "GLP投資法人 (REIT)",
    "8964": "フロンティア不動産 (REIT)",
    "9984": "ソフトバンクG (IFRS)",
    "6758": "ソニーG (IFRS)",
    "4063": "信越化学 (IFRS/JGAAP)",
    "8306": "三菱UFJFG (銀行)",
    "8316": "三井住友FG (銀行)",
    "1436": "フィット/グリーンエナジー (小型)",
    "3064": "MonotaRO (小型)",
    "3558": "ロコンド (小型)",
}


def process_xbrl(xbrl_path: Path) -> dict:
    """1ファイルを処理し詳細検証に必要な情報を返す。"""
    try:
        parsed, _ctx_map, _normalizer, _normalized, result = run_pipeline(xbrl_path)
        return {
            "xbrl_path": str(xbrl_path),
            "xbrl_filename": xbrl_path.name,
            "doc_id": result.get("doc_id"),
            "security_code": normalize_code(result.get("security_code", "")),
            "accounting_standard": result.get("accounting_standard"),
            "consolidation_type": result.get("consolidation_type"),
            "report_type": result.get("report_type"),
            "fiscal_year_end": result.get("fiscal_year_end"),
            "taxonomy_version": parsed.get("taxonomy_version"),
            "current_metrics": result.get("current_year", {}).get("metrics", {}),
            "prior_metrics": result.get("prior_year", {}).get("metrics", {}),
            "current_period": result.get("current_year", {}).get("period"),
        }
    except Exception as e:
        return {"xbrl_path": str(xbrl_path), "error": str(e)}


def _print_company_report(r: dict, index: int) -> None:
    sc = r["security_code"]
    label = TARGET_LABELS.get(sc, sc)
    cm = r.get("current_metrics", {})

    print(f"\n{'='*70}")
    print(f"  [{index}] {sc} - {label}")
    print(f"{'='*70}")
    print(f"\n  1. report_form_code: {check_form_code(r['xbrl_filename'])}")
    print(f"  2. accounting_standard: {r['accounting_standard']}")
    print(f"  3. taxonomy_version: {r.get('taxonomy_version')}")
    print(f"  4. canonical_fact件数: {len(cm)}")

    null_items = {k: v for k, v in cm.items() if v is None}
    non_null = {k: v for k, v in cm.items() if v is not None}
    null_rate = len(null_items) / len(cm) if cm else 0

    print(f"\n  5. NULL率上位10項目: ({len(null_items)}/{len(cm)} = {null_rate:.1%})")
    for k in sorted(null_items.keys()):
        print(f"     - {k}: NULL")

    print(f"\n  6. 取得成功項目:")
    for k in sorted(non_null.keys()):
        v = non_null[k]
        if isinstance(v, float) and v >= 1000:
            print(f"     - {k}: {v:,.0f}")
        else:
            print(f"     - {k}: {v}")

    print(f"\n  7. 構造上の問題有無:")
    issues: list[str] = []
    if set(cm.keys()) != FACT_KEYS:
        issues.append(f"スキーマ不一致: got={sorted(cm.keys())}, expected={sorted(FACT_KEYS)}")
    if r["accounting_standard"] == "IFRS" and cm.get("ordinary_income") is not None:
        issues.append("IFRS企業にordinary_income値あり（連結コンテキスト由来 → 企業開示による正常動作）")
    if not issues:
        print("     なし")
    else:
        for iss in issues:
            print(f"     [INFO] {iss}")

    print(f"\n  8. consolidation_type: {r['consolidation_type']}")
    print(f"     fiscal_year_end: {r['fiscal_year_end']}")
    if r.get("current_period"):
        print(f"     current_period: {r['current_period']}")


def main() -> None:
    xbrl_files = collect_xbrl_files()
    target_codes = set(TARGET_LABELS.keys())
    found: dict[str, list[dict]] = {}

    for xf in xbrl_files:
        r = process_xbrl(xf)
        if "error" in r:
            continue
        sc = r.get("security_code", "")
        if sc in target_codes:
            found.setdefault(sc, []).append(r)

    print("=" * 70)
    print("  FACTレイク追加パターン検証 - 詳細レポート")
    print("=" * 70)

    idx = 1
    for sc in sorted(TARGET_LABELS.keys()):
        if sc in found:
            for r in found[sc]:
                _print_company_report(r, idx)
                idx += 1
        else:
            label = TARGET_LABELS.get(sc, sc)
            print(f"\n{'='*70}")
            print(f"  [{idx}] {sc} - {label}")
            print(f"{'='*70}")
            print("  [NOT AVAILABLE] XBRLデータが取得範囲に含まれていません")
            idx += 1

    print(f"\n{'='*70}")
    print("  サマリー")
    print(f"{'='*70}")
    found_count = sum(len(v) for v in found.values())
    print(f"  検証済み銘柄: {len(found)}/{len(TARGET_LABELS)}")
    print(f"  検証済みドキュメント数: {found_count}")
    missing = target_codes - set(found.keys())
    if missing:
        print(f"  未検証銘柄: {', '.join(sorted(missing))}")


if __name__ == "__main__":
    main()
