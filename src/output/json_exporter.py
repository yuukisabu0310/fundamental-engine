"""
JSONExporter
FinancialMaster の出力を financial-dataset へ永続化する。

financial-dataset は「確定決算の財務Factのみ」を保存するデータレイク。
Derived指標は含めない。値が取得できなかった項目はnullとして出力する。

schema_version 1.0: 正式版未確定のため 1.0 のまま。破壊的変更時は将来インクリメント予定。
data_version represents fiscal period identity, not generation timestamp.

外部データリポジトリ（financial-dataset）に出力する。
DATASET_PATH 環境変数で出力先を指定する。

FACT_KEYS / DERIVED_KEYS / 会計基準マッピングは config/canonical_keys.yaml から読み込む。
"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src import __version__
try:
    from src.config_loader import (
        get_accounting_standard_mapping,
        get_derived_keys,
        get_fact_keys,
        get_valid_accounting_standards,
    )
except ModuleNotFoundError:
    from config_loader import (
        get_accounting_standard_mapping,
        get_derived_keys,
        get_fact_keys,
        get_valid_accounting_standards,
    )

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"

DERIVED_KEYS = get_derived_keys()
FACT_KEYS = get_fact_keys()
VALID_ACCOUNTING_STANDARDS = get_valid_accounting_standards()

_ACCOUNTING_STANDARD_MAP = get_accounting_standard_mapping()


def normalize_security_code(raw: str) -> str:
    """EDINET由来の銘柄コードを正規化する。5桁かつ末尾が'0'の場合のみ末尾1桁を削除。"""
    s = str(raw).strip()
    if len(s) == 5 and s.endswith("0"):
        return s[:4]
    return s


def _normalize_accounting_standard(raw: str | None) -> str | None:
    """会計基準を正規化。EDINET DEIの表記ゆれを吸収する。"""
    if not raw:
        return None
    s = raw.strip()
    return _ACCOUNTING_STANDARD_MAP.get(s, s)


def _validate_metrics(metrics: dict[str, Any], label: str, security_code: str) -> None:
    """
    出力前バリデーション。

    1. Derived指標が混入していないか
    2. 全項目がnullでないか（警告）
    """
    leaked = set(metrics.keys()) & DERIVED_KEYS
    if leaked:
        logger.error(
            "VALIDATION FAIL [%s] %s: Derived指標が混入 %s", security_code, label, leaked,
        )
        raise ValueError(f"Derived指標が metrics に混入しています: {leaked}")

    non_null_count = sum(1 for v in metrics.values() if v is not None)
    if non_null_count == 0:
        logger.warning("VALIDATION WARN [%s] %s: 全項目がnull", security_code, label)


class JSONExporter:
    """
    FinancialMaster の出力を JSON ファイルとして保存する。
    出力先: {DATASET_PATH}/{report_type}/{data_version}/{security_code}.json

    financial-dataset には財務Factのみを保存する。
    Derived指標は出力しない。値が取得できなかった項目はnullで出力する。
    全項目がnullの年度ブロックは省略する。
    """

    def __init__(self, base_dir: str | None = None) -> None:
        if base_dir is None:
            base_dir_str = os.environ.get("DATASET_PATH")
            if not base_dir_str:
                raise EnvironmentError(
                    "DATASET_PATH 環境変数が設定されていません。"
                    ".env ファイルまたは環境変数で DATASET_PATH を設定してください。"
                )
            base_dir = base_dir_str

        self.base_dir = Path(base_dir)

    def _generate_data_version(
        self, fiscal_year_end: str | None, report_type: str | None,
    ) -> str:
        """決算期から data_version を生成。"""
        if not fiscal_year_end:
            logger.warning("fiscal_year_end is None, using UNKNOWN")
            return "UNKNOWN"

        try:
            dt = datetime.strptime(fiscal_year_end, "%Y-%m-%d")
            year = dt.year
            month = dt.month

            if report_type == "annual":
                return f"{year}FY"
            elif report_type == "quarterly":
                quarter_map = {3: 1, 6: 2, 9: 3, 12: 4}
                quarter = quarter_map.get(month)
                if quarter is None:
                    logger.warning("Unexpected month for quarterly report: %d, using Q4", month)
                    quarter = 4
                return f"{year}Q{quarter}"
            else:
                logger.warning("report_type is %s, treating as annual", report_type or "None")
                return f"{year}FY"
        except ValueError as e:
            logger.warning("Failed to parse fiscal_year_end: %s, using UNKNOWN", e)
            return "UNKNOWN"

    def _sanitize_metrics(self, year_data: dict[str, Any]) -> dict[str, float | int | None] | None:
        """
        year_data から metrics を抽出し、Factのみを残す。
        Derived指標は除去するが、null値はそのまま保持する。
        全項目がnullでも metrics 自体は返す（年度ブロックの存在判定は呼び出し側で行う）。
        """
        metrics = year_data.get("metrics")
        if not metrics or not isinstance(metrics, dict):
            return None

        clean: dict[str, float | int | None] = {}
        for key, value in metrics.items():
            if key in DERIVED_KEYS:
                continue
            clean[key] = value

        return clean if clean else None

    def export(self, financial_dict: dict[str, Any]) -> str:
        """
        財務Factのみを JSON として書き出し、保存パスを返す。
        """
        raw_code = financial_dict.get("security_code")
        if not raw_code or not str(raw_code).strip():
            raise ValueError(
                "security_code が取得できません。"
                "有価証券報告書・四半期報告書以外の書類の可能性があります。"
            )

        sc = normalize_security_code(str(raw_code))

        fiscal_year_end = financial_dict.get("fiscal_year_end")
        report_type = financial_dict.get("report_type")
        data_version = self._generate_data_version(fiscal_year_end, report_type)

        if report_type not in ("annual", "quarterly"):
            raise ValueError(
                f"Invalid report_type: {report_type}. "
                "report_type must be 'annual' or 'quarterly'."
            )

        if not data_version or data_version == "UNKNOWN":
            raise ValueError(
                "data_version が生成できませんでした（fiscal_year_end が欠損している可能性があります）。"
                "有価証券報告書・四半期報告書以外の書類は処理対象外です。"
            )

        current_data = financial_dict.get("current_year", {})
        prior_data = financial_dict.get("prior_year", {})
        current_metrics = self._sanitize_metrics(current_data)
        prior_metrics = self._sanitize_metrics(prior_data)

        if current_metrics:
            _validate_metrics(current_metrics, "current_year", sc)
        if prior_metrics:
            _validate_metrics(prior_metrics, "prior_year", sc)

        if not current_metrics:
            raise ValueError(
                f"current_year に有効なFactが存在しません (security_code={sc})"
            )

        consolidation_type = financial_dict.get("consolidation_type", "consolidated")
        accounting_standard = _normalize_accounting_standard(
            financial_dict.get("accounting_standard"),
        )

        logger.info(
            "Exporting: security_code=%s, data_version=%s, standard=%s, current=%d facts, prior=%s facts",
            sc, data_version, accounting_standard,
            len(current_metrics),
            len(prior_metrics) if prior_metrics else 0,
        )

        output_dir = self.base_dir / report_type / data_version
        output_dir.mkdir(parents=True, exist_ok=True)

        current_block: dict[str, Any] = {"metrics": current_metrics}
        current_period = current_data.get("period")
        if current_period:
            current_block["period"] = current_period

        output_dict: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": __version__,
            "data_version": data_version,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "doc_id": financial_dict.get("doc_id", ""),
            "security_code": sc,
            "report_type": report_type,
            "consolidation_type": consolidation_type,
            "accounting_standard": accounting_standard,
            "currency": "JPY",
            "unit": "JPY",
            "current_year": current_block,
        }

        if prior_metrics:
            prior_block: dict[str, Any] = {"metrics": prior_metrics}
            prior_period = prior_data.get("period")
            if prior_period:
                prior_block["period"] = prior_period
            output_dict["prior_year"] = prior_block

        output_path = output_dir / f"{sc}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_dict, f, indent=2, ensure_ascii=False)

        logger.info("JSONExporter: 保存完了 - %s (data_version=%s)", output_path, data_version)

        try:
            from src.output.manifest_generator import DatasetManifestGenerator
            manifest_generator = DatasetManifestGenerator()
            manifest_path = manifest_generator.save()
            logger.info("Dataset manifest generated: %s", manifest_path)
        except Exception as e:
            logger.warning("Failed to generate dataset manifest: %s", e)

        return str(output_path)
