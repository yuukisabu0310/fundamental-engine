"""
JSONExporter
FinancialMaster の出力を financial-dataset へ永続化する。

financial-dataset は「確定決算の財務Factのみ」を保存するデータレイク。
Derived指標・null値・空データは一切含めない。

Schema changes must increment schema_version.
data_version represents fiscal period identity, not generation timestamp.

外部データリポジトリ（financial-dataset）に出力する。
DATASET_PATH 環境変数で出力先を指定する。
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

logger = logging.getLogger(__name__)

DERIVED_KEYS = frozenset({
    "roe", "roa", "operating_margin", "net_margin",
    "equity_ratio", "de_ratio",
    "sales_growth", "profit_growth", "eps_growth",
    "per", "pbr", "psr", "peg", "dividend_yield",
    "free_cash_flow",
})

FACT_KEYS = frozenset({
    "equity", "total_assets", "interest_bearing_debt",
    "net_sales", "operating_income", "profit_loss",
    "earnings_per_share",
})

MARKET_SECTION_KEYS = frozenset({"market", "valuation"})


def _validate_metrics(metrics: dict[str, Any], label: str, security_code: str) -> None:
    """
    出力前バリデーション。問題があればログに警告を出力する。

    1. Derived指標が混入していないか
    2. null値が存在しないか
    3. metricsが空でないか
    4. operating_income と profit_loss が同値でないか（警告）
    """
    leaked = set(metrics.keys()) & DERIVED_KEYS
    if leaked:
        logger.error(
            "VALIDATION FAIL [%s] %s: Derived指標が混入 %s", security_code, label, leaked,
        )
        raise ValueError(f"Derived指標が metrics に混入しています: {leaked}")

    null_keys = [k for k, v in metrics.items() if v is None]
    if null_keys:
        logger.error(
            "VALIDATION FAIL [%s] %s: null値が存在 %s", security_code, label, null_keys,
        )
        raise ValueError(f"null値が metrics に存在します: {null_keys}")

    if not metrics:
        logger.warning("VALIDATION WARN [%s] %s: metricsが空", security_code, label)

    op_income = metrics.get("operating_income")
    profit = metrics.get("profit_loss")
    if op_income is not None and profit is not None and op_income == profit:
        logger.warning(
            "VALIDATION WARN [%s] %s: operating_income と profit_loss が同値 (%s)",
            security_code, label, op_income,
        )


class JSONExporter:
    """
    FinancialMaster の出力を JSON ファイルとして保存する。
    出力先: {DATASET_PATH}/{report_type}/{data_version}/{security_code}.json

    financial-dataset には財務Factのみを保存する。
    Derived指標・null値は出力しない。空のprior_yearは省略する。
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

    def _sanitize_metrics(self, year_data: dict[str, Any]) -> dict[str, float] | None:
        """
        year_data から metrics を抽出し、Factのみを残す。
        - Derived指標を除去
        - null値を除去
        - market/valuation セクションを除去
        有効なFactがなければ None を返す。
        """
        metrics = year_data.get("metrics")
        if not metrics or not isinstance(metrics, dict):
            return None

        clean: dict[str, float] = {}
        for key, value in metrics.items():
            if key in DERIVED_KEYS:
                continue
            if value is None:
                continue
            clean[key] = value

        return clean if clean else None

    def export(self, financial_dict: dict[str, Any]) -> str:
        """
        財務Factのみを JSON として書き出し、保存パスを返す。

        Args:
            financial_dict: FinancialMaster.compute() の戻り値。

        Returns:
            保存された JSON ファイルのパス（文字列）。

        Raises:
            ValueError: security_code, report_type, data_version が存在しない、
                        またはバリデーション違反の場合。
        """
        security_code = financial_dict.get("security_code")
        if not security_code or not str(security_code).strip():
            raise ValueError(
                "security_code が取得できません。"
                "有価証券報告書・四半期報告書以外の書類の可能性があります。"
            )

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

        sc = str(security_code)

        current_metrics = self._sanitize_metrics(financial_dict.get("current_year", {}))
        prior_metrics = self._sanitize_metrics(financial_dict.get("prior_year", {}))

        if current_metrics:
            _validate_metrics(current_metrics, "current_year", sc)
        if prior_metrics:
            _validate_metrics(prior_metrics, "prior_year", sc)

        if not current_metrics:
            raise ValueError(
                f"current_year に有効なFactが存在しません (security_code={sc})"
            )

        logger.info(
            "Exporting: security_code=%s, data_version=%s, current=%d facts, prior=%s facts",
            sc, data_version, len(current_metrics),
            len(prior_metrics) if prior_metrics else 0,
        )

        output_dir = self.base_dir / report_type / data_version
        output_dir.mkdir(parents=True, exist_ok=True)

        output_dict: dict[str, Any] = {
            "schema_version": "2.0",
            "engine_version": __version__,
            "data_version": data_version,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "doc_id": financial_dict.get("doc_id", ""),
            "security_code": sc,
            "fiscal_year_end": fiscal_year_end,
            "report_type": report_type,
            "current_year": {"metrics": current_metrics},
        }

        if prior_metrics:
            output_dict["prior_year"] = {"metrics": prior_metrics}

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
