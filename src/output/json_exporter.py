"""
JSONExporter
FinancialMaster の出力を financial-dataset へ永続化する。

financial-dataset は「確定決算の財務Factのみ」を保存するデータレイク。
Derived指標・null値・空データは一切含めない。

schema_version 2.1:
  - consolidation_type / accounting_standard / currency / unit をトップレベルに追加
  - profit_loss → net_income_attributable_to_parent
  - earnings_per_share → earnings_per_share_basic / earnings_per_share_diluted
  - shares_outstanding を必須化
  - current_year / prior_year に period (start/end) を追加
  - EPS整合チェックを実装

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

SCHEMA_VERSION = "1.0"

DERIVED_KEYS = frozenset({
    "roe", "roa", "roic", "operating_margin", "net_margin",
    "equity_ratio", "de_ratio",
    "sales_growth", "profit_growth", "eps_growth",
    "per", "pbr", "psr", "peg", "dividend_yield",
    "free_cash_flow", "cagr",
    "profit_loss", "earnings_per_share",
})

FACT_KEYS = frozenset({
    "total_assets", "equity", "interest_bearing_debt",
    "net_sales", "operating_income",
    "net_income_attributable_to_parent",
    "earnings_per_share_basic", "earnings_per_share_diluted",
    "shares_outstanding",
})

VALID_ACCOUNTING_STANDARDS = frozenset({
    "JGAAP", "IFRS", "US-GAAP",
})

EPS_TOLERANCE_RATIO = 0.01


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
    mapping = {
        "Japan GAAP": "JGAAP",
        "日本基準": "JGAAP",
        "IFRS": "IFRS",
        "US GAAP": "US-GAAP",
        "US-GAAP": "US-GAAP",
        "JGAAP": "JGAAP",
    }
    return mapping.get(s, s)


def _validate_metrics(metrics: dict[str, Any], label: str, security_code: str) -> None:
    """
    出力前バリデーション。

    1. Derived指標が混入していないか
    2. null値が存在しないか
    3. metricsが空でないか
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


def _validate_eps_consistency(
    metrics: dict[str, Any], label: str, security_code: str,
) -> None:
    """
    EPS整合チェック: |EPS_basic - (net_income / shares_outstanding)| の誤差率が1%以内か。
    超えた場合は警告ログを出力する（データ不採用ではなく警告）。
    """
    eps = metrics.get("earnings_per_share_basic")
    net_income = metrics.get("net_income_attributable_to_parent")
    shares = metrics.get("shares_outstanding")

    if eps is None or net_income is None or shares is None or shares == 0:
        return

    computed_eps = net_income / shares
    if abs(eps) < 0.001:
        return

    error_ratio = abs(eps - computed_eps) / abs(eps)
    if error_ratio > EPS_TOLERANCE_RATIO:
        logger.warning(
            "EPS CONSISTENCY WARN [%s] %s: "
            "eps_basic=%.2f, computed=%.2f (net_income/shares), error=%.2f%%",
            security_code, label, eps, computed_eps, error_ratio * 100,
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

    def _sanitize_metrics(self, year_data: dict[str, Any]) -> dict[str, float | int] | None:
        """
        year_data から metrics を抽出し、Factのみを残す。
        有効なFactがなければ None を返す。
        """
        metrics = year_data.get("metrics")
        if not metrics or not isinstance(metrics, dict):
            return None

        clean: dict[str, float | int] = {}
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
            _validate_eps_consistency(current_metrics, "current_year", sc)
        if prior_metrics:
            _validate_metrics(prior_metrics, "prior_year", sc)
            _validate_eps_consistency(prior_metrics, "prior_year", sc)

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
