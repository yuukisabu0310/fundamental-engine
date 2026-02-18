"""
DatasetManifestGenerator（Phase6）
Data Repo内のフォルダをスキャンし、dataset_manifest.json を自動生成する。

型安全・拡張可能・Screening互換設計。

外部データリポジトリ（financial-dataset）をスキャンする。
DATASET_PATH 環境変数でスキャン先を指定する。
"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# __version__ を取得（プロジェクトルート基準でインポート）
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src import __version__

logger = logging.getLogger(__name__)

# 定数
SCHEMA_VERSION = "1.0"
ENGINE_VERSION = __version__
# manifestに含めない無効な決算期（不正データ防止）
EXCLUDED_PERIOD_NAMES = frozenset({"UNKNOWN"})


class DatasetManifestGenerator:
    """
    financial-dataset/ フォルダをスキャンし、決算期ごとのメタデータを生成する。

    前提ディレクトリ構造:
        financial-dataset/
        ├── annual/
        │   ├── 2025FY/
        │   └── 2024FY/
        ├── quarterly/
        │   ├── 2025Q1/
        │   └── ...
        └── metadata/
    """

    def __init__(self, base_path: str | None = None) -> None:
        """
        Args:
            base_path: スキャン対象のベースパス（プロジェクトルート基準）。
                       None の場合は DATASET_PATH 環境変数を使用。
        """
        if base_path is None:
            base_path_str = os.environ.get("DATASET_PATH")
            if not base_path_str:
                raise EnvironmentError(
                    "DATASET_PATH 環境変数が設定されていません。"
                    ".env ファイルまたは環境変数で DATASET_PATH を設定してください。"
                )
            base_path = base_path_str

        self.base_path = Path(base_path)

    def _scan_periods(self, category: str) -> tuple[list[str], dict[str, int]]:
        """
        指定カテゴリ（annual/quarterly）の決算期をスキャンする。

        Args:
            category: "annual" または "quarterly"

        Returns:
            (periods, record_counts) のタプル。
            - periods: 決算期のリスト（降順ソート済み）
            - record_counts: 決算期をキーとする件数辞書
        """
        category_dir = self.base_path / category

        if not category_dir.exists():
            logger.info("Category directory does not exist: %s", category_dir)
            return [], {}

        periods: list[str] = []
        record_counts: dict[str, int] = {}

        # サブディレクトリを走査
        for period_dir in category_dir.iterdir():
            if not period_dir.is_dir():
                continue

            period_name = period_dir.name

            # 無効な決算期（UNKNOWN等）はスキップ
            if period_name in EXCLUDED_PERIOD_NAMES:
                logger.debug("Skipping excluded period: %s", period_name)
                continue

            # .json ファイルのみカウント
            json_files = list(period_dir.glob("*.json"))
            count = len(json_files)

            if count > 0:
                periods.append(period_name)
                record_counts[period_name] = count
                logger.debug("Found %s: %d files", period_name, count)

        # 決算期を降順ソート
        periods.sort(reverse=True)

        logger.info(
            "Scanned %s: %d periods, total %d files",
            category,
            len(periods),
            sum(record_counts.values()),
        )

        return periods, record_counts

    def generate(self) -> dict[str, Any]:
        """
        フォルダ構造をスキャンし、manifest辞書を生成する。

        Returns:
            manifest辞書（型安全、null禁止）。
        """
        # annual と quarterly をスキャン
        annual_periods, annual_counts = self._scan_periods("annual")
        quarterly_periods, quarterly_counts = self._scan_periods("quarterly")

        # latest を決定（降順ソート済みなので先頭が最新）
        latest_annual = annual_periods[0] if annual_periods else None
        latest_quarterly = quarterly_periods[0] if quarterly_periods else None

        # UTC ISO8601形式の生成日時
        generated_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "generated_at": generated_at,
            "latest_annual": latest_annual,
            "latest_quarterly": latest_quarterly,
            "annual_periods": annual_periods,  # 常に list
            "quarterly_periods": quarterly_periods,  # 常に list
            "record_counts": {
                "annual": annual_counts,  # 常に dict
                "quarterly": quarterly_counts,  # 常に dict（空でも {}、null禁止）
            },
        }

        logger.info(
            "Manifest generated: latest_annual=%s, latest_quarterly=%s",
            latest_annual,
            latest_quarterly,
        )
        logger.info(
            "Annual periods: %d, Quarterly periods: %d",
            len(annual_periods),
            len(quarterly_periods),
        )

        return manifest

    def save(self) -> str:
        """
        metadata/dataset_manifest.json に保存する。

        Returns:
            保存されたファイルのパス。
        """
        manifest = self.generate()

        # metadata ディレクトリを作成
        metadata_dir = self.base_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)

        # JSONファイルに保存
        output_path = metadata_dir / "dataset_manifest.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        logger.info("Manifest saved to: %s", output_path)
        return str(output_path)
