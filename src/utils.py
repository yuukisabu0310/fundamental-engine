"""
共通ユーティリティ関数
"""
import logging
import os
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
import yaml
from dotenv import load_dotenv

# 日本標準時（JST = UTC+9）
JST = timezone(timedelta(hours=9))


def get_today_jst() -> str:
    """日本時間（JST）の本日の日付を YYYY-MM-DD で返す"""
    return datetime.now(JST).strftime("%Y-%m-%d")


def setup_logging(log_dir: Path) -> logging.Logger:
    """
    ログ設定を行う
    
    Args:
        log_dir: ログディレクトリのパス
        
    Returns:
        設定済みのロガー
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "edinet_download.log"
    
    # ログフォーマット設定
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ファイルハンドラ
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # コンソールハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # ロガー設定
    logger = logging.getLogger('edinet_downloader')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def load_settings(config_path: Path, env_path: Path = None) -> Dict[str, Any]:
    """
    設定ファイル（YAML）を読み込む
    .envファイルからAPIキーを読み込む（環境変数が優先）
    
    Args:
        config_path: 設定ファイルのパス（.yaml）
        env_path: .envファイルのパス（Noneの場合はプロジェクトルートを自動検出）
        
    Returns:
        設定辞書
        
    Raises:
        FileNotFoundError: 設定ファイルが存在しない場合
        yaml.YAMLError: YAMLの解析に失敗した場合
    """
    # .envファイルの読み込み
    if env_path is None:
        project_root = config_path.parent.parent
        env_path = project_root / '.env'
    
    if env_path.exists():
        load_dotenv(env_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        settings = yaml.safe_load(f)
    
    if settings is None:
        settings = {}
    
    # 環境変数からAPIキーを取得（.envファイルから読み込まれた値、または既存の環境変数）
    env_api_key = os.getenv('EDINET_API_KEY')
    if env_api_key:
        settings['api_key'] = env_api_key

    # start_date / end_date が未設定・空の場合は両方とも本日（JST）にする
    start_date = settings.get("start_date")
    end_date = settings.get("end_date")
    if not start_date or not str(start_date).strip() or not end_date or not str(end_date).strip():
        today = get_today_jst()
        settings["start_date"] = today
        settings["end_date"] = today

    return settings


def ensure_directories(base_dir: Path) -> Dict[str, Path]:
    """
    必要なディレクトリを作成する
    
    Args:
        base_dir: ベースディレクトリのパス
        
    Returns:
        ディレクトリパスの辞書
    """
    dirs = {
        'raw_zip': base_dir / 'edinet' / 'raw_zip',
        'raw_xbrl': base_dir / 'edinet' / 'raw_xbrl',
    }
    
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
    
    return dirs


def parse_date(date_str: str) -> datetime:
    """
    日付文字列をdatetimeオブジェクトに変換
    
    Args:
        date_str: YYYY-MM-DD形式の日付文字列
        
    Returns:
        datetimeオブジェクト
    """
    return datetime.strptime(date_str, '%Y-%m-%d')


def date_range(start_date: str, end_date: str):
    """
    日付範囲を生成するジェネレータ
    
    Args:
        start_date: 開始日（YYYY-MM-DD）
        end_date: 終了日（YYYY-MM-DD）
        
    Yields:
        日付文字列（YYYY-MM-DD）
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    current = start
    
    while current <= end:
        yield current.strftime('%Y-%m-%d')
        current = datetime(
            current.year,
            current.month,
            current.day
        )
        # 次の日へ
        from datetime import timedelta
        current += timedelta(days=1)


def debug_log_documents(
    documents_data: Dict[str, Any],
    date: str,
    logger: logging.Logger
) -> None:
    """
    1日分の書類一覧をデバッグログに出力
    
    Args:
        documents_data: 書類一覧のJSONデータ
        date: 日付（YYYY-MM-DD）
        logger: ロガー
    """
    if not documents_data or "results" not in documents_data:
        logger.debug(f"[DEBUG] [{date}] 書類データが空です")
        return
    
    results = documents_data["results"]
    total_count = len(results)
    
    logger.info(f"[DEBUG] [{date}] 書類総数: {total_count}件")
    
    # formCode別の集計
    form_code_count = {}
    for doc in results:
        form_code = doc.get("formCode")
        # Noneの場合は"不明"として扱う
        if form_code is None:
            form_code = "不明"
        form_code_count[form_code] = form_code_count.get(form_code, 0) + 1
    
    logger.info(f"[DEBUG] [{date}] formCode別集計:")
    # Noneが含まれる可能性があるため、キーを文字列に変換してソート
    for form_code, count in sorted(form_code_count.items(), key=lambda x: str(x[0]) if x[0] is not None else ""):
        logger.info(f"[DEBUG]   - formCode {form_code}: {count}件")
    
    # 有価証券報告書（030000）の詳細をログ出力
    filtered_030000 = [doc for doc in results if doc.get("formCode") == "030000"]
    logger.info(f"[DEBUG] [{date}] 有価証券報告書（030000）: {len(filtered_030000)}件")
    
    if filtered_030000:
        logger.info(f"[DEBUG] [{date}] 有価証券報告書の詳細（最初の10件）:")
        for i, doc in enumerate(filtered_030000[:10], 1):
            doc_id = doc.get("docID", "不明")
            form_code = doc.get("formCode", "不明")
            ordinance_code = doc.get("ordinanceCode", "不明")
            doc_type_code = doc.get("docTypeCode", "不明")
            doc_description = doc.get("docDescription", "不明")
            logger.info(
                f"[DEBUG]   {i}. docID: {doc_id}, "
                f"formCode: {form_code}, "
                f"ordinanceCode: {ordinance_code}, "
                f"docTypeCode: {doc_type_code}, "
                f"説明: {doc_description}"
            )
        if len(filtered_030000) > 10:
            logger.info(f"[DEBUG]   ... 他 {len(filtered_030000) - 10}件")
