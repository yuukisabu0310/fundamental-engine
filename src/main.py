"""
EDINET XBRL取得システム - メイン処理
"""
import sys
import os
from pathlib import Path
from typing import Dict, Any
from tqdm import tqdm

# プロジェクトルートとsrcディレクトリをパスに追加
# __file__が正しく設定されていない場合は環境変数から取得
if '__file__' in globals() and globals()['__file__']:
    _file_path = Path(globals()['__file__'])
    project_root = _file_path.parent.parent
    src_dir = _file_path.parent
else:
    # 環境変数から取得、なければカレントディレクトリ
    project_root = Path(os.environ.get('PROJECT_ROOT', Path.cwd()))
    src_dir = project_root / "src"

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(src_dir))

from utils import (
    setup_logging,
    load_settings,
    ensure_directories,
    date_range,
    debug_log_documents
)
from edinet_client import EdinetClient
from downloader import Downloader
from extractor import Extractor


def main():
    """メイン処理"""
    # パス設定
    # 環境変数PROJECT_ROOTが設定されている場合はそれを使用
    if 'PROJECT_ROOT' in os.environ:
        project_root = Path(os.environ['PROJECT_ROOT'])
    elif '__file__' in globals() and globals()['__file__']:
        # __file__から推測
        project_root = Path(globals()['__file__']).parent.parent
    else:
        # フォールバック: カレントディレクトリを使用
        project_root = Path.cwd()
    
    config_path = project_root / "config" / "settings.yaml"
    data_dir = project_root / "data"
    log_dir = project_root / "logs"
    
    # ログ設定
    logger = setup_logging(log_dir)
    logger.info("=" * 60)
    logger.info("EDINET XBRL取得システム - 開始")
    logger.info("=" * 60)
    
    try:
        # 設定読み込み
        settings = load_settings(config_path)
        api_key = settings.get("api_key")
        start_date = settings.get("start_date")
        end_date = settings.get("end_date")
        sleep_seconds = settings.get("sleep_seconds", 0.2)
        
        # APIキーチェック
        if not api_key or api_key == "YOUR_API_KEY":
            logger.error("APIキーが設定されていません。.envファイルまたは環境変数EDINET_API_KEYを確認してください。")
            sys.exit(1)
        
        logger.info(f"開始日: {start_date}")
        logger.info(f"終了日: {end_date}")
        logger.info(f"待機時間: {sleep_seconds}秒")
        
        # ディレクトリ作成
        dirs = ensure_directories(data_dir)
        logger.info(f"データディレクトリ: {data_dir}")
        
        # クライアント初期化
        client = EdinetClient(api_key, sleep_seconds)
        downloader = Downloader(client, dirs['raw_zip'])
        extractor = Extractor(dirs['raw_zip'], dirs['raw_xbrl'])
        
        # 日付ループ処理
        date_list = list(date_range(start_date, end_date))
        logger.info(f"処理対象日数: {len(date_list)}日")
        
        total_downloaded = 0
        total_skipped = 0
        total_errors = 0
        
        with tqdm(date_list, desc="Processing dates") as date_pbar:
            for date in date_pbar:
                date_pbar.set_postfix({"date": date})
                
                # 書類一覧取得
                documents_data = client.get_documents_list(date)
                if not documents_data:
                    logger.warning(f"書類一覧取得失敗 [{date}]")
                    continue
                
                # デバッグ: 1日分の書類一覧をログ出力
                debug_log_documents(documents_data, date, logger)
                
                # フィルタリング
                filtered_docs = client.filter_documents(documents_data)
                logger.info(f"フィルタ後対象書類数 [{date}]: {len(filtered_docs)}件")
                
                if not filtered_docs:
                    logger.debug(f"対象書類なし [{date}]")
                    continue
                
                # ZIPダウンロード
                download_results = downloader.download_documents(date, filtered_docs)
                
                # 統計更新
                for doc_id, status in download_results.items():
                    if status == "SUCCESS":
                        total_downloaded += 1
                        # ダウンロード成功したら即座に解凍
                        year = date[:4]
                        zip_path = downloader.get_zip_path(doc_id, year)
                        extractor.process_zip(zip_path, year)
                    elif status == "SKIP":
                        total_skipped += 1
                    else:
                        total_errors += 1
        
        # 最終統計
        logger.info("=" * 60)
        logger.info("処理完了")
        logger.info(f"ダウンロード成功: {total_downloaded}件")
        logger.info(f"スキップ: {total_skipped}件")
        logger.info(f"エラー: {total_errors}件")
        logger.info("=" * 60)
    
    except FileNotFoundError as e:
        logger.error(f"ファイルが見つかりません: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("処理が中断されました")
        sys.exit(1)
    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
