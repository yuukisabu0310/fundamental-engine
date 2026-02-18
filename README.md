# edinet-xbrl-parser

EDINETから有価証券報告書のXBRLを取得するツール。日本株分析のためのデータ取得基盤として利用できます。

## Phase1: EDINET XBRL取得システム

EDINETから有価証券報告書XBRLを完全取得する基盤構築

### 機能

- EDINET API v2 を使用した書類一覧取得
- 指定期間の日付ループ取得
- 有価証券報告書（formCode=030000）のみ抽出
- ZIP保存・解凍・XBRLファイル抽出
- 重複ダウンロード回避
- エラーハンドリングとログ出力

### セットアップ

1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

2. 環境変数の設定

`.env.example` を `.env` にコピーして、EDINET APIキーとDATASET_PATHを設定してください。

```bash
cp .env.example .env
```

`.env` ファイルを編集：

```env
EDINET_API_KEY=YOUR_API_KEY
DATASET_PATH=./financial-dataset
```

3. 外部データリポジトリ（financial-dataset）の設定

financial-dataset を submodule として追加する場合：

```bash
# financial-dataset リポジトリが存在する場合
git submodule add git@github.com:<yourname>/financial-dataset.git financial-dataset
git submodule update --init --recursive
```

または、ローカルで financial-dataset ディレクトリを作成する場合：

```bash
mkdir -p financial-dataset/annual financial-dataset/quarterly financial-dataset/metadata
```

4. 設定ファイルの編集（オプション）

`config/settings.yaml` が無い場合は、`config/settings.yaml.example` を `config/settings.yaml` にコピーしてください。その後、取得期間などを必要に応じて編集してください。

```yaml
# 取得期間（空の場合は日本時間の本日で取得）
start_date: "2021-01-01"
end_date: "2024-12-31"

# リクエスト間の待機秒数
sleep_seconds: 0.2
```

- **日付の省略**: `start_date` または `end_date` が未設定・空文字の場合は、**両方とも日本時間（JST）の本日**で取得します。日次実行で「本日分だけ」取得したい場合は、空文字 `""` にしておくかキーを省略できます。YAMLはコメントが書けるため、仕様のメモも追記しやすくなっています。
- EDINET APIキーは[EDINET API利用登録](https://disclosure.edinet-fsa.go.jp/guide/guide_api.html)から取得してください。
- APIキーは `.env` または環境変数 `EDINET_API_KEY` から読み込まれます（優先: 環境変数 > .env）。

### 実行方法

#### ローカル実行

```bash
python main.py
```

または：

```bash
cd src
python main.py
```

#### GitHub Actionsでの実行

1. **Environment secretsの設定**
   - GitHubリポジトリの Settings > Environments > production で `EDINET_API_KEY` を設定

2. **手動実行**
   - Actionsタブから「EDINET XBRL Download」ワークフローを選択
   - 「Run workflow」をクリック
   - 開始日・終了日を指定して実行（ワークフロー入力で上書き可能）

3. **自動実行（日次）**
   - 毎日午前3時（JST）に自動実行されます
   - 実行時は `config/settings.yaml.example` を `config/settings.yaml` にコピーして使用します。example で `start_date` / `end_date` を空にしておくと、**本日（JST）のみ**取得します。

### ディレクトリ構造

```
data/
 └─ edinet/
     ├─ raw_zip/
     │    └─ YYYY/
     │         └─ docID.zip
     │
     └─ raw_xbrl/
          └─ YYYY/
               └─ docID/
                    └─ *.xbrl

logs/
 └─ edinet_download.log

config/
 ├─ settings.yaml          # 実設定（Git管理外）
 └─ settings.yaml.example # テンプレート（日次実行時にコピー元）

.env
.env.example
```

### 取得対象条件

以下の条件を満たす書類のみを取得します：

- `formCode == "030000"`（有価証券報告書）

### ログ出力

`logs/edinet_download.log` に以下の情報が記録されます：

- 日付
- docID
- ステータス（SUCCESS / SKIP / ERROR）
- エラーメッセージ

### エラーハンドリング

- HTTPエラー時は3回リトライ
- それでも失敗したらログ出力して処理継続
- ZIPが既に存在すればスキップ
- 解凍済フォルダがあればスキップ

### Phase6: データセット自動生成とプッシュ

mainブランチへのpush時に、financial-datasetリポジトリへ自動的にJSONを生成・プッシュします。

#### セットアップ

1. **GitHub Secrets の設定**

   edinet-xbrl-parser リポジトリの Settings → Secrets and variables → Actions で以下を追加：

   - `DATASET_DEPLOY_KEY`: financial-dataset リポジトリに登録した Deploy Key の秘密鍵

2. **ワークフローの設定**

   `.github/workflows/push_dataset.yml` の `<yourname>` を実際のGitHubユーザー名に置き換えてください。

   ```yaml
   git clone git@github.com:<yourname>/financial-dataset.git dataset
   ```

3. **実行**

   mainブランチにpushすると、自動的に以下が実行されます：

   - XBRLファイルのパース
   - 書類種別のフィルタリング（有価証券報告書・四半期報告書のみ処理）
   - JSON生成（`financial-dataset/annual/YYYYFY/{security_code}.json`）
   - dataset_manifest.json の更新
   - financial-dataset リポジトリへの自動commit & push

### パイプラインのスキップ条件（process_all.py）

以下の書類は処理対象外としてスキップされます：

1. **ファイル名による早期スキップ**: ファイル名に `jplvh` 等のスキップパターンが含まれる場合（大量保有報告書など、財務データを含まない書類）
2. **必須項目検証**: `security_code` または `fiscal_year_end` が取得できない場合

これにより、有価証券報告書・四半期報告書以外のXBRL（例：大量保有報告書）がデータレイクに混入することを防止します。

### 今後の拡張予定

- taxonomy_version検知
- tag_alias正規化
- context判定
- financial_master生成
