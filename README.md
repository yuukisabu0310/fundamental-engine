# fundamental-engine

財務データ抽出エンジン。EDINETの有価証券報告書XBRLから財務Factを抽出・正規化し、financial-datasetへ出力する。

## プロジェクトの位置づけ

本リポジトリは投資データ基盤の**データ生成エンジン層**を担う。

```
fundamental-engine         ← 本リポジトリ（財務Fact生成）
├── financial-dataset       財務Factデータレイク（確定決算のみ）
├── market-dataset          (予定) 市場Factデータレイク（株価・出来高）
├── valuation-engine        (予定) 派生指標計算エンジン（PER/PBR/PSR/PEG）
└── screening-engine        (予定) 投資条件評価エンジン
```

### レイヤー分離アーキテクチャ

```
┌─────────────────────────────────────┐
│         screening-engine            │  ビジネスロジック層
│  PER<15, ROE>15%, 売上成長率>10%     │  (投資判断)
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│         valuation-engine            │  計算エンジン層
│  PER / PBR / PSR / PEG             │  (Derived: 再計算可能)
└──────┬──────────────────┬───────────┘
       │ JOIN             │ JOIN
┌──────▼──────┐  ┌────────▼───────────┐
│ financial-  │  │   market-dataset   │  Factデータレイク層
│   dataset   │  │   株価/出来高/配当  │  (不可逆な事実のみ)
│ 売上/利益   │  │                    │
│ EPS/FCF     │  │                    │
└──────┬──────┘  └────────────────────┘
       │
 fundamental-engine (本リポジトリ)
```

### 責務

- 有価証券報告書・四半期報告書のXBRL取得
- タグの正規化（PL / BS / CF / DEI）
- 財務Factの統合（equity / total_assets / net_sales / profit_loss / EPS / FCF等）
- financial-datasetへのJSON出力・manifest管理

### 設計方針

- **データソース非依存**: リポジトリ名・アーキテクチャはEDINETに依存しない。将来他のデータソースも統合可能
- **FactとDerivedの分離**: financial-datasetには確定決算の財務Factのみを保存。PER/PBR等の派生指標はvaluation-engineで算出
- **レイヤー責務の厳守**: 各リポジトリは単一責任を持つ
- **再計算可能な値は保存しない**: 株価依存の指標（PER/PBR/PSR等）はデータレイクに含めない

## パイプライン

```
EDINET API → Download → Extract → Parse → Normalize → FinancialMaster → JSONExport
                                                                              ↓
                                                              financial-dataset/{report_type}/{data_version}/{code}.json
```

FinancialMaster の出力が直接 JSONExporter に渡される。MarketIntegrator / ValuationEngine はパイプラインに含めない（別レイヤーの責務）。

## 機能

- EDINET API v2 を使用した書類一覧取得
- 指定期間の日付ループ取得
- 有価証券報告書・四半期報告書のみ抽出（docTypeCode: 120/130/140）
- ZIP保存・解凍・XBRLファイル抽出
- 重複ダウンロード回避
- エラーハンドリングとログ出力

## セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. 環境変数の設定

`.env.example` を `.env` にコピーして、EDINET APIキーとDATASET_PATHを設定してください。

```bash
cp .env.example .env
```

`.env` ファイルを編集：

```env
EDINET_API_KEY=YOUR_API_KEY
DATASET_PATH=./financial-dataset
```

### 3. 外部データリポジトリ（financial-dataset）の設定

financial-dataset を submodule として追加する場合：

```bash
git submodule add git@github.com:<yourname>/financial-dataset.git financial-dataset
git submodule update --init --recursive
```

または、ローカルで financial-dataset ディレクトリを作成する場合：

```bash
mkdir -p financial-dataset/annual financial-dataset/quarterly financial-dataset/metadata
```

### 4. 設定ファイルの編集（オプション）

`config/settings.yaml` が無い場合は、`config/settings.yaml.example` を `config/settings.yaml` にコピーしてください。その後、取得期間などを必要に応じて編集してください。

```yaml
# 取得期間（空の場合は日本時間の本日で取得）
start_date: "2021-01-01"
end_date: "2024-12-31"

# リクエスト間の待機秒数
sleep_seconds: 0.2
```

- **日付の省略**: `start_date` または `end_date` が未設定・空文字の場合は、**両方とも日本時間（JST）の本日**で取得します。日次実行で「本日分だけ」取得したい場合は、空文字 `""` にしておくかキーを省略できます。
- EDINET APIキーは[EDINET API利用登録](https://disclosure.edinet-fsa.go.jp/guide/guide_api.html)から取得してください。
- APIキーは `.env` または環境変数 `EDINET_API_KEY` から読み込まれます（優先: 環境変数 > .env）。

## 実行方法

### ローカル実行

```bash
python main.py
```

### GitHub Actionsでの実行

1. **Environment secretsの設定**
   - GitHubリポジトリの Settings > Environments > production で `EDINET_API_KEY` を設定

2. **手動実行**
   - Actionsタブから「EDINET XBRL Download」ワークフローを選択
   - 「Run workflow」をクリック
   - 開始日・終了日を指定して実行（ワークフロー入力で上書き可能）

3. **自動実行（日次）**
   - 毎日午前3時（JST）に自動実行されます
   - 実行時は `config/settings.yaml.example` を `config/settings.yaml` にコピーして使用します。`start_date` / `end_date` を空にしておくと、**本日（JST）のみ**取得します。

## ディレクトリ構造

```
data/
 └─ edinet/
     ├─ raw_zip/
     │    └─ YYYY/
     │         └─ docID.zip
     └─ raw_xbrl/
          └─ YYYY/
               └─ docID/
                    └─ *.xbrl

logs/
 └─ edinet_download.log

config/
 ├─ settings.yaml          # 実設定（Git管理外）
 └─ settings.yaml.example  # テンプレート（日次実行時にコピー元）

.env
.env.example
```

## 取得対象条件

以下の `docTypeCode` に一致する書類のみを取得します：

| docTypeCode | 書類種別 |
|---|---|
| `120` | 有価証券報告書 |
| `130` | 半期報告書 |
| `140` | 四半期報告書 |

> **注**: 以前は `formCode == "030000"` でフィルタしていましたが、大量保有報告書（第三号様式）等も `formCode=030000` に該当するため、`docTypeCode` ベースに変更しました。

## パイプラインのスキップ条件

以下の書類は処理対象外としてスキップされます：

1. **ファイル名による早期スキップ**: ファイル名に `jplvh`（大量保有報告書）/ `jpaud`（監査報告書）等のパターンが含まれる場合
2. **必須項目検証**: `security_code` または `fiscal_year_end` が取得できない場合

## JSON出力仕様（schema_version 1.0）

financial-dataset には**財務Factのみ**を保存する。Derived指標・null値・空データは一切含めない。
EDINET原本に対して100%トレーサブルなデータ構造を採用する。

```json
{
  "schema_version": "1.0",
  "engine_version": "1.0.0",
  "data_version": "2025FY",
  "generated_at": "2026-02-21T06:37:44Z",
  "doc_id": "S100XL6L",
  "security_code": "2734",
  "report_type": "annual",
  "consolidation_type": "consolidated",
  "accounting_standard": "JGAAP",
  "currency": "JPY",
  "unit": "JPY",
  "current_year": {
    "period": {
      "start": "2024-12-01",
      "end": "2025-11-30"
    },
    "metrics": {
      "total_assets": 218345000000.0,
      "equity": 81630000000.0,
      "net_sales": 251533000000.0,
      "operating_income": 7381000000.0,
      "net_income_attributable_to_parent": 5870000000.0,
      "earnings_per_share_basic": 91.44,
      "shares_outstanding": 64200000
    }
  }
}
```

### トップレベル項目

| 項目 | 説明 |
|---|---|
| `schema_version` | JSON構造バージョン（破壊的変更時にインクリメント） |
| `consolidation_type` | `consolidated` / `non_consolidated`（連結/個別でROE等が変わるため明示） |
| `accounting_standard` | `JGAAP` / `IFRS` / `US-GAAP`（会計基準による指標定義の違いを明示） |
| `currency` | 通貨コード（固定: `JPY`） |
| `unit` | 単位（固定: `JPY` — 百万円問題を完全排除） |

### 出力ルール

- **Factのみ**: 財務諸表に記載された数値のみ出力
- **null出力禁止**: 値が取得できなかった項目はキーごと省略
- **空prior_year省略**: prior_yearに有効なFactがなければキー自体を出力しない
- **Derived禁止**: ROE/ROA/ROIC/マージン/成長率/FCF/CAGR等の再計算可能な値はvaluation-engineの責務
- **security_code正規化**: 5桁かつ末尾"0"の場合のみ末尾1桁を削除（例: "27340" → "2734"）
- **会計定義明示**: consolidation_type / accounting_standard を必ず出力
- **EPS分離**: basic と diluted を明確に分離
- **period保持**: 変則決算・IFRS中間期に対応するため start/end を保持

### Fact項目一覧

| キー | 出典 | 説明 |
|---|---|---|
| `total_assets` | BS | 総資産 |
| `equity` | BS | 自己資本（shareholders_equity > equity > net_assets の優先順位で選択） |
| `interest_bearing_debt` | BS | 有利子負債（XBRLタグ存在時のみ、内訳合算は行わない） |
| `net_sales` | PL | 売上高 |
| `operating_income` | PL | 営業利益 |
| `net_income_attributable_to_parent` | PL | 親会社株主に帰属する当期純利益（IFRS対応。旧: profit_loss） |
| `earnings_per_share_basic` | PL | 基本的1株当たり当期純利益（旧: earnings_per_share） |
| `earnings_per_share_diluted` | PL | 希薄化後1株当たり当期純利益（存在時のみ） |
| `shares_outstanding` | BS | 発行済株式数（EPS整合検証に必須） |

### 含めないデータ（レイヤー分離原則）

| データ | 分類 | 所属レイヤー |
|---|---|---|
| ROE / ROA / ROIC / マージン / 成長率 / CAGR | Derived（再計算可能） | valuation-engine |
| FCF (営業CF + 投資CF) | Derived（CF合算計算値） | valuation-engine |
| stock_price / volume | 市場Fact | market-dataset |
| PER / PBR / PSR / PEG / dividend_yield | Derived（再計算可能） | valuation-engine |

### 出力前バリデーション

JSONExporter は出力前に以下を検証する：

1. metrics 内に Derived 指標が混入していないか → **エラー**
2. null 値が存在しないか → **エラー**
3. metrics が空でないか → **警告**
4. EPS整合チェック: `|EPS_basic - (net_income / shares_outstanding)|` の誤差率が1%以内か → **警告**

## データセット自動生成とプッシュ

mainブランチへのpush時に、financial-datasetリポジトリへ自動的にJSONを生成・プッシュします。

### CI セットアップ

1. **GitHub Secrets の設定**

   fundamental-engine リポジトリの Settings → Secrets and variables → Actions で以下を追加：

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

## ログ出力

`logs/edinet_download.log` に以下の情報が記録されます：

- 日付 / docID / ステータス（SUCCESS / SKIP / ERROR） / エラーメッセージ

## エラーハンドリング

- HTTPエラー時は3回リトライ
- それでも失敗したらログ出力して処理継続
- ZIPが既に存在すればスキップ
- 解凍済フォルダがあればスキップ
