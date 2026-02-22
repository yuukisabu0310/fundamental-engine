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
│ 株数        │  │                    │
└──────┬──────┘  └────────────────────┘
       │
 fundamental-engine (本リポジトリ)
```

### 責務

- 有価証券報告書・四半期報告書のXBRL取得
- タグの正規化（PL / BS / CF / DEI）
- 財務Factの統合（equity / total_assets / net_sales / profit_loss / total_number_of_issued_shares等）
- financial-datasetへのJSON出力・manifest管理

### 設計方針

- **データソース非依存**: リポジトリ名・アーキテクチャはEDINETに依存しない。将来他のデータソースも統合可能
- **FactとDerivedの分離**: financial-datasetには確定決算の財務Factのみを保存。PER/PBR等の派生指標はvaluation-engineで算出
- **レイヤー責務の厳守**: 各リポジトリは単一責任を持つ
- **再計算可能な値は保存しない**: 株価依存の指標（PER/PBR/PSR等）はデータレイクに含めない

### EPS設計（再計算可能な値は保存しない）

EPSは `net_income / total_number_of_issued_shares` の派生値であり、Fact-onlyの原則に従いfinancial-datasetには保存しない。
valuation-engineで `net_income_attributable_to_parent / total_number_of_issued_shares` として算出する。
日本基準では `total_number_of_issued_shares` は期末株式数（加重平均ではない）のため近似値となるが、Factレイクは原則を厳守する。

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

financial-dataset には**財務Factのみ**を保存する。Derived指標は含めない。
値が取得できなかった項目は `null` として出力する。EDINET原本に対して100%トレーサブルなデータ構造を採用する。

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
      "ordinary_income": 7200000000.0,
      "net_income_attributable_to_parent": 5870000000.0,
      "total_number_of_issued_shares": 64200000,
      "cash_and_equivalents": 15000000000.0,
      "operating_cash_flow": 8500000000.0,
      "depreciation": 3200000000.0,
      "dividends_per_share": 50.0,
      "short_term_borrowings": 5000000000.0,
      "current_portion_of_long_term_borrowings": 2000000000.0,
      "commercial_papers": null,
      "current_portion_of_bonds": null,
      "short_term_lease_obligations": 100000000.0,
      "bonds_payable": 10000000000.0,
      "long_term_borrowings": 15000000000.0,
      "long_term_lease_obligations": 300000000.0,
      "lease_obligations": null
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
| `unit` | 単位（固定: `JPY` — XBRL値をそのまま使用。EDINET主要指標は円単位で統一） |

### 出力ルール

- **Factのみ**: 財務諸表に記載された数値のみ出力
- **null許容**: 値が取得できなかった項目は `null` として出力（キーは常に存在）
- **空prior_year省略**: prior_yearに有効なFact（1つ以上の非null値）がなければキー自体を出力しない
- **Derived禁止**: ROE/ROA/ROIC/マージン/成長率/FCF/CAGR等の再計算可能な値はvaluation-engineの責務
- **security_code正規化**: 5桁かつ末尾"0"の場合のみ末尾1桁を削除（例: "27340" → "2734"）
- **会計定義明示**: consolidation_type / accounting_standard を必ず出力
- **period保持**: 変則決算・IFRS中間期に対応するため start/end を保持

### Fact項目一覧

#### 基礎財務項目

| キー | 出典 | 説明 |
|---|---|---|
| `total_assets` | BS | 総資産（JGAAP: `Assets` / IFRS: `Assets`）|
| `equity` | BS | 自己資本（`ShareholdersEquity > EquityAttributableToOwnersOfParent > Equity > NetAssets` の優先順位）|
| `net_sales` | PL | 売上高（JGAAP: `NetSales` / `OperatingRevenue1/2` / IFRS: `Revenue`）|
| `operating_income` | PL | 営業利益（JGAAP: `OperatingIncome` / IFRS: `OperatingProfitLoss`）|
| `ordinary_income` | PL | 経常利益（JGAAP特有。IFRSには概念なし）|
| `net_income_attributable_to_parent` | PL | 親会社株主に帰属する当期純利益 |
| `total_number_of_issued_shares` | DEI | 発行済株式数 |

#### 分析用追加項目

| キー | 出典 | 説明 |
|---|---|---|
| `cash_and_equivalents` | CF/BS | 現金及び現金同等物（`CashAndCashEquivalents > CashAndDeposits` の優先順位）|
| `operating_cash_flow` | CF | 営業キャッシュ・フロー |
| `depreciation` | CF | 減価償却費（`DepreciationAndAmortizationOpeCF > DepreciationSGA` の優先順位）|
| `dividends_per_share` | DEI | 1株当たり配当額（個別ベース。NonConsolidated contextからも取得）|

#### 有利子負債構成項目

合算はvaluation-engineで実施（リース債務の含む/含まないを切り替え可能にするため）。

| キー | 出典 | JGAAP タグ | 説明 |
|---|---|---|---|
| `short_term_borrowings` | BS | `ShortTermBorrowings` | 短期借入金 |
| `current_portion_of_long_term_borrowings` | BS | `CurrentPortionOfLongTermBorrowings` | 1年内返済予定の長期借入金 |
| `commercial_papers` | BS | `CommercialPapers` | コマーシャル・ペーパー |
| `current_portion_of_bonds` | BS | `CurrentPortionOfBonds` | 1年内償還予定の社債 |
| `short_term_lease_obligations` | BS | `LeaseObligationsCL` | 流動リース債務 |
| `bonds_payable` | BS | `BondsPayable` | 社債 |
| `long_term_borrowings` | BS | `LongTermBorrowings` | 長期借入金 |
| `long_term_lease_obligations` | BS | `LeaseObligationsNCL` | 固定リース債務 |
| `lease_obligations` | BS | `LeaseObligations` | リース債務（流動/固定が分離されていない場合）|

IFRS企業では `Borrowings`（流動/非流動）、`LeaseLiabilities`（流動/非流動）からもマッピングする。

### 含めないデータ（レイヤー分離原則）

| データ | 分類 | 所属レイヤー |
|---|---|---|
| ROE / ROA / ROIC / マージン / 成長率 / CAGR | Derived（再計算可能） | valuation-engine |
| FCF (営業CF + 投資CF) | Derived（CF合算計算値） | valuation-engine |
| EPS (純利益 / 株数) | Derived（再計算可能） | valuation-engine |
| 有利子負債合計 | Derived（構成項目の合算） | valuation-engine |
| stock_price / volume | 市場Fact | market-dataset |
| PER / PBR / PSR / PEG / dividend_yield | Derived（再計算可能） | valuation-engine |

### 単位の扱い

XBRL の `decimals` 属性は**精度**を示すもので、単位変換には使用しない（XBRL仕様）。
単位は `unitRef` が指す unit 定義で決まる。EDINET の主要財務指標は円単位で統一されているため、
FactNormalizer は XBRL の値をそのまま使用する（スケーリングなし）。

### 出力前バリデーション

JSONExporter は出力前に以下を検証する：

1. metrics 内に Derived 指標が混入していないか → **エラー**
2. 全項目がnullでないか → **警告**

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
