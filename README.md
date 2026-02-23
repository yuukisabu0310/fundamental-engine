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

## アーキテクチャ概要

### YAML駆動マッピング

タグ→canonical keyの変換ルール、Factキー定義、優先順位解決ルールは全て YAML 設定ファイルで管理。
コード内にXBRLタグ名、会計基準名、業種判定等のハードコードは一切存在しない。

| ファイル | 用途 |
|---|---|
| `config/taxonomy_mapping.yaml` | XBRL タグ → canonical key のマッピング定義（JGAAP/IFRS統合） |
| `config/canonical_keys.yaml` | Fact/Derived キーの定義、優先順位解決ルール、会計基準正規化マッピング |

### データフロー

```
EDINET API
    │
    ▼
┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌────────────────┐
│ XBRLParser│───▶│ContextResolver│───▶│FactNormalizer│───▶│FinancialMaster │
│ (Extractor)│    │  (Resolver)   │    │ (Normalizer) │    │  (Classifier)  │
└──────────┘    └───────────────┘    └──────────────┘    └────────────────┘
                                          ↑                       │
                                 taxonomy_mapping.yaml    canonical_keys.yaml
                                                                  │
                                                                  ▼
                                                          ┌────────────┐
                                                          │ JSONExporter│
                                                          └────────────┘
                                                                  │
                                                                  ▼
                                              financial-dataset/{report_type}/{data_version}/{code}.json
```

### レイヤー責務

| レイヤー | モジュール | 責務 |
|---|---|---|
| Document Layer | `XBRLParser` | 生XBRL → 構造化fact抽出 |
| Context Layer | `ContextResolver` | contextRef → 期間情報マップ構築 |
| Normalization Layer | `FactNormalizer` | タグ→canonical key変換、current/prior分類、xsi:nil処理 |
| Integration Layer | `FinancialMaster` | PL/BS/CF/配当の統合、resolution rule適用 |
| Output Layer | `JSONExporter` | financial-dataset へのJSON永続化 |

### xsi:nil 処理方針

- XBRLの `xsi:nil="true"` を `XBRLParser` で検出し `is_nil` フラグを付与
- `FactNormalizer` は nil fact を `None` として格納するが、同一 canonical key の低優先タグへのフォールバックを**抑止**
- nil は「値がない」の積極的表明であり、別タグで代替すべきでない

### BS本表アンカー方式

`FactNormalizer._extract_bs()` は BS 抽出時にアンカー方式を採用:

1. duration 由来の `current_year_end` で `total_assets` 取得を試行
2. 取得できない場合、アンカータグ (`TotalAssets`, `LiabilitiesAndNetAssets`, `NetAssets`) の実際の instant 日付を検出
3. 検出した日付で BS 全項目を再試行し、マージ

変則決算期・投資法人等で duration end_date と BS instant 日付がずれるケースに対応。

## NULL分類定義

`null` 値は以下の4種類に分類される。

| 分類 | 意味 | 例 |
|---|---|---|
| 経済実態NULL | 企業にその項目が存在しない | 無借金企業の `short_term_borrowings` |
| 会計基準差NULL | 会計基準上その概念が存在しない | IFRSの `ordinary_income`（経常利益） |
| 空値NULL | XBRLにタグが存在するが `xsi:nil="true"` または空値 | 無配企業の `dividends_per_share` |
| 取得失敗NULL | XBRLにタグ・値が存在するが抽出できていない | `taxonomy_mapping.yaml` 未対応タグ |

設計方針: 経済実態NULL・会計基準差NULL・空値NULL は正常、取得失敗NULL のみが改善対象。

NULL分類は**日付認識**で判定。当期コンテキスト (`current_year_end`) に存在するタグのみ対象とし、前期・前々期にのみ存在するタグを「取得失敗」に誤分類しない。

### 取得失敗率

| バージョン | 取得失敗率 | 主な改善内容 |
|---|---|---|
| 初回（3分類） | 7.2% | taxonomy_mapping 初版 |
| ShortTermLoansPayable追加 | 5.6% | BS負債タグ追加、xsi:nil 4分類化 |
| 日付認識 + BSアンカー | **2.8%** | classify スクリプト日付認識化、BS本表アンカー方式導入 |

検証コマンド: `python scripts/analysis/classify_null_reasons.py`

## ディレクトリ構造

```
fundamental-engine/
├── main.py                          # エントリーポイント（EDINET取得）
├── config/
│   ├── taxonomy_mapping.yaml        # XBRL タグ → canonical key マッピング
│   ├── canonical_keys.yaml          # Fact/Derived キー定義・解決ルール
│   └── settings.yaml.example        # 設定テンプレート
├── src/
│   ├── __init__.py                  # バージョン定義
│   ├── config_loader.py             # YAML設定ローダー
│   ├── constants.py                 # パイプライン定数
│   ├── utils.py                     # 共通ユーティリティ
│   ├── edinet_client.py             # EDINET API クライアント
│   ├── downloader.py                # ZIP ダウンローダー
│   ├── extractor.py                 # ZIP 展開
│   ├── main.py                      # ダウンロードパイプライン
│   ├── parser/
│   │   ├── xbrl_parser.py           # XBRL パーサー（生fact抽出）
│   │   └── context_resolver.py      # context_map 構築
│   ├── normalizer/
│   │   └── fact_normalizer.py       # タグ→canonical key正規化
│   ├── financial/
│   │   └── financial_master.py      # Fact統合・resolution適用
│   └── output/
│       ├── json_exporter.py         # JSON出力
│       └── manifest_generator.py    # dataset_manifest.json 生成
├── scripts/
│   ├── process_all.py               # 全XBRL一括処理パイプライン
│   ├── analysis/                    # 分析・検証スクリプト
│   │   ├── _pipeline.py             # 分析共通ユーティリティ
│   │   ├── classify_null_reasons.py # NULL理由4分類レポート
│   │   ├── verify_fact_lake.py      # FACTレイク設計整合性検証
│   │   └── verify_targets_detail.py # 対象銘柄詳細検証
│   └── tests/                       # 動作確認スクリプト
│       ├── test_parse.py            # XBRLParser テスト
│       ├── test_context.py          # ContextResolver テスト
│       ├── test_normalize.py        # FactNormalizer テスト
│       ├── test_financial_master.py # FinancialMaster テスト
│       ├── test_json_export.py      # JSONExporter テスト
│       ├── test_data_version.py     # data_version テスト
│       └── test_manifest.py         # ManifestGenerator テスト
├── data/
│   └── edinet/
│       ├── raw_zip/                 # ダウンロード済みZIP
│       └── raw_xbrl/               # 展開済みXBRL
└── financial-dataset/               # 出力データレイク
    ├── annual/{YYYY}FY/             # 年次データ
    └── metadata/                    # dataset_manifest.json
```

## 設計方針

- **YAML駆動アーキテクチャ**: タクソノミマッピング、Factキー定義、優先順位解決ルール等を全てYAML設定ファイルで管理
- **データソース非依存**: EDINET固有のロジックをコアに混入させない
- **FactとDerivedの分離**: financial-datasetには確定決算の財務Factのみを保存
- **JGAAP/IFRS統合マッピング**: 会計基準ごとのXBRLタグ差異を吸収し、統一されたcanonical keyに変換
- **レイヤー責務の厳守**: 様式依存分岐・業種依存分岐・会計基準条件分岐なし
- **再計算可能な値は保存しない**: EPS、PER/PBR等はvaluation-engineの責務

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

`config/settings.yaml.example` を `config/settings.yaml` にコピーして、取得期間を設定：

```yaml
start_date: "2021-01-01"
end_date: "2024-12-31"
sleep_seconds: 0.2
```

- 日付が未設定の場合は JST の本日で取得
- APIキーは `.env` または環境変数 `EDINET_API_KEY` から読み込み

## 実行方法

### データ取得

```bash
python main.py
```

### 全XBRL一括処理

```bash
python scripts/process_all.py
```

### NULL分類レポート

```bash
python scripts/analysis/classify_null_reasons.py
```

### FACTレイク検証

```bash
python scripts/analysis/verify_fact_lake.py
```

### 動作確認テスト

```bash
python scripts/tests/test_normalize.py
python scripts/tests/test_financial_master.py
python scripts/tests/test_json_export.py
```

## JSON出力仕様（schema_version 1.0）

financial-dataset には**財務Factのみ**を保存する。

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
    "period": { "start": "2024-12-01", "end": "2025-11-30" },
    "metrics": {
      "total_assets": 218345000000.0,
      "equity": 81630000000.0,
      "net_sales": 251533000000.0,
      "...": "..."
    }
  }
}
```

### 出力ルール

- **Factのみ**: 財務諸表に記載された数値のみ出力
- **null許容**: 値が取得できなかった項目は `null`（キーは常に存在）
- **xsi:nil フォールバック抑止**: nil fact は同一キーの低優先タグへフォールバックしない
- **空prior_year省略**: prior_yearに有効Factがなければキー自体を出力しない
- **Derived禁止**: ROE/ROA/ROIC/マージン/成長率/FCF/CAGR等は valuation-engine の責務
- **security_code正規化**: 5桁末尾"0"のみ末尾削除
- **会計定義明示**: consolidation_type / accounting_standard を必ず出力
- **period保持**: 変則決算・IFRS中間期に対応

### Fact項目一覧

#### 基礎財務項目

| キー | 出典 | 説明 |
|---|---|---|
| `total_assets` | BS | 総資産 |
| `equity` | BS | 自己資本（resolution rule で優先順位解決） |
| `net_sales` | PL | 売上高 |
| `operating_income` | PL | 営業利益 |
| `ordinary_income` | PL | 経常利益（IFRSには概念なし → null） |
| `net_income_attributable_to_parent` | PL | 親会社株主帰属当期純利益 |
| `total_number_of_issued_shares` | DEI | 発行済株式数 |

#### 分析用追加項目

| キー | 出典 | 説明 |
|---|---|---|
| `cash_and_equivalents` | CF/BS | 現金及び現金同等物 |
| `operating_cash_flow` | CF | 営業キャッシュ・フロー |
| `depreciation` | CF | 減価償却費 |
| `dividends_per_share` | DEI | 1株当たり配当額 |

#### 有利子負債構成項目

| キー | 出典 | 説明 |
|---|---|---|
| `short_term_borrowings` | BS | 短期借入金 |
| `current_portion_of_long_term_borrowings` | BS | 1年内返済予定の長期借入金 |
| `commercial_papers` | BS | コマーシャル・ペーパー |
| `current_portion_of_bonds` | BS | 1年内償還予定の社債 |
| `short_term_lease_obligations` | BS | 流動リース債務 |
| `bonds_payable` | BS | 社債 |
| `long_term_borrowings` | BS | 長期借入金 |
| `long_term_lease_obligations` | BS | 固定リース債務 |
| `lease_obligations` | BS | リース債務（CL/NCL未分割） |

タグマッピングの詳細は `config/taxonomy_mapping.yaml` を参照。

### 含めないデータ（レイヤー分離原則）

| データ | 分類 | 所属レイヤー |
|---|---|---|
| ROE / ROA / ROIC / マージン / 成長率 / CAGR | Derived | valuation-engine |
| FCF / EPS / 有利子負債合計 | Derived | valuation-engine |
| stock_price / volume | 市場Fact | market-dataset |
| PER / PBR / PSR / PEG / dividend_yield | Derived | valuation-engine |

## 取得対象条件

| docTypeCode | 書類種別 |
|---|---|
| `120` | 有価証券報告書 |
| `130` | 半期報告書 |
| `140` | 四半期報告書 |

## エラーハンドリング

- HTTPエラー時は3回リトライ
- 失敗時はログ出力して処理継続
- ZIPが既に存在すればスキップ
- 解凍済フォルダがあればスキップ

## 単位の扱い

XBRLの `decimals` 属性は精度を示すもので、単位変換には使用しない（XBRL仕様）。
EDINETの主要財務指標は円単位で統一されているため、値をそのまま使用する。
