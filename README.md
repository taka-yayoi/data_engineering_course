# データエンジニアリング実習

## フォルダ構造

```
data_engineering_course/
├── README.md                         # この説明ファイル
├── notebooks/                        # 直接実行するノートブック
│   ├── exercise_part1_imperative.py  # 演習1: 命令型ETL (PySpark)
│   └── exercise_part4_jobs.py        # 演習4: ジョブ設定（参考資料）
│
└── pipelines/                        # SQLリファレンス（コピペ用）
    ├── pipeline_basic.sql            # 演習2: 基本パイプライン
    └── pipeline_with_expectations.sql # 演習3: エクスペクテーション付き
```

---

## 演習1: PySparkによる命令型ETL（25分）

**ファイル**: `notebooks/exercise_part1_imperative.py`

### 実行方法
1. ノートブックを開く
2. **Run All** で実行

### 学習内容
- PySparkでのデータ読み込み・変換・保存
- Bronze → Silver → Gold のメダリオンアーキテクチャ
- カタログエクスプローラでのテーブル確認

---

## 演習2: Lakeflow SDP 宣言型パイプライン（25分）

**リファレンス**: `pipelines/pipeline_basic.sql`

### 実行方法

#### Step 1: パイプラインを作成
1. 左サイドバーで **新規** → **ETL パイプライン** を選択
2. パイプライン名を入力（例: `sdp_nyctaxi_pipeline`）
3. カタログ/スキーマを設定:
   - **カタログ**: `workspace`
   - **スキーマ**: 新規作成（例: `sdp_handson_<あなたの名前>`）
4. **空のファイルから開始** を選択
5. 言語は **SQL** を選択
6. **選択** をクリック

#### Step 2: SQLを入力
パイプラインエディタが開いたら、以下のSQLを入力:

```sql
-- Bronze層
CREATE MATERIALIZED VIEW bronze_trips AS
SELECT * FROM samples.nyctaxi.trips;

-- Silver層
CREATE MATERIALIZED VIEW silver_trips AS
SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip,
    DATE(tpep_pickup_datetime) AS pickup_date
FROM bronze_trips
WHERE fare_amount > 0
  AND trip_distance > 0;

-- Gold層
CREATE MATERIALIZED VIEW gold_daily_trips AS
SELECT 
    pickup_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM silver_trips
GROUP BY pickup_date
ORDER BY pickup_date;
```

#### Step 3: 実行
1. **パイプラインを実行** ボタンをクリック
2. 右側のDAG（依存関係グラフ）で進行状況を確認
3. 完了後、各テーブルをクリックしてデータをプレビュー

### 学習内容
- `CREATE MATERIALIZED VIEW` によるテーブル定義
- 依存関係の自動解決
- パイプラインエディタの使い方

---

## 演習3: エクスペクテーションの追加（15分）

**リファレンス**: `pipelines/pipeline_with_expectations.sql`

### 実行方法

#### 方法A: 既存パイプラインを編集
1. 演習2で作成したパイプラインを開く
2. Silver層の定義を以下に変更:

```sql
CREATE MATERIALIZED VIEW silver_trips (
    CONSTRAINT valid_fare EXPECT (fare_amount > 0) ON VIOLATION DROP ROW,
    CONSTRAINT valid_distance EXPECT (trip_distance > 0) ON VIOLATION DROP ROW,
    CONSTRAINT warn_high_fare EXPECT (fare_amount < 500)
) AS
SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip,
    DATE(tpep_pickup_datetime) AS pickup_date
FROM bronze_trips;
```

3. **パイプラインを実行** をクリック

#### Step 4: 品質メトリクスを確認
1. パイプライングラフで `silver_trips` をクリック
2. 下部パネルの **テーブル指標** タブを確認
3. エクスペクテーションの達成/未達成を確認

### エクスペクテーションの3つのモード

| モード | 構文 | 動作 |
|--------|------|------|
| 警告のみ | `EXPECT (条件)` | 警告を記録、処理は継続 |
| 行を除外 | `EXPECT (条件) ON VIOLATION DROP ROW` | 違反行を除外 |
| 失敗 | `EXPECT (条件) ON VIOLATION FAIL UPDATE` | パイプライン停止 |

---

## 演習4: Lakeflowジョブによる自動化（15分）

**参考資料**: `notebooks/exercise_part4_jobs.py`

### 実行方法

#### Step 1: ジョブを作成
1. 左メニューから **ワークフロー** を選択
2. **ジョブを作成** をクリック
3. タスク設定:
   - **タスク名**: `run_pipeline`
   - **タイプ**: `パイプライン`
   - **パイプライン**: 演習2または演習3のパイプラインを選択
4. ジョブ名を設定（画面上部の「名前のないジョブ...」をクリック）

#### Step 2: スケジュール設定（オプション）
1. 右側パネルの **スケジュールとトリガー** で **トリガーを追加**
2. **スケジュール済み** を選択し、頻度を設定（例: 毎日 06:00）
3. ⚠️ 演習後は **一時停止** にしておく

#### Step 3: 手動実行
1. **今すぐ実行** をクリック
2. **実行** タブで実行状況を確認

---

## 演習のポイント

### 命令型 (演習1) vs 宣言型 (演習2-3) の違い

| 観点 | 命令型 (PySpark) | 宣言型 (SQL) |
|------|-----------------|--------------|
| 記述方法 | `df.filter().write.saveAsTable()` | `CREATE MATERIALIZED VIEW AS SELECT` |
| コード量 | 多い | 少ない |
| 依存関係 | 手動で実行順序を管理 | 自動で解決 |
| 品質チェック | `.filter()` で自前実装 | `EXPECT` で宣言 |
| 除外件数 | 自分で計算・ログ出力 | 自動でメトリクス記録 |

---

## トラブルシューティング

### パイプラインが見つからない
- カタログ / スキーマ の設定を確認してください

### テーブルが作成されない
- パイプラインが正常に完了しているか確認（緑色のチェックマーク）
- エラーメッセージがあれば内容を確認

### エクスペクテーションが表示されない
- パイプラインを再実行してください
- テーブル指標タブを確認してください

---

## 参考リンク

- [Lakeflow SDP入門：基礎から実践まで](https://qiita.com/taka_yayoi/items/e15caec3c71a27aa12b1)
- [SQLだけで始めるLakeflow SDP](https://qiita.com/taka_yayoi/items/e6368446040c9e979d0f)
- [Lakeflow SDPでデータ品質を守るエクスペクテーション](https://qiita.com/taka_yayoi/items/0b525cb05a095ad0bbe1)
