# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 デモ: Lakeflow パイプラインエディターによるパイプライン作成
# MAGIC 
# MAGIC このノートブックでは、新しいLakeflowパイプラインエディターを使用したETLパイプラインの作成方法を解説します。
# MAGIC 
# MAGIC **参考ドキュメント**: [LakeflowパイプラインエディターによるETLパイプラインの開発とデバッグ](https://docs.databricks.com/aws/ja/ldp/multi-file-editor)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ETLパイプラインの作成
# MAGIC 
# MAGIC ### Step 1: 新しいETLパイプラインを作成
# MAGIC 
# MAGIC 1. サイドバー上部の **+ New** をクリック
# MAGIC 2. **ETL パイプライン** を選択
# MAGIC 
# MAGIC パイプラインが自動的に作成され、**Lakeflowパイプラインエディター**が開きます。
# MAGIC 
# MAGIC ### Step 2: パイプラインの基本設定
# MAGIC 
# MAGIC 1. 上部のパイプライン名を `part2_demo_pipeline` に変更
# MAGIC 2. 名前の横にある **カタログとスキーマ** をクリック
# MAGIC    - カタログ: `workspace`
# MAGIC    - スキーマ: `part2_demo`(新規作成される)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. パイプラインエディターのUI概要
# MAGIC 
# MAGIC Lakeflowパイプラインエディターは、パイプライン開発専用のIDEです。
# MAGIC 
# MAGIC ### 主要コンポーネント
# MAGIC 
# MAGIC | コンポーネント | 説明 |
# MAGIC |--------------|------|
# MAGIC | パイプラインアセットブラウザ(左) | ファイル/フォルダの管理、設定へのアクセス |
# MAGIC | コードエディター(中央) | SQLまたはPythonファイルの編集 |
# MAGIC | パイプライングラフ(下部) | テーブル間の依存関係(DAG)を可視化 |
# MAGIC | 問題パネル(下部) | エラー・警告の一覧表示 |
# MAGIC 
# MAGIC ### デフォルトのフォルダ構造
# MAGIC 
# MAGIC | フォルダ | 用途 |
# MAGIC |--------|------|
# MAGIC | `transformations` | パイプラインのソースコード(MV/ST定義) |
# MAGIC | `explorations` | 探索的分析用ノートブック(実行時には評価されない) |
# MAGIC | `utilities` | 再利用可能なPythonモジュール |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ソースコードの作成
# MAGIC 
# MAGIC ### Step 1: 変換ファイルの作成
# MAGIC 
# MAGIC 1. デフォルトで作成される `my_transformation` ファイルを使用
# MAGIC 2. 言語ドロップダウンで **SQL** を選択
# MAGIC 3. 以下の3つの方法でコードを追加可能:
# MAGIC    - **手動入力**: 直接SQLを記述
# MAGIC    - **Genie Codeで作成**: 自然言語で指示(例: 「NYCタクシーデータを読み込んで距離が0より大きいレコードだけ抽出して」)
# MAGIC    - **サンプルコードを使用**: テンプレートから開始
# MAGIC 
# MAGIC ### Step 2: Bronze層の定義
# MAGIC 
# MAGIC ```sql
# MAGIC -- Bronze層: 生データの取り込み
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW bronze_trips
# MAGIC COMMENT 'NYCタクシー生データ'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips;
# MAGIC ```
# MAGIC 
# MAGIC ### Step 3: Silver層の定義(データ品質チェック付き)
# MAGIC 
# MAGIC ```sql
# MAGIC -- Silver層: クレンジング済みデータ
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_trips (
# MAGIC   CONSTRAINT valid_distance EXPECT (trip_distance > 0) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_fare EXPECT (fare_amount >= 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'クレンジング済みタクシーデータ'
# MAGIC AS
# MAGIC SELECT
# MAGIC   tpep_pickup_datetime AS pickup_datetime,
# MAGIC   tpep_dropoff_datetime AS dropoff_datetime,
# MAGIC   trip_distance,
# MAGIC   fare_amount,
# MAGIC   pickup_zip,
# MAGIC   dropoff_zip
# MAGIC FROM bronze_trips
# MAGIC WHERE trip_distance IS NOT NULL;
# MAGIC ```
# MAGIC 
# MAGIC ### Step 4: Gold層の定義
# MAGIC 
# MAGIC ```sql
# MAGIC -- Gold層: ビジネス向け集計
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily_stats
# MAGIC COMMENT '日次統計サマリー'
# MAGIC AS
# MAGIC SELECT
# MAGIC   DATE(pickup_datetime) AS trip_date,
# MAGIC   COUNT(*) AS total_trips,
# MAGIC   ROUND(AVG(trip_distance), 2) AS avg_distance,
# MAGIC   ROUND(AVG(fare_amount), 2) AS avg_fare,
# MAGIC   ROUND(SUM(fare_amount), 2) AS total_revenue
# MAGIC FROM silver_trips
# MAGIC GROUP BY DATE(pickup_datetime)
# MAGIC ORDER BY trip_date;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. パイプラインの実行
# MAGIC 
# MAGIC ### 実行オプション
# MAGIC 
# MAGIC | アクション | 説明 | 用途 |
# MAGIC |----------|------|------|
# MAGIC | **パイプライン** | 全ファイルの全テーブルを更新 | 本番実行 |
# MAGIC | **ファイルの実行** | 現在のファイル内のテーブルのみ更新 | 開発・デバッグ |
# MAGIC | **テーブルの更新** | 単一テーブルのみ更新 | 個別テスト |
# MAGIC | **ドライラン** | データ更新なしで検証のみ | 構文チェック |
# MAGIC 
# MAGIC ### 実行手順
# MAGIC 
# MAGIC 1. コードを入力したら、まず **ドライラン** で構文確認
# MAGIC 2. 問題がなければ **パイプライン** をクリックして実行
# MAGIC 3. 下部の **パイプライングラフ** タブでDAGと進捗を確認

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 結果の確認
# MAGIC 
# MAGIC ### パイプライングラフでの確認
# MAGIC 
# MAGIC 1. 下部パネルの **パイプライングラフ** タブをクリック
# MAGIC 2. 各ノード(テーブル)の状態を確認:
# MAGIC    - 緑: 成功
# MAGIC    - 黄: 実行中
# MAGIC    - 赤: エラー
# MAGIC 3. ノードをクリックすると **データプレビュー** が表示される
# MAGIC 
# MAGIC ### データ品質(Expectations)の確認
# MAGIC 
# MAGIC 1. `silver_trips` ノードをクリック
# MAGIC 2. **データ品質** タブで以下を確認:
# MAGIC    - 各制約のPass/Fail件数
# MAGIC    - Dropされたレコード数

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. エラーのトラブルシューティング
# MAGIC 
# MAGIC ### 問題パネルの活用
# MAGIC 
# MAGIC 1. エラー発生時、下部の **問題パネル** にエラー一覧が表示
# MAGIC 2. エラーをクリックすると該当コードにジャンプ
# MAGIC 3. **エラーの診断** をクリックすると Genie Code がデバッグを支援
# MAGIC 
# MAGIC ### よくあるエラー
# MAGIC 
# MAGIC | エラー | 原因 | 対処 |
# MAGIC |-------|------|------|
# MAGIC | テーブルが見つからない | 上流テーブル未作成 | 先に上流テーブルを実行 |
# MAGIC | 構文エラー | SQLの誤り | ドライランで事前確認 |
# MAGIC | 権限エラー | カタログへのアクセス権なし | 管理者に確認 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Part 1(命令型)との比較
# MAGIC 
# MAGIC | 項目 | 命令型(Part 1) | 宣言型SDP(Part 2) |
# MAGIC |------|----------------|-------------------|
# MAGIC | 開発環境 | ノートブック | パイプラインエディター(IDE) |
# MAGIC | 言語 | PySpark | SQL または Python |
# MAGIC | コード量 | 多い(処理順序も記述) | 少ない(何が欲しいかだけ) |
# MAGIC | 依存関係 | 手動で管理 | 自動解決(DAG生成) |
# MAGIC | データ品質 | 手動でfilter() | CONSTRAINT EXPECTで宣言 |
# MAGIC | デバッグ | セル単位で実行 | テーブル/ファイル単位で実行 |
# MAGIC | 可視化 | なし | パイプライングラフ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 便利な機能
# MAGIC 
# MAGIC ### Genie Code(AIアシスタント)
# MAGIC 
# MAGIC 自然言語でパイプラインを生成・編集できます。
# MAGIC 
# MAGIC **使用例**:
# MAGIC - 「NYCタクシーデータからピックアップ地点ごとの集計を作成して」
# MAGIC - 「このテーブルにfare_amountが負の値を除外する制約を追加して」
# MAGIC - 「エラーを修正して」
# MAGIC 
# MAGIC ### 選択コードの実行
# MAGIC 
# MAGIC SQLコードの一部を選択して **選択したコードを実行** をクリックすると、データを実体化せずにクエリ結果をプレビューできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 作成されたテーブルの確認
# MAGIC 
# MAGIC パイプライン実行後、以下のテーブルが作成されます。
# MAGIC 
# MAGIC **カタログエクスプローラーで確認:**
# MAGIC 1. 左サイドバーの **カタログ** をクリック
# MAGIC 2. `workspace` → `part2_demo` を展開
# MAGIC 3. 各テーブルをクリックしてデータを確認
# MAGIC 
# MAGIC **ノートブックから確認(以下のセルを実行):**

# COMMAND ----------

# パイプライン実行後にこのセルを実行して確認
# spark.sql("SELECT * FROM workspace.part2_demo.bronze_trips LIMIT 5").display()

# COMMAND ----------

# spark.sql("SELECT * FROM workspace.part2_demo.silver_trips LIMIT 5").display()

# COMMAND ----------

# spark.sql("SELECT * FROM workspace.part2_demo.gold_daily_stats").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC 
# MAGIC ### 新しいパイプラインエディターの利点
# MAGIC 
# MAGIC 1. **統合開発環境(IDE)** - ファイル管理、編集、実行、デバッグが一画面で完結
# MAGIC 2. **パイプライングラフ** - 依存関係を視覚的に確認
# MAGIC 3. **選択的実行** - ファイル/テーブル単位でのテスト実行が可能
# MAGIC 4. **Genie Code** - 自然言語でパイプラインを生成・デバッグ
# MAGIC 5. **データプレビュー** - 各テーブルのデータをその場で確認
# MAGIC 
# MAGIC ### パイプラインの使い分け
# MAGIC 
# MAGIC | ユースケース | 推奨アプローチ |
# MAGIC |-------------|---------------|
# MAGIC | 定型ETL処理 | 宣言型SDP |
# MAGIC | データ品質管理が重要 | 宣言型SDP(Expectations) |
# MAGIC | 複雑なビジネスロジック | 命令型(PySpark) |
# MAGIC | 探索的データ分析 | 命令型(ノートブック) |

