# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 デモ: Lakeflow SDPパイプライン作成手順
# MAGIC 
# MAGIC このノートブックでは、SQL版Lakeflow SDPパイプラインの作成方法と、Part 1(命令型)との違いを解説します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パイプラインの作成手順
# MAGIC 
# MAGIC ### Step 1: パイプラインを作成
# MAGIC 
# MAGIC 1. 左サイドバーの **データエンジニアリング** をクリック
# MAGIC 2. **パイプライン** タブを選択
# MAGIC 3. **パイプラインを作成** をクリック
# MAGIC 4. 以下を設定：
# MAGIC    - **パイプライン名**: `part2_demo_pipeline`
# MAGIC    - **ソースコード**: `Part2_Demo_SDP_Pipeline.sql` を選択
# MAGIC    - **送信先**:
# MAGIC      - カタログ: `workspace`
# MAGIC      - ターゲットスキーマ: `part2_demo`(新規作成される)
# MAGIC 
# MAGIC ### Step 2: パイプラインを実行
# MAGIC 
# MAGIC 1. **開始** ボタンをクリック
# MAGIC 2. DAG(依存関係グラフ)が自動生成されることを確認
# MAGIC 3. 各テーブルの処理状況を確認

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL版SDPの構文
# MAGIC 
# MAGIC ### テーブル定義
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW テーブル名
# MAGIC COMMENT 'テーブルの説明'
# MAGIC AS
# MAGIC SELECT ... FROM ...;
# MAGIC ```
# MAGIC 
# MAGIC ### データ品質制約 (Expectations)
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW テーブル名 (
# MAGIC   CONSTRAINT 制約名 EXPECT (条件) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS
# MAGIC SELECT ...;
# MAGIC ```
# MAGIC 
# MAGIC ### 違反時のアクション
# MAGIC | アクション | 説明 |
# MAGIC |-----------|------|
# MAGIC | `DROP ROW` | 違反行を除外 |
# MAGIC | `FAIL UPDATE` | パイプライン全体を失敗させる |
# MAGIC | (なし) | 警告のみ、データは通過 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 命令型との比較
# MAGIC 
# MAGIC | 項目 | 命令型(Part 1) | 宣言型SDP(Part 2) |
# MAGIC |------|----------------|-------------------|
# MAGIC | 言語 | PySpark | SQL |
# MAGIC | コード量 | 多い(処理順序も記述) | 少ない(何が欲しいかだけ) |
# MAGIC | 依存関係 | 手動で管理 | 自動解決(DAG生成) |
# MAGIC | データ品質 | 手動でfilter() | CONSTRAINT EXPECTで宣言 |
# MAGIC | エラー処理 | try-except必要 | 自動リトライ |
# MAGIC | インクリメンタル | 自分で実装 | 自動 |
# MAGIC | 実行環境 | ノートブック | パイプライン専用環境 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Expectations(データ品質)の確認
# MAGIC 
# MAGIC パイプライン実行後、UIで以下を確認できます：
# MAGIC 
# MAGIC 1. パイプライン画面で対象テーブルをクリック
# MAGIC 2. **データ品質** タブを選択
# MAGIC 3. 確認できる情報：
# MAGIC    - 各Expectationの **Pass/Fail** 件数
# MAGIC    - Dropされたレコード数
# MAGIC    - データ品質の推移グラフ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 作成されたテーブルの確認
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
# MAGIC ## 6. Part 1との件数比較
# MAGIC 
# MAGIC Part 1で作成したテーブルと比較してみましょう。
# MAGIC 
# MAGIC - Bronze: 同じ件数(全データ取り込み)
# MAGIC - Silver: SDPの方が少ない(Expectationsで不正データを除外)

# COMMAND ----------

# Part 1 と Part 2 の比較(両方実行後に確認)
# print("=== Part 1 (命令型) ===")
# print(f"Bronze: {spark.table('workspace.part1_demo.bronze_trips').count():,} 件")
# print(f"Silver: {spark.table('workspace.part1_demo.silver_trips').count():,} 件")
# 
# print("\n=== Part 2 (宣言型SDP) ===")
# print(f"Bronze: {spark.table('workspace.part2_demo.bronze_trips').count():,} 件")
# print(f"Silver: {spark.table('workspace.part2_demo.silver_trips').count():,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC 
# MAGIC ### 宣言型パイプライン(SDP)の利点
# MAGIC 
# MAGIC 1. **コードがシンプル** - 「何が欲しいか」だけ書く
# MAGIC 2. **依存関係の自動解決** - DAGが自動生成される
# MAGIC 3. **データ品質管理** - Expectationsで品質チェック
# MAGIC 4. **インクリメンタル処理** - 差分のみ自動処理
# MAGIC 5. **エラー処理** - 自動リトライ、ロールバック
# MAGIC 
# MAGIC ### いつ宣言型を使うか
# MAGIC 
# MAGIC - 定型的なETLパイプライン
# MAGIC - データ品質管理が重要な場合
# MAGIC - 運用の自動化が必要な場合
# MAGIC 
# MAGIC ### いつ命令型を使うか
# MAGIC 
# MAGIC - 複雑なビジネスロジック
# MAGIC - 細かい制御が必要な場合
# MAGIC - 探索的なデータ分析
