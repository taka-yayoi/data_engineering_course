# Databricks notebook source
# MAGIC %md
# MAGIC # 演習4: Lakeflowジョブによる自動化（15分）
# MAGIC
# MAGIC この演習では、作成したパイプラインを **ジョブ** として自動実行する設定を行います。
# MAGIC
# MAGIC ## ⚠️ この演習について
# MAGIC
# MAGIC この演習は **GUI操作** が中心です。このノートブックは参考資料として使用してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ジョブとは？
# MAGIC
# MAGIC **ジョブ** = パイプラインやノートブックを自動実行する仕組み
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |------|------|
# MAGIC | スケジュール実行 | 毎日、毎時など定期的に実行 |
# MAGIC | トリガー実行 | ファイル到着やテーブル更新時に実行 |
# MAGIC | 依存関係 | 複数タスクの順序制御 |
# MAGIC | 通知 | 成功/失敗時にメール通知 |
# MAGIC | リトライ | 失敗時の自動再実行 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ジョブの作成手順
# MAGIC
# MAGIC ### Step 1: ジョブ画面を開く
# MAGIC
# MAGIC 左メニューから **ジョブとパイプライン** を選択し、**作成 > ジョブ** をクリック
# MAGIC
# MAGIC ### Step 2: タスクの設定
# MAGIC
# MAGIC **別のタスクタイプを追加**をクリックし**ETLパイプライン**を選択
# MAGIC
# MAGIC | 設定項目 | 値 |
# MAGIC |---------|-----|
# MAGIC | タスク名 | `run_pipeline` |
# MAGIC | 種類 | `パイプライン` |
# MAGIC | パイプライン | 演習2または演習3で作成したパイプラインを選択 |
# MAGIC
# MAGIC ![](img/job_1.png)
# MAGIC
# MAGIC **タスクを作成**をクリック
# MAGIC
# MAGIC ### Step 3: ジョブ名の設定
# MAGIC
# MAGIC 画面上部の「新規ジョブ」をクリックして、ジョブ名を設定

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. スケジュールの設定
# MAGIC
# MAGIC ### Step 1: スケジュール追加
# MAGIC
# MAGIC 右側パネルの **スケジュールとトリガー** セクションで **トリガーを追加** をクリック。トリガータイプで**スケジュール済み**を選択
# MAGIC
# MAGIC ![](img/job_2.png)
# MAGIC
# MAGIC ### Step 2: スケジュール内容（例）
# MAGIC
# MAGIC | 設定項目 | 例 |
# MAGIC |---------|-----|
# MAGIC | トリガータイプ | スケジュール済み |
# MAGIC | スケジュールのタイプ | Advanced |
# MAGIC | 間隔 | 1 日 |
# MAGIC | 予定時刻 | 16:00 |
# MAGIC | タイムゾーン | Asia/Tokyo |
# MAGIC
# MAGIC ![](img/job_3.png)
# MAGIC
# MAGIC ⚠️ **演習後は**: スケジュールは **一時停止** しておきましょう（無駄なリソース使用を防ぐため）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 手動実行と結果確認
# MAGIC
# MAGIC ### 手動実行
# MAGIC 1. ジョブ画面で **今すぐ実行** をクリック
# MAGIC
# MAGIC ### 実行結果の確認
# MAGIC 1. **ジョブの実行** タブで実行履歴を確認
# MAGIC 2. 各実行の状態:
# MAGIC    - ✅ **成功**
# MAGIC    - ❌ **失敗**
# MAGIC    - 🔄 **実行中**
# MAGIC
# MAGIC ![](img/job_4.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. パイプラインからのスケジュール設定（別の方法）
# MAGIC
# MAGIC ジョブを作成せずに、パイプラインから直接スケジュールを設定することもできます。
# MAGIC
# MAGIC ### 手順
# MAGIC 1. パイプラインエディタを開く
# MAGIC 2. 上部の **スケジュール** ボタンをクリック
# MAGIC 3. 実行頻度を設定（例: 毎日午前6時）
# MAGIC 4. **作成** をクリック
# MAGIC
# MAGIC この方法は、単一のパイプラインを定期実行したい場合にシンプルです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. まとめ
# MAGIC
# MAGIC ### 構成図
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────┐
# MAGIC │           Lakeflow Job              │
# MAGIC │                                     │
# MAGIC │  ┌─────────────────────────────┐   │
# MAGIC │  │      Lakeflow Pipeline       │   │
# MAGIC │  │                              │   │
# MAGIC │  │  Bronze → Silver → Gold      │   │
# MAGIC │  │    MV      MV       MV       │   │
# MAGIC │  │          (with expectations) │   │
# MAGIC │  └─────────────────────────────┘   │
# MAGIC │                                     │
# MAGIC │  Schedule: Daily at 06:00 (Paused)  │
# MAGIC └─────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 本番運用に向けて
# MAGIC
# MAGIC | 項目 | 推奨設定 |
# MAGIC |------|---------|
# MAGIC | 実行モード | Production |
# MAGIC | リトライ | 2-3回 |
# MAGIC | 通知 | 失敗時にアラート |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 おつかれさまでした！
# MAGIC
# MAGIC 今日の演習で学んだこと:
# MAGIC
# MAGIC | 演習 | 内容 | ポイント |
# MAGIC |------|------|---------|
# MAGIC | 演習1 | 命令型ETL (PySpark) | `df.filter().write.saveAsTable()` |
# MAGIC | 演習2 | 宣言型パイプライン (SQL) | `CREATE MATERIALIZED VIEW ... AS SELECT` |
# MAGIC | 演習3 | エクスペクテーション | `EXPECT (条件) ON VIOLATION DROP ROW` |
# MAGIC | 演習4 | ジョブによる自動化 | スケジュール実行、監視 |
# MAGIC
# MAGIC ### 命令型 vs 宣言型
# MAGIC
# MAGIC | 観点 | 命令型 (演習1) | 宣言型 (演習2-3) |
# MAGIC |------|---------------|----------------|
# MAGIC | 言語 | Python | SQL |
# MAGIC | コード量 | 多い | 少ない |
# MAGIC | 依存関係 | 手動管理 | 自動解決 |
# MAGIC | 品質チェック | 自前実装 | エクスペクテーション |
# MAGIC | 差分処理 | 自前実装 | ストリーミングテーブル（次のステップ） |
# MAGIC
