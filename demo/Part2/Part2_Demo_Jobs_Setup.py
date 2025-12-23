# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 デモ: ジョブによるパイプラインの自動実行
# MAGIC 
# MAGIC このノートブックでは、SDPパイプラインをジョブとしてスケジュール実行する方法を解説します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ジョブの作成手順
# MAGIC 
# MAGIC ### Step 1: ジョブ作成画面を開く
# MAGIC 
# MAGIC 1. 左サイドバーの **ワークフロー** をクリック
# MAGIC 2. **ジョブを作成** をクリック
# MAGIC 
# MAGIC ### Step 2: 基本設定
# MAGIC 
# MAGIC 1. **ジョブ名**: `part2_daily_pipeline`(クリックして編集)
# MAGIC 2. **タスク名**: `run_pipeline`
# MAGIC 3. **タイプ**: `パイプライン` を選択
# MAGIC 4. **パイプライン**: `part2_demo_pipeline` を選択

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. スケジュールの設定
# MAGIC 
# MAGIC ### Step 1: スケジュールを追加
# MAGIC 
# MAGIC 1. 右側パネルの **スケジュールとトリガー** セクションを展開
# MAGIC 2. **トリガーを追加** をクリック
# MAGIC 3. **スケジュール済み** を選択
# MAGIC 
# MAGIC ### Step 2: 実行タイミングを設定
# MAGIC 
# MAGIC | 設定項目 | 例 |
# MAGIC |---------|-----|
# MAGIC | 頻度 | 毎日 |
# MAGIC | 時刻 | 06:00 |
# MAGIC | タイムゾーン | Asia/Tokyo |
# MAGIC 
# MAGIC ### Cron式での設定例
# MAGIC 
# MAGIC | パターン | Cron式 |
# MAGIC |---------|--------|
# MAGIC | 毎日6時 | `0 6 * * *` |
# MAGIC | 平日9時 | `0 9 * * 1-5` |
# MAGIC | 毎時 | `0 * * * *` |
# MAGIC | 5分ごと | `*/5 * * * *` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 通知の設定
# MAGIC 
# MAGIC ### Step 1: 通知を追加
# MAGIC 
# MAGIC 1. 右側パネルの **通知** セクションを展開
# MAGIC 2. **通知先を追加** をクリック
# MAGIC 
# MAGIC ### Step 2: 通知条件を設定
# MAGIC 
# MAGIC | 条件 | 説明 |
# MAGIC |------|------|
# MAGIC | 開始 | ジョブ開始時に通知 |
# MAGIC | 成功 | 正常完了時に通知 |
# MAGIC | 失敗 | エラー発生時に通知 |
# MAGIC | スキップ | スキップ時に通知 |
# MAGIC 
# MAGIC ### 通知先
# MAGIC 
# MAGIC - メールアドレス
# MAGIC - Slackチャンネル(Webhook設定が必要)
# MAGIC - PagerDuty

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. リトライ設定
# MAGIC 
# MAGIC パイプライン自体に自動リトライ機能がありますが、ジョブレベルでも設定できます。
# MAGIC 
# MAGIC ### 設定手順
# MAGIC 
# MAGIC 1. タスクをクリック
# MAGIC 2. **詳細オプション** を展開
# MAGIC 3. **リトライ** セクションで設定
# MAGIC 
# MAGIC | 設定項目 | 推奨値 |
# MAGIC |---------|--------|
# MAGIC | 最大リトライ回数 | 2 |
# MAGIC | リトライ間隔 | 5分 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 複数タスクのワークフロー
# MAGIC 
# MAGIC 複数のパイプラインやノートブックを連携させることができます。
# MAGIC 
# MAGIC ### 例: データ取り込み → 変換 → レポート生成
# MAGIC 
# MAGIC ```
# MAGIC [取り込み] → [変換パイプライン] → [レポート生成]
# MAGIC     ↓              ↓                  ↓
# MAGIC   Task 1        Task 2             Task 3
# MAGIC ```
# MAGIC 
# MAGIC ### タスク追加手順
# MAGIC 
# MAGIC 1. **タスクを追加** をクリック
# MAGIC 2. **タスク名**を入力
# MAGIC 3. **タイプ**を選択(パイプライン/ノートブック/Python等)
# MAGIC 4. **依存先**で前のタスクを選択

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 実行履歴の確認
# MAGIC 
# MAGIC ### 確認手順
# MAGIC 
# MAGIC 1. **ワークフロー** → 対象のジョブをクリック
# MAGIC 2. **実行** タブで履歴を確認
# MAGIC 3. 各実行をクリックして詳細を確認
# MAGIC 
# MAGIC ### 確認できる情報
# MAGIC 
# MAGIC | 情報 | 説明 |
# MAGIC |------|------|
# MAGIC | ステータス | 成功/失敗/実行中/スキップ |
# MAGIC | 実行時間 | 開始〜終了までの時間 |
# MAGIC | タスク詳細 | 各タスクの個別状況 |
# MAGIC | ログ | エラー詳細やデバッグ情報 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 手動実行
# MAGIC 
# MAGIC スケジュール以外に手動で実行することも可能です。
# MAGIC 
# MAGIC ### 手順
# MAGIC 
# MAGIC 1. **ワークフロー** → 対象のジョブをクリック
# MAGIC 2. 右上の **今すぐ実行** をクリック
# MAGIC 3. パラメータを変更する場合は **パラメータ付きで実行** を選択

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC 
# MAGIC ### ジョブで実現できること
# MAGIC 
# MAGIC 1. **定期実行** - Cron式でスケジュール設定
# MAGIC 2. **監視・通知** - 失敗時にアラート
# MAGIC 3. **リトライ** - 一時的なエラーからの自動復旧
# MAGIC 4. **ワークフロー** - 複数タスクの依存関係管理
# MAGIC 5. **履歴管理** - 実行履歴の確認・分析
# MAGIC 
# MAGIC ### 本番運用のベストプラクティス
# MAGIC 
# MAGIC | 項目 | 推奨 |
# MAGIC |------|------|
# MAGIC | スケジュール | データ到着後に実行(余裕を持って) |
# MAGIC | 通知 | 失敗時は必ず通知 |
# MAGIC | リトライ | 2〜3回、間隔5〜10分 |
# MAGIC | タイムアウト | 想定時間の2〜3倍 |
# MAGIC | 監視 | ダッシュボードで傾向把握 |
