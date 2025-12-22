# Databricks notebook source
# MAGIC %md
# MAGIC # 演習1: PySparkによる命令型ETL（25分）
# MAGIC 
# MAGIC このノートブックでは、Part 1で学んだSparkの基礎を使って、命令型のETLパイプラインを構築します。
# MAGIC 
# MAGIC ## 学習目標
# MAGIC - PySparkでデータを読み込み、変換し、保存する
# MAGIC - Bronze → Silver → Gold のメダリオンアーキテクチャを実装する
# MAGIC - 命令型アプローチの特徴を理解する

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 環境設定

# COMMAND ----------

# ユーザー名を自動取得
user_name = spark.sql("SELECT current_user()").first()[0]
user_name_prefix = user_name.split("@")[0].replace(".", "_").replace("-", "_")

# カタログとスキーマの設定（Free Edition用）
CATALOG = "workspace"
SCHEMA = f"de_handson_{user_name_prefix}"

print(f"ユーザー: {user_name}")
print(f"カタログ: {CATALOG}")
print(f"スキーマ: {SCHEMA}")

# COMMAND ----------

# スキーマの作成（存在しない場合）
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"✅ スキーマ {CATALOG}.{SCHEMA} を使用します")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ソースデータの確認
# MAGIC 
# MAGIC NYCタクシーのサンプルデータを使用します。

# COMMAND ----------

# サンプルデータの読み込み
source_df = spark.read.table("samples.nyctaxi.trips")

# データの確認
print(f"レコード数: {source_df.count():,}")
source_df.printSchema()

# COMMAND ----------

# 先頭10件を表示
display(source_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze層の作成
# MAGIC 
# MAGIC Bronze層では、ソースデータをそのまま取り込みます。
# MAGIC 実際の業務では、ファイルやAPIから取り込んだ生データをここに保存します。

# COMMAND ----------

# Bronzeテーブルの作成
bronze_df = spark.read.table("samples.nyctaxi.trips")

# テーブルとして保存
bronze_df.write.mode("overwrite").saveAsTable("bronze_trips")

print("✅ Bronze層を作成しました")

# COMMAND ----------

# 確認
display(spark.table("bronze_trips").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver層の作成
# MAGIC 
# MAGIC Silver層では、データのクレンジング（品質向上）を行います。
# MAGIC 
# MAGIC ### クレンジング内容
# MAGIC - 不正なデータの除外（料金が0以下、距離が0以下）
# MAGIC - 必要なカラムの選択
# MAGIC - 日付カラムの追加

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Bronzeから読み込み
bronze_df = spark.table("bronze_trips")

# クレンジング処理
silver_df = (bronze_df
    # 不正データの除外
    .filter(col("fare_amount") > 0)
    .filter(col("trip_distance") > 0)
    # 必要カラムの選択と日付カラムの追加
    .select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "pickup_zip",
        "dropoff_zip",
        to_date("tpep_pickup_datetime").alias("pickup_date")
    )
)

# テーブルとして保存
silver_df.write.mode("overwrite").saveAsTable("silver_trips")

print("✅ Silver層を作成しました")

# COMMAND ----------

# 確認：レコード数の比較
bronze_count = spark.table("bronze_trips").count()
silver_count = spark.table("silver_trips").count()

print(f"Bronze: {bronze_count:,} 件")
print(f"Silver: {silver_count:,} 件")
print(f"除外された件数: {bronze_count - silver_count:,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold層の作成
# MAGIC 
# MAGIC Gold層では、ビジネス向けの集計を行います。
# MAGIC 
# MAGIC ### 集計内容
# MAGIC - 日別の乗車回数
# MAGIC - 日別の総売上
# MAGIC - 日別の平均料金
# MAGIC - 日別の平均距離

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, round

# Silverから読み込み
silver_df = spark.table("silver_trips")

# 日別集計
gold_df = (silver_df
    .groupBy("pickup_date")
    .agg(
        count("*").alias("trip_count"),
        round(sum("fare_amount"), 2).alias("total_fare"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(avg("trip_distance"), 2).alias("avg_distance")
    )
    .orderBy("pickup_date")
)

# テーブルとして保存
gold_df.write.mode("overwrite").saveAsTable("gold_daily_trips")

print("✅ Gold層を作成しました")

# COMMAND ----------

# 結果の確認
display(spark.table("gold_daily_trips"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. カタログエクスプローラで確認
# MAGIC 
# MAGIC 作成したテーブルをカタログエクスプローラで確認しましょう。
# MAGIC 
# MAGIC ### 確認手順
# MAGIC 
# MAGIC 1. 左メニューから **Catalog**（カタログアイコン）をクリック
# MAGIC 2. **workspace** カタログを展開
# MAGIC 3. **de_handson_<あなたの名前>** スキーマを展開
# MAGIC 4. 以下のテーブルが表示されることを確認：
# MAGIC    - `bronze_trips`
# MAGIC    - `silver_trips`
# MAGIC    - `gold_daily_trips`
# MAGIC 
# MAGIC ### カタログエクスプローラでできること
# MAGIC 
# MAGIC - **Sample Data**: データのプレビュー
# MAGIC - **Details**: テーブルのメタデータ（作成日時、所有者など）
# MAGIC - **Schema**: カラム定義の確認
# MAGIC - **Lineage**: データの依存関係（どこから来たか）

# COMMAND ----------

# カタログエクスプローラへのリンク（参考）
print(f"📂 カタログエクスプローラで確認:")
print(f"   Catalog → workspace → {SCHEMA}")
print(f"")
print(f"作成したテーブル:")
print(f"   - bronze_trips")
print(f"   - silver_trips")
print(f"   - gold_daily_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. まとめ
# MAGIC 
# MAGIC ### 作成したテーブル
# MAGIC 
# MAGIC | レイヤー | テーブル名 | 内容 |
# MAGIC |---------|-----------|------|
# MAGIC | Bronze | bronze_trips | 生データ（そのまま取り込み） |
# MAGIC | Silver | silver_trips | クレンジング済みデータ |
# MAGIC | Gold | gold_daily_trips | 日別集計データ |
# MAGIC 
# MAGIC ### 命令型アプローチの特徴
# MAGIC 
# MAGIC ✅ **メリット**
# MAGIC - 処理の流れが明確
# MAGIC - デバッグしやすい
# MAGIC - 柔軟な制御が可能
# MAGIC 
# MAGIC ❌ **課題**
# MAGIC - コード量が多い
# MAGIC - 差分処理は自分で実装が必要
# MAGIC - 依存関係の管理が手動
# MAGIC - データ品質チェックも自前実装
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 次の演習では、同じ処理を **SQL だけ** で、より簡潔に実装します。

