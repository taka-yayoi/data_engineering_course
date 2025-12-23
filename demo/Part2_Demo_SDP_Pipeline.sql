-- =============================================================================
-- Part 2 デモ: Lakeflow SDP パイプライン (SQL版)
-- =============================================================================
-- 
-- このファイルをパイプラインのソースコードとして設定します。
-- 
-- パイプラインの構造:
--   samples.nyctaxi.trips
--           ↓
--      [bronze_trips]     ← 生データをそのまま
--           ↓
--      [silver_trips]     ← クレンジング + 計算列追加
--           ↓
--    [gold_daily_stats]   ← 日別集計
--
-- パイプライン作成手順:
--   1. データエンジニアリング → パイプライン → パイプラインを作成
--   2. 設定:
--      - パイプライン名: demo_sdp_pipeline
--      - ソースコード: このファイルを選択
--      - 送信先:
--        - カタログ: workspace
--        - ターゲットスキーマ: demo_sdp_<あなたの名前>
--   3. 作成 → 開始
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Bronze層: 生データの取り込み
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW bronze_trips
COMMENT 'NYC Taxiの生データ（そのまま保存）'
AS
SELECT * FROM samples.nyctaxi.trips;


-- -----------------------------------------------------------------------------
-- Silver層: クレンジング処理
-- - 不正データを除外 (Expectations使用)
-- - 乗車時間、マイル単価を計算
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW silver_trips (
  CONSTRAINT valid_fare EXPECT (fare_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_distance EXPECT (trip_distance > 0) ON VIOLATION DROP ROW
)
COMMENT 'クレンジング済みデータ'
AS
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  -- 乗車時間（分）
  ROUND(
    (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60,
    2
  ) AS trip_duration_min,
  -- マイル単価
  ROUND(fare_amount / trip_distance, 2) AS fare_per_mile
FROM bronze_trips;


-- -----------------------------------------------------------------------------
-- Gold層: ビジネス用集計
-- 日別の統計サマリーを作成
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW gold_daily_stats
COMMENT '日別統計サマリー'
AS
SELECT
  TO_DATE(tpep_pickup_datetime) AS date,
  COUNT(*) AS trip_count,
  ROUND(SUM(fare_amount), 2) AS total_revenue,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM silver_trips
GROUP BY TO_DATE(tpep_pickup_datetime)
ORDER BY date;
