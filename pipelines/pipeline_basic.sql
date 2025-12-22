-- =============================================================================
-- 演習2: Lakeflow SDP 宣言型パイプライン（SQL版）
-- =============================================================================
-- 
-- 【このファイルについて】
-- パイプラインエディタで入力するSQLのリファレンスです。
-- 実際の演習では、パイプラインエディタ上で直接入力してください。
--
-- 【パイプライン作成手順】
-- 1. 左サイドバーで「新規」→「ETL パイプライン」を選択
-- 2. パイプライン名を入力（例: sdp_nyctaxi_pipeline）
-- 3. カタログ: workspace、スキーマ: 新規作成または既存を選択
-- 4. 「空のファイルから開始」を選択、言語は「SQL」
-- 5. 「選択」をクリック
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Bronze層: サンプルデータをそのまま取り込む
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW bronze_trips AS
SELECT * FROM samples.nyctaxi.trips;


-- -----------------------------------------------------------------------------
-- Silver層: データクレンジング
-- 不正データを除外し、必要なカラムを選択
-- -----------------------------------------------------------------------------
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


-- -----------------------------------------------------------------------------
-- Gold層: 日別売上集計
-- -----------------------------------------------------------------------------
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


-- -----------------------------------------------------------------------------
-- Gold層: ピックアップ地域別集計
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_trips_by_zone AS
SELECT 
    pickup_zip,
    COUNT(*) AS trip_count,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM silver_trips
GROUP BY pickup_zip
ORDER BY trip_count DESC;
