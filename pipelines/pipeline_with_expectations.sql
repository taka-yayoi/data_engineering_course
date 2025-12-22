-- =============================================================================
-- 演習3: エクスペクテーションの追加
-- =============================================================================
-- 
-- 【このファイルについて】
-- 演習2のパイプラインにデータ品質チェック（エクスペクテーション）を追加します。
-- 既存のパイプラインを編集するか、新しいパイプラインを作成してください。
--
-- 【エクスペクテーションの3つのモード】
-- - EXPECT (条件)                           : 警告のみ（データは保持）
-- - EXPECT (条件) ON VIOLATION DROP ROW     : 違反行を除外
-- - EXPECT (条件) ON VIOLATION FAIL UPDATE  : パイプライン停止
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Bronze層: サンプルデータをそのまま取り込む（変更なし）
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW bronze_trips AS
SELECT * FROM samples.nyctaxi.trips;


-- -----------------------------------------------------------------------------
-- Silver層: エクスペクテーション付きデータクレンジング
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW silver_trips (
    -- 違反行を除外: 料金が正の値であること
    CONSTRAINT valid_fare EXPECT (fare_amount > 0) ON VIOLATION DROP ROW,
    
    -- 違反行を除外: 距離が正の値であること
    CONSTRAINT valid_distance EXPECT (trip_distance > 0) ON VIOLATION DROP ROW,
    
    -- 警告のみ: 高額料金を監視（500ドル以上）
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


-- -----------------------------------------------------------------------------
-- Gold層: 日別売上集計（エクスペクテーション付き）
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_daily_trips (
    -- 警告のみ: 売上がマイナスでないことを確認
    CONSTRAINT valid_total EXPECT (total_fare >= 0),
    
    -- 違反行を除外: 注文数が0より大きいこと
    CONSTRAINT valid_count EXPECT (trip_count > 0) ON VIOLATION DROP ROW
) AS
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
-- Gold層: ピックアップ地域別集計（変更なし）
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
