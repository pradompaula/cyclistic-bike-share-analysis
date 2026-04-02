-- ============================================================
-- Cyclistic Bike-Share Analysis
-- Google Data Analytics Certificate — Capstone Project
-- Author: María Paula Prado
-- Date: April 2026
-- Tool: Google BigQuery
-- ============================================================


-- ============================================================
-- PHASE 2 — PREPARE: Combine all monthly tables into one
-- and add ride_length and day_of_week columns
-- ============================================================

CREATE TABLE cyclistic_data.all_trips AS
SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  start_station_name,
  end_station_name,
  member_casual,
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length,
  FORMAT_TIMESTAMP('%A', started_at) AS day_of_week
FROM (
  SELECT * FROM cyclistic_data.trips_202503
  UNION ALL SELECT * FROM cyclistic_data.trips_202504
  UNION ALL SELECT * FROM cyclistic_data.trips_202505_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202505_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202506_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202506_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202507_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202507_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202508_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202508_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202509_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202509_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202510_part1
  UNION ALL SELECT * FROM cyclistic_data.trips_202510_part2
  UNION ALL SELECT * FROM cyclistic_data.trips_202511
  UNION ALL SELECT * FROM cyclistic_data.trips_202512
  UNION ALL SELECT * FROM cyclistic_data.trips_202601
  UNION ALL SELECT * FROM cyclistic_data.trips_202602
);


-- ============================================================
-- PHASE 3 — PROCESS: Data exploration and cleaning
-- ============================================================

-- Check total rows
SELECT COUNT(*) AS total_rows
FROM cyclistic_data.all_trips;
-- Result: 5,601,662

-- Check for null values in key columns
SELECT
  COUNTIF(ride_id IS NULL) AS null_ride_id,
  COUNTIF(started_at IS NULL) AS null_started_at,
  COUNTIF(ended_at IS NULL) AS null_ended_at,
  COUNTIF(start_station_name IS NULL) AS null_start_station,
  COUNTIF(end_station_name IS NULL) AS null_end_station,
  COUNTIF(member_casual IS NULL) AS null_member_casual,
  COUNTIF(ride_length IS NULL) AS null_ride_length
FROM cyclistic_data.all_trips;
-- Result: 0 nulls in all key columns

-- Check for invalid ride lengths
SELECT
  COUNTIF(ride_length <= 0) AS rides_zero_or_negative,
  COUNTIF(ride_length > 1440) AS rides_over_24hrs
FROM cyclistic_data.all_trips;
-- Result: 149,691 zero/negative | 5,764 over 24 hours

-- Create cleaned table (remove invalid ride lengths)
CREATE TABLE cyclistic_data.all_trips_cleaned AS
SELECT *
FROM cyclistic_data.all_trips
WHERE ride_length > 0
  AND ride_length <= 1440;
-- Result: 5,446,207 rows retained


-- ============================================================
-- PHASE 4 — ANALYZE: Key findings
-- ============================================================

-- Query 1: Total rides by user type
SELECT
  member_casual,
  COUNT(*) AS total_rides
FROM cyclistic_data.all_trips_cleaned
GROUP BY member_casual
ORDER BY total_rides DESC;
-- Result: member 3,516,876 | casual 1,929,331

-- Query 2: Average and max ride duration by user type
SELECT
  member_casual,
  ROUND(AVG(ride_length), 2) AS avg_ride_length_minutes,
  ROUND(MAX(ride_length), 2) AS max_ride_length_minutes
FROM cyclistic_data.all_trips_cleaned
GROUP BY member_casual
ORDER BY avg_ride_length_minutes DESC;
-- Result: casual 19.4 min avg | member 11.75 min avg

-- Query 3: Usage by day of week
SELECT
  member_casual,
  day_of_week,
  COUNT(*) AS total_rides,
  ROUND(AVG(ride_length), 2) AS avg_ride_length
FROM cyclistic_data.all_trips_cleaned
GROUP BY member_casual, day_of_week
ORDER BY member_casual, total_rides DESC;
-- Result: Casuals peak Sat/Sun | Members peak Tue/Wed/Thu

-- Query 4: Usage by month
SELECT
  member_casual,
  FORMAT_TIMESTAMP('%Y-%m', started_at) AS month,
  COUNT(*) AS total_rides
FROM cyclistic_data.all_trips_cleaned
GROUP BY member_casual, month
ORDER BY member_casual, month;
-- Result: Both peak Jul-Aug | Casuals drop 92% in winter vs 75% for members

-- Query 5: Bike type preference
SELECT
  member_casual,
  rideable_type,
  COUNT(*) AS total_rides
FROM cyclistic_data.all_trips_cleaned
GROUP BY member_casual, rideable_type
ORDER BY member_casual, total_rides DESC;
-- Result: Both groups ~65% electric, ~35% classic
