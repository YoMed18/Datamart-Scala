-- CREATE EXTENSION IF NOT EXISTS dblink;
-- psql -h localhost -p 15435 -U postgres
-- \i sql_scripting/insertion.sql
-- to get the schema of the table : \d yellow_taxi_trips_2024_10
-- \l list databases
-- \dt list tables
-- DROP TABLE IF EXISTS yellow_taxi_trips_2024_10;
-- Connexion au Data Warehouse
SELECT dblink_connect('db_conn', 'host=host.docker.internal port=15432 dbname=datawarehouse user=postgres password=admin');

-- Insertion dans dim_time (pickup)
INSERT INTO dim_time (full_datetime, year, month, day, hour, minute, second)
SELECT DISTINCT "tpep_pickup_datetime",
    EXTRACT(YEAR FROM "tpep_pickup_datetime"),
    EXTRACT(MONTH FROM "tpep_pickup_datetime"),
    EXTRACT(DAY FROM "tpep_pickup_datetime"),
    EXTRACT(HOUR FROM "tpep_pickup_datetime"),
    EXTRACT(MINUTE FROM "tpep_pickup_datetime"),
    EXTRACT(SECOND FROM "tpep_pickup_datetime")
FROM dblink('db_conn', 'SELECT DISTINCT "tpep_pickup_datetime" FROM yellow_taxi_trips_2024_10')
    AS source("tpep_pickup_datetime" TIMESTAMP);

-- Insertion dans dim_time (dropoff)
INSERT INTO dim_time (full_datetime, year, month, day, hour, minute, second)
SELECT DISTINCT "tpep_dropoff_datetime",
    EXTRACT(YEAR FROM "tpep_dropoff_datetime"),
    EXTRACT(MONTH FROM "tpep_dropoff_datetime"),
    EXTRACT(DAY FROM "tpep_dropoff_datetime"),
    EXTRACT(HOUR FROM "tpep_dropoff_datetime"),
    EXTRACT(MINUTE FROM "tpep_dropoff_datetime"),
    EXTRACT(SECOND FROM "tpep_dropoff_datetime")
FROM dblink('db_conn', 'SELECT DISTINCT "tpep_dropoff_datetime" FROM yellow_taxi_trips_2024_10')
    AS source("tpep_dropoff_datetime" TIMESTAMP);

-- dim_passenger_count
INSERT INTO dim_passenger_count (passenger_count)
SELECT DISTINCT "passenger_count"
FROM dblink('db_conn', 'SELECT DISTINCT "passenger_count" FROM yellow_taxi_trips_2024_10')
    AS source("passenger_count" BIGINT);

-- dim_rate_code
INSERT INTO dim_rate_code (rate_code)
SELECT DISTINCT "RatecodeID"
FROM dblink('db_conn', 'SELECT DISTINCT "RatecodeID" FROM yellow_taxi_trips_2024_10')
    AS source("RatecodeID" BIGINT);

-- dim_location (PULocationID et DOLocationID combinés)
INSERT INTO dim_location (location_code)
SELECT DISTINCT loc_id FROM (
    SELECT "PULocationID" AS loc_id FROM dblink('db_conn', 'SELECT DISTINCT "PULocationID" FROM yellow_taxi_trips_2024_10')
    AS source("PULocationID" INT)
    UNION
    SELECT "DOLocationID" FROM dblink('db_conn', 'SELECT DISTINCT "DOLocationID" FROM yellow_taxi_trips_2024_10')
    AS source("DOLocationID" INT)
) AS combined_locations;

-- dim_payment_type
INSERT INTO dim_payment_type (payment_type_code)
SELECT DISTINCT "payment_type"
FROM dblink('db_conn', 'SELECT DISTINCT "payment_type" FROM yellow_taxi_trips_2024_10')
    AS source("payment_type" BIGINT);

-- Insertion dans fact_trips
INSERT INTO fact_trips (
    pickup_time_id, dropoff_time_id,
    passenger_count_id, rate_code_id,
    location_pu_id, location_do_id,
    payment_type_id, vendor_id,
    store_and_fwd_flag, trip_distance,
    fare_amount, extra, mta_tax,
    tip_amount, tolls_amount,
    improvement_surcharge, total_amount,
    congestion_surcharge, airport_fee
)
SELECT
    pu_time.time_id,
    do_time.time_id,
    pc.passenger_count_id,
    rc.rate_code_id,
    pu_loc.location_id,
    do_loc.location_id,
    pt.payment_type_id,
    raw."VendorID",
    raw."store_and_fwd_flag",
    raw."trip_distance",
    raw."fare_amount",
    raw."extra",
    raw."mta_tax",
    raw."tip_amount",
    raw."tolls_amount",
    raw."improvement_surcharge",
    raw."total_amount",
    raw."congestion_surcharge",
    raw."Airport_fee"
FROM dblink('db_conn', '
    SELECT * FROM yellow_taxi_trips_2024_10 LIMIT 10000
') AS raw(
    "VendorID" INT,
    "tpep_pickup_datetime" TIMESTAMP,
    "tpep_dropoff_datetime" TIMESTAMP,
    "passenger_count" BIGINT,
    "trip_distance" DOUBLE PRECISION,
    "RatecodeID" BIGINT,
    "store_and_fwd_flag" TEXT,
    "PULocationID" INT,
    "DOLocationID" INT,
    "payment_type" BIGINT,
    "fare_amount" DOUBLE PRECISION,
    "extra" DOUBLE PRECISION,
    "mta_tax" DOUBLE PRECISION,
    "tip_amount" DOUBLE PRECISION,
    "tolls_amount" DOUBLE PRECISION,
    "improvement_surcharge" DOUBLE PRECISION,
    "total_amount" DOUBLE PRECISION,
    "congestion_surcharge" DOUBLE PRECISION,
    "Airport_fee" DOUBLE PRECISION
    )
JOIN dim_time pu_time ON pu_time.full_datetime = raw."tpep_pickup_datetime"
JOIN dim_time do_time ON do_time.full_datetime = raw."tpep_dropoff_datetime"
JOIN dim_passenger_count pc ON pc.passenger_count = raw."passenger_count"
JOIN dim_rate_code rc ON rc.rate_code = raw."RatecodeID"
JOIN dim_location pu_loc ON pu_loc.location_code = raw."PULocationID"
JOIN dim_location do_loc ON do_loc.location_code = raw."DOLocationID"
JOIN dim_payment_type pt ON pt.payment_type_code = raw."payment_type";
