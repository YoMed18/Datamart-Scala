-- Création des dimensions

CREATE TABLE dim_time (
  time_id SERIAL PRIMARY KEY,
  full_datetime TIMESTAMP,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT,
  second INT
);

CREATE TABLE dim_passenger_count (
 passenger_count_id SERIAL PRIMARY KEY,
 passenger_count BIGINT UNIQUE
);

CREATE TABLE dim_rate_code (
  rate_code_id SERIAL PRIMARY KEY,
  rate_code BIGINT UNIQUE
);

CREATE TABLE dim_location (
  location_id SERIAL PRIMARY KEY,
  location_code INT UNIQUE
);

CREATE TABLE dim_payment_type (
  payment_type_id SERIAL PRIMARY KEY,
  payment_type_code BIGINT UNIQUE
);

-- Table des faits

CREATE TABLE fact_trips (
  trip_id SERIAL PRIMARY KEY,
  pickup_time_id INT REFERENCES dim_time(time_id),
  dropoff_time_id INT REFERENCES dim_time(time_id),
  passenger_count_id INT REFERENCES dim_passenger_count(passenger_count_id),
  rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
  location_pu_id INT REFERENCES dim_location(location_id),
  location_do_id INT REFERENCES dim_location(location_id),
  payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
  vendor_id INT,
  store_and_fwd_flag TEXT,
  trip_distance DOUBLE PRECISION,
  fare_amount DOUBLE PRECISION,
  extra DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION,
  airport_fee DOUBLE PRECISION
);
