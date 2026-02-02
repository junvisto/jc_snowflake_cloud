CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE silver.car_listings (
  listing_id        STRING,
  vin               STRING,
  make              STRING,
  model             STRING,
  year              INT,
  price             NUMBER(10,2),
  mileage           INT,
  dealer_id         STRING,
  city              STRING,
  state             STRING,
  listed_date       DATE,
  source_file_name  STRING,
  ingested_at       TIMESTAMP_NTZ,
  last_updated_at   TIMESTAMP_NTZ,
  CONSTRAINT pk_car_listings PRIMARY KEY (listing_id)
);

CREATE OR REPLACE TABLE silver.dealers (
  dealer_id         STRING,
  dealer_name       STRING,
  dealer_type       STRING,
  city              STRING,
  state             STRING,
  source_file_name  STRING,
  ingested_at       TIMESTAMP_NTZ,
  last_updated_at   TIMESTAMP_NTZ,
  CONSTRAINT pk_dealers PRIMARY KEY (dealer_id)
);

CREATE OR REPLACE TABLE silver.sales (
  listing_id        STRING,
  sold_price        NUMBER,
  sold_date         DATE,
  source_file_name  STRING,
  ingested_at       TIMESTAMP_NTZ,
  last_updated_at   TIMESTAMP_NTZ,
  CONSTRAINT pk_sales PRIMARY KEY (listing_id, sold_date)
);


##
CREATE OR REPLACE PROCEDURE silver.merge_dealers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO silver.dealers t
  USING (
    SELECT
      action,
      dealer_id,
      dealer_name,
      dealer_type,
      city,
      state,
      source_file_name,
      ingested_at,
      last_updated_at
    FROM (
      SELECT
        metadata$action AS action,
        dealer_id,
        dealer_name,
        dealer_type,
        city,
        state,
        source_file_name,
        ingested_at,
        CURRENT_TIMESTAMP() AS last_updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY dealer_id
          ORDER BY ingested_at DESC
        ) AS rn
      FROM bronze.dealers_raw_stream
    )
    WHERE rn = 1
  ) s
  ON t.dealer_id = s.dealer_id
  WHEN MATCHED AND s.action = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET
    dealer_name = s.dealer_name,
    dealer_type = s.dealer_type,
    city = s.city,
    state = s.state,
    source_file_name = s.source_file_name,
    ingested_at = s.ingested_at,
    last_updated_at = s.last_updated_at
  WHEN NOT MATCHED AND s.action <> 'DELETE' THEN INSERT
    (dealer_id, dealer_name, dealer_type, city, state, source_file_name, ingested_at, last_updated_at)
  VALUES
    (s.dealer_id, s.dealer_name, s.dealer_type, s.city, s.state, s.source_file_name, s.ingested_at, s.last_updated_at);

  RETURN 'dealers merged';
END;
$$;


CREATE OR REPLACE PROCEDURE silver.merge_car_listings()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO silver.car_listings t
  USING (
    SELECT
      metadata$action AS action,
      listing_id, 
      vin, 
      make, 
      model,
      CASE WHEN year BETWEEN 1980 AND 2035 THEN year ELSE NULL END AS year,
      CASE WHEN price < 0 THEN 0 ELSE price END AS price,
      CASE WHEN mileage < 0 THEN 0 ELSE mileage END AS mileage,
      dealer_id, 
      city, 
      state, 
      listed_date,
      source_file_name, 
      ingested_at,
      CURRENT_TIMESTAMP() AS last_updated_at
    FROM bronze.car_listings_raw_stream
  ) s
  ON t.listing_id = s.listing_id
  WHEN MATCHED AND s.action = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET
    vin = s.vin,
    make = s.make,
    model = s.model,
    year = s.year,
    price = s.price,
    mileage = s.mileage,
    dealer_id = s.dealer_id,
    city = s.city,
    state = s.state,
    listed_date = s.listed_date,
    source_file_name = s.source_file_name,
    ingested_at = s.ingested_at,
    last_updated_at = s.last_updated_at
  WHEN NOT MATCHED AND s.action <> 'DELETE' THEN INSERT
    (listing_id, vin, make, model, year, price, mileage, dealer_id, city, state,
     listed_date, source_file_name, ingested_at, last_updated_at)
  VALUES
    (s.listing_id, s.vin, s.make, s.model, s.year, s.price, s.mileage, s.dealer_id, s.city, s.state,
     s.listed_date, s.source_file_name, s.ingested_at, s.last_updated_at);

  RETURN 'car_listings merged';
END;
$$;

CREATE OR REPLACE PROCEDURE silver.merge_sales()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO silver.sales t
  USING (
    SELECT
      action,
      listing_id,
      sold_price,
      sold_date,
      source_file_name,
      ingested_at,
      last_updated_at
    FROM (
      SELECT
        metadata$action AS action,
        listing_id,
        CASE WHEN sold_price < 0 THEN 0 ELSE sold_price END AS sold_price,
        sold_date,
        source_file_name,
        ingested_at,
        CURRENT_TIMESTAMP() AS last_updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY listing_id, sold_date
          ORDER BY ingested_at DESC
        ) AS rn
      FROM bronze.sales_raw_stream
    )
    WHERE rn = 1
  ) s
  ON t.listing_id = s.listing_id AND t.sold_date = s.sold_date
  WHEN MATCHED AND s.action = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET
    sold_price = s.sold_price,
    source_file_name = s.source_file_name,
    ingested_at = s.ingested_at,
    last_updated_at = s.last_updated_at
  WHEN NOT MATCHED AND s.action <> 'DELETE' THEN INSERT
    (listing_id, sold_price, sold_date, source_file_name, ingested_at, last_updated_at)
  VALUES
    (s.listing_id, s.sold_price, s.sold_date, s.source_file_name, s.ingested_at, s.last_updated_at);

  RETURN 'sales merged';
END;
$$;
