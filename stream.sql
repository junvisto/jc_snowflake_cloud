USE SCHEMA BRONZE;

CREATE OR REPLACE STREAM CAR_LISTINGS_RAW_STREAM
  ON TABLE BRONZE.CAR_LISTINGS_RAW
  APPEND_ONLY = FALSE;

CREATE OR REPLACE STREAM DEALERS_RAW_STREAM
  ON TABLE BRONZE.DEALERS_RAW
  APPEND_ONLY = FALSE;

CREATE OR REPLACE STREAM SALES_RAW_STREAM
  ON TABLE BRONZE.SALES_RAW
  APPEND_ONLY = FALSE;


-- Streams only show row-level changes (DML) on the table. Re-COPYing the same file often results in 0
-- Remember to reload bronze table.

/*
INSERT INTO bronze.sales_raw (listing_id, sold_price, sold_date, source_file_name)
VALUES ('test_001', 9999, CURRENT_DATE(), 'manual_test');

SELECT metadata$action, metadata$isupdate, listing_id, sold_price, sold_date
FROM bronze.sales_raw_stream;


UPDATE bronze.sales_raw
SET sold_price = sold_price + 1
WHERE listing_id = 'test_001';

DELETE FROM bronze.sales_raw
WHERE listing_id = 'test_001';

SELECT metadata$action, metadata$isupdate, listing_id, sold_price, sold_date
FROM bronze.sales_raw_stream
ORDER BY listing_id;
*/
