USE SCHEMA SILVER;

SELECT *  FROM silver.dealers

CALL silver.merge_car_listings();
CALL silver.merge_dealers();
CALL silver.merge_sales();

CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- Columns overview
SELECT * 
from silver.car_listings c
LEFT JOIN silver.dealers d
    on c.dealer_id = d.dealer_id
LEFT JOIN silver.sales s
    on s.listing_id = c.LISTING_ID

DROP VIEW vw_inventory_sales_funnel
CREATE OR REPLACE VIEW gold.v_inventory_sales_funnel AS
SELECT
  l.listing_id,
  l.vin,
  l.make,
  l.model,
  l.year,
  l.city,
  l.state,
  l.dealer_id,
  d.dealer_name,
  d.dealer_type,
  l.listed_date,
  l.price AS listed_price,
  l.mileage,
  s.sold_date,
  s.sold_price,
  CASE WHEN s.sold_date IS NULL THEN 'available' ELSE 'sold' END AS listing_status,
  DATEDIFF('day', l.listed_date, COALESCE(s.sold_date, CURRENT_DATE())) AS days_on_market,
  (s.sold_price - l.price) AS price_delta,
  CASE WHEN s.sold_price IS NULL OR l.price = 0 THEN NULL
       ELSE ROUND((s.sold_price - l.price) / l.price * 100, 2)
  END AS price_delta_pct
FROM silver.car_listings l
LEFT JOIN silver.sales s
  ON s.listing_id = l.listing_id
LEFT JOIN silver.dealers d
  ON d.dealer_id = l.dealer_id;


SELECT * from gold.v_inventory_sales_funnel

CREATE OR REPLACE VIEW gold.v_dealer_scorecard AS
SELECT
  d.dealer_id,
  d.dealer_name,
  d.dealer_type,
  d.state,
  COUNT(s.listing_id) AS sales_cnt,
  SUM(s.sold_price) AS total_revenue,
  AVG(DATEDIFF('day', l.listed_date, s.sold_date)) AS avg_days_to_sell,
  AVG(s.sold_price - l.price) AS avg_price_delta
FROM silver.sales s
JOIN silver.car_listings l
  ON l.listing_id = s.listing_id
LEFT JOIN silver.dealers d
  ON d.dealer_id = l.dealer_id
GROUP BY 1,2,3,4;



CREATE OR REPLACE VIEW gold.v_market_pricing AS
SELECT
  l.state,
  l.make,
  l.model,
  l.year,
  COUNT(*) AS sold_cnt,
  AVG(l.mileage) AS avg_mileage,
  AVG(l.price) AS avg_listed_price,
  AVG(s.sold_price) AS avg_sold_price,
  AVG(s.sold_price - l.price) AS avg_discount
FROM silver.sales s
JOIN silver.car_listings l
  ON l.listing_id = s.listing_id
GROUP BY 1,2,3,4;

SELECT * FROM v_market_pricing;
