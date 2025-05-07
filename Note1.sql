USE CATALOG workspace;
USE default;

SELECT * FROM samples.accuweather.forecast_daily_calendar_imperial ;
SELECT * FROM samples.nyctaxi.trips;

Describe samples.accuweather.forecast_daily_calendar_imperial ;
Describe extended samples.accuweather.forecast_daily_calendar_imperial ;
DESCRIBE HISTORY TRIPS; --GIVES VERSIONS OF TABLE CREATED

Create table PURCHASE_DATES using delta;
Create or replace table purchase_dates(
Id string ,
Transaction_timestamp string,
Price string ,
Date DATE GENERATED ALWAYS AS ( cast(cast(TRANSACTION_TIMESTAMP/1e6 as TIMESTAMP) AS DATE))
COMMENT "generated based on 'transaction_timestamp' column");

Create or replace table PURCHASE_DATES   
Using delta as   -- not req default it creates delta table 
Select * from parquet.<file_path> ;  --PARQUET FILE

Create or replace temp view PURCHASE_SALES as select * from csv.'${da.paths.datasets}/ecommerce/raw/sales-csv'; 

Create or replace table PURCHASE_SALES
Using delta as 
Select * from read_files("${da.paths.datasets}/ecommerce/raw/sales-csv",
				Format =>"csv",
				Sep => "|" ,  --DELIMITER
				Header =>true,
				Mode => "failfast");

CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 'HR', 75000),
  (2, 'Bob', 'Engineering', 98000),
  (3, 'Charlie', 'HR', 62000),
  (4, 'David', 'Engineering', 115000),
  (5, 'Eve', 'Marketing', 68000)
AS employees(id, name, department, salary);

-------------------------------------------------------------
--Incrementally load data:

Copy into PURCHASE_SALES  from '${da.paths.datasets}/ecommerce/raw/sales-csv'
fileformat = parquet
copy_options(['mergeSchema'= 'true') ;  -- copy options are key value pairs  -- merge any schema it finds in these files  into this new table

Insert overwrite PURCHASE_SALES select * from parquet.'${da.paths.datasets}/ecommerce/raw/sales-csv';


--CLONING  --INCURS STORAGE COSTS FOR CLONED DATA
Create or replace table TRIPS_CLONE deep clone samples.nyctaxi.trips; --WITH DATA
Create or replace table TRIPS_CLONE1 shallow clone samples.nyctaxi.trips ; --WITHOUT DATA ONLY METADATA


--TIMETRAVEL 
SELECT count(*) FROM samples.nyctaxi.trips TIMESTAMP AS OF "2025-04-20" ;
SELECT count(*) FROM samples.nyctaxi.trips TIMESTAMP AS OF date_sub(current_date(), 1) ;
SELECT count(*) FROM samples.nyctaxi.trips TIMESTAMP AS OF "2025-04-20 01:30:00.000" ;

--USING VERSIONS
/*
SELECT count(*) FROM my_samples.nyctaxi.tripstable VERSION AS OF 01 ; --VERSION
SELECT count(*) FROM samples.nyctaxi.trips@v01 ;--VERSION
SELECT count(*) FROM delta.`/path/to/my/table@v01` ; --PATH
Select * from parquet.<file Path>  limit 10;  --for parquet file
*/

---------------------------------------------------------

CREATE TABLE EMPLOYEE (EMP_ID INTEGER, NAME STRING, SAL INTEGER);
CREATE TABLE IF NOT EXISTS EMPLOYEE (EMP_ID INTEGER, NAME STRING, SAL INTEGER);
CREATE OR REPLACE TABLE EMPLOYEE (EMP_ID INTEGER, NAME STRING, SAL INTEGER);

INSERT INTO EMPLOYEE VALUES (1, "QWERTY", 100);
INSERT INTO EMPLOYEE VALUES (2, "WERTYU", 200), (3, "ASDFG", 300),(4, "SDFGH", 400), (5, "ZXCVB", 500),(6, "XCVBN", 600);
CREATE OR REPLACE TABLE EMP_UPDATES (EMP_ID INTEGER, NAME STRING, SAL INTEGER);

INSERT INTO EMP_UPDATES VALUES (2, "WERTYU", 4000), (3, "ASDFG", 5000),(7, "MNBV", 7000), (8, "NBVC", 8000),(9, "LKJHG", 90000);

SELECT * FROM EMPLOYEE ORDER BY EMP_ID ;
SELECT * FROM EMP_UPDATES ORDER BY EMP_ID;

SELECT CURRENT_TIMESTAMP(); 

MERGE INTO EMPLOYEE E
USING EMP_UPDATES U
ON E.EMP_ID=U.EMP_ID
WHEN MATCHED AND (U.SAL > 0 AND E.SAL <> U.SAL)  THEN UPDATE SET *
WHEN NOT MATCHED BY TARGET THEN INSERT * 
WHEN NOT MATCHED BY SOURCE THEN DELETE
;

--SELECT * FROM EMPLOYEE TIMESTAMP AS OF "2025-04-28" ;

--STREAMS

CREATE OR REFRESH STREAMING LIVE TABLE streaming_sales
AS SELECT * FROM cloud_files(
  "/mnt/data/sales_stream/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders
AS
SELECT * FROM cloud_files("${datasets_path}/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))
     
SELECT * FROM streaming_sales
ORDER BY timestamp DESC
LIMIT 10;
--SELECT * FROM streaming_sales VERSION AS OF 1;
SELECT * FROM streaming_sales TIMESTAMP AS OF '2025-05-05T10:00:00Z';

SELECT 
  window.timestamp AS window_time,
  COUNT(*) AS sales_count,
  SUM(amount) AS total_amount
FROM TABLE(
  STREAMING_TABLE(SELECT * FROM streaming_sales)
)
GROUP BY window(timestamp, '5 minutes');

SELECT *
FROM streaming_sales
WHERE timestamp > current_timestamp() - INTERVAL 15 MINUTES;


CREATE OR REFRESH STREAMING LIVE TABLE customers
AS SELECT * FROM cloud_files("${datasets_path}/retail-org/customers/", "csv");

CREATE OR REFRESH STREAMING LIVE TABLE sales_clean(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
  TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
  DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
  f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.streaming_sales) f
  LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name



-----------------------------------------------------------------
