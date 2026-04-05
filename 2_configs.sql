Create or replace DATABASE SNOWPARK_DB;
Create or replace Schema Bronze_sch;
Create or replace Schema Silver_sch;
Create or replace Schema Gold_sch;
Create or replace Schema Config_sch;


create or replace file format Config_sch.my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- json file format with strip outer array true
create or replace file format Config_sch.my_json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- parquet file format
create or replace file format Config_sch.my_parquet_format
  type = parquet
  compression = snappy;


CREATE OR REPLACE STORAGE INTEGRATION STORAGE_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE  
  STORAGE_PROVIDER = S3  
  ENABLED = TRUE  
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::273354667658:role/apollo_ksr_project_role'  
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowfalke-ksr-project-aws/');  

  Desc integration STORAGE_S3_INTEGRATION;

  CREATE OR REPLACE STAGE SNOWPARK_DB.Bronze_sch.ext_stage
  STORAGE_INTEGRATION = STORAGE_S3_INTEGRATION  -- Links to the storage integration
  URL = 's3://snowfalke-ksr-project-aws/'; -- Specifies the S3 bucket location

List @SNOWPARK_DB.Bronze_sch.ext_stage/snowpark_project/;



--Csv file loading
SELECT
  $1,$2,$3,$4, $5,$6,  $7,$8,$9,$10, $11,$12,  $13,$14,$15,$16, $17,$18
  FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/csv/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_CSV_FORMAT');



SELECT
    $1::STRING  AS Transaction_ID,
    $2::DATE    AS Transaction_Date,
    $3::NUMBER(10,2) AS Transaction_Amount,
    $4::STRING  AS Transaction_Status,
    $5::STRING  AS Transaction_Type,
    $6::STRING  AS Customer_ID,
    $7::STRING  AS Customer_Name,
    $8::STRING  AS Gender,
    $9::DATE    AS DOB,
    $10::STRING AS Email,
    $11::STRING AS Phone,
    $12::STRING AS Customer_City,
    $13::STRING AS Card_ID,
    $14::STRING AS Card_Type,
    $15::STRING AS Issuer_Bank,
    $16::STRING AS Card_Tier,
    $17::DATE   AS Expiry_Date,
    $18::STRING AS Merchant_ID,
    $19::STRING AS Merchant_Name,
    $20::STRING AS Merchant_Category,
    $21::STRING AS Merchant_Country,
    $22::STRING AS Location_ID,
    $23::STRING AS City,
    $24::STRING AS State,
    $25::STRING AS Country,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED
FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/csv/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_CSV_FORMAT');
-----------------------------------

--Json file loading
Select $1
FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/json/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_JSON_FORMAT');


SELECT
    $1:"Transaction_ID"::STRING AS Transaction_ID,
    $1:"Transaction_Date"::DATE AS Transaction_Date,
    $1:"Transaction_Amount"::NUMBER(10,2) AS Transaction_Amount,
    $1:"Transaction_Status"::STRING AS Transaction_Status,
    $1:"Transaction_Type"::STRING AS Transaction_Type,
    $1:"Customer_ID"::STRING AS Customer_ID,
    $1:"Customer_Name"::STRING AS Customer_Name,
    $1:"Gender"::STRING AS Gender,
    $1:"DOB"::DATE AS DOB,
    $1:"Email"::STRING AS Email,
    $1:"Phone"::STRING AS Phone,
    $1:"Customer_City"::STRING AS Customer_City,
    $1:"Card_ID"::STRING AS Card_ID,
    $1:"Card_Type"::STRING AS Card_Type,
    $1:"Issuer_Bank"::STRING AS Issuer_Bank,
    $1:"Card_Tier"::STRING AS Card_Tier,
    $1:"Expiry_Date"::DATE AS Expiry_Date,
    $1:"Merchant_ID"::STRING AS Merchant_ID,
    $1:"Merchant_Name"::STRING AS Merchant_Name,
    $1:"Merchant_Category"::STRING AS Merchant_Category,
    $1:"Merchant_Country"::STRING AS Merchant_Country,
    $1:"Location_ID"::STRING AS Location_ID,
    $1:"City"::STRING AS City,
    $1:"State"::STRING AS State,
    $1:"Country"::STRING AS Country,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED
FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/json/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_JSON_FORMAT');
--------------------------------------------------

--Parquet file loading
Select $1
FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/parquet/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_PARQUET_FORMAT');

SELECT
    $1:"Transaction_ID"::STRING AS Transaction_ID,
    $1:"Transaction_Date"::DATE AS Transaction_Date,
    $1:"Transaction_Amount"::NUMBER(10,2) AS Transaction_Amount,
    $1:"Transaction_Status"::STRING AS Transaction_Status,
    $1:"Transaction_Type"::STRING AS Transaction_Type,
    $1:"Customer_ID"::STRING AS Customer_ID,
    $1:"Customer_Name"::STRING AS Customer_Name,
    $1:"Gender"::STRING AS Gender,
    $1:"DOB"::DATE AS DOB,
    $1:"Email"::STRING AS Email,
    $1:"Phone"::STRING AS Phone,
    $1:"Customer_City"::STRING AS Customer_City,
    $1:"Card_ID"::STRING AS Card_ID,
    $1:"Card_Type"::STRING AS Card_Type,
    $1:"Issuer_Bank"::STRING AS Issuer_Bank,
    $1:"Card_Tier"::STRING AS Card_Tier,
    $1:"Expiry_Date"::DATE AS Expiry_Date,
    $1:"Merchant_ID"::STRING AS Merchant_ID,
    $1:"Merchant_Name"::STRING AS Merchant_Name,
    $1:"Merchant_Category"::STRING AS Merchant_Category,
    $1:"Merchant_Country"::STRING AS Merchant_Country,
    $1:"Location_ID"::STRING AS Location_ID,
    $1:"City"::STRING AS City,
    $1:"State"::STRING AS State,
    $1:"Country"::STRING AS Country,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED
FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/parquet/
(FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_PARQUET_FORMAT');


----------------------------------------------

CREATE OR REPLACE TABLE SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING (
    Transaction_ID          STRING,
    Transaction_Date        DATE,
    Transaction_Amount      NUMBER(10,2),
    Transaction_Status      STRING,
    Transaction_Type        STRING,
    Customer_ID             STRING,
    Customer_Name           STRING,
    Gender                  STRING,
    DOB                     DATE,
    Email                   STRING,
    Phone                   STRING,
    Customer_City           STRING,
    Card_ID                 STRING,
    Card_Type               STRING,
    Issuer_Bank             STRING,
    Card_Tier               STRING,
    Expiry_Date             DATE,
    Merchant_ID             STRING,
    Merchant_Name           STRING,
    Merchant_Category       STRING,
    Merchant_Country        STRING,
    Location_ID             STRING,
    City                    STRING,
    State                   STRING,
    Country                 STRING,
    -- Metadata columns (for COPY INTO with `INCLUDE_METADATA`)
    STG_FILE_NAME           STRING,
    STG_ROW_NUMBER          NUMBER,
    STG_LAST_MODIFIED       TIMESTAMP_LTZ
);


select * from SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING;

CREATE OR REPLACE TABLE SNOWPARK_DB.SILVER_SCH.SILVER_TRANSACTIONS (
    Transaction_ID          STRING,
    Transaction_Date        DATE,
    Transaction_Amount      NUMBER(12,2),
    Transaction_Status      STRING,
    Transaction_Type        STRING,
    Customer_ID             STRING,
    Customer_Name           STRING,
    Gender                  STRING,
    DOB                     DATE,
    Email                   STRING,
    Phone                   STRING,
    Customer_City           STRING,
    Card_ID                 STRING,
    Card_Type               STRING,
    Issuer_Bank             STRING,
    Card_Tier               STRING,
    Expiry_Date             DATE,
    Merchant_ID             STRING,
    Merchant_Name           STRING,
    Merchant_Category       STRING,
    Merchant_Country        STRING,
    Location_ID             STRING,
    City                    STRING,
    State                   STRING,
    Country                 STRING
);


Select * from SNOWPARK_DB.SILVER_SCH.SILVER_TRANSACTIONS;


CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.DIM_CUSTOMER (
    Customer_ID         STRING PRIMARY KEY,
    Customer_Name       STRING,
    Gender              STRING,
    DOB                 DATE,
    Email               STRING,
    Phone               STRING,
    Customer_City       STRING
);

CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.DIM_MERCHANT (
    Merchant_ID         STRING PRIMARY KEY,
    Merchant_Name       STRING,
    Merchant_Category   STRING,
    Merchant_Country    STRING
);

CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.DIM_CARD (
    Card_ID         STRING PRIMARY KEY,
    Card_Type       STRING,
    Issuer_Bank     STRING,
    Card_Tier       STRING,
    Expiry_Date     DATE
);

CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.DIM_LOCATION (
    Location_ID     STRING PRIMARY KEY,
    City            STRING,
    State           STRING,
    Country         STRING
);

CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.DIM_DATE (
    Date_ID        DATE PRIMARY KEY,  
    Day            NUMBER,
    Month          NUMBER,
    Year           NUMBER,
    Quarter        NUMBER,
    Weekday        STRING
);


CREATE OR REPLACE TABLE SNOWPARK_DB.GOLD_SCH.FACT_TRANSACTIONS (
    Transaction_ID         STRING PRIMARY KEY,
    Transaction_Date       DATE,
    Transaction_Amount     NUMBER(12,2),
    Transaction_Status     STRING,
    Transaction_Type       STRING,
    
    -- Foreign Keys
    Customer_ID            STRING REFERENCES SNOWPARK_DB.GOLD_SCH.DIM_CUSTOMER(Customer_ID),
    Card_ID                STRING REFERENCES SNOWPARK_DB.GOLD_SCH.DIM_CARD(Card_ID),
    Merchant_ID            STRING REFERENCES SNOWPARK_DB.GOLD_SCH.DIM_MERCHANT(Merchant_ID),
    Location_ID            STRING REFERENCES SNOWPARK_DB.GOLD_SCH.DIM_LOCATION(Location_ID),
    Transaction_Date_ID    DATE REFERENCES SNOWPARK_DB.GOLD_SCH.DIM_DATE(Date_ID)
);

Select distinct TRANSACTION_STATUS from SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING;

Select distinct TRANSACTION_TYPE from SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING;

Select * from SNOWPARK_DB.GOLD_SCH.FACT_TRANSACTIONS;
Select * from SNOWPARK_DB.GOLD_SCH.DIM_DATE;
Select * from SNOWPARK_DB.GOLD_SCH.DIM_LOCATION;
Select * from SNOWPARK_DB.GOLD_SCH.DIM_CARD where CARD_TIER is Null ;
Select * from SNOWPARK_DB.GOLD_SCH.DIM_MERCHANT;
Select * from SNOWPARK_DB.GOLD_SCH.DIM_CUSTOMER;

Select distinct(merchant_name) from SNOWPARK_DB.GOLD_SCH.DIM_MERCHANT;

----------------------------------------------------------------------------------
-- 0 0 * * * UTC
-- │ │ │ │ │
-- │ │ │ │ └── Day of week (every day)
-- │ │ │ └──── Month (every month)
-- │ │ └────── Day of month (every day)
-- │ └──────── Hour = 0 (i.e., midnight)
-- └────────── Minute = 0


-- Task 1: Copy from Stage to Bronze Layer
CREATE OR REPLACE TASK SNOWPARK_DB.CONFIG_SCH.TASK_COPY_TO_BRONZE
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Optional: remove if using AFTER chaining only
AS
  CALL SNOWPARK_DB.CONFIG_SCH.COPY_STAGE_TO_BRONZE();

-- Task 2: Bronze to Silver (runs after Task 1)
CREATE OR REPLACE TASK SNOWPARK_DB.CONFIG_SCH.TASK_BRONZE_TO_SILVER
  WAREHOUSE = COMPUTE_WH
  AFTER SNOWPARK_DB.CONFIG_SCH.TASK_COPY_TO_BRONZE
AS
  CALL SNOWPARK_DB.CONFIG_SCH.BRONZE_TO_SILVER();

-- Task 3: Silver to Gold (runs after Task 2)
CREATE OR REPLACE TASK SNOWPARK_DB.CONFIG_SCH.TASK_SILVER_TO_GOLD
  WAREHOUSE = COMPUTE_WH
  AFTER SNOWPARK_DB.CONFIG_SCH.TASK_BRONZE_TO_SILVER
AS
  CALL SNOWPARK_DB.CONFIG_SCH.SILVER_TO_GOLD();

SHOW TASKS;

-- Enable all tasks
ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_COPY_TO_BRONZE RESUME;
ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_BRONZE_TO_SILVER RESUME;
ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_SILVER_TO_GOLD RESUME;

-- Trigger the root task once (manually)
EXECUTE TASK SNOWPARK_DB.CONFIG_SCH.TASK_COPY_TO_BRONZE;

ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_COPY_TO_BRONZE SUSPEND;
ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_BRONZE_TO_SILVER SUSPEND;
ALTER TASK SNOWPARK_DB.CONFIG_SCH.TASK_SILVER_TO_GOLD SUSPEND;