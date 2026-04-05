CREATE OR REPLACE PROCEDURE SNOWPARK_DB.CONFIG_SCH.COPY_STAGE_TO_BRONZE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main_copy_data'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

def ingest_in_transcation_csv(session):
    session.sql("""
        COPY INTO SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING
        FROM (
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
            (FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_CSV_FORMAT')
        )
        ON_ERROR = 'CONTINUE'
    """).collect()

def ingest_transaction_json(session):
    session.sql("""
        COPY INTO SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING
        FROM (
            SELECT
                $1:"Transaction_ID"::STRING,
                $1:"Transaction_Date"::DATE,
                $1:"Transaction_Amount"::NUMBER(10,2),
                $1:"Transaction_Status"::STRING,
                $1:"Transaction_Type"::STRING,
                $1:"Customer_ID"::STRING,
                $1:"Customer_Name"::STRING,
                $1:"Gender"::STRING,
                $1:"DOB"::DATE,
                $1:"Email"::STRING,
                $1:"Phone"::STRING,
                $1:"Customer_City"::STRING,
                $1:"Card_ID"::STRING,
                $1:"Card_Type"::STRING,
                $1:"Issuer_Bank"::STRING,
                $1:"Card_Tier"::STRING,
                $1:"Expiry_Date"::DATE,
                $1:"Merchant_ID"::STRING,
                $1:"Merchant_Name"::STRING,
                $1:"Merchant_Category"::STRING,
                $1:"Merchant_Country"::STRING,
                $1:"Location_ID"::STRING,
                $1:"City"::STRING,
                $1:"State"::STRING,
                $1:"Country"::STRING,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED
            FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/json/
            (FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_JSON_FORMAT')
        )
        ON_ERROR = 'CONTINUE'
    """).collect()

def ingest_transaction_parquet(session):
    session.sql("""
        COPY INTO SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING
        FROM (
            SELECT
                $1:"Transaction_ID"::STRING,
                $1:"Transaction_Date"::DATE,
                $1:"Transaction_Amount"::NUMBER(10,2),
                $1:"Transaction_Status"::STRING,
                $1:"Transaction_Type"::STRING,
                $1:"Customer_ID"::STRING,
                $1:"Customer_Name"::STRING,
                $1:"Gender"::STRING,
                $1:"DOB"::DATE,
                $1:"Email"::STRING,
                $1:"Phone"::STRING,
                $1:"Customer_City"::STRING,
                $1:"Card_ID"::STRING,
                $1:"Card_Type"::STRING,
                $1:"Issuer_Bank"::STRING,
                $1:"Card_Tier"::STRING,
                $1:"Expiry_Date"::DATE,
                $1:"Merchant_ID"::STRING,
                $1:"Merchant_Name"::STRING,
                $1:"Merchant_Category"::STRING,
                $1:"Merchant_Country"::STRING,
                $1:"Location_ID"::STRING,
                $1:"City"::STRING,
                $1:"State"::STRING,
                $1:"Country"::STRING,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED
            FROM @SNOWPARK_DB.BRONZE_SCH.EXT_STAGE/snowpark_project/parquet/
            (FILE_FORMAT => 'SNOWPARK_DB.CONFIG_SCH.MY_PARQUET_FORMAT')
        )
        ON_ERROR = 'CONTINUE'
    """).collect()

def main_copy_data(session: Session) -> str:
    ingest_in_transcation_csv(session)
    ingest_transaction_json(session)
    ingest_transaction_parquet(session)
    return "All formats ingested to Bronze layer successfully."
$$;



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

Select * from SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING;


CALL SNOWPARK_DB.CONFIG_SCH.COPY_STAGE_TO_BRONZE();
