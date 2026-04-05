
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


---------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SNOWPARK_DB.CONFIG_SCH.SILVER_TO_GOLD()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'silver_to_gold_main'
AS
$$
from datetime import datetime, timedelta
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import col

# 1. Get Snowpark Session
def get_snowpark_session():
    from snowflake.snowpark.session import Session
    return Session.builder.getOrCreate()

# 2. Minimal SCD Type 1 Upsert Logic
from snowflake.snowpark import Session, DataFrame

def upsert_scd1(session: Session, src_df: DataFrame, target_table: str, pk_cols: list):
    # Register source DataFrame as a temporary table
    src_df.write.save_as_table("TEMP_SRC", mode="overwrite")

    # Get list of all columns from the DataFrame
    all_cols = src_df.columns

    # Build ON condition using primary keys
    on_expr = " AND ".join([f"T.{col} = S.{col}" for col in pk_cols])

    # Build SET clause for update
    update_set = ", ".join([f"{col} = S.{col}" for col in all_cols if col not in pk_cols])

    # Build column list and values list for insert
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"S.{col}" for col in all_cols])

    # Construct the MERGE statement
    merge_sql = f"""
        MERGE INTO {target_table} T
        USING TEMP_SRC S
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    # Run the SQL in Snowflake
    session.sql(merge_sql).collect()

    # Optional cleanup
    session.sql("DROP TABLE IF EXISTS TEMP_SRC").collect()


# 3. Create/Refresh Date Dimension Table
def refresh_dim_date(session, start="2000-01-01", end="2035-12-31"):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    delta = end_date - start_date

    rows = []
    for i in range(delta.days + 1):
        current = start_date + timedelta(days=i)
        rows.append(Row(
            Date_ID=current.date(),
            Year=current.year,
            Month=current.month,
            Day=current.day,
            Week=current.isocalendar()[1],
            Quarter=(current.month - 1)//3 + 1,
            Weekday=current.strftime("%A")
        ))

    df = session.create_dataframe(rows)
    df.write.mode("overwrite").save_as_table("SNOWPARK_DB.GOLD_SCH.DIM_DATE")

# 4. Silver → Gold Star Schema Logic
def distribute_to_gold(session):
    df = session.table("SNOWPARK_DB.SILVER_SCH.SILVER_TRANSACTIONS")

    # Dimension tables
    upsert_scd1(
        session,
        df.select("Customer_ID", "Customer_Name", "Gender", "DOB", "Email", "Phone", "Customer_City").distinct(),
        "SNOWPARK_DB.GOLD_SCH.DIM_CUSTOMER",
        ["Customer_ID"]
    )

    upsert_scd1(
        session,
        df.select("Merchant_ID", "Merchant_Name", "Merchant_Category", "Merchant_Country").distinct(),
        "SNOWPARK_DB.GOLD_SCH.DIM_MERCHANT",
        ["Merchant_ID"]
    )

    upsert_scd1(
        session,
        df.select("Card_ID", "Card_Type", "Issuer_Bank", "Card_Tier", "Expiry_Date").distinct(),
        "SNOWPARK_DB.GOLD_SCH.DIM_CARD",
        ["Card_ID"]
    )

    upsert_scd1(
        session,
        df.select("Location_ID", "City", "State", "Country").distinct(),
        "SNOWPARK_DB.GOLD_SCH.DIM_LOCATION",
        ["Location_ID"]
    )

    # Fact table
    fact_df = df.select(
        "Transaction_ID",
        "Transaction_Date",
        "Transaction_Amount",
        "Transaction_Status",
        "Transaction_Type",
        "Customer_ID",
        "Card_ID",
        "Merchant_ID",
        "Location_ID",
        col("Transaction_Date").alias("Transaction_Date_ID")
    )

    fact_df.write.mode("overwrite").save_as_table("SNOWPARK_DB.GOLD_SCH.FACT_TRANSACTIONS")

# 5. Main handler
def silver_to_gold_main():
    session = get_snowpark_session()
    refresh_dim_date(session)
    distribute_to_gold(session)
    return "Silver → Gold star schema processing complete."
$$;


-- Run it
CALL SNOWPARK_DB.CONFIG_SCH.SILVER_TO_GOLD();