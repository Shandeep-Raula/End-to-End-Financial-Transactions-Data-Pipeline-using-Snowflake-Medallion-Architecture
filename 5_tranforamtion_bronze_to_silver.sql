CREATE OR REPLACE PROCEDURE SNOWPARK_DB.CONFIG_SCH.BRONZE_TO_SILVER()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'bronze_to_silver_main'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark.functions import (
    col, initcap, trim, lower, upper, regexp_replace, regexp_extract,
    when, to_date, lit, current_date, datediff, length
)
from datetime import datetime, timedelta

def transform_transaction_data(df, session):
    df = df.with_column("DOB", to_date(col("DOB"))) \
           .with_column("Transaction_Date", to_date(col("Transaction_Date"))) \
           .with_column("Expiry_Date", to_date(col("Expiry_Date"))) \
           .filter((datediff('day', col("DOB"), current_date()) > 0) &
                   (datediff('day', col("Expiry_Date"), current_date()) < 0))

    text_fields = [
        "Customer_Name", "Merchant_Name", "Customer_City", "City", "State",
        "Country", "Merchant_Country", "Issuer_Bank", "Card_Type"
    ]
    for field in text_fields:
        df = df.with_column(field, initcap(trim(regexp_replace(col(field), r"[^a-zA-Z\s]", ""))))
    df = df.with_column("Card_Tier", upper(trim(col("Card_Tier"))))

    df = df.dropna(subset=["Transaction_ID", "Customer_ID", "Card_ID"]) \
           .drop_duplicates(["Transaction_ID"])

    df = df.with_column("Transaction_Amount", col("Transaction_Amount").cast("float")) \
           .filter(col("Transaction_Amount") > 0)

    df = df.with_column(
        "Email",
        when(col("Email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lower(trim(col("Email"))))
        .otherwise(lit(None))
    )

    df = df.with_column(
        "Phone",
        when((col("Phone").rlike(r"^\d+$")) & (length(col("Phone")) >= 10), col("Phone"))
        .otherwise(lit(None))
    )

    df = df.with_column(
        "Transaction_Status",
        when(lower(trim(col("Transaction_Status"))) == "sucsess", "Success")
        .when(lower(trim(col("Transaction_Status"))) == "faild", "Failed")
        .otherwise(initcap(trim(col("Transaction_Status"))))
    )

    df = df.with_column(
        "Transaction_Type",
        when(lower(trim(col("Transaction_Type"))) == "pos", "POS")
        .when(lower(trim(col("Transaction_Type"))) == "atm", "ATM")
        .when(lower(trim(col("Transaction_Type"))) == "online", "Online")
        .otherwise(upper(trim(col("Transaction_Type"))))
    )

    placeholders = ["-", "null", "unknown", "N/A", "na", "none", "None"]
    string_fields = [
        "Customer_Name", "Merchant_Name", "Customer_City", "City", "State",
        "Country", "Merchant_Country", "Issuer_Bank", "Card_Type",
        "Transaction_Status", "Transaction_Type", "Email", "Phone"
    ]
    for field in string_fields:
        df = df.with_column(
            field,
            when(col(field).isin(placeholders), lit(None)).otherwise(col(field))
        )

    df = df.with_column("State", regexp_replace(col("State"), "Tmil Nadu", "Tamil Nadu")) \
           .with_column("Country", regexp_replace(col("Country"), "Inda", "India"))

    df = df.with_column(
        "Is_Valid_Record",
        when((col("Transaction_Amount") > 0) &
             (datediff('day', current_date(), col("DOB")) < 0) &
             (datediff('day', col("Expiry_Date"), current_date()) > 0),
             lit(True)).otherwise(lit(False))
    )

    df = df.with_column("City", initcap(trim(col("City")))) \
           .with_column("State", initcap(trim(col("State"))))

    df = df.with_column(
        "State",
        when(col("City") == "Bangalore", lit("Karnataka"))
        .when(col("City") == "Mumbai", lit("Maharashtra"))
        .when(col("City") == "Chennai", lit("Tamil Nadu"))
        .when(col("City") == "Delhi", lit("Delhi"))
        .when(col("City") == "Kolkata", lit("West Bengal"))
        .when(col("City") == "Hyderabad", lit("Telangana"))
        .when(col("City") == "Lucknow", lit("Uttar Pradesh"))
        .when(col("City") == "Ahmedabad", lit("Gujarat"))
        .when(col("City") == "Chandigarh", lit("Punjab"))
        .when(col("City") == "Kochi", lit("Kerala"))
        .otherwise(col("State"))
    )

    df = df.with_column(
        "Location_ID",
        when((col("City") == "Bangalore") & (col("State") == "Karnataka"), lit("L1001"))
        .when((col("City") == "Mumbai") & (col("State") == "Maharashtra"), lit("L1002"))
        .when((col("City") == "Chennai") & (col("State") == "Tamil Nadu"), lit("L1003"))
        .when((col("City") == "Delhi") & (col("State") == "Delhi"), lit("L1004"))
        .when((col("City") == "Kolkata") & (col("State") == "West Bengal"), lit("L1005"))
        .when((col("City") == "Hyderabad") & (col("State") == "Telangana"), lit("L1006"))
        .when((col("City") == "Lucknow") & (col("State") == "Uttar Pradesh"), lit("L1007"))
        .when((col("City") == "Ahmedabad") & (col("State") == "Gujarat"), lit("L1008"))
        .when((col("City") == "Chandigarh") & (col("State") == "Punjab"), lit("L1009"))
        .when((col("City") == "Kochi") & (col("State") == "Kerala"), lit("L1010"))
        .otherwise(lit("L9999"))
    )

    df = df.with_column(
        "Transaction_Status",
        when(lower(trim(col("Transaction_Status"))).isin("sucsess", "success"), lit("Success"))
        .when(lower(trim(col("Transaction_Status"))).isin("fail", "failed"), lit("Failed"))
        .when(lower(trim(col("Transaction_Status"))) == "fraudulent", lit("Fraudulent"))
        .otherwise(lit("Unknown"))
    )

    df = df.with_column(
        "Transaction_Type",
        when(lower(trim(col("Transaction_Type"))).isin("atm"), lit("ATM"))
        .when(lower(trim(col("Transaction_Type"))).isin("pos"), lit("POS"))
        .when(lower(trim(col("Transaction_Type"))).isin("online", "onlne"), lit("ONLINE"))
        .otherwise(lit("UNKNOWN"))
    )

    df = df.with_column(
        "Gender",
        when(upper(trim(col("Gender"))) == "M", lit("Male"))
        .when(upper(trim(col("Gender"))) == "F", lit("Female"))
        .otherwise(lit("Other"))
    )

    df = df.with_column(
        "Customer_Name",
        when(col("Customer_Name").is_null(), regexp_extract(col("Email"), r"^([^@]+)", 1))
        .otherwise(col("Customer_Name"))
    )

    return df


def load_to_silver_table(df, session):
    target_columns = [
        "Transaction_ID", "Transaction_Date", "Transaction_Amount", "Transaction_Status",
        "Transaction_Type", "Customer_ID", "Customer_Name", "Gender", "DOB", "Email", "Phone",
        "Customer_City", "Card_ID", "Card_Type", "Issuer_Bank", "Card_Tier", "Expiry_Date",
        "Merchant_ID", "Merchant_Name", "Merchant_Category", "Merchant_Country", "Location_ID",
        "City", "State", "Country"
    ]
    final_df = df.select([col(c) for c in target_columns])
    final_df.write.mode("overwrite").save_as_table("SNOWPARK_DB.SILVER_SCH.SILVER_TRANSACTIONS")


def bronze_to_silver_main(session) -> str:
    df_raw = session.table("SNOWPARK_DB.BRONZE_SCH.RAW_TRANSACTIONS_STAGING")
    df_transformed = transform_transaction_data(df_raw, session)
    load_to_silver_table(df_transformed, session)
    return "Bronze to Silver processing complete."
$$;

Select * from SNOWPARK_DB.SILVER_SCH.SILVER_TRANSACTIONS;

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

CALL SNOWPARK_DB.CONFIG_SCH.BRONZE_TO_SILVER();

