from snowflake.snowpark import Session
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"sdas-EU77200",
        "USER":"asdasd",
        "PASSWORD":"asdasda@2025",
        "ROLE":"ACCOUNTADMIN",
        "DATABASE":"SNOWPARK_DB",
        "SCHEMA":"Bronze_sch",
        "WAREHOUSE":"COMPUTE_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()   


def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    schema_df = session.sql("Show Schemas;")
    context_df.show(2)
    schema_df.show()
if __name__ == '__main__':
    main()  