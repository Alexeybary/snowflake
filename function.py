import os
import re
import pandas as pd
import json
import snowflake.connector as sf
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



def read_data():
    with open("snow.json") as f:
        datapath=json.load(f)
    data = pd.read_csv(datapath['path_to_data1'])
    hook =SnowflakeHook(snowflake_conn_id="snowflake_conn",database="NEW_DATABASES")
    engine=hook.get_sqlalchemy_engine()
    connection=engine.connect()
    data.to_sql('RAW_TABLE', if_exists='append',con=engine,index=False,chunksize=15000)
    connection.close()
    engine.dispose()
sql_create_db= """create or replace TABLE NEW_DATABASES.PUBLIC.RAW_TABLE(
            "_ID" VARCHAR(16777216),
            "IOS_App_Id" NUMBER(38,0),
            "Title" VARCHAR(16777216),
            "Developer_Name" VARCHAR(16777216),
            "Developer_IOS_Id" FLOAT,
            "IOS_Store_Url" VARCHAR(16777216),
            "Seller_Official_Website" VARCHAR(16777216),
            "Age_Rating" VARCHAR(16777216),
            "Total_Average_Rating" FLOAT,
            "Total_Number_of_Ratings" FLOAT,
            "Average_Rating_For_Version" FLOAT,
            "Number_of_Ratings_For_Version" NUMBER(38,0),
            "Original_Release_Date" VARCHAR(16777216),
            "Current_Version_Release_Date" VARCHAR(16777216),
            "Price_USD" FLOAT,
            "Primary_Genre" VARCHAR(16777216),
            "All_Genres" VARCHAR(16777216),
            "Languages" VARCHAR(16777216),
            "Description" VARCHAR(16777216)
            );
            create or replace TABLE NEW_DATABASES.PUBLIC.STAGE_TABLE like NEW_DATABASES.PUBLIC.RAW_TABLE;
            CREATE OR REPLACE TABLE NEW_DATABASES.PUBLIC.MASTER_TABLE LIKE NEW_DATABASES.PUBLIC.RAW_TABLE;"""
sql_insert_stage="""
    insert into NEW_DATABASES.PUBLIC.STAGE_TABLE select "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description" from NEW_DATABASES.PUBLIC.RAW_STREAM
    """
sql_insert_master="""
            insert into NEW_DATABASES.PUBLIC.MASTER_TABLE select "_ID",
                    "IOS_App_Id",
                    "Title",
                    "Developer_Name",
                    "Developer_IOS_Id",
                    "IOS_Store_Url",
                    "Seller_Official_Website",
                    "Age_Rating",
                    "Total_Average_Rating",
                    "Total_Number_of_Ratings",
                    "Average_Rating_For_Version",
                    "Number_of_Ratings_For_Version",
                    "Original_Release_Date",
                    "Current_Version_Release_Date",
                    "Price_USD",
                    "Primary_Genre",
                    "All_Genres",
                    "Languages",
                    "Description" from NEW_DATABASES.PUBLIC.STAGE_STREAM
            """
sql_create_stream= """
    create or replace stream NEW_DATABASES.PUBLIC.RAW_STREAM on table NEW_DATABASES.PUBLIC.RAW_TABLE;
    create or replace stream NEW_DATABASES.PUBLIC.STAGE_STREAM on table NEW_DATABASES.PUBLIC.STAGE_TABLE;"""