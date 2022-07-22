from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from function import read_data
with DAG('snowflake', description='snowflake',
         schedule_interval='@once',
         start_date=datetime(2017, 3, 20), catchup=False) as dag:
    task_write_csv=PythonOperator(task_id='write_csv',python_callable=read_data)
    task_create_db=SnowflakeOperator(task_id='task_create_db',snowflake_conn_id='snowflake_conn',sql="""create or replace TABLE NEW_DATABASES.PUBLIC.RAW_TABLE(
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
            CREATE OR REPLACE TABLE NEW_DATABASES.PUBLIC.MASTER_TABLE LIKE NEW_DATABASES.PUBLIC.RAW_TABLE;""")
    task_create_stream=SnowflakeOperator(task_id='task_create_stream', snowflake_conn_id='snowflake_conn', sql="""
    create or replace stream NEW_DATABASES.PUBLIC.RAW_STREAM on table NEW_DATABASES.PUBLIC.RAW_TABLE;
    create or replace stream NEW_DATABASES.PUBLIC.STAGE_STREAM on table NEW_DATABASES.PUBLIC.STAGE_TABLE;""")
    task_insert_into_stage = SnowflakeOperator(task_id='task_insert_stage', snowflake_conn_id='snowflake_conn', sql="""
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
    """)
    task_insert_into_master = SnowflakeOperator(task_id='task_insert_master', snowflake_conn_id='snowflake_conn', sql="""
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
        """)
task_create_db>>task_create_stream>>task_write_csv>>task_insert_into_stage>>task_insert_into_master