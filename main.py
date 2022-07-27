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
    task_create_db=SnowflakeOperator(task_id='task_create_db',snowflake_conn_id='conn_id',sql="create or replace TABLE NEW_DATABASES.PUBLIC.RAW_TABLE")
task_create_db>>task_write_csv