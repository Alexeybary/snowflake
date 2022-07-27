from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from function import read_data,sql_create_db, sql_insert_stage,sql_insert_master,sql_create_stream
with DAG('snowflake', description='snowflake',
         schedule_interval='@once',
         start_date=datetime(2017, 3, 20), catchup=False) as dag:
    task_write_csv=PythonOperator(task_id='write_csv',python_callable=read_data)
    task_create_db=SnowflakeOperator(task_id='task_create_db',snowflake_conn_id='snowflake_conn',sql=sql_create_db)
    task_create_stream=SnowflakeOperator(task_id='task_create_stream', snowflake_conn_id='snowflake_conn', sql=sql_create_stream)
    task_insert_into_stage = SnowflakeOperator(task_id='task_insert_stage', snowflake_conn_id='snowflake_conn', sql=sql_insert_stage)
    task_insert_into_master = SnowflakeOperator(task_id='task_insert_master', snowflake_conn_id='snowflake_conn', sql=sql_insert_master)
task_create_db>>task_create_stream>>task_write_csv>>task_insert_into_stage>>task_insert_into_master