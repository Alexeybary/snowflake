import os
import re
import pandas as pd
import json
import snowflake.connector as sf
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



def read_data():
    datapath = json.load(open("snow.json"))
    print(datapath)
    data = pd.read_csv(datapath['path_to_data1'])
    hook =SnowflakeHook(snowflake_conn_id="snowflake_conn",database="NEW_DATABASES")
    engine=hook.get_sqlalchemy_engine()
    connection=engine.connect()
    data.to_sql('RAW_TABLE', if_exists='append',con=engine,index=False,chunksize=15000)
    connection.close()
    engine.dispose()