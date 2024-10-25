from airflow import DAG
from airflow.decorators import task
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta, datetime
import snowflake.connector
import requests
import pandas as pd
import numpy as np


def return_snowflake_conn():
    user = 'pulkit1707'
    password = '@17PulSri'
    account = "tufnasm-xnb90038"

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse= "stock_warehouse",
        database = "dev",
        schema = "raw_data"
    )
    return conn.cursor()

@task
def createUserSessionChannel():
    cur = return_snowflake_conn()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
    );
    """
    cur.execute(create_table_query)
    cur.close()

@task
def createdSessionTimeStamp():
    cur = return_snowflake_conn()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
    );
    """
    cur.execute(create_table_query)
    cur.close()

@task
def createBlob():
    cur = return_snowflake_conn()

    create_blob_query = """
    CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """
    cur.execute(create_blob_query)
    cur.close()

@task
def copyToSessionChannel():
    cur = return_snowflake_conn()

    copy_query = """
    COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv;
    """
    cur.execute(copy_query)
    cur.close()

@task
def copyToSessionTimeStamp():
    cur = return_snowflake_conn()

    copy_query = """
    COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv;
    """
    cur.execute(copy_query)
    cur.close()

with DAG(
    dag_id='stocks_forecast_next_few_days',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    createUserSessionChannel = createUserSessionChannel()
    createdSessionTimeStamp = createdSessionTimeStamp()
    createBlob = createBlob()
    copyToSessionChannel = copyToSessionChannel()
    copyToSessionTimeStamp = copyToSessionTimeStamp()
