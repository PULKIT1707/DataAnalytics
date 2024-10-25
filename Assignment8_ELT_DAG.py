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
def combineTables():
    cur = return_snowflake_conn()

    create_blob_query = """
    CREATE OR REPLACE TABLE analytics.session_summary AS(
    SELECT 
        usc.userId,
        usc.sessionId,
        usc.channel,
        st.ts
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st
    ON usc.session_id = st.session_id
    WHERE usc.user_id IS NOT NULL
    );
    """
    cur.execute(create_blob_query)
    cur.close()

with DAG(
    dag_id='stocks_forecast_next_few_days',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    combineTables = combineTables()
    