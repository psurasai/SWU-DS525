import os
import glob
from sqlite3 import Timestamp
from typing import List
import json
from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook

year=2017

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS final_table (
        order_id text
        , customer_id text
        , seller_id text
        , product_id text
        , order_status text
        , month int
        , payment_type text
        , customer_state text
        , seller_state text
        , product_category_name_english text
        , payment_value decimal
        , price decimal
        , freight_value decimal
    )"""
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift
AWS_ACCESS_KEY_ID = "ASIAZIOCTQSURF24Z7OE"
AWS_SECRET_ACCESS_KEY = "Qvci92wpZxgONnzo9GjH9Y+dxtGGLIHU0HEjqioB"
AWS_SESSION_TOKEN = "FwoGZXIvYXdzEBoaDEU2kS8E3CuoQAXJriLNAWrPraQLKZ1hf8i+PHVCG9Wwjlk7xtmM04AIDfX8AF+XG90m6bDkNJHuvjbC7cAoD6kZ29JCBLy+0v19Wtk2eoPFP+TwVBRKI2RCKJky9k4rSc1dzFaMUyFiY+3DBhxRTyQncXmz1Bk8/pjEf9oMdqknfLoqFhrTXphRoA/GPweT/sSof5zS2ZsCNgsrr/QSiZGwlcxdDNU1E67IC1RN/2DEI19tnJcRuLok309LS6eLGBLmR59gYSMWsyC/wVKt+5B3YW0D8GwEVxs/R8kojpj7nAYyLRZJx6R3yjYtUpc7nvhEQZnkJ38MfpRksnxYMrVMMzwaeoxsFHn7MryJ1/bTGw=="

copy_table_queries = [
    """
    COPY final_table
    FROM 's3://brazilian-bucket-final/final_table/year={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """
]

truncate_table_queries = [
    """
    TRUNCATE TABLE final_table
    """
]

insert_dwh_queries = [
    """
    INSERT INTO final_table 
    Select
        *
    FROM final_table
    """
]

host = "redshift-cluster-1.ce1ofdjly7ll.us-east-1.redshift.amazonaws.com"
port = "5439"
dbname = "dev"
user = "awsuser"
password = "Ixaajph7"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def _truncate_datalake_tables():
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()

def _load_staging_tables():
    for query in copy_table_queries:
        cur.execute(query.format(year, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN))
        conn.commit()

def _insert_dwh_tables():
    for query in insert_dwh_queries:
        cur.execute(query)
        conn.commit()

with DAG(
    'Capstone',
    start_date = timezone.datetime(2022, 12, 1), # Start of the flow
    schedule = '@monthly', # Run once a month at midnight of the first day of the month
    tags = ['capstone'],
    catchup = False, # No need to catchup the missing run since start_date
) as dag:


    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = _create_tables,
    )

    truncate_datalake_tables = PythonOperator(
    task_id = 'truncate_datalake_tables',
    python_callable = _truncate_datalake_tables,
    )

    load_staging_tables = PythonOperator(
        task_id = 'load_staging_tables',
        python_callable = _load_staging_tables,
    )


    insert_dwh_tables = PythonOperator(
        task_id = 'insert_dwh_tables',
        python_callable = _insert_dwh_tables,
    )

    create_tables >> truncate_datalake_tables >> load_staging_tables >> insert_dwh_tables