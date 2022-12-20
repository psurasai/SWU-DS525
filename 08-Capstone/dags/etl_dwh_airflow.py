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

Day_of_Week = 1

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS final_table (
        Accident_Index text
        , longitude decimal
        , latitude decimal
        , police_force text
        , accident_severity text
        , number_of_vehicles text
        , number_of_casualties text
        , date date
        , time time
        , day_of_week text
        , first_road_class int
        , road_type text
        , speed_limit int
        , junction_control text
        , light_conditions text
        , weather_conditions text
        , road_surface_conditions text
        , special_conditions_at_site text
        , carriageway_harzards text
        , urban_or_rural text
        , police_attend_scene text
        , sex_of_casualty int
        , age_of_casualty int
        , age_band_of_casualty text
        , casualty_severity text
        , car_passenger int
        , bus_or_coach_passenger int
        , vehicle_ref text
        , vehicle_type text
        , towing_arti text
        , junction_location text
        , age_of_vehicle int
        , driver_home_area text

    )"""
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift
aws_access_key_id = "ASIAZIOCTQSU3M3LTABM"
aws_secret_access_key = "DlD0GAMvLs4Maj8hogHLqFjCQb03705YvT2rMwHv"
aws_session_token = "FwoGZXIvYXdzEE4aDJNI6TStaRTBgtOHbiLNAWbPQNye7khgJHW5NoVGRO0nHuAHF1qOwnNDtqaeHT9G9UyVIpaSIVZ0yDKzxfYXCicaS+dIm5+s0xluwxZWiXqsrZmDQxV8uWIRV89+b6q8FfXLo0XmcUiSs0IA7bLHI3ueuTXksI9QlH848HdZY0QSGrfX/ggFIzcXNZdBzw/eTeKrVN4hhdEVIBYK0CevC/v/DTrvCGF5GMbeFSHt6tME1xG0JqKB3y5U0r78n3bqAkZJcUkqUd1nk/74x48r7ufk3hZa3SeYhy5oWhgoodyGnQYyLWcJD0gjUOXubq8xXJN9jdEGjlXw1TR/yc/+8PWMgrrqWVYFCRfgKL14jwtEew=="

copy_table_queries = [
    """
    COPY final_table
    FROM 's3://uk-car-accidents/cleaned/Day_of_Week={0}'
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

host = "redshift-cluster-1.cak93pn3g0gg.us-east-1.redshift.amazonaws.com"
port = "5439"
dbname = "dev"
user = "awsuser"
password = "awsPassword1"
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
        cur.execute(query.format(Day_of_Week, aws_access_key_id, aws_secret_access_key, aws_session_token))
        conn.commit()

def _insert_dwh_tables():
    for query in insert_dwh_queries:
        cur.execute(query)
        conn.commit()

with DAG(
    'Capstone',
    start_date = timezone.datetime(2005, 1, 1), # Start of the flow
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