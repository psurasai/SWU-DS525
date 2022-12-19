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

year=2005

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS final_table (
        Accident_Index text
        , a.Longitude
        , a.Latitude
        , a.Police_Force
        , a.Accident_Severity
        , a.Number_of_Vehicles
        , a.Number_of_Casualties
        , year(a.Date) as year
        , a.Date as date
        , a.Time as time
        , a.Day_of_Week
        , a.1st_Road_Class
        , a.Road_Type
        , a.Speed_limit
        , a.Junction_Control
        , a.Pedestrian_Crossing-Human_Control
        , a.Pedestrian_Crossing-Physical_Facilities
        , a.Light_Conditions
        , a.Weather_Conditions
        , a.Road_Surface_Conditions
        , a.Special_Conditions_at_Site
        , a.Carriageway_Hazards
        , a.Urban_or_Rural_Area
        , a.Did_Police_Officer_Attend_Scene_of_Accident
        , c.Vehicle_Reference
        , c.Sex_of_Casualty
        , c.Age_of_Casualty
        , c.Age_Band_of_Casualty
        , c.Casualty_Severity
        , c.Car_Passenger
        , c.Bus_or_Coach_Passenger
        , v.Vehicle_Reference
        , v.Vehicle_Type
        , v.Towing_and_Articulation
        , v.Junction_Location
        , v.Engine_Capacity_(CC)
        , v.Age_of_Vehicle
        , v.Driver_Home_Area_Type

    )"""
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift
AWS_ACCESS_KEY_ID = "ASIAZIOCTQSUVUI2PPNB"
AWS_SECRET_ACCESS_KEY = "GY+U2LayKZVeL7YbGT3Nc8s4r4rEtvVwAHoaZXux"
AWS_SESSION_TOKEN = "FwoGZXIvYXdzEDYaDHx/4AOgK2/sB7Cs9CLNAWJqjRkkxO7fuFfBe1bddmLRoyEvbjp5n8xhb/7ZSXNz4LZrOB1Pi2u3LkFP9dwzOQIbgg5K84eqpRrquz2Yb2P0nPjJbiJHbQBLP6w3P+eXp5iYmH7cWtnqI2qxB9otD9Cay/ByK7nQyeUWgR1uMf8wAo8Dg2cqq10ZI7XZvklS4he9Hk9EyIX0Rad8kuMYJpNneSyStmKno8BMhGEEMwI7WyU/haH5VpywwdO/CeAg6A540DUuYJa3uOCsHjS70rb+VhljDgUneKdXCKco9rGBnQYyLZyvUfDBA7Qgg7iFjlXYNiOwFC8RU9ZsmlBCyTH/JBS7I52afNg1tldebLXxNg=="


copy_table_queries = [
    """
    COPY final_table
    FROM 's3://uk-car-accidents/cleaned/year={0}'
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
        cur.execute(query.format(year, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN))
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