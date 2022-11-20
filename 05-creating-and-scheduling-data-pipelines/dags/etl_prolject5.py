import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    #create the actors table that consists of 6 columns and assign id as the primary key
    table_create_actors = """
        CREATE TABLE IF NOT EXISTS actors (
            id bigint NOT NULL,
            login text,
            display_login text,
            gravatar_id text,
            url text,
            avatar_url text,
            PRIMARY KEY(id)
        )
    """
    #create the repo table that consists of 3 columns and assign id as the primary key
    table_create_repo = """
        CREATE TABLE IF NOT EXISTS repo(
            id bigint NOT NULL,
            name text,
            url text,
            PRIMARY KEY(id)
        )
    """

    #create the org table that consists of 5 columns and assign id as the primary key
    table_create_org = """
        CREATE TABLE IF NOT EXISTS org (
            id bigint NOT NULL,
            login text,
            gravatar_id text,
            url text,
            avatar_url text,
            PRIMARY KEY(id)
        )
    """

    #create the event table that consists of 7 columns and assign id as the primary key 
    #and actor_id, repo_id, org_id as foreign key of actor, repo and org tables, respectively
    table_create_events = """
        CREATE TABLE IF NOT EXISTS events (
            id bigint,
            type text,
            actor_id bigint,
            repo_id bigint,
            public text,
            created_at text,
            org_id bigint,
            PRIMARY KEY(id),
            CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id),
            CONSTRAINT fk_repo FOREIGN KEY(repo_id) REFERENCES repo(id),
            CONSTRAINT fk_org FOREIGN KEY(org_id) REFERENCES org(id)
        )
    """
    create_table_queries = [
    table_create_actors,
    table_create_repo,
    table_create_org,
    table_create_events,
    ]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    #connect to Postgres
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    #insert data into each table
    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                        each["payload"]["issue"]["url"],
                    )
                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                    )

                # Insert data into tables here
                insert_actors_statement = f"""
                    INSERT INTO actors (
                        id,
                        login,
                        display_login,
                        gravatar_id,
                        url,
                        avatar_url
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}',
                    '{each["actor"]["display_login"]}','{each["actor"]["gravatar_id"]}',
                    '{each["actor"]["url"]}','{each["actor"]["avatar_url"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_actors_statement)

                # Insert data into repo table here
                insert_repo_statement = f"""
                    INSERT INTO repo (
                        id,
                        name,
                        url
                    ) VALUES ({each["repo"]["id"]}, '{each["repo"]["name"]}',
                    '{each["repo"]["url"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_repo_statement)

                # Try insert data into org and event tables here
                try:
                    insert_org_statement = f"""
                        INSERT INTO org (
                            id,
                            login,
                            gravatar_id,
                            url,
                            avatar_url)
                        VALUES ({each["org"]["id"]}, '{each["org"]["login"]}',
                        '{each["org"]["gravatar_id"]}',
                        '{each["org"]["url"]}','{each["org"]["avatar_url"]}')
                        ON CONFLICT (id) DO NOTHING
                    """
                    # print(insert_statement)
                    cur.execute(insert_org_statement)

                # Insert data into event table here
                    insert_statement = f"""
                        INSERT INTO events (
                            id,
                            type,
                            actor_id,
                            repo_id,
                            public,
                            created_at,
                            org_id
                        ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}',
                        '{each["repo"]["id"]}','{each["public"]}','{each["created_at"]}','{each["org"]["id"]}')
                        ON CONFLICT (id) DO NOTHING
                    """
                # print(insert_statement)
                    cur.execute(insert_statement)

                # Insert data which does not have org data into event table here
                except:
                
                    insert_statement = f"""
                        INSERT INTO events (
                            id,
                            type,
                            actor_id,
                            repo_id,
                            public,
                            created_at
                        ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}',
                        '{each["repo"]["id"]}','{each["public"]}','{each["created_at"]}')
                        ON CONFLICT (id) DO NOTHING
                    """
                # print(insert_statement)
                    cur.execute(insert_statement)

                conn.commit()

#create DAG in airflow 
with DAG(
    #name of DAG
    "etl",
    #assign the start date, scheduling and tags
    start_date=timezone.datetime(2022, 10, 30),
    schedule="@daily",
    tags=["workshop","DS525"],
    catchup=False,
) as dag:

    #create task to get file from folder 'data' 
    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    #create task to create table from function '_create_tables'
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    #create task to insert data into table from function '_process'
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )
    
    #create process flow
    get_files >> create_tables >> process

    #end