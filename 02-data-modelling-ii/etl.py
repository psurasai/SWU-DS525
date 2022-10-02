import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster

#drop the exist tables
table_drop_events = "DROP TABLE events"
table_drop_actors = "DROP TABLE actors"

#create tables
table_create_events = """
    CREATE TABLE IF NOT EXISTS events
    (
        id text,
        type VARCHAR,
        actor_id text,
        actor VARCHAR,
        public boolean,
        created_at timestamp,
        PRIMARY KEY (
            (actor_id),
            created_at
        )
    )
"""
table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors
    (
        actor_id text,
        actor VARCHAR,
        number_events int,
        PRIMARY KEY (
            actor_id
        )
    )
"""

create_table_queries = [
    table_create_events,
    table_create_actors
]
drop_table_queries = [
    table_drop_events,
    table_drop_actors
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def get_files(filepath: str) -> List[str]:
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


def process(session, filepath):
    """
    Description: This function is used for insert data into events table
    """
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"],each["public"],each["created_at"])

                # Insert data into tables here
                query_events = f"""
                    INSERT INTO events (
                        id,
                        type,
                        actor_id,
                        actor,
                        public,
                        created_at
                    ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}', '{each["actor"]["login"]}',
                    {each["public"]},'{each["created_at"]}')
                    """
                session.execute(query_events)


def insert_sample_data(session):
    """
    Description: This function is used for query data from events table and count the events of each actors.
    Then, the query data are inserted into actors table.
    """
    #Query data from event table and count number of the events of each actor
    count_actors = """
    SELECT actor_id, actor, count(actor_id) as count_no from events GROUP BY actor_id ALLOW FILTERING
    """

    try:
        rows = session.execute(count_actors)
        for row in rows:
        # Insert data into actor tables here
            query_actors = f"""
                INSERT INTO actors (
                actor_id,
                actor,
                number_events
                ) VALUES ('{row[0]}', '{row[1]}', {row[2]});
                """
            session.execute(query_actors)
        #print(row)
    except Exception as e:
        print(e)

def main():
    #Connect to cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    #drop and create table
    drop_tables(session)
    create_tables(session)

    #insert data to events and actors tables
    process(session, filepath="../data")
    #insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    #Try to query data from event
    query = """
    SELECT * from events WHERE type = 'PushEvent' ALLOW FILTERING
    """
    try:
        print("query PushEvent")
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)

    # Select data in Cassandra and print them
    #Try to query data from actors
    query = """
    SELECT actor_id, actor, number_events from actors 
    WHERE number_events > 1 ALLOW FILTERING
    """
    try:
        print("query number of events by each actor if number of event more than 1")
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)

if __name__ == "__main__":
    main()