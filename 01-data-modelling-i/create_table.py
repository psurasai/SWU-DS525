from typing import NewType

import psycopg2


PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actors = "DROP TABLE IF EXISTS actors"
table_drop_repo = "DROP TABLE IF EXISTS repo"
table_drop_org = "DROP TABLE IF EXISTS orgs"

#Create actors table with 6 columns and id as primary key
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

#Create repos table with 4 columns and id as primary key
table_create_repo = """
    CREATE TABLE IF NOT EXISTS repo (
        id bigint NOT NULL,
        name text,
        url VARCHAR,
        PRIMARY KEY(id)
    )
"""

#Create repo table with 6 columns and id as primary key
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

#Create repos table with 7 columns and id as primary key
#Constraint actor_id, repo_id, org_id as foreign key of actor, repo and org tables, respectively
table_create_events = """
    CREATE TABLE IF NOT EXISTS events (
        id bigint NOT NULL,
        type text,
        actor_id int,
        repo_id int,
        public text,
        created_at text,
        org_id int,
        PRIMARY KEY(id),
        CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id),
        CONSTRAINT fk_repos FOREIGN KEY(repo_id) REFERENCES repo(id),
        CONSTRAINT fk_orgs FOREIGN KEY(org_id) REFERENCES org(id)
    )
"""

create_table_queries = [
    table_create_actors,
    table_create_repo,
    table_create_org,
    table_create_events
]
drop_table_queries = [
    table_drop_events,
    table_drop_actors,
    table_drop_repo,
    table_drop_org

]


def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()