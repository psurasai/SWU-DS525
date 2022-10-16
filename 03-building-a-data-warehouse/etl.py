import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS staging_events",
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS repos",
]

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        actor_id bigint,
        actor_name text,
        actor_url text,
        repo_id bigint,
        repo_name text,
        repo_url text,
        public boolean,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text,
        actor text,
        repo text,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        id bigint,
        name text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repos (
        id bigint,
        name text,
        url text
    )
    """,
]

copy_table_queries = [
    """
    COPY staging_events FROM 's3://wit-swu-lab3/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::636600157353:role/LabRole'
    JSON 's3://wit-swu-lab3/events_json_path.json'
    REGION 'us-east-1'
    """,
]

insert_table_queries = [
    """
    INSERT INTO events ( id, type, actor, repo, created_at )
    SELECT DISTINCT id, type, actor_name, repo_name, created_at
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM events)
    """,
    """
    INSERT INTO actors ( id, name, url )
    SELECT DISTINCT actor_id, actor_name, actor_url
    FROM staging_events
    WHERE actor_id NOT IN (SELECT DISTINCT id FROM actors)
    """,
    """
    INSERT INTO repos ( id, name, url )
    SELECT DISTINCT repo_id, repo_name, repo_url
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM repos)
    """,
]


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.cak93pn3g0gg.us-east-1.redshift.amazonaws.com"
    port = "5439"
    dbname = "dev"
    user = "awsuser"
    password = "awsPassword1"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # query data
    query = "select * from events"
    cur.execute(query)
    # print data
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()
