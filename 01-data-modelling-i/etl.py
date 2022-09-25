import glob
import json
import os
from typing import List

import psycopg2


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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"], each["actor"]["login"])

                
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

                # Insert data into actors tables
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
                insert_repos_statement = f"""
                    INSERT INTO repo (
                        id,
                        name,
                        url
                    ) VALUES ({each["repo"]["id"]}, '{each["repo"]["name"]}',
                    '{each["repo"]["url"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_repos_statement)

                # Try insert data into org and events tables here
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

                # Insert data into events table here
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



def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()