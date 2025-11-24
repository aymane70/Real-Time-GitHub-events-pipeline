#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import psycopg2



host = "postgres"
port = 5432
username = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
database = os.environ.get("POSTGRES_DB")
def init_database():
    conn = psycopg2.connect(
        dbname=database,
        user=username,
        password=password,
        host=host,
        port=port
    )

    conn.rollback()

    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS github_events (
        id TEXT PRIMARY KEY,
        type TEXT,
        repo_name TEXT,
        actor_login TEXT,
        created_at TIMESTAMP
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_database()
