"""Run SQL migration 002 on Supabase."""
import os, sys, re, httpx
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

URL = os.environ["SUPABASE_URL"]
KEY = os.environ["SUPABASE_SERVICE_KEY"]

sql = open(os.path.join(os.path.dirname(__file__), "db", "migrations", "002_dashboard_functions.sql")).read()

# Execute each function creation as a single statement via pg REST
# Supabase Management API or SQL Editor needed — try using postgrest rpc
# Actually we can create functions via the Supabase HTTP API using the /rest/v1/rpc endpoint
# But we need to use the Supabase Management API for DDL.

# Best approach: use psycopg2 or the query endpoint
# Let's try via the Supabase query endpoint (requires service_role)
resp = httpx.post(
    f"{URL}/rest/v1/rpc/",
    headers={
        "apikey": KEY,
        "Authorization": f"Bearer {KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    },
    json={},
    timeout=30,
)
print(f"RPC test: {resp.status_code}")

# If that doesn't work, we'll split and try each function
# via a temporary SQL execution function
print("\n--- Creating exec_sql helper function ---")
exec_sql_fn = """
create or replace function exec_sql(query text)
returns void language plpgsql security definer as $$
begin execute query; end;
$$;
"""
# We can't create a function without already having one...
# Let's check if we have DATABASE_URL for direct connection
db_url = os.environ.get("DATABASE_URL", "").strip()
if db_url:
    print("Using DATABASE_URL with psycopg2...")
    import psycopg2
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print("All functions created successfully!")
else:
    print("\nNo DATABASE_URL set. Please run 002_dashboard_functions.sql in the Supabase SQL Editor.")
    print(f"URL: {URL.replace('.supabase.co', '.supabase.co')}/project/default/sql")
