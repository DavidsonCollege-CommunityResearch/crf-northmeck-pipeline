# ==============================================================================
# POST-PIPELINE PROCESS: Cloud to Local Sync
# Author: Paul Park, Gemini Code
# Objective: Clone the finished MotherDuck cloud database to a local .duckdb file 
#            for offline analysis, BI tool connection, and backup.
# ==============================================================================

import os
import duckdb
from functions.mother_duck_connector import get_md_connection

# 1. Remove old local database file
local_db_path = 'nmidw_local_backup.duckdb'
if os.path.exists(local_db_path):
    os.remove(local_db_path)
    print(f"🗑️ Removed old local database: {local_db_path}")

# 2. Connect to the Cloud DB using the secure function
con = get_md_connection()

# 3. Create and attach local database
print(f"📂 Creating fresh local database: {local_db_path}...")
con.execute(f"ATTACH '{local_db_path}' AS local_db;")

# 4. Replicate schema structures
schemas = ['bronze', 'silver', 'gold', 'main']
for schema in schemas:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS local_db.{schema};")

# 5. Materialize all cloud tables
print("🔄 Syncing data from Cloud to Local...")
objects = con.execute("""
    SELECT DISTINCT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_catalog = 'nmidw'
      AND table_schema IN ('bronze', 'silver', 'gold', 'main')
""").fetchall()

for schema, name in objects:
    print(f"   -> Copying {schema}.{name}...")
    con.execute(f"CREATE OR REPLACE TABLE local_db.{schema}.{name} AS SELECT * FROM nmidw.{schema}.{name};")

# 6. Finalize and cleanup
con.execute("FORCE CHECKPOINT local_db;")
con.execute("DETACH local_db;")
con.close()

print(f"\n🎉 SUCCESS: Cloud database fully cloned to local file '{local_db_path}'!")