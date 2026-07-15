# ==============================================================================
# EXTERNAL DATA PIPELINE: Block Group + Block Boundary Geometry (TIGER/Line)
# Author: Paul Park, Claude Code
# Objective: Ingest Census TIGER/Line boundary shapefiles into Bronze/Silver:
#   - Block Group geometry -> feeds gold.dim_bg.geometry
#   - Block geometry        -> feeds the neighborhood union query directly
# ==============================================================================
import os
import sys
import zipfile
import requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection

con = get_md_connection()


def download_and_extract(url, data_dir, zip_path, shp_path):
    if os.path.exists(shp_path):
        print(f"   Using cached shapefile at {shp_path}")
        return
    os.makedirs(data_dir, exist_ok=True)
    if not os.path.exists(zip_path):
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"   Downloaded to {zip_path}")
    else:
        print(f"   Using cached zip at {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(data_dir)
    print(f"   Extracted to {data_dir}")


con.execute("INSTALL spatial; LOAD spatial;")

# ==============================================================================
# PART 1: BLOCK GROUP geometry -> feeds gold.dim_bg.geometry
# ==============================================================================
BG_URL      = "https://www2.census.gov/geo/tiger/TIGER2023/BG/tl_2023_37_bg.zip"
BG_DATA_DIR = "data/tl_2023_37_bg"
BG_ZIP_PATH = "data/tl_2023_37_bg.zip"
BG_SHP_PATH = f"{BG_DATA_DIR}/tl_2023_37_bg.shp"

print("1. Downloading NC block group boundary shapefile...")
download_and_extract(BG_URL, BG_DATA_DIR, BG_ZIP_PATH, BG_SHP_PATH)

print("2. Ingesting block group shapefile into Bronze layer...")
con.execute(f"""
    CREATE OR REPLACE TABLE bronze.bg_geometry AS
    SELECT * FROM ST_Read('{BG_SHP_PATH}');
    FORCE CHECKPOINT;
""")

print("3. Standardizing block group geometry in Silver layer...")
con.execute("""
    CREATE OR REPLACE VIEW silver.v_bronze_bg_geometry AS
    SELECT * FROM bronze.bg_geometry;

    CREATE OR REPLACE TABLE silver.bg_geometry AS
    SELECT
        GEOID AS block_group_GEOID,
        ST_AsGeoJSON(geom) AS geometry_geojson
    FROM silver.v_bronze_bg_geometry
    WHERE COUNTYFP = '119';

    DROP VIEW IF EXISTS silver.v_bronze_bg_geometry;
    FORCE CHECKPOINT;
""")
print("   Block group geometry complete.")


# ==============================================================================
# PART 2: BLOCK geometry -> feeds the neighborhood union query
# ==============================================================================
BLOCK_URL      = "https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/tl_2023_37_tabblock20.zip"
BLOCK_DATA_DIR = "data/tl_2023_37_tabblock20"
BLOCK_ZIP_PATH = "data/tl_2023_37_tabblock20.zip"
BLOCK_SHP_PATH = f"{BLOCK_DATA_DIR}/tl_2023_37_tabblock20.shp"

print("4. Downloading NC 2020 Census Block boundary shapefile...")
download_and_extract(BLOCK_URL, BLOCK_DATA_DIR, BLOCK_ZIP_PATH, BLOCK_SHP_PATH)

print("5. Ingesting block shapefile into Bronze layer...")
con.execute(f"""
    CREATE OR REPLACE TABLE bronze.block_geometry AS
    SELECT * FROM ST_Read('{BLOCK_SHP_PATH}');
    FORCE CHECKPOINT;
""")

print("6. Standardizing block geometry in Silver layer...")
con.execute("""
    CREATE OR REPLACE VIEW silver.v_bronze_block_geometry AS
    SELECT * FROM bronze.block_geometry;

    CREATE OR REPLACE TABLE silver.block_geometry AS
    SELECT
        GEOID20 AS block_GEOID,
        NAME20 AS block_name,
        ST_AsGeoJSON(geom) AS geometry_geojson
    FROM silver.v_bronze_block_geometry
    WHERE COUNTYFP20 = '119';

    DROP VIEW IF EXISTS silver.v_bronze_block_geometry;
    FORCE CHECKPOINT;
""")
print("   Block geometry complete.")

con.close()