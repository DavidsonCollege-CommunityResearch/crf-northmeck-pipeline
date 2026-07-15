# ==============================================================================
# North Meck Insights Data Warehouse Pipeline Code
#
# Author: Paul Park, Gemini Code
# Objective: Build a robust data warehouse pipeline for North Meck Insights using DuckDB and Polars, replicating R's tidycensus functionality in Python. The pipeline fetches ACS data, processes it into a tidy format, and constructs a Kimball-style star schema for analytics.
# ==============================================================================

import time
import os
import polars as pl
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection
from functions.tidycensus_replicator import run_ingestion_pipeline, fetch_acs_metadata

CENSUS_API_KEY = "d95612e8a94c486ec6e95901c5996f39dcf9240f"
STATE_FIPS = "37"
COUNTY_FIPS = "119"

# ==============================================================================
# STEP 1: BRONZE LAYER
# Objective: Create code that replicates the R tidycensus package's functionality to fetch ACS data and store it in a raw format in DuckDB./ Ingest raw data into data warehouse (Bronze Layer) using Polars and DuckDB.
# ==============================================================================

# ------------------------------------------------------------------------------
# Step 1: Defining variables
# ------------------------------------------------------------------------------

acs_variables_raw = [
    # --- 0. Core Demographics ---
    "B01003_001", "B01002_001", "B02001_002", "B02001_003", 
    "B02001_005", "B03003_003", "B19013_001", "B25064_001", 
    "B25077_001", "B17010_002", "B23025_005", "B21001_002",
    
    # --- 1. Housing ---
    "B25001_001", "B25003_002", "B25003_003", "B25071_001", 
    "B25070_007", "B25070_008", "B25070_009", "B25070_010", 
    "B25004_001",

    # --- 2. Education (B14001 & B15003 FULL SET) ---
    "B14001_001", "B14001_002", "B14001_003", "B14001_004", "B14001_005", 
    "B14001_006", "B14001_007", "B14001_008", "B14001_009", "B14001_010",
    "B15003_001", "B15003_002", "B15003_003", "B15003_004", "B15003_005", 
    "B15003_006", "B15003_007", "B15003_008", "B15003_009", "B15003_010", 
    "B15003_011", "B15003_012", "B15003_013", "B15003_014", "B15003_015", 
    "B15003_016", "B15003_017", "B15003_018", "B15003_019", "B15003_020", 
    "B15003_021", "B15003_022", "B15003_023", "B15003_024", "B15003_025",
    
    # --- 3. Childcare ---
    "B23008_001", "B23008_002", "B23008_015", "B23008_016", 
    "B11005_007", "B10002_002", "B11005_002",
    
    # --- 4. Healthcare ---
    "B27010_001", "B18101_001", "B18101_002", "B18101_021", "B27010_003", 
    "B27010_010", "B27010_017", "B27010_019", "B27010_026", "B27010_033", 
    "B27010_035", "B27010_042", "B27010_050", "B27010_052", "B27010_058", 
    "B27010_066",

    # --- [Addition] B18101 Sex by Age by Disability Status — full age breakdown ---
    "B18101_004", "B18101_005", "B18101_007", "B18101_008",
    "B18101_010", "B18101_011", "B18101_013", "B18101_014",
    "B18101_016", "B18101_017", "B18101_019", "B18101_020",
    "B18101_023", "B18101_024", "B18101_026", "B18101_027",
    "B18101_029", "B18101_030", "B18101_032", "B18101_033",
    "B18101_035", "B18101_036", "B18101_038", "B18101_039",
    
    # --- 5. Transportation ---
    "B08301_001", "B08301_003", "B08301_010", "B08301_021", 
    "B25044_001", "B08201_002", "B08136_001", "B25044_003", 
    "B25044_010", "B08301_018", "B08301_019",
    
    # --- 6. Food Security & Economic Mobility ---
    "B22001_001", "B22001_002", "B17001_001", "B17001_002", 
    "B17001_004", "B19083_001", "B19057_002",
    
    # --- Mobility and Digital Access ---
    "B07003_004", "B25038_009", "B05002_013", "C16002_004", 
    "B28002_002", "B28002_013", "B28001_011",
    
    # --- [Addition] Teammate's Core Household Income Brackets (B19001) ---
    "B19001_001", "B19001_002", "B19001_003", "B19001_004", "B19001_005", 
    "B19001_006", "B19001_007", "B19001_008", "B19001_009", "B19001_010", 
    "B19001_011", "B19001_012", "B19001_013", "B19001_014", "B19001_015", 
    "B19001_016", "B19001_017",

    # --- [Addition] Teammate's Employment Status & Health Insurance (B27011) ---
    "B27011_002", "B27011_004", "B27011_007", "B27011_009", "B27011_012", 
    "B27011_014", "B27011_017",

    # --- [Addition] Teammate's Income Bracket & Health Insurance (B27015) ---
    "B27015_001", "B27015_003", "B27015_006", "B27015_008", "B27015_011", 
    "B27015_013", "B27015_016", "B27015_018", "B27015_021", "B27015_023", 
    "B27015_026",

    # --- [Addition] Teammate's PDF Requirement: Health Insurance Status by Sex and Age (B27001) ---
    "B27001_001", "B27001_004", "B27001_005", "B27001_007", "B27001_008", 
    "B27001_010", "B27001_011", "B27001_013", "B27001_014", "B27001_016", 
    "B27001_017", "B27001_019", "B27001_020", "B27001_022", "B27001_023", 
    "B27001_025", "B27001_026", "B27001_028", "B27001_029", "B27001_032", 
    "B27001_033", "B27001_035", "B27001_036", "B27001_038", "B27001_039", 
    "B27001_041", "B27001_042", "B27001_044", "B27001_045", "B27001_047", 
    "B27001_048", "B27001_050", "B27001_051", "B27001_053", "B27001_054", 
    "B27001_056", "B27001_057",

    # --- [Addition] Teammate's PDF Requirement: Detailed Types of Health Insurance by Age (B27010) ---
    "B27010_004", "B27010_005", "B27010_006", "B27010_007", "B27010_008", 
    "B27010_009", "B27010_011", "B27010_012", "B27010_013", "B27010_014", 
    "B27010_015", "B27010_016", "B27010_020", "B27010_021", "B27010_022", 
    "B27010_023", "B27010_024", "B27010_025", "B27010_027", "B27010_028", 
    "B27010_029", "B27010_030", "B27010_031", "B27010_032", "B27010_036", 
    "B27010_037", "B27010_038", "B27010_039", "B27010_040", "B27010_041", 
    "B27010_043", "B27010_044", "B27010_045", "B27010_046", "B27010_047", 
    "B27010_048", "B27010_049", "B27010_053", "B27010_054", "B27010_055", 
    "B27010_056", "B27010_057", "B27010_059", "B27010_060", "B27010_061", 
    "B27010_062", "B27010_063", "B27010_064", "B27010_065",

    # --- [Addition] B25038 Tenure by Year Householder Moved Into Unit (full breakdown) ---
    "B25038_002", "B25038_003", "B25038_004", "B25038_005",
    "B25038_006", "B25038_007", "B25038_008",
    "B25038_010", "B25038_011", "B25038_012", "B25038_013",
    "B25038_014", "B25038_015",

    # --- [Addition] B23008 Age of Own Children by Employment Status of Parents (full breakdown) ---
    "B23008_004", "B23008_005", "B23008_006", "B23008_007",
    "B23008_010", "B23008_011", "B23008_013", "B23008_014",
    "B23008_017", "B23008_018", "B23008_019", "B23008_020",
    "B23008_023", "B23008_024", "B23008_026", "B23008_027",
    # --- [Addition] Block Group Demographics Enrichment ---

# B01001: Sex by Age (male/female + age groups)
"B01001_002",  # Male total
"B01001_026",  # Female total
# Under 18
"B01001_003", "B01001_004", "B01001_005", "B01001_006",  # Male under 5, 5-9, 10-14, 15-17
"B01001_027", "B01001_028", "B01001_029", "B01001_030",  # Female under 5, 5-9, 10-14, 15-17
# Working age 18-64
"B01001_007", "B01001_008", "B01001_009", "B01001_010",  # Male 18-19, 20, 21, 22-24
"B01001_011", "B01001_012", "B01001_013", "B01001_014",  # Male 25-29, 30-34, 35-39, 40-44
"B01001_015", "B01001_016", "B01001_017", "B01001_018", "B01001_019",  # Male 45-49, 50-54, 55-59, 60-61, 62-64
"B01001_031", "B01001_032", "B01001_033", "B01001_034",  # Female 18-19, 20, 21, 22-24
"B01001_035", "B01001_036", "B01001_037", "B01001_038",  # Female 25-29, 30-34, 35-39, 40-44
"B01001_039", "B01001_040", "B01001_041", "B01001_042", "B01001_043",  # Female 45-49, 50-54, 55-59, 60-61, 62-64
# Senior 65+
"B01001_020", "B01001_021", "B01001_022", "B01001_023", "B01001_024", "B01001_025",  # Male 65-66, 67-69, 70-74, 75-79, 80-84, 85+
"B01001_044", "B01001_045", "B01001_046", "B01001_047", "B01001_048", "B01001_049",  # Female 65-66, 67-69, 70-74, 75-79, 80-84, 85+

# C16002: Household Language (limited English)
"C16002_001",  # Total households
"C16002_007",  # Other Indo-European, limited English
"C16002_010",  # Asian and Pacific Islander, limited English
"C16002_013",  # Other, limited English

# B21001: Veteran status
"B21001_001",  # Total civilian population 18+

# --- [Addition] S1501 Educational Attainment (place level only — subject table) ---
"S1501_C01_001", "S1501_C01_002", "S1501_C01_003", "S1501_C01_004", "S1501_C01_005",
"S1501_C01_016", "S1501_C01_017", "S1501_C01_018", "S1501_C01_019", "S1501_C01_020",
"S1501_C01_021", "S1501_C01_022", "S1501_C01_023", "S1501_C01_024", "S1501_C01_025",
"S1501_C01_026", "S1501_C01_027", "S1501_C01_028", "S1501_C01_029", "S1501_C01_030",
"S1501_C01_031", "S1501_C01_032", "S1501_C01_033", "S1501_C01_034", "S1501_C01_035",
"S1501_C01_036", "S1501_C01_037", "S1501_C01_038", "S1501_C01_039", "S1501_C01_040",
"S1501_C01_041", "S1501_C01_042", "S1501_C01_043", "S1501_C01_044", "S1501_C01_045",
"S1501_C01_046", "S1501_C01_047", "S1501_C01_048", "S1501_C01_049", "S1501_C01_050",
"S1501_C01_051", "S1501_C01_052", "S1501_C01_053", "S1501_C01_054", "S1501_C01_055",
"S1501_C01_056", "S1501_C01_057", "S1501_C01_058", "S1501_C01_059", "S1501_C01_060",
"S1501_C01_061", "S1501_C01_062", "S1501_C01_063", "S1501_C01_064",
"S1501_C02_006", "S1501_C02_007", "S1501_C02_008", "S1501_C02_009",
"S1501_C02_023", "S1501_C02_024", "S1501_C02_026", "S1501_C02_027",
"S1501_C02_029", "S1501_C02_030", "S1501_C02_032", "S1501_C02_033",
"S1501_C02_035", "S1501_C02_036", "S1501_C02_037", "S1501_C02_038",
"S1501_C02_044", "S1501_C02_045", "S1501_C02_046", "S1501_C02_047",
"S1501_C02_050", "S1501_C02_051", "S1501_C02_053", "S1501_C02_054",
"S1501_C02_056", "S1501_C02_057", "S1501_C02_059", "S1501_C02_060",
"S1501_C02_062", "S1501_C02_063"
]

target_years = list(range(2018,2025))

# Extraction
raw_bg = run_ingestion_pipeline(target_years, acs_variables_raw, "block group", CENSUS_API_KEY, STATE_FIPS, COUNTY_FIPS)
raw_place = run_ingestion_pipeline(target_years, acs_variables_raw, "place", CENSUS_API_KEY, STATE_FIPS, COUNTY_FIPS)
raw_meta = fetch_acs_metadata(target_years)
# ------------------------------------------------------------------------------
# Step 3: Connecting to local DuckDB
# ------------------------------------------------------------------------------
con = get_md_connection()
con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

con.register("bg_view", raw_bg)
con.register("place_view", raw_place)
con.register("meta_view", raw_meta)

con.execute("CREATE OR REPLACE TABLE bronze.acs_blockgroup AS SELECT * FROM bg_view;")
con.execute("CREATE OR REPLACE TABLE bronze.acs_place AS SELECT * FROM place_view;")
con.execute("CREATE OR REPLACE TABLE bronze.acs_metadata AS SELECT * FROM meta_view;")
print("Successfully ingested raw ACS data into DuckDB Bronze layer.")

# ==============================================================================
# STEP 2: SILVER LAYER Data Cleaning (Pure SQL)
# Objective: Transform the raw Bronze data into a tidy, wide format suitable for analytics. This includes creating a variable crosswalk and pivoting the data into a Kimball-style fact table structure.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Create Silver Schema & Base Isolation Views
# ------------------------------------------------------------------------------
print("1. Creating silver schema and base isolation views...")
con.execute("""
    CREATE SCHEMA IF NOT EXISTS silver;

    CREATE OR REPLACE VIEW v_bronze_acs_bg AS
    SELECT * FROM bronze.acs_blockgroup;

    CREATE OR REPLACE VIEW v_bronze_acs_place AS 
    SELECT * FROM bronze.acs_place;

    CREATE OR REPLACE VIEW v_bronze_acs_metadata AS 
    SELECT * FROM bronze.acs_metadata;
""")

# ------------------------------------------------------------------------------
# 2. Store long-format ACS data in Silver layer
# Wide PIVOT is handled per concept in Gold layer
# ------------------------------------------------------------------------------
print("2. Cleaning acs_bg/ acs_place")
con.execute("""
    CREATE OR REPLACE TABLE silver.acs_bg AS
    SELECT
        GEOID,
        TRIM(NAME)                              AS NAME,
        REGEXP_REPLACE(variable, '[EM]$', '')   AS variable,
        CAST(vintage_year AS INTEGER)           AS vintage_year,
        CASE WHEN estimate = -666666666 THEN NULL
             ELSE TRY_CAST(estimate AS DOUBLE)  END AS estimate,
        CASE WHEN moe = -666666666 THEN NULL
             ELSE TRY_CAST(moe AS DOUBLE)       END AS moe
    FROM v_bronze_acs_bg;

    CREATE OR REPLACE TABLE silver.acs_place AS
    SELECT
        GEOID,
        TRIM(NAME)                              AS NAME,
        REGEXP_REPLACE(variable, '[EM]$', '')   AS variable,
        CAST(vintage_year AS INTEGER)           AS vintage_year,
        CASE WHEN estimate = -666666666 THEN NULL
             ELSE TRY_CAST(estimate AS DOUBLE)  END AS estimate,
        CASE WHEN moe = -666666666 THEN NULL
             ELSE TRY_CAST(moe AS DOUBLE)       END AS moe
    FROM v_bronze_acs_place
    WHERE REGEXP_MATCHES(LOWER(NAME), 'davidson town|cornelius town|huntersville town');

    FORCE CHECKPOINT;
""")

# ------------------------------------------------------------------------------
# 3. Build Automated Variable Crosswalk Table
# ------------------------------------------------------------------------------
print("3. Building metadata-driven automated variable crosswalk...")
con.execute("""
-- ==============================================================================
-- Silver Layer: Variable Crosswalk (v7)
-- Structure: year, name, variable, label_clean, concept, table_name
-- label_clean is used as the column name inside each fact table.
-- table_name is snake_case prefixed with fact_ (e.g. fact_household_income)
-- used directly as the Gold layer fact table name with _bg or _town suffix.
-- E variables only — MOE columns are derived at PIVOT time in Gold layer.
-- ==============================================================================
 
CREATE OR REPLACE TABLE silver.variable_crosswalk AS
 
WITH raw AS (
    SELECT
        year,
        name,                                           -- e.g. B01003_001E  (JOIN key)
        REGEXP_REPLACE(name, 'E$', '') AS variable,     -- e.g. B01003_001   (base code)
        label,
        concept
    FROM bronze.acs_metadata
    WHERE name LIKE '%E'
      AND name NOT IN ('for', 'in') -- Filter to only variables we actually ingested in the Silver layer
      AND REGEXP_REPLACE(name, 'E$', '') IN (
        SELECT DISTINCT variable FROM silver.acs_bg
        UNION
        SELECT DISTINCT variable FROM silver.acs_place
)),
 
cleaned AS (
    SELECT
        year,
        name,
        variable,
 
        -- Strip "Estimate!!" prefix, replace !! hierarchy separators with spaces, remove trailing colon
        TRIM(BOTH '_' FROM
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                LOWER(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(label, '^Estimate!!', ''),
                            '!!', ' ', 'g'),
                        ':', '', 'g') 
                ),
                '[^a-z0-9]+', '_', 'g'),
            '_+', '_', 'g')
    ) AS label_clean,
 
        -- Remove inflation-adjustment boilerplate from concept and cap at 200 chars
        SUBSTR(
            TRIM(REGEXP_REPLACE(concept, '[(]in [0-9]{4} Inflation-Adjusted Dollars[)]', '', 'g')),
            1, 200
        ) AS concept_clean
            
 
    FROM raw
)
 
SELECT
    year,
    name,           -- B01003_001E  (JOIN key for Silver → Gold)
    variable,       -- B01003_001   (base code reference)
    label_clean,    -- used as column name inside each fact table
    concept_clean   AS concept,
 
    -- table_name in snake_case prefixed with fact_
    -- e.g. fact_household_income, fact_types_of_health_insurance_coverage_by_age
    -- Gold layer appends _bg or _town to form the final table name
    'fact_' || TRIM(BOTH '_' FROM
        REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(COALESCE(concept_clean, 'misc')), '[^a-z0-9]+', '_', 'g'),
            '_+', '_', 'g')
    ) AS table_name
 
FROM cleaned;
""")



# ------------------------------------------------------------------------------
# 4. Clean up & Checkpoint
# ------------------------------------------------------------------------------
print("4. Forcing database checkpoint and syncing blocks to disk...")
con.execute("""
    DROP VIEW IF EXISTS v_bronze_acs_place; 
    DROP VIEW IF EXISTS v_bronze_acs_bg; 
    DROP VIEW IF EXISTS v_bronze_acs_metadata;
    
    FORCE CHECKPOINT;
""")

con.close()
print("🎉 Step 2 COMPLETE: Silver Layer built successfully from Bronze Layer using pure SQL!")








