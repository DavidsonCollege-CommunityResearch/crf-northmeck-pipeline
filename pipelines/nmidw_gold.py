# ==============================================================================
# GOLD LAYER PIPELINE
# Author: Paul Park, Claude Code
# Objective: Build the Kimball-style star schema (dimension and concept-based
#            fact tables) from the cleaned Silver layer.
# ==============================================================================

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection

con = get_md_connection()
 
# ==============================================================================
# STEP 1: Schema Setup & Dimension Tables
# ==============================================================================
print("1. Creating Gold schema and Dimension tables...")
con.execute("""
    -- Drop and recreate the entire Gold schema to avoid FK constraint conflicts
    -- from concept-based fact tables that may exist from previous pipeline runs
    DROP SCHEMA IF EXISTS gold CASCADE;
    CREATE SCHEMA gold;
 
    -- Date dimension: full spine from 1990-01-01 to 2050-12-31
    CREATE TABLE gold.dim_date (
        date_key         DATE PRIMARY KEY,
        calendar_year    INTEGER,
        calendar_quarter INTEGER,
        calendar_month   INTEGER,
        calendar_day     INTEGER,
        month_name       VARCHAR(50),
        day_name         VARCHAR(50),
        is_weekend       BOOLEAN
    );
    INSERT INTO gold.dim_date
    WITH RECURSIVE data_spine AS (
        SELECT CAST('1990-01-01' AS DATE) AS date_key
        UNION ALL
        SELECT date_key + INTERVAL 1 DAY FROM data_spine WHERE date_key < CAST('2050-12-31' AS DATE)
    )
    SELECT
        date_key,
        YEAR(date_key), QUARTER(date_key), MONTH(date_key), DAY(date_key),
        MONTHNAME(date_key), DAYNAME(date_key),
        CASE WHEN DAYNAME(date_key) IN ('Saturday','Sunday') THEN TRUE ELSE FALSE END
    FROM data_spine;
 
    -- Year dimension: derived from dim_date
    CREATE TABLE gold.dim_year (year_key INTEGER PRIMARY KEY, year_label VARCHAR(10));
    INSERT INTO gold.dim_year
    SELECT DISTINCT calendar_year, CAST(calendar_year AS TEXT) FROM gold.dim_date ORDER BY 1;
 
    -- Block Group dimension: extracts tract and county hierarchy via regex
    -- geometry is a GeoJSON string (from TIGER2023, via silver.bg_geometry).
    -- NULL for retired pre-2020 block group boundaries that no longer exist
    -- in current TIGER geography -- this is expected, not a bug: only the
    -- 2018/2019 ACS vintage years used the old boundaries (200 retired
    -- GEOIDs each); 2020-2024 vintage years match today's TIGER file 1:1
    -- (confirmed 624/624 matched -- see nmidw_bg_geometry_diagnostic.py).
            
    CREATE TABLE gold.dim_bg (
        block_group_GEOID  VARCHAR(50) PRIMARY KEY,
        block_group_name   VARCHAR(50),
        census_tract_GEOID VARCHAR(50),
        census_tract_name  VARCHAR(50),
        county_GEOID       VARCHAR(50),
        county_name        VARCHAR(50),
        geometry            VARCHAR
    );
    INSERT INTO gold.dim_bg
    SELECT DISTINCT
        a.GEOID,
        REGEXP_EXTRACT(a.NAME, '^(Block Group [0-9]+)', 1),
        SUBSTR(a.GEOID, 1, 11),
        REGEXP_EXTRACT(a.NAME, '(Census Tract [0-9\\.]+)', 1),
        SUBSTR(a.GEOID, 1, 5),
        REGEXP_EXTRACT(a.NAME, '(Mecklenburg County)', 1),
        g.geometry_geojson
    FROM silver.acs_bg AS a
    LEFT JOIN silver.bg_geometry AS g ON a.GEOID = g.block_group_GEOID;
            


     -- Block dimension: derived from TIGER 2020 Census Blocks (silver.block_geometry).
    -- No FK to dim_bg -- kept minimal (GEOID, name, geometry only). Not
    -- referenced by any fact table; used directly by the neighborhood
    -- boundary union query.
    CREATE TABLE gold.dim_block (
        block_GEOID  VARCHAR(50) PRIMARY KEY,
        block_name   VARCHAR(50),
        geometry      VARCHAR
    );
    INSERT INTO gold.dim_block
    SELECT
        block_GEOID,
        block_name,
        geometry_geojson
    FROM silver.block_geometry;
            

    -- Town dimension: maps place GEOIDs to standardized town names
    CREATE TABLE gold.dim_town (place_GEOID VARCHAR(50) PRIMARY KEY, town_name VARCHAR(50));
    INSERT INTO gold.dim_town
    SELECT DISTINCT GEOID,
        CASE
            WHEN LOWER(NAME) LIKE '%davidson%'     THEN 'Davidson'
            WHEN LOWER(NAME) LIKE '%cornelius%'    THEN 'Cornelius'
            WHEN LOWER(NAME) LIKE '%huntersville%' THEN 'Huntersville'
            ELSE 'Other'
        END
    FROM silver.acs_place;
 
    -- County dimension: single-row reference for Mecklenburg County
    CREATE TABLE gold.dim_county (county_GEOID VARCHAR(50) PRIMARY KEY, county_name VARCHAR(50));
    INSERT INTO gold.dim_county VALUES ('37119', 'Mecklenburg County')
    ON CONFLICT (county_GEOID) DO NOTHING;
 
    FORCE CHECKPOINT;
""")
print("   Dimension tables built.")
 
# ==============================================================================
# STEP 2: Auto-generate Concept-Based Fact Tables
# Reads distinct table_names from silver.variable_crosswalk.
# table_name is snake_case prefixed with fact_ (e.g. fact_household_income).
# Silver has already stripped E/M suffix from variable column, so JOIN uses
# s.variable = c.variable (base code e.g. B01003_001).
# Each concept produces:
#   gold.{table_name}_bg   for block group data
#   gold.{table_name}_town for town (place) data
# ==============================================================================
print("\n2. Auto-generating concept-based fact tables from variable crosswalk...")
 
concepts = con.execute("""
    SELECT DISTINCT table_name
    FROM silver.variable_crosswalk
    WHERE year = (SELECT MAX(year) FROM silver.variable_crosswalk)
      AND table_name IS NOT NULL
    ORDER BY table_name
""").fetchall()
 
table_names = [row[0] for row in concepts]
print(f"   Found {len(table_names)} distinct concepts → up to {len(table_names) * 2} fact tables")
 
for table_name in table_names:
    for geo_type in ["bg", "town"]:
 
        # table_name from crosswalk is snake_case (e.g. fact_household_income)
        # append _bg or _town suffix to form the final Gold table name
        geo_suffix    = "bg" if geo_type == "bg" else "town"
        fact_table    = f"gold.{table_name}_{geo_suffix}"
        # Silver stores town data in acs_place (not acs_town)
        source_silver = "silver.acs_bg" if geo_type == "bg" else "silver.acs_place"
        geo_key       = "block_group_GEOID" if geo_type == "bg" else "place_GEOID"
        dim_table     = "gold.dim_bg"       if geo_type == "bg" else "gold.dim_town"
 
        print(f"   Building {fact_table}...")
 
        try:
            con.execute(f"DROP TABLE IF EXISTS {fact_table} CASCADE;")
 
            count = con.execute(f"""
                SELECT COUNT(*) FROM silver.variable_crosswalk
                WHERE table_name = '{table_name}'
            """).fetchone()[0]
 
            if count == 0:
                print(f"   Skipping {fact_table}: no variables found.")
                continue
 
            # Step 1: PIVOT estimates into a temp table
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE tmp_est AS
                PIVOT (
                    SELECT s.GEOID, s.vintage_year, c.label_clean, s.estimate
                    FROM {source_silver} AS s
                    JOIN silver.variable_crosswalk AS c
                      ON s.variable = c.variable
                    WHERE c.table_name = '{table_name}'
                      AND c.year = (SELECT MAX(year) FROM silver.variable_crosswalk)
                ) ON label_clean USING FIRST(estimate)
            """)
 
            # Step 2: PIVOT MOEs into a temp table
            # Append _moe to label_clean so MOE columns are distinct from estimate columns
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE tmp_moe AS
                PIVOT (
                    SELECT s.GEOID, s.vintage_year, c.label_clean || '_moe' AS label_clean, s.moe
                    FROM {source_silver} AS s
                    JOIN silver.variable_crosswalk AS c
                      ON s.variable = c.variable
                    WHERE c.table_name = '{table_name}'
                      AND c.year = (SELECT MAX(year) FROM silver.variable_crosswalk)
                ) ON label_clean USING FIRST(moe)
            """)
 
            # Step 3: Read column names from temp tables to build dynamic DDL
            est_cols = [
                row[0] for row in con.execute("DESCRIBE tmp_est").fetchall()
                if row[0] not in ('GEOID', 'vintage_year')
            ]
            moe_cols = [
                row[0] for row in con.execute("DESCRIBE tmp_moe").fetchall()
                if row[0] not in ('GEOID', 'vintage_year')
            ]
 
            # Skip if no matching variables found in Silver for this concept
            if not est_cols:
                print(f"   Skipping {fact_table}: no matching data in Silver.")
                con.execute("DROP TABLE IF EXISTS tmp_est; DROP TABLE IF EXISTS tmp_moe;")
                continue
 
            # Step 4: Build CREATE TABLE DDL with PK and FK constraints
            data_col_defs = ',\n    '.join(
                f'"{c}" DOUBLE PRECISION' for c in est_cols + moe_cols
            )
            con.execute(f"""
                CREATE TABLE {fact_table} (
                    "{geo_key}"  VARCHAR(50),
                    "year_key"   INTEGER,
                    {data_col_defs},
                    PRIMARY KEY ("{geo_key}", "year_key"),
                    FOREIGN KEY ("{geo_key}") REFERENCES {dim_table}("{geo_key}"),
                    FOREIGN KEY ("year_key")  REFERENCES gold.dim_year("year_key")
                )
            """)
 
            # Step 5: INSERT joined estimate + MOE data
            con.execute(f"""
                INSERT INTO {fact_table}
                SELECT
                    e.GEOID        AS "{geo_key}",
                    e.vintage_year AS year,
                    e.* EXCLUDE (GEOID, vintage_year),
                    m.* EXCLUDE (GEOID, vintage_year)
                FROM tmp_est AS e
                JOIN tmp_moe AS m ON e.GEOID = m.GEOID AND e.vintage_year = m.vintage_year
                ORDER BY
                    "{geo_key}", year
            """)
 
            # Clean up temp tables
            con.execute("DROP TABLE IF EXISTS tmp_est; DROP TABLE IF EXISTS tmp_moe;")
 
        except Exception as ex:
            print(f"   Warning: Skipped {fact_table} — {ex}")
            con.execute("DROP TABLE IF EXISTS tmp_est; DROP TABLE IF EXISTS tmp_moe;")
            continue


print("4. Cleaning up Gold Layer Views...")
con.execute("""
    -- Remove temporary source views
    DROP VIEW IF EXISTS gold.v_source_acs_bg; 
    DROP VIEW IF EXISTS gold.v_source_acs_place;
    
    -- Force disk write to secure data
    FORCE CHECKPOINT;
""")

print("5. Integrating Zillow data into Gold layer (Star Schema)...")
con.execute("""
            CREATE OR REPLACE VIEW gold.v_silver_zillow_zhvi AS
            SELECT 
            *
            FROM
            silver.zillow_zhvi; 

            CREATE OR REPLACE VIEW gold.v_silver_zillow_zori AS
            SELECT 
            *
            FROM
            silver.zillow_zori;

            DROP TABLE IF EXISTS gold.fact_zillow_home_value;
            CREATE TABLE gold.fact_zillow_home_value (
            place_GEOID VARCHAR(50),
            date_key DATE,
            housing_type VARCHAR(50),
            home_value DOUBLE PRECISION,
            
            PRIMARY KEY (place_GEOID, date_key, housing_type),
            FOREIGN KEY (place_GEOID) REFERENCES gold.dim_town(place_GEOID),
            FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
            );

            INSERT INTO gold.fact_zillow_home_value
            SELECT
            t.place_GEOID,
            z.date_key,
            z.housing_type,
            z.home_value
            FROM
            gold.v_silver_zillow_zhvi AS z
            JOIN
            gold.dim_town as t
            ON
            z.town_name = t.town_name;

            DROP TABLE IF EXISTS gold.fact_zillow_rent;
            CREATE TABLE gold.fact_zillow_rent (
            place_GEOID VARCHAR(50),
            date_key DATE,
            rent_value DOUBLE PRECISION,

            PRIMARY KEY (place_GEOID, date_key),
            FOREIGN KEY (place_GEOID) REFERENCES gold.dim_town(place_GEOID),
            FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
            );

            INSERT INTO gold.fact_zillow_rent
            SELECT
            t.place_GEOID,
            z.date_key,
            z.rent_value
            FROM
            gold.v_silver_zillow_zori AS z
            JOIN
            gold.dim_town AS t
            ON
            z.town_name = t.town_name;

            DROP VIEW IF EXISTS gold.v_silver_zillow_zhvi;
            DROP VIEW IF EXISTS gold.v_silver_zillow_zori;
                
            FORCE CHECKPOINT;
            """)

print("6. Integrating ALICE data into Gold layer star schema...")
con.execute("""CREATE OR REPLACE VIEW gold.v_silver_alice_town AS 
    SELECT
    *
    FROM
    silver.alice_town_household;

    CREATE OR REPLACE VIEW gold.v_silver_alice_county AS
    SELECT
    *
    FROM
    silver.alice_county;
    
    DROP TABLE IF EXISTS gold.fact_alice_county;
    DROP TABLE IF EXISTS gold.fact_alice_town_household;


    DROP TABLE IF EXISTS gold.dim_county;
    CREATE TABLE gold.dim_county (
        county_GEOID VARCHAR(50) PRIMARY KEY,
        county_name VARCHAR(50)
    );

    INSERT INTO gold.dim_county (county_GEOID, county_name)
    VALUES ('37119','Mecklenburg County')
    ON CONFLICT (county_GEOID) DO NOTHING;


    CREATE TABLE gold.fact_alice_town_household (
        place_GEOID VARCHAR(50),
        year_key INTEGER,
        total_households INTEGER,
        poverty_households INTEGER,
        alice_households INTEGER,
        above_alice_households INTEGER,

        PRIMARY KEY (place_GEOID, year_key),
        FOREIGN KEY (place_GEOID) REFERENCES gold.dim_town(place_GEOID),
        FOREIGN KEY (year_key) REFERENCES gold.dim_year(year_key)
    );

    INSERT INTO gold.fact_alice_town_household
    SELECT
        t.place_GEOID,
        a.year_key,
        a.total_households,
        a.poverty_households,
        a.alice_households,
        a.above_alice_households
    FROM
        gold.v_silver_alice_town AS a
    JOIN
        gold.dim_town AS t
    ON
        LOWER(a.town_name) = LOWER(t.town_name);

    CREATE TABLE gold.fact_alice_county (
        county_GEOID VARCHAR(50),
        year_key INTEGER,
        total_households INTEGER,
        poverty_households INTEGER,
        alice_households INTEGER,
        above_alice_households INTEGER,

        PRIMARY KEY (county_GEOID, year_key),
        FOREIGN KEY (county_GEOID) REFERENCES gold.dim_county(county_GEOID),
        FOREIGN KEY (year_key) REFERENCES gold.dim_year(year_key)
    );

    INSERT INTO gold.fact_alice_county
    SELECT
        county_GEOID,
        year_key,
        total_households,
        poverty_households,
        alice_households,
        above_alice_households
    FROM
        gold.v_silver_alice_county;

    DROP VIEW IF EXISTS gold.v_silver_alice_town;
    DROP VIEW IF EXISTS gold.v_silver_alice_county;

    FORCE CHECKPOINT;
            """)
print("6. [MH/SU] Loading into Gold as independent fact table...")
con.execute("""
            
        CREATE VIEW IF NOT EXISTS gold.v_silver_mh_su_facilities AS 
        SELECT *
        FROM silver.mh_su_facilities_clean;
        
        DROP TABLE IF EXISTS gold.fact_mh_su_facilities;
        
        -- No FK constraints — facilities span multiple counties outside our dim_town/dim_bg schema
        CREATE TABLE gold.fact_mh_su_facilities (
            facility_name       VARCHAR(200),
            street1             VARCHAR(200),
            street2             VARCHAR(200),
            city                VARCHAR(100),
            state               VARCHAR(5),
            zip                 VARCHAR(10),
            county              VARCHAR(100),
            phone               VARCHAR(50),
            website             VARCHAR(500),
            latitude            DOUBLE PRECISION,
            longitude           DOUBLE PRECISION,
            type_facility       VARCHAR(10),
            facility_type_label VARCHAR(50),
            is_mecklenburg      BOOLEAN
        );
    
        INSERT INTO gold.fact_mh_su_facilities
        SELECT * FROM gold.v_silver_mh_su_facilities;
    
        DROP VIEW IF EXISTS gold.v_silver_mh_su_facilities;
        FORCE CHECKPOINT;
    """)
print("   MH/SU Facilities table built in Gold.")

print("3. [AMI] Loading into Gold as independent reference table...")
con.execute("""
    CREATE VIEW IF NOT EXISTS gold.v_silver_ami_affordability_gap AS
    SELECT * FROM silver.ami_affordability_gap;

    CREATE VIEW IF NOT EXISTS gold.v_silver_charlotte_housing_wage AS
    SELECT * FROM silver.charlotte_housing_wage;

    CREATE VIEW IF NOT EXISTS gold.v_silver_charlotte_rent_by_bedroom AS
    SELECT * FROM silver.charlotte_rent_by_bedroom;

    -- NOTE: confirm these 3 silver table names actually match what your
    -- Bronze/Silver scripts created (they may be named silver.rent_by_bedroom
    -- and silver.housing_wage instead -- fix whichever side is wrong).

    -- ------------------------------------------------------------------------------
    -- dim_region: FIX -- added SELECT DISTINCT (source has 20 rows, only
    -- 3 distinct regions -- without DISTINCT this inserted 20 duplicate
    -- rows). Column renamed region_id -> region_key to match every FK/JOIN
    -- reference elsewhere in this script.
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.dim_region;
    CREATE TABLE gold.dim_region (
        region_key  INTEGER PRIMARY KEY,
        region_name VARCHAR(100)
    );
    INSERT INTO gold.dim_region
    SELECT
        ROW_NUMBER() OVER (ORDER BY region) AS region_key,
        region AS region_name
    FROM (SELECT DISTINCT region FROM gold.v_silver_charlotte_rent_by_bedroom) t;

    -- ------------------------------------------------------------------------------
    -- dim_bedrooms: FIX -- added SELECT DISTINCT (source has 20 rows,
    -- only 5 distinct bedroom types -- same row-multiplication bug as
    -- dim_region above).
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.dim_bedrooms;
    CREATE TABLE gold.dim_bedrooms (
        bedroom_key            INTEGER PRIMARY KEY,
        bedroom_label          VARCHAR(50),
        assumed_household_size INTEGER
    );
    INSERT INTO gold.dim_bedrooms
    SELECT
        ROW_NUMBER() OVER (ORDER BY household_size) AS bedroom_key,
        bedrooms AS bedroom_label,
        household_size AS assumed_household_size
    FROM (SELECT DISTINCT bedrooms, household_size FROM gold.v_silver_ami_affordability_gap) t;

    -- ------------------------------------------------------------------------------
    -- dim_ami_level: FIX -- added missing semicolon after the closing ")"
    -- (this was the exact reported ParserException). Also ORDER BY changed
    -- from ami_level (text -- sorts "100%" before "30%") to ami_pct (numeric).
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.dim_ami_level;
    CREATE TABLE gold.dim_ami_level (
        ami_level_key   INTEGER PRIMARY KEY,
        ami_level_label VARCHAR(50),
        ami_pct         INTEGER
    );
    INSERT INTO gold.dim_ami_level
    SELECT
        ROW_NUMBER() OVER (ORDER BY ami_pct) AS ami_level_key,
        ami_level AS ami_level_label,
        ami_pct
    FROM (
        SELECT DISTINCT
            ami_level,
            CAST(REGEXP_EXTRACT(ami_level, '([0-9]+)%', 1) AS INTEGER) AS ami_pct
        FROM gold.v_silver_ami_affordability_gap
    ) t;  -- <-- the missing semicolon that caused the ParserException was here

    -- ------------------------------------------------------------------------------
    -- dim_occupation: unchanged -- source already has exactly 9 unique
    -- occupation rows so no DISTINCT bug here, but added for safety.
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.dim_occupation;
    CREATE TABLE gold.dim_occupation (
        occupation_key  INTEGER PRIMARY KEY,
        occupation_name VARCHAR(100),
        category        VARCHAR(50)
    );
    INSERT INTO gold.dim_occupation
    SELECT
        ROW_NUMBER() OVER (ORDER BY occupation) AS occupation_key,
        occupation AS occupation_name,
        category
    FROM (SELECT DISTINCT occupation, category FROM gold.v_silver_charlotte_housing_wage) t;

    -- ------------------------------------------------------------------------------
    -- fact_fair_market_rent: FIX -- wrong schema prefix on the source view
    -- (was "silver.v_silver_..." -- that view lives in gold, not silver).
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.fact_fair_market_rent;
    CREATE TABLE gold.fact_fair_market_rent (
        region_key  INTEGER,
        year_key    INTEGER,
        bedroom_key INTEGER,
        fmr         INTEGER,
        PRIMARY KEY (region_key, year_key, bedroom_key),
        FOREIGN KEY (region_key)  REFERENCES gold.dim_region(region_key),
        FOREIGN KEY (year_key)    REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (bedroom_key) REFERENCES gold.dim_bedrooms(bedroom_key)
    );
    INSERT INTO gold.fact_fair_market_rent
    SELECT
        d.region_key,
        r.year_key,
        b.bedroom_key,
        r.fmr
    FROM gold.v_silver_charlotte_rent_by_bedroom AS r
    JOIN gold.dim_region   AS d ON r.region   = d.region_name
    JOIN gold.dim_bedrooms AS b ON r.bedrooms = b.bedroom_label;

    -- ------------------------------------------------------------------------------
    -- fact_ami_affordability_gap: FIX -- removed stray quote + fixed typo
    -- "montly_gap" -> "monthly_gap" in the CREATE TABLE. FIX -- the source
    -- (silver.ami_affordability_gap) has NO "region" column at all (the
    -- CSV never had one -- this is Charlotte-only reference data), so
    -- "ON a.region = d.region_name" would fail with a binder error.
    -- Join to dim_region on the literal region name instead. FIX --
    -- dim_ami_level's column is ami_level_label, not ami_level.
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.fact_ami_affordability_gap;
    CREATE TABLE gold.fact_ami_affordability_gap (
        region_key           INTEGER,
        year_key             INTEGER,
        bedroom_key          INTEGER,
        ami_level_key        INTEGER,
        annual_income        INTEGER,
        max_affordable_rent  INTEGER,
        monthly_gap          INTEGER,
        affordability_status VARCHAR(50),
        PRIMARY KEY (region_key, year_key, bedroom_key, ami_level_key),
        FOREIGN KEY (region_key)    REFERENCES gold.dim_region(region_key),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (bedroom_key)   REFERENCES gold.dim_bedrooms(bedroom_key),
        FOREIGN KEY (ami_level_key) REFERENCES gold.dim_ami_level(ami_level_key)
    );
    INSERT INTO gold.fact_ami_affordability_gap
    SELECT
        d.region_key,
        2025 AS year_key,  -- HUD FY2025 basis (verified vs. rent_by_bedroom 2025 Charlotte fmr match)
        b.bedroom_key,
        l.ami_level_key,
        a.annual_income,
        a.max_affordable_rent,
        a.monthly_gap,
        a.affordability_status
    FROM gold.v_silver_ami_affordability_gap AS a
    JOIN gold.dim_region    AS d ON d.region_name    = 'Charlotte-Concord-Gastonia'
    JOIN gold.dim_bedrooms  AS b ON a.bedrooms       = b.bedroom_label
    JOIN gold.dim_ami_level AS l ON a.ami_level      = l.ami_level_label;

    -- ------------------------------------------------------------------------------
    -- fact_occupation_housing_wage: FIX -- same "no region column in
    -- source" issue as above -- hardcode the region join instead.
    -- ------------------------------------------------------------------------------
    DROP TABLE IF EXISTS gold.fact_occupation_housing_wage;
    CREATE TABLE gold.fact_occupation_housing_wage (
        region_key     INTEGER,
        year_key       INTEGER,
        occupation_key INTEGER,
        hourly_wage    DOUBLE PRECISION,
        employment     INTEGER,
        PRIMARY KEY (region_key, year_key, occupation_key),
        FOREIGN KEY (region_key)     REFERENCES gold.dim_region(region_key),
        FOREIGN KEY (year_key)       REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (occupation_key) REFERENCES gold.dim_occupation(occupation_key)
    );
    INSERT INTO gold.fact_occupation_housing_wage
    SELECT
        d.region_key,
        2025 AS year_key,  -- NLIHC "Out of Reach 2025" basis (verified: $35.08 Charlotte-Concord-Gastonia housing wage match)
        o.occupation_key,
        h.hourly_wage,
        h.employment
    FROM gold.v_silver_charlotte_housing_wage AS h
    JOIN gold.dim_region     AS d ON d.region_name     = 'Charlotte-Concord-Gastonia'
    JOIN gold.dim_occupation AS o ON h.occupation      = o.occupation_name;
            
    DROP VIEW IF EXISTS gold.v_silver_ami_affordability_gap;
    DROP VIEW IF EXISTS gold.v_silver_charlotte_housing_wage;
    DROP VIEW IF EXISTS gold.v_silver_charlotte_rent_by_bedroom;
            
    FORCE CHECKPOINT;
""")
print("   Charlotte regional housing affordability star schema built in Gold.")

print("[PLACES CDC] Building category-based fact tables in Gold layer...")

con.execute("""
        CREATE OR REPLACE VIEW gold.v_cdc_places AS 
        SELECT * FROM silver.cdc_places;
            
        DROP TABLE IF EXISTS gold.cdc_places;
        CREATE TABLE gold.cdc_places AS SELECT * FROM gold.v_cdc_places;
            
        FORCE CHECKPOINT;
                """)
categories = {
    'HLTHOUT': 'fact_health_outcomes_town',
    'HLTHSTAT': 'fact_health_status_town',
    'PREVENT': 'fact_prevention_town',
    'DISABLT': 'fact_disability_town',
    'RISKBEH': 'fact_risk_behaviors_town',
    'SOCLNEED': 'fact_social_needs_town',
}

for cat_id, table_name in categories.items():
    print(f" Building gold.{table_name}...")
        # Step 1: PIVOT into temp table

    con.execute(f"""
        --Gold Table: Pivot from silver view
        CREATE OR REPLACE TEMP TABLE tmp_cdc AS
        PIVOT (
            SELECT
                place_GEOID,
                year_key,
                col_name,
                data_value
            FROM gold.v_cdc_places
            WHERE category_id = '{cat_id}'
        )
        ON col_name
        USING FIRST(data_value)
        GROUP BY place_GEOID, year_key;
    """)
    #step 2: Get measure columns from temp table
    cols = [
        row[0] for row in con.execute("DESCRIBE tmp_cdc").fetchall()
        if row[0] not in ('place_GEOID', 'year_key')
    ]

   # step 3: Build dynamic DDL with PK and FK
    data_col_defs = ',\n   '.join(f'"{c}" DOUBLE PRECISION' for c in cols)
    con.execute(f"""
        DROP TABLE IF EXISTS gold.{table_name};
        CREATE TABLE gold.{table_name} (
            "place_GEOID"   VARCHAR(20),
            "town_name"     VARCHAR(50),
            "year_key"      INTEGER,
            {data_col_defs},
            PRIMARY KEY ("place_GEOID", "year_key"),
            FOREIGN KEY ("place_GEOID") REFERENCES gold.dim_town("place_GEOID"),
            FOREIGN KEY ("year_key")    REFERENCES gold.dim_year("year_key")
        );
    """)

    # step 4: INSERT with town_name joined in
    col_list = ', '.join(f'f."{c}"' for c in cols)
    con.execute(f"""
        INSERT INTO gold.{table_name}
        SELECT f.place_GEOID, t.town_name, f.year_key, {col_list}
        FROM tmp_cdc AS f
        JOIN gold.dim_town AS t ON f.place_GEOID = t.place_GEOID;
        DROP TABLE IF EXISTS tmp_cdc;
    """)
con.execute("""
    DROP VIEW IF EXISTS gold.v_cdc_places;
    FORCE CHECKPOINT;
""")
print("Complete: CDC PLACES Bronze → Silver → Gold pipeline done!")

con.execute("""
-- ==============================================================================
    -- Expose Silver tables as Gold-facing views first (matches the
    -- pattern used elsewhere for Gold sections that source from Silver,
    -- e.g. the Charlotte housing / CDC PLACES blocks in nmidw_gold.py)
    -- ==============================================================================
    CREATE OR REPLACE VIEW gold.v_silver_school_combined_test_results AS
        SELECT * FROM silver.school_combined_test_results;
    CREATE OR REPLACE VIEW gold.v_silver_school_growth AS
        SELECT * FROM silver.school_growth;
    CREATE OR REPLACE VIEW gold.v_silver_school_other_hs_indicators AS
        SELECT * FROM silver.school_other_hs_indicators;
    CREATE OR REPLACE VIEW gold.v_silver_school_assess_ind_master AS
        SELECT * FROM silver.school_assess_ind_master;
    CREATE OR REPLACE VIEW gold.v_silver_school_eog_eoc AS
        SELECT * FROM silver.school_eog_eoc;
    CREATE OR REPLACE VIEW gold.v_silver_school_act_grade11 AS
        SELECT * FROM silver.school_act_grade11;
    CREATE OR REPLACE VIEW gold.v_silver_school_workkeys AS
        SELECT * FROM silver.school_workkeys;
    CREATE OR REPLACE VIEW gold.v_silver_school_english_learner AS
        SELECT * FROM silver.school_english_learner;
    CREATE OR REPLACE VIEW gold.v_silver_school_subject_code_format AS
        SELECT * FROM silver.school_subject_code_format;
    CREATE OR REPLACE VIEW gold.v_silver_school_subgroup_format AS
        SELECT * FROM silver.school_subgroup_format;

    -- ==============================================================================
    -- DIM: dim_school
    -- PK = school_code (not school_name -- avoids the "Hopewell
    -- Elementary (Randolph Co.) vs Hopewell High School (CMS)"
    -- name-collision risk found earlier). district_name/grade_span/
    -- is_title_1 confirmed constant per school in school_assess_ind_master
    -- before being folded in here.
    --
    -- town_name: hardcoded from NCES CCD (Common Core of Data) official
    -- school addresses -- verified for all 23 schools, Mailing Address
    -- and Physical Address town agreed for every one (no ambiguous
    -- cases). Caveat: this is town per USPS mailing/physical address
    -- city, NOT verified against actual municipal boundary polygons --
    -- a school addressed "Huntersville, NC" could technically sit in
    -- unincorporated Mecklenburg County just outside town limits. Good
    -- enough for address-based mapping; upgrade to point-in-polygon
    -- against TIGER PLACE boundaries later if exact-boundary precision
    -- is ever needed.
    --
    -- place_GEOID: added so dim_school is a REAL FK-linked dimension in
    -- the star schema, not just a free-floating text label. Looked up
    -- via JOIN against gold.dim_town (not hardcoded GEOID literals,
    -- since I don't have those memorized/verified -- deriving via JOIN
    -- avoids guessing a GEOID string wrong). Requires nmidw_gold.py's
    -- dim_town to already exist before this runs.
    -- ==============================================================================
    DROP TABLE IF EXISTS gold.dim_school;
    CREATE TABLE gold.dim_school (
        school_code   VARCHAR(20) PRIMARY KEY,
        school_name   VARCHAR(100),
        district_name VARCHAR(100),
        grade_span    VARCHAR(20),
        is_title_1    BOOLEAN,
        place_GEOID   VARCHAR(50),
        town_name     VARCHAR(20),
        FOREIGN KEY (place_GEOID) REFERENCES gold.dim_town(place_GEOID)
    );
    INSERT INTO gold.dim_school
    SELECT
        s.school_code,
        s.school_name,
        s.district_name,
        s.grade_span,
        s.is_title_1,
        dt.place_GEOID,
        s.town_name
    FROM (
        SELECT
            school_code, school_name, district_name, grade_span, is_title_1,
            CASE school_code
                -- Cornelius (5)
                WHEN '600312' THEN 'Cornelius'   -- William Amos Hough High
                WHEN '600313' THEN 'Cornelius'   -- Bailey Middle
                WHEN '600346' THEN 'Cornelius'   -- Cornelius Elementary
                WHEN '600433' THEN 'Cornelius'   -- J.V. Washam Elementary
                WHEN '61J000' THEN 'Cornelius'   -- Lakeside Charter Academy
                -- Davidson (2)
                WHEN '600357' THEN 'Davidson'    -- Davidson K-8 School
                WHEN '60I000' THEN 'Davidson'    -- Community School of Davidson
                -- Huntersville (16)
                WHEN '600305' THEN 'Huntersville' -- J. M. Alexander Middle
                WHEN '600306' THEN 'Huntersville' -- North Academy of World Languages
                WHEN '600328' THEN 'Huntersville' -- Barnette Elementary
                WHEN '600394' THEN 'Huntersville' -- Francis Bradley Middle
                WHEN '600415' THEN 'Huntersville' -- Hopewell High School
                WHEN '600420' THEN 'Huntersville' -- Huntersville Elementary
                WHEN '600442' THEN 'Huntersville' -- Blythe Elementary
                WHEN '600444' THEN 'Huntersville' -- Long Creek Elementary
                WHEN '600480' THEN 'Huntersville' -- North Mecklenburg High School
                WHEN '600557' THEN 'Huntersville' -- Torrence Creek Elementary
                WHEN '600558' THEN 'Huntersville' -- Grand Oak Elementary
                WHEN '600594' THEN 'Huntersville' -- Merancas Middle College-CPCC
                WHEN '60D000' THEN 'Huntersville' -- Lake Norman Charter
                WHEN '61V000' THEN 'Huntersville' -- Bonnie Cone Classical Academy
                WHEN '62M000' THEN 'Huntersville' -- Bonnie Cone Leadership Academy
                WHEN '62N000' THEN 'Huntersville' -- Aspire Trade High
                ELSE NULL  -- flag anything unexpected instead of silently mismapping
            END AS town_name
        FROM (
            SELECT DISTINCT school_code, school_name, district_name, grade_span, is_title_1
            FROM gold.v_silver_school_assess_ind_master
        )
    ) AS s
    LEFT JOIN gold.dim_town AS dt ON s.town_name = dt.town_name;
    -- VERIFY AFTER RUNNING: SELECT COUNT(*) FROM gold.dim_school; should
    -- still be 23. If it's more, dim_town has duplicate town_name values
    -- for Cornelius/Davidson/Huntersville (shouldn't happen given how
    -- dim_town is built from distinct ACS place GEOIDs, but not verified
    -- against a live run). Also check for NULL place_GEOID rows --
    -- should be 0 (would mean a town_name didn't match dim_town at all).

    -- ==============================================================================
    -- DIM: dim_subgroup
    -- REDESIGNED: subgroup_code list is now DERIVED from
    -- school_assess_ind_master (20 codes actually used across our 23
    -- schools -- MIG does not happen to appear and correctly drops out,
    -- same treatment as dim_subject_code). subgroup_label comes from
    -- silver.school_subgroup_format, which is itself parsed straight
    -- from the workbook's Introduction tab text (Bronze -> Silver, not
    -- hand-retyped). NAIG and NELS are the two remaining hardcoded
    -- exceptions: confirmed NOT in that source text (19 official pairs
    -- extracted, NAIG/NELS not among them) even though both codes are
    -- genuinely used in our real data -- their labels are taken
    -- verbatim from the matching text used in the other 7 tables
    -- ("Not Academically or Intellectually Gifted", "Not English
    -- Learner"), which is a direct data fact, not a guess.
    -- ==============================================================================
    DROP TABLE IF EXISTS gold.dim_subgroup;
    CREATE TABLE gold.dim_subgroup (
        subgroup_code  VARCHAR(10) PRIMARY KEY,
        subgroup_label VARCHAR(60)
    );
    INSERT INTO gold.dim_subgroup
    SELECT
        d.subgroup_code,
        COALESCE(fmt.subgroup_label, undoc.subgroup_label) AS subgroup_label
    FROM (SELECT DISTINCT subgroup_code FROM gold.v_silver_school_assess_ind_master) AS d
    LEFT JOIN gold.v_silver_school_subgroup_format AS fmt
        ON d.subgroup_code = fmt.subgroup_code
    LEFT JOIN (VALUES
        ('NAIG', 'Not Academically or Intellectually Gifted'),
        ('NELS', 'Not English Learner')
    ) AS undoc(subgroup_code, subgroup_label) ON d.subgroup_code = undoc.subgroup_code;
    -- NOTE: subgroup_label wording here is the source documentation's own
    -- phrasing (e.g. "Economically Disadvantaged Student"), which differs
    -- slightly from the shorter text actually used in the other 7 tables
    -- ("Economically Disadvantaged"). This is cosmetic only -- the CASE
    -- WHEN blocks in each fact table match against the real raw text
    -- directly and resolve to subgroup_code (the actual FK/PK), never
    -- against this label column, so the wording difference cannot cause
    -- a join or lookup failure anywhere in this schema.

    -- ==============================================================================
    -- DIM: dim_subject_code
    -- REDESIGNED (round 2): descriptions now come from
    -- silver.school_subject_code_format, which is itself extracted
    -- directly from the workbook's own "Asses-Ind Data Set Format" tab
    -- (Bronze -> Silver, not hand-retyped into SQL). This closes the
    -- transcription-error risk of copying 46+ descriptions by hand.
    -- MA37 is the one remaining hardcoded exception: it's genuinely
    -- NOT in the source documentation table (confirmed: 47 official
    -- rows extracted, MA37 not among them), so its description had to
    -- be inferred from data and is kept as a small manual addendum.
    -- ==============================================================================
    DROP TABLE IF EXISTS gold.dim_subject_code;
    CREATE TABLE gold.dim_subject_code (
        subject_code VARCHAR(10) PRIMARY KEY,
        description  VARCHAR(400)
    );
    INSERT INTO gold.dim_subject_code
    SELECT
        d.subject_code,
        COALESCE(fmt.description, undoc.description) AS description
    FROM (SELECT DISTINCT subject_code FROM gold.v_silver_school_assess_ind_master) AS d
    LEFT JOIN gold.v_silver_school_subject_code_format AS fmt
        ON d.subject_code = fmt.subject_code
    LEFT JOIN (VALUES
        ('MA37', '[UNDOCUMENTED CODE -- not in the workbook''s own Format tab] Inferred from data: Mathematics grades 3-7 combined, i.e. MAGS excluding grade 8. Verified against 2 independent schools: Barnette Elementary (PK-05, no grade 6-8 data at all) has MA37 exactly equal to MAGS (same denominator 374, same glp_pct 73.3); J. M. Alexander Middle (06-08) has MA37''s denominator (531) exactly equal to MAGS''s denominator (791) minus MA08''s denominator (260). Plausible reason: grade 8 math is reported separately (M8SEP/MA08) since some grade-8 students take the NC Math 1 EOC instead of the standard grade-8 EOG.')
    ) AS undoc(subject_code, description) ON d.subject_code = undoc.subject_code;
    -- NOTE: dim_subject_code.description for 'ACT' is the verbatim source
    -- text and says "19 or higher" -- this is the source workbook's own
    -- typo/error. dim_act_measure.benchmark_score for the corresponding
    -- real measure ('ACT composite score of 17 or higher') is verified
    -- as 17 via cross-matched data (see that dim's comments). Trust 17.

    -- ==============================================================================
    -- DIM: dim_grade_scope
    -- REDESIGNED: grade_scope values are now DERIVED (UNION DISTINCT
    -- across the 2 tables that use this dimension) rather than hand
    -- typed. grade_min/grade_max/is_eoc are computed via regex parsing
    -- of the label text itself, not a hardcoded lookup -- there's no
    -- external documentation involved here (the grade numbers are
    -- literally embedded in the label), so this can be 100% derived.
    -- ==============================================================================
    DROP TABLE IF EXISTS gold.dim_grade_scope;
    CREATE TABLE gold.dim_grade_scope (
        grade_scope VARCHAR(20) PRIMARY KEY,
        grade_min   INTEGER,
        grade_max   INTEGER,
        is_eoc      BOOLEAN
    );
    INSERT INTO gold.dim_grade_scope
    SELECT
        grade_scope,
        CASE WHEN grade_scope = 'All' THEN NULL
             ELSE TRY_CAST(REGEXP_EXTRACT(grade_scope, '([0-9]+)', 1) AS INTEGER)
        END AS grade_min,
        CASE WHEN grade_scope = 'All' THEN NULL
             WHEN REGEXP_EXTRACT(grade_scope, '[0-9]+[-&]([0-9]+)', 1) <> ''
                  THEN TRY_CAST(REGEXP_EXTRACT(grade_scope, '[0-9]+[-&]([0-9]+)', 1) AS INTEGER)
             ELSE TRY_CAST(REGEXP_EXTRACT(grade_scope, '([0-9]+)', 1) AS INTEGER)  -- single grade: max = min
        END AS grade_max,
        (grade_scope LIKE '%EOC%') AS is_eoc
    FROM (
        SELECT DISTINCT grade_scope FROM gold.v_silver_school_combined_test_results
        UNION
        SELECT DISTINCT grade_scope FROM gold.v_silver_school_eog_eoc
    ) t;

    -- ==============================================================================
    -- DIM: dim_act_measure
    -- REDESIGNED: act_measure list is now DERIVED from Silver. Only
    -- benchmark_score is a hardcoded lookup, since that number comes
    -- from the workbook's own ACT documentation text ("Benchmark = 18"
    -- etc.) and isn't present as a column in any Silver table. The two
    -- "sum/all benchmarks met" measures aren't in the lookup, so they
    -- correctly get NULL via the LEFT JOIN (composites across the 4
    -- subtests, not single benchmark scores -- not a data gap).
    -- ==============================================================================
    DROP TABLE IF EXISTS gold.dim_act_measure;
    CREATE TABLE gold.dim_act_measure (
        act_measure     VARCHAR(60) PRIMARY KEY,
        benchmark_score INTEGER
    );
    INSERT INTO gold.dim_act_measure
    SELECT
        d.act_measure,
        lut.benchmark_score
    FROM (SELECT DISTINCT act_measure FROM gold.v_silver_school_act_grade11) AS d
    LEFT JOIN (VALUES
        ('ACT composite score of 17 or higher', 17),
        ('ACT English subtest',                 18),
        ('ACT Math subtest',                    22),
        ('ACT Reading subtest',                 22),
        ('ACT Science subtest',                 23)
    ) AS lut(act_measure, benchmark_score) ON d.act_measure = lut.act_measure;


    -- ==============================================================================
    -- FACT TABLES
    -- Shared subgroup-label -> subgroup_code normalization is repeated
    -- per fact (not centralized) since each source table's raw text
    -- column is independent; verified exhaustive (21 distinct label
    -- values across all 7 text-based tables, all 21 mapped, 0 unmapped)
    -- before this was written. school_english_learner in particular was
    -- caught during verification actually using the FULL 21-value label
    -- set (race/gender/disability breakdowns), not just the 4 EL-related
    -- ones originally assumed -- fixed before this version.
    -- ==============================================================================

 -- fact_school_test_results: grain = (school, subgroup, grade_scope)
    -- glp_raw/ccr_raw added: needed to reproduce proficiency.csv/ccr.csv's
    -- suppression-preserving text (">95" etc.) in the Aggregate layer --
    -- these were already in silver.school_combined_test_results but had
    -- been dropped when this fact was first built.
    DROP TABLE IF EXISTS gold.fact_school_test_results;
    CREATE TABLE gold.fact_school_test_results (
        school_code   VARCHAR(20),
        year_key      INTEGER,
        subgroup_code VARCHAR(10),
        grade_scope   VARCHAR(20),
        glp_raw       VARCHAR(10),
        glp_pct       DOUBLE,
        ccr_raw       VARCHAR(10),
        ccr_pct       DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code, grade_scope),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code),
        FOREIGN KEY (grade_scope)   REFERENCES gold.dim_grade_scope(grade_scope)
    );
    INSERT INTO gold.fact_school_test_results
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.grade_scope,
        s.glp_raw,
        s.glp_pct,
        s.ccr_raw,
        s.ccr_pct
    FROM gold.v_silver_school_combined_test_results AS s;

    -- fact_school_growth: grain = (school, subgroup, growth_type)
    DROP TABLE IF EXISTS gold.fact_school_growth;
    CREATE TABLE gold.fact_school_growth (
        school_code        VARCHAR(20),
        year_key            INTEGER,
        subgroup_code       VARCHAR(10),
        growth_type          VARCHAR(20),
        growth_status        VARCHAR(20),
        growth_index_score   DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code, growth_type),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code)
    );
    INSERT INTO gold.fact_school_growth
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.growth_type,
        s.growth_status,
        s.growth_index_score
    FROM gold.v_silver_school_growth AS s;

    -- fact_school_hs_indicators: grain = (school, subgroup)
      DROP TABLE IF EXISTS gold.fact_school_hs_indicators;
    CREATE TABLE gold.fact_school_hs_indicators (
        school_code               VARCHAR(20),
        year_key                   INTEGER,
        subgroup_code               VARCHAR(10),
        act_workkeys_indicator_raw  VARCHAR(10),
        act_workkeys_indicator_pct  DOUBLE,
        passing_math3_raw           VARCHAR(10),
        passing_math3_pct           DOUBLE,
        grad_4yr_raw                VARCHAR(10),
        grad_4yr_pct                DOUBLE,
        grad_5yr_raw                VARCHAR(10),
        grad_5yr_pct                DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code)
    );
    INSERT INTO gold.fact_school_hs_indicators
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.act_workkeys_indicator_raw,
        s.act_workkeys_indicator_pct,
        s.passing_math3_raw,
        s.passing_math3_pct,
        s.grad_4yr_raw,
        s.grad_4yr_pct,
        s.grad_5yr_raw,
        s.grad_5yr_pct
    FROM gold.v_silver_school_other_hs_indicators AS s;

    -- fact_school_assessment_master: grain = (school, subgroup, subject)
    DROP TABLE IF EXISTS gold.fact_school_assessment_master;
    CREATE TABLE gold.fact_school_assessment_master (
        school_code   VARCHAR(20),
        year_key       INTEGER,
        subgroup_code   VARCHAR(10),
        subject_code    VARCHAR(10),
        is_title_1       BOOLEAN,
        denominator       INTEGER,
        total_pct          DOUBLE,
        notprof_pct         DOUBLE,
        lev3_pct             DOUBLE,
        lev4_pct              DOUBLE,
        lev5_pct               DOUBLE,
        glp_pct                 DOUBLE,
        ccr_pct                  DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code, subject_code),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code),
        FOREIGN KEY (subject_code)  REFERENCES gold.dim_subject_code(subject_code)
    );
    INSERT INTO gold.fact_school_assessment_master
    SELECT
        s.school_code,
        s.reporting_year AS year_key,
        s.subgroup_code,
        s.subject_code,
        s.is_title_1,
        s.denominator,
        s.total_pct,
        s.notprof_pct,
        s.lev3_pct,
        s.lev4_pct,
        s.lev5_pct,
        s.glp_pct,
        s.ccr_pct
    FROM gold.v_silver_school_assess_ind_master AS s;

    -- fact_school_eog_eoc: grain = (school, subgroup, subject_area, grade_scope)
    DROP TABLE IF EXISTS gold.fact_school_eog_eoc;
    CREATE TABLE gold.fact_school_eog_eoc (
        school_code         VARCHAR(20),
        year_key              INTEGER,
        subgroup_code          VARCHAR(10),
        subject_area             VARCHAR(20),
        grade_scope                VARCHAR(20),
        original_subject_label VARCHAR(50),
        not_proficient_pct           DOUBLE,
        level_3_pct                    DOUBLE,
        level_4_pct                      DOUBLE,
        level_5_pct                        DOUBLE,
        glp_pct                              DOUBLE,
        ccr_pct                                DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code,original_subject_label),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code),
        FOREIGN KEY (grade_scope)   REFERENCES gold.dim_grade_scope(grade_scope)
    );
    INSERT INTO gold.fact_school_eog_eoc
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.subject_area,
        s.grade_scope,
        s.original_subject_label,
        s.not_proficient_pct,
        s.level_3_pct,
        s.level_4_pct,
        s.level_5_pct,
        s.glp_pct,
        s.ccr_pct
    FROM gold.v_silver_school_eog_eoc AS s;

    -- fact_school_act: grain = (school, subgroup, act_measure)
    DROP TABLE IF EXISTS gold.fact_school_act;
    CREATE TABLE gold.fact_school_act (
        school_code             VARCHAR(20),
        year_key                  INTEGER,
        subgroup_code               VARCHAR(10),
        act_measure                   VARCHAR(60),
        pct_meeting_benchmark           DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code, act_measure),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code),
        FOREIGN KEY (act_measure)   REFERENCES gold.dim_act_measure(act_measure)
    );
    INSERT INTO gold.fact_school_act
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.act_measure,
        s.pct_meeting_benchmark
    FROM gold.v_silver_school_act_grade11 AS s;

    -- fact_school_workkeys: grain = (school, subgroup)
    DROP TABLE IF EXISTS gold.fact_school_workkeys;
    CREATE TABLE gold.fact_school_workkeys (
        school_code           VARCHAR(20),
        year_key                INTEGER,
        subgroup_code             VARCHAR(10),
        pct_silver_or_higher        DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code)
    );
    INSERT INTO gold.fact_school_workkeys
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.pct_silver_or_higher
    FROM gold.v_silver_school_workkeys AS s;

    -- fact_school_english_learner: grain = (school, subgroup)
    -- Uses the FULL subgroup mapping (not just EL-related codes) --
    -- verified this file actually reports 12 distinct subgroups
    -- (race/gender/economic/disability breakdowns too, not just
    -- "All English Learners"/"English Learner"/"Not English Learner").
    DROP TABLE IF EXISTS gold.fact_school_english_learner;
    CREATE TABLE gold.fact_school_english_learner (
        school_code                    VARCHAR(20),
        year_key                         INTEGER,
        subgroup_code                      VARCHAR(10),
        total_el_progress_pct                DOUBLE,
        pct_exiting_el_status                  DOUBLE,
        pct_meeting_annual_progress              DOUBLE,
        PRIMARY KEY (school_code, year_key, subgroup_code),
        FOREIGN KEY (school_code)   REFERENCES gold.dim_school(school_code),
        FOREIGN KEY (year_key)      REFERENCES gold.dim_year(year_key),
        FOREIGN KEY (subgroup_code) REFERENCES gold.dim_subgroup(subgroup_code)
    );
    INSERT INTO gold.fact_school_english_learner
    SELECT
        s.school_code,
        2025 AS year_key,
        CASE s.subgroup
            WHEN 'All Students' THEN 'ALL'
            WHEN 'Economically Disadvantaged' THEN 'EDS'
            WHEN 'Not Economically Disadvantaged' THEN 'NEDS'
            WHEN 'Academically or Intellectually Gifted' THEN 'AIG'
            WHEN 'Not Academically or Intellectually Gifted' THEN 'NAIG'
            WHEN 'Students With Disabilities' THEN 'SWD'
            WHEN 'Not Student with Disabilities' THEN 'NSWD'
            WHEN 'English Learner' THEN 'ELS'
            WHEN 'All English Learners' THEN 'ELS'
            WHEN 'Not English Learner' THEN 'NELS'
            WHEN 'Black' THEN 'BLCK'
            WHEN 'White' THEN 'WHTE'
            WHEN 'Hispanic' THEN 'HISP'
            WHEN 'Asian' THEN 'ASIA'
            WHEN 'American Indian' THEN 'AMIN'
            WHEN 'Two or More Races' THEN 'MULT'
            WHEN 'Female' THEN 'FEM'
            WHEN 'Male' THEN 'MALE'
            WHEN 'Military Connected' THEN 'MIL'
            WHEN 'Homeless' THEN 'HMS'
            WHEN 'Foster Care' THEN 'FCS'
            ELSE NULL
        END AS subgroup_code,
        s.total_el_progress_pct,
        s.pct_exiting_el_status,
        s.pct_meeting_annual_progress
    FROM gold.v_silver_school_english_learner AS s;

    -- ==============================================================================
    -- Clean up the Silver-exposing views now that Gold tables are built
    -- ==============================================================================
    DROP VIEW IF EXISTS gold.v_silver_school_combined_test_results;
    DROP VIEW IF EXISTS gold.v_silver_school_growth;
    DROP VIEW IF EXISTS gold.v_silver_school_other_hs_indicators;
    DROP VIEW IF EXISTS gold.v_silver_school_assess_ind_master;
    DROP VIEW IF EXISTS gold.v_silver_school_eog_eoc;
    DROP VIEW IF EXISTS gold.v_silver_school_act_grade11;
    DROP VIEW IF EXISTS gold.v_silver_school_workkeys;
    DROP VIEW IF EXISTS gold.v_silver_school_english_learner;
    DROP VIEW IF EXISTS gold.v_silver_school_subject_code_format;
    DROP VIEW IF EXISTS gold.v_silver_school_subgroup_format;
            """)
print("Complete: school_assessment Bronze → Silver → Gold pipeline done!")

con.close()
print("🎉 Step 3 Complete: Gold Layer Star Schema successfully built!")