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
    CREATE TABLE gold.dim_bg (
        block_group_GEOID  VARCHAR(50) PRIMARY KEY,
        block_group_name   VARCHAR(50),
        census_tract_GEOID VARCHAR(50),
        census_tract_name  VARCHAR(50),
        county_GEOID       VARCHAR(50),
        county_name        VARCHAR(50)
    );
    INSERT INTO gold.dim_bg
    SELECT DISTINCT
        GEOID,
        REGEXP_EXTRACT(NAME, '^(Block Group [0-9]+)', 1),
        SUBSTR(GEOID, 1, 11),
        REGEXP_EXTRACT(NAME, '(Census Tract [0-9\\.]+)', 1),
        SUBSTR(GEOID, 1, 5),
        REGEXP_EXTRACT(NAME, '(Mecklenburg County)', 1)
    FROM silver.acs_bg;
 
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

    -- --------------------------------------------------------------
    -- dim_region: FIX -- added SELECT DISTINCT (source has 20 rows, only
    -- 3 distinct regions -- without DISTINCT this inserted 20 duplicate
    -- rows). Column renamed region_id -> region_key to match every FK/JOIN
    -- reference elsewhere in this script.
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- dim_bedrooms: FIX -- added SELECT DISTINCT (source has 20 rows,
    -- only 5 distinct bedroom types -- same row-multiplication bug as
    -- dim_region above).
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- dim_ami_level: FIX -- added missing semicolon after the closing ")"
    -- (this was the exact reported ParserException). Also ORDER BY changed
    -- from ami_level (text -- sorts "100%" before "30%") to ami_pct (numeric).
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- dim_occupation: unchanged -- source already has exactly 9 unique
    -- occupation rows so no DISTINCT bug here, but added for safety.
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- fact_fair_market_rent: FIX -- wrong schema prefix on the source view
    -- (was "silver.v_silver_..." -- that view lives in gold, not silver).
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- fact_ami_affordability_gap: FIX -- removed stray quote + fixed typo
    -- "montly_gap" -> "monthly_gap" in the CREATE TABLE. FIX -- the source
    -- (silver.ami_affordability_gap) has NO "region" column at all (the
    -- CSV never had one -- this is Charlotte-only reference data), so
    -- "ON a.region = d.region_name" would fail with a binder error.
    -- Join to dim_region on the literal region name instead. FIX --
    -- dim_ami_level's column is ami_level_label, not ami_level.
    -- --------------------------------------------------------------
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

    -- --------------------------------------------------------------
    -- fact_occupation_housing_wage: FIX -- same "no region column in
    -- source" issue as above -- hardcode the region join instead.
    -- --------------------------------------------------------------
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

con.close()
print("🎉 Step 3 Complete: Gold Layer Star Schema successfully built!")