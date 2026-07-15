# ==============================================================================
# EXTERNAL DATA PIPELINE: Charlotte Metro Housing Affordability Reference Data
# Author: Paul Park, Claude Code
# Objective: Ingest AMI affordability gap, housing wage, and rent by bedroom
#            data into Star Schema
# ==============================================================================
import os
import duckdb
import time
import requests
import polars as pl
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection
 
con = get_md_connection()
 
# ==============================================================================
# STEP 1: BRONZE LAYER (Raw Ingestion from CSV)
# ==============================================================================
print("1. Ingesting raw charlotte  CSV files into Bronze layer...")
con.execute(""" 
            CREATE OR REPLACE TABLE bronze.charlotte_ami_affordability_gap AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/charlotte/charlotte_metro_ami_affordability_gap.csv');
            
            CREATE OR REPLACE TABLE bronze.charlotte_housing_wage AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/charlotte/charlotte_housing_wage.csv');
            
            CREATE OR REPLACE TABLE bronze.charlotte_rent_by_bedroom AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/charlotte/charlotte_rent_by_bedroom.csv');
            
        
            FORCE CHECKPOINT;
            """)
print("🎉 Step 1 Bronze layer data successfully ingested.")
 
# ==============================================================================
# STEP 2: SILVER LAYER (Standardization & Cleaning)
# ==============================================================================
print("2. Standardizing and cleaning charlotte metro ami affordability gap data in Silver layer...")
con.execute("""
    
    CREATE OR REPLACE VIEW silver.v_bronze_charlotte_ami_affordability_gap AS
    SELECT * FROM bronze.charlotte_ami_affordability_gap;
            
    CREATE OR REPLACE VIEW silver.v_bronze_charlotte_housing_wage AS
    SELECT * FROM bronze.charlotte_housing_wage;
            
    CREATE OR REPLACE VIEW silver.v_bronze_charlotte_rent_by_bedroom AS
    SELECT * FROM bronze.charlotte_rent_by_bedroom;
 
 
    CREATE OR REPLACE TABLE silver.ami_affordability_gap AS
    SELECT
        TRIM(bedrooms)                          AS bedrooms,
        CAST(household_size AS INTEGER)         AS household_size,
        TRIM(ami_level)                         AS ami_level,
        CAST(annual_income AS INTEGER)          AS annual_income,
        CAST(max_affordable_rent AS INTEGER)    AS max_affordable_rent,
        CAST(fmr AS INTEGER)                    AS fmr,
        CAST(monthly_gap AS INTEGER)            AS monthly_gap,
        -- Derived: positive gap = affordable, negative = unaffordable
        CASE
            WHEN monthly_gap >= 0 THEN 'Affordable'
            ELSE 'Unaffordable'
        END AS affordability_status
    FROM silver.v_bronze_charlotte_ami_affordability_gap;
            
    CREATE OR REPLACE TABLE silver.charlotte_housing_wage AS 
    SELECT
        TRIM(occupation) AS occupation,
        CAST(hourly_wage AS DOUBLE) AS hourly_wage,
        TRY_CAST(employment AS INTEGER) AS employment,  -- 'NA' (Benchmark row) becomes NULL instead of erroring
        TRIM(category) AS category 
    FROM
        silver.v_bronze_charlotte_housing_wage;
    
    CREATE OR REPLACE TABLE silver.charlotte_rent_by_bedroom AS
    SELECT
        TRIM(region) AS region,
        CAST(year AS INTEGER) AS year_key,
        TRIM(bedrooms) AS bedrooms,
        CAST(fmr AS INTEGER) AS fmr
    FROM
        silver.v_bronze_charlotte_rent_by_bedroom;
            
 
    DROP VIEW IF EXISTS silver.v_bronze_charlotte_ami_affordability_gap; 
    DROP VIEW IF EXISTS silver.v_bronze_charlotte_housing_wage;
    DROP VIEW IF EXISTS silver.v_bronze_charlotte_rent_by_bedroom;
            
    FORCE CHECKPOINT;
""")
con.close()
print("🎉 Step 2 Silver layer data successfully standardized and cleaned.")