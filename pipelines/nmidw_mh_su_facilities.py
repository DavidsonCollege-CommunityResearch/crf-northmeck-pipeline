# ==============================================================================
# EXTERNAL DATA PIPELINE: Mental Health/ Substance Use Facilities
# Author: Paul Park, Gemini Code
# Objective: Ingest mental health facility data into Star Schema
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
print("1. Ingesting raw mental health/ substance use facilities CSV files into Bronze layer...")
con.execute(""" 
            CREATE OR REPLACE TABLE bronze.mh_su_facilities AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/healthcare/mh_su_facilities.csv');
            
        
            FORCE CHECKPOINT;
            """)
print("🎉 Step 1 Bronze layer data successfully ingested.")

# ==============================================================================
# STEP 2: SILVER LAYER (Standardization & Cleaning)
# ==============================================================================
print("2. Standardizing and cleaning mental health/ substance use facilities data in Silver layer...")
con.execute("""
            CREATE OR REPLACE VIEW silver.v_bronze_mh_su_facilities AS
            SELECT
                *
            FROM
                bronze.mh_su_facilities;


            CREATE OR REPLACE TABLE silver.mh_su_facilities_clean AS
            SELECT
                TRIM(name1)                                 AS facility_name,
                TRIM(street1)                AS street1,
                TRIM(street2)                 AS street2,
                TRIM(city)                                  AS city,
                TRIM(state)                                 AS state,
                CAST(zip AS VARCHAR)                        AS zip,
                TRIM(COALESCE(county, 'Unknown'))           AS county,
                TRIM(phone)                                 AS phone,
                TRIM(website)                 AS website,
                TRY_CAST(latitude AS DOUBLE)                AS latitude,
                TRY_CAST(longitude AS DOUBLE)               AS longitude,
                TRIM(type_facility)                         AS type_facility,
                -- Expand type code to readable label
                CASE TRIM(type_facility)
                    WHEN 'MH'   THEN 'Mental Health'
                    WHEN 'SU'   THEN 'Substance Use'
                    WHEN 'OTP'  THEN 'Opioid Treatment Program'
                    WHEN 'HRSA' THEN 'HRSA Health Center'
                    ELSE TRIM(type_facility)
                END AS facility_type_label,
                TRIM(payment_accepted),
                TRIM(services),
                -- Flag facilities within Mecklenburg County for primary analysis
                CASE
                    WHEN LOWER(TRIM(county)) = 'mecklenburg' THEN TRUE
                    ELSE FALSE
                END AS is_mecklenburg
            FROM
                silver.v_bronze_mh_su_facilities
            WHERE
                name1 IS NOT NULL;
        DROP VIEW IF EXISTS silver.v_bronze_mh_su_facilities;

            FORCE CHECKPOINT;
            """)
con.close()
print("🎉 Step 2 Silver layer data successfully standardized and cleaned.")

