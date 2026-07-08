# ==============================================================================
# EXTERNAL DATA PIPELINE: CDC PLACES Health Data
# Author: Paul Park, Claude Code
# Objective: Ingest CDC PLACES health outcome and prevention data for
#            Davidson, Cornelius, and Huntersville towns.
# Data: CDC PLACES (Population Level Analysis and Community Estimates)
#       — town-level health measures from BRFSS survey
# ==============================================================================
 
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection
 
con = get_md_connection()
 
# ==============================================================================
# STEP 1: BRONZE LAYER — Raw Ingestion
# ==============================================================================
print("1. [PLACES] Ingesting raw CSV into Bronze layer...")
con.execute("""
    CREATE OR REPLACE TABLE bronze.cdc_places_raw AS
    SELECT *
    FROM read_csv_auto('data/healthcare/All_Towns_CDC_PLACES_data.csv');
 
    FORCE CHECKPOINT;
""")
print("   Bronze: bronze.cdc_places_raw loaded.")
 

 
# ==============================================================================
# STEP 2: SILVER LAYER — Clean & Standardize
# ==============================================================================
print("2. [PLACES] Cleaning data in Silver layer...")
con.execute("""
    CREATE OR REPLACE VIEW silver.v_bronze_cdc_places AS
    SELECT * FROM bronze.cdc_places_raw;
    
    CREATE OR REPLACE TABLE silver.cdc_places AS 
    SELECT
        CAST(Year AS INTEGER) AS year_key,
        TRIM(StateAbbr) AS state_abbr,
        TRIM(LocationName) AS town_name,
        CAST(LocationID AS VARCHAR) AS place_GEOID,
        TRIM(DataSource) AS data_source,
        TRIM(CategoryID) AS category_id,
        TRIM(MeasureID) AS measure_id,
        TRIM(DataValueTypeID) AS data_value_type_id,
        TRIM(Measure) AS measure_text,
        TRIM(Short_Question_Text) AS short_question_text,
        TRY_CAST(Data_Value AS DOUBLE) AS data_value,
        TRY_CAST(Low_Confidence_Limit AS DOUBLE) AS low_confidence_limit,
        TRY_CAST(High_Confidence_Limit AS DOUBLE) AS high_confidence_llimit,
        TRY_CAST(REPLACE(TotalPopulation, ',', '') AS INTEGER) AS total_population,
        --Derived pivot col name: arthritis_aageadjprv
        LOWER(MeasureID) || '_' || LOWER(DataValueTypeID) AS col_name
    FROM silver.v_bronze_cdc_places
    WHERE
        LocationName IS NOT NULL AND Data_Value IS NOT NULL AND DataValueTypeID IN ('AgeAdjPrv', 'CrdPrv');
            
 
    FORCE CHECKPOINT;
""")
print("   Silver: silver.cdc_places cleaned.")
 