# ==============================================================================
# EXTERNAL DATA PIPELINE: ZILLOW REAL ESTATE METRICS
# Author: Paul Park, Gemini Code
# Objective: Ingest Zillow CSVs and integrate them into the existing Star Schema
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

# ----------------------------------------------
# 1. BRONZE LAYER: Raw Ingestion from CSV
# ----------------------------------------------
print("1. Ingesting raw Zillow CSV files into Bronze layer...")
con.execute("""
            CREATE OR REPLACE TABLE bronze.zillow_zhvi_long AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/zillow/north_meck_zhvi_long.csv');
            
            CREATE OR REPLACE TABLE bronze.zillow_zori_long AS
            SELECT
                *
            FROM
                read_csv_auto('data/zillow/north_meck_zori_long.csv');
            
            FORCE CHECKPOINT;
            """)
print("🎉 Step 1 Complete: raw data successfully loaded!")

# ----------------------------------------------
# 2. SILVER LAYER: Standardization & Data Typing
# ----------------------------------------------
print("2. Refining Zillow data in Silver layer...")
con.execute("""
            CREATE OR REPLACE VIEW silver.v_bronze_zhvi AS
            SELECT
                *
            FROM
                bronze.zillow_zhvi_long;

            CREATE OR REPLACE VIEW silver.v_bronze_zori AS
            SELECT
                *
            FROM
                bronze.zillow_zori_long;

            CREATE OR REPLACE TABLE silver.zillow_zhvi AS
            SELECT
                TRIM(town) AS town_name,
                TRIM(housing_type) AS housing_type,
                TRY_CAST(date AS DATE) AS date_key,
                TRY_CAST(home_value AS DOUBLE PRECISION) AS home_value
            FROM
                silver.v_bronze_zhvi
            WHERE 
            town IS NOT NULL;

            CREATE OR REPLACE TABLE silver.zillow_zori AS 
            SELECT
                TRIM(RegionName) AS town_name,
                CAST(date AS DATE) AS date_key,  
                TRY_CAST(rent_index AS DOUBLE PRECISION) AS rent_value
            FROM
                silver.v_bronze_zori
            WHERE
                RegionName IS NOT NULL;

            DROP VIEW IF EXISTS silver.v_bronze_zhvi;
            DROP VIEW IF EXISTS silver.v_bronze_zori;
            FORCE CHECKPOINT;
            """)
con.close()
print("🎉 Step 2 Silver Layer data successfully refined and views dropped.")

