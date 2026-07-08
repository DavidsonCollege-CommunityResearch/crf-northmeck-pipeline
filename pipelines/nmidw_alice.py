# ==============================================================================
# EXTERNAL DATA PIPELINE: ALICE (Asset Limited, Income Constrained, Employed)
# Author: Paul Park, Gemini Code
# Objective: Ingest ALICE household financial hardship data into Star Schema
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
print("1. Ingesting raw ALICE CSV files into Bronze layer...")
con.execute(""" 
            CREATE OR REPLACE TABLE bronze.alice_household AS
            SELECT 
                * 
            FROM 
                read_csv_auto('data/alice/alice_town_households.csv');
            
            CREATE OR REPLACE TABLE bronze.alice_county AS
            SELECT
                *
            FROM
                read_csv_auto('data/alice/alice_county_trend.csv');
            
            FORCE CHECKPOINT;
            """)
print("🎉 Step 1 Bronze layer data successfully ingested.")

# ==============================================================================
# STEP 2: SILVER LAYER (Standardization & Cleaning)
# ==============================================================================
print("2. Standardizing and cleaning ALICE data in Silver layer...")
con.execute("""
            CREATE OR REPLACE VIEW silver.v_bronze_alice_household AS
            SELECT
                *
            FROM
                bronze.alice_household;

            CREATE OR REPLACE VIEW silver.v_bronze_alice_county AS
            SELECT
                *
            FROM
                bronze.alice_county;

            CREATE OR REPLACE TABLE silver.alice_town_household AS
            SELECT
                TRIM(town) AS town_name,
                CAST(year AS INTEGER) AS year_key,
                CAST(total_households AS INTEGER) AS total_households,
                CAST(poverty_households AS INTEGER) AS poverty_households,
                CAST(alice_households AS INTEGER) AS alice_households,
                CAST(above_alice_households AS INTEGER) AS above_alice_households
            FROM
                silver.v_bronze_alice_household
            WHERE
                town IS NOT NULL;

            CREATE OR REPLACE TABLE silver.alice_county AS
            SELECT
                '37119' AS county_GEOID, 
                TRIM(county) || ' County' AS county_name,
                CAST(year AS INTEGER) AS year_key,
                CAST(total_households AS INTEGER) AS total_households,
                CAST(poverty_households AS INTEGER) AS poverty_households,
                CAST(alice_households AS INTEGER) AS alice_households,
                CAST(above_alice_households AS INTEGER) AS above_alice_households
            FROM
                silver.v_bronze_alice_county
            WHERE
                county IS NOT NULL;

            DROP VIEW IF EXISTS silver.v_bronze_alice_household;
            DROP VIEW IF EXISTS silver.v_bronze_alice_county;

            FORCE CHECKPOINT;
            """)
con.close()
print("🎉 Step 2 Silver layer data successfully standardized and cleaned.")

