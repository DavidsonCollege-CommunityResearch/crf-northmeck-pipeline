# ==============================================================================
# EXTERNAL DATA PIPELINE: NC School Assessment & Other Indicator Data (2024-25)
# Author: Paul Park, Claude Code
# Objective: Ingest North Mecklenburg public school performance data (23
#            schools, verified against the NC DPI source workbook) into the
#            Bronze layer, ahead of Silver cleaning and a school-level star
#            schema in Gold.
# Source: NC DPI "2024-25 School Assessment and Other Indicator Data"
#         reporting_year = 2025 (per the source file's own reporting_year field)
# NOTE: normalize_names=true is used because several source headers contain
#       embedded newlines (e.g. "Percent\nLevel 3 and Above\n(GLP)") which
#       are otherwise painful/error-prone to reference in SQL. Per DuckDB's
#       docs, this strips non-alphanumeric characters so columns can be used
#       without quoting -- Silver will still rename everything to clean,
#       business-friendly names; this only fixes raw SQL-usability.
# ==============================================================================
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection

con = get_md_connection()

# ==============================================================================
# STEP 1: BRONZE LAYER (Raw Ingestion from CSV)
# ==============================================================================
print("1. Ingesting NC school assessment CSV files into Bronze layer...")
con.execute("""
    CREATE OR REPLACE TABLE bronze.school_combined_test_results AS
    SELECT * FROM read_csv('data/school_assessment/bronze_combined_test_results.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_growth AS
    SELECT * FROM read_csv('data/school_assessment/bronze_school_growth.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_other_hs_indicators AS
    SELECT * FROM read_csv('data/school_assessment/bronze_other_hs_indicators.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_assess_ind_master AS
    SELECT * FROM read_csv('data/school_assessment/bronze_master_dataset.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_eog_eoc AS
    SELECT * FROM read_csv('data/school_assessment/bronze_eog_eoc.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_act_grade11 AS
    SELECT * FROM read_csv('data/school_assessment/bronze_act_grade11.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_workkeys AS
    SELECT * FROM read_csv('data/school_assessment/bronze_workkeys.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_english_learner AS
    SELECT * FROM read_csv('data/school_assessment/bronze_english_learner.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);

    CREATE OR REPLACE TABLE bronze.school_subject_code_format AS
    SELECT * FROM read_csv('data/school_assessment/bronze_subject_code_format.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);
     
    CREATE OR REPLACE TABLE bronze.school_subgroup_format AS
    SELECT * FROM read_csv('data/school_assessment/bronze_subgroup_format.csv', delim=',', quote='"', header=true, normalize_names=true, all_varchar=true);
            
    FORCE CHECKPOINT;
""")
print("   Bronze layer data successfully ingested.")


# ==============================================================================
# STEP 2: SILVER LAYER (Standardization & Cleaning)
# ==============================================================================
con = get_md_connection()
 
print("2. Standardizing and cleaning NC school assessment data in Silver layer...")
con.execute("""
    -- ------------------------------------------------------------------------------
    -- Expose Bronze tables as Silver views first (matches convention
    -- used elsewhere in the pipeline, e.g. nmidw_charlotte.py)
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE VIEW silver.v_bronze_school_combined_test_results AS
        SELECT * FROM bronze.school_combined_test_results;
    CREATE OR REPLACE VIEW silver.v_bronze_school_growth AS
        SELECT * FROM bronze.school_growth;
    CREATE OR REPLACE VIEW silver.v_bronze_school_other_hs_indicators AS
        SELECT * FROM bronze.school_other_hs_indicators;
    CREATE OR REPLACE VIEW silver.v_bronze_school_assess_ind_master AS
        SELECT * FROM bronze.school_assess_ind_master;
    CREATE OR REPLACE VIEW silver.v_bronze_school_eog_eoc AS
        SELECT * FROM bronze.school_eog_eoc;
    CREATE OR REPLACE VIEW silver.v_bronze_school_act_grade11 AS
        SELECT * FROM bronze.school_act_grade11;
    CREATE OR REPLACE VIEW silver.v_bronze_school_workkeys AS
        SELECT * FROM bronze.school_workkeys;
    CREATE OR REPLACE VIEW silver.v_bronze_school_english_learner AS
        SELECT * FROM bronze.school_english_learner;
    CREATE OR REPLACE VIEW silver.v_bronze_school_subject_code_format AS
        SELECT * FROM bronze.school_subject_code_format;
    CREATE OR REPLACE VIEW silver.v_bronze_school_subgroup_format AS
        SELECT * FROM bronze.school_subgroup_format;
 
    -- ------------------------------------------------------------------------------
    -- Combined Test Results: "subject" actually mixes "always combined
    -- subjects" with "which grade band" -- split into grade_scope.
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_combined_test_results AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        CASE TRIM(subject)
            WHEN 'All Subjects' THEN 'All'
            WHEN 'Grade 3-8'    THEN '3-8'
            WHEN 'Grade 9-12'   THEN '9-12'
            WHEN 'Grade 3'      THEN '3'
            WHEN 'Grade 4'      THEN '4'
            WHEN 'Grade 5'      THEN '5'
            WHEN 'Grade 6'      THEN '6'
            WHEN 'Grade 7'      THEN '7'
            WHEN 'Grade 8'      THEN '8'
            ELSE NULL  -- flag unexpected values instead of silently mislabeling
        END AS grade_scope,
        percent_level_3_and_above_glp AS glp_raw,
        TRY_CAST(percent_level_3_and_above_glp AS DOUBLE) AS glp_pct,
        percent_level_4_and_above_ccr AS ccr_raw,
        TRY_CAST(percent_level_4_and_above_ccr AS DOUBLE) AS ccr_pct,
        TRY_CAST(number_of_days_missed_due_to_hurricane_helene AS INTEGER) AS missed_days_helene
    FROM silver.v_bronze_school_combined_test_results;
 
    -- ------------------------------------------------------------------------------
    -- School Growth
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_growth AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        TRIM(school_growth_type)   AS growth_type,
        TRIM(school_growth_status) AS growth_status,
        TRY_CAST(school_growth_index_score AS DOUBLE) AS growth_index_score
    FROM silver.v_bronze_school_growth;
 
    -- ------------------------------------------------------------------------------
    -- Other High School Indicators
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_other_hs_indicators AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        actworkkeys_assessments_indicator_percent AS act_workkeys_indicator_raw,
        TRY_CAST(actworkkeys_assessments_indicator_percent AS DOUBLE) AS act_workkeys_indicator_pct,
        passing_nc_math_3_percent AS passing_math3_raw,
        TRY_CAST(passing_nc_math_3_percent AS DOUBLE) AS passing_math3_pct,
        fouryear_cohort_graduation_rate_percent AS grad_4yr_raw,
        TRY_CAST(fouryear_cohort_graduation_rate_percent AS DOUBLE) AS grad_4yr_pct,
        fiveyear_cohort_graduation_rate_percent AS grad_5yr_raw,
        TRY_CAST(fiveyear_cohort_graduation_rate_percent AS DOUBLE) AS grad_5yr_pct
    FROM silver.v_bronze_school_other_hs_indicators;
 
    -- ------------------------------------------------------------------------------
    -- Master Assess-Ind Data Set: adds denominator (den) and Title I
    -- status, which none of the other tabs have. subject stays as its
    -- NCDPI code (PCALL, RDGS, MAGS, ACT, WK, CGRS, etc.) -- a
    -- code->description lookup belongs in Gold, not here.
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_assess_ind_master AS
    SELECT
        CAST(reporting_year AS INTEGER) AS reporting_year,
        TRIM(school_code) AS school_code,
        TRIM(school_name) AS school_name,
        TRIM(lea_name)    AS district_name,
        TRIM(grade_span)  AS grade_span,
        TRY_CAST(missed_days AS INTEGER) AS missed_days_helene,
        (TRIM(title_1) = 'Y') AS is_title_1,
        TRIM(subgroup) AS subgroup_code,
        TRIM(subject)  AS subject_code,
        TRY_CAST(den AS INTEGER) AS denominator,
        total_pct AS total_pct_raw,
        TRY_CAST(total_pct AS DOUBLE) AS total_pct,
        TRY_CAST(notprof_pct AS DOUBLE) AS notprof_pct,
        TRY_CAST(lev3_pct AS DOUBLE) AS lev3_pct,
        TRY_CAST(lev4_pct AS DOUBLE) AS lev4_pct,
        TRY_CAST(lev5_pct AS DOUBLE) AS lev5_pct,
        glp_pct AS glp_pct_raw,
        TRY_CAST(glp_pct AS DOUBLE) AS glp_pct,
        ccr_pct AS ccr_pct_raw,
        TRY_CAST(ccr_pct AS DOUBLE) AS ccr_pct
    FROM silver.v_bronze_school_assess_ind_master;
 
    -- ------------------------------------------------------------------------------
    -- EOG and EOC: "subject" mixes subject area (Reading/Math/Science/
    -- English) with grade scope -- split into subject_area + grade_scope.
    -- Mapping verified exhaustive against all 21 distinct subject values
    -- actually present for our 23 schools before this was written.
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_eog_eoc AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        TRIM(subject)        AS original_subject_label,
        CASE TRIM(subject)
            WHEN 'Biology'           THEN 'Science'
            WHEN 'English II'        THEN 'English'
            WHEN 'NC Math 1 (9-12)'  THEN 'Math'
            WHEN 'NC Math 3 (9-12)'  THEN 'Math'
            WHEN 'Reading Grade 3-8' THEN 'Reading'
            WHEN 'Reading Grade 3'   THEN 'Reading'
            WHEN 'Reading Grade 4'   THEN 'Reading'
            WHEN 'Reading Grade 5'   THEN 'Reading'
            WHEN 'Reading Grade 6'   THEN 'Reading'
            WHEN 'Reading Grade 7'   THEN 'Reading'
            WHEN 'Reading Grade 8'   THEN 'Reading'
            WHEN 'Math Grade 3-8'    THEN 'Math'
            WHEN 'Math Grade 3'      THEN 'Math'
            WHEN 'Math Grade 4'      THEN 'Math'
            WHEN 'Math Grade 5'      THEN 'Math'
            WHEN 'Math Grade 6'      THEN 'Math'
            WHEN 'Math Grade 7'      THEN 'Math'
            WHEN 'Math Grade 8'      THEN 'Math'
            WHEN 'Science Grade 5&8' THEN 'Science'
            WHEN 'Science Grade 5'   THEN 'Science'
            WHEN 'Science Grade 8'   THEN 'Science'
            ELSE NULL
        END AS subject_area,
        CASE TRIM(subject)
            WHEN 'Biology'           THEN 'EOC (9-12)'
            WHEN 'English II'        THEN 'EOC (9-12)'
            WHEN 'NC Math 1 (9-12)'  THEN 'EOC (9-12)'
            WHEN 'NC Math 3 (9-12)'  THEN 'EOC (9-12)'
            WHEN 'Reading Grade 3-8' THEN '3-8'
            WHEN 'Reading Grade 3'   THEN '3'
            WHEN 'Reading Grade 4'   THEN '4'
            WHEN 'Reading Grade 5'   THEN '5'
            WHEN 'Reading Grade 6'   THEN '6'
            WHEN 'Reading Grade 7'   THEN '7'
            WHEN 'Reading Grade 8'   THEN '8'
            WHEN 'Math Grade 3-8'    THEN '3-8'
            WHEN 'Math Grade 3'      THEN '3'
            WHEN 'Math Grade 4'      THEN '4'
            WHEN 'Math Grade 5'      THEN '5'
            WHEN 'Math Grade 6'      THEN '6'
            WHEN 'Math Grade 7'      THEN '7'
            WHEN 'Math Grade 8'      THEN '8'
            WHEN 'Science Grade 5&8' THEN '5&8'
            WHEN 'Science Grade 5'   THEN '5'
            WHEN 'Science Grade 8'   THEN '8'
            ELSE NULL
        END AS grade_scope,
        not_proficient AS not_proficient_raw,
        TRY_CAST(not_proficient AS DOUBLE) AS not_proficient_pct,
        TRY_CAST(percent_level_3 AS DOUBLE) AS level_3_pct,
        TRY_CAST(percent_level_4 AS DOUBLE) AS level_4_pct,
        TRY_CAST(percent_level_5 AS DOUBLE) AS level_5_pct,
        percent_level_3_and_above_glp AS glp_raw,
        TRY_CAST(percent_level_3_and_above_glp AS DOUBLE) AS glp_pct,
        percent_level_4_and_above_ccr AS ccr_raw,
        TRY_CAST(percent_level_4_and_above_ccr AS DOUBLE) AS ccr_pct
    FROM silver.v_bronze_school_eog_eoc;
 
    -- ------------------------------------------------------------------------------
    -- The ACT Grade 11
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_act_grade11 AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        TRIM(the_act_subtest_or_composite) AS act_measure,
        percent_meeting_benchmark_or_standard AS pct_meeting_benchmark_raw,
        TRY_CAST(percent_meeting_benchmark_or_standard AS DOUBLE) AS pct_meeting_benchmark
    FROM silver.v_bronze_school_act_grade11;
 
    -- ------------------------------------------------------------------------------
    -- WorkKeys
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_workkeys AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        percent_silver_or_higher AS pct_silver_or_higher_raw,
        TRY_CAST(percent_silver_or_higher AS DOUBLE) AS pct_silver_or_higher
    FROM silver.v_bronze_school_workkeys;
 
    -- ------------------------------------------------------------------------------
    -- English Learner Progress Indicator
    -- ------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE silver.school_english_learner AS
    SELECT
        TRIM(school_code)   AS school_code,
        TRIM(school_name)   AS school_name,
        TRIM(district_name) AS district_name,
        TRIM(grade_span)    AS grade_span,
        TRIM(subgroup)      AS subgroup,
        total_el_progress_exited_plus_annual_progress AS total_el_progress_raw,
        TRY_CAST(total_el_progress_exited_plus_annual_progress AS DOUBLE) AS total_el_progress_pct,
        percent_exiting_el_status AS pct_exiting_el_status_raw,
        TRY_CAST(percent_exiting_el_status AS DOUBLE) AS pct_exiting_el_status,
        percent_meeting_annual_progress_toward_exiting AS pct_meeting_annual_progress_raw,
        TRY_CAST(percent_meeting_annual_progress_toward_exiting AS DOUBLE) AS pct_meeting_annual_progress
    FROM silver.v_bronze_school_english_learner;
 
    -- TRIM on subject_code fixes a real source-data issue: the raw sheet
    -- has 'WK ' (trailing space) for the WorkKeys code, which would
    -- otherwise fail to match the actual 'WK' code used everywhere else
    -- in the workbook (e.g. school_assess_ind_master.subject).
    CREATE OR REPLACE TABLE silver.school_subject_code_format AS
    SELECT
        TRIM(subject_code) AS subject_code,
        TRIM(description)  AS description
    FROM silver.v_bronze_school_subject_code_format;
 
    CREATE OR REPLACE TABLE silver.school_subgroup_format AS
    SELECT
        TRIM(subgroup_code)  AS subgroup_code,
        TRIM(subgroup_label) AS subgroup_label
    FROM silver.v_bronze_school_subgroup_format;
 
    -- ------------------------------------------------------------------------------
    -- Clean up the Bronze-exposing views now that Silver tables are built
    -- ------------------------------------------------------------------------------
    DROP VIEW IF EXISTS silver.v_bronze_school_combined_test_results;
    DROP VIEW IF EXISTS silver.v_bronze_school_growth;
    DROP VIEW IF EXISTS silver.v_bronze_school_other_hs_indicators;
    DROP VIEW IF EXISTS silver.v_bronze_school_assess_ind_master;
    DROP VIEW IF EXISTS silver.v_bronze_school_eog_eoc;
    DROP VIEW IF EXISTS silver.v_bronze_school_act_grade11;
    DROP VIEW IF EXISTS silver.v_bronze_school_workkeys;
    DROP VIEW IF EXISTS silver.v_bronze_school_english_learner;
    DROP VIEW IF EXISTS silver.v_bronze_school_subject_code_format;
    DROP VIEW IF EXISTS silver.v_bronze_school_subgroup_format;
 
    FORCE CHECKPOINT;
""")
print("   Silver layer data successfully standardized and cleaned.")
con.close()