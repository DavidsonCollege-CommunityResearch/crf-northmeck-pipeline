# ==============================================================================
# AGGREGATE LAYER PIPELINE
# Author: Paul Park, Gemini Code
# Objective: Create pre-calculated, flattened tables for BI dashboards & charts
# Updated: Sources now JOIN multiple concept-based fact tables instead of a
#          single wide fact_town_metrics / fact_bg_metrics table.
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

print("\nStarting (Aggregate Layer)...")
con.execute("""
    -- Expose Gold schema objects to the main schema space for easier querying
    CREATE OR REPLACE VIEW dim_bg AS SELECT * FROM gold.dim_bg;
    CREATE OR REPLACE VIEW dim_date AS SELECT * FROM gold.dim_date;
    CREATE OR REPLACE VIEW dim_town AS SELECT * FROM gold.dim_town;
    CREATE OR REPLACE VIEW dim_year AS SELECT * FROM gold.dim_year;
    CREATE OR REPLACE VIEW dim_county AS SELECT * FROM gold.dim_county;
    CREATE OR REPLACE VIEW fact_alice_town_household AS SELECT * FROM gold.fact_alice_town_household;
    CREATE OR REPLACE VIEW fact_alice_county AS SELECT * FROM gold.fact_alice_county;
    CREATE OR REPLACE VIEW fact_zillow_home_value AS SELECT * FROM gold.fact_zillow_home_value;
    CREATE OR REPLACE VIEW fact_zillow_rent AS SELECT * FROM gold.fact_zillow_rent;

    -- Expose concept-based fact tables used across the aggregations below
    CREATE OR REPLACE VIEW fact_household_income_in_the_past_12_months_town AS SELECT * FROM gold.fact_household_income_in_the_past_12_months_town;
    CREATE OR REPLACE VIEW fact_tenure_by_vehicles_available_town AS SELECT * FROM gold.fact_tenure_by_vehicles_available_town;
    CREATE OR REPLACE VIEW fact_types_of_health_insurance_coverage_by_age_town AS SELECT * FROM gold.fact_types_of_health_insurance_coverage_by_age_town;
    CREATE OR REPLACE VIEW fact_health_insurance_coverage_status_and_type_by_employment_status_town AS SELECT * FROM gold.fact_health_insurance_coverage_status_and_type_by_employment_status_town;
    CREATE OR REPLACE VIEW fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town AS SELECT * FROM gold.fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town;
    CREATE OR REPLACE VIEW fact_median_household_income_in_the_past_12_months_town AS SELECT * FROM gold.fact_median_household_income_in_the_past_12_months_town;
    CREATE OR REPLACE VIEW fact_median_value_dollars_town AS SELECT * FROM gold.fact_median_value_dollars_town;
    CREATE OR REPLACE VIEW fact_median_gross_rent_dollars_town AS SELECT * FROM gold.fact_median_gross_rent_dollars_town;
    CREATE OR REPLACE VIEW fact_gini_index_of_income_inequality_town AS SELECT * FROM gold.fact_gini_index_of_income_inequality_town;
    CREATE OR REPLACE VIEW fact_tenure_town AS SELECT * FROM gold.fact_tenure_town;
    CREATE OR REPLACE VIEW fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town AS SELECT * FROM gold.fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town;
    CREATE OR REPLACE VIEW fact_sex_by_age_by_disability_status_town AS SELECT * FROM gold.fact_sex_by_age_by_disability_status_town;
    CREATE OR REPLACE VIEW fact_race_town AS SELECT * FROM gold.fact_race_town;
    CREATE OR REPLACE VIEW fact_hispanic_or_latino_origin_town AS SELECT * FROM gold.fact_hispanic_or_latino_origin_town;
    CREATE OR REPLACE VIEW fact_place_of_birth_by_nativity_and_citizenship_status_town AS SELECT * FROM gold.fact_place_of_birth_by_nativity_and_citizenship_status_town;
    CREATE OR REPLACE VIEW fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over_town AS SELECT * FROM gold.fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over_town;
    CREATE OR REPLACE VIEW fact_educational_attainment_for_the_population_25_years_and_over_town AS SELECT * FROM gold.fact_educational_attainment_for_the_population_25_years_and_over_town;
    CREATE OR REPLACE VIEW fact_types_of_computers_in_household_town AS SELECT * FROM gold.fact_types_of_computers_in_household_town;
    CREATE OR REPLACE VIEW fact_presence_and_types_of_internet_subscriptions_in_household_town AS SELECT * FROM gold.fact_presence_and_types_of_internet_subscriptions_in_household_town;
    CREATE OR REPLACE VIEW fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town AS SELECT * FROM gold.fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town;
    CREATE OR REPLACE VIEW fact_poverty_status_in_the_past_12_months_by_sex_by_age_town AS SELECT * FROM gold.fact_poverty_status_in_the_past_12_months_by_sex_by_age_town;
    CREATE OR REPLACE VIEW fact_employment_status_for_the_population_16_years_and_over_town AS SELECT * FROM gold.fact_employment_status_for_the_population_16_years_and_over_town;
    CREATE OR REPLACE VIEW fact_health_insurance_coverage_status_by_sex_by_age_town AS SELECT * FROM gold.fact_health_insurance_coverage_status_by_sex_by_age_town;
    CREATE OR REPLACE VIEW fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town AS SELECT * FROM gold.fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town;

    -- New views for topic-based aggregations (Housing, Healthcare, Childcare, Transportation, Equity)
    CREATE OR REPLACE VIEW fact_vacancy_status_town AS SELECT * FROM gold.fact_vacancy_status_town;
    CREATE OR REPLACE VIEW fact_housing_units_town AS SELECT * FROM gold.fact_housing_units_town;
    CREATE OR REPLACE VIEW fact_tenure_by_year_householder_moved_into_unit_town AS SELECT * FROM gold.fact_tenure_by_year_householder_moved_into_unit_town;
    CREATE OR REPLACE VIEW fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_town AS SELECT * FROM gold.fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_town;
    CREATE OR REPLACE VIEW fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_town AS SELECT * FROM gold.fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_town;
    CREATE OR REPLACE VIEW fact_means_of_transportation_to_work_town AS SELECT * FROM gold.fact_means_of_transportation_to_work_town;
    CREATE OR REPLACE VIEW fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_town AS SELECT * FROM gold.fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_town;
    CREATE OR REPLACE VIEW fact_household_size_by_vehicles_available_town AS SELECT * FROM gold.fact_household_size_by_vehicles_available_town;
    CREATE OR REPLACE VIEW fact_total_population_town AS SELECT * FROM gold.fact_total_population_town;

    CREATE OR REPLACE VIEW fact_educational_attainment_town AS SELECT * FROM gold.fact_educational_attainment_town;
    CREATE OR REPLACE VIEW fact_mh_su_facilities AS SELECT * FROM gold.fact_mh_su_facilities;
    -- CDC PLACES: expose Gold views into main schema for aggregate layer
    CREATE OR REPLACE VIEW fact_health_outcomes_town  AS SELECT * FROM gold.fact_health_outcomes_town;
    CREATE OR REPLACE VIEW fact_health_status_town    AS SELECT * FROM gold.fact_health_status_town;
    CREATE OR REPLACE VIEW fact_prevention_town       AS SELECT * FROM gold.fact_prevention_town;
    CREATE OR REPLACE VIEW fact_disability_town       AS SELECT * FROM gold.fact_disability_town;
    CREATE OR REPLACE VIEW fact_risk_behaviors_town   AS SELECT * FROM gold.fact_risk_behaviors_town;
    CREATE OR REPLACE VIEW fact_social_needs_town     AS SELECT * FROM gold.fact_social_needs_town;
    CREATE OR REPLACE VIEW v_gold_fact_fair_market_rent AS SELECT * FROM gold.fact_fair_market_rent;
    CREATE OR REPLACE VIEW v_gold_fact_ami_affordability_gap AS SELECT * FROM gold.fact_ami_affordability_gap;
    CREATE OR REPLACE VIEW v_gold_fact_occupation_housing_wage AS SELECT * FROM gold.fact_occupation_housing_wage;
    CREATE OR REPLACE VIEW v_gold_cdc_places AS SELECT * FROM gold.cdc_places;
            
    -- ==========================================
    -- Aggregation 1: Town Health Data
    -- Groups income and calculates insurance statuses
    -- Sources: fact_household_income_in_the_past_12_months_town,
    --          fact_tenure_by_vehicles_available_town,
    --          fact_types_of_health_insurance_coverage_by_age_town,
    --          fact_health_insurance_coverage_status_and_type_by_employment_status_town,
    --          fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_health_data;
    CREATE OR REPLACE TABLE main.agg_town_health_data AS
    SELECT
      hi.place_GEOID AS GEOID,
      t.town_name AS town,
      hi.total AS total_population,

      -- Combine micro-brackets into high-level income groups
      (hi.total_less_than_10_000 + hi.total_10_000_to_14_999 + hi.total_15_000_to_19_999 + hi.total_20_000_to_24_999) AS "Income Under $25k",
      (hi.total_25_000_to_29_999 + hi.total_30_000_to_34_999 + hi.total_35_000_to_39_999 + hi.total_40_000_to_44_999 + hi.total_45_000_to_49_999) AS "Income $25k - $50k",
      (hi.total_50_000_to_59_999 + hi.total_60_000_to_74_999 + hi.total_75_000_to_99_999) AS "Income $50k - $100k",
      (hi.total_100_000_to_124_999 + hi.total_125_000_to_149_999 + hi.total_150_000_to_199_999 + hi.total_200_000_or_more) AS "Income $100k +",

      tv.total_owner_occupied_no_vehicle_available AS total_owner_no_vehicle,
      tv.total_renter_occupied_no_vehicle_available AS total_renter_no_vehicle,

      -- Aggregate insurance metrics across age groups
      (hia.total_under_19_years_with_one_type_of_health_insurance_coverage + hia.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage) AS ins_U19,
      hia.total_under_19_years_no_health_insurance_coverage AS no_ins_U19,
      he.total_in_labor_force_employed_with_health_insurance_coverage AS emp_insured,
      he.total_in_labor_force_employed_no_health_insurance_coverage AS emp_uninsured,
      he.total_in_labor_force_unemployed_with_health_insurance_coverage AS unemp_insured,
      he.total_in_labor_force_unemployed_no_health_insurance_coverage AS unemp_uninsured,
      he.total_in_labor_force AS tot_lf,
      he.total_not_in_labor_force_with_health_insurance_coverage AS nilf_insured,
      he.total_not_in_labor_force_no_health_insurance_coverage AS nilf_uninsured,

      -- Sum total insured/uninsured across income brackets
      (hii.total_under_25_000_with_health_insurance_coverage + hii.total_under_25_000_no_health_insurance_coverage +
      hii.total_25_000_to_49_999_with_health_insurance_coverage + hii.total_25_000_to_49_999_no_health_insurance_coverage +
      hii.total_50_000_to_74_999_with_health_insurance_coverage + hii.total_50_000_to_74_999_no_health_insurance_coverage +
      hii.total_75_000_to_99_999_with_health_insurance_coverage + hii.total_75_000_to_99_999_no_health_insurance_coverage +
      hii.total_100_000_or_more_with_health_insurance_coverage + hii.total_100_000_or_more_no_health_insurance_coverage) AS tot_ins_inc_pop,

      hii.total_under_25_000_with_health_insurance_coverage AS ins_U25,
      hii.total_under_25_000_no_health_insurance_coverage AS no_ins_U25,
      hii.total_25_000_to_49_999_with_health_insurance_coverage AS ins_25_50,
      hii.total_25_000_to_49_999_no_health_insurance_coverage AS no_ins_25_50,
      hii.total_50_000_to_74_999_with_health_insurance_coverage AS ins_50_75,
      hii.total_50_000_to_74_999_no_health_insurance_coverage AS no_ins_50_75,
      hii.total_75_000_to_99_999_with_health_insurance_coverage AS ins_75_100,
      hii.total_75_000_to_99_999_no_health_insurance_coverage AS no_ins_75_100,
      hii.total_100_000_or_more_with_health_insurance_coverage AS ins_100_above,
      hii.total_100_000_or_more_no_health_insurance_coverage AS no_ins_100_above,
      hi.year_key AS year
    FROM fact_household_income_in_the_past_12_months_town AS hi
    JOIN dim_town AS t ON hi.place_GEOID = t.place_GEOID
    JOIN fact_tenure_by_vehicles_available_town AS tv
      ON hi.place_GEOID = tv.place_GEOID AND hi.year_key = tv.year_key
    JOIN fact_types_of_health_insurance_coverage_by_age_town AS hia
      ON hi.place_GEOID = hia.place_GEOID AND hi.year_key = hia.year_key
    JOIN fact_health_insurance_coverage_status_and_type_by_employment_status_town AS he
      ON hi.place_GEOID = he.place_GEOID AND hi.year_key = he.year_key
    JOIN fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town AS hii
      ON hi.place_GEOID = hii.place_GEOID AND hi.year_key = hii.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 2: Town Health Insurance Data Mart
    -- Translates teammate's R 'mutate' logic into DuckDB SQL
    -- Sources: fact_health_insurance_coverage_status_by_sex_by_age_town (sex/age breakdown)
    --          fact_types_of_health_insurance_coverage_by_age_town (coverage type breakdown)
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_health_insurance;
    CREATE TABLE main.agg_town_health_insurance AS
    SELECT
        dt.town_name AS town,
        sa.place_GEOID AS GEOID,
        sa.year_key AS year,

        -- -----------------------------------------------------
        -- 1. INSURED BY AGE (ins_*)
        -- -----------------------------------------------------
        (sa.total_male_under_6_years_with_health_insurance_coverage +
         sa.total_male_6_to_18_years_with_health_insurance_coverage +
         sa.total_female_under_6_years_with_health_insurance_coverage +
         sa.total_female_6_to_18_years_with_health_insurance_coverage) AS ins_U18,

        (sa.total_male_19_to_25_years_with_health_insurance_coverage +
         sa.total_female_19_to_25_years_with_health_insurance_coverage) AS ins_19_25,

        (sa.total_male_26_to_34_years_with_health_insurance_coverage +
         sa.total_female_26_to_34_years_with_health_insurance_coverage) AS ins_26_34,

        (sa.total_male_35_to_44_years_with_health_insurance_coverage +
         sa.total_male_45_to_54_years_with_health_insurance_coverage +
         sa.total_male_55_to_64_years_with_health_insurance_coverage +
         sa.total_female_35_to_44_years_with_health_insurance_coverage +
         sa.total_female_45_to_54_years_with_health_insurance_coverage +
         sa.total_female_55_to_64_years_with_health_insurance_coverage) AS ins_35_64,

        (sa.total_male_65_to_74_years_with_health_insurance_coverage +
         sa.total_male_75_years_and_over_with_health_insurance_coverage +
         sa.total_female_65_to_74_years_with_health_insurance_coverage +
         sa.total_female_75_years_and_over_with_health_insurance_coverage) AS ins_65_over,

        -- -----------------------------------------------------
        -- 2. UNINSURED BY AGE (unins_*)
        -- -----------------------------------------------------
        (sa.total_male_under_6_years_no_health_insurance_coverage +
         sa.total_male_6_to_18_years_no_health_insurance_coverage +
         sa.total_female_under_6_years_no_health_insurance_coverage +
         sa.total_female_6_to_18_years_no_health_insurance_coverage) AS unins_U18,

        (sa.total_male_19_to_25_years_no_health_insurance_coverage +
         sa.total_female_19_to_25_years_no_health_insurance_coverage) AS unins_19_25,

        (sa.total_male_26_to_34_years_no_health_insurance_coverage +
         sa.total_female_26_to_34_years_no_health_insurance_coverage) AS unins_26_34,

        (sa.total_male_35_to_44_years_no_health_insurance_coverage +
         sa.total_male_45_to_54_years_no_health_insurance_coverage +
         sa.total_male_55_to_64_years_no_health_insurance_coverage +
         sa.total_female_35_to_44_years_no_health_insurance_coverage +
         sa.total_female_45_to_54_years_no_health_insurance_coverage +
         sa.total_female_55_to_64_years_no_health_insurance_coverage) AS unins_35_64,

        (sa.total_male_65_to_74_years_no_health_insurance_coverage +
         sa.total_male_75_years_and_over_no_health_insurance_coverage +
         sa.total_female_65_to_74_years_no_health_insurance_coverage +
         sa.total_female_75_years_and_over_no_health_insurance_coverage) AS unins_65_over,

        -- -----------------------------------------------------
        -- 3. OVERALL TOTALS
        -- -----------------------------------------------------
        (sa.total_male_under_6_years_with_health_insurance_coverage + sa.total_male_6_to_18_years_with_health_insurance_coverage + sa.total_female_under_6_years_with_health_insurance_coverage + sa.total_female_6_to_18_years_with_health_insurance_coverage + sa.total_male_19_to_25_years_with_health_insurance_coverage + sa.total_female_19_to_25_years_with_health_insurance_coverage + sa.total_male_26_to_34_years_with_health_insurance_coverage + sa.total_female_26_to_34_years_with_health_insurance_coverage + sa.total_male_35_to_44_years_with_health_insurance_coverage + sa.total_male_45_to_54_years_with_health_insurance_coverage + sa.total_male_55_to_64_years_with_health_insurance_coverage + sa.total_female_35_to_44_years_with_health_insurance_coverage + sa.total_female_45_to_54_years_with_health_insurance_coverage + sa.total_female_55_to_64_years_with_health_insurance_coverage + sa.total_male_65_to_74_years_with_health_insurance_coverage + sa.total_male_75_years_and_over_with_health_insurance_coverage + sa.total_female_65_to_74_years_with_health_insurance_coverage + sa.total_female_75_years_and_over_with_health_insurance_coverage) AS all_ins,

        (sa.total_male_under_6_years_no_health_insurance_coverage + sa.total_male_6_to_18_years_no_health_insurance_coverage + sa.total_female_under_6_years_no_health_insurance_coverage + sa.total_female_6_to_18_years_no_health_insurance_coverage + sa.total_male_19_to_25_years_no_health_insurance_coverage + sa.total_female_19_to_25_years_no_health_insurance_coverage + sa.total_male_26_to_34_years_no_health_insurance_coverage + sa.total_female_26_to_34_years_no_health_insurance_coverage + sa.total_male_35_to_44_years_no_health_insurance_coverage + sa.total_male_45_to_54_years_no_health_insurance_coverage + sa.total_male_55_to_64_years_no_health_insurance_coverage + sa.total_female_35_to_44_years_no_health_insurance_coverage + sa.total_female_45_to_54_years_no_health_insurance_coverage + sa.total_female_55_to_64_years_no_health_insurance_coverage + sa.total_male_65_to_74_years_no_health_insurance_coverage + sa.total_male_75_years_and_over_no_health_insurance_coverage + sa.total_female_65_to_74_years_no_health_insurance_coverage + sa.total_female_75_years_and_over_no_health_insurance_coverage) AS all_unins,

        -- -----------------------------------------------------
        -- 4. TYPE OF COVERAGE (B27010 Variables)
        -- -----------------------------------------------------

        -- Employer based:
        -- Note: Added 19-34 variables here for data integrity (Teammate's R code missed them)
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_employer_based_health_insurance_only +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_employer_based_health_insurance_only +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_35_to_64_years_with_one_type_of_health_insurance_coverage_with_employer_based_health_insurance_only +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_65_years_and_over_with_one_type_of_health_insurance_coverage_with_employer_based_health_insurance_only +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage) AS emp_based_ins,

        -- Direct purchase:
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_direct_purchase_health_insurance_only +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_direct_purchase_health_insurance_only +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_direct_purchase_and_medicare_coverage +
         hc.total_65_years_and_over_with_one_type_of_health_insurance_coverage_with_direct_purchase_health_insurance_only +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_direct_purchase_coverage +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_direct_purchase_and_medicare_coverage) AS dir_purchase_ins,

        -- Medicare:
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_medicare_coverage_only +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_medicare_coverage_only +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_35_to_64_years_with_one_type_of_health_insurance_coverage_with_medicare_coverage_only +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_direct_purchase_and_medicare_coverage +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_65_years_and_over_with_one_type_of_health_insurance_coverage_with_medicare_coverage_only +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_employer_based_and_medicare_coverage +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_direct_purchase_and_medicare_coverage +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage) AS medicare_cov,

        -- Medicaid:
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_medicaid_means_tested_public_coverage_only +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_medicaid_means_tested_public_coverage_only +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_35_to_64_years_with_one_type_of_health_insurance_coverage_with_medicaid_means_tested_public_coverage_only +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_with_medicare_and_medicaid_means_tested_public_coverage) AS medicaid_cov,

        -- TRICARE:
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_tricare_military_health_coverage_only +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_tricare_military_health_coverage_only +
         hc.total_35_to_64_years_with_one_type_of_health_insurance_coverage_with_tricare_military_health_coverage_only +
         hc.total_65_years_and_over_with_one_type_of_health_insurance_coverage_with_tricare_military_health_coverage_only) AS tricare_cov,

        -- VA coverage:
        (hc.total_under_19_years_with_one_type_of_health_insurance_coverage_with_va_health_care_only +
         hc.total_19_to_34_years_with_one_type_of_health_insurance_coverage_with_va_health_care_only +
         hc.total_35_to_64_years_with_one_type_of_health_insurance_coverage_with_va_health_care_only +
         hc.total_65_years_and_over_with_one_type_of_health_insurance_coverage_with_va_health_care_only) AS va_cov,

        -- Other:
        (hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_other_private_only_combinations +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_other_public_only_combinations +
         hc.total_under_19_years_with_two_or_more_types_of_health_insurance_coverage_other_coverage_combinations +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_other_private_only_combinations +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_other_public_only_combinations +
         hc.total_19_to_34_years_with_two_or_more_types_of_health_insurance_coverage_other_coverage_combinations +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_other_private_only_combinations +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_other_public_only_combinations +
         hc.total_35_to_64_years_with_two_or_more_types_of_health_insurance_coverage_other_coverage_combinations +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_other_private_only_combinations +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_other_public_only_combinations +
         hc.total_65_years_and_over_with_two_or_more_types_of_health_insurance_coverage_other_coverage_combinations) AS other_cov_type

    FROM
      fact_health_insurance_coverage_status_by_sex_by_age_town AS sa
    JOIN
      dim_town AS dt
    ON
      sa.place_GEOID = dt.place_GEOID
    JOIN
      fact_types_of_health_insurance_coverage_by_age_town AS hc
    ON
      sa.place_GEOID = hc.place_GEOID AND sa.year_key = hc.year_key
    ORDER BY
      town, year;


    -- ==========================================
    -- Aggregation 3: Town Economic Trends
    -- Extracts pure median indicators for financial tracking
    -- Sources: fact_median_household_income_in_the_past_12_months_town,
    --          fact_median_value_dollars_town, fact_median_gross_rent_dollars_town,
    --          fact_gini_index_of_income_inequality_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_economic_trends;
    CREATE TABLE main.agg_town_economic_trends AS
    SELECT
      mi.place_GEOID AS GEOID,
      t.town_name AS town,
      mi.year_key AS year,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_income,
      mv.median_value_dollars AS median_home_value,
      mr.median_gross_rent AS median_rent,
      gi.gini_index AS income_inequality_gini
    FROM fact_median_household_income_in_the_past_12_months_town AS mi
    JOIN dim_town AS t ON mi.place_GEOID = t.place_GEOID
    JOIN fact_median_value_dollars_town AS mv
      ON mi.place_GEOID = mv.place_GEOID AND mi.year_key = mv.year_key
    JOIN fact_median_gross_rent_dollars_town AS mr
      ON mi.place_GEOID = mr.place_GEOID AND mi.year_key = mr.year_key
    JOIN fact_gini_index_of_income_inequality_town AS gi
      ON mi.place_GEOID = gi.place_GEOID AND mi.year_key = gi.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 4: Economic Mobility & Education
    -- Tracks relationship between degree attainment and poverty
    -- Sources: fact_educational_attainment_for_the_population_25_years_and_over_town,
    --          fact_median_household_income_in_the_past_12_months_town,
    --          fact_gini_index_of_income_inequality_town,
    --          fact_poverty_status_in_the_past_12_months_by_sex_by_age_town,
    --          fact_employment_status_for_the_population_16_years_and_over_town (unemployed count),
    --          fact_health_insurance_coverage_status_and_type_by_employment_status_town (labor force total)
    -- ==========================================
    DROP TABLE IF EXISTS main.economic_mobility_education;
    CREATE TABLE main.economic_mobility_education AS
    SELECT
      ed.place_GEOID AS GEOID,
      t.town_name AS town,
      ed.year_key AS year,
      ed.total AS total_population,
      ROUND(((ed.total_bachelor_s_degree + ed.total_master_s_degree)/ed.total)*100,2) AS bachelors_masters_rate_pct,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_income,
      gi.gini_index AS income_inequality_gini,
      ROUND((pv.total_income_in_the_past_12_months_below_poverty_level/ed.total)*100,2) AS poverty_rate_pct,
      ROUND((es.total_in_labor_force_civilian_labor_force_unemployed/he.total_in_labor_force)*100,2) AS labor_unemployment_rate_pct
    FROM fact_educational_attainment_for_the_population_25_years_and_over_town AS ed
    JOIN dim_town AS t ON ed.place_GEOID = t.place_GEOID
    JOIN fact_median_household_income_in_the_past_12_months_town AS mi
      ON ed.place_GEOID = mi.place_GEOID AND ed.year_key = mi.year_key
    JOIN fact_gini_index_of_income_inequality_town AS gi
      ON ed.place_GEOID = gi.place_GEOID AND ed.year_key = gi.year_key
    JOIN fact_poverty_status_in_the_past_12_months_by_sex_by_age_town AS pv
      ON ed.place_GEOID = pv.place_GEOID AND ed.year_key = pv.year_key
    JOIN fact_employment_status_for_the_population_16_years_and_over_town AS es
      ON ed.place_GEOID = es.place_GEOID AND ed.year_key = es.year_key
    JOIN fact_health_insurance_coverage_status_and_type_by_employment_status_town AS he
      ON ed.place_GEOID = he.place_GEOID AND ed.year_key = he.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 5: Educational Attainment by Town
    -- Mirrors structure of educational_attainment_by_town_acs_b15003.csv reference
    -- Source: fact_educational_attainment_for_the_population_25_years_and_over_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_educational_attainment;
    CREATE TABLE main.agg_town_educational_attainment AS
    SELECT
      ed.place_GEOID AS GEOID,
      t.town_name AS town,
      ed.year_key AS year,
      ed.total AS total_pop_25_plus,

      -- Less than high school (no schooling through 12th grade no diploma)
      (ed.total_no_schooling_completed + ed.total_nursery_school + ed.total_kindergarten +
       ed.total_1st_grade + ed.total_2nd_grade + ed.total_3rd_grade + ed.total_4th_grade +
       ed.total_5th_grade + ed.total_6th_grade + ed.total_7th_grade + ed.total_8th_grade +
       ed.total_9th_grade + ed.total_10th_grade + ed.total_11th_grade +
       ed.total_12th_grade_no_diploma) AS n_less_than_hs,

      ed.total_regular_high_school_diploma AS n_hs_diploma,
      ed.total_ged_or_alternative_credential AS n_ged_or_equiv,
      ed.total_some_college_1_or_more_years_no_degree + ed.total_some_college_less_than_1_year AS n_some_college_no_degree,
      ed.total_associate_s_degree AS n_associates,
      ed.total_bachelor_s_degree AS n_bachelors,
      ed.total_master_s_degree AS n_masters,
      ed.total_professional_school_degree AS n_professional_degree,
      ed.total_doctorate_degree AS n_doctorate,

      -- Group totals (matches CSV reference pattern)
      (ed.total_regular_high_school_diploma + ed.total_ged_or_alternative_credential) AS n_hs_or_equiv_total,
      (ed.total_master_s_degree + ed.total_professional_school_degree + ed.total_doctorate_degree) AS n_graduate_or_prof,

      -- Percentages (rounded to 2 decimals, matches CSV reference precision)
      ROUND((
        (ed.total_no_schooling_completed + ed.total_nursery_school + ed.total_kindergarten +
         ed.total_1st_grade + ed.total_2nd_grade + ed.total_3rd_grade + ed.total_4th_grade +
         ed.total_5th_grade + ed.total_6th_grade + ed.total_7th_grade + ed.total_8th_grade +
         ed.total_9th_grade + ed.total_10th_grade + ed.total_11th_grade +
         ed.total_12th_grade_no_diploma) / NULLIF(ed.total,0)
      ) * 100, 2) AS pct_less_than_hs,

      ROUND(((ed.total_regular_high_school_diploma + ed.total_ged_or_alternative_credential) / NULLIF(ed.total,0)) * 100, 2) AS pct_hs_or_equiv,
      ROUND(((ed.total_some_college_1_or_more_years_no_degree + ed.total_some_college_less_than_1_year) / NULLIF(ed.total,0)) * 100, 2) AS pct_some_college_no_degree,
      ROUND((ed.total_associate_s_degree / NULLIF(ed.total,0)) * 100, 2) AS pct_associates,
      ROUND((ed.total_bachelor_s_degree / NULLIF(ed.total,0)) * 100, 2) AS pct_bachelors,
      ROUND(((ed.total_master_s_degree + ed.total_professional_school_degree + ed.total_doctorate_degree) / NULLIF(ed.total,0)) * 100, 2) AS pct_graduate_or_prof,
      ROUND(((ed.total_bachelor_s_degree + ed.total_master_s_degree + ed.total_professional_school_degree + ed.total_doctorate_degree) / NULLIF(ed.total,0)) * 100, 2) AS pct_bachelors_or_higher

    FROM fact_educational_attainment_for_the_population_25_years_and_over_town AS ed
    JOIN dim_town AS t ON ed.place_GEOID = t.place_GEOID
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 6: School Enrollment by Level by Town
    -- Mirrors structure of school_enrollment_by_level_by_town_acs_b14001.csv reference
    -- Source: fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_school_enrollment;
    CREATE TABLE main.agg_town_school_enrollment AS
    SELECT
      se.place_GEOID AS GEOID,
      t.town_name AS town,
      se.year_key AS year,
      se.total AS total_pop_3_plus,
      se.total_enrolled_in_school AS n_enrolled_total,
      se.total_enrolled_in_school_enrolled_in_nursery_school_preschool AS n_enrolled_preschool,
      se.total_enrolled_in_school_enrolled_in_kindergarten AS n_enrolled_kindergarten,
      se.total_enrolled_in_school_enrolled_in_grade_1_to_grade_4 AS n_enrolled_elem_gr1_4,
      se.total_enrolled_in_school_enrolled_in_grade_5_to_grade_8 AS n_enrolled_elem_gr5_8,
      se.total_enrolled_in_school_enrolled_in_grade_9_to_grade_12 AS n_enrolled_high_school_gr9_12,

      -- K-12 total (kindergarten through grade 12)
      (se.total_enrolled_in_school_enrolled_in_kindergarten +
       se.total_enrolled_in_school_enrolled_in_grade_1_to_grade_4 +
       se.total_enrolled_in_school_enrolled_in_grade_5_to_grade_8 +
       se.total_enrolled_in_school_enrolled_in_grade_9_to_grade_12) AS n_enrolled_k12_total,

      se.total_enrolled_in_school_enrolled_in_college_undergraduate_years AS n_enrolled_college_undergrad,
      se.total_enrolled_in_school_graduate_or_professional_school AS n_enrolled_grad_professional,
      se.total_not_enrolled_in_school AS n_not_enrolled,

      -- Percentages (rounded to 2 decimals, matches CSV reference precision)
      ROUND((se.total_enrolled_in_school / NULLIF(se.total,0)) * 100, 2) AS pct_enrolled_total,
      ROUND((
        (se.total_enrolled_in_school_enrolled_in_kindergarten +
         se.total_enrolled_in_school_enrolled_in_grade_1_to_grade_4 +
         se.total_enrolled_in_school_enrolled_in_grade_5_to_grade_8 +
         se.total_enrolled_in_school_enrolled_in_grade_9_to_grade_12) / NULLIF(se.total,0)
      ) * 100, 2) AS pct_enrolled_k12,
      ROUND((se.total_enrolled_in_school_enrolled_in_nursery_school_preschool / NULLIF(se.total,0)) * 100, 2) AS pct_enrolled_preschool,
      ROUND((se.total_enrolled_in_school_enrolled_in_grade_9_to_grade_12 / NULLIF(se.total,0)) * 100, 2) AS pct_enrolled_high_school,
      ROUND(((se.total_enrolled_in_school_enrolled_in_college_undergraduate_years + se.total_enrolled_in_school_graduate_or_professional_school) / NULLIF(se.total,0)) * 100, 2) AS pct_enrolled_college_plus

    FROM fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town AS se
    JOIN dim_town AS t ON se.place_GEOID = t.place_GEOID
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 7: Town Demographics
    -- Normalizes demographic totals into percentage rates
    -- Sources: fact_sex_by_age_by_disability_status_town, fact_race_town,
    --          fact_hispanic_or_latino_origin_town, fact_place_of_birth_by_nativity_and_citizenship_status_town,
    --          fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over_town,
    --          fact_educational_attainment_for_the_population_25_years_and_over_town,
    --          fact_types_of_computers_in_household_town, fact_presence_and_types_of_internet_subscriptions_in_household_town,
    --          fact_median_household_income_in_the_past_12_months_town, fact_median_value_dollars_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_demographics;
    CREATE TABLE main.agg_town_demographics AS
    SELECT
      sx.place_GEOID AS GEOID,
      t.town_name AS town,
      sx.year_key AS year,
      sx.total AS total_population,
      sx.total_male AS male_population,
      sx.total_female AS female_population,
      ROUND((sx.total_male/sx.total)*100,2) AS "male_rate",
      ROUND((sx.total_female/sx.total)*100,2) AS "female_rate",
      -- Race categories (mutually exclusive ACS race groups)
      rc.total_white_alone AS race_white_alone,
      rc.total_black_or_african_american_alone AS race_black_alone,
      rc.total_asian_alone AS race_asian_alone,
      -- Hispanic/Latino is a separate ACS ethnicity axis, not a race category —
      -- it can overlap with any race group above, so it is reported independently
      -- rather than summed alongside race_* fields
      hl.total_hispanic_or_latino AS ethnicity_hispanic_or_latino,
      ROUND((hl.total_hispanic_or_latino/sx.total)*100,2) AS "hispanic_or_latino_rate",
      pb.total_foreign_born AS foregin_born_population,
      ROUND((pb.total_foreign_born/sx.total)*100,2) AS "foreign_born_rate",
      vt.total_veteran AS veteran_population,
      ROUND((vt.total_veteran/sx.total)*100,2) AS "verteran_rate",
      ed.total_regular_high_school_diploma AS edu_high_school_diploma,
      ed.total_bachelor_s_degree AS edu_bachelor_degree,
      ed.total_master_s_degree AS edu_master_degree,
      cp.total_no_computer AS digital_no_computer,
      it.total_no_internet_access AS digital_no_interest_access,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_household_income,
      mv.median_value_dollars AS median_home_value
    FROM fact_sex_by_age_by_disability_status_town AS sx
    JOIN dim_town AS t ON sx.place_GEOID = t.place_GEOID
    JOIN fact_race_town AS rc
      ON sx.place_GEOID = rc.place_GEOID AND sx.year_key = rc.year_key
    JOIN fact_hispanic_or_latino_origin_town AS hl
      ON sx.place_GEOID = hl.place_GEOID AND sx.year_key = hl.year_key
    JOIN fact_place_of_birth_by_nativity_and_citizenship_status_town AS pb
      ON sx.place_GEOID = pb.place_GEOID AND sx.year_key = pb.year_key
    JOIN fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over_town AS vt
      ON sx.place_GEOID = vt.place_GEOID AND sx.year_key = vt.year_key
    JOIN fact_educational_attainment_for_the_population_25_years_and_over_town AS ed
      ON sx.place_GEOID = ed.place_GEOID AND sx.year_key = ed.year_key
    JOIN fact_types_of_computers_in_household_town AS cp
      ON sx.place_GEOID = cp.place_GEOID AND sx.year_key = cp.year_key
    JOIN fact_presence_and_types_of_internet_subscriptions_in_household_town AS it
      ON sx.place_GEOID = it.place_GEOID AND sx.year_key = it.year_key
    JOIN fact_median_household_income_in_the_past_12_months_town AS mi
      ON sx.place_GEOID = mi.place_GEOID AND sx.year_key = mi.year_key
    JOIN fact_median_value_dollars_town AS mv
      ON sx.place_GEOID = mv.place_GEOID AND sx.year_key = mv.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 8: Disability Status by Age and Sex by Town
    -- Tracks disability prevalence across age groups, broken out by sex
    -- Source: fact_sex_by_age_by_disability_status_town
    -- Requires acs_variables_raw to include the full B18101 age x sex x
    -- disability breakdown (B18101_004 through B18101_039)
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_disability_status;
    CREATE TABLE main.agg_town_disability_status AS
    SELECT
      ds.place_GEOID AS GEOID,
      t.town_name AS town,
      ds.year_key AS year,
      ds.total AS total_population,

      -- -----------------------------------------------------
      -- 1. WITH A DISABILITY, BY AGE GROUP (combined male + female)
      -- -----------------------------------------------------
      (ds.total_male_under_5_years_with_a_disability + ds.total_female_under_5_years_with_a_disability) AS disab_under5,
      (ds.total_male_5_to_17_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability) AS disab_5_17,
      (ds.total_male_18_to_34_years_with_a_disability + ds.total_female_18_to_34_years_with_a_disability) AS disab_18_34,
      (ds.total_male_35_to_64_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability) AS disab_35_64,
      (ds.total_male_65_to_74_years_with_a_disability + ds.total_female_65_to_74_years_with_a_disability) AS disab_65_74,
      (ds.total_male_75_years_and_over_with_a_disability + ds.total_female_75_years_and_over_with_a_disability) AS disab_75_over,

      -- -----------------------------------------------------
      -- 2. NO DISABILITY, BY AGE GROUP (combined male + female)
      -- -----------------------------------------------------
      (ds.total_male_under_5_years_no_disability + ds.total_female_under_5_years_no_disability) AS no_disab_under5,
      (ds.total_male_5_to_17_years_no_disability + ds.total_female_5_to_17_years_no_disability) AS no_disab_5_17,
      (ds.total_male_18_to_34_years_no_disability + ds.total_female_18_to_34_years_no_disability) AS no_disab_18_34,
      (ds.total_male_35_to_64_years_no_disability + ds.total_female_35_to_64_years_no_disability) AS no_disab_35_64,
      (ds.total_male_65_to_74_years_no_disability + ds.total_female_65_to_74_years_no_disability) AS no_disab_65_74,
      (ds.total_male_75_years_and_over_no_disability + ds.total_female_75_years_and_over_no_disability) AS no_disab_75_over,

      -- -----------------------------------------------------
      -- 3. TOTAL DISABILITY COUNT & OVERALL RATE
      -- -----------------------------------------------------
      (ds.total_male_under_5_years_with_a_disability + ds.total_female_under_5_years_with_a_disability +
       ds.total_male_5_to_17_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability +
       ds.total_male_18_to_34_years_with_a_disability + ds.total_female_18_to_34_years_with_a_disability +
       ds.total_male_35_to_64_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability +
       ds.total_male_65_to_74_years_with_a_disability + ds.total_female_65_to_74_years_with_a_disability +
       ds.total_male_75_years_and_over_with_a_disability + ds.total_female_75_years_and_over_with_a_disability) AS total_with_disability,

      ROUND((
        (ds.total_male_under_5_years_with_a_disability + ds.total_female_under_5_years_with_a_disability +
         ds.total_male_5_to_17_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability +
         ds.total_male_18_to_34_years_with_a_disability + ds.total_female_18_to_34_years_with_a_disability +
         ds.total_male_35_to_64_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability +
         ds.total_male_65_to_74_years_with_a_disability + ds.total_female_65_to_74_years_with_a_disability +
         ds.total_male_75_years_and_over_with_a_disability + ds.total_female_75_years_and_over_with_a_disability)
        / NULLIF(ds.total, 0)
      ) * 100, 2) AS disability_rate_pct,

      -- -----------------------------------------------------
      -- 4. DISABILITY RATE BY AGE GROUP (% of that age group's population)
      -- -----------------------------------------------------
      ROUND((
        (ds.total_male_under_5_years_with_a_disability + ds.total_female_under_5_years_with_a_disability)
        / NULLIF(ds.total_male_under_5_years_with_a_disability + ds.total_female_under_5_years_with_a_disability +
                 ds.total_male_under_5_years_no_disability + ds.total_female_under_5_years_no_disability, 0)
      ) * 100, 2) AS disability_rate_under5_pct,

      ROUND((
        (ds.total_male_5_to_17_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability)
        / NULLIF(ds.total_male_5_to_17_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability +
                 ds.total_male_5_to_17_years_no_disability + ds.total_female_5_to_17_years_no_disability, 0)
      ) * 100, 2) AS disability_rate_5_17_pct,

      ROUND((
        (ds.total_male_18_to_34_years_with_a_disability + ds.total_female_18_to_34_years_with_a_disability)
        / NULLIF(ds.total_male_18_to_34_years_with_a_disability + ds.total_female_18_to_34_years_with_a_disability +
                 ds.total_male_18_to_34_years_no_disability + ds.total_female_18_to_34_years_no_disability, 0)
      ) * 100, 2) AS disability_rate_18_34_pct,

      ROUND((
        (ds.total_male_35_to_64_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability)
        / NULLIF(ds.total_male_35_to_64_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability +
                 ds.total_male_35_to_64_years_no_disability + ds.total_female_35_to_64_years_no_disability, 0)
      ) * 100, 2) AS disability_rate_35_64_pct,

      ROUND((
        (ds.total_male_65_to_74_years_with_a_disability + ds.total_female_65_to_74_years_with_a_disability)
        / NULLIF(ds.total_male_65_to_74_years_with_a_disability + ds.total_female_65_to_74_years_with_a_disability +
                 ds.total_male_65_to_74_years_no_disability + ds.total_female_65_to_74_years_no_disability, 0)
      ) * 100, 2) AS disability_rate_65_74_pct,

      ROUND((
        (ds.total_male_75_years_and_over_with_a_disability + ds.total_female_75_years_and_over_with_a_disability)
        / NULLIF(ds.total_male_75_years_and_over_with_a_disability + ds.total_female_75_years_and_over_with_a_disability +
                 ds.total_male_75_years_and_over_no_disability + ds.total_female_75_years_and_over_no_disability, 0)
      ) * 100, 2) AS disability_rate_75_over_pct,

      -- -----------------------------------------------------
      -- 5. DISABILITY RATE BY SEX
      -- -----------------------------------------------------
      ROUND((
        (ds.total_male_under_5_years_with_a_disability + ds.total_male_5_to_17_years_with_a_disability +
         ds.total_male_18_to_34_years_with_a_disability + ds.total_male_35_to_64_years_with_a_disability +
         ds.total_male_65_to_74_years_with_a_disability + ds.total_male_75_years_and_over_with_a_disability)
        / NULLIF(ds.total_male, 0)
      ) * 100, 2) AS disability_rate_male_pct,

      ROUND((
        (ds.total_female_under_5_years_with_a_disability + ds.total_female_5_to_17_years_with_a_disability +
         ds.total_female_18_to_34_years_with_a_disability + ds.total_female_35_to_64_years_with_a_disability +
         ds.total_female_65_to_74_years_with_a_disability + ds.total_female_75_years_and_over_with_a_disability)
        / NULLIF(ds.total_female, 0)
      ) * 100, 2) AS disability_rate_female_pct

    FROM fact_sex_by_age_by_disability_status_town AS ds
    JOIN dim_town AS t ON ds.place_GEOID = t.place_GEOID
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 9: Racial Equity Snapshot
    -- Race/ethnicity population shares as a foundation for cross-topic equity analysis
    -- (e.g. join this to poverty, housing burden, or health insurance tables by GEOID/year
    -- to compute disparity ratios across topics)
    -- Source: fact_race_town, fact_hispanic_or_latino_origin_town, fact_total_population_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_racial_equity_snapshot;
    CREATE TABLE main.agg_town_racial_equity_snapshot AS
    SELECT
      rc.place_GEOID AS GEOID,
      t.town_name AS town,
      rc.year_key AS year,
      tp.total AS total_population,
      rc.total_white_alone AS pop_white_alone,
      rc.total_black_or_african_american_alone AS pop_black_alone,
      rc.total_asian_alone AS pop_asian_alone,
      hl.total_hispanic_or_latino AS pop_hispanic_or_latino,
      ROUND((rc.total_white_alone / NULLIF(tp.total, 0)) * 100, 2) AS pct_white_alone,
      ROUND((rc.total_black_or_african_american_alone / NULLIF(tp.total, 0)) * 100, 2) AS pct_black_alone,
      ROUND((rc.total_asian_alone / NULLIF(tp.total, 0)) * 100, 2) AS pct_asian_alone,
      ROUND((hl.total_hispanic_or_latino / NULLIF(tp.total, 0)) * 100, 2) AS pct_hispanic_or_latino
    FROM fact_race_town AS rc
    JOIN dim_town AS t ON rc.place_GEOID = t.place_GEOID
    JOIN fact_hispanic_or_latino_origin_town AS hl
      ON rc.place_GEOID = hl.place_GEOID AND rc.year_key = hl.year_key
    JOIN fact_total_population_town AS tp
      ON rc.place_GEOID = tp.place_GEOID AND rc.year_key = tp.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 10: Town Housing Burden
    -- Calculates rent burden percentages
    -- Sources: fact_tenure_town, fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_housing_burden;
    CREATE TABLE main.agg_town_housing_burden AS
    SELECT
      tn.place_GEOID AS GEOID,
      t.town_name AS town,
      tn.year_key AS year,
      tn.total_renter_occupied AS total_households,
      (rb.total_30_0_to_34_9_percent + rb.total_35_0_to_39_9_percent + rb.total_40_0_to_49_9_percent) AS cost_burdened_households,
      rb.total_50_0_percent_or_more AS severely_cost_burdened_households,
      ROUND(((rb.total_30_0_to_34_9_percent + rb.total_35_0_to_39_9_percent + rb.total_40_0_to_49_9_percent + rb.total_50_0_percent_or_more) / tn.total_renter_occupied) * 100, 1) AS "housing_burden_rate_%"
    FROM fact_tenure_town AS tn
    JOIN dim_town AS t ON tn.place_GEOID = t.place_GEOID
    JOIN fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town AS rb
      ON tn.place_GEOID = rb.place_GEOID AND tn.year_key = rb.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 11: Housing Affordability Index
    -- Derives price-to-income and rent-to-income ratios
    -- Sources: fact_median_household_income_in_the_past_12_months_town, fact_median_value_dollars_town,
    --          fact_median_gross_rent_dollars_town,
    --          fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_housing_affordability_index;
    CREATE TABLE main.agg_town_housing_affordability_index AS
    SELECT
      mi.place_GEOID AS GEOID,
      t.town_name AS town,
      mi.year_key AS year,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_household_income,
      mv.median_value_dollars AS median_home_value,
      mr.median_gross_rent AS median_gross_rent,
      rp.median_gross_rent_as_a_percentage_of_household_income AS "median_rent_to_income_ratio_%",
      ROUND(mv.median_value_dollars / mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars, 2) AS "home_price_to_income_ratio"
    FROM fact_median_household_income_in_the_past_12_months_town AS mi
    JOIN dim_town AS t ON mi.place_GEOID = t.place_GEOID
    JOIN fact_median_value_dollars_town AS mv
      ON mi.place_GEOID = mv.place_GEOID AND mi.year_key = mv.year_key
    JOIN fact_median_gross_rent_dollars_town AS mr
      ON mi.place_GEOID = mr.place_GEOID AND mi.year_key = mr.year_key
    JOIN fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town AS rp
      ON mi.place_GEOID = rp.place_GEOID AND mi.year_key = rp.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 12: Housing Stability & Displacement Risk
    -- Now uses full B25038 breakdown (year moved in by tenure) for true
    -- displacement risk signal: recent movers vs long-tenured residents
    -- Source: fact_vacancy_status_town, fact_housing_units_town,
    --         fact_tenure_by_year_householder_moved_into_unit_town, fact_tenure_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_housing_stability;
    CREATE TABLE main.agg_town_housing_stability AS
    SELECT
      hu.place_GEOID AS GEOID,
      t.town_name AS town,
      hu.year_key AS year,
      hu.total AS total_housing_units,
      vc.total AS vacant_units,
      ROUND((vc.total / NULLIF(hu.total, 0)) * 100, 2) AS vacancy_rate_pct,

      tn.total_renter_occupied AS total_renter_occupied,
      ROUND((tn.total_renter_occupied / NULLIF(hu.total, 0)) * 100, 2) AS renter_occupied_rate_pct,
      ROUND((tn.total_owner_occupied / NULLIF(hu.total, 0)) * 100, 2) AS owner_occupied_rate_pct,

      -- Recent movers (moved in last 1-2 years) — higher values signal more turnover/displacement risk
      (mv.total_owner_occupied_moved_in_2023_or_later + mv.total_renter_occupied_moved_in_2023_or_later) AS recent_movers_2023_or_later,
      ROUND((
        (mv.total_owner_occupied_moved_in_2023_or_later + mv.total_renter_occupied_moved_in_2023_or_later)
        / NULLIF(hu.total, 0)
      ) * 100, 2) AS recent_movers_rate_pct,

      -- Renters who moved recently vs long-tenured renters — proxy for renter turnover/displacement
      mv.total_renter_occupied_moved_in_2023_or_later AS renter_moved_2023_or_later,
      mv.total_renter_occupied_moved_in_2020_to_2022 AS renter_moved_2020_2022,
      (mv.total_renter_occupied_moved_in_2010_to_2019 + mv.total_renter_occupied_moved_in_2000_to_2009 +
       mv.total_renter_occupied_moved_in_1990_to_1999 + mv.total_renter_occupied_moved_in_1989_or_earlier) AS renter_long_tenured,
      ROUND((
        (mv.total_renter_occupied_moved_in_2023_or_later + mv.total_renter_occupied_moved_in_2020_to_2022)
        / NULLIF(tn.total_renter_occupied, 0)
      ) * 100, 2) AS renter_turnover_rate_pct,

      -- Owners who moved recently — far lower turnover is typically expected/healthy
      mv.total_owner_occupied_moved_in_2023_or_later AS owner_moved_2023_or_later,
      mv.total_owner_occupied_moved_in_2020_to_2022 AS owner_moved_2020_2022,
      (mv.total_owner_occupied_moved_in_2010_to_2019 + mv.total_owner_occupied_moved_in_2000_to_2009 +
       mv.total_owner_occupied_moved_in_1990_to_1999 + mv.total_owner_occupied_moved_in_1989_or_earlier) AS owner_long_tenured,
      ROUND((
        (mv.total_owner_occupied_moved_in_2023_or_later + mv.total_owner_occupied_moved_in_2020_to_2022)
        / NULLIF(tn.total_owner_occupied, 0)
      ) * 100, 2) AS owner_turnover_rate_pct

    FROM fact_housing_units_town AS hu
    JOIN dim_town AS t ON hu.place_GEOID = t.place_GEOID
    JOIN fact_vacancy_status_town AS vc
      ON hu.place_GEOID = vc.place_GEOID AND hu.year_key = vc.year_key
    JOIN fact_tenure_town AS tn
      ON hu.place_GEOID = tn.place_GEOID AND hu.year_key = tn.year_key
    JOIN fact_tenure_by_year_householder_moved_into_unit_town AS mv
      ON hu.place_GEOID = mv.place_GEOID AND hu.year_key = mv.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 13: Childcare & Parental Employment
    -- Now uses full B23008 breakdown to show how parental labor force
    -- participation shapes children's living/care arrangements
    -- Source: fact_age_of_own_children_under_18_years_in_families_and_subfamilies_..._town,
    --         fact_grandchildren_under_18_years_living_with_a_grandparent_householder_..._town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_childcare;
    CREATE TABLE main.agg_town_childcare AS
    SELECT
      ch.place_GEOID AS GEOID,
      t.town_name AS town,
      ch.year_key AS year,
      ch.total AS total_children_under_18,
      ch.total_under_6_years AS children_under6,
      ch.total_6_to_17_years AS children_6_17,

      -- -----------------------------------------------------
      -- Under 6 years: living arrangement by parental labor force status
      -- -----------------------------------------------------
      ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force AS under6_two_parents_both_working,
      ch.total_under_6_years_living_with_two_parents_father_only_in_labor_force AS under6_two_parents_father_only_working,
      ch.total_under_6_years_living_with_two_parents_mother_only_in_labor_force AS under6_two_parents_mother_only_working,
      ch.total_under_6_years_living_with_two_parents_neither_parent_in_labor_force AS under6_two_parents_neither_working,
      (ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force) AS under6_one_parent_working,
      (ch.total_under_6_years_living_with_one_parent_living_with_father_not_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_mother_not_in_labor_force) AS under6_one_parent_not_working,

      -- Likely needs childcare: under-6 children where all available parents in household are working
      (ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force) AS under6_likely_needs_childcare,
      ROUND((
        (ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force +
         ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
         ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force)
        / NULLIF(ch.total_under_6_years, 0)
      ) * 100, 2) AS pct_under6_likely_needs_childcare,

      -- -----------------------------------------------------
      -- 6 to 17 years: living arrangement by parental labor force status
      -- -----------------------------------------------------
      ch.total_6_to_17_years_living_with_two_parents AS children_6_17_two_parent_hh,
      ROUND((ch.total_6_to_17_years_living_with_two_parents / NULLIF(ch.total_6_to_17_years, 0)) * 100, 2) AS pct_6_17_two_parent_hh,

      ch.total_6_to_17_years_living_with_two_parents_both_parents_in_labor_force AS age6_17_two_parents_both_working,
      ch.total_6_to_17_years_living_with_two_parents_father_only_in_labor_force AS age6_17_two_parents_father_only_working,
      ch.total_6_to_17_years_living_with_two_parents_mother_only_in_labor_force AS age6_17_two_parents_mother_only_working,
      ch.total_6_to_17_years_living_with_two_parents_neither_parent_in_labor_force AS age6_17_two_parents_neither_working,
      (ch.total_6_to_17_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_mother_in_labor_force) AS age6_17_one_parent_working,
      (ch.total_6_to_17_years_living_with_one_parent_living_with_father_not_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_mother_not_in_labor_force) AS age6_17_one_parent_not_working,

      -- After-school care need proxy: 6-17 children where all available parents in household are working
      (ch.total_6_to_17_years_living_with_two_parents_both_parents_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_mother_in_labor_force) AS age6_17_likely_needs_afterschool_care,
      ROUND((
        (ch.total_6_to_17_years_living_with_two_parents_both_parents_in_labor_force +
         ch.total_6_to_17_years_living_with_one_parent_living_with_father_in_labor_force +
         ch.total_6_to_17_years_living_with_one_parent_living_with_mother_in_labor_force)
        / NULLIF(ch.total_6_to_17_years, 0)
      ) * 100, 2) AS pct_6_17_likely_needs_afterschool_care,

      -- -----------------------------------------------------
      -- Grandparent caregivers (all ages)
      -- -----------------------------------------------------
      gp.total_grandparent_householder_responsible_for_own_grandchildren_under_18_years AS grandparent_caregiver_households

    FROM fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_town AS ch
    JOIN dim_town AS t ON ch.place_GEOID = t.place_GEOID
    JOIN fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_town AS gp
      ON ch.place_GEOID = gp.place_GEOID AND ch.year_key = gp.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 14: Transportation Access & Commute Burden
    -- Mode share, average commute time, and vehicle access by town
    -- Source: fact_means_of_transportation_to_work_town,
    --         fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_town,
    --         fact_household_size_by_vehicles_available_town
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_transportation_access;
    CREATE TABLE main.agg_town_transportation_access AS
    SELECT
      mt.place_GEOID AS GEOID,
      t.town_name AS town,
      mt.year_key AS year,
      mt.total AS total_workers,
      mt.total_car_truck_or_van_drove_alone AS commute_drove_alone,
      mt.total_public_transportation AS commute_public_transit,
      mt.total_walked AS commute_walked,
      mt.total_bicycle AS commute_bicycle,
      mt.total_worked_from_home AS commute_worked_from_home,
      ROUND((mt.total_car_truck_or_van_drove_alone / NULLIF(mt.total, 0)) * 100, 2) AS pct_drove_alone,
      ROUND((mt.total_public_transportation / NULLIF(mt.total, 0)) * 100, 2) AS pct_public_transit,
      ROUND((mt.total_worked_from_home / NULLIF(mt.total, 0)) * 100, 2) AS pct_worked_from_home,
      tt.aggregate_travel_time_to_work_in_minutes AS aggregate_commute_minutes,
      ROUND(tt.aggregate_travel_time_to_work_in_minutes / NULLIF(mt.total, 0), 1) AS avg_commute_minutes,
      hv.total_no_vehicle_available AS households_no_vehicle
    FROM fact_means_of_transportation_to_work_town AS mt
    JOIN dim_town AS t ON mt.place_GEOID = t.place_GEOID
    JOIN fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_town AS tt
      ON mt.place_GEOID = tt.place_GEOID AND mt.year_key = tt.year_key
    JOIN fact_household_size_by_vehicles_available_town AS hv
      ON mt.place_GEOID = hv.place_GEOID AND mt.year_key = hv.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 15: Infrastructure Accessibility
    -- Rates of car ownership, internet access, and healthcare coverage
    -- Sources: fact_tenure_town, fact_tenure_by_vehicles_available_town,
    --          fact_presence_and_types_of_internet_subscriptions_in_household_town,
    --          fact_health_insurance_coverage_status_and_type_by_employment_status_town,
    --          fact_types_of_health_insurance_coverage_by_age_town
    -- ==========================================
    DROP TABLE IF EXISTS main.analytics_infrastructure_accesibility;
    CREATE TABLE main.analytics_infrastructure_accesibility AS
    SELECT
      tn.place_GEOID AS GEOID,
      t.town_name AS town,
      tn.year_key AS year,
      ROUND((tv.total_owner_occupied_no_vehicle_available/tn.total_owner_occupied)*100,2) AS owner_no_car_rate_pct,
      ROUND((tv.total_renter_occupied_no_vehicle_available/tn.total_renter_occupied)*100,2) AS renter_no_car_rate_pct,
      ROUND((it.total_no_internet_access/(tn.total_owner_occupied+tn.total_renter_occupied))*100,2) AS household_no_internet_rate_pct,
      ROUND(((he.total_in_labor_force_employed_no_health_insurance_coverage+he.total_in_labor_force_unemployed_no_health_insurance_coverage)/he.total_in_labor_force)*100,2) AS labor_force_uninsured_rate_pct,
      ROUND((hia.total_65_years_and_over_no_health_insurance_coverage/hia.total)*100,2) AS senior_uninsured_rate_pct
    FROM fact_tenure_town AS tn
    JOIN dim_town AS t ON tn.place_GEOID = t.place_GEOID
    JOIN fact_tenure_by_vehicles_available_town AS tv
      ON tn.place_GEOID = tv.place_GEOID AND tn.year_key = tv.year_key
    JOIN fact_presence_and_types_of_internet_subscriptions_in_household_town AS it
      ON tn.place_GEOID = it.place_GEOID AND tn.year_key = it.year_key
    JOIN fact_health_insurance_coverage_status_and_type_by_employment_status_town AS he
      ON tn.place_GEOID = he.place_GEOID AND tn.year_key = he.year_key
    JOIN fact_types_of_health_insurance_coverage_by_age_town AS hia
      ON tn.place_GEOID = hia.place_GEOID AND tn.year_key = hia.year_key
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 16: Town Financial Hardship (ALICE)
    -- Tracks the proportion of households living in poverty or as ALICE
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_alice_household;
    CREATE TABLE main.agg_town_alice_household AS
    SELECT
      f.place_GEOID AS GEOID,
      t.town_name AS town,
      f.year_key AS year,
      f.total_households,
      f.poverty_households,
      f.alice_households,
      f.above_alice_households
    FROM fact_alice_town_household AS f
    JOIN dim_town AS t ON f.place_GEOID = t.place_GEOID
    WHERE t.town_name != 'Other'
    ORDER BY town, year;


    -- ==========================================
    -- Aggregation 17: County Financial Hardship (ALICE)
    -- Macro-level tracking of financial hardship across the entire county
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_county_alice_household;
    CREATE TABLE main.agg_county_alice_household AS
    SELECT
      f.county_GEOID AS GEOID,
      c.county_name AS county,
      f.year_key AS year,
      f.total_households,
      f.poverty_households,
      f.alice_households,
      f.above_alice_households
    FROM fact_alice_county AS f
    JOIN dim_county AS c ON f.county_GEOID = c.county_GEOID
    ORDER BY county, year;


    -- ==========================================
    -- Aggregation 18: Town Home Value Trends
    -- show home value data by town, housing type, and date acquired from zillow
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_home_value_trends;
    CREATE TABLE main.agg_town_home_value_trends AS
    SELECT
      t.town_name AS town,
      z.date_key AS date,
      z.housing_type,
      z.home_value,
      LAG(z.home_value) OVER (PARTITION BY t.town_name, z.housing_type ORDER BY z.date_key) AS prev_home_value
    FROM
      fact_zillow_home_value AS z
    LEFT JOIN
      dim_town AS t
    ON
      z.place_GEOID = t.place_GEOID
    ORDER BY
      town, housing_type, date;


    -- ==========================================
    -- Aggregation 19: Town Rent Trends
    -- show rent data by town and date acquired from zillow
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_rent_trends;
    CREATE TABLE main.agg_town_rent_trends AS
    SELECT
      t.town_name AS town,
      z.date_key AS date,
      z.rent_value,
      LAG(rent_value) OVER (PARTITION BY t.town_name ORDER BY z.date_key) AS prev_rent_value
    FROM
      fact_zillow_rent AS z
    LEFT JOIN
      dim_town AS t
    ON
      z.place_GEOID = t.place_GEOID
    ORDER BY
      town, date;


    -- ==========================================
    -- Aggregation 20: Zillow Market Affordability Index
    -- show affordability of the zillow market with a combination of census data
    -- ==========================================
    DROP TABLE IF EXISTS main.zillow_market_affordability_index;
    CREATE TABLE main.zillow_market_affordability_index AS
    WITH avg_home_values AS (
      SELECT place_GEOID, YEAR(date_key) AS year, AVG(home_value) AS avg_home_val
      FROM fact_zillow_home_value
      GROUP BY place_GEOID, YEAR(date_key)
    ),
    avg_rent AS (
      SELECT place_GEOID, YEAR(date_key) AS year, AVG(rent_value) AS avg_rent
      FROM fact_zillow_rent
      GROUP BY place_GEOID, YEAR(date_key)
    )
    SELECT
      dt.town_name AS town,
      mi.year_key AS year,
      a.avg_home_val,
      r.avg_rent,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_income,
      ROUND(a.avg_home_val/NULLIF(mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars, 0),2) AS price_to_income_ratio
    FROM
      fact_median_household_income_in_the_past_12_months_town AS mi
    LEFT JOIN
      avg_home_values AS a
    ON
      mi.place_GEOID = a.place_GEOID AND mi.year_key = a.year
    LEFT JOIN
      dim_town AS dt
    ON
      mi.place_GEOID = dt.place_GEOID
    LEFT JOIN
      avg_rent AS r
    ON
      mi.place_GEOID = r.place_GEOID AND mi.year_key = r.year
    ORDER BY
      town, year;

    -- ==========================================
    -- Aggregation 21: Town Educational Attainment by Age, Race, and Earnings (S1501)
    -- Source: fact_educational_attainment_town (S1501 subject table — place level only)
    -- Includes: population counts + percent by age group, race/ethnicity,
    --           median earnings by education level, and poverty rate by education level
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_educational_attainment_s1501;
    CREATE TABLE main.agg_town_educational_attainment_s1501 AS
    SELECT
      ed.place_GEOID AS GEOID,
      t.town_name AS town,
      ed.year_key AS year,

      -- -----------------------------------------------------
      -- 1. Population 18-24 by Educational Attainment (counts)
      -- -----------------------------------------------------
      ed.total_age_by_educational_attainment_population_18_to_24_years AS pop_18_24,
      ed.total_age_by_educational_attainment_population_18_to_24_years_less_than_high_school_graduate AS pop_18_24_less_than_hs,
      ed.total_age_by_educational_attainment_population_18_to_24_years_high_school_graduate_includes_equivalency AS pop_18_24_hs_grad,
      ed.total_age_by_educational_attainment_population_18_to_24_years_some_college_or_associate_s_degree AS pop_18_24_some_college,
      ed.total_age_by_educational_attainment_population_18_to_24_years_bachelor_s_degree_or_higher AS pop_18_24_bachelors_plus,

      -- Percent 18-24 (from C02)
      ed.percent_age_by_educational_attainment_population_25_years_and_over AS pct_25_plus_hs_grad_or_higher,

      -- -----------------------------------------------------
      -- 2. Population 25+ by Age Group (counts + hs/bachelors rates)
      -- -----------------------------------------------------
      ed.total_age_by_educational_attainment_population_25_to_34_years AS pop_25_34,
      ed.total_age_by_educational_attainment_population_25_to_34_years_high_school_graduate_or_higher AS pop_25_34_hs_grad_plus,
      ed.total_age_by_educational_attainment_population_25_to_34_years_bachelor_s_degree_or_higher AS pop_25_34_bachelors_plus,

      ed.total_age_by_educational_attainment_population_35_to_44_years AS pop_35_44,
      ed.total_age_by_educational_attainment_population_35_to_44_years_high_school_graduate_or_higher AS pop_35_44_hs_grad_plus,
      ed.total_age_by_educational_attainment_population_35_to_44_years_bachelor_s_degree_or_higher AS pop_35_44_bachelors_plus,

      ed.total_age_by_educational_attainment_population_45_to_64_years AS pop_45_64,
      ed.total_age_by_educational_attainment_population_45_to_64_years_high_school_graduate_or_higher AS pop_45_64_hs_grad_plus,
      ed.total_age_by_educational_attainment_population_45_to_64_years_bachelor_s_degree_or_higher AS pop_45_64_bachelors_plus,
      ed.percent_age_by_educational_attainment_population_45_to_64_years_high_school_graduate_or_higher AS pct_45_64_hs_grad_plus,
      ed.percent_age_by_educational_attainment_population_45_to_64_years_bachelor_s_degree_or_higher AS pct_45_64_bachelors_plus,

      ed.total_age_by_educational_attainment_population_65_years_and_over AS pop_65_plus,
      ed.total_age_by_educational_attainment_population_65_years_and_over_high_school_graduate_or_higher AS pop_65_plus_hs_grad_plus,
      ed.total_age_by_educational_attainment_population_65_years_and_over_bachelor_s_degree_or_higher AS pop_65_plus_bachelors_plus,
      ed.percent_age_by_educational_attainment_population_65_years_and_over_high_school_graduate_or_higher AS pct_65_plus_hs_grad_plus,
      ed.percent_age_by_educational_attainment_population_65_years_and_over_bachelor_s_degree_or_higher AS pct_65_plus_bachelors_plus,

      -- -----------------------------------------------------
      -- 3. Race/Ethnicity by Educational Attainment (counts + rates)
      -- For equity analysis — compare attainment across racial groups
      -- Note: Hispanic/Latino is ethnicity not race (can overlap with race categories)
      -- -----------------------------------------------------
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_not_hispanic_or_latino AS pop_white_non_hispanic,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_not_hispanic_or_latino_high_school_graduate_or_higher AS pop_white_non_hispanic_hs_plus,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_not_hispanic_or_latino_bachelor_s_degree_or_higher AS pop_white_non_hispanic_bachelors_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_not_hispanic_or_latino_high_school_graduate_or_higher AS pct_white_non_hispanic_hs_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_not_hispanic_or_latino_bachelor_s_degree_or_higher AS pct_white_non_hispanic_bachelors_plus,

      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_black_alone AS pop_black_alone,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_black_alone_high_school_graduate_or_higher AS pop_black_alone_hs_plus,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_black_alone_bachelor_s_degree_or_higher AS pop_black_alone_bachelors_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_black_alone_high_school_graduate_or_higher AS pct_black_alone_hs_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_black_alone_bachelor_s_degree_or_higher AS pct_black_alone_bachelors_plus,

      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_asian_alone AS pop_asian_alone,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_asian_alone_high_school_graduate_or_higher AS pop_asian_alone_hs_plus,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_asian_alone_bachelor_s_degree_or_higher AS pop_asian_alone_bachelors_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_high_school_graduate_or_higher AS pct_white_alone_hs_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_white_alone_bachelor_s_degree_or_higher AS pct_white_alone_bachelors_plus,

      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_hispanic_or_latino_origin AS pop_hispanic_or_latino,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_hispanic_or_latino_origin_high_school_graduate_or_higher AS pop_hispanic_or_latino_hs_plus,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_hispanic_or_latino_origin_bachelor_s_degree_or_higher AS pop_hispanic_or_latino_bachelors_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_hispanic_or_latino_origin_high_school_graduate_or_higher AS pct_hispanic_or_latino_hs_plus,
      ed.percent_race_and_hispanic_or_latino_origin_by_educational_attainment_hispanic_or_latino_origin_bachelor_s_degree_or_higher AS pct_hispanic_or_latino_bachelors_plus,

      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_two_or_more_races AS pop_two_or_more_races,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_two_or_more_races_high_school_graduate_or_higher AS pop_two_or_more_races_hs_plus,
      ed.total_race_and_hispanic_or_latino_origin_by_educational_attainment_two_or_more_races_bachelor_s_degree_or_higher AS pop_two_or_more_races_bachelors_plus,

      -- -----------------------------------------------------
      -- 4. Median Earnings by Education Level
      -- Economic return on education — key for economic mobility analysis
      -- -----------------------------------------------------
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings AS median_earnings_all,
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings_less_than_high_school_graduate AS median_earnings_less_than_hs,
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings_high_school_graduate_includes_equivalency AS median_earnings_hs_grad,
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings_some_college_or_associate_s_degree AS median_earnings_some_college,
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings_bachelor_s_degree AS median_earnings_bachelors,
      ed.total_median_earnings_in_the_past_12_months_in_2024_inflation_adjusted_dollars_population_25_years_and_over_with_earnings_graduate_or_professional_degree AS median_earnings_graduate,

      -- -----------------------------------------------------
      -- 5. Poverty Rate by Education Level
      -- Shows intersection of education and economic hardship
      -- -----------------------------------------------------
      ed.total_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_less_than_high_school_graduate AS poverty_count_less_than_hs,
      ed.total_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_high_school_graduate_includes_equivalency AS poverty_count_hs_grad,
      ed.total_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_some_college_or_associate_s_degree AS poverty_count_some_college,
      ed.total_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_bachelor_s_degree_or_higher AS poverty_count_bachelors_plus,
      ed.percent_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_high_school_graduate_includes_equivalency AS pct_poverty_hs_grad,
      ed.percent_poverty_rate_for_the_population_25_years_and_over_for_whom_poverty_status_is_determined_by_educational_attainment_level_some_college_or_associate_s_degree AS pct_poverty_some_college

    FROM fact_educational_attainment_town AS ed
    JOIN dim_town AS t ON ed.place_GEOID = t.place_GEOID
    WHERE t.town_name != 'Other'
    ORDER BY town, year;
            

    -- ==========================================
    -- Aggregation 22: MH/SU Facility Count Summary by County and Type
    -- Counts of facilities by county x facility type for access analysis
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_mhsu_facility_summary;
    CREATE TABLE main.agg_mhsu_facility_summary AS
    SELECT
        county,
        state,
        facility_type_label,
        type_facility,
        COUNT(*) AS facility_count,
        is_mecklenburg
    FROM main.fact_mh_su_facilities
    GROUP BY county, state, facility_type_label, type_facility, is_mecklenburg
    ORDER BY is_mecklenburg DESC, county, facility_type_label;
 
    -- ==========================================
    -- Aggregation 23: MH/SU Facilities Detail
    -- Full facility list with location for map visualization
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_mhsu_facility_detail;
    CREATE TABLE main.agg_mhsu_facility_detail AS
    SELECT
        facility_name,
        street1,
        street2,
        city,
        state,
        zip,
        county,
        phone,
        website,
        latitude,
        longitude,
        type_facility,
        facility_type_label,
        is_mecklenburg
    FROM fact_mh_su_facilities
    ORDER BY is_mecklenburg DESC, county, facility_name;

    -- ==========================================
    -- Aggregation 24: CDC PLACES Health Outcomes by Town
    -- arthritis_ageadjprv, bphigh_ageadjprv, cancer_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_health_outcomes;
    CREATE TABLE main.agg_town_cdc_health_outcomes AS
    SELECT * FROM fact_health_outcomes_town ORDER BY town_name, year_key;

    -- ==========================================
    -- Aggregation 25: CDC PLACES Health Status by Town
    -- ghlth_ageadjprv, mhlth_ageadjprv, phlth_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_health_status;
    CREATE TABLE main.agg_town_cdc_health_status AS
    SELECT * FROM fact_health_status_town ORDER BY town_name, year_key;

    -- ==========================================
    -- Aggregation 26: CDC PLACES Prevention by Town
    -- checkup_ageadjprv, dental_ageadjprv, mammouse_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_prevention;
    CREATE TABLE main.agg_town_cdc_prevention AS
    SELECT * FROM fact_prevention_town ORDER BY town_name, year_key;

    -- ==========================================
    -- Aggregation 27: CDC PLACES Disability by Town
    -- disability_ageadjprv, cognition_ageadjprv, mobility_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_disability;
    CREATE TABLE main.agg_town_cdc_disability AS
    SELECT * FROM fact_disability_town ORDER BY town_name, year_key;

    -- ==========================================
    -- Aggregation 28: CDC PLACES Health Risk Behaviors by Town
    -- binge_ageadjprv, csmoking_ageadjprv, lpa_ageadjprv, sleep_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_risk_behaviors;
    CREATE TABLE main.agg_town_cdc_risk_behaviors AS
    SELECT * FROM fact_risk_behaviors_town ORDER BY town_name, year_key;

    -- ==========================================
    -- Aggregation 29: CDC PLACES Health-Related Social Needs by Town
    -- loneliness_ageadjprv, foodinsecu_ageadjprv, housinsecu_ageadjprv, etc.
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_town_cdc_social_needs;
    CREATE TABLE main.agg_town_cdc_social_needs AS
    SELECT * FROM fact_social_needs_town ORDER BY town_name, year_key;
            
    
    -- --------------------------------------------------------------
    -- Aggregation 30: Fair Market Rent by region / year / bedroom
    -- --------------------------------------------------------------
    DROP TABLE IF EXISTS main.agg_charlotte_fair_market_rent;
    CREATE TABLE main.agg_charlotte_fair_market_rent AS
    SELECT
        r.region_name,
        f.year_key AS year,
        b.bedroom_label,
        b.assumed_household_size,
        f.fmr
    FROM v_gold_fact_fair_market_rent AS f
    JOIN gold.dim_region   AS r ON f.region_key  = r.region_key
    JOIN gold.dim_bedrooms AS b ON f.bedroom_key = b.bedroom_key
    ORDER BY r.region_name, f.year_key, b.assumed_household_size;
 
    -- --------------------------------------------------------------
    -- Aggregation 31: AMI Affordability Gap
    -- fmr is deliberately NOT stored on gold.fact_ami_affordability_gap
    -- (it duplicated fact_fair_market_rent exactly) -- LEFT JOIN it back
    -- in here so the reporting table is still self-contained for BI use.
    -- LEFT JOIN (not INNER) so a future region/year/bedroom gap in
    -- fact_fair_market_rent shows up as NULL fmr instead of silently
    -- dropping the whole affordability-gap row.
    -- --------------------------------------------------------------
    DROP TABLE IF EXISTS main.agg_charlotte_ami_affordability_gap;
    CREATE TABLE main.agg_charlotte_ami_affordability_gap AS
    SELECT
        r.region_name,
        a.year_key AS year,
        b.bedroom_label,
        b.assumed_household_size,
        l.ami_level_label,
        l.ami_pct,
        a.annual_income,
        a.max_affordable_rent,
        fmr.fmr,
        a.monthly_gap,
        a.affordability_status
    FROM v_gold_fact_ami_affordability_gap AS a
    JOIN gold.dim_region    AS r ON a.region_key    = r.region_key
    JOIN gold.dim_bedrooms  AS b ON a.bedroom_key   = b.bedroom_key
    JOIN gold.dim_ami_level AS l ON a.ami_level_key = l.ami_level_key
    LEFT JOIN gold.fact_fair_market_rent AS fmr
        ON fmr.region_key  = a.region_key
       AND fmr.year_key    = a.year_key
       AND fmr.bedroom_key = a.bedroom_key
    ORDER BY r.region_name, b.assumed_household_size, l.ami_pct;
 
    -- --------------------------------------------------------------
    -- Aggregation 32: Occupation Housing Wage
    -- --------------------------------------------------------------
    DROP TABLE IF EXISTS main.agg_charlotte_occupation_housing_wage;
    CREATE TABLE main.agg_charlotte_occupation_housing_wage AS
    SELECT
        r.region_name,
        w.year_key AS year,
        o.occupation_name,
        o.category,
        w.hourly_wage,
        w.employment
    FROM v_gold_fact_occupation_housing_wage AS w
    JOIN gold.dim_region     AS r ON w.region_key     = r.region_key
    JOIN gold.dim_occupation AS o ON w.occupation_key = o.occupation_key
    ORDER BY r.region_name, o.category, o.occupation_name;

    -- ==========================================
    -- Post-processing Cleanup
    -- ==========================================
    DROP VIEW IF EXISTS main.dim_bg;
    DROP VIEW IF EXISTS main.dim_date;
    DROP VIEW IF EXISTS main.dim_town;
    DROP VIEW IF EXISTS main.dim_year;
    DROP VIEW IF EXISTS main.dim_county;
    DROP VIEW IF EXISTS main.fact_alice_town_household;
    DROP VIEW IF EXISTS main.fact_alice_county;
    DROP VIEW IF EXISTS main.fact_zillow_home_value;
    DROP VIEW IF EXISTS main.fact_zillow_rent;
    DROP VIEW IF EXISTS main.fact_household_income_in_the_past_12_months_town;
    DROP VIEW IF EXISTS main.fact_tenure_by_vehicles_available_town;
    DROP VIEW IF EXISTS main.fact_types_of_health_insurance_coverage_by_age_town;
    DROP VIEW IF EXISTS main.fact_health_insurance_coverage_status_and_type_by_employment_status_town;
    DROP VIEW IF EXISTS main.fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town;
    DROP VIEW IF EXISTS main.fact_median_household_income_in_the_past_12_months_town;
    DROP VIEW IF EXISTS main.fact_median_value_dollars_town;
    DROP VIEW IF EXISTS main.fact_median_gross_rent_dollars_town;
    DROP VIEW IF EXISTS main.fact_gini_index_of_income_inequality_town;
    DROP VIEW IF EXISTS main.fact_tenure_town;
    DROP VIEW IF EXISTS main.fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town;
    DROP VIEW IF EXISTS main.fact_sex_by_age_by_disability_status_town;
    DROP VIEW IF EXISTS main.fact_race_town;
    DROP VIEW IF EXISTS main.fact_hispanic_or_latino_origin_town;
    DROP VIEW IF EXISTS main.fact_place_of_birth_by_nativity_and_citizenship_status_town;
    DROP VIEW IF EXISTS main.fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over_town;
    DROP VIEW IF EXISTS main.fact_educational_attainment_for_the_population_25_years_and_over_town;
    DROP VIEW IF EXISTS main.fact_types_of_computers_in_household_town;
    DROP VIEW IF EXISTS main.fact_presence_and_types_of_internet_subscriptions_in_household_town;
    DROP VIEW IF EXISTS main.fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town;
    DROP VIEW IF EXISTS main.fact_poverty_status_in_the_past_12_months_by_sex_by_age_town;
    DROP VIEW IF EXISTS main.fact_employment_status_for_the_population_16_years_and_over_town;
    DROP VIEW IF EXISTS main.fact_health_insurance_coverage_status_by_sex_by_age_town;
    DROP VIEW IF EXISTS main.fact_educational_attainment_town;
    DROP VIEW IF EXISTS main.fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town;
    DROP VIEW IF EXISTS main.fact_vacancy_status_town;
    DROP VIEW IF EXISTS main.fact_housing_units_town;
    DROP VIEW IF EXISTS main.fact_tenure_by_year_householder_moved_into_unit_town;
    DROP VIEW IF EXISTS main.fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_town;
    DROP VIEW IF EXISTS main.fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_town;
    DROP VIEW IF EXISTS main.fact_means_of_transportation_to_work_town;
    DROP VIEW IF EXISTS main.fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_town;
    DROP VIEW IF EXISTS main.fact_household_size_by_vehicles_available_town;
    DROP VIEW IF EXISTS main.fact_total_population_town;
    DROP VIEW IF EXISTS main.fact_health_outcomes_town;
    DROP VIEW IF EXISTS main.fact_health_status_town;
    DROP VIEW IF EXISTS main.fact_prevention_town;
    DROP VIEW IF EXISTS main.fact_disability_town;
    DROP VIEW IF EXISTS main.fact_risk_behaviors_town;
    DROP VIEW IF EXISTS main.fact_social_needs_town;
    DROP VIEW IF EXISTS main.fact_mh_su_facilities;
    DROP VIEW IF EXISTS v_gold_fact_fair_market_rent;
    DROP VIEW IF EXISTS v_gold_fact_ami_affordability_gap;
    DROP VIEW IF EXISTS v_gold_fact_occupation_housing_wage;
 

   
    FORCE CHECKPOINT;
""")

con.close()
print("🎉 Complete: Aggregate Pipeline Successfully Executed and Database Safely Disconnected!")