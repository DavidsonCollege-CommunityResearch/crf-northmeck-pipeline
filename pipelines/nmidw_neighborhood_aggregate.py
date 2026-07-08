# ==============================================================================
# NEIGHBORHOOD AGGREGATE LAYER PIPELINE
# Author: Paul Park, Gemini Code
# Objective: Create pre-calculated aggregate tables for Focus Neighborhoods
#            using ACS block group level data as an approximation.
#
# Focus Neighborhoods:
#   - Huntington Green  (block group: 371190062241)
#   - Pottstown         (block groups: 371190063071, 371190063072)
#   - West Davidson     (block group: 371190064031)
#   - Smithville        (block group: 371190064111)
#
# IMPORTANT LIMITATION:
#   ACS data is only available at the block group level, which is the smallest
#   geographic unit published. These block groups approximate but do not
#   perfectly align with neighborhood boundaries — each block group may include
#   residents outside the neighborhood boundary.
#   Pottstown spans 2 block groups and therefore carries greater boundary
#   imprecision than the other three neighborhoods.
#   All tables include a 'disclaimer' column to surface this limitation
#   in downstream dashboards and reports.
# ==============================================================================
 
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions.mother_duck_connector import get_md_connection
 
con = get_md_connection()
 
print("\nStarting Neighborhood Aggregate Layer...")
 
con.execute("""
    -- ============================================================
    -- Expose Gold _bg fact tables as views for easier querying
    -- ============================================================
    -- Note: B18101 (Sex by Age by Disability Status) is NOT available at block group level
    -- Census only publishes disability data at census tract level and above
    -- B18101_001/002/021 (total/male/female) are also null at block group level
    -- Using fact_total_population_bg (B01003) for total population instead
    CREATE OR REPLACE VIEW fact_total_population_bg AS SELECT * FROM gold.fact_total_population_bg;
    CREATE OR REPLACE VIEW fact_race_bg AS SELECT * FROM gold.fact_race_bg;
    CREATE OR REPLACE VIEW fact_hispanic_or_latino_origin_bg AS SELECT * FROM gold.fact_hispanic_or_latino_origin_bg;
    CREATE OR REPLACE VIEW fact_place_of_birth_by_nativity_and_citizenship_status_bg AS SELECT * FROM gold.fact_place_of_birth_by_nativity_and_citizenship_status_bg;
    CREATE OR REPLACE VIEW fact_household_income_in_the_past_12_months_bg AS SELECT * FROM gold.fact_household_income_in_the_past_12_months_bg;
    CREATE OR REPLACE VIEW fact_median_household_income_in_the_past_12_months_bg AS SELECT * FROM gold.fact_median_household_income_in_the_past_12_months_bg;
    CREATE OR REPLACE VIEW fact_gini_index_of_income_inequality_bg AS SELECT * FROM gold.fact_gini_index_of_income_inequality_bg;
    CREATE OR REPLACE VIEW fact_poverty_status_in_the_past_12_months_by_sex_by_age_bg AS SELECT * FROM gold.fact_poverty_status_in_the_past_12_months_by_sex_by_age_bg;
    CREATE OR REPLACE VIEW fact_housing_units_bg AS SELECT * FROM gold.fact_housing_units_bg;
    CREATE OR REPLACE VIEW fact_tenure_bg AS SELECT * FROM gold.fact_tenure_bg;
    CREATE OR REPLACE VIEW fact_median_gross_rent_dollars_bg AS SELECT * FROM gold.fact_median_gross_rent_dollars_bg;
    CREATE OR REPLACE VIEW fact_median_value_dollars_bg AS SELECT * FROM gold.fact_median_value_dollars_bg;
    CREATE OR REPLACE VIEW fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_bg AS SELECT * FROM gold.fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_bg;
    CREATE OR REPLACE VIEW fact_tenure_by_vehicles_available_bg AS SELECT * FROM gold.fact_tenure_by_vehicles_available_bg;
    CREATE OR REPLACE VIEW fact_health_insurance_coverage_status_and_type_by_employment_status_bg AS SELECT * FROM gold.fact_health_insurance_coverage_status_and_type_by_employment_status_bg;
    CREATE OR REPLACE VIEW fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_bg AS SELECT * FROM gold.fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_bg;
    CREATE OR REPLACE VIEW fact_educational_attainment_for_the_population_25_years_and_over_bg AS SELECT * FROM gold.fact_educational_attainment_for_the_population_25_years_and_over_bg;
    CREATE OR REPLACE VIEW fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_bg AS SELECT * FROM gold.fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_bg;
    CREATE OR REPLACE VIEW fact_means_of_transportation_to_work_bg AS SELECT * FROM gold.fact_means_of_transportation_to_work_bg;
    CREATE OR REPLACE VIEW fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_bg AS SELECT * FROM gold.fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_bg;
    CREATE OR REPLACE VIEW fact_household_size_by_vehicles_available_bg AS SELECT * FROM gold.fact_household_size_by_vehicles_available_bg;
    CREATE OR REPLACE VIEW fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_bg AS SELECT * FROM gold.fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_bg;
    CREATE OR REPLACE VIEW fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_bg AS SELECT * FROM gold.fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_bg;
 
    -- ==========================================
    -- Aggregation 1: Neighborhood Demographics
    -- Note: total population from fact_total_population_bg (B01003)
    --       Sex breakdown not available at block group level
    --       B18101 disability data not available at block group level
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_demographics;
    CREATE TABLE main.agg_neighborhood_demographics AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name,
               'Note: block group approximation of neighborhood boundary' AS disclaimer
        FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown',
               'Note: neighborhood spans 2 block groups — boundary imprecision is higher'
        FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson',
               'Note: block group approximation of neighborhood boundary'
        FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville',
               'Note: block group approximation of neighborhood boundary'
        FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      nm.disclaimer,
      tp.block_group_GEOID AS GEOID,
      tp.year_key AS year,
      tp.total AS total_population,
      rc.total_white_alone AS race_white_alone,
      rc.total_black_or_african_american_alone AS race_black_alone,
      rc.total_asian_alone AS race_asian_alone,
      hl.total_hispanic_or_latino AS ethnicity_hispanic_or_latino,
      ROUND((hl.total_hispanic_or_latino / NULLIF(tp.total, 0)) * 100, 2) AS hispanic_or_latino_rate,
      pb.total_foreign_born AS foreign_born_population,
      ROUND((pb.total_foreign_born / NULLIF(tp.total, 0)) * 100, 2) AS foreign_born_rate
    FROM fact_total_population_bg AS tp
    JOIN neighborhood_map AS nm ON tp.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_race_bg AS rc
      ON tp.block_group_GEOID = rc.block_group_GEOID AND tp.year_key = rc.year_key
    JOIN fact_hispanic_or_latino_origin_bg AS hl
      ON tp.block_group_GEOID = hl.block_group_GEOID AND tp.year_key = hl.year_key
    JOIN fact_place_of_birth_by_nativity_and_citizenship_status_bg AS pb
      ON tp.block_group_GEOID = pb.block_group_GEOID AND tp.year_key = pb.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
    -- ==========================================
    -- Aggregation 2: Neighborhood Economic Profile
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_economic_profile;
    CREATE TABLE main.agg_neighborhood_economic_profile AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown' FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson' FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville' FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      hi.block_group_GEOID AS GEOID,
      hi.year_key AS year,
      hi.total AS total_households,
      (hi.total_less_than_10_000 + hi.total_10_000_to_14_999 + hi.total_15_000_to_19_999 + hi.total_20_000_to_24_999) AS income_under_25k,
      (hi.total_25_000_to_29_999 + hi.total_30_000_to_34_999 + hi.total_35_000_to_39_999 + hi.total_40_000_to_44_999 + hi.total_45_000_to_49_999) AS income_25k_50k,
      (hi.total_50_000_to_59_999 + hi.total_60_000_to_74_999 + hi.total_75_000_to_99_999) AS income_50k_100k,
      (hi.total_100_000_to_124_999 + hi.total_125_000_to_149_999 + hi.total_150_000_to_199_999 + hi.total_200_000_or_more) AS income_100k_plus,
      mi.median_household_income_in_the_past_12_months_in_2024_inflation_adjusted_dollars AS median_household_income,
      gi.gini_index AS income_inequality_gini,
      pv.total_income_in_the_past_12_months_below_poverty_level AS below_poverty_level,
      ROUND((pv.total_income_in_the_past_12_months_below_poverty_level / NULLIF(pv.total, 0)) * 100, 2) AS poverty_rate_pct
    FROM fact_household_income_in_the_past_12_months_bg AS hi
    JOIN neighborhood_map AS nm ON hi.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_median_household_income_in_the_past_12_months_bg AS mi
      ON hi.block_group_GEOID = mi.block_group_GEOID AND hi.year_key = mi.year_key
    JOIN fact_gini_index_of_income_inequality_bg AS gi
      ON hi.block_group_GEOID = gi.block_group_GEOID AND hi.year_key = gi.year_key
    JOIN fact_poverty_status_in_the_past_12_months_by_sex_by_age_bg AS pv
      ON hi.block_group_GEOID = pv.block_group_GEOID AND hi.year_key = pv.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
    -- ==========================================
    -- Aggregation 3: Neighborhood Housing Profile
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_housing;
    CREATE TABLE main.agg_neighborhood_housing AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown' FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson' FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville' FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      hu.block_group_GEOID AS GEOID,
      hu.year_key AS year,
      hu.total AS total_housing_units,
      tn.total_owner_occupied,
      tn.total_renter_occupied,
      ROUND((tn.total_owner_occupied / NULLIF(hu.total, 0)) * 100, 2) AS owner_occupied_rate_pct,
      ROUND((tn.total_renter_occupied / NULLIF(hu.total, 0)) * 100, 2) AS renter_occupied_rate_pct,
      mr.median_gross_rent AS median_gross_rent,
      mv.median_value_dollars AS median_home_value,
      (rb.total_30_0_to_34_9_percent + rb.total_35_0_to_39_9_percent + rb.total_40_0_to_49_9_percent) AS cost_burdened_households,
      rb.total_50_0_percent_or_more AS severely_cost_burdened_households,
      ROUND(((rb.total_30_0_to_34_9_percent + rb.total_35_0_to_39_9_percent + rb.total_40_0_to_49_9_percent + rb.total_50_0_percent_or_more) / NULLIF(tn.total_renter_occupied, 0)) * 100, 2) AS housing_burden_rate_pct,
      tv.total_owner_occupied_no_vehicle_available AS owner_no_vehicle,
      tv.total_renter_occupied_no_vehicle_available AS renter_no_vehicle
    FROM fact_housing_units_bg AS hu
    JOIN neighborhood_map AS nm ON hu.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_tenure_bg AS tn
      ON hu.block_group_GEOID = tn.block_group_GEOID AND hu.year_key = tn.year_key
    JOIN fact_median_gross_rent_dollars_bg AS mr
      ON hu.block_group_GEOID = mr.block_group_GEOID AND hu.year_key = mr.year_key
    JOIN fact_median_value_dollars_bg AS mv
      ON hu.block_group_GEOID = mv.block_group_GEOID AND hu.year_key = mv.year_key
    JOIN fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_bg AS rb
      ON hu.block_group_GEOID = rb.block_group_GEOID AND hu.year_key = rb.year_key
    JOIN fact_tenure_by_vehicles_available_bg AS tv
      ON hu.block_group_GEOID = tv.block_group_GEOID AND hu.year_key = tv.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
   
    -- ==========================================
    -- Aggregation 5: Neighborhood Education
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_education;
    CREATE TABLE main.agg_neighborhood_education AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown' FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson' FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville' FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      ed.block_group_GEOID AS GEOID,
      ed.year_key AS year,
      ed.total AS total_pop_25_plus,
      (ed.total_no_schooling_completed + ed.total_nursery_school + ed.total_kindergarten +
       ed.total_1st_grade + ed.total_2nd_grade + ed.total_3rd_grade + ed.total_4th_grade +
       ed.total_5th_grade + ed.total_6th_grade + ed.total_7th_grade + ed.total_8th_grade +
       ed.total_9th_grade + ed.total_10th_grade + ed.total_11th_grade +
       ed.total_12th_grade_no_diploma) AS n_less_than_hs,
      (ed.total_regular_high_school_diploma + ed.total_ged_or_alternative_credential) AS n_hs_or_equiv,
      (ed.total_some_college_1_or_more_years_no_degree + ed.total_some_college_less_than_1_year) AS n_some_college,
      ed.total_associate_s_degree AS n_associates,
      ed.total_bachelor_s_degree AS n_bachelors,
      (ed.total_master_s_degree + ed.total_professional_school_degree + ed.total_doctorate_degree) AS n_graduate_or_prof,
      ROUND(((ed.total_no_schooling_completed + ed.total_nursery_school + ed.total_kindergarten +
              ed.total_1st_grade + ed.total_2nd_grade + ed.total_3rd_grade + ed.total_4th_grade +
              ed.total_5th_grade + ed.total_6th_grade + ed.total_7th_grade + ed.total_8th_grade +
              ed.total_9th_grade + ed.total_10th_grade + ed.total_11th_grade +
              ed.total_12th_grade_no_diploma) / NULLIF(ed.total, 0)) * 100, 2) AS pct_less_than_hs,
      ROUND(((ed.total_regular_high_school_diploma + ed.total_ged_or_alternative_credential) / NULLIF(ed.total, 0)) * 100, 2) AS pct_hs_or_equiv,
      ROUND((ed.total_bachelor_s_degree / NULLIF(ed.total, 0)) * 100, 2) AS pct_bachelors,
      ROUND(((ed.total_bachelor_s_degree + ed.total_master_s_degree + ed.total_professional_school_degree + ed.total_doctorate_degree) / NULLIF(ed.total, 0)) * 100, 2) AS pct_bachelors_or_higher,
      se.total_enrolled_in_school AS n_enrolled_total,
      (se.total_enrolled_in_school_enrolled_in_kindergarten +
       se.total_enrolled_in_school_enrolled_in_grade_1_to_grade_4 +
       se.total_enrolled_in_school_enrolled_in_grade_5_to_grade_8 +
       se.total_enrolled_in_school_enrolled_in_grade_9_to_grade_12) AS n_enrolled_k12
    FROM fact_educational_attainment_for_the_population_25_years_and_over_bg AS ed
    JOIN neighborhood_map AS nm ON ed.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_bg AS se
      ON ed.block_group_GEOID = se.block_group_GEOID AND ed.year_key = se.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
    -- ==========================================
    -- Aggregation 6: Neighborhood Transportation
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_transportation;
    CREATE TABLE main.agg_neighborhood_transportation AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown' FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson' FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville' FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      mt.block_group_GEOID AS GEOID,
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
    FROM fact_means_of_transportation_to_work_bg AS mt
    JOIN neighborhood_map AS nm ON mt.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_bg AS tt
      ON mt.block_group_GEOID = tt.block_group_GEOID AND mt.year_key = tt.year_key
    JOIN fact_household_size_by_vehicles_available_bg AS hv
      ON mt.block_group_GEOID = hv.block_group_GEOID AND mt.year_key = hv.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
    -- ==========================================
    -- Aggregation 7: Neighborhood Childcare & Family Structure
    -- ==========================================
    DROP TABLE IF EXISTS main.agg_neighborhood_childcare;
    CREATE TABLE main.agg_neighborhood_childcare AS
    WITH neighborhood_map AS (
        SELECT block_group_GEOID, 'Huntington Green' AS neighborhood_name FROM (VALUES ('371190062241')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Pottstown' FROM (VALUES ('371190063071'), ('371190063072')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'West Davidson' FROM (VALUES ('371190064031')) t(block_group_GEOID)
        UNION ALL
        SELECT block_group_GEOID, 'Smithville' FROM (VALUES ('371190064111')) t(block_group_GEOID)
    )
    SELECT
      nm.neighborhood_name,
      ch.block_group_GEOID AS GEOID,
      ch.year_key AS year,
      ch.total AS total_children_under_18,
      ch.total_under_6_years AS children_under6,
      ch.total_6_to_17_years AS children_6_17,
      ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force AS under6_both_parents_working,
      ch.total_under_6_years_living_with_two_parents_neither_parent_in_labor_force AS under6_neither_parent_working,
      (ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force) AS under6_single_parent_working,
      (ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force) AS under6_likely_needs_childcare,
      ROUND((
        (ch.total_under_6_years_living_with_two_parents_both_parents_in_labor_force +
         ch.total_under_6_years_living_with_one_parent_living_with_father_in_labor_force +
         ch.total_under_6_years_living_with_one_parent_living_with_mother_in_labor_force)
        / NULLIF(ch.total_under_6_years, 0)
      ) * 100, 2) AS pct_under6_likely_needs_childcare,
      (ch.total_6_to_17_years_living_with_two_parents_both_parents_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_father_in_labor_force +
       ch.total_6_to_17_years_living_with_one_parent_living_with_mother_in_labor_force) AS age6_17_likely_needs_afterschool,
      ROUND((
        (ch.total_6_to_17_years_living_with_two_parents_both_parents_in_labor_force +
         ch.total_6_to_17_years_living_with_one_parent_living_with_father_in_labor_force +
         ch.total_6_to_17_years_living_with_one_parent_living_with_mother_in_labor_force)
        / NULLIF(ch.total_6_to_17_years, 0)
      ) * 100, 2) AS pct_6_17_likely_needs_afterschool,
      gp.total_grandparent_householder_responsible_for_own_grandchildren_under_18_years AS grandparent_caregiver_households
    FROM fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_bg AS ch
    JOIN neighborhood_map AS nm ON ch.block_group_GEOID = nm.block_group_GEOID
    JOIN fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_bg AS gp
      ON ch.block_group_GEOID = gp.block_group_GEOID AND ch.year_key = gp.year_key
    ORDER BY neighborhood_name, GEOID, year;
 
    -- ==========================================
    -- Post-processing Cleanup
    -- ==========================================
    DROP VIEW IF EXISTS main.fact_total_population_bg;
    DROP VIEW IF EXISTS main.fact_race_bg;
    DROP VIEW IF EXISTS main.fact_hispanic_or_latino_origin_bg;
    DROP VIEW IF EXISTS main.fact_place_of_birth_by_nativity_and_citizenship_status_bg;
    DROP VIEW IF EXISTS main.fact_household_income_in_the_past_12_months_bg;
    DROP VIEW IF EXISTS main.fact_median_household_income_in_the_past_12_months_bg;
    DROP VIEW IF EXISTS main.fact_gini_index_of_income_inequality_bg;
    DROP VIEW IF EXISTS main.fact_poverty_status_in_the_past_12_months_by_sex_by_age_bg;
    DROP VIEW IF EXISTS main.fact_housing_units_bg;
    DROP VIEW IF EXISTS main.fact_tenure_bg;
    DROP VIEW IF EXISTS main.fact_median_gross_rent_dollars_bg;
    DROP VIEW IF EXISTS main.fact_median_value_dollars_bg;
    DROP VIEW IF EXISTS main.fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_bg;
    DROP VIEW IF EXISTS main.fact_tenure_by_vehicles_available_bg;
    DROP VIEW IF EXISTS main.fact_health_insurance_coverage_status_and_type_by_employment_status_bg;
    DROP VIEW IF EXISTS main.fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_bg;
    DROP VIEW IF EXISTS main.fact_educational_attainment_for_the_population_25_years_and_over_bg;
    DROP VIEW IF EXISTS main.fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_bg;
    DROP VIEW IF EXISTS main.fact_means_of_transportation_to_work_bg;
    DROP VIEW IF EXISTS main.fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work_bg;
    DROP VIEW IF EXISTS main.fact_household_size_by_vehicles_available_bg;
    DROP VIEW IF EXISTS main.fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents_bg;
    DROP VIEW IF EXISTS main.fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent_bg;
 
    FORCE CHECKPOINT;
""")
 
con.close()
print("🎉 Complete: Neighborhood Aggregate Pipeline Successfully Executed!")