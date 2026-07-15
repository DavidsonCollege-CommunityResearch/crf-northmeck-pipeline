# Data Dictionary — Gold & Main Layers

Detailed, table-by-table reference for everything `pipelines/nmidw_gold.py`, `pipelines/nmidw_aggregate.py`, and `pipelines/nmidw_neighborhood_aggregate.py` build. See the [README](../README.md) for the high-level architecture; this document exists so an analyst can find "which table has X" without reading the pipeline source.

Unless noted otherwise: **town-grain** tables cover Davidson, Cornelius, and Huntersville only (`dim_town.town_name != 'Other'` is filtered out everywhere in Main); **year** is the ACS 5-year vintage (2018–2024) unless the table is one-time reference data (Charlotte housing, most school data — both fixed at a single reporting year).

---

## 1. Gold Layer (`gold` schema)

### 1.1 Dimensions

| Table | Grain | Description |
|---|---|---|
| `dim_date` | 1 row/day, 1990-01-01 to 2050-12-31 | Standard date spine (year/quarter/month/day/weekday flag). Backs `fact_zillow_home_value`/`fact_zillow_rent`, which are date-grain rather than year-grain. |
| `dim_year` | 1 row/calendar year | Distinct years from `dim_date`; the year dimension used by every ACS/ALICE/CDC fact. |
| `dim_bg` | 1 row/block group | `block_group_GEOID` (PK), block group/tract/county names (regex-parsed from the ACS `NAME` field), and `geometry` (GeoJSON polygon from TIGER 2023). `geometry` is `NULL` for ~200 block groups retired between the 2018/2019 ACS vintage and the 2020 redistricting — expected, not a load failure. |
| `dim_block` | 1 row/2020 Census block | `block_GEOID`, `block_name`, `geometry`. Not referenced by any fact table (no FK from facts) — it exists solely to supply geometry for the block-level neighborhood union in `nmidw_aggregate.py`. |
| `dim_town` | 1 row/place GEOID | Maps every ACS place GEOID to `'Davidson'`, `'Cornelius'`, `'Huntersville'`, or `'Other'`. `'Other'` rows exist because the place-level ACS pull returns all NC places matching the town-name regex filter in Silver, not just the three towns — Main-layer queries filter them out. |
| `dim_county` | 1 row | Single reference row: Mecklenburg County, GEOID `37119`. |
| `dim_region` | 1 row/Charlotte-metro region | Built from `silver.charlotte_rent_by_bedroom`. In practice there is currently one region, `Charlotte-Concord-Gastonia`. |
| `dim_bedrooms` | 1 row/bedroom count | `bedroom_label` (e.g. `"0"`, `"1"`, …) + `assumed_household_size`, from the AMI affordability CSV. |
| `dim_ami_level` | 1 row/AMI threshold | `ami_level_label` (e.g. `"30% AMI"`) + `ami_pct` (parsed integer, e.g. `30`). |
| `dim_occupation` | 1 row/occupation | `occupation_name` + `category`, from the Charlotte housing-wage CSV (9 occupations). |
| `dim_school` | 1 row/school (23 total) | `school_code` (PK — chosen over name to avoid name collisions across districts), `school_name`, `district_name`, `grade_span`, `is_title_1`, and `town_name`/`place_GEOID` (hand-mapped from NCES CCD mailing address per school — a *mailing-address* town, not a verified point-in-polygon municipal boundary match; see caveat in source comments). |
| `dim_subgroup` | 1 row/reporting subgroup (20) | Subgroup code (e.g. `EDS`, `SWD`, `BLCK`) → label. Derived from the data actually present plus 2 hardcoded labels (`NAIG`, `NELS`) not in the source documentation tab. |
| `dim_subject_code` | 1 row/subject code | NC DPI subject codes (`RDGS`, `MAGS`, `ACT`, `WK`, `CGRS`, …) → description, sourced from the workbook's own format tab plus one hand-inferred code (`MA37`, math grades 3–7 combined — verified against 2 schools' denominators). |
| `dim_grade_scope` | 1 row/grade scope (e.g. `"3-8"`, `"9-12"`, `"All"`) | `grade_min`/`grade_max` parsed via regex from the scope label, `is_eoc` flag. |
| `dim_act_measure` | 1 row/ACT subtest or composite | `benchmark_score` hardcoded from the source workbook's ACT documentation (composite 17, English 18, Math/Reading 22, Science 23). |

### 1.2 Concept-driven ACS facts (dynamically generated)

The core of the star schema is **not hand-written** — `nmidw_gold.py` reads every distinct `table_name` out of `silver.variable_crosswalk` (at the latest ACS vintage year) and, for each concept, `PIVOT`s `silver.acs_bg`/`silver.acs_place` into a wide table twice: once at block-group grain, once at town grain.

```
gold.{table_name}_bg     PK (block_group_GEOID, year_key)   FK → dim_bg, dim_year
gold.{table_name}_town   PK (place_GEOID, year_key)         FK → dim_town, dim_year
```

Every numeric ACS variable becomes two columns: the estimate (named from the cleaned ACS label, e.g. `total_bachelor_s_degree`) and its margin of error (same name + `_moe`).

Not every one of these is necessarily consumed downstream (any ACS concept in `acs_variables_raw` gets a fact table pair automatically), but the following are the concepts actually joined into Main-layer tables today — i.e. the "core" fact set in practice:

| Concept table stem | Source ACS table | What it holds |
|---|---|---|
| `fact_total_population` | B01003 | Total population |
| `fact_race` | B02001 | Population by race (White/Black/Asian alone, etc.) |
| `fact_hispanic_or_latino_origin` | B03003 | Hispanic/Latino origin (ethnicity axis, independent of race) |
| `fact_place_of_birth_by_nativity_and_citizenship_status` | B05002 | Foreign-born / nativity status |
| `fact_sex_by_age_by_veteran_status_for_the_civilian_population_18_years_and_over` | B21001 | Veteran population, civilian 18+ |
| `fact_sex_by_age_by_disability_status` | B18101 | Population with/without a disability, by sex × 6 age bands. **Not published at block-group level** — town-grain only. |
| `fact_educational_attainment_for_the_population_25_years_and_over` | B15003 | Full educational-attainment ladder (no schooling → doctorate), pop 25+ |
| `fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over` | B14001 | School enrollment by level (preschool through grad school) |
| `fact_educational_attainment` | S1501 (subject table) | Attainment by age group, race/ethnicity, median earnings by degree, poverty rate by degree. **Town-level only** — Census does not publish subject tables at block group. |
| `fact_median_household_income_in_the_past_12_months` | B19013 | Median household income |
| `fact_household_income_in_the_past_12_months` | B19001 | Household counts by income bracket |
| `fact_gini_index_of_income_inequality` | B19083 | Gini coefficient |
| `fact_poverty_status_in_the_past_12_months_by_sex_by_age` | B17001 | Population below/above poverty line |
| `fact_employment_status_for_the_population_16_years_and_over` | B23025 | Labor force / employed / unemployed counts |
| `fact_housing_units` | B25001 | Total housing units |
| `fact_vacancy_status` | B25004 | Vacant units |
| `fact_tenure` | B25003 | Owner- vs. renter-occupied units |
| `fact_tenure_by_vehicles_available` | B25044 | Owner/renter households with zero vehicles |
| `fact_tenure_by_year_householder_moved_into_unit` | B25038 | Owner/renter households by year moved in (recency bands) — displacement/turnover signal |
| `fact_median_value_dollars` | B25077 | Median home value |
| `fact_median_gross_rent_dollars` | B25064 | Median gross rent |
| `fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months` | B25070 | Renter households by rent-burden bracket (e.g. 30–34.9%, 50%+) |
| `fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars` | B25071 | Median rent-to-income ratio |
| `fact_types_of_health_insurance_coverage_by_age` | B27010 | Insurance coverage *type* (employer, direct purchase, Medicare, Medicaid, TRICARE, VA) by age band |
| `fact_health_insurance_coverage_status_by_sex_by_age` | B27001 | Insured/uninsured by sex × age band |
| `fact_health_insurance_coverage_status_and_type_by_employment_status` | B27011 | Insurance status by labor-force/employment status |
| `fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months` | B27015 | Insurance status by household income bracket |
| `fact_types_of_computers_in_household` | B28001 | Computer ownership |
| `fact_presence_and_types_of_internet_subscriptions_in_household` | B28002 | Internet access/subscription type |
| `fact_means_of_transportation_to_work` | B08301 | Commute mode share (drove alone, transit, walked, bicycle, WFH) |
| `fact_aggregate_travel_time_to_work_in_minutes_of_workers_by_means_of_transportation_to_work` | B08136 | Aggregate commute minutes (÷ workers = average commute time) |
| `fact_household_size_by_vehicles_available` | B08201 | Households with zero vehicles available |
| `fact_age_of_own_children_under_18_years_in_families_and_subfamilies_by_living_arrangements_by_employment_status_of_parents` | B23008 | Children under 18 by living arrangement × parental labor-force status — childcare-need proxy |
| `fact_grandchildren_under_18_years_living_with_a_grandparent_householder_by_grandparent_responsibility_and_presence_of_parent` | B10002 | Grandparent caregiver households |

### 1.3 Fixed (hand-written) fact tables

**Zillow:**

| Table | Grain | Columns | Notes |
|---|---|---|---|
| `fact_zillow_home_value` | place × date × housing_type | `home_value` | ZHVI, joined to `dim_town` by matching `town_name` text (not GEOID) |
| `fact_zillow_rent` | place × date | `rent_value` | ZORI |

**ALICE (United Way financial hardship study):**

| Table | Grain | Columns |
|---|---|---|
| `fact_alice_town_household` | place × year | `total_households`, `poverty_households`, `alice_households`, `above_alice_households` |
| `fact_alice_county` | county × year | Same 4 measures, county-wide |

**MH/SU facilities:**

| Table | Grain | Notes |
|---|---|---|
| `fact_mh_su_facilities` | 1 row/facility | Name, address, phone, website, lat/long, `facility_type_label` (Mental Health / Substance Use / Opioid Treatment Program / HRSA Health Center), `is_mecklenburg` flag. **No FK constraints** — facilities span counties outside `dim_town`/`dim_bg`. |

**Charlotte regional housing affordability** (all fixed at year 2025 — HUD FY2025 / NLIHC "Out of Reach 2025" basis):

| Table | Grain | Columns |
|---|---|---|
| `fact_fair_market_rent` | region × year × bedroom | `fmr` (fair market rent) |
| `fact_ami_affordability_gap` | region × year × bedroom × AMI level | `annual_income`, `max_affordable_rent`, `monthly_gap`, `affordability_status` (Affordable/Unaffordable) |
| `fact_occupation_housing_wage` | region × year × occupation | `hourly_wage`, `employment` |

**CDC PLACES** (BRFSS-derived town health measures):

| Table | Grain | Notes |
|---|---|---|
| `cdc_places` | raw pass-through of `silver.cdc_places` | Long format — kept for ad-hoc querying |
| `fact_health_outcomes_town` | place × year | Pivoted from category `HLTHOUT` — e.g. arthritis, high blood pressure, cancer prevalence |
| `fact_health_status_town` | place × year | Category `HLTHSTAT` — general/mental/physical health status |
| `fact_prevention_town` | place × year | Category `PREVENT` — checkups, dental visits, cancer screenings |
| `fact_disability_town` | place × year | Category `DISABLT` — disability, cognition, mobility difficulty |
| `fact_risk_behaviors_town` | place × year | Category `RISKBEH` — binge drinking, smoking, physical inactivity, short sleep |
| `fact_social_needs_town` | place × year | Category `SOCLNEED` — loneliness, food insecurity, housing insecurity |

All 6 category tables are built the same way: pivot `silver.cdc_places` filtered to that `category_id`, one column per `measure_id + data_value_type_id` combination (e.g. `arthritis_ageadjprv`).

**NC DPI school assessment** (all fixed at year 2025 except `fact_school_assessment_master`, which carries its own `reporting_year`):

| Table | Grain | Columns |
|---|---|---|
| `fact_school_test_results` | school × subgroup × grade_scope | `glp_pct`/`glp_raw` (% Grade Level Proficient, incl. suppressed `">95"`-style text), `ccr_pct`/`ccr_raw` (% College & Career Ready) |
| `fact_school_growth` | school × subgroup × growth_type | `growth_status`, `growth_index_score` (growth_type ∈ Overall/Reading/Mathematics) |
| `fact_school_hs_indicators` | school × subgroup | ACT WorkKeys indicator, NC Math 3 pass rate, 4-yr and 5-yr graduation rates |
| `fact_school_assessment_master` | school × subgroup × subject_code | Denominator + full proficiency-level breakdown (not proficient, level 3/4/5, GLP, CCR) — the most granular subject-level source table, includes `is_title_1` |
| `fact_school_eog_eoc` | school × subgroup × subject_area × grade_scope | Same proficiency breakdown, split by subject area (Reading/Math/Science/English) and EOG vs. EOC |
| `fact_school_act` | school × subgroup × act_measure | `pct_meeting_benchmark` per ACT subtest/composite |
| `fact_school_workkeys` | school × subgroup | `pct_silver_or_higher` |
| `fact_school_english_learner` | school × subgroup | EL progress %, % exiting EL status, % meeting annual progress |

All 8 school fact tables share the same `subgroup` text → `subgroup_code` `CASE` mapping (21 distinct raw labels → codes like `ALL`, `EDS`, `SWD`, `BLCK`, `FEM`, …), repeated per table rather than centralized.

---

## 2. Main Layer — Town/County/Region Aggregates (`main` schema, from `nmidw_aggregate.py`)

All tables below are grain **town × year** unless noted, and exclude `town_name = 'Other'`.

| # | Table | Grain | What it computes | Key sources |
|---|---|---|---|---|
| 1 | `agg_town_health_data` | town × year | Income brackets collapsed to 4 tiers, insurance coverage by employment status and by income bracket | `fact_household_income_in_the_past_12_months_town`, `fact_tenure_by_vehicles_available_town`, `fact_types_of_health_insurance_coverage_by_age_town`, `fact_health_insurance_coverage_status_and_type_by_employment_status_town`, `fact_health_insurance_coverage_status_and_type_by_household_income_in_the_past_12_months_town` |
| 2 | `agg_town_health_insurance` | town × year | Insured/uninsured counts by 5 age bands, plus coverage broken out by type (employer, direct purchase, Medicare, Medicaid, TRICARE, VA, other) | `fact_health_insurance_coverage_status_by_sex_by_age_town`, `fact_types_of_health_insurance_coverage_by_age_town` |
| 3 | `agg_town_economic_trends` | town × year | Median household income, median home value, median rent, Gini index — pure time-series indicators | `fact_median_household_income_in_the_past_12_months_town`, `fact_median_value_dollars_town`, `fact_median_gross_rent_dollars_town`, `fact_gini_index_of_income_inequality_town` |
| 4 | `economic_mobility_education` | town × year | Bachelor's+/master's rate, median income, Gini, poverty rate, labor-force unemployment rate — education vs. economic outcome | `fact_educational_attainment_for_the_population_25_years_and_over_town`, `fact_median_household_income_in_the_past_12_months_town`, `fact_gini_index_of_income_inequality_town`, `fact_poverty_status_in_the_past_12_months_by_sex_by_age_town`, `fact_employment_status_for_the_population_16_years_and_over_town`, `fact_health_insurance_coverage_status_and_type_by_employment_status_town` |
| 5 | `agg_town_educational_attainment` | town × year | Full attainment ladder (less than HS → doctorate) as counts and % of pop 25+ | `fact_educational_attainment_for_the_population_25_years_and_over_town` |
| 6 | `agg_town_school_enrollment` | town × year | Enrollment by level (preschool, K-12 by band, college, grad school), counts and % | `fact_school_enrollment_by_level_of_school_for_the_population_3_years_and_over_town` |
| 7 | `agg_town_demographics` | town × year | Sex ratio, race/ethnicity counts and rates, foreign-born rate, veteran rate, education snapshot, digital access, median income/home value — general-purpose demographic overview | 10 concept fact tables joined on `place_GEOID`/`year_key` |
| 8 | `agg_town_disability_status` | town × year | Disability counts by 6 age bands (combined sex), overall disability rate, rate by age band, rate by sex | `fact_sex_by_age_by_disability_status_town` (town-only — not published at BG level) |
| 9 | `agg_town_racial_equity_snapshot` | town × year | Race/ethnicity population shares — designed as a join target for computing disparity ratios against other topic tables | `fact_race_town`, `fact_hispanic_or_latino_origin_town`, `fact_total_population_town` |
| 10 | `agg_town_housing_burden` | town × year | Cost-burdened (30–49.9% of income on rent) and severely cost-burdened (50%+) renter household counts and rate | `fact_tenure_town`, `fact_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_town` |
| 11 | `agg_town_housing_affordability_index` | town × year | Rent-to-income ratio and home-price-to-income ratio | `fact_median_household_income_in_the_past_12_months_town`, `fact_median_value_dollars_town`, `fact_median_gross_rent_dollars_town`, `fact_median_gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_dollars_town` |
| 12 | `agg_town_housing_stability` | town × year | Vacancy rate, owner/renter occupancy rate, recent-mover counts/rates (2023+, 2020–22 bands), long-tenured counts, renter vs. owner turnover rate — displacement-risk proxy | `fact_housing_units_town`, `fact_vacancy_status_town`, `fact_tenure_town`, `fact_tenure_by_year_householder_moved_into_unit_town` |
| 13 | `agg_town_childcare` | town × year | Children under 6 and 6–17 by parental labor-force arrangement; "likely needs childcare/after-school care" proxy counts and rates; grandparent caregiver household count | `fact_age_of_own_children_under_18_years_..._town`, `fact_grandchildren_under_18_years_..._town` |
| 14 | `agg_town_transportation_access` | town × year | Commute mode share and %, average commute minutes, households with no vehicle | `fact_means_of_transportation_to_work_town`, `fact_aggregate_travel_time_to_work_in_minutes_..._town`, `fact_household_size_by_vehicles_available_town` |
| 15 | `analytics_infrastructure_accesibility` *(sic)* | town × year | No-car rate (owner/renter), household no-internet rate, labor-force uninsured rate, senior (65+) uninsured rate | `fact_tenure_town`, `fact_tenure_by_vehicles_available_town`, `fact_presence_and_types_of_internet_subscriptions_in_household_town`, `fact_health_insurance_coverage_status_and_type_by_employment_status_town`, `fact_types_of_health_insurance_coverage_by_age_town` |
| 16 | `agg_town_alice_household` | town × year | Total/poverty/ALICE/above-ALICE household counts | `fact_alice_town_household` |
| 17 | `agg_county_alice_household` | county × year | Same 4 measures, county-wide | `fact_alice_county` |
| 18 | `agg_town_home_value_trends` | town × date × housing_type | Home value with `LAG()`-computed previous-period value for trend charts | `fact_zillow_home_value` |
| 19 | `agg_town_rent_trends` | town × date | Rent index with `LAG()`-computed previous-period value | `fact_zillow_rent` |
| 20 | `zillow_market_affordability_index` | town × year | Annual-average home value and rent (aggregated from Zillow's monthly series) vs. median household income; price-to-income ratio | `fact_zillow_home_value`, `fact_zillow_rent`, `fact_median_household_income_in_the_past_12_months_town` |
| 21 | `agg_town_educational_attainment_s1501` | town × year | ACS Subject Table S1501: attainment by age band (18–24, 25–34, …, 65+), by race/ethnicity, median earnings by degree level, poverty rate by degree level | `fact_educational_attainment_town` (S1501, town-only) |

**Facility & CDC tables:**

| # | Table | Grain | Description |
|---|---|---|---|
| 22 | `agg_mhsu_facility_summary` | county × state × facility type | Facility counts grouped by county/type, with `is_mecklenburg` flag |
| 23 | `agg_mhsu_facility_detail` | 1 row/facility | Full address + lat/long detail list, for map visualization |
| 24 | `agg_town_cdc_health_outcomes` | town × year | Direct copy of `fact_health_outcomes_town` |
| 25 | `agg_town_cdc_health_status` | town × year | Direct copy of `fact_health_status_town` |
| 26 | `agg_town_cdc_prevention` | town × year | Direct copy of `fact_prevention_town` |
| 27 | `agg_town_cdc_disability` | town × year | Direct copy of `fact_disability_town` |
| 28 | `agg_town_cdc_risk_behaviors` | town × year | Direct copy of `fact_risk_behaviors_town` |
| 29 | `agg_town_cdc_social_needs` | town × year | Direct copy of `fact_social_needs_town` |

**Charlotte regional reference (2025 basis):**

| # | Table | Grain | Description |
|---|---|---|---|
| 30 | `agg_charlotte_fair_market_rent` | region × year × bedroom | Denormalized FMR with bedroom label and assumed household size |
| 31 | `agg_charlotte_ami_affordability_gap` | region × year × bedroom × AMI level | Full affordability-gap detail; `fmr` is `LEFT JOIN`ed back in from `fact_fair_market_rent` (not stored redundantly on the Gold fact) so this table stays self-contained for BI use |
| 32 | `agg_charlotte_occupation_housing_wage` | region × year × occupation | Hourly wage and employment count by occupation/category |

**Neighborhood geometry (block-level, finer than the block-group version in `nmidw_neighborhood_aggregate.py`):**

| # | Table | Grain | Description |
|---|---|---|---|
| 33 | `agg_neighborhood_blocks` | 1 row/2020 Census block | Hand-mapped block → neighborhood assignment (33 blocks total across the 5 focus neighborhoods) joined to `dim_block` for geometry |
| — | `agg_neighborhood_geometry_block` | 1 row/neighborhood | `ST_Union_Agg` of `agg_neighborhood_blocks.geometry` — the more precise of the two neighborhood boundary representations in the warehouse |

**School analysis tables** (all school-grain or school×subgroup-grain; source: the 8 Gold school fact tables + `dim_school`):

| Table | What it reproduces / computes |
|---|---|
| `agg_school_proficiency` | All-students, all-grades GLP/CCR per school (mirrors the team's original `proficiency.csv`) |
| `agg_school_ccr` | Same, filtered to grade scope `9-12` (mirrors `ccr.csv`) |
| `agg_school_economic_gap` | GLP gap between economically-disadvantaged and not-disadvantaged students, all grades |
| `agg_school_hs_economic_gap` | Same gap, high-school grade scope only |
| `agg_school_growth` | Overall growth status/index plus Reading- and Math-specific growth, side by side |
| `agg_school_graduation` | 4-yr/5-yr graduation rate, ACT WorkKeys and Math 3 indicators; `suppressed` flag when the raw graduation rate text isn't a parseable number (small-N suppression) |
| `agg_school_race_gap` | Long-format GLP gap of each race subgroup (Black/Hispanic/Asian/American Indian/Multiracial) vs. White, per school |
| `agg_school_disability_gap` | GLP gap between students with and without disabilities |
| `agg_town_school_summary` | School count and average GLP/CCR, rolled up to town |
| `agg_school_college_readiness` | CCR%, ACT composite benchmark %, and WorkKeys Silver+ %, high-school grade scope |
| `agg_school_scorecard` | One row per school: GLP, CCR, growth status/index, 4-yr grad rate — single-page overview |
| `agg_school_subject_proficiency` | GLP/CCR/denominator side-by-side for Reading, Math, Science, and combined EOC subjects |
| `agg_school_grade_level_proficiency` | Finest-grain GLP/CCR by subject area × grade scope |
| `agg_school_english_learner_progress` | EL progress %, % exiting EL status, % meeting annual progress (subgroup = `ELS` only) |

---

## 3. Main Layer — Neighborhood Aggregates (`main` schema, from `nmidw_neighborhood_aggregate.py`)

There is no official Census geography for the 5 "focus neighborhoods," so every table here approximates a neighborhood as a hand-picked group of block groups (grain: **neighborhood × block group × year**, using Gold's `*_bg` fact tables):

- **Huntington Green** — 1 block group (`371190062241`)
- **Pottstown** — 2 block groups (`371190063071`, `371190063072`)
- **West Davidson** — 1 block group (`371190064031`)
- **Smithville** — 1 block group (`371190064111`)
- **East Catawba** — 5 block groups (`371190064082`, `371190064091`, `371190064102`, `371190064111`, `371190064112`)

> **Known conflict:** block group `371190064111` is claimed by both Smithville (its full boundary) and East Catawba (15 of its constituent 2020 Census blocks, per a stakeholder-verified map). Any total that sums across both neighborhoods double-counts this block group's population. `agg_neighborhood_demographics.disclaimer` documents this inline; it is not resolved automatically because deciding which neighborhood "owns" the block group requires a stakeholder decision, not a data fix.

| Table | Description |
|---|---|
| `agg_neighborhood_demographics` | Total population, race/ethnicity counts and rates, foreign-born rate, plus the `disclaimer` column noting boundary-approximation caveats per neighborhood |
| `agg_neighborhood_economic_profile` | Household income brackets, median household income, Gini index, poverty count/rate |
| `agg_neighborhood_housing` | Housing units, owner/renter occupancy rate, median rent/home value, cost-burden counts/rate, no-vehicle counts |
| `agg_neighborhood_education` | Attainment ladder (counts + %), K-12 enrollment |
| `agg_neighborhood_transportation` | Commute mode share/%, average commute minutes, no-vehicle households |
| `agg_neighborhood_childcare` | Children under 6 / 6–17 by parental labor-force arrangement, childcare/after-school need proxy, grandparent caregiver count |
| `agg_neighborhood_blockgroup_geometry` | 1 row/neighborhood — `ST_Union_Agg` of `gold.dim_bg.geometry` for that neighborhood's block groups (coarser than `agg_neighborhood_geometry_block` in the aggregate pipeline, since it unions whole block groups rather than individual blocks) |

Note: `B18101` (disability by sex/age) and sex-disaggregated population counts are **not available at block group level** — Census only publishes that breakdown at census-tract level and above — so no neighborhood disability or sex-breakdown table exists; `fact_total_population_bg` (B01003) is used for neighborhood population totals instead.
