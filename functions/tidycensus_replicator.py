# ==============================================================================
# Tidycensus Replicator
# Author: Paul Park, Claude Code
# Updated: fetch_acs_metadata now accepts target_years list → per-year metadata
#          with year column for crosswalk versioning
# ==============================================================================

import requests
import polars as pl
import time

CENSUS_API_KEY = "d95612e8a94c486ec6e95901c5996f39dcf9240f"
STATE_FIPS = "37"
COUNTY_FIPS = "119"


def chunk_variables(var_list, chunk_size=20):
    """Yield successive chunk_size-sized chunks from var_list to bypass API limits."""
    for i in range(0, len(var_list), chunk_size):
        yield var_list[i:i + chunk_size]


def classify_variables(raw_vars):
    """
    Splits a mixed variable list into two groups by table type:
      - 'detailed': B/C prefix → /acs/acs5
      - 'subject':  S prefix   → /acs/acs5/subject
    Returns a dict: {"detailed": [...], "subject": [...]}
    """
    detailed, subject = [], []
    for var in raw_vars:
        if var.upper().startswith("S"):
            subject.append(var)
        else:
            detailed.append(var)
    return {"detailed": detailed, "subject": subject}


def get_safe_variables(year, raw_vars, table_type="detailed"):
    """
    Validates variables against the ACS metadata for a given year and table type.
    Variables missing from that vintage year are logged and excluded.
      table_type: "detailed" → /acs/acs5/variables.json
                  "subject"  → /acs/acs5/subject/variables.json
    """
    endpoint = {
        "detailed": f"https://api.census.gov/data/{year}/acs/acs5/variables.json",
        "subject":  f"https://api.census.gov/data/{year}/acs/acs5/subject/variables.json",
    }[table_type]

    print(f"[{year}] Validating {table_type} variables against ACS metadata...")
    response = requests.get(endpoint)

    # If metadata fetch fails, return all variables as-is and let the API handle errors
    if response.status_code != 200:
        return raw_vars
    valid_keys = response.json().get('variables', {}).keys()

    safe_vars, missing_vars = [], []
    for var in raw_vars:
        if f"{var}E" in valid_keys:
            safe_vars.append(var)
        else:
            missing_vars.append(var)

    if missing_vars:
        print(f"  Warning: Missing in {year} ({table_type}), will be null: {missing_vars}")
    return safe_vars


def fetch_census_chunk(year, geography, variables, api_key, state_fips, county_fips, table_type="detailed"):
    """
    Fetches a single chunk of variables from the Census API and returns a tidy DataFrame.
    Subject tables do not provide MOE columns — those are filled with null.
      table_type: "detailed" → /acs/acs5
                  "subject"  → /acs/acs5/subject
    """
    # Subject tables only expose E (estimate) columns, no M (margin of error)
    has_moe = (table_type == "detailed")

    get_vars = ["NAME"] + [f"{v}E" for v in variables]
    if has_moe:
        get_vars += [f"{v}M" for v in variables]
    get_str = ",".join(get_vars)

    if geography == "block group":
        for_str = "block%20group:*"
        in_str = f"state:{state_fips}%20county:{county_fips}"
    else:
        for_str = "place:*"
        in_str = f"state:{state_fips}"

    base_url = {
        "detailed": f"https://api.census.gov/data/{year}/acs/acs5",
        "subject":  f"https://api.census.gov/data/{year}/acs/acs5/subject",
    }[table_type]

    url = f"{base_url}?get={get_str}&for={for_str}&in={in_str}&key={api_key}"

    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"API Error {response.status_code}: {response.text}")

    data = response.json()
    if len(data) <= 1:
        return pl.DataFrame()

    # Convert raw JSON response into a Polars DataFrame
    df = pl.DataFrame(data[1:], schema=data[0], orient="row")

    # Build GEOID by concatenating geographic identifiers
    if geography == "block group":
        df = df.with_columns(
            pl.concat_str([pl.col("state"), pl.col("county"), pl.col("tract"), pl.col("block group")]).alias("GEOID")
        )
    else:
        df = df.with_columns(
            pl.concat_str([pl.col("state"), pl.col("place")]).alias("GEOID")
        )

    # Cast all estimate and MOE columns to Float64
    e_cols = [c for c in df.columns if c.endswith("E") and c != "NAME"]
    m_cols = [c for c in df.columns if c.endswith("M") and c != "NAME"] if has_moe else []

    df = df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False) for c in e_cols + m_cols
    ])

    # Unpivot estimate columns into long (tidy) format
    df_e = df.unpivot(index=["GEOID", "NAME"], on=e_cols, variable_name="variable_e", value_name="estimate")
    df_e = df_e.with_columns(
        pl.col("variable_e").str.slice(0, pl.col("variable_e").str.len_chars() - 1).alias("variable")
    )

    # Unpivot MOE columns; for subject tables fill MOE with null
    if has_moe and m_cols:
        df_m = df.unpivot(index=["GEOID", "NAME"], on=m_cols, variable_name="variable_m", value_name="moe")
        df_m = df_m.with_columns(
            pl.col("variable_m").str.slice(0, pl.col("variable_m").str.len_chars() - 1).alias("variable")
        )
        df_tidy = df_e.join(df_m.select(["GEOID", "NAME", "variable", "moe"]), on=["GEOID", "NAME", "variable"], how="left")
    else:
        df_tidy = df_e.with_columns(pl.lit(None).cast(pl.Float64).alias("moe"))

    return df_tidy.select(["GEOID", "NAME", "variable", "estimate", "moe"])


def fetch_acs_metadata(target_years: list) -> pl.DataFrame:
    """
    Fetches ACS variable metadata for every year in target_years (both detailed and subject tables).
    Returns a single DataFrame with a 'year' column so the crosswalk can track
    variable definitions that change across ACS vintage years.
    """
    print("\nFetching ACS Variable Metadata per year...")

    # Relative paths for each table type's metadata endpoint
    metadata_paths = {
        "detailed": "/acs/acs5/variables.json",
        "subject":  "/acs/acs5/subject/variables.json",
    }

    all_records = []

    for year in target_years:
        print(f"  [{year}] Fetching metadata...")
        for table_type, path in metadata_paths.items():
            url = f"https://api.census.gov/data/{year}{path}"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"    Warning: Could not fetch {table_type} metadata for {year} — skipping.")
                continue

            variables = response.json().get('variables', {})
            for var_code, details in variables.items():
                if var_code in ["for", "in"]:
                    continue
                all_records.append({
                    "year":    year,
                    "name":    var_code,
                    "label":   details.get("label", ""),
                    "concept": details.get("concept", ""),
                })

    return pl.DataFrame(all_records)


def run_ingestion_pipeline(target_years, acs_variables_raw, geography, api_key, state_fips, county_fips):
    """
    Runs the full ingestion loop for all target years with retry logic.
    Automatically routes B/C variables to the detailed endpoint
    and S variables to the subject endpoint, then concatenates results.
    Note: Subject tables are skipped at block group level (not supported by Census API).
    """
    data_frames = []

    for yr in target_years:
        print(f"\nProcessing {geography} for year: {yr}")

        classified = classify_variables(acs_variables_raw)
        yearly_chunks = []

        for table_type, vars_for_type in classified.items():
            if not vars_for_type:
                continue

            # Census API does not support subject tables at block group geography
            if table_type == "subject" and geography == "block group":
                print(f"  Skipping subject tables for block group (unsupported geography level)")
                continue

            safe_vars = get_safe_variables(yr, vars_for_type, table_type=table_type)
            if not safe_vars:
                continue

            max_retries = 5
            attempt = 1
            success = False

            while not success and attempt <= max_retries:
                try:
                    chunked_results = []
                    for var_chunk in chunk_variables(safe_vars, 20):
                        res = fetch_census_chunk(
                            yr, geography, var_chunk,
                            api_key, state_fips, county_fips,
                            table_type=table_type
                        )
                        chunked_results.append(res)

                    yearly_chunks.append(pl.concat(chunked_results))
                    success = True

                except Exception as e:
                    print(f"  Retry {attempt}/{max_retries} failed for {yr} ({table_type}): {e}")
                    time.sleep(attempt * 5)
                    attempt += 1

            if not success:
                raise RuntimeError(f"Fatal Ingestion Failure: API rejected year {yr} ({table_type})")

        if yearly_chunks:
            yearly_res = pl.concat(yearly_chunks)
            yearly_res = yearly_res.with_columns(pl.lit(yr).alias("vintage_year"))
            data_frames.append(yearly_res)

    return pl.concat(data_frames)