# Shared Function Reference (`functions/`)

Every pipeline script imports from these two modules rather than duplicating connection or Census-API logic. This is a function-by-function reference; see the [README](../README.md) for how they fit into the overall pipeline and [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the tables they ultimately feed.

---

## `functions/mother_duck_connector.py`

The single chokepoint every pipeline script uses to reach the warehouse — no script opens a `duckdb` connection any other way.

### `get_md_connection()`

```python
def get_md_connection() -> duckdb.DuckDBPyConnection
```

Loads environment variables via `python-dotenv` (`load_dotenv()` runs at import time), opens a MotherDuck connection with `duckdb.connect("md:")` — which authenticates using the `MOTHERDUCK_TOKEN` environment variable, read implicitly by the MotherDuck DuckDB extension — then runs `CREATE DATABASE IF NOT EXISTS nmidw;` and `USE nmidw;` before returning the connection.

- **Parameters:** none.
- **Returns:** an open `duckdb.DuckDBPyConnection` already pointed at the `nmidw` database.
- **Side effects:** creates the `nmidw` MotherDuck database on first call if it doesn't exist yet; prints a `☁️ Connecting to MotherDuck Cloud (nmidw)...` status line.
- **Caller responsibility:** every pipeline script calls this once near the top and is responsible for calling `.close()` on the returned connection itself — the function does not manage the connection lifecycle beyond opening it.

---

## `functions/tidycensus_replicator.py`

A Python port of the parts of R's [tidycensus](https://walker-data.com/tidycensus/) package the warehouse needs: pulling ACS 5-year estimates for a set of variables/years/geographies and reshaping the response into tidy (long) format. Used exclusively by `pipelines/nmidw_census.py`.

Module-level constants (defined at the top of the file, **not** parameterized per call):

| Constant | Value | Notes |
|---|---|---|
| `CENSUS_API_KEY` | hardcoded string | See [Known Issues](../README.md#known-issues) in the README — should move to `.env` like `MOTHERDUCK_TOKEN`. `nmidw_census.py` also declares its own copy of this constant rather than importing it. |
| `STATE_FIPS` | `"37"` | North Carolina |
| `COUNTY_FIPS` | `"119"` | Mecklenburg County |

These module constants are **not** actually used by the functions below — every function that needs them takes `api_key`/`state_fips`/`county_fips` as explicit parameters instead (`nmidw_census.py` passes its own copies in). They exist as leftover defaults / documentation of what the pipeline targets.

### `chunk_variables(var_list, chunk_size=20)`

```python
def chunk_variables(var_list: list, chunk_size: int = 20) -> Iterator[list]
```

Generator that yields successive `chunk_size`-sized slices of `var_list`. Exists because the Census API rejects requests with too many variables in a single `get=` query string — chunking keeps every request under that limit.

- **Parameters:** `var_list` — flat list of variable codes; `chunk_size` — max variables per API call (default 20).
- **Yields:** successive sub-lists of `var_list`.

### `classify_variables(raw_vars)`

```python
def classify_variables(raw_vars: list) -> dict
```

Splits a mixed list of ACS variable codes into two buckets by table type, based on the code's first letter: codes starting with `S` (e.g. `S1501_C01_001`) go to `"subject"` (served by the `/acs/acs5/subject` endpoint); everything else (`B`/`C` prefix, e.g. `B01003_001`) goes to `"detailed"` (served by `/acs/acs5`).

- **Parameters:** `raw_vars` — list of variable codes, case-insensitive.
- **Returns:** `{"detailed": [...], "subject": [...]}`.

### `get_safe_variables(year, raw_vars, table_type="detailed")`

```python
def get_safe_variables(year: int, raw_vars: list, table_type: str = "detailed") -> list
```

Validates a list of variable codes against that year's real ACS metadata before requesting them, since not every variable exists in every vintage year. Fetches `.../variables.json` for the given year and table type, checks whether `f"{var}E"` is a key in the response, and drops (with a logged warning) any variable missing from that vintage.

- **Parameters:** `year` — ACS vintage year; `raw_vars` — variable codes to validate; `table_type` — `"detailed"` or `"subject"`, selects which metadata endpoint to check against.
- **Returns:** the subset of `raw_vars` confirmed present in that year's metadata. **Fails open**: if the metadata request itself fails (non-200), returns `raw_vars` unchanged and lets the actual data request surface any error.
- **Side effect:** prints which variables are missing for that year/table type, if any.

### `fetch_census_chunk(year, geography, variables, api_key, state_fips, county_fips, table_type="detailed")`

```python
def fetch_census_chunk(
    year: int, geography: str, variables: list,
    api_key: str, state_fips: str, county_fips: str,
    table_type: str = "detailed",
) -> pl.DataFrame
```

Fetches **one chunk** (≤20 variables) from the Census API for a single year/geography and reshapes the wide JSON response into a tidy long DataFrame with columns `GEOID, NAME, variable, estimate, moe`.

- **Parameters:**
  - `year` — ACS vintage year
  - `geography` — `"block group"` (queries `for=block%20group:*&in=state:{state}%20county:{county}`) or anything else, treated as place-level (`for=place:*&in=state:{state}`)
  - `variables` — base variable codes (no `E`/`M` suffix) for this chunk
  - `api_key`, `state_fips`, `county_fips` — passed straight into the request URL
  - `table_type` — `"detailed"` (requests both `E` estimate and `M` margin-of-error columns) or `"subject"` (Census subject tables only expose `E` columns — MOE is filled with `NULL`, not requested)
- **Returns:** a `pl.DataFrame` with one row per `(GEOID, variable)`: `GEOID` (block group: state+county+tract+block group concatenated; place: state+place concatenated), `NAME`, `variable` (base code, `E`/`M` suffix stripped), `estimate`, `moe`.
- **Raises:** `ValueError` if the HTTP response is not 200.
- **Notes:** an empty API response (`len(data) <= 1`, i.e. header row only) returns an empty `pl.DataFrame()` rather than raising.

### `fetch_acs_metadata(target_years)`

```python
def fetch_acs_metadata(target_years: list) -> pl.DataFrame
```

Fetches ACS variable metadata (label + concept text) for every year in `target_years`, for both the detailed (`/acs/acs5/variables.json`) and subject (`/acs/acs5/subject/variables.json`) endpoints, and stacks everything into one DataFrame. This is the source of `bronze.acs_metadata`, which `nmidw_census.py` later turns into `silver.variable_crosswalk` — the table that drives dynamic Gold fact-table generation.

- **Parameters:** `target_years` — list of ACS vintage years to pull metadata for.
- **Returns:** `pl.DataFrame` with columns `year, name (raw variable code), label, concept`. The `for`/`in` pseudo-variables are filtered out.
- **Notes:** if a given year/table-type metadata request fails, that combination is skipped with a warning — the function does not raise on a single failed year.

### `run_ingestion_pipeline(target_years, acs_variables_raw, geography, api_key, state_fips, county_fips)`

```python
def run_ingestion_pipeline(
    target_years: list, acs_variables_raw: list, geography: str,
    api_key: str, state_fips: str, county_fips: str,
) -> pl.DataFrame
```

The top-level orchestrator — this is the one function `nmidw_census.py` actually calls (once for `"block group"`, once for `"place"`). For every year in `target_years`:

1. Classifies `acs_variables_raw` into detailed/subject via `classify_variables`.
2. Skips subject tables entirely at `geography == "block group"` — the Census API does not support subject tables below place level.
3. Validates variables for that year via `get_safe_variables`.
4. Chunks safe variables via `chunk_variables` and calls `fetch_census_chunk` per chunk, concatenating results.
5. Retries the whole year/table-type combination up to 5 times (5s × attempt-number backoff) on any exception.
6. Tags the concatenated result with a `vintage_year` column and appends it to the running list.

- **Parameters:** `target_years` — years to pull (`nmidw_census.py` uses `range(2018, 2025)`); `acs_variables_raw` — full variable list (mixed detailed + subject codes); `geography` — `"block group"` or `"place"`; `api_key`, `state_fips`, `county_fips` — Census API credentials/target area.
- **Returns:** a single `pl.DataFrame` — all years, all variables, all chunks concatenated — with columns `GEOID, NAME, variable, estimate, moe, vintage_year`.
- **Raises:** `RuntimeError` if a given year/table-type combination still fails after 5 retries — this is a hard stop, not a skip-and-continue (unlike `fetch_acs_metadata`'s per-year failure handling).

### Call graph (as used by `nmidw_census.py`)

```
run_ingestion_pipeline(geography="block group")  ──┐
run_ingestion_pipeline(geography="place")         ──┼──> bronze.acs_blockgroup / bronze.acs_place
fetch_acs_metadata(target_years)                  ──┘         bronze.acs_metadata

  each run_ingestion_pipeline() call internally chains:
  classify_variables → get_safe_variables → chunk_variables → fetch_census_chunk (× N chunks, × N years)
```
