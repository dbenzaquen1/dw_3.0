## Data_Warehouse 2.0

Modernized ELT stack for loading operational data from SQL Server into BigQuery and modeling it with dbt, orchestrated with Dagster.

### Stack

- **Orchestration**: `dagster` (assets, schedules, jobs in `qmi_etl.definitions`)
- **Transformations / Warehouse modeling**: `dbt-core`, `dbt-bigquery` in `src/qmi_etl/datawarehouse_2`
- **Warehouse**: Google BigQuery
- **Sources**: SQL Server tables (see `SQL_SERVER_TABLES` in `src/qmi_etl/definitions.py`)
- **Packaging / env**: `uv` with `pyproject.toml`

### Prerequisites

- **Python**: version **>=3.10,<3.15**
- **uv** installed (`pip install uv` or see [uv docs](https://docs.astral.sh/uv))
- Access to:
  - SQL Server instance with the expected schemas/tables
  - BigQuery project/dataset
  - dbt profile credentials for BigQuery

### Install

```bash
uv sync
```

This installs the `qmi_etl` package and all runtime + dev dependencies defined in `pyproject.toml`.

### dbt project

The dbt project lives under `src/qmi_etl/datawarehouse_2`:

- Models: `models/staging`, `models/base`, `models/reporting`
- Seeds: `seeds/`
- Macros: `macros/`
- Config: `dbt_project.yml`, `profiles.yml`

Useful `uv` scripts (from `pyproject.toml`):

```bash
# Parse and compile the dbt project
uv run dbt-parse

# Run + build all models
uv run dbt-build
```

Both commands use:

- `--project-dir src/qmi_etl/datawarehouse_2`
- `--profiles-dir src/qmi_etl/datawarehouse_2`

Ensure `profiles.yml` is configured for your BigQuery environment before running them.

### Dagster assets & jobs

Dagster is configured in `src/qmi_etl/definitions.py`:

- **SQL Server source assets**: built via `build_sql_server_table_assets` for tables listed in `SQL_SERVER_TABLES`
- **dbt assets**: loaded with `dagster-dbt` as `datawarehouse_2_dbt_assets`
- **Jobs**:
  - `refresh_dbt_assets`: runs only dbt-backed assets (groups `base_dbt`, `staging_dbt`, `reporting_dbt`, `seeds_dbt`)
  - `refresh_sql_server_assets`: refreshes all SQL Server table assets
- **Schedules**:
  - `daily_refresh_schedule`: runs `all_assets_job` at 06:00, 12:00, 15:00, 18:00 US/Central
  - `sql_server_refresh_schedule`: runs `refresh_sql_server_assets` at 00:00, 04:00, 06:00, 12:00, 15:00, 18:00 US/Central

To run Dagster locally (one typical pattern):

```bash
uv run dagster dev
```

Then open the Dagster UI in your browser and trigger jobs / view asset materializations.

### Local development

- **Run tests**:

  ```bash
  uv run pytest
  ```

- **Code location**:
  - Dagster `Definitions` object: `qmi_etl.definitions:defs`
  - Root Python package for project code: `qmi_etl` under `src/`

### Project structure (high level)

- `pyproject.toml` – project metadata, dependencies, uv scripts, Dagster config
- `src/qmi_etl/definitions.py` – Dagster `Definitions`, assets, jobs, schedules
- `src/qmi_etl/defs/` – supporting Dagster/dbt asset and IO manager definitions
- `src/qmi_etl/datawarehouse_2/` – dbt project (models, seeds, macros, profiles)

### Notes / gotchas

- Verify time zone (`US/Central`) and cron expressions in `definitions.py` align with your operational expectations.
- The repo currently links source code references to the Dagster GitHub example (`link_code_references_to_git`); update `git_url`, `git_branch`, and `file_anchor_path_in_repository` if you want accurate links for this repository.
