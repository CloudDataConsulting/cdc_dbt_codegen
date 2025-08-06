# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CDC DBT Codegen is a dbt package that automates the generation of staging layer SQL and YAML files for data sources. It follows Cloud Data Consulting's patterns for organizing raw data in dedicated databases with well-named schemas for each source system.

## Common Development Commands

### DBT Commands
```bash
# Run the code generation models
dbt run -m src_table_list
dbt run -m gen_stage_files  
dbt run -m gen_stg_yml
dbt run -m gen_stg_src_name_yml
dbt run -m gen_stg_sql

# Seed the configuration data
dbt seed

# Run all models in the dw_util schema
dbt run --models cdc_dbt_codegen.dw_util.*
```

### Python Code Generation Commands
```bash
# Stage generation (generates SQL and YML files for staging layer)
python py/code_gen.py stage

# Non-stage YML generation (for dimensional/fact tables)
python py/code_gen.py nonstage --database <database> --schema <schema> --table <table>

# Generate YML files only
python py/code_gen.py yml --database <database> --schema <schema> --table <table>
```

### Modern Staging Generator (Key Pair Auth Support)
```bash
# List available sources
python py/generate_staging_modern.py --list-sources

# Generate staging files for a specific source
python py/generate_staging_modern.py --source asana

# Dry run to preview what would be generated
python py/generate_staging_modern.py --source asana --dry-run

# Use a specific dbt target
python py/generate_staging_modern.py --source asana --target prod
```

This script supports:
- Private key authentication (file or environment variable)
- SSO/external browser authentication fallback
- Environment variables: DBT_PRIVATE_KEY, DBT_PRIVATE_KEY_PASSPHRASE

### Legacy Python Scripts
```bash
# Generate staging files by source (requires prior dbt runs)
python py/generate_dbt_files_by_source.py

# Generate non-staging YML files
python py/generate_non_stg_yml_files.py

# Generate staging YML files  
python py/generate_stg_yml_files.py
```

## Architecture

### Project Structure
- **models/dw_util/**: Contains SQL views that generate the staging code
  - `src_table_list.sql`: Lists all tables from source schemas
  - `gen_stage_files.sql`: Generates source YML content
  - `gen_stg_sql.sql`: Generates staging SQL files
  - `gen_stg_yml.sql`: Generates staging YML files
  - `gen_non_stg_yml.sql`: Generates YML for dimensional/fact tables
  - `generate_db_table.sql`: Database metadata generation
  - `generate_fk_table.sql`: Foreign key relationship detection

- **seeds/**: Configuration files
  - `code_gen_config.csv`: Master configuration for source systems
  - `code_gen_config_yml.txt`: YAML configuration template

- **py/**: Python scripts for file generation
  - `code_gen.py`: Main CLI interface (work in progress)
  - `generate_dbt_files_by_source.py`: Legacy source-based generation
  - `generate_non_stg_yml_files.py`: Non-staging YML generation
  - `generate_stg_yml_files.py`: Staging YML generation

### Code Generation Flow
1. Configure source systems in `seeds/code_gen_config.csv`
2. Run `dbt seed` to load configuration
3. Run dbt models to generate metadata views
4. Use Python scripts to write files based on metadata

### Generated File Structure
- **Staging files**: `models/staging/{source_name}/`
  - `src_{source_name}.yml`: Source configuration
  - `stg_{source_name}__{table}.sql`: Staging SQL
  - `stg_{source_name}__{table}.yml`: Staging tests/docs

- **Non-staging files**:
  - Intermediate: `models/staging/intermediate/{table}.yml`
  - Dimensional: `models/marts/{mart_name}/{table}.yml`

### Key Patterns
- Source databases configured via `source_dbs` variable in parent project's dbt_project.yml
- Primary keys follow naming convention: `{table}_key` (e.g., `dim_date.date_key`)
- Foreign keys automatically detected from database constraints
- Staging tables use double underscore separator: `stg_{source}__{table}`
- All generated code includes CDC copyright header

### Important Notes
- The project requires dbt version >=1.0.0, <2.0.0
- Uses Snowflake information_schema for metadata discovery
- Tables must exist before YML generation for non-staging objects
- Foreign key constraints must be defined for relationship test generation
- The main `code_gen.py` CLI is partially implemented; use specific Python scripts for now