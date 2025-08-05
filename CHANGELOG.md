# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0-beta.1] - 2025-01-05

### Added
- **Complete Python CLI** (`codegen` command) with subcommands:
  - `stage`: Generate staging files for sources
  - `dimensional`: Generate YML for dimensional/fact tables  
  - `list-sources`: List configured source systems
- **Modern package structure**:
  - `cdc_dbt_codegen/core/`: Core generation logic
  - `cdc_dbt_codegen/utils/`: Utility functions
  - Centralized connection management
- **Comprehensive test suite**:
  - 63 unit tests with 78% code coverage
  - Mock validation framework for Snowflake responses
  - Pytest fixtures for common test scenarios
- **Development tooling**:
  - `requirements.txt` with pinned dependencies
  - `requirements-dev.txt` for development dependencies
  - `pytest.ini` configuration
  - `.coverage` configuration
- **Documentation**:
  - `CLAUDE.md` for AI assistant guidance
  - `docs/enhancement-todo.md` with prioritized improvements
  - `docs/testing-guide.md` with comprehensive testing documentation
  - `docs/sample_snowflake_responses.py` documenting Snowflake response formats

### Changed
- **Refactored `py/code_gen.py`**: Complete rewrite from broken state to working CLI
- **Consolidated generation logic**: Extracted from scattered scripts into organized modules
- **Improved error handling**: Better error messages and validation throughout
- **Enhanced connection management**: Support for multiple auth methods (password, key-pair, OAuth)

### Fixed
- **CLI functionality**: Main `code_gen.py` was non-functional, now fully operational
- **Import errors**: Fixed circular imports and missing dependencies
- **Snowflake cursor behavior**: Properly handle uppercase keys and iteration patterns
- **Configuration loading**: Fixed issues with profile and source configuration

### Deprecated
- Legacy scripts in `py/` directory (still functional but superseded by new CLI):
  - `generate_dbt_files_by_source.py`
  - `generate_non_stg_yml_files.py`
  - `generate_stg_yml_files.py`

## [0.2.11] - Previous Version

### Known Issues (Fixed in 0.3.0)
- Main CLI (`code_gen.py`) was incomplete and non-functional
- Duplicate code across multiple SQL models
- Hardcoded 'dev' profile references
- Missing modern Python packaging standards
- No test coverage

## Migration Guide

### From 0.2.x to 0.3.0

The new CLI provides the same functionality as the legacy scripts but with a unified interface:

**Old way:**
```bash
python py/generate_dbt_files_by_source.py
python py/generate_non_stg_yml_files.py
```

**New way:**
```bash
codegen stage --all
codegen dimensional --database DB --schema SCHEMA --table TABLE
```

### Installation

With the new package structure, you can install directly:
```bash
pip install -e .
```

This installs the `codegen` command globally for easier usage.

### Configuration

The package still uses the same configuration files:
- `seeds/code_gen_config.csv` for source configuration
- dbt profiles for connection settings

But now supports additional environment variables for connection:
- `DBT_SNOWFLAKE_ACCOUNT`
- `DBT_SNOWFLAKE_USER`
- `DBT_SNOWFLAKE_PASSWORD`
- `DBT_SNOWFLAKE_ROLE`
- `DBT_SNOWFLAKE_WAREHOUSE`