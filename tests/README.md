# Testing CDC DBT Codegen

This document explains how to run and write tests for the CDC DBT Codegen project.

## Prerequisites

Install the development dependencies:

```bash
pip install -e ".[dev]"
```

Or install test dependencies directly:

```bash
pip install pytest pytest-cov pytest-mock
```

## Running Tests

### Run all tests

```bash
# From the project root
pytest

# With coverage report
pytest --cov=cdc_dbt_codegen --cov-report=html

# Verbose output
pytest -v

# Run specific test file
pytest tests/unit/test_config.py

# Run specific test
pytest tests/unit/test_config.py::TestConfig::test_init_with_working_dir
```

### Run only unit tests

```bash
pytest tests/unit/
```

### Run only integration tests

```bash
pytest tests/integration/
```

## Test Structure

```
tests/
├── __init__.py
├── conftest.py          # Shared fixtures and test configuration
├── unit/                # Unit tests for individual modules
│   ├── test_config.py   # Tests for config module
│   ├── test_connection.py # Tests for connection module
│   └── test_generators.py # Tests for generators module
├── integration/         # Integration tests
│   └── test_cli.py      # Tests for CLI functionality
└── fixtures/            # Test data files (if needed)
```

## Writing Tests

### Unit Tests

Unit tests should:
- Test individual functions/methods in isolation
- Mock external dependencies (database, file system)
- Be fast and deterministic
- Cover edge cases and error conditions

Example:

```python
def test_get_staging_path(self, mock_dbt_project):
    """Test getting staging path for a source."""
    config = Config(working_dir=mock_dbt_project)
    
    path = config.get_staging_path('sfdc')
    assert path == mock_dbt_project / 'models' / 'staging' / 'sfdc'
```

### Integration Tests

Integration tests should:
- Test interactions between components
- Test CLI commands end-to-end
- Mock only external systems (Snowflake)
- Verify file operations work correctly

Example:

```python
def test_stage_command_success(self, mock_generator_class):
    """Test successful stage command execution."""
    args = Mock()
    args.all = True
    args.source = 'sfdc'
    
    # ... test implementation
```

## Key Test Fixtures

### `temp_dir`
Creates a temporary directory for test files that's automatically cleaned up.

### `mock_dbt_project`
Creates a mock dbt project structure with:
- dbt_project.yml
- models/ directory structure
- seeds/ directory

### `mock_snowflake_connection`
Provides a mocked Snowflake connection for testing database operations.

### `mock_source_config`
Provides sample source configuration data.

### `mock_staging_sql_data`
Provides sample staging SQL generation data.

## Testing Without Snowflake

All tests are designed to run without a real Snowflake connection. Database operations are mocked using the fixtures in `conftest.py`.

## Manual Testing

To test the actual CLI with a real Snowflake connection:

1. Ensure you have valid Snowflake credentials configured
2. Have a dbt project with the codegen package installed
3. Run dbt seed to load configuration data
4. Test commands:

```bash
# List configured sources
./codegen list-sources

# Generate staging files (dry run)
./codegen stage --all --source sfdc --dry-run

# Generate staging files
./codegen stage --all --source sfdc

# Generate dimensional YAMLs
./codegen dimensional --database dev_edw_db --schema bpruss_base
```

## Debugging Tests

### Use pytest's built-in debugging

```bash
# Drop into debugger on failures
pytest --pdb

# Show print statements
pytest -s

# Show local variables on failure
pytest -l
```

### VS Code Configuration

Add to `.vscode/settings.json`:

```json
{
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "tests"
    ]
}
```

## Coverage Goals

Aim for:
- 80%+ overall coverage
- 90%+ coverage for core modules
- 100% coverage for critical paths (connection, config, generators)

Check coverage report:

```bash
pytest --cov=cdc_dbt_codegen --cov-report=term-missing
```

## Continuous Integration

When setting up CI/CD, use these commands:

```bash
# Install dependencies
pip install -e ".[dev]"

# Run linting
flake8 cdc_dbt_codegen tests

# Run type checking
mypy cdc_dbt_codegen

# Run tests with coverage
pytest --cov=cdc_dbt_codegen --cov-report=xml

# Run black formatter check
black --check cdc_dbt_codegen tests
```