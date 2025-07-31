# CDC DBT Codegen Testing Guide

## Overview

This guide explains the testing strategy for CDC DBT Codegen, including how we ensure our mocks accurately represent Snowflake's behavior, how to write effective tests, and how to maintain test quality over time.

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Structure](#test-structure)
3. [Mock Validation Strategy](#mock-validation-strategy)
4. [Writing Tests](#writing-tests)
5. [Running Tests](#running-tests)
6. [Maintaining Test Quality](#maintaining-test-quality)
7. [Troubleshooting](#troubleshooting)

## Testing Philosophy

Our testing approach follows these principles:

1. **No External Dependencies**: All tests run without requiring Snowflake access
2. **Accurate Mocks**: Mocks accurately represent Snowflake's actual behavior
3. **Fast Execution**: Tests run quickly to encourage frequent testing
4. **Clear Failures**: Test failures provide clear information about what went wrong
5. **Comprehensive Coverage**: Critical paths have 100% test coverage

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                      # Shared fixtures and configuration
├── README.md                        # Quick reference for running tests
├── fixtures/                        # Test data and mock validation
│   ├── README.md                   # Mock validation guide
│   ├── sample_snowflake_responses.py # Documented Snowflake responses
│   └── test_mock_validation.py     # Tests to validate mock accuracy
├── unit/                           # Unit tests for individual modules
│   ├── test_config.py             # Configuration management tests
│   ├── test_connection.py         # Connection management tests
│   ├── test_generators.py         # Code generation logic tests
│   └── test_file_ops.py           # File operation tests
└── integration/                    # Integration tests
    └── test_cli.py                # CLI command tests
```

## Mock Validation Strategy

### Understanding Snowflake's Behavior

Snowflake has specific behaviors that our mocks must accurately represent:

1. **Column Name Case**: Snowflake returns column names in UPPERCASE by default
2. **Data Type Mappings**:
   - `VARCHAR` → Python `str`
   - `INTEGER/NUMBER` → Python `int`
   - `FLOAT` → Python `float`
   - `BOOLEAN` → Python `str` ('TRUE'/'FALSE')
   - `TIMESTAMP_*` → Python `datetime` or `str`
   - `NULL` → Python `None`
3. **DictCursor Behavior**: Returns lists of dictionaries with uppercase keys
4. **Connection Properties**: Connections have `database`, `schema`, `is_closed()` etc.

### Capturing Real Snowflake Responses

To ensure mock accuracy, periodically capture real responses:

```sql
-- 1. Capture table metadata
SELECT * FROM information_schema.tables 
WHERE table_schema = 'YOUR_SCHEMA' 
LIMIT 5;

-- 2. Capture column metadata
SELECT * FROM information_schema.columns 
WHERE table_schema = 'YOUR_SCHEMA' 
AND table_name = 'YOUR_TABLE'
ORDER BY ordinal_position
LIMIT 20;

-- 3. Capture generated views (after running dbt models)
SELECT * FROM your_db.your_schema_dw_util.gen_stg_sql LIMIT 5;
SELECT * FROM your_db.your_schema_dw_util.gen_stg_yml LIMIT 5;
SELECT * FROM your_db.your_schema_dw_util.gen_stg_src_name_yml LIMIT 5;

-- 4. Capture configuration
SELECT * FROM your_db.your_schema.code_gen_config;
```

### Creating Accurate Mocks

#### Good Mock Example
```python
# Matches Snowflake's actual response format
mock_cursor.fetchall.return_value = [
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',      # UPPERCASE keys
        'TABLE_SCHEMA': 'SFDC',             # UPPERCASE values for identifiers
        'TABLE_NAME': 'ACCOUNT',            
        'ROW_COUNT': 12345,                 # Integer, not string
        'CREATED': datetime(2024, 1, 15),   # Datetime object
        'COMMENT': None,                    # None for NULL
        'IS_TRANSIENT': 'NO'               # String for boolean
    }
]
```

#### Bad Mock Example
```python
# DON'T DO THIS - Doesn't match Snowflake
mock_cursor.fetchall.return_value = [
    {
        'table_catalog': 'prd_raw_db',     # Wrong: lowercase keys
        'row_count': '12345',              # Wrong: string instead of int
        'created': '',                     # Wrong: empty string for NULL
        'is_transient': False              # Wrong: boolean instead of string
    }
]
```

### Mock Validation Tests

We maintain `tests/fixtures/test_mock_validation.py` to ensure our mocks are accurate:

```python
def test_snowflake_response_format():
    """Ensure mocked responses match Snowflake's format."""
    response = SAMPLE_TABLES_RESPONSE[0]
    
    # All keys should be uppercase
    assert all(key.isupper() for key in response.keys())
    
    # Check data types match Snowflake's Python mappings
    assert isinstance(response['ROW_COUNT'], int)
    assert isinstance(response['TABLE_NAME'], str)
    assert response['IS_TRANSIENT'] in ('YES', 'NO')
```

## Writing Tests

### Unit Tests

Unit tests should test individual functions/methods in isolation:

```python
class TestConfig:
    def test_get_staging_path(self, mock_dbt_project):
        """Test staging path generation."""
        config = Config(working_dir=mock_dbt_project)
        
        # Test the specific behavior
        path = config.get_staging_path('sfdc')
        
        # Assert expected outcome
        assert path == mock_dbt_project / 'models' / 'staging' / 'sfdc'
        assert isinstance(path, Path)
```

### Integration Tests

Integration tests verify that components work together:

```python
def test_stage_command_end_to_end(self, mock_generator_class):
    """Test complete stage command execution."""
    # Setup
    args = create_args(command='stage', all=True, source='sfdc')
    config = Mock()
    conn_manager = Mock()
    
    # Configure mocks
    mock_generator = Mock()
    mock_generator.generate.return_value = ['file1.sql', 'file2.yml']
    mock_generator_class.return_value = mock_generator
    
    # Execute
    result = handle_stage_command(args, config, conn_manager)
    
    # Verify
    assert result == 0  # Success
    mock_generator.generate.assert_called_once()
```

### Testing Error Conditions

Always test error paths:

```python
def test_connection_error_handling(self):
    """Test behavior when Snowflake connection fails."""
    with patch('snowflake.connector.connect') as mock_connect:
        mock_connect.side_effect = Exception("Connection failed")
        
        manager = ConnectionManager()
        
        with pytest.raises(Exception) as exc_info:
            manager.get_connection()
        
        assert "Connection failed" in str(exc_info.value)
```

### Using Fixtures Effectively

```python
@pytest.fixture
def mock_snowflake_data():
    """Provide realistic Snowflake response data."""
    return {
        'cursor_response': [
            {'TABLE_NAME': 'ACCOUNT', 'ROW_COUNT': 100},
            {'TABLE_NAME': 'CONTACT', 'ROW_COUNT': 200}
        ],
        'expected_count': 2
    }

def test_with_fixture(mock_snowflake_data):
    """Test using fixture data."""
    cursor = Mock()
    cursor.fetchall.return_value = mock_snowflake_data['cursor_response']
    
    results = cursor.fetchall()
    assert len(results) == mock_snowflake_data['expected_count']
```

## Running Tests

### Basic Test Execution

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=cdc_dbt_codegen --cov-report=html

# Run specific test file
pytest tests/unit/test_config.py

# Run specific test
pytest tests/unit/test_config.py::TestConfig::test_get_staging_path

# Run with verbose output
pytest -v

# Show print statements
pytest -s
```

### Test Categories

```bash
# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run only mock validation tests
pytest tests/fixtures/

# Run tests matching a pattern
pytest -k "test_connection"
```

### Debugging Failed Tests

```bash
# Drop into debugger on failure
pytest --pdb

# Show local variables on failure
pytest -l

# Show full traceback
pytest --tb=long

# Run last failed tests
pytest --lf
```

## Maintaining Test Quality

### Regular Mock Validation

1. **Quarterly Reviews**: Compare mocks against real Snowflake responses
2. **Version Updates**: Update mocks when upgrading Snowflake connector
3. **Schema Changes**: Update mocks when dbt models change
4. **Document Changes**: Keep sample_snowflake_responses.py current

### Test Coverage Goals

- **Overall**: 80%+ coverage
- **Core modules**: 90%+ coverage (config, connection, generators)
- **Critical paths**: 100% coverage (connection creation, file generation)
- **Error handling**: All error paths tested

### Code Review Checklist

When reviewing tests:
- [ ] Tests are isolated (no dependencies between tests)
- [ ] Mocks match Snowflake's actual behavior
- [ ] Error conditions are tested
- [ ] Test names clearly describe what's being tested
- [ ] Assertions have clear failure messages
- [ ] No hardcoded paths or environment-specific values

### Continuous Improvement

1. **Track Flaky Tests**: Document and fix intermittent failures
2. **Performance Monitoring**: Keep test suite under 30 seconds
3. **Mock Accuracy**: Regular validation against real Snowflake
4. **Documentation**: Update this guide as patterns evolve

## Troubleshooting

### Common Test Issues

#### Import Errors
```python
# Problem: ImportError: No module named 'snowflake'
# Solution: Install test dependencies
pip install -e ".[dev]"
```

#### Mock Not Behaving Correctly
```python
# Problem: 'Mock' object is not iterable
# Solution: Configure the mock properly
mock_cursor = Mock()
mock_cursor.fetchall.return_value = []  # Make it return iterable
mock_cursor.__iter__ = Mock(return_value=iter([]))  # For direct iteration
```

#### Case Sensitivity Issues
```python
# Problem: KeyError: 'table_name' (lowercase)
# Solution: Use uppercase keys to match Snowflake
result['TABLE_NAME']  # Correct
result['table_name']  # Wrong
```

### Debugging Tips

1. **Print Mock Calls**: 
   ```python
   print(mock_object.call_args_list)
   ```

2. **Check Mock Configuration**:
   ```python
   assert mock_cursor.fetchall.called
   print(mock_cursor.fetchall.call_count)
   ```

3. **Use Mock Spec**:
   ```python
   # This ensures mock has same interface as real object
   mock_cursor = Mock(spec=DictCursor)
   ```

4. **Test in Isolation**:
   ```python
   # Run single test with full output
   pytest -xvs path/to/test.py::test_name
   ```

## Best Practices Summary

1. **Always use uppercase keys** in Snowflake response mocks
2. **Test both success and failure paths**
3. **Use fixtures for common test data**
4. **Keep tests fast and independent**
5. **Document why a test exists** with clear test names
6. **Validate mocks against real Snowflake** periodically
7. **Use type hints** in test code for clarity
8. **Avoid testing implementation details** - test behavior

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Python Mock Library](https://docs.python.org/3/library/unittest.mock.html)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector)
- [dbt Testing Best Practices](https://docs.getdbt.com/docs/best-practices/writing-custom-tests)