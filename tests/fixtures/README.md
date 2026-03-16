# Test Fixtures and Mock Validation

This directory contains test fixtures and documentation for validating that our mocks accurately represent Snowflake's actual behavior.

## How to Validate Mock Accuracy

### 1. Capture Real Snowflake Responses

To ensure our mocks are accurate, periodically capture real responses from Snowflake:

```sql
-- Run these queries in Snowflake to capture actual response structures

-- 1. Table metadata
SELECT * FROM information_schema.tables 
WHERE table_schema = 'YOUR_SCHEMA' 
LIMIT 5;

-- 2. Column metadata
SELECT * FROM information_schema.columns 
WHERE table_schema = 'YOUR_SCHEMA' 
AND table_name = 'YOUR_TABLE'
LIMIT 10;

-- 3. Generated staging SQL (after running dbt models)
SELECT * FROM your_db.your_schema_dw_util.gen_stg_sql
WHERE source_name = 'your_source'
LIMIT 5;

-- 4. Generated staging YML
SELECT * FROM your_db.your_schema_dw_util.gen_stg_yml
WHERE source_name = 'your_source'
LIMIT 5;

-- 5. Source configuration
SELECT * FROM your_db.your_schema.code_gen_config;

-- 6. Foreign key relationships
SHOW IMPORTED KEYS IN DATABASE your_db;
```

### 2. Update sample_snowflake_responses.py

Update the sample responses in `sample_snowflake_responses.py` with the actual data structure from Snowflake. Pay attention to:

- Column names (Snowflake returns uppercase by default)
- Data types
- NULL vs empty strings
- Date/timestamp formats
- Any special Snowflake-specific fields

### 3. Validate Mock Behavior

Create integration tests that compare mock behavior with documented Snowflake behavior:

```python
def test_mock_matches_snowflake_structure():
    """Validate that our mock returns data in Snowflake's format."""
    mock_cursor = create_mock_cursor()
    mock_cursor.fetchall.return_value = SAMPLE_TABLES_RESPONSE
    
    result = mock_cursor.fetchall()
    
    # Validate structure matches Snowflake
    assert all(key.isupper() for key in result[0].keys())  # Snowflake returns uppercase
    assert 'TABLE_CATALOG' in result[0]
    assert 'TABLE_SCHEMA' in result[0]
    assert 'TABLE_NAME' in result[0]
```

### 4. Test Against Real Snowflake (Optional)

For critical functionality, create optional integration tests that can run against a real Snowflake instance:

```python
@pytest.mark.snowflake  # Mark tests that require real Snowflake
@pytest.mark.skipif(not os.getenv('SNOWFLAKE_TEST_ACCOUNT'), 
                    reason="Snowflake credentials not configured")
def test_real_snowflake_query():
    """Test against real Snowflake to validate mock accuracy."""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_TEST_USER'),
        password=os.getenv('SNOWFLAKE_TEST_PASSWORD'),
        database='TEST_DB',
        schema='TEST_SCHEMA'
    )
    
    cur = conn.cursor(DictCursor)
    cur.execute("SELECT * FROM information_schema.tables LIMIT 1")
    result = cur.fetchone()
    
    # Validate structure
    assert all(key.isupper() for key in result.keys())
    assert 'TABLE_CATALOG' in result
    # ... more validations
```

### 5. Key Differences to Watch For

When creating mocks, be aware of these Snowflake-specific behaviors:

1. **Case Sensitivity**: Snowflake returns column names in UPPERCASE by default
2. **DictCursor**: When using `DictCursor`, results are dictionaries with uppercase keys
3. **NULL Handling**: Snowflake returns Python `None` for SQL NULL values
4. **Timestamps**: Snowflake timestamps may include timezone info depending on type
5. **Numeric Types**: Large numbers may be returned as `Decimal` objects
6. **String Trimming**: Snowflake doesn't automatically trim strings

### 6. Maintaining Mock Accuracy

1. **Regular Updates**: Update mocks when Snowflake behavior changes
2. **Version Testing**: Test against different Snowflake connector versions
3. **Schema Evolution**: Update mocks when dbt models change
4. **Document Assumptions**: Document any simplifications made in mocks

### 7. Mock Validation Checklist

- [ ] Column names match Snowflake's case convention
- [ ] Data types match Snowflake's Python type mappings
- [ ] NULL/None handling is consistent
- [ ] Result structure matches (list of dicts for DictCursor)
- [ ] Special fields (timestamps, decimals) are handled correctly
- [ ] Error scenarios match Snowflake's error messages

## Running Validation Tests

```bash
# Run only mock validation tests
pytest tests/fixtures/test_mock_validation.py -v

# Run tests against real Snowflake (requires credentials)
SNOWFLAKE_TEST_ACCOUNT=xxx pytest -m snowflake -v
```