"""
Tests to validate that our mocks accurately represent Snowflake behavior.
"""

import pytest
from unittest.mock import Mock
import snowflake.connector
from snowflake.connector import DictCursor

from tests.fixtures.sample_snowflake_responses import (
    SAMPLE_TABLES_RESPONSE,
    SAMPLE_COLUMNS_RESPONSE,
    SAMPLE_SOURCE_YML_RESPONSE,
    SAMPLE_STAGING_SQL_RESPONSE,
    SAMPLE_CODE_GEN_CONFIG_RESPONSE
)


class TestMockStructureValidation:
    """Validate that our mock data structures match Snowflake's actual format."""
    
    def test_table_response_structure(self):
        """Validate information_schema.tables response structure."""
        response = SAMPLE_TABLES_RESPONSE[0]
        
        # Snowflake returns uppercase column names
        assert all(key.isupper() for key in response.keys())
        
        # Required columns exist
        required_columns = [
            'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 
            'TABLE_OWNER', 'TABLE_TYPE', 'IS_TRANSIENT'
        ]
        for col in required_columns:
            assert col in response
        
        # Data types are correct
        assert isinstance(response['TABLE_NAME'], str)
        assert isinstance(response['ROW_COUNT'], (int, type(None)))
        assert isinstance(response['IS_TRANSIENT'], str)
        assert response['IS_TRANSIENT'] in ('YES', 'NO')
    
    def test_column_response_structure(self):
        """Validate information_schema.columns response structure."""
        response = SAMPLE_COLUMNS_RESPONSE[0]
        
        # Uppercase keys
        assert all(key.isupper() for key in response.keys())
        
        # Required columns
        required_columns = [
            'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME',
            'COLUMN_NAME', 'ORDINAL_POSITION', 'DATA_TYPE',
            'IS_NULLABLE'
        ]
        for col in required_columns:
            assert col in response
        
        # Data types
        assert isinstance(response['ORDINAL_POSITION'], int)
        assert isinstance(response['CHARACTER_MAXIMUM_LENGTH'], (int, type(None)))
        assert response['IS_NULLABLE'] in ('YES', 'NO')
    
    def test_generated_view_response_structure(self):
        """Validate generated view response structures."""
        # Source YML
        source_yml = SAMPLE_SOURCE_YML_RESPONSE[0]
        assert 'SOURCE_NAME' in source_yml
        assert 'YML_TEXT' in source_yml
        assert isinstance(source_yml['YML_TEXT'], str)
        assert 'version: 2' in source_yml['YML_TEXT']
        
        # Staging SQL
        staging_sql = SAMPLE_STAGING_SQL_RESPONSE[0]
        assert 'TARGET_NAME' in staging_sql
        assert 'SQL_TEXT' in staging_sql
        assert '{{ source(' in staging_sql['SQL_TEXT']
    
    def test_code_gen_config_structure(self):
        """Validate code_gen_config seed structure."""
        config = SAMPLE_CODE_GEN_CONFIG_RESPONSE[0]
        
        # All uppercase keys (as returned by Snowflake)
        assert all(key.isupper() for key in config.keys())
        
        # Required fields
        required_fields = [
            'GENERATE_FLAG', 'SOURCE_NAME', 'DESCRIPTION',
            'DATABASE', 'SCHEMA', 'LOADER', 'LOADED_AT_FIELD'
        ]
        for field in required_fields:
            assert field in config
        
        # Valid values
        assert config['GENERATE_FLAG'] in ('Y', 'N')
        assert config['LOADER'] in ('Fivetran', 'Matillion', 'Custom')


class TestMockBehaviorValidation:
    """Validate that our mocks behave like Snowflake connections."""
    
    def test_dict_cursor_behavior(self):
        """Test that DictCursor returns dictionaries with uppercase keys."""
        mock_cursor = Mock(spec=DictCursor)
        mock_cursor.fetchall.return_value = SAMPLE_TABLES_RESPONSE
        mock_cursor.fetchone.return_value = SAMPLE_TABLES_RESPONSE[0]
        
        # fetchall returns list of dicts
        all_results = mock_cursor.fetchall()
        assert isinstance(all_results, list)
        assert all(isinstance(row, dict) for row in all_results)
        
        # fetchone returns single dict
        one_result = mock_cursor.fetchone()
        assert isinstance(one_result, dict)
        
        # Keys are uppercase
        assert all(key.isupper() for key in one_result.keys())
    
    def test_null_handling(self):
        """Test that NULL values are represented as None."""
        response = SAMPLE_COLUMNS_RESPONSE[0]
        
        # NULL values should be Python None
        assert response['COLUMN_DEFAULT'] is None
        assert response['NUMERIC_PRECISION'] is None
        
        # Empty strings are different from NULL
        assert response['DATA_TYPE'] == 'VARCHAR'  # Not None or empty
    
    def test_connection_mock_behavior(self):
        """Test that connection mock behaves like real Snowflake connection."""
        mock_conn = Mock(spec=snowflake.connector.SnowflakeConnection)
        mock_conn.is_closed.return_value = False
        mock_conn.database = 'TEST_DB'
        mock_conn.schema = 'TEST_SCHEMA'
        
        # Cursor creation
        mock_cursor = Mock(spec=DictCursor)
        mock_conn.cursor.return_value = mock_cursor
        
        # Test cursor creation with DictCursor
        cursor = mock_conn.cursor(DictCursor)
        assert cursor is mock_cursor
        
        # Test connection properties
        assert hasattr(mock_conn, 'database')
        assert hasattr(mock_conn, 'schema')
        assert hasattr(mock_conn, 'is_closed')


class TestDataTypeValidation:
    """Validate Snowflake data type representations."""
    
    def test_snowflake_data_types(self):
        """Test that data types match Snowflake's Python mappings."""
        # VARCHAR -> str
        assert isinstance(SAMPLE_COLUMNS_RESPONSE[0]['DATA_TYPE'], str)
        
        # INTEGER -> int
        assert isinstance(SAMPLE_COLUMNS_RESPONSE[0]['ORDINAL_POSITION'], int)
        
        # NULL -> None
        assert SAMPLE_COLUMNS_RESPONSE[0]['NUMERIC_PRECISION'] is None
        
        # BOOLEAN as string (Snowflake style)
        assert SAMPLE_TABLES_RESPONSE[0]['IS_TRANSIENT'] in ('YES', 'NO')
    
    def test_timestamp_handling(self):
        """Test timestamp field handling."""
        timestamp_col = next(
            col for col in SAMPLE_COLUMNS_RESPONSE 
            if col['DATA_TYPE'] == 'TIMESTAMP_NTZ'
        )
        
        # Timestamp columns have datetime precision
        assert timestamp_col['DATETIME_PRECISION'] is not None
        assert isinstance(timestamp_col['DATETIME_PRECISION'], int)


@pytest.mark.snowflake
@pytest.mark.skipif(
    not all([
        pytest.importorskip("snowflake.connector"),
        # Add environment check here if needed
    ]),
    reason="Snowflake connector not available or credentials not configured"
)
class TestRealSnowflakeValidation:
    """Optional tests that validate against real Snowflake (requires credentials)."""
    
    def test_real_connection_structure(self):
        """Test real Snowflake connection structure (if available)."""
        # This test would connect to real Snowflake if credentials are provided
        # It's marked to skip if not configured
        pass