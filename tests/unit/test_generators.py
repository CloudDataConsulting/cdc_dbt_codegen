"""
Unit tests for the generators module.
"""

import pytest
from unittest.mock import Mock, patch, call
from pathlib import Path

from cdc_dbt_codegen.core.generators import StageGenerator, DimensionalGenerator, SourceLister
from cdc_dbt_codegen.core.config import Config
from cdc_dbt_codegen.core.connection import ConnectionManager


class TestStageGenerator:
    """Test the StageGenerator class."""
    
    def test_init(self):
        """Test initialization."""
        config = Mock(spec=Config)
        conn_manager = Mock(spec=ConnectionManager)
        
        generator = StageGenerator(config, conn_manager)
        assert generator.config == config
        assert generator.conn_manager == conn_manager
        assert generator.schema_suffix == "_DW_UTIL"
    
    def test_generate_with_source_filter(self, mock_snowflake_connection, mock_source_yml_data, 
                                        mock_staging_sql_data, mock_staging_yml_data):
        """Test generating files for a specific source."""
        config = Mock(spec=Config)
        config.get_staging_path.return_value = Path('/test/models/staging/sfdc')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        # Mock cursor responses
        cursor = mock_snowflake_connection.cursor.return_value
        # Configure cursor to return DictCursor behavior
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        
        # Set up different responses for different queries
        cursor.fetchall.side_effect = [mock_source_yml_data]
        
        # Make cursor iterable for direct iteration (for row in cur:)
        cursor.__iter__ = Mock(side_effect=[
            iter(mock_staging_sql_data),  # First iteration for SQL files
            iter(mock_staging_yml_data)   # Second iteration for YML files
        ])
        
        generator = StageGenerator(config, conn_manager)
        
        with patch('builtins.open', create=True) as mock_open:
            with patch('pathlib.Path.mkdir'):
                files = generator.generate(
                    source_name='sfdc',
                    generate_sql=True,
                    generate_yml=True,
                    dry_run=False
                )
        
        # Verify files were generated
        assert len(files) > 0
        assert any('src_sfdc.yml' in f for f in files)
        assert any('stg_sfdc__account.sql' in f for f in files)
        assert any('stg_sfdc__account.yml' in f for f in files)
    
    def test_generate_dry_run(self, mock_snowflake_connection, mock_source_yml_data):
        """Test dry run mode doesn't create files."""
        config = Mock(spec=Config)
        config.get_staging_path.return_value = Path('/test/models/staging/sfdc')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        cursor.fetchall.side_effect = [mock_source_yml_data]
        # Make cursor return empty list when iterated
        cursor.__iter__ = Mock(return_value=iter([]))
        
        generator = StageGenerator(config, conn_manager)
        
        # In dry run, open should not be called
        with patch('builtins.open', create=True) as mock_open:
            files = generator.generate(dry_run=True)
            mock_open.assert_not_called()
        
        assert len(files) > 0  # Files are listed but not created
    
    def test_generate_sql_only(self, mock_snowflake_connection, mock_source_yml_data, 
                              mock_staging_sql_data):
        """Test generating only SQL files."""
        config = Mock(spec=Config)
        config.get_staging_path.return_value = Path('/test/models/staging/sfdc')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        cursor.fetchall.side_effect = [mock_source_yml_data]
        # Only return SQL data when cursor is iterated
        cursor.__iter__ = Mock(return_value=iter(mock_staging_sql_data))
        
        generator = StageGenerator(config, conn_manager)
        
        with patch('builtins.open', create=True):
            with patch('pathlib.Path.mkdir'):
                files = generator.generate(
                    source_name='sfdc',
                    generate_sql=True,
                    generate_yml=False,
                    dry_run=False
                )
        
        # Should not have YML files
        assert not any('.yml' in f for f in files if 'src_' not in f)
        assert any('.sql' in f for f in files)
    
    def test_generate_with_table_filter(self, mock_snowflake_connection, mock_source_yml_data):
        """Test generating files for a specific table."""
        config = Mock(spec=Config)
        config.get_staging_path.return_value = Path('/test/models/staging/sfdc')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        cursor.fetchall.return_value = mock_source_yml_data
        # Make cursor iterable
        cursor.__iter__ = Mock(return_value=iter([]))
        
        generator = StageGenerator(config, conn_manager)
        
        with patch('builtins.open', create=True):
            with patch('pathlib.Path.mkdir'):
                generator.generate(
                    source_name='sfdc',
                    table_name='account',
                    generate_sql=True,
                    dry_run=False
                )
        
        # Verify table filter was applied in SQL query
        calls = cursor.execute.call_args_list
        sql_query = None
        for call in calls:
            if 'gen_stg_sql' in str(call):
                sql_query = call[0][0]
                break
        
        assert sql_query is not None
        assert 'account' in sql_query.lower()


class TestDimensionalGenerator:
    """Test the DimensionalGenerator class."""
    
    def test_init(self):
        """Test initialization."""
        config = Mock(spec=Config)
        conn_manager = Mock(spec=ConnectionManager)
        
        generator = DimensionalGenerator(config, conn_manager)
        assert generator.config == config
        assert generator.conn_manager == conn_manager
        assert generator.schema_suffix == "_DW_UTIL"
    
    def test_generate_dimensional_yml(self, mock_snowflake_connection):
        """Test generating dimensional YAML files."""
        config = Mock(spec=Config)
        config.get_intermediate_path.return_value = Path('/test/models/intermediate')
        config.get_marts_path.return_value = Path('/test/models/marts/base')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        # Mock dimensional data
        dim_data = [
            {
                'TABLE_NAME': 'dim_customer',
                'YML_TEXT': 'version: 2\n\nmodels:\n  - name: dim_customer',
                'TABLE_TYPE': 'dim'
            },
            {
                'TABLE_NAME': 'int_orders_grouped',
                'YML_TEXT': 'version: 2\n\nmodels:\n  - name: int_orders_grouped',
                'TABLE_TYPE': 'int'
            }
        ]
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        # Cursor iteration returns the data directly
        cursor.__iter__ = Mock(return_value=iter(dim_data))
        
        generator = DimensionalGenerator(config, conn_manager)
        
        with patch('builtins.open', create=True):
            with patch('pathlib.Path.mkdir'):
                files = generator.generate(dry_run=False)
        
        assert len(files) == 2
        assert any('dim_customer.yml' in f for f in files)
        assert any('int_orders_grouped.yml' in f for f in files)
    
    def test_generate_with_filters(self, mock_snowflake_connection):
        """Test generating with schema and table filters."""
        config = Mock(spec=Config)
        config.get_marts_path.return_value = Path('/test/models/marts/base')
        
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.get_schema_with_suffix.return_value = 'test_schema_DW_UTIL'
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        # Empty iteration
        cursor.__iter__ = Mock(return_value=iter([]))
        
        generator = DimensionalGenerator(config, conn_manager)
        
        generator.generate(
            database='test_db',
            schema='test_schema',
            table_name='dim_customer',
            dry_run=True
        )
        
        # Verify filters were applied in query
        call_args = cursor.execute.call_args[0][0]
        assert 'test_schema' in call_args
        assert 'dim_customer' in call_args


class TestSourceLister:
    """Test the SourceLister class."""
    
    def test_list_sources(self, mock_snowflake_connection, mock_source_config):
        """Test listing configured sources."""
        conn_manager = Mock(spec=ConnectionManager)
        conn_manager.__enter__ = Mock(return_value=mock_snowflake_connection)
        conn_manager.__exit__ = Mock(return_value=None)
        
        cursor = mock_snowflake_connection.cursor.return_value
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=None)
        cursor.fetchall.return_value = mock_source_config
        
        lister = SourceLister(conn_manager)
        sources = lister.list_sources()
        
        assert len(sources) == 2
        assert sources[0]['SOURCE_NAME'] == 'sfdc'
        assert sources[1]['SOURCE_NAME'] == 'hubspot'
        
        # Verify correct query was executed
        cursor.execute.assert_called_once()
        query = cursor.execute.call_args[0][0]
        assert 'code_gen_config' in query
        assert 'ORDER BY source_name' in query