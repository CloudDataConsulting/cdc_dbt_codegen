"""
Integration tests for the CLI module.
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import argparse

from cdc_dbt_codegen.cli import (
    create_parser, 
    handle_stage_command,
    handle_dimensional_command,
    handle_list_sources_command,
    main
)


class TestCreateParser:
    """Test the argument parser creation."""
    
    def test_create_parser(self):
        """Test that parser is created with all commands."""
        parser = create_parser()
        
        # Test parsing stage command
        args = parser.parse_args(['stage', '--all', '--source', 'sfdc'])
        assert args.command == 'stage'
        assert args.all is True
        assert args.source == 'sfdc'
        
        # Test parsing dimensional command
        args = parser.parse_args(['dimensional', '--database', 'test_db', '--schema', 'test_schema'])
        assert args.command == 'dimensional'
        assert args.database == 'test_db'
        assert args.schema == 'test_schema'
        
        # Test parsing list-sources command
        args = parser.parse_args(['list-sources'])
        assert args.command == 'list-sources'
    
    def test_parser_global_options(self):
        """Test global options."""
        parser = create_parser()
        
        args = parser.parse_args(['--profile', 'my_profile', '--target', 'prod', 'list-sources'])
        assert args.profile == 'my_profile'
        assert args.target == 'prod'
    
    def test_parser_help(self, capsys):
        """Test help output."""
        parser = create_parser()
        
        with pytest.raises(SystemExit):
            parser.parse_args(['--help'])
        
        captured = capsys.readouterr()
        assert 'CDC DBT Codegen' in captured.out
        assert 'stage' in captured.out
        assert 'dimensional' in captured.out
        assert 'list-sources' in captured.out


class TestHandleStageCommand:
    """Test the handle_stage_command function."""
    
    def test_stage_command_validation_error(self):
        """Test validation errors."""
        args = Mock()
        args.all = False
        args.sql = False
        args.yml = False
        args.table = 'test_table'
        args.source = None
        
        config = Mock()
        conn_manager = Mock()
        
        # Should fail - no file type specified
        result = handle_stage_command(args, config, conn_manager)
        assert result == 1
        
        # Should fail - table without source
        args.sql = True
        result = handle_stage_command(args, config, conn_manager)
        assert result == 1
    
    @patch('cdc_dbt_codegen.cli.StageGenerator')
    def test_stage_command_success(self, mock_generator_class):
        """Test successful stage command execution."""
        args = Mock()
        args.all = True
        args.sql = True
        args.yml = True
        args.source = 'sfdc'
        args.table = None
        args.dry_run = False
        args.backup = True
        
        config = Mock()
        conn_manager = Mock()
        
        # Mock generator
        mock_generator = Mock()
        mock_generator.generate.return_value = [
            '/test/models/staging/sfdc/src_sfdc.yml',
            '/test/models/staging/sfdc/stg_sfdc__account.sql',
            '/test/models/staging/sfdc/stg_sfdc__account.yml'
        ]
        mock_generator_class.return_value = mock_generator
        
        result = handle_stage_command(args, config, conn_manager)
        
        assert result == 0
        mock_generator.generate.assert_called_once_with(
            source_name='sfdc',
            table_name=None,
            generate_sql=True,
            generate_yml=True,
            dry_run=False
        )
    
    @patch('cdc_dbt_codegen.cli.StageGenerator')
    def test_stage_command_error(self, mock_generator_class):
        """Test stage command with error."""
        args = Mock()
        args.all = True
        args.source = None
        args.table = None
        args.dry_run = False
        
        config = Mock()
        conn_manager = Mock()
        
        # Mock generator to raise error
        mock_generator = Mock()
        mock_generator.generate.side_effect = Exception("Test error")
        mock_generator_class.return_value = mock_generator
        
        result = handle_stage_command(args, config, conn_manager)
        assert result == 1


class TestHandleDimensionalCommand:
    """Test the handle_dimensional_command function."""
    
    @patch('cdc_dbt_codegen.cli.DimensionalGenerator')
    def test_dimensional_command_success(self, mock_generator_class):
        """Test successful dimensional command execution."""
        args = Mock()
        args.database = 'test_db'
        args.schema = 'test_schema'
        args.table = None
        args.dry_run = False
        args.backup = True
        
        config = Mock()
        conn_manager = Mock()
        
        # Mock generator
        mock_generator = Mock()
        mock_generator.generate.return_value = [
            '/test/models/marts/base/dim_customer.yml',
            '/test/models/marts/base/fct_orders.yml'
        ]
        mock_generator_class.return_value = mock_generator
        
        result = handle_dimensional_command(args, config, conn_manager)
        
        assert result == 0
        mock_generator.generate.assert_called_once_with(
            database='test_db',
            schema='test_schema',
            table_name=None,
            dry_run=False
        )


class TestHandleListSourcesCommand:
    """Test the handle_list_sources_command function."""
    
    @patch('cdc_dbt_codegen.cli.SourceLister')
    def test_list_sources_success(self, mock_lister_class, capsys):
        """Test successful list sources command."""
        conn_manager = Mock()
        
        # Mock lister
        mock_lister = Mock()
        mock_lister.list_sources.return_value = [
            {
                'SOURCE_NAME': 'sfdc',
                'DESCRIPTION': 'Salesforce data',
                'DATABASE': 'PRD_RAW_DB',
                'SCHEMA': 'SFDC',
                'LOADER': 'Fivetran',
                'GENERATE_FLAG': 'Y'
            }
        ]
        mock_lister_class.return_value = mock_lister
        
        result = handle_list_sources_command(conn_manager)
        
        assert result == 0
        captured = capsys.readouterr()
        assert 'sfdc' in captured.out
        assert 'Salesforce data' in captured.out
        assert '[ACTIVE]' in captured.out
    
    @patch('cdc_dbt_codegen.cli.SourceLister')
    def test_list_sources_empty(self, mock_lister_class, capsys):
        """Test list sources with no results."""
        conn_manager = Mock()
        
        # Mock lister with empty results
        mock_lister = Mock()
        mock_lister.list_sources.return_value = []
        mock_lister_class.return_value = mock_lister
        
        result = handle_list_sources_command(conn_manager)
        
        assert result == 0
        captured = capsys.readouterr()
        assert 'No sources found' in captured.out
        assert 'dbt seed' in captured.out


class TestMain:
    """Test the main function."""
    
    def test_main_no_args(self):
        """Test main with no arguments shows help."""
        with patch('sys.argv', ['cdc-dbt-codegen']):
            result = main()
            assert result == 1
    
    @patch('cdc_dbt_codegen.cli.get_config')
    @patch('cdc_dbt_codegen.cli.ConnectionManager')
    @patch('cdc_dbt_codegen.cli.handle_stage_command')
    def test_main_stage_command(self, mock_handle_stage, mock_conn_class, mock_get_config):
        """Test main with stage command."""
        mock_config = Mock()
        mock_get_config.return_value = mock_config
        
        mock_conn_manager = Mock()
        mock_conn_class.return_value = mock_conn_manager
        
        mock_handle_stage.return_value = 0
        
        with patch('sys.argv', ['cdc-dbt-codegen', 'stage', '--all']):
            result = main()
            
        assert result == 0
        mock_handle_stage.assert_called_once()
        mock_conn_manager.close.assert_called_once()
    
    @patch('cdc_dbt_codegen.cli.get_config')
    def test_main_init_error(self, mock_get_config):
        """Test main with initialization error."""
        mock_get_config.side_effect = Exception("Config error")
        
        with patch('sys.argv', ['cdc-dbt-codegen', 'list-sources']):
            result = main()
            
        assert result == 1