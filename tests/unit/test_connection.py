"""
Unit tests for the connection module.
"""

import pytest
import os
from unittest.mock import Mock, patch, mock_open
import yaml
import snowflake.connector

from cdc_dbt_codegen.core.connection import ConnectionManager, get_snowflake_connection


class TestConnectionManager:
    """Test the ConnectionManager class."""
    
    def test_init(self):
        """Test initialization."""
        manager = ConnectionManager(profile_name='test_profile', target='prod')
        assert manager.profile_name == 'test_profile'
        assert manager.target == 'prod'
    
    def test_init_with_defaults(self):
        """Test initialization with defaults."""
        manager = ConnectionManager()
        assert manager.profile_name is None
        assert manager.target == 'dev'  # Default target
    
    def test_init_with_env_target(self):
        """Test initialization with DBT_TARGET env var."""
        with patch.dict(os.environ, {'DBT_TARGET': 'staging'}):
            manager = ConnectionManager()
            assert manager.target == 'staging'
    
    @patch('snowflake.connector.connect')
    def test_get_connection_creates_new(self, mock_connect):
        """Test that get_connection creates a new connection."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        mock_connect.return_value = mock_conn
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_pass',
            'SNOWFLAKE_ROLE': 'test_role',
            'SNOWFLAKE_WAREHOUSE': 'test_wh',
            'SNOWFLAKE_DATABASE': 'test_db'
        }):
            manager = ConnectionManager()
            conn = manager.get_connection()
            
            assert conn == mock_conn
            mock_connect.assert_called_once_with(
                account='test_account',
                user='test_user',
                password='test_pass',
                role='test_role',
                warehouse='test_wh',
                database='test_db'
            )
    
    @patch('snowflake.connector.connect')
    def test_get_connection_reuses_existing(self, mock_connect):
        """Test that get_connection reuses existing connection."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        mock_connect.return_value = mock_conn
        
        with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test'}):
            manager = ConnectionManager()
            conn1 = manager.get_connection()
            conn2 = manager.get_connection()
            
            assert conn1 is conn2
            assert mock_connect.call_count == 1
    
    def test_close(self):
        """Test closing connection."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        
        manager = ConnectionManager()
        manager._connection = mock_conn
        
        manager.close()
        
        mock_conn.close.assert_called_once()
        assert manager._connection is None
    
    def test_get_env_config(self):
        """Test getting configuration from environment variables."""
        with patch.dict(os.environ, {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USER': 'env_user',
            'SNOWFLAKE_PASSWORD': 'env_password',
            'SNOWFLAKE_ROLE': 'env_role',
            'SNOWFLAKE_WAREHOUSE': 'env_warehouse',
            'SNOWFLAKE_DATABASE': 'env_database'
        }):
            manager = ConnectionManager()
            config = manager._get_env_config()
            
            assert config['account'] == 'env_account'
            assert config['user'] == 'env_user'
            assert config['password'] == 'env_password'
            assert config['role'] == 'env_role'
            assert config['warehouse'] == 'env_warehouse'
            assert config['database'] == 'env_database'
    
    def test_get_env_config_with_private_key(self):
        """Test getting configuration with private key auth."""
        with patch.dict(os.environ, {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USER': 'env_user',
            'SNOWFLAKE_PRIVATE_KEY_PATH': '/path/to/key',
            'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': 'passphrase'
        }):
            manager = ConnectionManager()
            config = manager._get_env_config()
            
            assert 'password' not in config
            assert config['private_key_file'] == '/path/to/key'
            assert config['private_key_file_pwd'] == 'passphrase'
    
    def test_get_env_config_with_authenticator(self):
        """Test getting configuration with external authenticator."""
        with patch.dict(os.environ, {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USER': 'env_user',
            'SNOWFLAKE_AUTHENTICATOR': 'externalbrowser'
        }):
            manager = ConnectionManager()
            config = manager._get_env_config()
            
            assert 'password' not in config
            assert config['authenticator'] == 'externalbrowser'
    
    def test_get_dbt_profile_config(self, mock_profiles):
        """Test getting configuration from dbt profiles."""
        profiles_content = yaml.dump(mock_profiles)
        
        with patch('builtins.open', mock_open(read_data=profiles_content)):
            manager = ConnectionManager(profile_name='test_profile', target='dev')
            config = manager._get_dbt_profile_config()
            
            assert config['account'] == 'test_account'
            assert config['user'] == 'test_user'
            assert config['password'] == 'test_password'
            assert config['role'] == 'test_role'
            assert config['warehouse'] == 'test_warehouse'
            assert config['database'] == 'test_database'
            assert config['schema'] == 'test_schema'
    
    def test_get_dbt_profile_config_no_profiles_file(self):
        """Test error when profiles.yml doesn't exist."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = ConnectionManager(profile_name='test_profile')
            
            with pytest.raises(FileNotFoundError):
                manager._get_dbt_profile_config()
    
    def test_get_dbt_profile_config_invalid_profile(self, mock_profiles):
        """Test error when profile doesn't exist."""
        profiles_content = yaml.dump(mock_profiles)
        
        with patch('builtins.open', mock_open(read_data=profiles_content)):
            manager = ConnectionManager(profile_name='invalid_profile')
            
            with pytest.raises(ValueError, match="Profile 'invalid_profile' not found"):
                manager._get_dbt_profile_config()
    
    def test_get_schema_with_suffix(self):
        """Test getting schema name with suffix."""
        with patch.dict(os.environ, {
            'SNOWFLAKE_ACCOUNT': 'test',
            'SNOWFLAKE_USER': 'test',
            'SNOWFLAKE_DATABASE': 'test_db'
        }):
            manager = ConnectionManager()
            manager._config = {'schema': 'base_schema'}
            
            result = manager.get_schema_with_suffix('_DW_UTIL')
            assert result == 'base_schema_DW_UTIL'
    
    def test_context_manager(self):
        """Test using ConnectionManager as context manager."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        
        with patch('snowflake.connector.connect', return_value=mock_conn):
            with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test'}):
                manager = ConnectionManager()
                
                with manager as conn:
                    assert conn == mock_conn
                
                # Connection should be closed after exiting context
                mock_conn.close.assert_called_once()


class TestGetSnowflakeConnection:
    """Test the get_snowflake_connection function."""
    
    @patch('snowflake.connector.connect')
    def test_creates_default_manager(self, mock_connect):
        """Test that function creates a default connection manager."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        mock_connect.return_value = mock_conn
        
        with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test'}):
            conn = get_snowflake_connection()
            assert conn == mock_conn
    
    @patch('snowflake.connector.connect')
    @patch('cdc_dbt_codegen.core.connection._default_manager', None)  # Reset singleton
    def test_reuses_default_manager(self, mock_connect):
        """Test that function reuses the same manager."""
        mock_conn = Mock()
        mock_conn.is_closed.return_value = False
        mock_connect.return_value = mock_conn
        
        with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test'}):
            # First call creates manager and connection
            conn1 = get_snowflake_connection()
            # Second call reuses existing manager and connection
            conn2 = get_snowflake_connection()
            
            assert conn1 is conn2
            assert mock_connect.call_count == 1