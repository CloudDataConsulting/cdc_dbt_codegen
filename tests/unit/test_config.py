"""
Unit tests for the config module.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, mock_open
import yaml

from cdc_dbt_codegen.core.config import Config, get_config


class TestConfig:
    """Test the Config class."""
    
    def test_init_with_working_dir(self, temp_dir):
        """Test initialization with a specific working directory."""
        config = Config(working_dir=temp_dir)
        assert config.working_dir == temp_dir
    
    def test_init_without_working_dir(self):
        """Test initialization without working directory (uses cwd)."""
        config = Config()
        assert config.working_dir == Path.cwd()
    
    def test_load_dbt_project(self, mock_dbt_project):
        """Test loading dbt_project.yml."""
        config = Config(working_dir=mock_dbt_project)
        
        assert config.project_name == 'test_project'
        assert config.profile_name == 'test_profile'
        assert config.models_path == mock_dbt_project / 'models'
        assert config.seeds_path == mock_dbt_project / 'seeds'
    
    def test_get_source_databases_from_vars(self, mock_dbt_project):
        """Test getting source databases from dbt vars."""
        config = Config(working_dir=mock_dbt_project)
        
        source_dbs = config.get_source_databases()
        # The code prioritizes 'source_db' over 'source_dbs', and our mock has 'source_db' as a single item
        assert source_dbs == ['test_raw_db']
    
    def test_get_source_databases_from_env(self, temp_dir):
        """Test getting source databases from environment variable."""
        with patch.dict(os.environ, {'CDC_SOURCE_DATABASES': 'db1,db2,db3'}):
            config = Config(working_dir=temp_dir)
            source_dbs = config.get_source_databases()
            assert source_dbs == ['db1', 'db2', 'db3']
    
    def test_get_staging_path(self, mock_dbt_project):
        """Test getting staging path for a source."""
        config = Config(working_dir=mock_dbt_project)
        
        path = config.get_staging_path('sfdc')
        assert path == mock_dbt_project / 'models' / 'staging' / 'sfdc'
    
    def test_get_marts_path(self, mock_dbt_project):
        """Test getting marts path."""
        config = Config(working_dir=mock_dbt_project)
        
        # Without mart name
        path = config.get_marts_path()
        assert path == mock_dbt_project / 'models' / 'marts'
        
        # With mart name
        path = config.get_marts_path('finance')
        assert path == mock_dbt_project / 'models' / 'marts' / 'finance'
    
    def test_get_intermediate_path(self, mock_dbt_project):
        """Test getting intermediate path."""
        config = Config(working_dir=mock_dbt_project)
        
        path = config.get_intermediate_path()
        assert path == mock_dbt_project / 'models' / 'staging' / 'intermediate'
    
    def test_get_target_default(self, temp_dir):
        """Test getting default target."""
        config = Config(working_dir=temp_dir)
        assert config.get_target() == 'dev'
    
    def test_get_target_from_env(self, temp_dir):
        """Test getting target from environment."""
        with patch.dict(os.environ, {'DBT_TARGET': 'prod'}):
            config = Config(working_dir=temp_dir)
            assert config.get_target() == 'prod'
    
    def test_get_var(self, mock_dbt_project):
        """Test getting dbt variables."""
        config = Config(working_dir=mock_dbt_project)
        
        # Get existing var
        assert config.get_var('source_db') == ['test_raw_db']
        
        # Get non-existing var with default
        assert config.get_var('non_existing', 'default_value') == 'default_value'
    
    def test_get_var_from_env(self, temp_dir):
        """Test getting var from environment."""
        with patch.dict(os.environ, {'DBT_VAR_MY_VAR': 'env_value'}):
            config = Config(working_dir=temp_dir)
            assert config.get_var('my_var') == 'env_value'
    
    def test_get_profile_config(self, mock_dbt_project, mock_profiles):
        """Test getting profile configuration."""
        config = Config(working_dir=mock_dbt_project)
        # Manually set the profiles since we're mocking file loading
        config._profiles = mock_profiles
        
        # Get dev profile
        dev_config = config.get_profile_config(profile_name='test_profile', target='dev')
        assert dev_config['account'] == 'test_account'
        assert dev_config['user'] == 'test_user'
        
        # Get prod profile
        prod_config = config.get_profile_config(profile_name='test_profile', target='prod')
        assert prod_config['account'] == 'prod_account'
        assert prod_config['user'] == 'prod_user'
    
    def test_to_dict(self, mock_dbt_project):
        """Test exporting config as dictionary."""
        config = Config(working_dir=mock_dbt_project)
        
        config_dict = config.to_dict()
        assert config_dict['project_name'] == 'test_project'
        assert config_dict['profile_name'] == 'test_profile'
        assert config_dict['target'] == 'dev'
        assert 'models_path' in config_dict
        assert 'seeds_path' in config_dict


class TestGetConfig:
    """Test the get_config function."""
    
    def test_singleton_behavior(self, temp_dir):
        """Test that get_config returns the same instance."""
        config1 = get_config(temp_dir)
        config2 = get_config()
        
        assert config1 is config2
    
    def test_new_instance_with_different_dir(self, temp_dir):
        """Test that a new instance is created for different working dir."""
        config1 = get_config(temp_dir)
        
        new_dir = temp_dir / 'subdir'
        new_dir.mkdir()
        config2 = get_config(new_dir)
        
        assert config1 is not config2
        assert config1.working_dir != config2.working_dir