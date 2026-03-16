# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
Snowflake connection management for CDC DBT Codegen.

This module handles all Snowflake database connections, supporting multiple
authentication methods and configuration sources.
"""

import os
import logging
from typing import Optional, Dict, Any
from pathlib import Path
import snowflake.connector
from snowflake.connector import SnowflakeConnection
import yaml

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages Snowflake database connections with support for multiple auth methods."""
    
    def __init__(self, profile_name: Optional[str] = None, target: Optional[str] = None):
        """
        Initialize the connection manager.
        
        Args:
            profile_name: The dbt profile name to use
            target: The target environment (dev, prod, etc.)
        """
        self.profile_name = profile_name
        self.target = target or os.getenv('DBT_TARGET', 'dev')
        self._connection: Optional[SnowflakeConnection] = None
        self._config: Optional[Dict[str, Any]] = None
    
    def get_connection(self) -> SnowflakeConnection:
        """
        Get or create a Snowflake connection.
        
        Returns:
            Active Snowflake connection
        """
        if self._connection and not self._connection.is_closed():
            return self._connection
        
        config = self._get_connection_config()
        self._connection = self._create_connection(config)
        return self._connection
    
    def close(self):
        """Close the current connection if open."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._connection = None
    
    def _get_connection_config(self) -> Dict[str, Any]:
        """
        Get connection configuration from various sources.
        
        Priority order:
        1. Environment variables
        2. dbt profiles.yml
        3. Direct configuration
        
        Returns:
            Dictionary with connection parameters
        """
        if self._config:
            return self._config
        
        # Try environment variables first
        if os.getenv('SNOWFLAKE_ACCOUNT'):
            self._config = self._get_env_config()
        else:
            # Try dbt profiles
            self._config = self._get_dbt_profile_config()
        
        return self._config
    
    def _get_env_config(self) -> Dict[str, Any]:
        """Get configuration from environment variables."""
        config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
        }
        
        # Handle authentication
        if os.getenv('SNOWFLAKE_PASSWORD'):
            config['password'] = os.getenv('SNOWFLAKE_PASSWORD')
        elif os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
            config['private_key_file'] = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
                config['private_key_file_pwd'] = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        elif os.getenv('SNOWFLAKE_AUTHENTICATOR'):
            config['authenticator'] = os.getenv('SNOWFLAKE_AUTHENTICATOR')
        
        return config
    
    def _get_dbt_profile_config(self) -> Dict[str, Any]:
        """Get configuration from dbt profiles.yml."""
        profiles_path = Path.home() / '.dbt' / 'profiles.yml'
        
        if not profiles_path.exists():
            raise FileNotFoundError(f"No dbt profiles found at {profiles_path}")
        
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
        
        # Get profile name from dbt_project.yml if not specified
        if not self.profile_name:
            project_path = Path.cwd() / 'dbt_project.yml'
            if project_path.exists():
                with open(project_path, 'r') as f:
                    project = yaml.safe_load(f)
                    self.profile_name = project.get('profile')
        
        if not self.profile_name:
            raise ValueError("No profile name specified and could not determine from dbt_project.yml")
        
        if self.profile_name not in profiles:
            raise ValueError(f"Profile '{self.profile_name}' not found in profiles.yml")
        
        profile = profiles[self.profile_name]
        if 'outputs' not in profile or self.target not in profile['outputs']:
            raise ValueError(f"Target '{self.target}' not found in profile '{self.profile_name}'")
        
        output = profile['outputs'][self.target]
        
        # Map dbt config to Snowflake connector config
        config = {
            'account': output['account'],
            'user': output['user'],
            'role': output.get('role'),
            'warehouse': output.get('warehouse'),
            'database': output.get('database'),
            'schema': output.get('schema'),
        }
        
        # Handle authentication
        if 'password' in output:
            config['password'] = output['password']
        elif 'private_key_path' in output:
            config['private_key_file'] = output['private_key_path']
            if 'private_key_passphrase' in output:
                config['private_key_file_pwd'] = output['private_key_passphrase']
        elif 'authenticator' in output:
            config['authenticator'] = output['authenticator']
        
        return config
    
    def _create_connection(self, config: Dict[str, Any]) -> SnowflakeConnection:
        """Create a Snowflake connection with the given configuration."""
        # Remove None values
        config = {k: v for k, v in config.items() if v is not None}
        
        logger.info(f"Connecting to Snowflake account: {config.get('account')}")
        
        try:
            return snowflake.connector.connect(**config)
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def get_schema_with_suffix(self, suffix: str) -> str:
        """
        Get the schema name with a suffix (e.g., _DW_UTIL).
        
        Args:
            suffix: The suffix to append to the schema name
            
        Returns:
            Schema name with suffix
        """
        config = self._get_connection_config()
        base_schema = config.get('schema', '')
        return f"{base_schema}{suffix}"
    
    def __enter__(self):
        """Context manager entry."""
        return self.get_connection()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Singleton instance for backward compatibility
_default_manager: Optional[ConnectionManager] = None


def get_snowflake_connection() -> SnowflakeConnection:
    """
    Get a Snowflake connection using the default connection manager.
    
    This function maintains backward compatibility with existing code.
    
    Returns:
        Active Snowflake connection
    """
    global _default_manager
    if _default_manager is None:
        _default_manager = ConnectionManager()
    return _default_manager.get_connection()


def close_connection():
    """Close the default connection if open."""
    global _default_manager
    if _default_manager:
        _default_manager.close()
        _default_manager = None