# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
Configuration management for CDC DBT Codegen.

This module handles loading and managing configuration from various sources
including dbt_project.yml, profiles.yml, and environment variables.
"""

import os
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class Config:
    """Manages configuration for CDC DBT Codegen."""
    
    def __init__(self, working_dir: Optional[Path] = None):
        """
        Initialize configuration.
        
        Args:
            working_dir: Working directory path (defaults to current directory)
        """
        self.working_dir = working_dir or Path.cwd()
        self._dbt_project: Optional[Dict[str, Any]] = None
        self._profiles: Optional[Dict[str, Any]] = None
        self._env_vars: Dict[str, str] = {}
        
        self._load_configs()
    
    def _load_configs(self):
        """Load configuration from various sources."""
        # Load dbt_project.yml
        project_path = self.working_dir / 'dbt_project.yml'
        if project_path.exists():
            with open(project_path, 'r') as f:
                self._dbt_project = yaml.safe_load(f)
        
        # Load profiles.yml
        profiles_path = Path.home() / '.dbt' / 'profiles.yml'
        if profiles_path.exists():
            with open(profiles_path, 'r') as f:
                self._profiles = yaml.safe_load(f)
        
        # Load relevant environment variables
        env_prefixes = ['DBT_', 'SNOWFLAKE_', 'CDC_']
        for key, value in os.environ.items():
            if any(key.startswith(prefix) for prefix in env_prefixes):
                self._env_vars[key] = value
    
    @property
    def profile_name(self) -> Optional[str]:
        """Get the dbt profile name."""
        if self._dbt_project:
            return self._dbt_project.get('profile')
        return None
    
    @property
    def project_name(self) -> Optional[str]:
        """Get the dbt project name."""
        if self._dbt_project:
            return self._dbt_project.get('name')
        return None
    
    @property
    def models_path(self) -> Path:
        """Get the models directory path."""
        if self._dbt_project and 'model-paths' in self._dbt_project:
            # dbt 1.0+ uses model-paths
            paths = self._dbt_project['model-paths']
            if isinstance(paths, list) and paths:
                return self.working_dir / paths[0]
        return self.working_dir / 'models'
    
    @property
    def seeds_path(self) -> Path:
        """Get the seeds directory path."""
        if self._dbt_project and 'seed-paths' in self._dbt_project:
            paths = self._dbt_project['seed-paths']
            if isinstance(paths, list) and paths:
                return self.working_dir / paths[0]
        return self.working_dir / 'seeds'
    
    def get_staging_path(self, source_name: str) -> Path:
        """
        Get the staging directory path for a specific source.
        
        Args:
            source_name: Name of the source system
            
        Returns:
            Path to the staging directory
        """
        return self.models_path / 'staging' / source_name
    
    def get_marts_path(self, mart_name: Optional[str] = None) -> Path:
        """
        Get the marts directory path.
        
        Args:
            mart_name: Optional specific mart name
            
        Returns:
            Path to the marts directory
        """
        marts_path = self.models_path / 'marts'
        if mart_name:
            return marts_path / mart_name
        return marts_path
    
    def get_intermediate_path(self) -> Path:
        """Get the intermediate models directory path."""
        return self.models_path / 'staging' / 'intermediate'
    
    def get_source_databases(self) -> List[str]:
        """
        Get the list of source databases from configuration.
        
        Returns:
            List of source database names
        """
        # Check for vars in dbt_project.yml
        if self._dbt_project and 'vars' in self._dbt_project:
            vars_config = self._dbt_project['vars']
            if isinstance(vars_config, dict):
                # Could be at root level or under project name
                source_dbs = vars_config.get('source_db') or vars_config.get('source_dbs')
                if not source_dbs and self.project_name:
                    project_vars = vars_config.get(self.project_name, {})
                    source_dbs = project_vars.get('source_db') or project_vars.get('source_dbs')
                
                if source_dbs:
                    return source_dbs if isinstance(source_dbs, list) else [source_dbs]
        
        # Check environment variable
        env_dbs = self._env_vars.get('CDC_SOURCE_DATABASES', '').strip()
        if env_dbs:
            return [db.strip() for db in env_dbs.split(',')]
        
        # Default empty list
        return []
    
    def get_target(self) -> str:
        """Get the target environment."""
        return self._env_vars.get('DBT_TARGET', 'dev')
    
    def get_profile_config(self, profile_name: Optional[str] = None, 
                          target: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific profile and target.
        
        Args:
            profile_name: Profile name (defaults to project profile)
            target: Target name (defaults to DBT_TARGET or 'dev')
            
        Returns:
            Profile configuration dictionary
        """
        profile_name = profile_name or self.profile_name
        target = target or self.get_target()
        
        if not self._profiles or not profile_name:
            return {}
        
        profile = self._profiles.get(profile_name, {})
        outputs = profile.get('outputs', {})
        return outputs.get(target, {})
    
    def get_var(self, var_name: str, default: Any = None) -> Any:
        """
        Get a dbt variable value.
        
        Args:
            var_name: Variable name
            default: Default value if not found
            
        Returns:
            Variable value or default
        """
        if self._dbt_project and 'vars' in self._dbt_project:
            vars_config = self._dbt_project['vars']
            if isinstance(vars_config, dict):
                # Check root level
                if var_name in vars_config:
                    return vars_config[var_name]
                # Check project level
                if self.project_name:
                    project_vars = vars_config.get(self.project_name, {})
                    if var_name in project_vars:
                        return project_vars[var_name]
        
        # Check environment variables
        env_var = f"DBT_VAR_{var_name.upper()}"
        if env_var in self._env_vars:
            return self._env_vars[env_var]
        
        return default
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Export configuration as a dictionary.
        
        Returns:
            Configuration dictionary
        """
        return {
            'working_dir': str(self.working_dir),
            'profile_name': self.profile_name,
            'project_name': self.project_name,
            'models_path': str(self.models_path),
            'seeds_path': str(self.seeds_path),
            'target': self.get_target(),
            'source_databases': self.get_source_databases(),
        }


# Singleton instance
_config: Optional[Config] = None


def get_config(working_dir: Optional[Path] = None) -> Config:
    """
    Get the configuration instance.
    
    Args:
        working_dir: Working directory path
        
    Returns:
        Configuration instance
    """
    global _config
    if _config is None or (working_dir and working_dir != _config.working_dir):
        _config = Config(working_dir)
    return _config