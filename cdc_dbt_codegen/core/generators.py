# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
Code generation logic for CDC DBT Codegen.

This module contains the core logic for generating dbt staging and
non-staging files from Snowflake metadata.
"""

import os
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from snowflake.connector import DictCursor

from .connection import ConnectionManager
from .config import Config

logger = logging.getLogger(__name__)


class StageGenerator:
    """Generates staging layer files (SQL and YAML) from source metadata."""
    
    def __init__(self, config: Config, connection_manager: ConnectionManager):
        """
        Initialize the stage generator.
        
        Args:
            config: Configuration instance
            connection_manager: Connection manager instance
        """
        self.config = config
        self.conn_manager = connection_manager
        self.schema_suffix = "_DW_UTIL"
    
    def generate(self, source_name: Optional[str] = None, 
                 table_name: Optional[str] = None,
                 generate_sql: bool = True,
                 generate_yml: bool = True,
                 dry_run: bool = False) -> List[str]:
        """
        Generate staging files.
        
        Args:
            source_name: Optional source to filter by
            table_name: Optional table to filter by
            generate_sql: Whether to generate SQL files
            generate_yml: Whether to generate YAML files
            dry_run: If True, don't write files
            
        Returns:
            List of generated file paths
        """
        generated_files = []
        
        with self.conn_manager as conn:
            schema = self.conn_manager.get_schema_with_suffix(self.schema_suffix)
            
            # Get sources to process
            sources = self._get_sources(conn, schema, source_name)
            
            for source in sources:
                src_name = source['SOURCE_NAME']
                
                # Generate source YAML
                if generate_yml:
                    yml_content = source['YML_TEXT']
                    file_path = self._write_source_yml(src_name, yml_content, dry_run)
                    if file_path:
                        generated_files.append(file_path)
                
                # Generate table files
                table_files = self._generate_table_files(
                    conn, schema, src_name, table_name,
                    generate_sql, generate_yml, dry_run
                )
                generated_files.extend(table_files)
        
        return generated_files
    
    def _get_sources(self, conn, schema: str, source_name: Optional[str]) -> List[Dict]:
        """Get sources to process from the database."""
        cur = conn.cursor(DictCursor)
        try:
            query = f"""
                SELECT source_name, yml_text 
                FROM {conn.database}.{schema}.gen_stg_src_name_yml
            """
            if source_name:
                query += f" WHERE source_name = '{source_name}'"
            
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()
    
    def _write_source_yml(self, source_name: str, yml_text: str, dry_run: bool) -> Optional[str]:
        """Write source YAML file."""
        folder_path = self.config.get_staging_path(source_name)
        file_path = folder_path / f"src_{source_name}.yml"
        
        if dry_run:
            logger.info(f"[DRY RUN] Would create: {file_path}")
            return str(file_path)
        
        # Create directory if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(yml_text + '\n')
        
        logger.info(f"Created: {file_path}")
        return str(file_path)
    
    def _generate_table_files(self, conn, schema: str, source_name: str,
                             table_name: Optional[str],
                             generate_sql: bool, generate_yml: bool,
                             dry_run: bool) -> List[str]:
        """Generate SQL and YAML files for tables."""
        generated_files = []
        cur = conn.cursor(DictCursor)
        
        try:
            # Generate SQL files
            if generate_sql:
                table_filter = ""
                if table_name:
                    table_filter = f" AND target_name LIKE '%{source_name}__{table_name}'"
                
                query = f"""
                    SELECT target_name, sql_text 
                    FROM {conn.database}.{schema}.gen_stg_sql 
                    WHERE source_name = '{source_name.lower()}'{table_filter}
                """
                
                cur.execute(query)
                for row in cur:
                    target = row['TARGET_NAME']
                    sql_text = row['SQL_TEXT']
                    file_path = self._write_table_file(
                        source_name, target, sql_text, 'sql', dry_run
                    )
                    if file_path:
                        generated_files.append(file_path)
            
            # Generate YAML files
            if generate_yml:
                yml_filter = f" AND source_name = '{source_name}'"
                if table_name:
                    yml_filter += f" AND table_name = '{table_name}'"
                
                query = f"""
                    SELECT target_name, yml_text, table_name 
                    FROM {conn.database}.{schema}.gen_stg_yml 
                    WHERE source_name = '{source_name.lower()}'{yml_filter}
                """
                
                cur.execute(query)
                for row in cur:
                    target = row['TARGET_NAME']
                    yml_text = row['YML_TEXT']
                    file_path = self._write_table_file(
                        source_name, target, yml_text, 'yml', dry_run
                    )
                    if file_path:
                        generated_files.append(file_path)
        
        finally:
            cur.close()
        
        return generated_files
    
    def _write_table_file(self, source_name: str, target_name: str,
                         content: str, extension: str, dry_run: bool) -> Optional[str]:
        """Write a table SQL or YAML file."""
        folder_path = self.config.get_staging_path(source_name)
        file_path = folder_path / f"{target_name.lower()}.{extension}"
        
        if dry_run:
            logger.info(f"[DRY RUN] Would create: {file_path}")
            return str(file_path)
        
        # Create directory if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content + '\n')
        
        logger.info(f"Created: {file_path}")
        return str(file_path)


class DimensionalGenerator:
    """Generates YAML files for dimensional and fact tables."""
    
    def __init__(self, config: Config, connection_manager: ConnectionManager):
        """
        Initialize the dimensional generator.
        
        Args:
            config: Configuration instance
            connection_manager: Connection manager instance
        """
        self.config = config
        self.conn_manager = connection_manager
        self.schema_suffix = "_DW_UTIL"
    
    def generate(self, database: Optional[str] = None,
                 schema: Optional[str] = None,
                 table_name: Optional[str] = None,
                 dry_run: bool = False) -> List[str]:
        """
        Generate dimensional YAML files.
        
        Args:
            database: Optional database to use
            schema: Optional schema to filter by
            table_name: Optional table to filter by
            dry_run: If True, don't write files
            
        Returns:
            List of generated file paths
        """
        generated_files = []
        
        with self.conn_manager as conn:
            util_schema = self.conn_manager.get_schema_with_suffix(self.schema_suffix)
            
            # Build query
            query = f"""
                SELECT table_name, yml_text, table_type
                FROM {conn.database}.{util_schema}.gen_non_stg_yml
            """
            
            filters = []
            if schema:
                filters.append(f"table_schema = '{schema}'")
            if table_name:
                filters.append(f"table_name = '{table_name}'")
            
            if filters:
                query += " WHERE " + " AND ".join(filters)
            
            cur = conn.cursor(DictCursor)
            try:
                cur.execute(query)
                for row in cur:
                    table = row['TABLE_NAME']
                    yml_text = row['YML_TEXT']
                    table_type = row.get('TABLE_TYPE', '')
                    
                    file_path = self._write_dimensional_yml(
                        table, yml_text, table_type, dry_run
                    )
                    if file_path:
                        generated_files.append(file_path)
            finally:
                cur.close()
        
        return generated_files
    
    def _write_dimensional_yml(self, table_name: str, yml_text: str,
                              table_type: str, dry_run: bool) -> Optional[str]:
        """Write dimensional YAML file."""
        # Determine output path based on table prefix
        if table_name.lower().startswith('int_'):
            folder_path = self.config.get_intermediate_path()
        else:
            # Try to determine mart from table name or use base
            folder_path = self.config.get_marts_path('base')
        
        file_path = folder_path / f"{table_name.lower()}.yml"
        
        if dry_run:
            logger.info(f"[DRY RUN] Would create: {file_path}")
            return str(file_path)
        
        # Create directory if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(yml_text + '\n')
        
        logger.info(f"Created: {file_path}")
        return str(file_path)


class SourceLister:
    """Lists available sources from configuration."""
    
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the source lister.
        
        Args:
            connection_manager: Connection manager instance
        """
        self.conn_manager = connection_manager
    
    def list_sources(self) -> List[Dict[str, str]]:
        """
        List all configured sources.
        
        Returns:
            List of source dictionaries
        """
        with self.conn_manager as conn:
            cur = conn.cursor(DictCursor)
            try:
                query = """
                SELECT DISTINCT 
                    source_name,
                    description,
                    database,
                    schema,
                    loader,
                    generate_flag
                FROM code_gen_config
                ORDER BY source_name
                """
                
                cur.execute(query)
                return cur.fetchall()
            finally:
                cur.close()