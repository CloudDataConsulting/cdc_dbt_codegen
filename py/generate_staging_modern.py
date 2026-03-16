#!/usr/bin/env python3
"""
Modern staging file generator for CDC DBT Codegen
Supports key pair authentication and current dbt profiles

Usage:
    python generate_staging_modern.py --source asana
    python generate_staging_modern.py --source asana --dry-run
    python generate_staging_modern.py --list-sources
"""

import os
import sys
import argparse
import yaml
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class Config:
    """Configuration management for the generator"""
    
    def __init__(self):
        # Find dbt project root by looking for dbt_project.yml
        self.working_dir = Path.cwd()
        
        # Search upward for dbt_project.yml
        current = self.working_dir
        while current != current.parent:
            if (current / "dbt_project.yml").exists():
                self.working_dir = current
                break
            current = current.parent
        
        self.dbt_project_path = self.working_dir / "dbt_project.yml"
        
        if not self.dbt_project_path.exists():
            raise FileNotFoundError(f"Could not find dbt_project.yml. Please run from within a dbt project.")
        self.profiles_path = Path.home() / ".dbt" / "profiles.yml"
        self.models_path = self.working_dir / "models" / "staging"
        
        # Load dbt project config
        with open(self.dbt_project_path) as f:
            self.dbt_project = yaml.safe_load(f)
        
        # Load profiles
        with open(self.profiles_path) as f:
            self.profiles = yaml.safe_load(f)
        
        self.profile_name = self.dbt_project.get('profile')
        self.target = os.environ.get('DBT_TARGET', 'dev')
        
    def get_connection_params(self) -> Dict:
        """Get connection parameters from profiles and environment"""
        profile = self.profiles[self.profile_name]['outputs'][self.target]
        
        params = {
            'account': profile['account'],
            'user': profile.get('user', os.environ.get('DBT_USER')),
            'role': profile.get('role'),
            'database': profile.get('database'),
            'warehouse': profile.get('warehouse'),
            'schema': profile.get('schema', os.environ.get('DBT_SCHEMA'))
        }
        
        # Check for private key authentication
        if 'private_key_path' in profile or 'DBT_PRIVATE_KEY' in os.environ:
            params['private_key'] = self._get_private_key(profile)
        elif 'password' in profile:
            params['password'] = profile['password']
        else:
            # Try external browser SSO
            params['authenticator'] = 'externalbrowser'
            
        return params
    
    def _get_private_key(self, profile: Dict) -> bytes:
        """Load private key from file or environment"""
        # Try environment variable first
        if 'DBT_PRIVATE_KEY' in os.environ:
            private_key_str = os.environ['DBT_PRIVATE_KEY']
            # Handle the key string format
            if not private_key_str.startswith('-----BEGIN'):
                # It might be base64 encoded or escaped
                private_key_str = private_key_str.replace('\\n', '\n')
        else:
            # Load from file
            key_path = Path(profile['private_key_path']).expanduser()
            with open(key_path, 'rb') as key_file:
                private_key_str = key_file.read().decode('utf-8')
        
        # Get passphrase if needed
        passphrase = os.environ.get('DBT_PRIVATE_KEY_PASSPHRASE')
        if passphrase:
            passphrase = passphrase.encode()
        
        # Parse the private key
        private_key = serialization.load_pem_private_key(
            private_key_str.encode('utf-8'),
            password=passphrase,
            backend=default_backend()
        )
        
        # Convert to DER format for Snowflake
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return private_key_der


class SnowflakeConnection:
    """Manages Snowflake connection with lazy initialization"""
    
    def __init__(self, config: Config):
        self.config = config
        self._connection = None
        
    def connect(self):
        """Create connection if not exists"""
        if not self._connection:
            params = self.config.get_connection_params()
            print(f"Connecting to Snowflake...")
            print(f"  Account: {params['account']}")
            print(f"  User: {params['user']}")
            print(f"  Database: {params['database']}")
            print(f"  Schema: {params['schema']}")
            
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}
            
            self._connection = snowflake.connector.connect(**params)
            print("Connected successfully!")
            
        return self._connection
    
    def close(self):
        """Close connection if open"""
        if self._connection:
            self._connection.close()
            self._connection = None


class StageGenerator:
    """Generates staging files from Snowflake metadata"""
    
    def __init__(self, config: Config, connection: SnowflakeConnection):
        self.config = config
        self.connection = connection
        self.schema_prefix = config.get_connection_params()['schema']
        
    def list_sources(self) -> List[Dict]:
        """List all configured sources"""
        conn = self.connection.connect()
        cur = conn.cursor()
        
        query = f"""
        SELECT 
            source_name,
            description,
            database,
            schema,
            loader,
            generate_flag
        FROM {self.schema_prefix}_seed_data.code_gen_config
        ORDER BY source_name
        """
        
        cur.execute(query)
        results = []
        for row in cur:
            results.append({
                'source_name': row[0],
                'description': row[1],
                'database': row[2],
                'schema': row[3],
                'loader': row[4],
                'active': row[5] == 'Y'
            })
        
        cur.close()
        return results
    
    def generate_source_yml(self, source_name: str) -> str:
        """Generate source YML content"""
        conn = self.connection.connect()
        cur = conn.cursor()
        
        query = f"""
        SELECT yml_text 
        FROM {self.schema_prefix}_dw_util.gen_stg_src_name_yml
        WHERE source_name = '{source_name}'
        """
        
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        
        if result:
            return result[0]
        return None
    
    def generate_staging_files(self, source_name: str) -> List[Tuple[str, str, str]]:
        """Generate all staging SQL and YML files for a source
        Returns list of (filename, content, file_type) tuples
        """
        conn = self.connection.connect()
        cur = conn.cursor()
        files = []
        
        # Get SQL files
        sql_query = f"""
        SELECT target_name, sql_text 
        FROM {self.schema_prefix}_dw_util.gen_stg_sql 
        WHERE source_name = '{source_name}'
        ORDER BY target_name
        """
        
        cur.execute(sql_query)
        for row in cur:
            files.append((f"{row[0]}.sql", row[1], 'sql'))
        
        # Get YML files
        yml_query = f"""
        SELECT target_name, yml_text 
        FROM {self.schema_prefix}_dw_util.gen_stg_yml 
        WHERE source_name = '{source_name}'
        ORDER BY target_name
        """
        
        cur.execute(yml_query)
        for row in cur:
            files.append((f"{row[0]}.yml", row[1], 'yml'))
        
        cur.close()
        return files
    
    def write_files(self, source_name: str, dry_run: bool = False):
        """Write all files for a source"""
        # Create source directory
        source_dir = self.config.models_path / source_name
        
        if not dry_run:
            source_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate and write source YML
        print(f"\nGenerating files for source: {source_name}")
        print(f"Output directory: {source_dir}")
        
        source_yml = self.generate_source_yml(source_name)
        if source_yml:
            source_file = source_dir / f"src_{source_name}.yml"
            if dry_run:
                print(f"  Would create: {source_file}")
            else:
                with open(source_file, 'w') as f:
                    f.write(source_yml)
                print(f"  Created: {source_file}")
        
        # Generate and write staging files
        files = self.generate_staging_files(source_name)
        print(f"\nGenerating {len(files)} staging files...")
        
        for filename, content, file_type in files:
            file_path = source_dir / filename
            if dry_run:
                print(f"  Would create: {file_path}")
            else:
                with open(file_path, 'w') as f:
                    f.write(content)
                print(f"  Created: {file_path}")
        
        if dry_run:
            print(f"\nDRY RUN: No files were actually created")
        else:
            print(f"\nSuccessfully generated {len(files) + 1} files")


def main():
    parser = argparse.ArgumentParser(
        description='Generate dbt staging files from Snowflake metadata'
    )
    parser.add_argument(
        '--source',
        help='Source name to generate files for'
    )
    parser.add_argument(
        '--list-sources',
        action='store_true',
        help='List all available sources'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be generated without creating files'
    )
    parser.add_argument(
        '--target',
        default='dev',
        help='dbt target to use (default: dev)'
    )
    
    args = parser.parse_args()
    
    # Set target if provided
    if args.target:
        os.environ['DBT_TARGET'] = args.target
    
    try:
        # Initialize configuration and connection
        config = Config()
        connection = SnowflakeConnection(config)
        generator = StageGenerator(config, connection)
        
        if args.list_sources:
            # List sources
            sources = generator.list_sources()
            print("\nConfigured Sources:")
            print("-" * 60)
            for source in sources:
                status = "ACTIVE" if source['active'] else "INACTIVE"
                print(f"\n{source['source_name']} [{status}]")
                print(f"  Description: {source['description']}")
                print(f"  Location: {source['database']}.{source['schema']}")
                print(f"  Loader: {source['loader']}")
        
        elif args.source:
            # Generate files for source
            generator.write_files(args.source, args.dry_run)
        
        else:
            parser.print_help()
            return 1
        
        # Cleanup
        connection.close()
        return 0
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())