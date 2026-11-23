# /* Copyright (C) 2022 Cloud Data Consulting, Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
CDC DBT Codegen CLI - Main entry point for code generation

This module provides the command-line interface for generating dbt staging
and non-staging files based on Snowflake metadata.
"""

import sys
import argparse
import os
from pathlib import Path
from typing import Dict, Optional

# Import the existing generation modules
try:
    from generate_stg_yml_files import generate as generate_staging, get_snowflake_connection
    from generate_non_stg_yml_files import generate_yml as generate_non_staging
except ImportError:
    # Handle case where script is run directly
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from generate_stg_yml_files import generate as generate_staging, get_snowflake_connection
    from generate_non_stg_yml_files import generate_yml as generate_non_staging


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="code_gen.py",
        description="CDC DBT Codegen - Generate dbt staging and dimensional files from Snowflake metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all staging files for all sources
  %(prog)s stage --all
  
  # Generate staging files for a specific source
  %(prog)s stage --all --source sfdc_arc
  
  # Generate only SQL files for a specific table
  %(prog)s stage --sql --source sfdc_arc --table account
  
  # Generate dimensional YAML files
  %(prog)s dimensional --database dev_edw_db --schema bpruss_base
  
  # List available sources
  %(prog)s list-sources
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Stage command
    stage_parser = subparsers.add_parser(
        'stage', 
        help='Generate staging layer files (SQL and/or YAML)'
    )
    stage_parser.add_argument(
        '--all', 
        action='store_true',
        help='Generate both SQL and YAML files'
    )
    stage_parser.add_argument(
        '--sql', 
        action='store_true',
        help='Generate only SQL files'
    )
    stage_parser.add_argument(
        '--yml', '--yaml',
        action='store_true',
        dest='yml',
        help='Generate only YAML files'
    )
    stage_parser.add_argument(
        '--source', '--src',
        dest='source',
        help='Source name to generate files for (e.g., sfdc_arc)'
    )
    stage_parser.add_argument(
        '--table',
        help='Specific table to generate files for'
    )
    stage_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be generated without creating files'
    )
    
    # Dimensional command
    dim_parser = subparsers.add_parser(
        'dimensional',
        help='Generate YAML files for dimensional/fact tables'
    )
    dim_parser.add_argument(
        '--database',
        required=True,
        help='Database name (e.g., dev_edw_db)'
    )
    dim_parser.add_argument(
        '--schema',
        required=True,
        help='Schema name (e.g., bpruss_base)'
    )
    dim_parser.add_argument(
        '--table',
        help='Specific table to generate YAML for'
    )
    dim_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be generated without creating files'
    )
    
    # List sources command
    list_parser = subparsers.add_parser(
        'list-sources',
        help='List available sources from configuration'
    )
    
    # Add version argument
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.3.0'
    )
    
    return parser


def handle_stage_command(args: argparse.Namespace) -> int:
    """Handle the stage command."""
    # Validate arguments
    if not any([args.all, args.sql, args.yml]):
        print("Error: Must specify --all, --sql, or --yml")
        return 1
    
    if args.table and not args.source:
        print("Error: --table requires --source to be specified")
        return 1
    
    # Build switches for the existing generate function
    switches = {
        "add_sql": args.all or args.sql,
        "add_yml": args.all or args.yml,
        "source_name": args.source or "",
        "table": args.table or "",
    }
    
    if args.dry_run:
        print("DRY RUN MODE - No files will be created")
        print(f"Would generate: {'SQL and YAML' if args.all else 'SQL' if args.sql else 'YAML'} files")
        if args.source:
            print(f"For source: {args.source}")
        if args.table:
            print(f"For table: {args.table}")
        return 0
    
    try:
        print("Generating staging files...")
        if args.source:
            print(f"Source: {args.source}")
        if args.table:
            print(f"Table: {args.table}")
        
        # Use the existing generation function
        generate_staging(switches)
        print("Staging file generation complete!")
        return 0
    except Exception as e:
        print(f"Error generating staging files: {e}")
        return 1


def handle_dimensional_command(args: argparse.Namespace) -> int:
    """Handle the dimensional command."""
    if args.dry_run:
        print("DRY RUN MODE - No files will be created")
        print(f"Would generate YAML files for: {args.database}.{args.schema}")
        if args.table:
            print(f"For table: {args.table}")
        return 0
    
    try:
        print(f"Generating dimensional YAML files for {args.database}.{args.schema}...")
        if args.table:
            print(f"Table: {args.table}")
        
        # TODO: Modify generate_non_stg_yml_files.py to accept parameters
        # For now, we'll need to update that module
        print("Note: Dimensional generation currently uses settings from profiles.yml")
        generate_non_staging()
        print("Dimensional YAML generation complete!")
        return 0
    except Exception as e:
        print(f"Error generating dimensional files: {e}")
        return 1


def handle_list_sources_command() -> int:
    """Handle the list-sources command."""
    try:
        # Get connection and query for sources
        con = get_snowflake_connection()
        cur = con.cursor()
        
        # Query the seed data for configured sources
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
        results = cur.fetchall()
        
        if not results:
            print("No sources found in configuration")
            return 0
        
        print("\nConfigured Sources:")
        print("-" * 80)
        for row in results:
            source_name, desc, db, schema, loader, active = row
            status = "ACTIVE" if active == 'Y' else "INACTIVE"
            print(f"\nSource: {source_name} [{status}]")
            print(f"  Description: {desc}")
            print(f"  Location: {db}.{schema}")
            print(f"  Loader: {loader}")
        
        cur.close()
        con.close()
        return 0
    except Exception as e:
        print(f"Error listing sources: {e}")
        print("Make sure you have run 'dbt seed' to load the configuration")
        return 1


def main():
    """Main entry point."""
    parser = create_parser()
    
    # Print help if no arguments
    if len(sys.argv) == 1:
        parser.print_help()
        return 1
    
    args = parser.parse_args()
    
    # Handle commands
    if args.command == 'stage':
        return handle_stage_command(args)
    elif args.command == 'dimensional':
        return handle_dimensional_command(args)
    elif args.command == 'list-sources':
        return handle_list_sources_command()
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())