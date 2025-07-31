# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
CDC DBT Codegen CLI - Main entry point for code generation.

This module provides the command-line interface for generating dbt staging
and non-staging files based on Snowflake metadata.
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

from .core.config import get_config
from .core.connection import ConnectionManager
from .core.generators import StageGenerator, DimensionalGenerator, SourceLister

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="cdc-dbt-codegen",
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
  
  # Use dry-run to preview changes
  %(prog)s stage --all --source sfdc_arc --dry-run
        """
    )
    
    # Global arguments
    parser.add_argument(
        '--profile',
        help='dbt profile to use'
    )
    parser.add_argument(
        '--target',
        help='dbt target to use (default: dev)'
    )
    parser.add_argument(
        '--project-dir',
        type=Path,
        help='dbt project directory (default: current directory)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.3.0'
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
    stage_parser.add_argument(
        '--backup',
        action='store_true',
        default=True,
        help='Backup existing files before overwriting (default: True)'
    )
    
    # Dimensional command
    dim_parser = subparsers.add_parser(
        'dimensional',
        help='Generate YAML files for dimensional/fact tables'
    )
    dim_parser.add_argument(
        '--database',
        help='Database name (e.g., dev_edw_db)'
    )
    dim_parser.add_argument(
        '--schema',
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
    dim_parser.add_argument(
        '--backup',
        action='store_true',
        default=True,
        help='Backup existing files before overwriting (default: True)'
    )
    
    # List sources command
    list_parser = subparsers.add_parser(
        'list-sources',
        help='List available sources from configuration'
    )
    
    return parser


def handle_stage_command(args: argparse.Namespace, config, conn_manager) -> int:
    """Handle the stage command."""
    # Validate arguments
    if not any([args.all, args.sql, args.yml]):
        print("Error: Must specify --all, --sql, or --yml")
        return 1
    
    if args.table and not args.source:
        print("Error: --table requires --source to be specified")
        return 1
    
    try:
        generator = StageGenerator(config, conn_manager)
        
        print("Generating staging files...")
        if args.source:
            print(f"Source: {args.source}")
        if args.table:
            print(f"Table: {args.table}")
        
        files = generator.generate(
            source_name=args.source,
            table_name=args.table,
            generate_sql=args.all or args.sql,
            generate_yml=args.all or args.yml,
            dry_run=args.dry_run
        )
        
        print(f"\nGenerated {len(files)} files:")
        for file in files:
            print(f"  - {file}")
        
        return 0
    except Exception as e:
        logger.error(f"Error generating staging files: {e}", exc_info=True)
        return 1


def handle_dimensional_command(args: argparse.Namespace, config, conn_manager) -> int:
    """Handle the dimensional command."""
    try:
        generator = DimensionalGenerator(config, conn_manager)
        
        print(f"Generating dimensional YAML files...")
        if args.database:
            print(f"Database: {args.database}")
        if args.schema:
            print(f"Schema: {args.schema}")
        if args.table:
            print(f"Table: {args.table}")
        
        files = generator.generate(
            database=args.database,
            schema=args.schema,
            table_name=args.table,
            dry_run=args.dry_run
        )
        
        print(f"\nGenerated {len(files)} files:")
        for file in files:
            print(f"  - {file}")
        
        return 0
    except Exception as e:
        logger.error(f"Error generating dimensional files: {e}", exc_info=True)
        return 1


def handle_list_sources_command(conn_manager) -> int:
    """Handle the list-sources command."""
    try:
        lister = SourceLister(conn_manager)
        sources = lister.list_sources()
        
        if not sources:
            print("No sources found in configuration")
            print("Make sure you have run 'dbt seed' to load the configuration")
            return 0
        
        print("\nConfigured Sources:")
        print("-" * 80)
        for source in sources:
            status = "ACTIVE" if source['GENERATE_FLAG'] == 'Y' else "INACTIVE"
            print(f"\nSource: {source['SOURCE_NAME']} [{status}]")
            print(f"  Description: {source['DESCRIPTION']}")
            print(f"  Location: {source['DATABASE']}.{source['SCHEMA']}")
            print(f"  Loader: {source['LOADER']}")
        
        return 0
    except Exception as e:
        logger.error(f"Error listing sources: {e}", exc_info=True)
        return 1


def main():
    """Main entry point."""
    parser = create_parser()
    
    # Print help if no arguments
    if len(sys.argv) == 1:
        parser.print_help()
        return 1
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get configuration
    try:
        config = get_config(args.project_dir)
        conn_manager = ConnectionManager(
            profile_name=args.profile or config.profile_name,
            target=args.target
        )
    except Exception as e:
        logger.error(f"Error initializing: {e}")
        return 1
    
    # Handle commands
    try:
        if args.command == 'stage':
            return handle_stage_command(args, config, conn_manager)
        elif args.command == 'dimensional':
            return handle_dimensional_command(args, config, conn_manager)
        elif args.command == 'list-sources':
            return handle_list_sources_command(conn_manager)
        else:
            parser.print_help()
            return 1
    finally:
        # Clean up connection
        conn_manager.close()


if __name__ == '__main__':
    sys.exit(main())