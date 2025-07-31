"""
Pytest configuration and shared fixtures for CDC DBT Codegen tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import yaml
import snowflake.connector


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_dbt_project(temp_dir):
    """Create a mock dbt project structure."""
    # Create dbt_project.yml
    project_config = {
        'name': 'test_project',
        'version': '1.0.0',
        'profile': 'test_profile',
        'model-paths': ['models'],
        'seed-paths': ['seeds'],
        'vars': {
            'source_db': ['test_raw_db'],
            'source_dbs': ['test_raw_db', 'test_fivetran_db']
        }
    }
    
    project_file = temp_dir / 'dbt_project.yml'
    with open(project_file, 'w') as f:
        yaml.dump(project_config, f)
    
    # Create directory structure
    (temp_dir / 'models').mkdir()
    (temp_dir / 'models' / 'staging').mkdir()
    (temp_dir / 'models' / 'marts').mkdir()
    (temp_dir / 'seeds').mkdir()
    
    return temp_dir


@pytest.fixture
def mock_profiles():
    """Create mock dbt profiles."""
    return {
        'test_profile': {
            'outputs': {
                'dev': {
                    'type': 'snowflake',
                    'account': 'test_account',
                    'user': 'test_user',
                    'password': 'test_password',
                    'role': 'test_role',
                    'database': 'test_database',
                    'warehouse': 'test_warehouse',
                    'schema': 'test_schema',
                },
                'prod': {
                    'type': 'snowflake',
                    'account': 'prod_account',
                    'user': 'prod_user',
                    'password': 'prod_password',
                    'role': 'prod_role',
                    'database': 'prod_database',
                    'warehouse': 'prod_warehouse',
                    'schema': 'prod_schema',
                }
            },
            'target': 'dev'
        }
    }


@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection."""
    mock_conn = Mock(spec=snowflake.connector.SnowflakeConnection)
    mock_conn.is_closed.return_value = False
    mock_conn.database = 'test_database'
    mock_conn.schema = 'test_schema'
    
    # Mock cursor
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    return mock_conn


@pytest.fixture
def mock_source_config():
    """Mock source configuration data."""
    return [
        {
            'SOURCE_NAME': 'sfdc',
            'DESCRIPTION': 'Salesforce data',
            'DATABASE': 'PRD_RAW_DB',
            'SCHEMA': 'SFDC',
            'LOADER': 'Fivetran',
            'GENERATE_FLAG': 'Y',
            'LOADED_AT_FIELD': '_fivetran_synced'
        },
        {
            'SOURCE_NAME': 'hubspot',
            'DESCRIPTION': 'HubSpot data',
            'DATABASE': 'PRD_RAW_DB',
            'SCHEMA': 'HUBSPOT',
            'LOADER': 'Fivetran',
            'GENERATE_FLAG': 'Y',
            'LOADED_AT_FIELD': '_fivetran_synced'
        }
    ]


@pytest.fixture
def mock_staging_sql_data():
    """Mock staging SQL generation data."""
    return [
        {
            'TARGET_NAME': 'stg_sfdc__account',
            'SQL_TEXT': '''with account as (
    select * from {{ source('sfdc','account') }}
),
final as (
    select 
        trim(id) as account_id,
        trim(name) as name,
        trim(type) as type,
        created_date as created_date,
        'sfdc' as dw_source_name
    from account
)
select * from final'''
        },
        {
            'TARGET_NAME': 'stg_sfdc__contact',
            'SQL_TEXT': '''with contact as (
    select * from {{ source('sfdc','contact') }}
),
final as (
    select 
        trim(id) as contact_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(email) as email,
        'sfdc' as dw_source_name
    from contact
)
select * from final'''
        }
    ]


@pytest.fixture
def mock_staging_yml_data():
    """Mock staging YAML generation data."""
    return [
        {
            'TARGET_NAME': 'stg_sfdc__account',
            'YML_TEXT': '''version: 2

models:
  - name: stg_sfdc__account
    description: tbd
    columns:
      - name: account_id
        description: tbd
        data_tests:
          - unique
          - not_null
      - name: name
        description: tbd
      - name: type
        description: tbd
      - name: created_date
        description: tbd
      - name: dw_source_name
        description: tbd'''
        }
    ]


@pytest.fixture
def mock_source_yml_data():
    """Mock source YAML data."""
    return [
        {
            'SOURCE_NAME': 'sfdc',
            'YML_TEXT': '''version: 2

sources:
  - name: sfdc
    description: Salesforce data
    database: prd_raw_db
    schema: sfdc
    loader: Fivetran
    loaded_at_field: _fivetran_synced
    tables:
      - name: account
        description: tbd
      - name: contact
        description: tbd
      - name: opportunity
        description: tbd'''
        }
    ]