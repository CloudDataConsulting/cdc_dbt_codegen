"""
Sample Snowflake query responses for testing.

This file documents the actual structure of Snowflake responses to help ensure
our mocks accurately represent what Snowflake returns.
"""

# Sample response from: SELECT * FROM information_schema.tables
SAMPLE_TABLES_RESPONSE = [
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',
        'TABLE_SCHEMA': 'SFDC',
        'TABLE_NAME': 'ACCOUNT',
        'TABLE_OWNER': 'SYSADMIN',
        'TABLE_TYPE': 'BASE TABLE',
        'IS_TRANSIENT': 'NO',
        'CLUSTERING_KEY': None,
        'ROW_COUNT': 15234,
        'BYTES': 2048000,
        'RETENTION_TIME': 1,
        'CREATED': '2023-01-15 10:30:00',
        'LAST_ALTERED': '2024-01-20 15:45:00',
        'COMMENT': None
    },
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',
        'TABLE_SCHEMA': 'SFDC',
        'TABLE_NAME': 'CONTACT',
        'TABLE_OWNER': 'SYSADMIN',
        'TABLE_TYPE': 'BASE TABLE',
        'IS_TRANSIENT': 'NO',
        'CLUSTERING_KEY': None,
        'ROW_COUNT': 45678,
        'BYTES': 4096000,
        'RETENTION_TIME': 1,
        'CREATED': '2023-01-15 10:30:00',
        'LAST_ALTERED': '2024-01-20 15:45:00',
        'COMMENT': None
    }
]

# Sample response from: SELECT * FROM information_schema.columns
SAMPLE_COLUMNS_RESPONSE = [
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',
        'TABLE_SCHEMA': 'SFDC',
        'TABLE_NAME': 'ACCOUNT',
        'COLUMN_NAME': 'ID',
        'ORDINAL_POSITION': 1,
        'COLUMN_DEFAULT': None,
        'IS_NULLABLE': 'NO',
        'DATA_TYPE': 'VARCHAR',
        'CHARACTER_MAXIMUM_LENGTH': 18,
        'CHARACTER_OCTET_LENGTH': 72,
        'NUMERIC_PRECISION': None,
        'NUMERIC_PRECISION_RADIX': None,
        'NUMERIC_SCALE': None,
        'DATETIME_PRECISION': None,
        'INTERVAL_TYPE': None,
        'INTERVAL_PRECISION': None,
        'CHARACTER_SET_CATALOG': None,
        'CHARACTER_SET_SCHEMA': None,
        'CHARACTER_SET_NAME': None,
        'COMMENT': 'Salesforce Account ID'
    },
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',
        'TABLE_SCHEMA': 'SFDC',
        'TABLE_NAME': 'ACCOUNT',
        'COLUMN_NAME': 'NAME',
        'ORDINAL_POSITION': 2,
        'COLUMN_DEFAULT': None,
        'IS_NULLABLE': 'YES',
        'DATA_TYPE': 'VARCHAR',
        'CHARACTER_MAXIMUM_LENGTH': 255,
        'CHARACTER_OCTET_LENGTH': 1020,
        'NUMERIC_PRECISION': None,
        'NUMERIC_PRECISION_RADIX': None,
        'NUMERIC_SCALE': None,
        'DATETIME_PRECISION': None,
        'INTERVAL_TYPE': None,
        'INTERVAL_PRECISION': None,
        'CHARACTER_SET_CATALOG': None,
        'CHARACTER_SET_SCHEMA': None,
        'CHARACTER_SET_NAME': None,
        'COMMENT': 'Account Name'
    },
    {
        'TABLE_CATALOG': 'PRD_RAW_DB',
        'TABLE_SCHEMA': 'SFDC',
        'TABLE_NAME': 'ACCOUNT',
        'COLUMN_NAME': '_FIVETRAN_SYNCED',
        'ORDINAL_POSITION': 10,
        'COLUMN_DEFAULT': None,
        'IS_NULLABLE': 'YES',
        'DATA_TYPE': 'TIMESTAMP_NTZ',
        'CHARACTER_MAXIMUM_LENGTH': None,
        'CHARACTER_OCTET_LENGTH': None,
        'NUMERIC_PRECISION': None,
        'NUMERIC_PRECISION_RADIX': None,
        'NUMERIC_SCALE': None,
        'DATETIME_PRECISION': 9,
        'INTERVAL_TYPE': None,
        'INTERVAL_PRECISION': None,
        'CHARACTER_SET_CATALOG': None,
        'CHARACTER_SET_SCHEMA': None,
        'CHARACTER_SET_NAME': None,
        'COMMENT': 'Fivetran sync timestamp'
    }
]

# Sample response from gen_stg_src_name_yml view
SAMPLE_SOURCE_YML_RESPONSE = [
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

# Sample response from gen_stg_sql view
SAMPLE_STAGING_SQL_RESPONSE = [
    {
        'SOURCE_NAME': 'sfdc',
        'SOURCE_DB': 'prd_raw_db',
        'SOURCE_SCHEMA': 'sfdc',
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
        _fivetran_synced as _fivetran_synced,
        'sfdc' as dw_source_name
    from account
) 
select * from final'''
    }
]

# Sample response from gen_stg_yml view
SAMPLE_STAGING_YML_RESPONSE = [
    {
        'SOURCE_NAME': 'sfdc',
        'TABLE_NAME': 'account',
        'TARGET_NAME': 'stg_sfdc__account',
        'YML_TEXT': '''version: 2

# reminder: Replace TBD descriptions with proper descriptions
# reminder: Add tests  
 
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
      - name: _fivetran_synced
        description: tbd
      - name: dw_source_name
        description: tbd'''
    }
]

# Sample response from gen_non_stg_yml view
SAMPLE_DIMENSIONAL_YML_RESPONSE = [
    {
        'TABLE_SCHEMA': 'MARTS_BASE',
        'TABLE_NAME': 'dim_customer',
        'TABLE_TYPE': 'dim',
        'IS_DIM': True,
        'YML_TEXT': '''version: 2

# reminder: Replace TBD descriptions with proper descriptions
# reminder: Add tests  
 
models:
  - name: dim_customer
    description: tbd
    columns:
      - name: customer_key
        description: tbd
        data_tests:
          - unique
          - not_null
      - name: customer_id
        description: tbd
      - name: customer_name
        description: tbd
      - name: customer_email
        description: tbd'''
    }
]

# Sample response from code_gen_config seed
SAMPLE_CODE_GEN_CONFIG_RESPONSE = [
    {
        'GENERATE_FLAG': 'Y',
        'SOURCE_NAME': 'sfdc',
        'DESCRIPTION': 'Salesforce CRM data',
        'DATABASE': 'PRD_RAW_DB',
        'SCHEMA': 'SFDC',
        'LOADER': 'Fivetran',
        'LOADED_AT_FIELD': '_fivetran_synced'
    },
    {
        'GENERATE_FLAG': 'Y',
        'SOURCE_NAME': 'hubspot',
        'DESCRIPTION': 'HubSpot marketing data',
        'DATABASE': 'PRD_RAW_DB',
        'SCHEMA': 'HUBSPOT',
        'LOADER': 'Fivetran',
        'LOADED_AT_FIELD': '_fivetran_synced'
    },
    {
        'GENERATE_FLAG': 'N',
        'SOURCE_NAME': 'legacy_erp',
        'DESCRIPTION': 'Legacy ERP system',
        'DATABASE': 'PRD_RAW_DB',
        'SCHEMA': 'LEGACY_ERP',
        'LOADER': 'Custom',
        'LOADED_AT_FIELD': 'load_timestamp'
    }
]

# Sample SHOW IMPORTED KEYS response for foreign key relationships
SAMPLE_FK_RESPONSE = [
    {
        'PK_DATABASE_NAME': 'DEV_EDW_DB',
        'PK_SCHEMA_NAME': 'MARTS_BASE',
        'PK_TABLE_NAME': 'DIM_CUSTOMER',
        'PK_COLUMN_NAME': 'CUSTOMER_KEY',
        'FK_DATABASE_NAME': 'DEV_EDW_DB',
        'FK_SCHEMA_NAME': 'MARTS_BASE',
        'FK_TABLE_NAME': 'FCT_ORDERS',
        'FK_COLUMN_NAME': 'CUSTOMER_KEY',
        'FK_NAME': 'FK_FCT_ORDERS_CUSTOMER',
        'KEY_SEQUENCE': 1,
        'UPDATE_RULE': 'NO ACTION',
        'DELETE_RULE': 'NO ACTION',
        'MATCH_OPTION': 'NONE',
        'CREATED_ON': '2024-01-15 10:30:00',
        'COMMENT': None
    }
]