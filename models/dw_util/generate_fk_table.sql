-- depends_on: {{ ref('generate_db_table') }}
{% set myuser = target.user %} 
{%- set suspended_query -%}
create or replace table {{ myuser }}_dw_util.dbt_tmp__gen_fk_list as 
select 
"pk_database_name" as PK_DATABASE_NAME
,"fk_schema_name" as FK_SCHEMA_NAME
,"fk_table_name" as FK_TABLE_NAME
,"fk_column_name" as FK_COLUMN_NAME
,"pk_table_name" as PK_TABLE_NAME
,"pk_column_name" as PK_COLUMN_NAME
,"pk_schema_name" as PK_SCHEMA_NAME
,'\n      tests:\n          - relationships:\n              to: ref(''' 
|| lower(pk_table_name) || ''')\n              field: ' 
|| lower(fk_column_name) || '\n          - not_null' as fk_test
from TABLE(RESULT_SCAN(last_query_id()));

{%- endset -%}


{% set query %}
    CREATE SCHEMA IF NOT EXISTS {{ myuser }}_DW_UTIL
{% endset %}

{% do run_query(query) %}
{% do run_query('begin') %}
{# {% do run_query('show tasks in account') %} #}
{% do run_query('show imported keys') %}
{% set results = run_query(suspended_query) %}
{% do run_query('commit') %}

select *
from {{ myuser }}_dw_util.dbt_tmp__gen_fk_list