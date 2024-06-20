/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{% set myschema = target.schema %} 
{%- set suspended_query -%}
create or replace table {{ myschema }}_dw_util.dbt_tmp__gen_db_list as 
select 
"name" as DB_NAME
,"created_on" as CREATED_ON
,"origin" as origin
,"owner" as owner
,"comment" as comment
from TABLE(RESULT_SCAN(last_query_id()));

{%- endset -%}

{% set query %}
    CREATE SCHEMA IF NOT EXISTS {{ myschema }}_DW_UTIL
{% endset %}

{% do run_query(query) %}
{% do run_query('BEGIN') %}
{# {% do run_query('show tasks in account') %} #}
{% do run_query('show databases') %}
{% set results = run_query(suspended_query) %}
{% do run_query('COMMIT') %}

select *
from {{ myschema }}_dw_util.dbt_tmp__gen_db_list


