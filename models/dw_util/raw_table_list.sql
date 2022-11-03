/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', )  }}

with fivetran_raw_tables as (select * from  {{ source('fivetran_raw', 'tables') }}),
    matillion_raw_tables as  (select * from  {{ source('matillion_raw', 'tables') }}),
final as (
select
    table_catalog
    , table_schema
    , table_name
    , case when upper(table_name) <> table_name then '"' || table_name ||'"' else table_name end as table_name_quoted
    , case when upper(table_name) <> table_name then True else False end as is_table_name_quoted
    , table_owner
    , table_type
    , is_transient
    , clustering_key
    , row_count
    , bytes
    , retention_time
    , created
    , last_altered
    , comment
from fivetran_raw_tables
where table_schema not in ('INFORMATION_SCHEMA')
union all 
select
    table_catalog
    , table_schema
    , table_name
    , case when upper(table_name) <> table_name then '"' || table_name ||'"' else table_name end as table_name_quoted
    , case when upper(table_name) <> table_name then True else False end as is_table_name_quoted
    , table_owner
    , table_type
    , is_transient
    , clustering_key
    , row_count
    , bytes
    , retention_time
    , created
    , last_altered
    , comment
from matillion_raw_tables
where table_schema not in ('INFORMATION_SCHEMA')
)
select * from final 