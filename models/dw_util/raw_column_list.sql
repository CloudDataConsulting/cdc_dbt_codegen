/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', )  }}

with fivetran_columns   as (select * from  {{ source('fivetran_raw' , 'columns') }} ),
     matillion_columns  as (select * from  {{ source('matillion_raw', 'columns') }} ),
     snowflake_columns  as (select * from  {{ source('snowflake'    , 'columns') }} ),
final as (
select
      table_catalog
    , table_schema
    , table_name
    , case when upper(table_name) <> table_name then '"' || table_name ||'"' else table_name end as table_name_quoted
    , case when upper(table_name) <> table_name then True else False end as is_table_name_quoted
    , column_name
    , case when upper(column_name) <> column_name then '"' || column_name ||'"' else column_name end as column_name_quoted
    , case when upper(column_name) <> column_name then True else False end as is_column_name_quoted
    , ordinal_position
    , column_default
    , is_nullable
    , data_type
    , character_maximum_length
    , character_octet_length
    , numeric_precision
    , numeric_precision_radix
    , numeric_scale
    , datetime_precision
    , interval_type
    , interval_precision
    , character_set_catalog
    , character_set_schema
    , character_set_name
    , comment
from fivetran_columns    
where table_schema not in ('INFORMATION_SCHEMA')
union all
select
      table_catalog
    , table_schema
    , table_name
    , case when upper(table_name) <> table_name then '"' || table_name ||'"' else table_name end as table_name_quoted
    , case when upper(table_name) <> table_name then True else False end as is_table_name_quoted
    , column_name
    , case when upper(column_name) <> column_name then '"' || column_name ||'"' else column_name end as column_name_quoted
    , case when upper(column_name) <> column_name then True else False end as is_column_name_quoted
    , ordinal_position
    , column_default
    , is_nullable
    , data_type
    , character_maximum_length
    , character_octet_length
    , numeric_precision
    , numeric_precision_radix
    , numeric_scale
    , datetime_precision
    , interval_type
    , interval_precision
    , character_set_catalog
    , character_set_schema
    , character_set_name
    , comment
from matillion_columns    
where table_schema not in ('INFORMATION_SCHEMA')
union all 
select
      table_catalog
    , table_schema
    , table_name
    , case when upper(table_name) <> table_name then '"' || table_name ||'"' else table_name end as table_name_quoted
    , case when upper(table_name) <> table_name then True else False end as is_table_name_quoted
    , column_name
    , case when upper(column_name) <> column_name then '"' || column_name ||'"' else column_name end as column_name_quoted
    , case when upper(column_name) <> column_name then True else False end as is_column_name_quoted
    , ordinal_position
    , column_default
    , is_nullable
    , data_type
    , character_maximum_length
    , character_octet_length
    , numeric_precision
    , numeric_precision_radix
    , numeric_scale
    , datetime_precision
    , interval_type
    , interval_precision
    , character_set_catalog
    , character_set_schema
    , character_set_name
    , comment
from snowflake_columns    
where table_schema not in ('INFORMATION_SCHEMA')
)
select * from final 