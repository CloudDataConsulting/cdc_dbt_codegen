/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', )  }}
{% set sources =  var('source_db')  %}

with src_columns as ( select * from (
                    {% for src_database in sources %}
                    select * from  {{src_database}}.information_schema.columns
                    {% if not loop.last %}union all{% endif %}
                    {% endfor %}
                       )  where table_schema not in ('INFORMATION_SCHEMA') 
                    ),

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
from src_columns    
)
select * from final 