/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', )  }}

{% set sources =  var('source_db')  %}

with src_table_list as ( select * from (
                    {% for src_database in sources %}
                    select * from  {{source_db}}.information_schema.tables 
                    {% if not loop.last %}union all{% endif %}
                    {% endfor %}
                        )  where table_schema not in ('INFORMATION_SCHEMA') )
                        ,
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
from src_table_list
)
select * from final 