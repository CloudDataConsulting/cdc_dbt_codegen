/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - CONFIDENTIAL
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', ) 
 }}
with 
code_gen_config as 
(select * from {{ ref('code_gen_config') }} where generate_flag = 'Y'
),
columns as (select * from  {{ref ('raw_column_list')}} 
),
all_columns as 
(select code_gen_config.source_name,   columns.* from columns 
inner join code_gen_config 
on upper(columns.table_catalog) = upper(code_gen_config."DATABASE")
and upper(columns.table_schema) = upper(code_gen_config."SCHEMA") 
),
header as (
    select lower(source_name) as source_name , lower(table_name) as table_name, 'stg_' || lower(source_name) ||'__' ||lower(table_name) as target_name,  
-9 as ordinal_position,  
'version: 2

# reminder: Replact TBD descriptions with proper descriptions
# reminder: Add tests  
 
models:
  - name: ' || target_name || '
    description: "TBD"
    columns:' as yml_text  from all_columns  qualify row_number() over (partition by all_columns.source_name, all_columns.table_name order by all_columns.ordinal_position) = 1
),
format_text as (
select source_name, table_name, target_name, ordinal_position,  yml_text from header 
union all     
select lower(source_name) as source_name , lower(table_name) as table_name, 'stg_' || lower(source_name) ||'__' ||lower(table_name) as target_name,  
ordinal_position,  '    - name: '
  || case when lower(column_name) = 'id' then lower(table_name)||'_'|| lower(column_name) else lower(column_name) end 
  || '\n      description: tbd' 
  || case when lower(column_name) = 'id' then '\n      tests:\n        - unique\n        - not_null'  else '' end as yml_text  
 from all_columns  
),
final as 
(
select source_name, table_name, target_name,  listagg(yml_text, '\n') within group (order by ordinal_position) as yml_text from format_text
group by source_name, table_name, target_name 
)
select * from final 