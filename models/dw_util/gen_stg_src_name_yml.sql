/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', ) 
}}

with src_table_list as (select * from {{ ref('src_table_list') }}
),
code_gen_config as (select * from {{ ref('code_gen_config')  }} where generate_flag = 'Y'
), 
header as (
Select code_gen_config.source_name,  -1 sort_by, 
'version: 2

sources:
  - name: ' || code_gen_config.source_name || '\n' ||
'    description: '|| description || '\n' ||
'    database: ' || lower(database) || '\n' || 
'    schema: ' || lower(schema) || '\n' || 
'    loader: '|| loader  || '\n' || 
'    loaded_at_field: '|| loaded_at_field || '\n' || 
'    tables:' as yml_text from code_gen_config
),  
list as (
  select code_gen_config.source_name, 0 sort_by, '      - name: ' || lower(src_table_list.table_name)  
   || '\n        description: tbd '  as yml_text 
  from src_table_list 
       inner join code_gen_config on upper(src_table_list.table_schema) = upper(code_gen_config."SCHEMA")
       and  upper(src_table_list.table_catalog)  = upper(code_gen_config.database)
order by src_table_list.table_name ),
union_final as (
  select * from header
  union all 
  select * from list 
)
select source_name, listagg(yml_text, '\n') within group (order by sort_by, yml_text ) as yml_text from union_final
group by source_name

