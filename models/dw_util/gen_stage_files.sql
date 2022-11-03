/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - CONFIDENTIAL
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{{  config(  materialized='view', ) 
 }}
with raw_table_list as (select * from {{ ref('raw_table_list') }}
),
automation_config as (select * from {{ ref('code_gen_config')  }} where generate_flag = 'Y'
), 
header as (
Select automation_config.source_name,  -1 sort_by, 
'version: 2

sources:
  - name: ' || automation_config.source_name || '\n' ||
'    description: '|| description || '\n' ||
'    database: ' || lower(database) || '\n' || 
'    schema: ' || lower(schema) || '\n' || 
'    loader: '|| loader  || '\n' || 
'    loaded_at_field: '|| loaded_at_field || '\n' || 
'    tables:' as yml_text from automation_config
),  
list as (
  select automation_config.source_name, 0 sort_by, '      - name: ' || lower(raw_table_list.table_name)  
   || '\n        description: tbd '  as yml_text 
  from raw_table_list 
       inner join automation_config on upper(raw_table_list.table_schema) = upper(automation_config."SCHEMA")
order by raw_table_list.table_name ),
union_final as (
  select * from header
  union all 
  select * from list 
)
select source_name, listagg(yml_text, '\n') within group (order by sort_by, yml_text ) as yml_text from union_final
group by source_name

