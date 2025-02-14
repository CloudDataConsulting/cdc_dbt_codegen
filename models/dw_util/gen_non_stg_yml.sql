/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

-- depends_on: {{ ref('generate_fk_table') }}
{% set myuser = target.user %} 
with 
all_columns as (select columns.* from information_schema.columns where table_catalog = 'DEV_EDW_DB'), 
fk as (select * from {{ref ('generate_fk_table')}} ), 
header as (
    select lower(table_schema) as table_schema , lower(table_name) as table_name --,  'stg_' || lower(table_schema) ||'__' ||lower(table_name) as target_name
,-9 as ordinal_position,  
'version: 2

# reminder: Replace TBD descriptions with proper descriptions
# reminder: Add tests  
 
models:
  - name: ' || lower(table_name) ||
  '\n    description: tbd' ||
  '\n    columns:' as yml_text  from all_columns  qualify row_number() over (partition by all_columns.table_schema, all_columns.table_name order by all_columns.ordinal_position) = 1
),


format_text as (
select table_schema, table_name,  ordinal_position,  yml_text from header 
union all     
select lower(table_schema) as table_schema , lower(table_name) as table_name 
,ordinal_position,  '    - name: '
  || case when lower(column_name) = 'id' then lower(table_name)||'_'|| lower(column_name) else lower(column_name) end ||

'    ' || '\n      description: tbd'
  || case when ( substr(lower(table_name),1,3) = 'dim' and  contains(lower(column_name), 'key') ) then '\n      data_tests:\n        - unique\n        - not_null'  
          when ( substr(lower(table_name),1,3) = 'fct' and  contains(lower(column_name), 'key') ) then coalesce(fk.fk_test,'\n      tests:\n        - unique\n        - not_null'  )
          else '' end as yml_text 
 from all_columns
 left outer join fk on all_columns.table_schema = fk.fk_schema_name 
                                                 and all_columns.table_name =   fk.fk_table_name 
                                                 and all_columns.column_name =  fk.fk_column_name  
),
final as 
(
select table_schema, table_name,   listagg(yml_text, '\n') within group (order by ordinal_position) as yml_text 
from format_text
group by table_schema, table_name
)
select substr(table_name,1,3) table_type, contains(lower(table_name), 'dim') as is_dim, final.* from final
where table_schema like lower(current_user()) || '_%' 
      and lower(table_schema) not in ( lower(current_user()) ||'_dw_util'
                                     , lower(current_user()) || '_seed_data'
                                     , lower(current_user()) ||'_data_ops') 
      and lower(table_name) not like 'stg%'
      and lower(table_name) not like 'unique_%'
      and lower(table_name) not like 'not_null%'
      and lower(table_name) not like 'pk_%'
      and lower(table_name) not like 'dbt_utils_%'
      and lower(table_name) not like 'relationship_%'      