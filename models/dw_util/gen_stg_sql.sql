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
(select * from  {{ ref('code_gen_config') }} where generate_flag = 'Y'
),
columns as (select * from {{ref ('raw_column_list')}} ),
all_columns as 
(select 
code_gen_config.source_name ,columns.* from columns
inner join code_gen_config 
on upper(columns.table_catalog) = upper(code_gen_config."DATABASE") 
and upper(columns.table_schema) = upper(code_gen_config."SCHEMA" )
),
join_filter as (
select 
   lower(all_columns.source_name) as source_name 
  ,lower(all_columns.table_catalog) as source_db
  ,lower(all_columns.table_schema) as source_schema 
  ,all_columns.table_name_quoted  as source_table_quoted
  ,lower(all_columns.table_name)  as source_table
  ,all_columns.is_table_name_quoted
  ,'stg_' || source_name ||'__'|| lower(all_columns.table_name) as target_name
  , all_columns.column_name_quoted as column_name_quoted
  , lower(all_columns.column_name) as column_name
  ,all_columns.is_column_name_quoted
  ,all_columns.ordinal_position
  ,lower(all_columns.data_type) as data_type
  ,character_maximum_length
  ,numeric_precision
  ,numeric_scale
  ,datetime_precision
from all_columns 
),
final  as (
select join_filter.source_name, join_filter.source_db, join_filter.source_schema, join_filter.source_table, join_filter.target_name, join_filter.column_name, -9 ordinal_position, 
'with ' || source_table || ' as ( select * from \{\{ source(''' || source_name || ''',''' || source_table || ''') \}\}
), 
final 
as ( select  ' as sql_text from join_filter  
  qualify row_number() over (partition by join_filter.source_name, source_db, source_schema, join_filter.source_table order by join_filter.ordinal_position) = 1
union all 
select 
 source_name
,source_db
,source_schema 
,source_table
,target_name
,column_name
,ordinal_position
,case when ordinal_position = 1 then ' ' else ',' end || 
case when data_type in ('text','varchar','varying','variant') then
  case when lower(column_name) in ('current','schema','start','table','order','group') 
     then 
       'trim("' || upper(column_name) || '") as "' || upper(column_name) ||'"'
     else 
        case when lower(column_name) = 'id' then 'trim(' || column_name || ') as ' || lower(source_table) ||'_id'
            else
            'trim(' || column_name || ') as ' || lower(column_name)
        end
     end 
 ELSE -- data_type not in list above 
   case when lower(column_name) in ('current','schema','start','table','order','group') then 
      'trim("' || upper(column_name) || '") as "' || upper(column_name) ||'"' 
   else
     case when lower(column_name) = 'id' then 'trim(' || column_name ||') as ' || source_table ||'_id'
        else
       column_name ||' as ' || lower(column_name)  
     end 
   end 
 END
 || case when ( data_type ilike 'timestamp_ntz' or data_type ilike 'datetime') and SOURCE_SCHEMA ='raw_sfdc' then '::timestamp_tz as ' || LOWER(column_name)   else '' end 
  AS sql_text
from join_filter 
union all 
select source_name, source_db, source_schema, source_table, target_name, column_name, 9999 ordinal_position, 
 ','''|| source_name || '''  as source_name 
  from ' || source_table || 
'
) 
select * from final' as sql_text
from join_filter 
qualify row_number() over (partition by join_filter.source_name, source_db, source_schema, join_filter.source_table order by join_filter.ordinal_position) = 1
)
select source_name, source_db, source_schema, target_name, listagg(sql_text, '\n') within group (order by ordinal_position) as sql_text 
from final
group by source_name, source_db, source_schema,  target_name