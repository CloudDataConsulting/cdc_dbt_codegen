# Cloud Data Consulting Code Generator

We have a pattern that we follow when working with dbt. 
We put our raw data in its own database. 
Each source system, in its own well named schema. 

## Installation

Include in the models code block reference to the cdc_dbt_utils:

models:
  cdc_dbt_codegen:
    dw_util:
      +materialized: view
      +schema: dw_util

## vars added release 0.2.0 
define the source databases you wish to generate code for here: 

In your project.yml:

```vars: 
  source_dbs: ["prd_fivetran_raw_db","prd_matillion_raw_db","snowflake"]```


## Release Notes 0.2.0 
- added the ability to use variables for source databases. 
- renamed "raw" to "src" as not all sources are necessarily "raw" datbases.



## Raw to stage 
For each source schema we create a seed record in our **code_gen_config.csv** with the following columns: 
* generate_flag (Y/N) - This column lets us control with data which 
* source_name - A short concise name for this data source
* description - A description of this source in terms relevant to the organization.  
* database - The name of the production database where this data source is found. 
* schema - The schema in the database in which this data source is found.  
* loader - The tool/technology that loads this data source. (Fivetran/Matillion/Custom)
* loaded_at_field - The name of the field in each table that has the timestamp of the time that each record was loaded into the raw table. (_fivetran_synced, matillion_updated_timestamp, last_updated_ts)

Then for each source system/source schema we will code generate the following: 

* A directory: models/staging/{source_name} and in that directory: 

* src_{source_name}.yml - Contains a header with the {description} and which maps the {database}.{schema} to the {source_name}.
And which contains a list of all the tables in that {database}.{schema}. 
A list of each table in the {source_name}:
name: {table_name}
description: ""/tbd 

* For each table in the {source} we create two files: 
    * stg_{source_name}__{tablename}.sql
    * stg_{source_name}__{tablename}.yml


## Non Raw/Stage objects
We refer to our next set of functionality as "non-stage" since for 
intermediate, dimensional tables as well as reporting tables, 
we have built models (modelname.sql) that we now need to have a yml file for.  So in this case we do not need to refer to the config table. We just need to know the {database}.{schema}.{tablename} and from this we can find the information in the {database}.information_schema.columns that we need to generate the yml file.

For our dimensional models our practice is to add primary keys to the dimensions tables, and Foreign Keys from the fact tables to the dimensions.  This then allows us to use the code generator to add a relationships test to the fact_tabe.yml file.  

Note: Since we are using the information_schema for our code generator this means the table has to exist for us to generate the yml files. 


## Commands
### Staging 

codegen.py   -- default to help if no parameters entered.  

codegen.py all 

codegen.py all --source {source_name}   -- note maybe we don't need this since we have a flag in the seed table. 

codegen.py table --source {source_name} --table {table_name} 

codegen.py table --source {source_name} --table {table_name} --sql/yml 

Where to write these files: 

models/staging/{source_name}/src_{source_name}.yml 

models/staging/{source_name}/stg_{source_name}__{tablename}.sql 

models/staging/{source_name}/stg_{source_name}__{tablename}.yml  

note: Should we rename a file if it exists so as to be less destructive? 

### Non-Staging 

codegen.py --yml --schema {database}.{schema}   example:codegen.py --yml --database prd_edw_db --schema bpruss_base

codegen.py --yml --table  {database}.{schema}.{table} 

example:codegen.py --yml --database dev_edw_db --schema bpruss_base --table dim_date

Where do we write these files: 

if prefix = 'int_' then 

models/staging/intermediate/{tablename}.yml

else -- dimensional 

models/marts/{martname}/{table_name}.yml



---
## Technical details
Objects used for Staging Tables: 
generate_yml_files_tuned.py
Select source_name, yml_text from {database}.{schema}.gen_stg_src_name_yml
        Select target_name, sql_text from {database}.{schema}.gen_stg_sql WHERE source_name = '{source.lower()}' 
        Select target_name, yml_text from {database}.{schema}.gen_stg_yml WHERE source_name = '{source.lower()}'

## Fact and Dimension tables! 
### Tests to be generated in the yml files: 

For each dim table generate
* dim_table.yml 
    * test PK for unique and not null 
    * pk naming standard is {table}_key (with the the prefix)
        * example: dim_date - date_key 
        * example: dim_customer - customer_key 


for each fct table generate 
* fct_table.yml
    * columns 
       * FK-> PK tests for all _KEY columns or db created PK to FK relationships. 

Note for this to work, we have to have defined the PKs for each dimension, 
and the foreign keys in each fact table.  

We do this with post_hook statments in the config block for the model: 
```
{{  config(  materialized='table', persist_docs={"relation": true, "columns": true},
 post_hook="alter table {{ this }} add primary key (account_key)",  ) 
 }}
```
and for a fact table: 

        