#this is our main stage code_gen file

import sys
import argparse


'''
Requirements: 
1) Generate by source the staging layer:
 models/staging/{source_name}/
    src_{source_name}.yml 
    stg_{source_name}__{tablename}.sql
    stg_{source_name}__{tablename}.yml 


2) generate non-stage layer .yml files.  The .sql files we create by other means. 
models/intermediate/{target_name}/
    for each int_* table 
    int_{rest_of_table_name}.yml 
models/marts/{target_name}/
    for each table 
    {table_name}.yml 

Example CLI calls: 

code_gen.py 

####################################################################################################################################
# cli argument parser
# 3 functions: generate: stg_sql, stg_yml, dim_yml 
# 3 commands: 1. All: stg_sql and stg_yml, 2. table: stg_sql, 3. yml: stg_yml, dim_yml
'''


# Create parser
parser = argparse.ArgumentParser(
    prog="code_gen.py",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    usage='''
    Run dbt code gen scripts.
    Usage:
    ''')
    
# If no parameters print usage
if len(sys.argv) <2:
    parser.print_usage()
    sys.exit(1)

# Add parameters
parser.add_argument('--sql', action='store_true', help='sql only')
parser.add_argument('--yml', action='store_true', help='yml only')
parser.add_argument('--src', help='src file only')
parser.add_argument('--database', help='database')
parser.add_argument('--schema', help='src file only')
parser.add_argument('--table', help='table name')


subparsers = parser.add_subparsers(dest='command')

# Parameters for stage
stage = subparsers.add_parser('stage', help="Command: stage")
stage.add_argument('--sql', action='store_true', help='sql only', dest='stage_sql')
stage.add_argument('--yml', action='store_true', help='yml only', dest="stage_yml")
stage.add_argument('--src', help='src file only', dest="stage_src")
stage.add_argument('--database', help='database', dest="stage_database")
stage.add_argument('--schema', help='src file only', dest="stage_schema")
stage.add_argument('--table', help='table name', dest="stage_table")


# Parameters for nonstage
nonstage = subparsers.add_parser('nonstage')
nonstage.add_argument('--sql', action='store_true', help='sql only', dest='nonstage_sql')
nonstage.add_argument('--yml', action='store_true', help='yml only', dest="nonstage_yml")
nonstage.add_argument('--src', help='src file only', dest="nonstage_src")
nonstage.add_argument('--database', help='database', dest="nonstage_database")
nonstage.add_argument('--schema', help='src file only', dest="nonstage_schema")
nonstage.add_argument('--table', help='table name', dest="nonstage_table", required=True, type=str)

# Parameters for yml
yml = subparsers.add_parser('yml')
yml.add_argument('--sql', action='store_true', help='sql only', dest='yml_sql')
yml.add_argument('--yml', action='store_true', help='yml only', dest="yml_yml")
yml.add_argument('--src', help='src file only', dest="yml_src")
yml.add_argument('--database', help='database', dest="yml_database")
yml.add_argument('--schema', help='src file only', dest="yml_schema")
yml.add_argument('--table', help='table name', dest="yml_table", required=True, type=str)


args = parser.parse_args()

# generate stg_sql, stg_yml

if args.command == 'stage':
    # print(args.command)
    print('modelsPath = Path(f"{workingDir}/models/staging") ') 
    # print(args.sql)
    #target_name = args.table
    #print(target_name) 
    # if args.source:
    #     source_name = args.source
    #     target_flag = 'N'
    #     target_name = args.table
    #     print('dbt_stg_sql_yml()') , 
    #     print('gen_stg_yml()') 

# generate stg_sql
# if args.command == 'table':
#     source_name = args.source
#     target_flag = 'Y'
#     target_name = args.table
#     print(target_name)
#     if args.table:
#         modelsPath = Path(f"{workingDir}/models/staging")
#         print(modelsPath)
#         print('dbt_stg_sql_yml()') 

'''
# generate stg_yml, dim_yml

if args.command == 'yml':
    source_name = '' # args.source
    target_name = args.table
    print(target_name)
    if args.yml == 'stg_yml':
        modelsPath = Path(f"{workingDir}/models/staging")
        if args.table == 'all':
            target_flag = 'N'
            gen_src_yml(database,schema, source_name),
            gen_yml_file(database,schema, source_name, target_name )
        elif args.table == str and not 'all':
            target_flag = 'Y'
            args.table = target_name
            gen_stg_yml()
    elif args.yml == 'dim_yml':
        modelsPath = Path(f"{workingDir}/models/marts/base")
        gen_dim_yml()
'''