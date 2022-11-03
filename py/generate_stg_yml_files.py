# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - CONFIDENTIAL
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */


import argparse
import sys
import snowflake.connector 
import logging
import os
import confuse
import datetime
import pathlib
from pathlib import Path
from snowflake.connector import DictCursor

models_path=""
account = ""
user = ""
password = ""
role = ""
database = ""
warehouse = ""
schema = ""


# Main stg_yml_YML generator
def generate (switches): 
    '''
        It generates both sql and yml files based on switches
        :param switches:  Flags that enable/disable the generation of the files
        :return: None
    '''    
    cur = con.cursor(DictCursor)

    # filter source_name
    src_query = ""
    if switches['source_name'] != "":
        src_query = f"where source_name = '{switches['source_name']}'"

    try:
        v_sql_txt = f"""
            select source_name, yml_text 
            from {database}.{schema}.gen_stg_src_name_yml
            {src_query}
        """
        cur.execute(v_sql_txt)
        for rec in cur:
            # write the source_name YML file
            if switches['add_yml']:
                write_source_yml(rec['SOURCE_NAME'], rec['YML_TEXT'])

            # generate table SQL & YML files for this source_name
            gen_table_metadata(rec['SOURCE_NAME'], switches)

    finally:
        cur.close()


# write the source_name yml created in the snowflake view to a file.  
def write_source_yml(source_name, yml_text):
    '''
    Writes the yml text to a file
    :param source_name: source_name name
    :param yml_text: yml to be written
    :returns: None
    '''
    # file_name = "DDL_Scripts_P/%s/%s/%s.sql" % (schemaName,objType, file_name)
    # example: models/staging/xero/src_xero.yml   
    folder_name = "%s/%s" % (models_path, source_name)
    file_name = "%s/src_%s.yml" % (folder_name, source_name)
    write_file(yml_text, file_name, source_name)   


#generate yml and sql files for each table in the source_name
def gen_table_metadata (source_name, switches):
    '''
    Generates sql and yml for the tables
    :param source_name: source_name
    :param switches:  Flags that control the flow
    :returns: None
    '''
    
    # generate SQL'
    if switches['add_sql']:
        cur = con.cursor(DictCursor)
        try:
            table_query =""
            if switches['table'] != "":
                table_query = f" and target_name like '%{source_name}__{switches['table']}'"
            v_sql_txt = f"""
                select target_name, sql_text 
                from {database}.{schema}.gen_stg_sql 
                where source_name = '{source_name.lower()}' {table_query}"""
            #print (v_sql_txt)
            cur.execute(v_sql_txt)
            for rec in cur:
                target = rec['TARGET_NAME']
                sql_txt = rec['SQL_TEXT']
                file_name = f"""{target.lower()}.sql"""
                path = f"""{models_path}/{source_name}/{file_name}"""
                write_file(sql_txt, path,source_name)

        finally:
            cur.close()

    # generate yml
    if switches['add_yml']:
        cur = con.cursor(DictCursor)
        yml_query =""
        if switches['source_name'] != "":
            yml_query = f" and source_name = '{switches['source_name']}'"
        if switches["table"] != "":
            yml_query += f" and table_name = '{switches['table']}'"
        try:
            v_sql_txt = f"""
            select target_name, yml_text, table_name 
            from {database}.{schema}.gen_stg_yml 
            where source_name = '{source_name.lower()}' {yml_query}"""
            #print (v_sql_txt)
            cur.execute(v_sql_txt)
            for rec in cur:
                target = rec['TARGET_NAME']
                sql_txt = rec['YML_TEXT']
                file_name = f"""{target.lower()}.yml"""
                path = f"""{models_path}/{source_name}/{file_name}"""
                write_file(sql_txt, path, source_name)

        finally:
            cur.close()


# write out the file
def write_file(content, file_name, source_name, newfile=True):
    '''
    Write the content to "file_name"
    :param content: string to be written
    :param file_name: File name
    :param source_name: source_name
    :param newfile: Create a new file (true) or append to the existing one (false)
    :return: None
    '''
    # Create directories
    folder_name = "%s/%s" % (models_path, source_name)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    print(file_name)
    if os.path.exists(file_name) and not newfile:
        appendWrite = 'a'  # append if already exists
    else:
        appendWrite = 'w'  # make a new file if not
    with open(file_name, appendWrite, encoding='utf-8') as fin:
        if (newfile):
            fin.write("%s \n" % (content))


# get the snowflake connection object
def get_snowflake_connection():
    '''
    Defines the snowflake connection object.
    :return: Snowflake connection object.
    '''
        # Instantiates config. Confuse searches for a config_default.yaml
    config = confuse.Configuration('MydbtUtils', __name__)

    # Get working directory
    working_dir = pathlib.Path().absolute()
    print (f"Start up directory: {working_dir}")

    # Add config items from specified file
    config.set_file('./dbt_project.yml')

    profile = config['profile'].get()
    print(f"Profile to be used: {profile}")
    env='dev'
    myfile_name = os.path.expanduser('~/.dbt/profiles.yml')
    config.set_file(myfile_name)

    account = config[profile]['outputs'][env]['account'].get()
    user = config[profile]['outputs'][env]['user'].get()
    password = config[profile]['outputs'][env]['password'].get()
    role = config[profile]['outputs'][env]['role'].get()
    global database
    database = config[profile]['outputs'][env]['database'].get()
    warehouse = config[profile]['outputs'][env]['warehouse'].get()
    global schema
    schema = config[profile]['outputs'][env]['schema'].get()

    schema = f"""{schema}_DW_UTIL"""
    global models_path
    models_path = Path(f"{working_dir}/models/staging")
    print (f"Models Path: {models_path}")

    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        role=role,
        warehouse=warehouse,
    )


# parse the command line arguments
def get_run_settings(args):
    '''
    Parses the command line arguments and returns a dictionary of switches that will control the flow
    :param args: Command line arguments
    :return: switches to control the flow
    '''
    # Create parser
    parser = argparse.ArgumentParser(
    prog="code_gen.py",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    usage='''
    generate_stg_yml_files.py
    Usage:
        codegen.py --all : Generates sql and yml for all sources and all tables
        codegen.py --all --src {source_name} : Generates sql and yml for "source_name"
        codegen.py --all --src {source_name} --table {table_name} : Generates sql and yml for "source_name.table_name"

        codegen.py --sql : Generates sql for all sources and all tables
        codegen.py --sql --src {source_name} : Generates sql for "source_name"
        codegen.py --sql --src {source_name} --table {table_name} : Generates sql for "source_name.table_name"

        codegen.py --yml : Generates yml for all sources and all tables
        codegen.py --yml --src {source_name} : Generates yml for "source_name"
        codegen.py --yml --src {source_name} --table {table_name} : Generates yml for "source_name.table_name"

    ''')
        
    if len(args) <2:
        parser.print_usage()
        sys.exit(1)

    # Add parameters
    parser.add_argument('--sql', action='store_true', help='sql only')
    parser.add_argument('--yml', action='store_true', help='yml only')
    parser.add_argument('--all', action='store_true', help='Create sql and yml')
    parser.add_argument('--src', help='src file only')
    parser.add_argument('--database', help='database')
    parser.add_argument('--schema', help='src file only')
    parser.add_argument('--table', help='table name')

    args = parser.parse_args()
    switches= {
        "add_sql": False,
        "add_yml": False,
        "command": "",
        "source_name": "",
        "table" : "",
    }

    if args.all:
        switches['add_sql'] = True
        switches['add_yml'] = True
    else:
        switches['add_sql'] = True if args.sql else False
        switches['add_yml'] = True if args.yml else False

    switches["source_name"] = "" if args.src == None else args.src
    switches["table"] = "" if args.table == None else args.table

    return switches

 
if __name__ == '__main__':
    switches = get_run_settings(sys.argv)
    con = get_snowflake_connection()
    generate(switches) 
