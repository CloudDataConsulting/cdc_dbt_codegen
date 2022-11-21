# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */


# process_yaml.py file
import snowflake.connector 
import logging
import os
import confuse
import datetime
import pathlib
from pathlib import Path
from snowflake.connector import DictCursor


# Instantiates config. Confuse searches for a config_default.yaml
config = confuse.Configuration('MydbtUtils', __name__)

# Get working directory
workingDir = pathlib.Path().absolute()
print (workingDir)

# Add config items from specified file
config.set_file('./dbt_project.yml')

profile = config['profile'].get()
print(profile)
env='dev'
myfilename = os.path.expanduser('~/.dbt/profiles.yml')
config.set_file(myfilename)

account = config[profile]['outputs'][env]['account'].get()
user = config[profile]['outputs'][env]['user'].get()
password = config[profile]['outputs'][env]['password'].get()
role = config[profile]['outputs'][env]['role'].get()
database = config[profile]['outputs'][env]['database'].get()
warehouse = config[profile]['outputs'][env]['warehouse'].get()
schema = config[profile]['outputs'][env]['schema'].get()

schema = f"""{schema}_DW_UTIL"""

modelsPath = Path(f"{workingDir}/models/codegen_yml")
print (modelsPath)

con = snowflake.connector.connect(
  user=user,
  password=password,
  account=account,
  database=database,
  role=role,
  warehouse=warehouse,
)


# Main generate_non_stg_yml YML generator
def generate_yml (): 
#generate yml and sql files for each table in the source
    write_dimensional_yml()

def write_dimensional_yml ():

    cur = con.cursor(DictCursor)
    try:
        v_sql_txt = f"""Select table_name, yml_text from {database}.{schema}.gen_non_stg_yml """
        #print (v_sql_txt)
        cur.execute(v_sql_txt)
        for rec in cur:
            target = rec['TABLE_NAME']
            sql_txt = rec['YML_TEXT']
            folderName = "%s/%s" % (modelsPath, 'codegen') 
            if not os.path.exists(folderName):
                os.makedirs(folderName)
            filename = f"""{target.lower()}.yml"""
            path = f"""{modelsPath}/{filename}"""
            write_file(sql_txt, path, "")

    finally:
        cur.close()

# write out the file
def write_file(content, fileName, process_name, newfile=True):
    #fileName = "%s.sql" % (fileName)
    print(fileName)
    if os.path.exists(fileName) and not newfile:
        appendWrite = 'a'  # append if already exists
    else:
        #print ('new file')
        appendWrite = 'w'  # make a new file if not
    with open(fileName, appendWrite, encoding='utf-8') as fin:
        if (newfile):
            fin.write("%s \n" % (content))

generate_yml() 
