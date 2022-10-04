## Bernie thinks this is obsolete and can be safely deleted. 
## the functionality to select only one source_name is now handled with a flag
## in the config csv file.  

# process_yaml.py file
import snowflake.connector 
import logging
import os
import confuse
import datetime
# import getpass
import pathlib
from pathlib import Path
from snowflake.connector import DictCursor

#***************************************************
# Instructions
#
# First edit file data_automation_config.csv with source schema to be loaded
#
# Prior to running this script...the following dbt jobs must be run
# in order to populate the table used by this script in file creation
#
# dbt run -m raw_table_list 
# dbt run -m gen_stage_files  
# dbt run -m gem_stage_yml
# dbt run -m gen_stage_src_views
# dbt seed
#
# Set source_name to the data schema which you are loading from the raw database
source_name = 'sfdc_arc'


#code follows....

# Instantiates config. Confuse searches for a config_default.yaml
config = confuse.Configuration('MydbtUtils', __name__)

# Get working directory
workingDir = pathlib.Path().absolute()
print (workingDir)

# Add config items from specified file
config.set_file('./dbt/dbt_project.yml')

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

schema = f"{schema}_DW_UTIL"

modelsPath = Path(f"{workingDir}/models/staging")
print (modelsPath)

#print(env, account, user, password, role, database, role, warehouse, schema)

con = snowflake.connector.connect(
  user=user,
  password=password,
  account=account,
  database=database,
  role=role,
  warehouse=warehouse,
)


# Main dbt_files_by_source_YML generator
def generate_yml (): 
    
    cur = con.cursor(DictCursor)
    try:
        v_sql_txt = f"""Select source_name, yml_text from {database}.{schema}.GEN_STAGE_FILES where source_name = '{source_name}' """
        cur.execute(v_sql_txt)
        for rec in cur:
            #Write the Source YML file
            write_source_yml(rec['SOURCE_NAME'], rec['YML_TEXT'])

            #Generate table SQL & YML files for this Source
            gen_table_metadata(rec['SOURCE_NAME'])

    finally:
        cur.close()


#write out the source yml file
def write_source_yml(source_name, yml_text):

    # folderName = "models/staging/%s" % (source_name)
    folderName = "%s/%s" % (modelsPath, source_name) 
    fileName = "%s/src_%s.yml" % (folderName, source_name)
    print(folderName, fileName)
    #isdir
    if not os.path.exists(folderName):
        os.makedirs(folderName)
    appendWrite = 'w' # over write old file if exists
    with open(fileName, 'w') as fin:
        fin.write("%s \n" % (yml_text)) 
    

#generate yml and sql files for each table in the source
def gen_table_metadata (source):
    
    print ('Generate tables SQL')
    cur = con.cursor(DictCursor)
    try:
        v_sql_txt = f"""Select target_name, sql_text from {database}.{schema}.GEN_STAGE_SRC_VIEWS WHERE source_name = '{source.lower()}' """
        print (v_sql_txt)
        cur.execute(v_sql_txt)
        for rec in cur:

            print (rec['TARGET_NAME'])

            target = rec['TARGET_NAME']
            sql_txt = rec['SQL_TEXT']
            filename = f"""{target.lower()}.sql"""
            path = f"""{modelsPath}/{source}/{filename}"""
            write_file(sql_txt, path, "")

    finally:
        cur.close()

    print ('Generate tables YML')
    cur = con.cursor(DictCursor)
    try:
        v_sql_txt = f"""Select target_name, yml_text from {database}.{schema}.GEN_STAGE_YML WHERE source_name = '{source.lower()}' """
        #print (v_sql_txt)
        cur.execute(v_sql_txt)
        for rec in cur:
            target = rec['TARGET_NAME']
            sql_txt = rec['YML_TEXT']
            filename = f"""{target.lower()}.yml"""
            path = f"""{modelsPath}/{source}/{filename}"""
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
