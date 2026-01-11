from database_utils import DatabaseConnector
import pandas as pd 

# Instantiation of Classes 
connection = DatabaseConnector() 
# Reading in backend secrets configuration file 
config_file = connection.read_database_credentials('db_creds.yaml')
# Creating pandas dataframe from excel metadata spreadsheet 
metadata_df = pd.read_excel('metadata.xlsx')
print(metadata_df)

# Extracting connection string from metadata excel spreadsheet 
conn_string_dict = metadata_df['CONNECTION'].to_dict()


# Creating connection string to connect to database
secret_conn_string = conn_string_dict[0]
full_connection_string_server = secret_conn_string.replace('***', config_file['RDS_PASSWORD'])
# print(full_connection_string_server)

# Extract database_name from the metadata spreadsheet 
new_db_name_dict = metadata_df['DATABASE_NAME'].to_dict()
new_db_name = new_db_name_dict[0]

full_connection_string_target = full_connection_string_server + f"/{new_db_name}"

database_connect_source = connection.initialise_database_connection(full_connection_string_target)
if database_connect_source: 
    print(f'connection successful to {database_connect_source}')

full_connection_string_target = full_connection_string_server + '/test_db_schemas'
database_connect_target = connection.initialise_database_connection(full_connection_string_target)
if database_connect_target: 
    print(f'connection successful to {database_connect_target}')

# --- METADATA CREATION --- 
#TODO: Once the Source History Layer has completed, apply similar logic for the metadata layer to retain historical records. 
# Apply this new staged metadata table to the staging layer. 
# creating metadata schema 
metadata_schema = connection.create_schema(database_connect_target, 'metadata')

if metadata_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")
else: 
    print('schema already exists')
#TODO: Check if the current metadata_table exists 
#TODO: If the table exists, then perform the historical logic. 
# First Run: Add ETL columns to metadata table 
etl_timestamp = pd.Timestamp.now(tz="UTC")
metadata_df["etl_load_datetime"] = etl_timestamp
metadata_df["etl_effective_from"] = etl_timestamp
metadata_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
metadata_df["etl_record_indicator"] = 'N'

print(metadata_df)

# Upload the initial metadata table to database.
connection.upload_to_db(metadata_df, database_connect_target, 'metadata','metadata_table','replace')

# Upload the stage metadata table to the database to keep track of the history / changes 

#TODO: Add historical logic to metadata_table 

# -- STAGING LAYER -- 

# creating metadata schema 
staging_schema = connection.create_schema(database_connect_target, 'staging')

if staging_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")
else: 
    print('schema already exists')


# Extracting tables from source db 
metadata_table = pd.read_sql_table('metadata_table', con=database_connect_target, schema='metadata')

# Creating list of dictionaries using pd.to_dict() method 
metadata_table_dict = metadata_df.to_dict(orient="records")

source_tables_dict = {}

# Creating a dictionary of dataframes to upload to the database
for object in metadata_table_dict:
    if object['LOAD_TYPE'] == 'DELTA':
        table_check = connection.check_table("'staging'", f"'stg_job_{object['OBJECT_NAME']}'", database_connect_target)
        if table_check == True: 
            #Extract the HWM value
            hwm_value = connection.extract_hwm_value("staging", f"stg_job_{object['OBJECT_NAME']}", database_connect_target, object['HWM_VALUE'])
            object['WHERE_CLAUSE'] = (f"WHERE {object['HWM_VALUE']} > {hwm_value}")
            df = pd.read_sql(f"SELECT {object['DATA_ITEMS']} FROM {object['OBJECT_NAME']} {object['WHERE_CLAUSE']}", con=database_connect_source)
            df["etl_load_datetime"] = etl_timestamp
            df["etl_effective_from"] = etl_timestamp
            df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
            source_tables_dict[object['OBJECT_NAME']] = df 
        else:
            # Treat the table like it's a full load
            df = pd.read_sql(f"SELECT {object['DATA_ITEMS']} FROM {object['OBJECT_NAME']} {object['WHERE_CLAUSE']}", con=database_connect_source)
            etl_timestamp = pd.Timestamp.now(tz="UTC")
            df["etl_load_datetime"] = etl_timestamp
            df["etl_effective_from"] = etl_timestamp
            df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
            # set the key of the object name to the completed dataframe 
            source_tables_dict[object['OBJECT_NAME']] = df

    else:
        # Load the table in as a FULL load 
        df = pd.read_sql(f"SELECT {object['DATA_ITEMS']} FROM {object['OBJECT_NAME']} {object['WHERE_CLAUSE']}", con=database_connect_source)
        etl_timestamp = pd.Timestamp.now(tz="UTC")
        df["etl_load_datetime"] = etl_timestamp
        df["etl_effective_from"] = etl_timestamp
        df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
        # set the key of the object name to the completed dataframe 
        source_tables_dict[object['OBJECT_NAME']] = df 

# Uploading each of the tables
for key, value in source_tables_dict.items():
    connection.upload_to_db(value, database_connect_target, 'staging', f"stg_{metadata_df.iloc[1,0]}_{key}", 'replace')


#---- SOURCE HISTORY LAYER -----



# Creating source_history schema 

# source_history_schema = connection.create_schema(database_connect_target, 'history')
# # On the second run, compare rows between tables identifying different types of records

# # On the first run, load all tables in as normal adding a new column etl_record_indicator 

# # Create a column called etl_record_indicator, and set it to N for New 
# # Select all table names within the staging schema. Add these to a list
# stage_table_names_table = pd.read_sql_query(f"""SELECT * FROM information_schema.tables WHERE table_schema = 'staging';""", con=database_connect_target) 
# list_of_stage_table_names = stage_table_names_table['table_name'].to_list()

# history_table_dict = {}
# for table_name in list_of_stage_table_names:
#     if connection.check_table("'history'", f"'{table_name}'", database_connect_target) == False:
#         print(f'Table {table_name} does not exist')
#         stage_table = pd.read_sql_table(table_name, database_connect_target, schema='staging')
#         stage_table['etl_record_indicator'] = 'N'
#         # Update the etl_load_datetime and etl_effective_from fields 
#         now = pd.Timestamp.now(tz="UTC")
#         stage_table["etl_load_datetime"] = now
#         stage_table["etl_load_datetime"] = now
#         # Add the table_name and modified dataframe to the history_table_dict
#         history_table_dict[table_name] = stage_table

# # Upload the table to the history layer 1st run 
# for key, value in history_table_dict.items():
#     connection.upload_to_db(value, database_connect_target, 'history', f"history_{metadata_df.iloc[1,0]}_{key}", 'replace')




"""
Creating a column called ETL_RECORD_INDICATOR 
Should have the following 
New Records = N 
Identical Records = I 
Changed Records = C 
Deleted Records = D 
"""