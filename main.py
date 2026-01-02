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
# creating metadata schema 
metadata_schema = connection.create_schema(database_connect_target, 'metadata')

if metadata_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")
else: 
    print('schema already exists')

# Add ETL columns to metadata table 
etl_timestamp = pd.Timestamp.now(tz="UTC")
metadata_df["etl_load_datetime"] = etl_timestamp
metadata_df["etl_effective_from"] = etl_timestamp
metadata_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")

print(metadata_df)

# Upload metadata table to database.
connection.upload_to_db(metadata_df, database_connect_target, 'metadata','metadata_table','replace')


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

