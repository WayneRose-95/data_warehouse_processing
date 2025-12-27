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
print(conn_string_dict)

# Creating connection string to connect to database
secret_conn_string = conn_string_dict[0]

full_connection_string_server = secret_conn_string.replace('***', config_file['RDS_PASSWORD'])
print(full_connection_string_server)

new_db_name_dict = metadata_df['DATABASE_NAME'].to_dict()
new_db_name = new_db_name_dict[0]

full_connection_string = full_connection_string_server + f"/{new_db_name}"


database_connect = connection.initialise_database_connection(full_connection_string)
if database_connect: 
    print(f'connection successful to {database_connect}')

metadata_schema = connection.create_schema(database_connect, 'metadata')

if metadata_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")
else: 
    print('schema already exists')