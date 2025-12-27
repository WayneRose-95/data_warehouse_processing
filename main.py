from database_utils import DatabaseConnector

connection = DatabaseConnector() 

config_file = connection.read_database_credentials('db_creds.yaml')
# print(config_file)
conn_string = connection.create_connection_string(config_file, connect_to_database=True, new_db_name=config_file['RDS_DATABASE'])
# print(conn_string)
database_connect = connection.initialise_database_connection(conn_string)
if database_connect: 
    print(f'connection successful to {database_connect}')

metadata_schema = connection.create_schema(database_connect, 'metadata')

if metadata_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")