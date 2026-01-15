from database_utils import DatabaseConnector
from table_history import TableHistoryProcessor
from sqlalchemy import text
import pandas as pd 

# Instantiation of Classes 
connection = DatabaseConnector() 
table_histories = TableHistoryProcessor() 
# Reading in backend secrets configuration file 
config_file = connection.read_database_credentials('db_creds.yaml')
# Creating pandas dataframe from excel metadata spreadsheet 
metadata_df = pd.read_excel('metadata.xlsx')
print(metadata_df)

etl_source_code = metadata_df.iloc[1,0]
# Extracting connection string from metadata excel spreadsheet 
conn_string_dict = metadata_df['connection'].to_dict()


# Creating connection string to connect to database
secret_conn_string = conn_string_dict[0]
full_connection_string_server = secret_conn_string.replace('***', config_file['RDS_PASSWORD'])
# print(full_connection_string_server)

# Extract database_name from the metadata spreadsheet 
new_db_name_dict = metadata_df['database_name'].to_dict()
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
metadata_table_check = connection.check_table("'metadata'", f"'stage_data_objects_source'", database_connect_target)
if metadata_table_check == True: 
    print('Table already exists')
    # Upload the initial load_metadata table 
    etl_timestamp = pd.Timestamp.now(tz="UTC")
    metadata_df["etl_load_datetime"] = etl_timestamp
    metadata_df["etl_effective_from"] = etl_timestamp
    metadata_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
    metadata_df["etl_record_indicator"] = 'N'
    metadata_df['etl_active_flag'] = True
    # Upload the initial metadata table to database.
    connection.upload_to_db(metadata_df, database_connect_target, 'metadata','load_data_objects_source','replace')

    # Read in current metadata table 
    current_metadata_table = pd.read_sql_table('load_data_objects_source', database_connect_target, schema='metadata')
    metadata_df['row_hash'] = table_histories.build_row_hash(metadata_df, None)
    current_metadata_table['row_hash'] = table_histories.build_row_hash(current_metadata_table, None)
    comparison_df = table_histories.compare_new_vs_exisiting(current_metadata_table, metadata_df, metadata_df['business_keys'], 'outer')
    record_indicator_df = table_histories.assign_record_indicator(comparison_df)
    # Removing old columns and replacing them with the new columns.
    cleaned_df = (
    record_indicator_df
    .drop(columns=record_indicator_df.filter(regex="_old$").columns)
    # Rename the _new columns
    .rename(columns=lambda c: c.replace("_new", ""))
    )
    cleaned_df.drop(columns=['row_hash', 'key_0'], inplace=True)
    cleaned_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
    connection.upload_to_db(cleaned_df, database_connect_target, 'metadata', 'stage_data_objects_source', 'append')
    # Updating the etl_active_record column post load 
    update_active_sql = text("""
    UPDATE metadata.stage_data_objects_source
    SET etl_active_flag = TRUE
    WHERE etl_record_indicator IN ('C', 'I');
    """)

    update_inactive_sql = text("""
    UPDATE metadata.stage_data_objects_source
    SET etl_active_flag = FALSE
    WHERE etl_record_indicator = 'N';
    """)

    update_current_records_sql = text(
    """
    UPDATE metadata.stage_data_objects_source sm
    SET etl_active_flag = CASE
        WHEN sm.etl_effective_from = latest.max_effective_from THEN TRUE
        ELSE FALSE
    END
    FROM (
        SELECT
            SOURCE,
            SOURCE_TYPE,
            OBJECT_NAME,
            MAX(etl_effective_from) AS max_effective_from
        FROM metadata.stage_data_objects_source
        WHERE etl_record_indicator IN ('C', 'I')
        GROUP BY SOURCE, SOURCE_TYPE, OBJECT_NAME
    ) latest
    WHERE sm.SOURCE = latest.SOURCE
    AND sm.SOURCE_TYPE = latest.SOURCE_TYPE
    AND sm.OBJECT_NAME = latest.OBJECT_NAME;
    """
    )
    with database_connect_target.begin() as conn:
        conn.execute(update_active_sql)
        conn.execute(update_inactive_sql)
        conn.execute(update_current_records_sql)
else:
    #TODO: If the table exists, then perform the historical logic. 
    # First Run: Add ETL columns to metadata table 
    etl_timestamp = pd.Timestamp.now(tz="UTC")
    metadata_df["etl_load_datetime"] = etl_timestamp
    metadata_df["etl_effective_from"] = etl_timestamp
    metadata_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
    metadata_df["etl_record_indicator"] = 'N'
    metadata_df["etl_active_flag"] = True

    print(metadata_df)

    # Upload the initial metadata table to database.
    connection.upload_to_db(metadata_df, database_connect_target, 'metadata','load_data_objects_source','replace')

    etl_timestamp = pd.Timestamp.now(tz="UTC")
    # Update the metadata columns for the staging metadata table 
    metadata_df["etl_load_datetime"] = etl_timestamp
    metadata_df["etl_effective_from"] = etl_timestamp
    metadata_df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
    metadata_df["etl_record_indicator"] = 'N'
    metadata_df['etl_active_flag'] = True

    connection.upload_to_db(metadata_df, database_connect_target, 'metadata','stage_data_objects_source','replace')



# Upload the stage metadata table to the database to keep track of the history / changes 

# -- STAGING LAYER -- 

# creating metadata schema 
staging_schema = connection.create_schema(database_connect_target, 'staging')

if staging_schema:
    print(f"schema created on {config_file['RDS_DATABASE']}")
else: 
    print('schema already exists')


# Extracting tables from source db 
metadata_table = pd.read_sql_table('load_data_objects_source', con=database_connect_target, schema='metadata')

# Creating list of dictionaries using pd.to_dict() method 
metadata_table_dict = metadata_df.to_dict(orient="records")

source_tables_dict = {}

# Creating a dictionary of dataframes to upload to the database
for object in metadata_table_dict:
    if object['load_type'] == 'DELTA':
        table_check = connection.check_table("'staging'", f"'stage_{etl_source_code}_{object['object_name']}'", database_connect_target)
        if table_check == True: 
            #Extract the HWM value
            hwm_value = connection.extract_hwm_value("staging", f"stage_{etl_source_code}_{object['object_name']}", database_connect_target, object['hwm_value'])
            object['where_clause'] = (f"WHERE {object['hwm_value']} > {hwm_value}")
            df = pd.read_sql(f"SELECT {object['data_items']} FROM {object['object_name']} {object['where_clause']}", con=database_connect_source)
            df["etl_load_datetime"] = etl_timestamp
            df["etl_effective_from"] = etl_timestamp
            df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
            source_tables_dict[object['object_name']] = df 
        else:
            # Treat the table like it's a full load
            df = pd.read_sql(f"SELECT {object['data_items']} FROM {object['object_name']} {object['where_clause']}", con=database_connect_source)
            etl_timestamp = pd.Timestamp.now(tz="UTC")
            df["etl_load_datetime"] = etl_timestamp
            df["etl_effective_from"] = etl_timestamp
            df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
            # set the key of the object name to the completed dataframe 
            source_tables_dict[object['object_name']] = df

    else:
        # Load the table in as a FULL load 
        df = pd.read_sql(f"SELECT {object['data_items']} FROM {object['object_name']} {object['where_clause']}", con=database_connect_source)
        etl_timestamp = pd.Timestamp.now(tz="UTC")
        df["etl_load_datetime"] = etl_timestamp
        df["etl_effective_from"] = etl_timestamp
        df["etl_effective_to"] = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")
        # set the key of the object name to the completed dataframe 
        source_tables_dict[object['object_name']] = df 

# Uploading each of the tables
for key, value in source_tables_dict.items():
    connection.upload_to_db(value, database_connect_target, 'staging', f"stage_{etl_source_code}_{key}", 'replace')


#---- SOURCE HISTORY LAYER -----



# Creating source_history schema 

source_history_schema = connection.create_schema(database_connect_target, 'history')
# # # On the second run, compare rows between tables identifying different types of records

# # # On the first run, load all tables in as normal adding a new column etl_record_indicator 

# # # Create a column called etl_record_indicator, and set it to N for New 
# # # Select all table names within the staging schema. Add these to a list
metdata_table_names = pd.read_sql_table("stage_data_objects_source", con=database_connect_target, schema='metadata') 
stage_table_names_table = pd.read_sql_query(f"""SELECT * FROM information_schema.tables WHERE table_schema = 'staging';""", con=database_connect_target) 
current_records_table = metdata_table_names[metdata_table_names['etl_active_flag'] == True]
list_of_stage_table_names = stage_table_names_table['table_name'].to_list()
metadata_table_dict_reference = metdata_table_names.to_dict() 


history_table_dict = {}
for table_name in list_of_stage_table_names:
    if connection.check_table("'history'", f"'{table_name}'", database_connect_target) == False:
        print(f'Table {table_name} does not exist')
        stage_table = pd.read_sql_table(table_name, database_connect_target, schema='staging')
        stage_table['etl_record_indicator'] = 'N'
        # Update the etl_load_datetime and etl_effective_from fields 
        now = pd.Timestamp.now(tz="UTC")
        stage_table["etl_load_datetime"] = now
        stage_table["etl_load_datetime"] = now
        # Add the table_name and modified dataframe to the history_table_dict
        table_name = table_name.replace(f'stage_{etl_source_code}_', '')
        history_table_dict[table_name] = stage_table
    else:
        #TODO: Implement Historical Table logic here or implement it above assuming check_table returns True
        pass

# Upload the table to the history layer 1st run 
for key, value in history_table_dict.items():
    connection.upload_to_db(value, database_connect_target, 'history', f"history_{etl_source_code}_{key}", 'replace')




"""
Creating a column called ETL_RECORD_INDICATOR 
Should have the following 
New Records = N 
Identical Records = I 
Changed Records = C 
Deleted Records = D 
"""