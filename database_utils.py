import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP, INTEGER, UUID, DATETIME, DECIMAL, BIGINT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateSchema
import pandas as pd
import os
import logging




class DatabaseConnector:
    def __init__(self):
                self.type_mapping =  {
            "BIGINT": BIGINT,
            "VARCHAR": VARCHAR,
            "DATE": DATE,
            "FLOAT": FLOAT,
            "SMALLINT": SMALLINT,
            "BOOLEAN": BOOLEAN,
            "TIME": TIME,
            "NUMERIC": NUMERIC,
            "TIMESTAMP": TIMESTAMP,
            "INTEGER": INTEGER,
            "UUID": UUID, 
            "DECIMAL": DECIMAL,
            "DATETIME": DATETIME 
        }
    def read_database_credentials(self, config_file_name : str):
        """
        Method to read database_credentials from a yaml file

        Parameters:
        config_file
        The file pathway to the yaml file

        Returns:
        database_credentials : dict
        A dictionary of the database credentials from yaml file

        """
        try:
        
            with open(config_file_name) as file:
                database_credentials = yaml.safe_load(file)

            # Return the yaml file as a dictionary
            return database_credentials
        # If the file is not found, raise an exception
        except FileNotFoundError:
            raise FileNotFoundError("Config file not found")
        # If the config file is not in a YAML format, raise an exception
        except yaml.YAMLError:
            raise yaml.YAMLError("Invalid YAML format.")

    def create_connection_string(
        self, database_credentials : dict , connect_to_database=False, new_db_name=None
    ):
        """
        Method to create the connection_string needed to connect to a postgresql database

        Parameters:
        config_file_name:
        The pathway to the config_file used to create the string

        connect_to_database: bool
        Flag indicating whether to connect to a specific database within the server (default: False)

        new_db_name: str
        The name of the new database to connect to if connect_to_database is True (default: None)

        Returns:
        Connection string : str
        A string used to connect to the database

        """
        # i.e. recieves the wrong weapon type
        if not database_credentials:
            raise Exception("Invalid database credentials.")

        connection_string = f"{database_credentials['DATABASE_TYPE']}+{database_credentials['DBAPI']}://{database_credentials['RDS_USER']}:{database_credentials['RDS_PASSWORD']}@{database_credentials['RDS_HOST']}:{database_credentials['RDS_PORT']}"

        if connect_to_database:
            if not new_db_name:
                raise ValueError("New database name not provided")
            connection_string += f"/{new_db_name}"

        return connection_string

    def initialise_database_connection(
        self,
        connection_string : str,
        isolation_level="AUTOCOMMIT",
    ):
        """
        Method to establish a connection to the database

        Parameters:
        config_file_name: str
        The file pathway to the yaml file

        connect_to_database: bool
        Flag indicating whether to connect to a specific database within the server (default: False)

        new_db_name: str
        The name of the new database to connect to if connect_to_database is True (default: None)

        Returns:
        database_engine: engine
        A database engine object

        isolation_level : str = AUTOCOMMIT

        The level of isolation for the database transaction.
        By default, this is set to AUTOCOMMIT


        """

        # Lastly try to connect to the database using your connection string variable
        try:

            database_engine = create_engine(
                connection_string, isolation_level=isolation_level
            )

            database_engine.connect()

            return database_engine
        except OperationalError:

            print("Error Connecting to the Database")
            raise OperationalError

    def list_db_tables(self, engine : Engine): 
        """
        Method to list the tables within a database

        Parameters:
        engine : Engine 
        The database Engine object which is used to interact with the source or target database

        """
        try:
            # Initilise the database connection using the Engine object 

            # database_utils_logger.debug(f"Using {engine}")

            # Use the inspect method of sqlalchemy to get an inspector element
            inspector = inspect(engine)

            # Get the table names using the get_table_names method
            table_names = inspector.get_table_names()
            # print the table names to the console
            print(table_names)

            # Output: ['legacy_store_details', 'legacy_users', 'orders_table']
            # Return the list of table names as an output 
            return table_names
        
        # Raise an Exception if there are any issues with listing the tables.   
        except Exception as e:
            print("Error occurred while listing tables: %s", str(e))
            raise Exception
        
    def parse_column_type(self, column_type : str):
            """
            Method to parse a column type from a string 

            Parameters
            ---------- 

                column_type : str 

                    The type of column in a string format 

            Returns
            -------

                column_type_object : dict 

                    A dictionary which matches the column type to an integer 

            """
            # Split the desired column name by the first occurence of the '(' character
            # NOTE: the *column_parameters will set everything else that is split to a list. 
            column_type_name, *column_parameters = column_type.split('(')
            # Strip the Whitespace from the column_name 
            column_type_name = column_type_name.strip()
            # For the column_parameters variable select the first element of the list,
            # then remove the last character from the list. 
            # Afterwards, apply the split method to split the rest of the string by ','s
            # If the column_parameters variable is empty return an empty list
            column_parameters = column_parameters[0][:-1].split(',') if column_parameters else []
            # Map the variable to a SQLAlchemy Type
            column_type_object = self.type_mapping[column_type_name]
            # If condition for if column_parameters is not empty
            if column_parameters:
                # convert each value in the column_parameter variable to an integer
                column_type_object = column_type_object(*map(int, column_parameters))
            return column_type_object

    def generate_table_schema(self, table_name : str, columns : dict):
        """
        Method to generate the schema for a desired table name
        Utilising SQLAlchemy's ORM syntax 

        Parameters
        ---------- 

            table_name : str 

                The name of the table. 

            columns : dict 

                A dictionary containing key-value pairs for each of the columns

        Returns
        -------

            table : Table 

                A Table object representing the table to be created inside the database
        """
        # Create a MetaData() object
        metadata = MetaData()
        table_columns = []
        # Iterate through the columns dictionary
        for column_name, column_type in columns.items():
            # In each iteration, call the parse_column_type function
            column_type_object = self.parse_column_type(column_type)
            # Append the tuple to the list of table_columns
            table_columns.append(Column(column_name, column_type_object))
        # Create a Table object based on the metadata and schema of the columns dictionary
        table = Table(table_name, metadata, *table_columns)

        # Print detailed schema information
        print(f"Table: {table_name}")
        for column in table.columns:
            print(f"Column: {column.name}, Type: {column.type}")
        
        return table    

    def upload_to_db(
        self,
        dataframe: pd.DataFrame,
        connection : Engine,
        target_schema : str,
        table_name: str,
        table_condition : str = "append" or "replace" or "fail",
        schema_config=None
    ):
        """
        Method to upload the table to the database

        Parameters:
        dataframe : pd.DataFrame
        A pandas dataframe

        connection
        The engine to connect to the database

        table_name : str
        The name of the table to be uploaded to the database

        table_condition : str = "append" or "replace" or "fail" 
        The table condition specified either append, replace or fail

        mapping : dict = None 
        An optional parameter to apply mapping to the dataframe. 
        By default, it is None 

        subset : list = None 
        An optional parameter to filter the dataframe by a list of values. 
        By defult, it is None 

        additional_rows : list = None 
        An optional parameter to add additional rows to the start of the dataframe. 
        By default, it is None 
        """
        try:

            if schema_config:
                # Upload with a schema attached
                column_types = schema_config["schemas"]["tables"][table_name]
                table_schema = self.generate_table_schema(table_name, column_types)
                dataframe.to_sql(table_name, schema=target_schema, con=connection, if_exists=table_condition, method='multi', dtype={col.name: col.type for col in table_schema.columns}, index=False)
            else:
                # Upload with no schema attached
                dataframe.to_sql(table_name, schema=target_schema, con=connection, if_exists=table_condition, method='multi', index=False)

        except:

            print("Error uploading table to the database")
            raise Exception
    
    def create_schema(self, database_engine : Engine, schema_name : str):

        conn = database_engine.connect() 
        if not conn.dialect.has_schema(conn, schema_name):
            conn.execute(CreateSchema(schema_name, if_not_exists=True))
            return conn 
        
    def create_database(self, database_name: str, connection_string : str):
        # Create the database with the provided database_name and database_username
        
        connection = create_engine(connection_string)
        database_engine = connection.connect()
        if database_engine:
            # Disable transactional behavior
            database_engine.execution_options(isolation_level="AUTOCOMMIT")

            # Check if the database exists
            check_db_stmt = text(
                f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
            )
            result = database_engine.execute(check_db_stmt)

            if result.scalar():
                # Database already exists, so skip database creation
                print(
                    f"Database '{database_name}' already exists. Skipping database creation."
                )

            else:
                # Create a new database
                create_db_stmt = text(f"CREATE DATABASE {database_name}")
                database_engine.execute(create_db_stmt)
                print(f"Database '{database_name}' created successfully.")


            # Close the connection
            database_engine.close()
        else:
            print("Connection to the database failed.")
   
    def alter_and_update(self, sql_file_path: str, database_engine : Engine):
        # Create a session object using sessionmaker
        sql_session = sessionmaker(bind=database_engine)
        session = sql_session()

        try:
            with open(sql_file_path, "r") as file:
                sql_statement = file.read()
                print(sql_statement)

            session.execute(text(sql_statement))
            session.commit()

            print("SQL statement submitted to database. Please verify.")
        except:

            print(
                f"Error when running sql_statement. The sql statement submitted was {sql_statement}"
            )
            raise Exception

        finally:
            session.close()

if __name__ == "__main__":
    # yaml_file_path = "../credentials/db_creds.yaml"
    new_database = DatabaseConnector()
    # conn_string = new_database.create_connection_string(yaml_file_path)
    # new_database.initialise_database_connection(yaml_file_path, connect_to_database=True, new_db_name='postgres')