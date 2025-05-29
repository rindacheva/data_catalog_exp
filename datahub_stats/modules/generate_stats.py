import pyodbc
import pandas as pd
import logging
import requests
from urllib.parse import quote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MinMaxStats():
    """Class for generating min/max statistics for MSSQL views and tables"""

    def __init__(self, server: str, database: str, schema_name: str, view_name: str, username: str, password: str, driver: str):
        self.server = server
        self.database = database
        self.schema_name = schema_name
        self.view_name = view_name
        self.username = username
        self.password = password
        self.driver = driver
        logging.info("MinMaxStats initialized for server: %s with driver: %s", server, driver)


    def column_info(self, driver: str, server: str, database: str, schema_name: str, view_name: str, username: str, password: str) -> dict:
        """
        Function to get the column info: column names and data types.

        Input:
        driver      : the ODBC connection driver, usually {ODBC Driver 18 for SQL Server}
        server      : server name
        database    : database name
        schema_name : schema name
        view_name   : view name
        username    : username for MSSQL
        password    : MSSQL user password

        Output:
        columns_info : dictionary containing each column name as key, and its data type.

        """

        try:
            conn = pyodbc.connect(
                f'DRIVER={driver};SERVER={server};'
                f'DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes',
                timeout=10 
            )
            logging.info("Connected to db: %s successfully", database)
            
            cursor = conn.cursor()

            # Query to fetch column names and data types
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA='{schema_name}' AND TABLE_NAME='{view_name}'
            """
            logging.info("Running column info query: %s", columns_query)
            cursor.execute(columns_query)
            columns_info = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}
            logging.info("Got columns info.")

            return columns_info

        except Exception as e:
            logging.error(f"Error connecting to database: {e}")

        finally:
            # Close the connection
            if conn:
                conn.close()
                logging.info("Connection closed.")

    
    def min_max_query_creator(self, driver: str, server: str, database: str, schema_name: str, view_name: str, username: str, password: str) -> str:
        """
        Function to generate the query for min and max values of a column. 

        Note that this only works for the following SQL types and must be updated for other formats such as dates:
        int, smallint, bigint, decimal, numeric, float, real.

        Input:

        columns_info : a dictionary with the name of the column, and its associated data type
        schema_name  : schema name
        view_name    : view name

        Output:
        
        query_minmax : SQL query to get min and max values of every column

        """
        logging.info("Creating SQL min/max query.")

        # Get Column Info
        logging.info("Getting column info data...")    
        columns_info = self.column_info(server=server, database=database, schema_name=schema_name, view_name=view_name, username=username, password=password, driver=driver)

        # SQL data types that are numeric
        numeric_types = ['int', 'smallint', 'bigint', 'decimal', 'numeric', 'float', 'real']

        # Construct the SQL query
        query_parts = []
        for column, data_type in columns_info.items():
            is_numeric = data_type in numeric_types
            if is_numeric:
                query_part = f"""SELECT '{column}' AS column_name,
                                '{data_type}' AS data_type, 
                                (SELECT MIN([{column}]) FROM {schema_name}.{view_name}) AS min_val, 
                                (SELECT MAX([{column}]) FROM {schema_name}.{view_name}) AS max_val"""
            else:
                query_part = f"""SELECT '{column}' AS column_name,
                                '{data_type}' AS data_type, 
                                NULL AS min_val, 
                                NULL AS max_val"""
            query_parts.append(query_part)

        # Combine all parts with UNION ALL
        query_minmax = "\nUNION ALL \n\n".join(query_parts) + ";"

        logging.info("Min/max query generated.")

        return query_minmax
    

    def adjust_int_values_minmax(self, row):
        """
        Function to adjust and round integer values for min and max values.

        Input:

        row : a row from a dataframe which must contain columns with names min_val and max_val

        Output:

        row : updated rows according to the rounding

        """
        if row['data_type'] in ['int','smallint', 'bigint']:
            row['min_val'] = int(row['min_val']) if pd.notnull(row['min_val']) else None
            row['max_val'] = int(row['max_val']) if pd.notnull(row['max_val']) else None

        # Return the row unchanged for other data types
        return row
        

    def min_max_df(self, driver: str, server: str, database: str, schema_name: str, view_name: str, username: str, password: str) -> pd.DataFrame:
        """
        Function to obtain the dataframe with min and max values

        Input:
        driver       : the ODBC connection driver, usually {ODBC Driver 18 for SQL Server}
        server       : server name
        database     : database name
        schema_name  : schema name
        view_name    : view name
        username     : username for MSSQL
        password     : MSSQL user password
        query_minmax : query that obtains the min and max values of every column, and stores its data type

        """

        try:
            conn = pyodbc.connect(
                f'DRIVER={driver};SERVER={server};'
                f'DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes',
                timeout=10 
            )
            logging.info("Connected to db: %s successfully", database)
            cursor = conn.cursor()

            logging.info("Generating min/max query...")
            query_minmax = self.min_max_query_creator(server=server, database=database, schema_name=schema_name, view_name=view_name, username=username, password=password, driver=driver)    

            logging.info("Executing min/max query.")
            cursor.execute(query_minmax)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]

            df_minmax = pd.DataFrame.from_records(rows, columns=columns)
            logging.info("Got min max data.")

            # We want to correct any int, bigint or smallint values to integers in the dataframe
            df_minmax_corrected = df_minmax.apply(self.adjust_int_values_minmax, axis=1)
            logging.info("Rounded min/max integer data")

            return df_minmax_corrected

        except Exception as e:
            logging.error(f"Error connecting to database or executing query: {e}")

        finally:
            # Close the connection
            if conn:
                conn.close()
                logging.info("Connection closed.")


class SampleValues():
    """Class for generating sample value statistics for MSSQL views and tables"""

    def __init__(self, server: str, database: str, schema_name: str, table_name: str, username: str, password: str, driver: str, gms_server: str):
        self.server = server
        self.database = database
        self.schema_name = schema_name
        self.table_name = table_name
        self.username = username
        self.password = password
        self.driver = driver
        self.gms_server = gms_server
        logging.info("SampleValues initialized for server: %s with driver: %s, and gms_server: %s", server, driver, gms_server)

    class MSSQLConnection:
        """Class to initialize connection to MSSQL, and close connection automatically."""

        def __init__(self, server: str, database: str, username: str, password: str, driver: str):
            """
            Initialize the connection manager with the database details.
            
            server      : server name
            database    : database name
            username    : username for MSSQL
            password    : MSSQL user password
            driver      : the ODBC connection driver, usually {ODBC Driver 18 for SQL Server}
            
            """
            self.server = server
            self.database = database
            self.username = username
            self.password = password
            self.driver = driver
            self.conn = None

        def __enter__(self) -> pyodbc.Connection:
            """
            Establishes the database connection and returns it.
            """
            self.conn = pyodbc.connect(
                f'DRIVER={self.driver};SERVER={self.server};'
                f'DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes',
                timeout=10
            )
            logging.info("Connected to db: %s successfully", self.database)
            return self.conn

        def __exit__(self):
            """
            Closes the database connection when exiting the context.
            """
            if self.conn:
                self.conn.close()
                logging.info("Connection to db: %s closed successfully", self.database)


    def get_string_columns(self, urn: str, gms_server: str, token: str) -> list[str]:
        """
        Function to get all string columns in a dataset on DataHub.

        Input:

        urn        : urn of the dataset in format
        gms_server : server where we host datahub, port 8080
        token      : authentication token for datahub

        Output:

        List of column names of the dataset.

        """

        logging.info(f"Getting columns for urn: {urn}")

        url = f'{gms_server}/openapi/entities/v1/latest?urns={quote(urn)}'

        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {token}',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            # Get the list of columns which are of type string
            schema_metadata = response.json()['responses'][urn]['aspects']['schemaMetadata']['value']['fields']
            string_columns = [field['fieldPath'] for field in schema_metadata if field['type']['type'].get('__type') == 'StringType']

            logging.info(f"Got {len(string_columns)} string columns for dataset: {urn}")
            
            return string_columns

        else:

            logging.warning(f"Couldn't get columns for urn: {urn}, HTTP Status Code: {response.status_code}")
            logging.info(f"Error: {response.json()}")

    

    def column_distinct_values(self, database: str, schema_name: str, table_name: str, column_name: str, distinct_max=10000) -> list[str]:
        """
        Function to get all the distinct values from a string column.capitalize

        Input:

        conn         : database connection from connection function
        database     : database name
        schema_name  : schema name
        table_name   : table name
        column_name  : column name
        distinct_max : maximum number of distinct values, defaults to 100000
        
        Output:

        List of sample values for a given column (strings)

        """
        with self.MSSQLConnection(self.server, self.database, self.username, self.password, self.driver) as conn:
            cursor = conn.cursor()

            # Check how many distinct values there are
            count_query = f"""SELECT COUNT(DISTINCT [{column_name}]) FROM [{database}].[{schema_name}].[{table_name}]"""
            cursor.execute(count_query)

            count = cursor.fetchone()
            distinct_count = count[0] if count else None  

            logging.info(f"Distinct values in column {column_name}: {distinct_count}")

            if distinct_count <= int(distinct_max): # Set limit of how many distinct values we have.

                logging.info(f"Getting column values for column: {column_name} in table: {schema_name}.{table_name}")

                distinct_query = f"""SELECT DISTINCT [{column_name}] FROM [{database}].[{schema_name}].[{table_name}]"""
                cursor.execute(distinct_query)

                results = cursor.fetchall()

                # Extract the distinct values into a list of strings
                distinct_values = [str(row[0]) for row in results]

                logging.info(f"Got distinct values for column: {column_name}")
                
                return distinct_values

            else:

                logging.warning(f"Too many values in column: {column_name}")
                return []

