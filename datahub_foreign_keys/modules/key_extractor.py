import pandas as pd
import pyodbc
import re

class MSSQLClient:
    """A client for connecting to a Microsoft SQL Server database."""
    def __init__(self, logger, config):
        self.logger = logger
        self.server = config['mssql']['server']
        self.database = config['mssql']['database']
        self.username = config['mssql']['username']
        self.password = config['mssql']['password']
        self.driver = config['mssql']['driver']
        self.sql_script_path = config['mssql']['sql_script_path']
        self.connection_string = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes;"

    
    def fetch_foreign_keys_info(self):
        """Fetch foreign key information from the database."""
        with open(self.sql_script_path, 'r') as file:
            sql_script = file.read()

        conn = pyodbc.connect(self.connection_string)        
        fks_info_df = pd.read_sql(sql_script, conn)
        self.logger.info("Fetched foreign key information from the database.")
        return fks_info_df


class KeysProcessing:
    """A class for processing foreign keys and primary keys given a DataFrame of foreign key information."""
    
    def __init__(self, logger, config, fks_info_df):
        self.logger = logger
        self.config = config
        self.fks_info_df = fks_info_df

    def extract_table_name(self, urn:str) -> str:
        """
        Extracts the table name from a DataHub dataset URN.
        Example: urn:li:dataset:(urn:li:dataPlatform:mssql,ekofisk.CubeDevTest.UCube.BridgeCompanySubsidiary,DEV)
        Returns: BridgeCompanySubsidiary
        """
        # Split on commas and take the second-to-last part, then split on '.' and take the last part
        try:
            self.logger.debug(f"Extracting table name from URN: {urn}")
            parts = urn.split(',')
            if len(parts) < 2:
                self.logger.warning("URN does not contain enough parts to extract table name.")
                return ""
            
            table_part = parts[-2]
            table_name = table_part.split('.')[-1]
            self.logger.info(f"Extracted table name: {table_name}")
            return table_name
        
        except Exception as e:
            self.logger.error(f"Error extracting table name from URN: {urn}. Exception: {e}")
            return ""
        

    def get_pks(self, urn: str, urn_json: dict) -> list:
        """
        Get primary keys for a given URN from the foreign keys info DataFrame.
        
        Args:
            urn (str): The dataset URN.
            urn_json (dict): The JSON response from DataHub containing schema metadata.

        Returns:
            list: List of primary key field paths.
        """
        pks = []

        # Get table name from the URN
        table_name = self.extract_table_name(urn)
        if not table_name:
            self.logger.warning(f"Could not extract table name from URN: {urn}")
            return []
        
        # Get info for the table schema from the urn
        fields_raw = urn_json['responses'][urn]['aspects']['schemaMetadata']['value']['fields']
        column_names = [f['fieldPath'] for f in fields_raw]

        # Any parent table's parent key can be taken to be PK for that table
        if table_name in self.fks_info_df['parent_table'].values:
            self.logger.info(f"{table_name} found in parent_table column")
            keys = self.fks_info_df.loc[self.fks_info_df['parent_table'] == table_name, 'parent_key'].unique()
            pks.extend(keys)
        else:
            self.logger.info(f"{table_name} NOT found in parent_table column")

        # Additionally, check for PK column names in the table itself
        for col in column_names:
            if re.match(r'PK\w+', col):
                pks.append(col)
                self.logger.info(f"Found PK column: {col}")

        self.logger.info(f"Primary keys for {table_name}: {pks}")

        return pks
    

    def get_fks(self, urn: str, server: str, db: str, schema: str) -> list:
        """
        Get foreign keys for a given URN from the foreign keys info DataFrame.
        
        Args:
            urn (str): The dataset URN.
            fks_info_df (pd.DataFrame): DataFrame containing foreign key information.
        
        Returns:
            list: List of foreign key dictionaries.
        """
        fks = []

        # Get table name from the URN
        table_name = self.extract_table_name(urn)
        if not table_name:
            print(f"Could not extract table name from URN: {urn}")
            return []

        # Check for foreign keys in the DataFrame
        if table_name in self.fks_info_df['child_table'].values:
            self.logger.info(f"{table_name} found in child_table column")
            fks_info = self.fks_info_df.loc[self.fks_info_df['child_table'] == table_name]
            for _, row in fks_info.iterrows():
                fk = {
                    "name": row['child_key'],
                    "sourceFields": [row['child_key']],
                    "foreignFields": [row['parent_key']],
                    "foreignDataset": f"urn:li:dataset:(urn:li:dataPlatform:mssql,{server}.{db}.{schema}.{row['parent_table']},DEV)"
                }
                fks.append(fk)
        else:
            self.logger.warning(f"{table_name} NOT found in child_table column")

        self.logger.info(f"Foreign keys for {table_name}: {fks}")

        return fks

        