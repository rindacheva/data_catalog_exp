from modules.populate_descriptions import CustomDescriptionsDC 
from src.table_descriptions_store import TABLES
import pandas as pd
import logging
import configparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read('datahub_descriptions/src/config.conf')

def push_metadata(tables_dict: dict[str: str]):
    env = config['DATAHUB']['env']
    gms_server = config['DATAHUB']['gms_server'] # Must point to 8080 for metadata emitting, not 9002
    token = config['DATAHUB']['token']

    logging.info("Starting metadata emitting process.")
    metadata_emitter = CustomDescriptionsDC(tables_dict=tables_dict, env=env, gms_server=gms_server, token=token)
    
    for table_name, table_desc in tables_dict.items():
        # Set urn from table name and the column description dictionary from the dataframe
        urn = metadata_emitter.urn_generator_ucube(table_name=table_name, env=env)
        metadata_emitter.table_desc_emitter(gms_server=gms_server,
                                            token=token,
                                            urn=urn,
                                            table_description=table_desc)
        
# Get tables from the table_description_store
tables_desc_dict = TABLES

# Push metadata to DataHub
column_desc_tables = push_metadata(tables_dict=tables_desc_dict)
