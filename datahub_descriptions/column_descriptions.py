from modules.scrape_tables import TableScraperDC 
from modules.populate_descriptions import CustomDescriptionsDC 
import pandas as pd
import logging
import configparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read('datahub_descriptions/src/config.conf')

def get_column_descriptions():
    # Set pdf path location to the desired pdf file, in this case the UCube structure
    pdf_path = config['FILES']['pdf_path']
    xlsx_path = config['FILES']['xlsx_path']
    scraper = TableScraperDC(pdf_path=pdf_path, xlsx_path=xlsx_path)
    
    formatted_tables = scraper.extract_tables_from_xlsx(xlsx_path=xlsx_path)
    logging.info("Number of tables extracted: %s.", len(formatted_tables.keys()))

    if formatted_tables:
        logging.info("Formatted tables are ready for use.")
        return formatted_tables
    else:
        logging.info("No tables were extracted or formatted.")
        return{}

    # # FOR PDF USAGE
    # if tables:
    #     formatted_tables = scraper.ucube_format(tables)
    #     logging.info("Formatted tables are ready for use.")
    #     return formatted_tables
    # else:
    #     logging.info("No tables were extracted or formatted.")
    #     return{}

def push_metadata(tables_dict: dict[str, pd.DataFrame]):
    env = config['DATAHUB']['env']
    gms_server = config['DATAHUB']['gms_server'] # Must point to 8080 for metadata emitting, not 9002
    token = config['DATAHUB']['token']

    logging.info("Starting metadata emitting process.")
    metadata_emitter = CustomDescriptionsDC(tables_dict=tables_dict, env=env, gms_server=gms_server, token=token)

    for table_name, table_df in tables_dict.items():
        # Set urn from table name and the column description dictionary from the dataframe
        urn = metadata_emitter.urn_generator_ucube(table_name=table_name, env=env)
        column_description_dict = table_df.set_index('column_name')['description'].to_dict()
        metadata_emitter.column_desc_emitter(gms_server=gms_server, token=token, urn=urn, column_dict=column_description_dict)

# Call the functions
        
# Get column descriptions
column_desc_tables = get_column_descriptions()
# Push metadata to DataHub
push_metadata(tables_dict=column_desc_tables)
