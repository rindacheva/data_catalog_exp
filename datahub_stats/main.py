from modules.generate_stats import MinMaxStats, SampleValues
from modules.push_stats import PushStats
import pandas as pd
import logging
import configparser
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read('datahub_stats/src/config.conf')

# DataHub configuration parameters
token = config['DATAHUB']['token']
gms_server = config['DATAHUB']['gms_server']
# max_distinct = config['DATAHUB']['max_distinct']

# Database connection parameters
server = config['MSSQL']['server']
database = config['MSSQL']['database']
username = config['MSSQL']['username']
password = config['MSSQL']['password']
driver = config['MSSQL']['driver'] # The scripts use ODBC connection, so the driver is typically {ODBC Driver 18 for SQL Server}


def main():
    "Main function"

    start_time = time.time()  # Start timer

    domain = "ucube_poc_v2"
    push_stats = PushStats(token=token, domain=domain, gms_server=gms_server)

    # Get list of all urns in the domain
    logging.info("Getting URNs for domain %s ...", domain)
    urn_ls = push_stats.get_urns_for_domain(token=token, domain=domain, gms_server=gms_server)

    for urn in urn_ls:

        logging.info("Initiating stats process for urn: %s", urn)

        parts = urn.split(',')
        # Extract the relevant part of the URN and split by dots
        relevant_part = parts[1].split('.')
        schema_name = relevant_part[-2]
        view_name = relevant_part[-1] 

        logging.info("Generating stats for view %s, in schema %s", view_name, schema_name)
        minmax_stats = MinMaxStats(server=server, database=database, schema_name=schema_name, view_name=view_name, username=username, password=password, driver=driver)
        # sample_val_stats = SampleValues(server=server, database=database, schema_name=schema_name, table_name=view_name, username=username, password=password, driver=driver, gms_server=gms_server)

        logging.info("Getting min/max data")
        df_stats = minmax_stats.min_max_df(server=server, database=database, schema_name=schema_name, view_name=view_name, username=username, password=password, driver=driver)

        # # Add sample val column to df
        # df_stats['sample_val'] = None # She's empty now but ready to be filled with charisma, uniqueness, nerve, and talent

        # logging.info("Getting sample values for string columns")
        # # Get all string columns in the table
        # string_columns = sample_val_stats.get_string_columns(urn=urn, gms_server=gms_server, token=token)
        
        # logging.info("Getting sample values for columns in dataset: %s", view_name)
        # for column_name in string_columns:
        #     if column_name in df_stats['column_name'].values:
        #         # Is she ready to show us what she's got? Let's check, darling
        #         sample_values = sample_val_stats.column_distinct_values(database=database, schema_name=schema_name, table_name=view_name,column_name=column_name, distinct_max=max_distinct)
        #         # Add the list of sample values to the correct column
        #         df_stats.loc[df_stats['column_name'] == column_name, 'sample_val'] = df_stats.loc[df_stats['column_name'] == column_name, 'sample_val'].apply(lambda x: sample_values)

        # logging.info("Got sample values for string columns.")

        # Now, let's give df_stats the performance of a lifetime by sending it to DataHub
        logging.info("Initiating DataHub population of stats...")
        push_stats.push_min_max_stats(df_minmax=df_stats, view_urn=urn, gms_server=gms_server, token=token)
        

    end_time = time.time()  # End timer
    elapsed_time = end_time - start_time 
    logging.info(f"Execution time: {elapsed_time:.2f} seconds")
    logging.info("END")

if __name__ == "__main__":
    main()