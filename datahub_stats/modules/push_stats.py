from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    DatasetFieldProfileClass
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from time import time
import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PushStats():
    """Class for pushing statistics for MSSQL views and tables"""

    def __init__(self, token:str, domain:str, gms_server: str):
        self.token = token
        self.domain = domain
        self.gms_server = gms_server
        logging.info("PushStats initialized for domain: %s ", domain)

    def get_urns_for_domain(self, token: str, domain:str, gms_server: str) -> list:
        """ 
        FUNCTION TAKEN FROM ForeignKeyMapper.py on feature/categorical-value-ingestion

        Get all URNs for a given domain from DataHub. 
        
        Returns:
        - list: A list of URNs for the given schema.
        """

        logging.info("Getting URNs for domain: %s", domain)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        variables = {
        "domain": domain
        }

        data = {
            "query": """query SearchEntitiesInDomain($domain: String!) {
                listDomains(input: {
                    query: $domain
                    start: 0,
                    count: 1,
                }) {
                    domains {
                        urn,
                        type,
                        entities {
                            total,
                            searchResults {
                                entity {
                                    type,
                                    urn,
                                }
                            }
                        }
                    }
                }
            }""",
            "variables": variables
        }

        response = requests.post(f"{gms_server}/api/graphql", headers=headers, json=data)
        if response.status_code != 200:
            logging.error("Error fetching URNs: %s", response.status_code)
            raise Exception(f"Error fetching URNs: {response.status_code}")
        
        results_json = response.json()["data"]["listDomains"]["domains"][0]["entities"]["searchResults"]
        
        logging.info("Fetched urns for domain: %s", domain)
        return [result["entity"]["urn"] for result in results_json]


    def create_dataset_field_profile_class_minmax(self, column_name: str, min_val: str, max_val: str) -> DatasetFieldProfileClass:
        """
        Function to create a DatasetFieldProfileClass for a given set of variables for min and max values.

        Input:

        column_name : name of column we take stats for
        min_val     : minimum value of the column, as a string
        max_val     : maximum value of the column, as a string

        Output:

        DatasetFieldProfileClass for the stats of the column, for example
            dataset_field_profile_Fk_Geography = DatasetFieldProfileClass(
                fieldPath = 'Fk_Geography',
                min = '1',
                max = '83774'
            )
        """

        logging.info("Creating DatasetFieldProfile class for column: %s ...", column_name)
        
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath = column_name,
            min = min_val,
            max = max_val
        )

        logging.info("Created DatasetFieldProfile class for column: %s", column_name)

        return dataset_field_profile
    
    def create_dataset_field_profile_class_all_stats(self, column_name: str, min_val: str, max_val: str, sample_val : list[str]) -> DatasetFieldProfileClass:
        """
        Function to create a DatasetFieldProfileClass for all stats. Currently these are min, max and sample values, 
        but this function should be updated to incorporate more stats.

        Input:

        column_name : name of column we take stats for
        min_val     : minimum value of the column, as a string
        max_val     : maximum value of the column, as a string
        sample_val  : list of sample string values

        Output:

        DatasetFieldProfileClass for the stats of the column, for example
            dataset_field_profile_Asset = DatasetFieldProfileClass(
                fieldPath = 'Asset',
                min = 'None',
                max = 'None',
                sampleValues = ['0005 Deszk-I, HU , ...']
            )

        Of course, min max will be populated for numerical columns as before.
        """

        logging.info("Creating DatasetFieldProfile class for column: %s ...", column_name)
        
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath = column_name,
            min=min_val,
            max=max_val,
            sampleValues= sample_val
        )

        logging.info("Created DatasetFieldProfile class for column: %s", column_name)

        return dataset_field_profile
    

    def push_min_max_stats(self, df_minmax: pd.DataFrame, view_urn: str, gms_server: str, token: str) -> None:
        """
        Function to push the min/max stats for a particular view to DataHub.

        Input:

        df_minmax : a pandas dataframe with 4 columns, containing values for a particular view:
                    - column_name, data_type, min_val, max_val
        
        view_urn  : the urn for the view which we are pushing stats for

        Output:

        This queen returns None, it just sashays away all the min/max stats into DataHub.

        """
        # Create list of field profile properties for a particular view, containing the field profile class of each column

        logging.info("Generate list of field profiles for the columns of view: %s", view_urn)

        field_profiles_ls = []

        for index, row in df_minmax.iterrows():
            field_profile = self.create_dataset_field_profile_class_minmax(
                column_name = str(row['column_name']),
                min_val = str(row['min_val']),
                max_val = str(row['max_val'])
            )
            field_profiles_ls.append(field_profile)

        logging.info("Setting up emitter...")
        emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time() * 1000),
            fieldProfiles=field_profiles_ls
        )

        mce = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=view_urn,
            aspectName="datasetProfile",
            aspect=dataset_profile
            )

        logging.info("Emitting stats for view: %s", view_urn)
        emitter.emit(mce)

        logging.info("Pushed stats for urn: %s", view_urn)
        logging.info("Closing emitter.")
        emitter.close()


    def push_all_stats(self, df_stats: pd.DataFrame, table_urn: str, gms_server: str, token: str) -> None:
        """
        Function to push the all stats into DataHub. Currently it handles min, max and sample values, but 
        in the future it can be expanded into more stats.

        Input:

        df_stats : a pandas dataframe with 5 columns, containing values for a particular view:
                    - column_name, data_type, min_val, max_val, sample_val
        table_urn  : the urn for the view which we are pushing stats for

        Output:

        This queen returns None, it just sashays away all the stats into DataHub.

        """
        # Create list of field profile properties for a particular view, containing the field profile class of each column

        logging.info("Generate list of field profiles for the columns of view: %s", table_urn)

        field_profiles_ls = []

        for index, row in df_stats.iterrows():
            field_profile = self.create_dataset_field_profile_class_all_stats(
                column_name = str(row['column_name']),
                min_val = str(row['min_val']),
                max_val = str(row['max_val']),
                sample_val= row['sample_val']
            )
            field_profiles_ls.append(field_profile)

        logging.info("Setting up emitter...")
        emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

        dataset_profile = DatasetProfileClass(
            timestampMillis=int(time() * 1000),
            fieldProfiles=field_profiles_ls
        )

        mce = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=table_urn,
            aspectName="datasetProfile",
            aspect=dataset_profile
            )

        logging.info("Emitting stats for view: %s", table_urn)
        emitter.emit(mce)

        logging.info("Pushed stats for urn: %s", table_urn)
        logging.info("Closing emitter.")
        emitter.close()


class GetStats():
    """Class for getting statistics for MSSQL views and tables from DataHub"""

    def __init__(self, token:str, domain:str, gms_server: str):
        self.token = token
        self.domain = domain
        self.gms_server = gms_server
        logging.info("PushStats initialized for domain: %s ", domain)

    def get_minmax_stats(self, gms_server: str, urn: str, token:str) -> pd.DataFrame:
        """ 
        Function to get current min max stats in DataHub for a specific urn.

        Input:

        gms_server : server hosting DataHub, port 8080
        urn        : urn that we are getting stats for
        token      : authorization token

        Output:

        Pandas dataframe containing the current statistics. 
        Columns: column_name, min_val, max_val.

        """

        logging.info(f"Getting min-max stats for dataset: {urn} ...")

        endpoint = f'{gms_server}/api/graphql'

        # Query here uses GraphiQL because of the simpler stats structure.
        query = f"""
        query {{
        dataset(urn:"{urn}")
        {{
            datasetProfiles{{
            fieldProfiles{{
                fieldPath
                uniqueCount
                uniqueProportion
                nullCount
                min
                max
                mean
                median
                stdev
                sampleValues
            }}
            }}
        }}
        }}"""

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.post(endpoint, 
                                headers=headers,
                                json={'query': query})
        
        if response.status_code != 200:
            logging.error("Error fetching URNs: %s", response.status_code)
            raise Exception(f"Error fetching URNs: {response.status_code}")
        
        data = response.json()

        # Extract the data
        field_profiles = data['data']['dataset']['datasetProfiles'][0]['fieldProfiles']
        df = pd.DataFrame(field_profiles)

        # Rename the columns
        df_fin = df.rename(columns={'fieldPath': 'column_name', 'min': 'min_val', 'max': 'max_val'})[['column_name', 'min_val', 'max_val']]

        logging.info(f"Successfully extracted min-max stats for dataset {urn}")

        return df_fin