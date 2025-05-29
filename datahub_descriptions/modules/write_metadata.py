import requests
import pandas as pd
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import EditableSchemaMetadataClass, EditableDatasetPropertiesClass
import argparse
import os
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()

class MetadataWriter:
    """Reads in metadata from a given Data Catalog domain and collects and writes
    the data to a given path"""

    def __init__(self, domain: str, platform: str):
        self.domain_urn = f"urn:li:domain:{domain}"
        self.client = DataHubGraph(
            DatahubClientConfig(server=os.getenv("DATAHUB_URL"), token=os.getenv("DATAHUB_TOKEN")))
        self.platform = platform
        
    def get_dataset_urns(self) -> List[str]:
        """Get a list of dataset urns"""

        urns = [
            dataset
            for dataset
            in self.client.get_urns_by_filter(
                entity_types=["dataset"], 
                env=os.getenv("DATAHUB_ENV"),
                platform=self.platform,
                extraFilters=[{"field": "domains", "values": [self.domain_urn]}]
            )
        ]
        return urns
    
    def get_table_dicts(self) -> List[Dict[str, str]]:
        """Get a list of dictionaries with table information"""

        dataset_urns = self.get_dataset_urns()
        tables = []
        for urn in dataset_urns:
            name = urn.split(',')[-2].split('.')[-1]
            column_fields = self.client.get_aspect(urn, EditableSchemaMetadataClass).editableSchemaFieldInfo
            table_dict = {
                "table_name": name,
                "table_description": self.client.get_aspect(urn, EditableDatasetPropertiesClass).description,
                "column_descriptions": [
                    {
                        "column_name": c.fieldPath,
                        "column_description": c.description
                    }
                    for c
                    in column_fields
                ]
            }
            tables.append(table_dict)
        return tables
    
    def get_column_dicts(self) -> List[Dict[str, str]]:
        """Get a list of exploded dictionaries from get_table_dicts"""

        tables = self.get_table_dicts()
        columns = []
        for table in tables:
            for col in table["column_descriptions"]:
                col_dict = {
                    "table_name": table["table_name"],
                    "table_description": table["table_description"],
                    "column_name": col["column_name"],
                    "column_description": col["column_description"]
                }
                columns.append(col_dict)
        return columns
    
    def write_metadata(self, path: str) -> None:
        """Write collected metadata as CSV to given path"""

        column_dicts = self.get_column_dicts()
        df = pd.DataFrame.from_records(column_dicts)
        df.to_csv(path)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Script that writes Data Catalog metadata to a csv file")
    parser.add_argument("--domain", required=True, type=str)
    parser.add_argument("--platform", required=True, type=str)
    parser.add_argument("--path", required=True, type=str)
    args = parser.parse_args()

    domain = args.domain
    platform = args.platform
    path = args.path

    Writer = MetadataWriter(domain=domain, platform=platform)
    Writer.write_metadata(path=path)

