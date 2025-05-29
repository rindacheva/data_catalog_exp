from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    EditableSchemaMetadataClass,
    EditableDatasetPropertiesClass,
)
import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CustomDescriptionsDC():
    """Class for pushing custom description metadata to DataHub"""

    def __init__(self, tables_dict: dict[str, pd.DataFrame], env: str, gms_server: str, token: str):
        self.tables_dict = tables_dict
        self.env = env
        self.gms_server = gms_server
        self.token = token
        logging.info("CustomDescriptionsDC initialized for environment: %s", env)

    def urn_generator_ucube(self, table_name: str, env: str) -> list:
        """
        Function needed to generate URNs for all entities to be updated for UCube

        Input:
        - table_name  : table name
        - env         : environment to be populated (i.e. DEV or PROD)

        Output:
        - list or URNs to be updated

        This function is specific for the UCube entities, which start with "vDataFeed_UCube_"
        
        """
        urn = f"urn:li:dataset:(urn:li:dataPlatform:mssql,ekofisk.RECubeDataRelease.dbo.vDataFeed_UCube_{table_name},{env})"
        logging.info("Generated URN for table %s in %s environment", table_name, env)
        logging.info("URN: %s", urn)
        return urn
    

    def table_desc_emitter(self, gms_server: str, token:str, urn: str, table_description: str) -> None:
        """ 
        Function to emit metadata to PROD environement.

        Input:
        - gms_server        : the host for DataHub
        - token             : authentication token
        - urn               : the PRODUCTION urn we will be pushing to 
        - table_description : description of the table gorgeous

        Output:
        - None, emits metadata

        """
        try:
            logging.info("Emitting table description for URN: %s", urn)
            emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

            editable_dataset_properties_aspect = EditableDatasetPropertiesClass(
                description=table_description
            )

            dataset_snapshot = DatasetSnapshotClass(
                urn=urn,
                aspects=[
                    editable_dataset_properties_aspect,
                ]
            )

            mce = MetadataChangeEventClass(proposedSnapshot=dataset_snapshot)

            emitter.emit(mce)
            emitter.close()
            logging.info("Successfully emitted table description metadata")

        except Exception as e:
            logging.error("Error emitting table description for URN %s: %s", urn, e)


    def column_desc_emitter(self, gms_server: str, token:str, urn:str, column_dict: dict[str, str]) -> None:

        """ 
        Function to emit metadata to PROD environement.

        Input:
        - gms_server  : the host for DataHub
        - token       : authentication token
        - urn         : the PRODUCTION urn we will be pushing to
        - column_dict : a dictionary of column names and their associated descriptions bestie

        Output:
        - None

        """
        try:
            logging.info("Emitting column descriptions for URN: %s", urn)
            emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

            data_to_push = []
            for column_name, column_description in column_dict.items():
                column_data = {
                    "fieldPath": column_name,
                    "description": column_description
                }
                data_to_push.append(column_data)

            editable_schema_metadata_aspect = EditableSchemaMetadataClass(
                editableSchemaFieldInfo=data_to_push
            )

            dataset_snapshot = DatasetSnapshotClass(
                urn=urn,
                aspects=[
                    editable_schema_metadata_aspect,
                ]
            )

            mce = MetadataChangeEventClass(proposedSnapshot=dataset_snapshot)

            emitter.emit(mce)
            emitter.close()
            logging.info("Successfully emitted column description metadata.")

        except Exception as e:
            logging.error("Error emitting column descriptions for URN %s: %s", urn, e)
