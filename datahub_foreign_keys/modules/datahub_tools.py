import requests
import logging

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    ForeignKeyConstraintClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    NumberTypeClass,
    StringTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    TimeTypeClass,
    EnumTypeClass,
    NullTypeClass,
    MapTypeClass,
    ArrayTypeClass,
    UnionTypeClass,
    RecordTypeClass,
)


TYPE_CLASS_MAP = {
    "NumberType": NumberTypeClass,
    "StringType": StringTypeClass,
    "BooleanType": BooleanTypeClass,
    "BytesType": BytesTypeClass,
    "DateType": DateTypeClass,
    "TimeType": TimeTypeClass,
    "EnumType": EnumTypeClass,
    "NullType": NullTypeClass,
    "MapType": MapTypeClass,
    "ArrayType": ArrayTypeClass,
    "UnionType": UnionTypeClass,
    "RecordType": RecordTypeClass,
}


class DatahubCustomBuilder:
    """ Builder for DataHub metadata, specifically for schema metadata."""
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def build_schema_field(self, field):
        """
        Builds a schema field from the provided field data.
        """
        type_str = field["type"]["type"]["__type"]
        type_cls = TYPE_CLASS_MAP.get(type_str)
        if not type_cls:
            raise ValueError(f"Unknown type: {type_str}")
        
        schema_field_class = SchemaFieldClass(
            **{
                **field,
                "type": SchemaFieldDataTypeClass(type=type_cls()),
            }
        )
        
        return schema_field_class
    
    
    def make_field_urn(self, dataset_urn: str, field_path: str) -> str:
        """
        Builds a field URN for a given dataset URN and field path.
        """
        return f"urn:li:schemaField:({dataset_urn},{field_path})"


class DatahubCustomExtractor:
    """ Extractor for DataHub metadata, specifically for schema metadata."""

    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)


    def get_dataset_json(self, urn):
        """
        Fetches the dataset JSON from DataHub for a given URN.
        """
        endpoint = f"/openapi/entities/v1/latest?urns={urn}"
        url = f"{self.config['datahub']['host']}:{self.config['datahub']['port']}{endpoint}"
        headers = {"Authorization": f"Bearer {self.config['datahub']['token']}"}
        
        try:
            self.logger.debug(f"Fetching dataset JSON for URN: {urn}")
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")
            raise RuntimeError(f"An error occurred: {err}") from err
        self.logger.debug(f"Successfully fetched dataset JSON for URN: {urn}")
        
        return resp.json()
    
    
    def extract_schema_field_metadata(self, urn, dataset_json):
        """
        Extracts schema field metadata from the dataset JSON.
        """
        builder = DatahubCustomBuilder(logger=self.logger)
        
        schema_name = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['schemaName']
        platform = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['platform']
        version = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['version']
        hash = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['hash']
        platform_schema = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['platformSchema']

        fields_raw = dataset_json['responses'][urn]['aspects']['schemaMetadata']['value']['fields']
        fields = [builder.build_schema_field(f) for f in fields_raw]

        schema_metadata = {
            "schema_name": schema_name,
            "platform": platform,
            "version": version,
            "hash": hash,
            "platform_schema": platform_schema,
            "fields": fields
        }

        return schema_metadata
    

    def get_urns_for_domain(self, domain) -> list:
        """ 
        Get all URNs for a given domain from DataHub. 
        
        Returns:
        - list: A list of URNs for the given schema.
        """

        self.logger.info(f"Getting URNs for domain: {domain}")

        domain_urn = f'urn:li:domain:{domain}'

        headers = {
            "Authorization": f"Bearer {self.config['datahub']['token']}",
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
                    count: 10,
                }) {
                    domains {
                        urn,
                        type,
                        entities(input:{
                          start:0,
                          count: 100
                        }) {
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

        response = requests.post(f"{self.config['datahub']['host']}:{self.config['datahub']['port']}/api/graphql", headers=headers, json=data)

        if response.status_code != 200:
            print(f"Error fetching URNs: {response.status_code}")
            raise Exception(f"Error fetching URNs: {response.status_code}")
        
        
        domain_results = [
            domain for domain in response.json()['data']['listDomains']['domains']
            if domain['urn'] == domain_urn
        ]

        results_json = domain_results[0]["entities"]["searchResults"]

        self.logger.info(f"Fetched urns for domain: {domain}")
        return [result["entity"]["urn"] for result in results_json]
    

class DatahubCustomEmitter:
    """ Emitter for DataHub metadata, specifically for schema metadata."""
    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.emitter = DatahubRestEmitter(
            gms_server=f"{config['datahub']['host']}:{config['datahub']['port']}",
            token=config['datahub']['token']
        )

    def emit_keys(
        self,
        urn: str,
        schema_metadata: dict,         
        primary_keys: list,
        foreign_keys: list, 
    ):
        """
        Emit primary and foreign keys for a dataset.

        Args:
            urn: Dataset URN
            schema_metadata: Schema metadata dictionary from extractor
            primary_keys: List of field paths (str)
            foreign_keys: List of dicts with keys: name, sourceFields, foreignFields, foreignDataset
        """
        self.logger.info(f"Emitting keys for URN: {urn}")

        builder = DatahubCustomBuilder(logger=self.logger)

        fk_constraints = [
            ForeignKeyConstraintClass(
                name=fk['name'],
                sourceFields=[builder.make_field_urn(urn, sf) for sf in fk['sourceFields']],
                foreignFields=[builder.make_field_urn(fk['foreignDataset'], ff) for ff in fk['foreignFields']],
                foreignDataset=fk['foreignDataset'],
            )
            for fk in foreign_keys
        ]

        schema_metadata_kwargs = dict(
            schemaName=schema_metadata['schema_name'],
            platform=schema_metadata['platform'],
            version=schema_metadata['version'],
            hash=schema_metadata['hash'],
            platformSchema=schema_metadata['platform_schema'],
            fields=schema_metadata['fields'],
        )

        if primary_keys:
            schema_metadata_kwargs['primaryKeys'] = primary_keys
        if fk_constraints:
            schema_metadata_kwargs['foreignKeys'] = fk_constraints

        schema_metadata_obj = SchemaMetadataClass(**schema_metadata_kwargs)

        dataset_snapshot = DatasetSnapshotClass(
            urn=urn,
            aspects=[schema_metadata_obj]
)

        mce = MetadataChangeEventClass(proposedSnapshot=dataset_snapshot)
        self.emitter.emit(mce)
        self.emitter.close()
        print(f"Emitted primary and foreign keys for {urn}")