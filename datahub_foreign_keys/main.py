import yaml
import logging
from modules.datahub_tools import DatahubCustomExtractor, DatahubCustomEmitter
from modules.key_extractor import MSSQLClient, KeysProcessing

def setup_logger():
    """Set up logger to print all messages to the console."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def main():
    logger = setup_logger()

    # Get credentials from config.yaml
    with open('/home/rin/code/llm_proj/datahub_dev/Skuld-LLM/datahub_foreign_keys/src/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Set variables
    domain = 'ucube_cubedevtest'
    server = 'ekofisk'
    db = 'CubeDevTest'
    schema = 'UCube'

    # Get urns from DataHub domain
    extractor = DatahubCustomExtractor(config, logger)
    urns = extractor.get_urns_for_domain(domain=domain)
    logger.info(f"Found {len(urns)} urns in domain '{domain}'")

    # Get foreign keys info from MSSQL database
    mssql_client = MSSQLClient(logger, config)
    fks_info_df = mssql_client.fetch_foreign_keys_info()
    logger.info(f"Fetched foreign keys info from MSSQL database with {len(fks_info_df)} rows")

    # Process foreign keys and primary keys
    keys_processor = KeysProcessing(logger, config, fks_info_df)
    emitter = DatahubCustomEmitter(config, logger)

    for urn in urns:
        logger.info(f"Processing URN: {urn}")

        # Get urn JSON from DataHub for this dataset
        urn_json = extractor.get_dataset_json(urn)
        # Get primary keys
        primary_keys = keys_processor.get_pks(urn, urn_json)
        # Get foreign keys
        foreign_keys = keys_processor.get_fks(urn, server, db, schema)

        # Emit keys to Datahub
        # Get schema metadata from urn_json
        schema_metadata = extractor.extract_schema_field_metadata(urn, urn_json)
        emitter.emit_keys(
            urn=urn,
            schema_metadata=schema_metadata,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys
        )
        logger.info("--------------------------------------------")

if __name__ == "__main__":
    main()
