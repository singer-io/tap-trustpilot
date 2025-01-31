from singer.catalog import Catalog, CatalogEntry, Schema
from tap_trustpilot import schemas

def check_credentials_are_authorized(ctx):
    """
    validates access_key
    """
    ctx.client.validate_api_key()

def discover(ctx):
    """
    Constructs a singer Catalog object based on the schemas and metadata.
    """
    check_credentials_are_authorized(ctx)
    discover_schemas, field_metadata = schemas.get_schemas()
    streams = []
    for stream_name, raw_schema in discover_schemas.items():
        schema = Schema.from_dict(raw_schema)
        mdata = field_metadata[stream_name]
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_name,
                stream=stream_name,
                schema=schema,
                key_properties=schemas.PK_FIELDS[stream_name],
                metadata=mdata
            )
        )
    return Catalog(streams)