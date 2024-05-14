from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection, AuthProvider,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseServiceType,
)
from metadata.generated.schema.api.data.createDatabase import (
    CreateDatabaseRequest,
)
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import (
    Column,
    DataType,
    Table,
)
from dh_openmetadata_connector.core_helper import (CoreHelper)
from dh_openmetadata_connector.data_item import (PostgresParser, S3Parser)


# DatabaseService -> Database -> Schema -> Table

def createPostgresDatabaseService(metadata):
    create_service = CreateDatabaseServiceRequest(
        name="DigitalHubPostgres",
        displayName="DigitalHub",
        serviceType=DatabaseServiceType.Postgres,
    )    
    service_entity = metadata.create_or_update(data=create_service)
    #print(service_entity.json())
    return service_entity

def createS3DatabaseService(metadata):
    create_service = CreateDatabaseServiceRequest(
        name="DigitalHubS3",
        displayName="DigitalHub",
        serviceType=DatabaseServiceType.Datalake,
    )    
    service_entity = metadata.create_or_update(data=create_service)
    #print(service_entity.json())
    return service_entity
    
def createDatabase(metadata, fullyQualifiedName, dbName):
    create_db = CreateDatabaseRequest(
        name=dbName,
        service=fullyQualifiedName,
    )
    db_entity = metadata.create_or_update(create_db)
    #print(db_entity.json())
    return db_entity

def createDatabaseSchema(metadata, fullyQualifiedName, dbSchema):
    create_schema = CreateDatabaseSchemaRequest(
        name=dbSchema,
        database=fullyQualifiedName
    )
    schema_entity = metadata.create_or_update(data=create_schema)
    #print(schema_entity.json())
    return schema_entity

def createTable(metadata, fullyQualifiedName, item):
    columns: list[Column] = []
    for c in item.columns:
        column = Column(name=c.name, displayName=c.name, dataType=c.type)
        columns.append(column)

    create_table = CreateTableRequest(
        name=item.key,
        displayName=item.dbTable,
        databaseSchema=fullyQualifiedName,
        columns=columns,
    )
    table_entity = metadata.create_or_update(create_table) 
    #print(table_entity.json())   
    return table_entity

def get_dataitems(metadata, postres_service, s3_service):
    for itemNode in CoreHelper.getDataItems("http://localhost:9090/api/v1/dataitems"):
        if itemNode['spec']['path'].startswith("s3"):
            item = S3Parser(itemNode)
            database = createDatabase(metadata, s3_service.fullyQualifiedName, item.dbName)
        else:
            item = PostgresParser(itemNode)
            database = createDatabase(metadata, postres_service.fullyQualifiedName, item.dbName)
        schema = createDatabaseSchema(metadata, database.fullyQualifiedName, item.dbSchema)
        table = createTable(metadata, schema.fullyQualifiedName, item)
        yield table

server_config = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MTU2MDE2NzMsImV4cCI6bnVsbH0.ZHqpBVIBKR-iSMTSBGWpCWNK1113sHlWY-JeLvNDna8QQ3MOSItFjiTe-IAon_JzkCowbpgKX7OjsJhSW4TgiNToPVzcInAAQGBnTV8re-k3ql2RJAOXSjzf3thpLXM3JYbOcvMfsPgF9GbkgVz9y0Tr5Tsmmw-mmDZRLpontnUbpBSYwFvv2aihodPeFZgJJUUjtnghQQ1Q1GUZpxiwnHntIHnPYMHFTl6O3eh5uJvwV46cTdHC6R5KuitnmAAv9WPuu9OlmemswsOdodZaGTaIb0vUe05r139r3QiEPbfofGw-9tu7JOD5fZwE-wFjw2Th_peI6Wf8U5ftImYvsw",
    ),
)

metadata = OpenMetadata(server_config)
postres_service = createPostgresDatabaseService(metadata)
s3_service = createS3DatabaseService(metadata)
for item in get_dataitems(metadata, postres_service, s3_service):
    print("table -> " + item.name.json())
