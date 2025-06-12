# SPDX-License-Identifier: Apache-2.0
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
    TableData,
    ColumnName
)
from dh_openmetadata_connector.core_helper import (CoreHelper)
from dh_openmetadata_connector.data_item import (PostgresParser, S3Parser)
from oauth2_client.credentials_manager import CredentialManager, ServiceInformation, OAuthError
import json

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
        sampleData=td
    )
    table_entity = metadata.create_or_update(create_table) 

    td = TableData()
    if len(item.columns) > 0:
        columnNames: list[ColumnName] = []
        sampleData: list[list] = []
        for c in item.columns:
            columnNames.append(c.name)
            for rowIndex, preview in enumerate(c.preview):
                objectList = []
                if len(sampleData) <= rowIndex:
                    sampleData.append(objectList)
                else:
                    objectList = sampleData[rowIndex]
                objectList.append(preview)
        
        td.columns = columnNames
        td.rows = sampleData
        metadata.ingest_table_sample_data(table_entity, td)

    #print(table_entity.json())   
    return table_entity

def get_dataitems(metadata, postres_service, s3_service):
    service_information = ServiceInformation(
        authorize_service='https://aac.digitalhub-dev.smartcommunitylab.it/oauth/authorize',
        token_service='https://aac.digitalhub-dev.smartcommunitylab.it/oauth/token',
        client_id='',
        client_secret='',
        scopes=['tenant1-core']
    )    
    manager = CredentialManager(service_information)
    manager.init_with_client_credentials()
    for itemNode in CoreHelper.getDataItems("https://core.tenant1.digitalhub-dev.smartcommunitylab.it/api/v1", manager._access_token):
        print("----- item -----")
        print(json.dumps(itemNode))
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
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MTU5NDM1MzYsImV4cCI6bnVsbH0.fff4hCoHvdtRcUZLGqezptUDcSWaRhnf6CMhX1jogMCdYjoBLg9w0VdyStSZ4g_bZB36dyLYSgK4VidNEZkQ46RZO_FZQiN5oErqm5qa1Frd2Y4HI7QK6DDxbF7G0djs4UJfNpoQ3nH6fwd5ZlGKylq9QSb8glrtMOcKjUZnHBRVi4b1TtVgVlKBRDUL1Bn2ioj452OB8tB7-JoX2xwjQ7lo2jeczoE87lOEOeh0BxLPhsgeAPDIvUWBaPvdhH3_lCHprLk0mkz_OjxzlX_XMsR6CP03-R-sJYI-ILgKSbYyDxWD9J1Fbltza9-Ck6-o1yBdLhGcptNb_EgitCupiQ",
    ),
)

metadata = OpenMetadata(server_config)
postres_service = createPostgresDatabaseService(metadata)
s3_service = createS3DatabaseService(metadata)
for item in get_dataitems(metadata, postres_service, s3_service):
    print("table -> " + item.name.json())
