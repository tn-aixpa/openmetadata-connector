# SPDX-License-Identifier: Apache-2.0
import re 
from typing import Iterable, Optional

from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.ingestion.api.models import Either
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.generated.schema.api.data.createDatabase import (
    CreateDatabaseRequest,
)
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import (
    CreateTableRequest,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.entity.data.table import (
    Column,
    DataType,
    Table,
    TableData,
    ColumnName
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

from oauth2_client.credentials_manager import (CredentialManager, ServiceInformation, OAuthError)

from dh_openmetadata_connector.core_helper import (CoreHelper)
from dh_openmetadata_connector.data_item import (PostgresParser, S3Parser)

import traceback

logger = ingestion_logger()

class DigitalHubConnectorDataItem(Source):
    """
    Custom connector to ingest Database metadata
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata
        self.service_connection = config.serviceConnection.root.config

        try:
            self.apiUrl: str = self.service_connection.connectionOptions.root.get("api-url")
            self.authorize_service: str = self.service_connection.connectionOptions.root.get("authorize-service") 
            self.token_service: str = self.service_connection.connectionOptions.root.get("token-service")  
            self.client_id: str = self.service_connection.connectionOptions.root.get("client-id") 
            self.client_secret: str = self.service_connection.connectionOptions.root.get("client-secret") 
            self.scopes: str = self.service_connection.connectionOptions.root.get("scopes") 
            filters : str = self.service_connection.connectionOptions.root.get("project-filters")
            self.projectFilters: list[str] = filters.split(',')
            self.service_information = ServiceInformation(
                authorize_service=self.authorize_service,
                token_service=self.token_service,
                client_id=self.client_id,
                client_secret=self.client_secret,
                scopes=self.scopes.split(',')
            )
            self.credential_manager = CredentialManager(self.service_information)
            self.credential_manager.init_with_client_credentials()
        except Exception as ex:
            raise InvalidSourceException(
                f"Error in DigitalHubConnectorDataItem init: {ex}"
            )
        logger.info("DigitalHubConnectorDataItem __init__")
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection, pipeline_name: Optional[str]
    ) -> "DigitalHubConnectorDataItem":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata_config)

    def prepare(self):
        pass

    def create_service_request(self):
        return self.metadata.get_create_service_from_source(
            entity=DatabaseService, config=self.config
        )
    
    def get_service(self) -> DatabaseService:
        return self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )

    def create_db_request(self, item):
        create_db = CreateDatabaseRequest(
            name=item.dbName,
            service=self.service_entity.fullyQualifiedName,
        )
        return create_db
    
    def crate_schema_request(self, item):
        create_schema = CreateDatabaseSchemaRequest(
            name=item.dbSchema,
            database=f"{self.config.serviceName}.{item.dbName}"
        )
        return create_schema
    
    def create_table_request(self, item):
        itemColumns: list[Column] = []
        for c in item.columns:
            column = Column(name=c.name, displayName=c.name, dataType=c.type)
            itemColumns.append(column)

        create_table = CreateTableRequest(
            name=item.key,
            displayName=item.dbTable,
            databaseSchema=f"{self.config.serviceName}.{item.dbName}.{item.dbSchema}",
            columns=itemColumns,
        )
        return create_table
    
    def add_table_sampladata(self, item):
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
            
            td = TableData()
            td.columns = columnNames
            td.rows = sampleData
            table_entity: Table = self.metadata.get_by_name(
                entity=Table, fqn=f"{self.config.serviceName}.{item.dbName}.{item.dbSchema}.{item.key}"
            )            
            self.metadata.ingest_table_sample_data(table_entity, td)

    def _iter(self) -> Iterable[Either[Entity]]:
        logger.info("DigitalHubConnectorDataItem._iter")
        yield Either(
            right=self.create_service_request()
        )
        try:
            for item in self.get_dataitems():
                logger.info("entity -> " + item.name.json())
                yield Either(
                    right=item
                )
        except Exception as ex:
            logger.error(f"Error:{ex}")
            yield Either(
                left=StackTraceError(
                    name="DH Error",
                    error=f"Error:{ex}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def test_connection(self) -> None:
        pass
    
    def close(self):
        pass

    def get_dataitems(self):
        logger.info("DigitalHubConnectorDataItem.get_dataitems")
        self.service_entity = self.get_service()
        for itemNode in CoreHelper.getDataItems(self.apiUrl, self.credential_manager._access_token):
            logger.info("get item -> " + itemNode['key'])
            #check kind == table
            if itemNode['kind'] == 'table':
                #check publish
                if itemNode['metadata'].get('openmetadata') and itemNode['metadata']['openmetadata'].get('publish'):
                    publishItem = False
                    #check project filters
                    if((self.projectFilters) and (len(self.projectFilters) > 0)):
                        for f in self.projectFilters:
                            regex = re.compile(f, re.IGNORECASE)
                            if regex.match(itemNode['project']):
                                publishItem = True
                                break
                    else:
                        publishItem = True

                    if(publishItem):
                        logger.info("parse item -> " + itemNode['key'])
                        item = None
                        if itemNode['spec']['path'].startswith("s3"):
                            item = S3Parser(itemNode)
                        elif itemNode['spec']['path'].startswith("sql"):
                            item = PostgresParser(itemNode)
                        else:
                            continue
                        database_request = self.create_db_request(item)
                        yield database_request
                        schema_request = self.crate_schema_request(item)
                        yield schema_request
                        table_request = self.create_table_request(item)
                        yield table_request
                        self.add_table_sampladata(item)


