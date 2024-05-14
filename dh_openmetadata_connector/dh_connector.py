from typing import Iterable

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
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger
from dh_openmetadata_connector.core_helper import (CoreHelper)
from dh_openmetadata_connector.data_item import (PostgresParser, S3Parser)
import traceback

logger = ingestion_logger()

class DigitalHubConnector(Source):
    """
    Custom connector to ingest Database metadata
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata
        self.service_connection = config.serviceConnection.__root__.config
        logger.info("DigitalHubConnector __init__")
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection
    ) -> "DigitalHubConnector":
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

    def _iter(self) -> Iterable[Either[Entity]]:
        logger.info("DigitalHubConnector._iter")
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
        logger.info("DigitalHubConnector.get_dataitems")
        self.service_entity = self.get_service()
        for itemNode in CoreHelper.getDataItems("http://host.docker.internal:9090/api/v1/dataitems"):
            item = None
            if itemNode['spec']['path'].startswith("s3"):
                item = S3Parser(itemNode)
            else:
                item = PostgresParser(itemNode)
            database_request = self.create_db_request(item)
            yield database_request
            schema_request = self.crate_schema_request(item)
            yield schema_request
            table_request = self.create_table_request(item)
            yield table_request

