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
from metadata.generated.schema.api.data.createMlModel import (CreateMlModelRequest)
from metadata.generated.schema.entity.services.mlmodelService import (MlModelService)
from metadata.generated.schema.entity.data.mlmodel import (MlHyperParameter, MlStore)
from metadata.generated.schema.metadataIngestion.workflow import (Source as WorkflowSource)
from metadata.generated.schema.type.basic import (SourceUrl)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

from oauth2_client.credentials_manager import (CredentialManager, ServiceInformation, OAuthError)

from dh_openmetadata_connector.core_helper import (CoreHelper)
from dh_openmetadata_connector.model import (ModelParser, MlflowModelParser, SKLearnModelParser, HuggingFaceModelParser)

import traceback

logger = ingestion_logger()

class DigitalHubConnectorModel(Source):
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
                f"Error in DigitalHubConnectorModel init: {ex}"
            )
        logger.info("DigitalHubConnectorModel __init__")
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection, pipeline_name: Optional[str]
    ) -> "DigitalHubConnectorModel":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata_config)

    def prepare(self):
        pass

    def create_service_request(self):
        return self.metadata.get_create_service_from_source(
            entity=MlModelService, config=self.config
        )
    
    def get_service(self) -> MlModelService:
        return self.metadata.get_by_name(
            entity=MlModelService, fqn=self.config.serviceName
        )

    def create_model_request(self, item):
        params: list[MlHyperParameter] = []
        for p in item.parameters:            
            hp = MlHyperParameter(
                    name=p.name,
                    value=str(p.value)
                )
            #logger.info("MlHyperParameter:" + hp.json())
            params.append(hp)
        
        create_model = CreateMlModelRequest(
            name=item.key,
            displayName=item.name,
            service=self.service_entity.fullyQualifiedName,
            algorithm=item.algorithm,
            mlStore= MlStore(storage=item.source, imageRepository=item.path),
            mlHyperParameters=params
        )
        return create_model  

    def _iter(self) -> Iterable[Either[Entity]]:
        logger.info("DigitalHubConnectorModel._iter")
        yield Either(
            right=self.create_service_request()
        )
        try:
            for item in self.get_models():
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

    def get_models(self):
        logger.info("DigitalHubConnectorModel.get_models")
        self.service_entity = self.get_service()
        for itemNode in CoreHelper.getModels(self.apiUrl, self.credential_manager._access_token):
            logger.info("get item -> " + itemNode['key'])
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
                    item = ModelParser(itemNode)
                    model_request = self.create_model_request(item)
                    yield model_request
                


