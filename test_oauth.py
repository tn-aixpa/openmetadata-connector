from oauth2_client.credentials_manager import CredentialManager, ServiceInformation, OAuthError
from dh_openmetadata_connector.core_helper import (CoreHelper)
import json

service_information = ServiceInformation(
    authorize_service='https://aac.digitalhub-dev.smartcommunitylab.it/oauth/authorize',
    token_service='https://aac.digitalhub-dev.smartcommunitylab.it/oauth/token',
    client_id='',
    client_secret='',
    scopes=['tenantcoreclient']
)

manager = CredentialManager(service_information)
manager.init_with_client_credentials()
print(f"access token:{manager._access_token}")
for itemNode in CoreHelper.getDataItems("https://core.tenant1.digitalhub-dev.smartcommunitylab.it/api/v1/dataitems", manager._access_token):
    print(json.dumps(itemNode))