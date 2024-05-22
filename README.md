# openmetadata-connector

Custom connetor for injecting data-items from DH core plaftorm into OpenMetadata.

In OpenMetadata create a new `Database Service`, using `Custom Database` connector.

The configuration parameters you should set:
- choose `dh_openmetadata_connector.dh_connector.DigitalHubConnector` as `sourcePythonClass`
- `api-url`: core platform API url, like http://localhost:9090/api/v1/
- `authorize-service`: oauth2 autorize service, like https://aac.com/oauth/authorize
- `token-service`: oauth2 token service, like https://aac.com/oauth/token
- `client-id`: for oauth2 client credentials flow 
- `client-secret`: for oauth2 client credentials flow 
- `scopes`: scopes to request, separated by comma 