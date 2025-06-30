# OpenMetadata Connector

[![license](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/tn-aixpa/openmetadata-connector/LICENSE)
![Status](https://img.shields.io/badge/status-stable-gold)

This project allows to create a custom connetor for injection of dataitem entities from a DH core platform into a OpenMetadata server.

A new docker image for the Airflow Ingestion framework is deployed, with a custom python connector that import metadata from the DH platform into the OpemMetadata server.

The custom connector can be configured from the OpenMetadata console.

Explore the full OpenMetadata documentation at the [https://docs.open-metadata.org/latest/connectors/custom-connectors](link).

## Quick start

Run the Makefile or the runDocker.bat in order to create the new Airflow docker image. Start the OpenMetadata server.

## Configuration

From the OpenMetadata console create a new `Database Service`, using `Custom Database` connector.

The configuration parameters you should set:
- choose `dh_openmetadata_connector.dh_connector.DigitalHubConnector` as `sourcePythonClass`
- `api-url`: core platform API url, like http://localhost:9090/api/v1/
- `authorize-service`: oauth2 autorize service, like https://aac.com/oauth/authorize
- `token-service`: oauth2 token service, like https://aac.com/oauth/token
- `client-id`: for oauth2 client credentials flow 
- `client-secret`: for oauth2 client credentials flow 
- `scopes`: scopes to oauth2 request, separated by comma 
- `project-filters`: list of projects to import, separated by comma 

## Development

In `dh_openmetadata_connector` folder you can find the python source file for the custom connector.
In `docker` folder you can file the docker and yalm file for creating the docker image

See CONTRIBUTING for contribution instructions.

### Build container images

Run the Makefile or the runDocker.bat in order to create the new docker image.

## Security Policy

The current release is the supported version. Security fixes are released together with all other fixes in each new release.

If you discover a security vulnerability in this project, please do not open a public issue.

Instead, report it privately by emailing us at digitalhub@fbk.eu. Include as much detail as possible to help us understand and address the issue quickly and responsibly.

## Contributing

To report a bug or request a feature, please first check the existing issues to avoid duplicates. If none exist, open a new issue with a clear title and a detailed description, including any steps to reproduce if it's a bug.

To contribute code, start by forking the repository. Clone your fork locally and create a new branch for your changes. Make sure your commits follow the [Conventional Commits v1.0](https://www.conventionalcommits.org/en/v1.0.0/) specification to keep history readable and consistent.

Once your changes are ready, push your branch to your fork and open a pull request against the main branch. Be sure to include a summary of what you changed and why. If your pull request addresses an issue, mention it in the description (e.g., “Closes #123”).

Please note that new contributors may be asked to sign a Contributor License Agreement (CLA) before their pull requests can be merged. This helps us ensure compliance with open source licensing standards.

We appreciate contributions and help in improving the project!

## Authors

This project is developed and maintained by **DSLab – Fondazione Bruno Kessler**, with contributions from the open source community. A complete list of contributors is available in the project’s commit history and pull requests.

For questions or inquiries, please contact: [digitalhub@fbk.eu](mailto:digitalhub@fbk.eu)

## Copyright and license

Copyright © 2025 DSLab – Fondazione Bruno Kessler and individual contributors.

This project is licensed under the Apache License, Version 2.0.
You may not use this file except in compliance with the License. Ownership of contributions remains with the original authors and is governed by the terms of the Apache 2.0 License, including the requirement to grant a license to the project.
