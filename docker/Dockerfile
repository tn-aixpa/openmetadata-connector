# SPDX-License-Identifier: Apache-2.0
FROM openmetadata/ingestion:1.5.3

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
COPY dh_openmetadata_connector dh_openmetadata_connector
COPY setup.py .
RUN pip install --no-deps oauth2-client==1.4.2 
RUN pip install --no-deps .
