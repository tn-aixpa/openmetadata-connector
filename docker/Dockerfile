FROM openmetadata/ingestion:1.3.1

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
COPY dh_openmetadata_connector dh_openmetadata_connector
COPY setup.py .
RUN pip install --no-deps .
