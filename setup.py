from setuptools import setup, find_packages

base_requirements = {"openmetadata-ingestion==1.5.3"}

setup(
    name="DigitalHubConnector",
    version="0.0.1",
    url="https://open-metadata.org/",
    author="FBK",
    license="Apache License 2.0",
    description="Ingestion Framework for OpenMetadata",
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=list(base_requirements),
    packages=find_packages(include=["dh_openmetadata_connector", "dh_openmetadata_connector.*"]),
)