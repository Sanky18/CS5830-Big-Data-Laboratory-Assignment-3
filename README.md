# Climatological Data Pipeline

This assignment aims to set up a data pipeline to acquire public domain climatological data from the National Centers for Environmental Information (NCEI) website. The pipeline is divided into two main tasks: DataFetch Pipeline and Analytics Pipeline. It leverages Apache Airflow for task fetching and Apache Beam for data processing. It further, utilizes Git for version control and tracks the changes made to the code, data, and configurations. Additionally, Data Version Control (DVC) is used to manage and version control the generated data.

## Repository Structure

The Repository structure consists of the following directories and files:

- `dags/`: Contains Apache Airflow DAGs for task scheduling.
- `data/`: Directory for storing generated data.
- `plots/`: Directory for storing generated visualizations.
- `requirements.txt`: List of Python dependencies.
- `docker-compose.yml`: Docker Compose file for containerized deployment.
- `dockerfile`: Dockerfile for building Docker images.
