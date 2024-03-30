# Import necessary modules and classes
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from bs4 import BeautifulSoup
import random
import logging
import re
import pandas as pd
import os
import zipfile

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG object
dag = DAG(
    'fetch_data_pipeline',  # DAG ID
    default_args=default_args,  # Default arguments
    description='Pipeline for fetching climatological data',  # DAG description
    schedule=None,
)

# Define paths and URLs
download_path = "A:\\Jan-May 2024\\CS5830-Big Data Laboratory\\Assignment-2\\Workspace\\Data"
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access"
year = "2023"
num_files = 0.1

# Function to randomly select files from HTML page
def select_random_files(**kwargs):
    """
    Selects random CSV files from the HTML page.

    Args:
        **kwargs: Additional arguments.

    Returns:
        list: List of selected CSV files.
    """
    # Read the HTML content
    with open(f'{download_path}/{year}_data.html', 'r') as file:
        html_content = file.read()
    
    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all links to CSV files
    links = soup.find_all('a', href=re.compile(r'.csv$'))
    
    # Extract CSV file names
    csv_files = [link.get('href').split('/')[-1] for link in links]
    
    # Log the number of CSV files found
    logging.info(f"Number of CSV files found: {len(csv_files)}")
    
    # Select random files
    selected_files = random.sample(csv_files, 1000)
    
    return selected_files

# Function to fetch CSV files
def fetch_csv_files(**kwargs):
    """
    Fetches CSV files from the base URL.

    Args:
        **kwargs: Additional arguments.
    """
    selected_files = kwargs['ti'].xcom_pull(task_ids='select_files')
    for file_name in selected_files:
        # Download CSV files using curl
        os.system(f"curl -o {download_path}/{file_name} {base_url}/{year}/{file_name}")

# Function to zip selected files
def zip_files(**kwargs):
    """
    Zips selected CSV files and removes the originals.

    Args:
        **kwargs: Additional arguments.
    """
    selected_files = kwargs['ti'].xcom_pull(task_ids='select_files')
    with zipfile.ZipFile(f"{download_path}/{year}_data.zip", "w") as zipf:
        for file_name in selected_files:
            # Read CSV file and check if 'DATE' column exists
            Data = pd.read_csv(f"{download_path}/{file_name}")
            if 'DATE' in Data.columns:
                # If 'DATE' column exists, add file to ZIP archive
                zipf.write(f"{download_path}/{file_name}", arcname=file_name)
                # Remove the original CSV file
                os.remove(f"{download_path}/{file_name}")
            else:
                # If 'DATE' column does not exist, log a warning and skip the file
                logging.warning(f"DATE column not found in {file_name}. Skipping the file.")
                os.remove(f"{download_path}/{file_name}")

# Define tasks for the DAG
move_zip_file_task = BashOperator(
    task_id='move_zip_file',
    bash_command = f"mv {download_path}\\{year}_data.zip A:\\Jan-May 2024\\CS5830-Big Data Laboratory\\Assignment-2\\Workspace\\Data_storage",
    dag=dag,
)

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    dag=dag,
)

fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=f'curl -o {download_path}/{year}_data.html {base_url}/{year}/',
    dag=dag,
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    dag=dag,
)

fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_csv_files,
    dag=dag,
)

# Define task dependencies
fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_zip_file_task

