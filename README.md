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
## Building the Repository and the Data Pipeline

1. **Clone the Repository:** 
   Clone the repository from the remote repository to your local machine.
2. **Navigate to the Project Directory:** 
   Change your current directory to the cloned repository.
3. **Initialize Git:** 
   Initialize Git in the project directory.
4. **Create a .gitignore File:** 
   Create a `.gitignore` file to specify files and directories that should be ignored by Git. You can create this file manually or use tools like [gitignore.io](https://www.gitignore.io/).
5. **Create a .dvc Directory:** 
   Initialize Data Version Control (DVC) in the project directory.
6. **Install Dependencies:** 
   Install the required Python dependencies specified in the `requirements.txt` file.
7. **Start Apache Airflow:** 
   If not already installed, set up Apache Airflow according to its documentation and start the Airflow webserver and scheduler.
8. **Start Docker Compose:** 
   If using Docker Compose for containerized deployment, start the services defined in `docker-compose.yml`.
9. **Run DAGs:** 
   Once Airflow is running, enable and trigger the DAGs from the Airflow UI. The DAGs should start running according to their schedules.
10. **DataFetch Pipeline (Task 1) Steps:**
    1. **Fetch Data:** 
       Fetch the HTML page containing location-wise datasets for a specific year from the NCEI website.
    2. **Select Files:** 
       Randomly select CSV files from the fetched HTML page.
    3. **Fetch Files:** 
       Download the selected CSV files.
    4. **Zip Files:** 
       Compress the downloaded CSV files into a ZIP archive.
    5. **Move Zip File:** 
       Move the ZIP archive to a specified location.
    6. **DVC Add:** 
       Once all the data is archived in the specified location inside the cloned repository, track it using DVC with the command `dvc add`.
       
![Pipeline-1](https://github.com/Sanky18/CS5830-Big-Data-Laboratory-Assignment-3/assets/119156783/6b513d63-7029-4742-83c5-b0d1e9c20667)

11. **Analytics Pipeline (Task 2) Steps:**
    1. **Wait for Archive:** 
       Wait for the ZIP archive to be available at the specified location.
    2. **Unzip Archive:** 
       Unzip the contents of the archive into individual CSV files.
    3. **Extract and Filter Data:** 
        Extract required fields from CSV files and filter them based on string-only columns containing hourly values.
    4. **Compute Monthly Averages:** 
       Compute monthly averages of the required fields containing hourly values.
    5. **Combine Data:** 
        Combine data from different locations for at least one month files into a single DataFrame.
    6. **Generate Geomaps:** 
       Generate geospatial visualizations using the average values of combined data. Additionally, take the magnitude of negative average values into account, where larger average values result in bigger dots on the geomaps, while smaller magnitudes lead to negligible dot sizes.
       
![Pipeline-2](https://github.com/Sanky18/CS5830-Big-Data-Laboratory-Assignment-3/assets/119156783/a534a9e9-244c-4370-af90-6ebc3fade2af)

12. Execute the above two DAG pipelines for the years 2023 and 2024, filtering those columns which have at least one month hourly data for required fields.

## Results
The results of the Analytics Pipeline includes, Geospatial visualizations (geomaps) for dif-
ferent parameters. We have shown some sample animation of geomaps for two different years 2023 and
2024, for month july and jan respectively.

https://github.com/Sanky18/CS5830-Big-Data-Laboratory-Assignment-3/assets/119156783/c68e322d-05e8-4427-ac76-2e9ab3dfc6eb


*The series of geomaps depict climatological data for the month of July 2023, focusing on Hourly Altimeter Setting, Hourly Pressure Tendency, Hourly Wind Gust Speed, and
Hourly Wind Speed. Each map provides a spatial representation of the respective parameter’s
distribution across the specified time frame.*



https://github.com/Sanky18/CS5830-Big-Data-Laboratory-Assignment-3/assets/119156783/805f6d4a-f9ee-42be-89f6-4cd4c460ffca

*The series of geomaps depict climatological data for the month of Jan 2024, focusing on Hourly Wet Bulb Temperature, Hourly Pressure Tendency, Hourly Relative Humidity,
and Hourly Wind Speed. Each map provides a spatial representation of the respective parameter’s distribution across the specified time frame.*

