# Entity Matching

## Prerequisites

Before running the project, ensure you have Docker Desktop installed. You can find the installation instructions [here](https://docs.docker.com/engine/install/).

## Getting Started

To run the project:

1. Navigate to the directory containing the `docker-compose.yaml` file.

2. Initialize the database:
    ```sh
    docker compose up airflow-init
    ```

3. Start Airflow:
    ```sh
    docker compose up --build
    ```
    This will automatically install the required packages.


4. Open your web browser and navigate to [http://localhost:8080](http://localhost:8080).

5. Log in with the following credentials:
    - **Username:** `airflow`
    - **Password:** `airflow`

6. Unpause the `extenso_assignment` to start the workflow.

## About Entity Matching

Entity matching is the process of identifying and merging records that refer to the same entity across different data sources. This project automates the entity matching process by merging records that are highly similar.

### Key Functions and Components

- **combine_layouts(A, B, metric, threshold):**
    - This function combines two dataframes, A and B, using the specified metric (Levenshtein distance or cosine similarity) and a threshold to determine the similarity between records.
    - Matching records are combined, with preference given to non-null values.

- **Airflow DAG:**
    - **load_task:** Loads the data from the specified directory.
    - **preprocess_task:** Preprocesses the data to create a "soup" of key attributes. The key attributes used for creating the soup are `Name`, `Date of Birth`, and `Father_Name`.

### Directory Structure

- `files`: Contains the input_files that will be used for entity matching and output_files to store the result.
- `notebooks`: Contains notebooks for the development of entity matching.
- `dags`: Contains the dags for the Entity_matching_dag.
- `requirement`: Contains the necessary library for entity matching.
