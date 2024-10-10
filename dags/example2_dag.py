from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['example_dag'])
def example_four_task_dag():

    # Task 1: Extract data
    @task
    def extract_data():
        data = {"key": "value", "id": 123}
        print("Data extracted:", data)
        return data

    # Task 2: Transform data
    @task
    def transform_data(data):
        transformed_data = {k: str(v).upper() for k, v in data.items()}
        print("Data transformed:", transformed_data)
        return transformed_data

    # Task 3: Load data
    @task
    def load_data(transformed_data):
        print("Loading data to destination:", transformed_data)
        # Simulate loading data
        return "Load complete"

    # Task 4: Finalize process
    @task
    def finalize_process():
        print("DAG process completed.")
        return "DAG completed successfully"

    # Task dependencies
    data = extract_data()
    transformed_data = transform_data(data)
    load_result = load_data(transformed_data)
    finalize_process()

# Instantiate the DAG
example_four_task_dag_dag = example_four_task_dag()
