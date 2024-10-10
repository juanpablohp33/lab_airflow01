from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_example'])
def mysql_example_dag():

    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        # Create a MySQL hook to connect to the database
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        # Define the query to extract data
        query = "select * from db_lab_utec.operaciones"
        # Run the query and fetch results
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Return the fetched rows
        print(f"Extracted rows: {rows}")
        return rows

    # Task 2: Transform the extracted data
    @task
    def transform_data(data):
        transformed_data = []
        for row in data:
            transformed_data.append({
                "id": row[0],
                "name": row[1].upper(),  # Example transformation: uppercase the name
                "value": row[2] * 100  # Example transformation: multiply value by 100
            })
        print(f"Transformed data: {transformed_data}")
        return transformed_data

    # Task 3: Load the transformed data (could be to another DB or file)
    @task
    def load_data(transformed_data):
        # For this example, we simply print the data.
        # In a real case, this could be loading into another system or database.
        print("Loading data:", transformed_data)
        # Simulate loading the data to a destination
        return "Load successful"

    # Task 4: Finalize process
    @task
    def finalize_process():
        print("DAG process completed.")
        return "DAG completed successfully"

    # Define task dependencies
    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    finalize_process()

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()
