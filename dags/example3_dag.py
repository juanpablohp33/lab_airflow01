from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['independent_tasks'])
def independent_tasks_dag():

    # Task 1: Run a Python script or command (independent)
    @task
    def task1():
        print("Running Task 1: Extracting data...")
        # Simulate running a script or command
        # Example: subprocess.run(['python', 'script1.py'])
        return "Task 1 complete"

    # Task 2: Run another Python script or command (independent)
    @task
    def task2():
        print("Running Task 2: Processing data...")
        # Simulate running a different script or command
        # Example: subprocess.run(['python', 'script2.py'])
        return "Task 2 complete"

    # Task 3: Run another Python script or command (independent)
    @task
    def task3():
        print("Running Task 3: Loading data...")
        # Simulate running another independent script or command
        # Example: subprocess.run(['python', 'script3.py'])
        return "Task 3 complete"

    # Define task dependencies (if any, e.g. sequential execution)
    task1() >> task2() >> task3()

# Instantiate the DAG
independent_tasks_dag_dag = independent_tasks_dag()
