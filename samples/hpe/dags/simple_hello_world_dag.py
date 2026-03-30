from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id="hello_world_taskflow",
    start_date=datetime(2026, 3, 5, tz="UTC"),
    schedule=None,  # Set to None for unscheduled, manual runs
    catchup=False,
    tags=["example", "hello-world"],
)
def hello_world_dag():
    """
    A simple "Hello World" DAG using the TaskFlow API.
    """

    @task()
    def greet():
        print("Hello World!")
        return "Hello"

    @task()
    def print_message(message):
        print(f"Message received: {message}")

    # Set the dependency between tasks
    hello_message = greet()
    print_message(hello_message)

# Instantiate the DAG
hello_world_dag()

