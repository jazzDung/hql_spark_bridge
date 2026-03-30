from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Step 1: Function to fetch Postgres table and store as Airflow Variables
def fetch_postgres_table_to_variables():
    hook = PostgresHook(postgres_conn_id="etl_control")

    records = hook.get_records("""
        SELECT var_name, var_value
        FROM public.c_flow_variable
    """)

    if not records:
        print("No variables found.")
        return

    result_dict = {}

    for key, value in records:
        Variable.set(key, value)
        result_dict[key] = value

    print("Airflow Variables set from Postgres table:")
    print(result_dict)


# Step 2: Define the DAG
with DAG(
    dag_id="update_airflow_variable",
    start_date=datetime(2026, 2, 11),
    schedule=None,
    catchup=False,
) as dag:

    setup_variables_task = PythonOperator(
        task_id="fetch_and_set_variables",
        python_callable=fetch_postgres_table_to_variables,
    )

    setup_variables_task