import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config, get_env, etl_run_start

source_name = "k2"
table_name = "account"
DAG_ID = source_name + "_" + table_name

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

# connection env variable for ctr and mssql source, eg: get_env("etl_control", "k2")
env = get_env("etl_control")

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=schedule,
    catchup=catchup,
    max_active_tasks=1,
)

etl_run_start = PythonOperator(
    task_id="etl_run_start",
    python_callable=etl_run_start,
    op_kwargs={
            "source_name": source_name,
            "table_name": table_name
    },
    dag=dag,
)

# trigger for cur flow
dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
)

etl_run_start >> finish