import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config

DAG_ID = "guava_company"

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=schedule,
    catchup=catchup,
    max_active_tasks=1,
)

raw_fra_connected_parties = BashOperator(
    task_id="raw_fra_connected_parties",
    bash_command="echo 1",
    dag=dag
)

dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
)

raw_fra_connected_parties >> finish