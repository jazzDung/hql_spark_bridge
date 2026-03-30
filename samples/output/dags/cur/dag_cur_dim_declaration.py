import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.models import Variable

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config_ds

DAG_ID = "cur_dim_declaration"

schedule, catchup, default_args = load_dag_config_ds(DAG_ID, __file__)

dataset_flow = None
if schedule:
    dataset_flow = [Dataset(name) for name in schedule]

dag = DAG(
    dag_id=DAG_ID,
    schedule=dataset_flow,
    max_active_tasks=1,
)

raw_fra_connected_parties = BashOperator(
    task_id="raw_fra_connected_parties",
    bash_command="echo 1",
    dag=dag
)
