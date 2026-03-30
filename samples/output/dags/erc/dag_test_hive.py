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
from dag_common_function import load_dag_config, get_env, etl_run_start, etl_run_end

source_name = "k2"
table_name = "account"
DAG_ID = source_name + "_" + table_name

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

# connection env variable for ctr and mssql source, eg: get_env("etl_control", "k2")
env = get_env("etl_control", "cloudera")
print(env)

DAG_ID = "test_hive"

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=schedule,
    catchup=catchup,
    max_active_tasks=1,
)

# replace the script name below
test_hive = BashOperator(
    task_id="test_hive",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_hive.py
        """,
    env=env,
    dag=dag,
)