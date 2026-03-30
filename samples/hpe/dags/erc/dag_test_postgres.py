import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config, sftp_download_files, get_env

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

env = get_env("etl_control")

dag = DAG(
    'test_postgres',
    default_args=default_args,
    description='',
    schedule=None,
    catchup=False,
    max_active_tasks=3,
)

test_postgres = BashOperator(
    task_id='test_postgres',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_postgres.py
        """,
    env=env,
    dag=dag,
)