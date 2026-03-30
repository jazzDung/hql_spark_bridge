import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.models import Variable

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config, sftp_download_files, get_env

source_name = "fra"
table_name = "connected_parties"
DAG_ID = source_name + "_" + table_name

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

# connection env variable for ctr and mssql source, eg: get_env("etl_control", "k2")
env = get_env("etl_control")

# ------------- SFTP CONFIGURATION - sftp source only -------------------------------------
source_name = 'fra'                                            # Source name in c_etl_run
table_name = 'connected_parties'                               # Table name in c_etl_run
sftp_conn_id = 'fra'                                           # Airflow connection ID
remote_path = '/'                                              # Path on SFTP server
local_base_path = '/mapr/Edfdev.kenanga.local/data/input/'     # Base local folder
file_pattern = 'ConnectedParties_Data_{}.TXT'                  # Filename pattern with date
# -----------------------------------------------------------------------------------------

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=schedule,
    catchup=catchup,
    max_active_tasks=1,
)

# sftp source only ------------------------
sftp_download_task = PythonOperator(
    task_id='sftp_download_task',
    python_callable=sftp_download_files,
    op_kwargs={
        "source_name": source_name,
        "table_name": table_name,
        "sftp_conn_id": sftp_conn_id,
        "remote_path": remote_path,
        "local_base_path": local_base_path,
        "file_pattern": file_pattern
    }
)
# -----------------------------------------
   
raw_fra_connected_parties = BashOperator(
    task_id="raw_fra_connected_parties",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_raw_fra_connected_parties_df.py
        """,
    env=env,
    dag=dag,
)

dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
)


sftp_download_task >> raw_fra_connected_parties >> finish