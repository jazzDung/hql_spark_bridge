import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.datasets import Dataset

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import load_dag_config, get_env, sftp_download_files, etl_run_start, etl_run_end_daily, check_should_continue

# ------------- SFTP CONFIGURATION - sftp source only -------------------------------------
source_name = 'mhbos'                                          # Source name in c_etl_run
table_name = 'm_client_ext'                                      # Table name in c_etl_run
sftp_conn_id = 'sftp_datalake'                                 # Airflow connection ID
remote_path = '/input/{}/mhbos'                                # Path on SFTP server, replace yyyymmdd to {}
local_base_path = '/mapr/Edfdev.kenanga.local/data/input/'     # Base local folder
file_pattern = 'mhbos_m_client_ext_i.{}.dat'                     # Replace yyyymmdd to {}
# -----------------------------------------------------------------------------------------
DAG_ID = "erc_" + source_name + "_" + table_name

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

# connection env variable for ctr and mssql source, eg: get_env("etl_control", "k2")
env = get_env("etl_control", "mhbos")

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=schedule,
    catchup=catchup,
    max_active_tasks=1,
)
'''
etl_run_start = PythonOperator(
    task_id="etl_run_start",
    python_callable=etl_run_start,
    op_kwargs={
            "source_name": source_name,
            "table_name": table_name
    },
    dag=dag,
)
'''
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
    },
    dag=dag,
)
# -----------------------------------------


raw_mhbos_m_client_ext_nas = BashOperator(
    task_id='raw_mhbos_m_client_ext_nas',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/raw_mhbos_m_client_ext_nas.py
        """,
    env=env,
    dag=dag,
)
'''


raw_mhbos_m_client_ext = BashOperator(
    task_id='raw_mhbos_m_client_ext',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/raw_mhbos_m_client_ext.py
        """,
    env=env,
    dag=dag,
)


check_task = ShortCircuitOperator(
    task_id='check_task',
    python_callable=check_should_continue,
    op_kwargs={
        "upstream_task_id": "raw_mhbos_m_client_ext"
    }
)

# trigger for cur flow
dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
#)

etl_run_end_daily = PythonOperator(
    task_id="etl_run_end_daily",
    python_callable=etl_run_end_daily,
    op_kwargs={
            "source_name": source_name,
            "table_name": table_name
    },
    dag=dag,
)
'''

#raw_mhbos_m_client_ext
sftp_download_task >> raw_mhbos_m_client_ext_nas