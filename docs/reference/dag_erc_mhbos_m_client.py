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

source_name = "mhbos"
table_name = "m_client"
DAG_ID = "erc_" + source_name + "_" + table_name

schedule, catchup, default_args = load_dag_config(DAG_ID, __file__)

# connection env variable for ctr and mssql source, eg: get_env("etl_control", "mhbos")
env = get_env("etl_control", "mhbos")

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


# RAW
raw_mhbos_m_client = BashOperator(
    task_id='raw_mhbos_m_client',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/raw_mhbos_m_client.py
        """,
    env=env,
    dag=dag,
)

# COM_R
com_r_mhbos_m_client = BashOperator(
    task_id='com_r_mhbos_m_client',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_com }}/com_r_mhbos_m_client.py
        """,
    dag=dag,
)

# COM_T
com_t_mhbos_m_client = BashOperator(
    task_id='com_t_mhbos_m_client',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_com }}/com_t_mhbos_m_client.py
        """,
    dag=dag,
)

# COM_M
com_m_mhbos_m_client = BashOperator(
    task_id='com_m_mhbos_m_client',
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_com }}/com_m_mhbos_m_client.py
        """,
    dag=dag,
)




# trigger for cur flow
dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
)

etl_run_end = PythonOperator(
    task_id="etl_run_end",
    python_callable=etl_run_end,
    op_kwargs={
            "source_name": source_name,
            "table_name": table_name
    },
    dag=dag,
)

etl_run_start >> raw_mhbos_m_client >> com_r_mhbos_m_client >> com_t_mhbos_m_client >> com_m_mhbos_m_client >> finish >> etl_run_end

