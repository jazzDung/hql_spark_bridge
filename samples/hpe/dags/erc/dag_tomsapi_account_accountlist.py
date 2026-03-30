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

source_name = "tomsapi"
table_name = "account_accountlist"
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

# -----------------------------------------

tomsapi_account_accountlist_post = BashOperator(
    task_id="tomsapi_account_accountlist_post",
    bash_command=f"""
        /jcmAgent/etlscript/execution_engine/apiintegrate/src/main.py tomsapi_account_accountlist_post {{ ds | replace('-', '') }} 
        """,
    env=env,
    dag=dag,
)

# -----------------------------------------

raw_tomsapi_account_accountlist = BashOperator(
    task_id="raw_tomsapi_account_accountlist",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_raw_tomsapi_account_accountlist_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)

# -----------------------------------------

raw_tomsapi_account_accountlist_address = BashOperator(
    task_id="raw_tomsapi_account_accountlist_address",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_raw_tomsapi_account_accountlist_address_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)


# -----------------------------------------

raw_tomsapi_account_accountlist_tax = BashOperator(
    task_id="raw_tomsapi_account_accountlist_tax",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_raw_tomsapi_account_accountlist_tax_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)


# -----------------------------------------

raw_tomsapi_account_accountlist_bank = BashOperator(
    task_id="raw_tomsapi_account_accountlist_bank",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_raw_tomsapi_account_accountlist_bank_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)


# -----------------------------------------

com_t_tomsapi_account_accountlist = BashOperator(
    task_id="com_t_tomsapi_account_accountlist",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_com_t_tomsapi_account_accountlist_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)


# -----------------------------------------

com_t_tomsapi_account_accountlist_address = BashOperator(
    task_id="com_t_tomsapi_account_accountlist_address",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_com_t_tomsapi_account_accountlist_address_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)

# -----------------------------------------

com_t_tomsapi_account_accountlist_tax = BashOperator(
    task_id="com_t_tomsapi_account_accountlist_tax",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_com_t_tomsapi_account_accountlist_tax_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)

# -----------------------------------------

com_t_tomsapi_account_accountlist_bank = BashOperator(
    task_id="com_t_tomsapi_account_accountlist_bank",
    bash_command="""
        set -euo pipefail
        {{ var.value.spark_submit_config_low }} \
        {{ var.value.script_home_raw }}/test_com_t_tomsapi_account_accountlist_bank_df.py \
        --batch_date {{ ds | replace('-', '') }}
        """,
    env=env,
    dag=dag,
)

dataset_flow = Dataset(dag.dag_id)

finish = EmptyOperator(
    task_id="finish",
    outlets=[dataset_flow]
)


tomsapi_account_accountlist_post >> [
    [raw_tomsapi_account_accountlist >> com_t_tomsapi_account_accountlist],
    [raw_tomsapi_account_accountlist_address >> com_t_tomsapi_account_accountlist_address],
    [raw_tomsapi_account_accountlist_tax >> com_t_tomsapi_account_accountlist_tax],
    [raw_tomsapi_account_accountlist_bank >> com_t_tomsapi_account_accountlist_bank],
] >> finish