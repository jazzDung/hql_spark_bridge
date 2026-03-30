from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime
import re

SFTP_CONN_ID = "fra"
REMOTE_DIR = "/"
PATTERN = r"ConnectedParties_Data_(\d{8}).TXT"


def list_sftp_files():
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

    # List all files in remote dir
    files = hook.list_directory(REMOTE_DIR)

    # Filter files matching pattern
    matched_files = [f for f in files if re.match(PATTERN, f)]

    # Sort descending (latest date first)
    latest_files = sorted(matched_files, reverse=True)[:10]

    print("Latest 10 files matching pattern:")
    for f in latest_files:
        print(f)


with DAG(
    dag_id="sftp_list_files_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_sftp_files
    )