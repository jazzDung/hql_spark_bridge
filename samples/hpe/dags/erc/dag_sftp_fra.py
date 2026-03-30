from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/dags")
from dag_common_function import sftp_download_files

# ------------- SFTP CONFIGURATION --------------
source_name = 'fra'                                            # Source name
table_name = 'connected_parties'                               # Table name
sftp_conn_id = 'fra'                                           # Airflow connection ID
remote_path = '/'                                              # Path on SFTP server
local_base_path = '/mapr/Edfdev.kenanga.local/data/input/'     # Base local folder
file_pattern = 'ConnectedParties_Data_{}.TXT'                  # Filename pattern with date
# -----------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sftp_download_by_date_folder',
    default_args=default_args,
    description='Download files from SFTP into folders by date',
    schedule=None,  # manual trigger
    start_date=datetime(2026, 3, 16),
    catchup=False,
) as dag:

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
    
    raw_fra_connected_parties = BashOperator(
        task_id="raw_fra_connected_parties",
        #bash_command="/opt/mapr/spark/spark-3.5.5/bin/spark-submit /mapr/Edfdev.kenanga.local/EDF/py_script/raw/raw_fra_connected_parties.py",
        bash_command="{{ var.value.spark_submit_config_low }} {{ var.value.script_home_raw }}/test_raw_fra_connected_parties.py",
        dag=dag
    )

    sftp_download_task >> raw_fra_connected_parties