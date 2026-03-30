from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="update_flow_config",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    def export_flow_config():
        hook = PostgresHook(postgres_conn_id="etl_control")
    
        records = hook.get_records("""
            SELECT 
                flow_name,
                cron as schedule,
                email,
                email_on_failure,
                email_on_retry,
                retries,
                retry_delay,
                catchup
            FROM public.c_flow_config a 
            left join public.c_schedule_cron b on a.schedule = b.schedule
        """)
    
        columns = [
            "flow_name", "schedule", "email",
            "email_on_failure", "email_on_retry",
            "retries", "retry_delay", "catchup"
        ]
    
        data = [dict(zip(columns, row)) for row in records]
    
        output_path = "/mapr/Edfdev.kenanga.local/EDF/dags/erc/flow_config.json"
    
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
    
    export_json = PythonOperator(
        task_id="export_flow_config",
        python_callable=export_flow_config,
        dag=dag,
    )

    def export_flow_config_ds():
        hook = PostgresHook(postgres_conn_id="etl_control")
    
        records = hook.get_records("""
            SELECT 
                flow_name,
                schedule,
                email,
                email_on_failure,
                email_on_retry,
                retries,
                retry_delay,
                catchup
            FROM public.c_flow_config_ds
        """)
    
        columns = [
            "flow_name", "schedule", "email",
            "email_on_failure", "email_on_retry",
            "retries", "retry_delay", "catchup"
        ]
    
        data = [dict(zip(columns, row)) for row in records]
    
        output_path = "/mapr/Edfdev.kenanga.local/EDF/dags/cur/flow_config.json"
    
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
    
    export_json_ds = PythonOperator(
        task_id="export_flow_config_ds",
        python_callable=export_flow_config_ds,
        dag=dag,
    )