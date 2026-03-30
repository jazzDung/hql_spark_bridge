import json
import os
import subprocess
from datetime import datetime, timedelta
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook


def sftp_download_files(source_name, table_name, sftp_conn_id, remote_path, local_base_path, file_pattern):
    query = """
        SELECT 
            ext_start_time,
            COALESCE(ext_end_time, TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYYMMDD')) AS ext_end_time
        FROM public.c_etl_run
        WHERE source_name = %s 
          AND table_name = %s
    """

    pg_hook = PostgresHook(postgres_conn_id="etl_control")
    records = pg_hook.get_records(query, parameters=(source_name, table_name))

    if not records:
        raise ValueError("Query returns no rows")

    ext_start_time, ext_end_time = records[0]
    
    hook = SFTPHook(ssh_conn_id=sftp_conn_id)

    start_date = datetime.strptime(ext_start_time, '%Y%m%d')
    end_date = datetime.strptime(ext_end_time, '%Y%m%d')
    delta = timedelta(days=1)
    
    files_to_download = []
    current_date = start_date
    while current_date <= end_date and end_date >= start_date:
        date_str = current_date.strftime('%Y%m%d')

        filename = file_pattern.format(date_str)
        remote_file = os.path.join(remote_path, filename)

        # Only add to list if file exists on SFTP
        try:
            # This will throw if file doesn't exist
            hook.path_exists(remote_file)
            files_to_download.append((remote_file, date_str))
        except Exception as e:
            # file not found — just ignore
            print(f"File {remote_file} not found: {e}")

        current_date += delta

    # --- Skip if no files at all ---
    if not files_to_download:
        raise AirflowSkipException(
            f"No files to download for {source_name}/{table_name} "
            f"between {ext_start_time} and {ext_end_time}"
        )

    # --- Perform downloads for existing files ---
    for remote_file, date_str in files_to_download:

        local_path = os.path.join(local_base_path, date_str)
        os.makedirs(local_path, exist_ok=True)

        local_file = os.path.join(local_path, os.path.basename(remote_file))
        print(f"Downloading {remote_file} ? {local_file}")

        try:
            hook.retrieve_file(remote_file, local_file)
        except Exception as e:
            print(f"Error retrieving {remote_file}: {e}")


def str_to_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() == "true"
    return False


def load_dag_config(dag_id, dag_file):
    dag_dir = os.path.dirname(dag_file)
    config_path = os.path.join(dag_dir, "flow_config.json")

    with open(config_path) as f:
        flows = json.load(f)

    flow_config = next((f for f in flows if f.get("flow_name") == dag_id), None)

    if flow_config is None:
        raise ValueError(f"No config found for DAG_ID={dag_id}")

    retry_delay = timedelta(
        minutes=int(flow_config.get("retry_delay_minutes", 5))
    )

    schedule = flow_config.get("schedule")
    if schedule == "None" or schedule is None:
        schedule = None

    email_on_failure = str_to_bool(flow_config.get("email_on_failure", False))
    email_on_retry = str_to_bool(flow_config.get("email_on_retry", False))
    catchup = str_to_bool(flow_config.get("catchup", False))

    emails_raw = flow_config.get("email", "")

    if isinstance(emails_raw, str):
        emails = [e.strip() for e in emails_raw.split(",") if e.strip()]
    else:
        emails = emails_raw

    default_args = {
        "start_date": datetime(2025, 4, 22),
        "email": emails,
        "email_on_failure": email_on_failure,
        "email_on_retry": email_on_retry,
        "retries": flow_config.get("retries", 1),
        "retry_delay": retry_delay,
    }

    return schedule, catchup, default_args

 
def load_dag_config_ds(dag_id, dag_file):
    dag_dir = os.path.dirname(dag_file)
    config_path = os.path.join(dag_dir, "flow_config.json")

    with open(config_path) as f:
        flows = json.load(f)

    flow_config = next((f for f in flows if f.get("flow_name") == dag_id), None)

    if flow_config is None:
        raise ValueError(f"No config found for DAG_ID={dag_id}")

    retry_delay = timedelta(
        minutes=int(flow_config.get("retry_delay_minutes", 5))
    )

    schedule_raw = flow_config.get("schedule")

    if schedule_raw in (None, "None"):
        schedule = None

    elif isinstance(schedule_raw, str):
        schedule = [s.strip() for s in schedule_raw.split(",") if s.strip()]

    elif isinstance(schedule_raw, list):
        schedule = schedule_raw

    email_on_failure = str_to_bool(flow_config.get("email_on_failure", False))
    email_on_retry = str_to_bool(flow_config.get("email_on_retry", False))
    catchup = str_to_bool(flow_config.get("catchup", False))

    emails_raw = flow_config.get("email", "")

    if isinstance(emails_raw, str):
        emails = [e.strip() for e in emails_raw.split(",") if e.strip()]
    else:
        emails = emails_raw

    default_args = {
        "start_date": datetime(2025, 4, 22),
        "email": emails,
        "email_on_failure": email_on_failure,
        "email_on_retry": email_on_retry,
        "retries": flow_config.get("retries", 1),
        "retry_delay": retry_delay,
    }

    return schedule, catchup, default_args


def get_env(*conn_ids):
    env = {}
    for conn_id in conn_ids:
        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type.lower() == "postgres":
            env.update({
                "POSTGRES_HOST": conn.host,
                "POSTGRES_PORT": str(conn.port),
                "POSTGRES_DB": conn.schema,
                "POSTGRES_USER": conn.login,
                "POSTGRES_PASSWORD": conn.password,
            })
        elif conn.conn_type.lower() == "mssql":
            env.update({
                "MSSQL_HOST": conn.host,
                "MSSQL_PORT": str(conn.port),
                "MSSQL_DB": conn.schema,
                "MSSQL_USER": conn.login,
                "MSSQL_PASSWORD": conn.password,
            })
        elif conn.conn_type.lower() == "hiveserver2":
            env.update({
                "HIVE_HOST": conn.host,
                "HIVE_PORT": str(conn.port),
                "HIVE_DB": conn.schema,
                "HIVE_USER": conn.login,
                "HIVE_PASSWORD": conn.password,
            })
    return env


def etl_run_start(source_name, table_name):
    hook = PostgresHook(postgres_conn_id="etl_control")
    hook.run(f"""
        UPDATE public.c_etl_run
        SET last_etl_start_time = CURRENT_TIMESTAMP,
        last_etl_end_time = null
        WHERE source_name = '{source_name}' AND table_name = '{table_name}'
    """)


def etl_run_end(source_name, table_name):
    hook = PostgresHook(postgres_conn_id="etl_control")
    hook.run(f"""
        UPDATE public.c_etl_run
        SET last_etl_end_time = CURRENT_TIMESTAMP,
        last_ext_start_time = ext_start_time,
        last_ext_end_time = ext_end_time,
        ext_start_time = TO_CHAR(TO_DATE(ext_start_time, 'YYYYMMDD') + INTERVAL '1 day', 'YYYYMMDD'),
        ext_end_time = TO_CHAR(TO_DATE(ext_end_time, 'YYYYMMDD') + INTERVAL '1 day', 'YYYYMMDD')
        WHERE source_name = '{source_name}' AND table_name = '{table_name}'
    """)