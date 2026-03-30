import os
import re
import psycopg2
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function_postgres import run_etl, set_parameter, drop_partition_day, batch_start, batch_end_sftp_daily, get_available_files

source_name = "fra"
table_name = "connected_parties"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)

print(f"ext_start_time     : {ext_start_time}")
print(f"ext_end_time       : {ext_end_time}")
print(f"today_date         : {today_date}")
print(f"Yesterday date     : {yesterday_date}")

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)
print(params["raw_schema"])

try:
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"]
    )
    cur = conn.cursor()

    # Update a control table after Spark job
    cur.execute("""
        UPDATE public.c_etl_control
        SET last_etl_start_time = CURRENT_TIMESTAMP
    """)

    conn.commit()
    print("Control table updated successfully.")

except Exception as e:
    print("Error updating control table:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()