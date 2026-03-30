import os
import re
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, batch_start, batch_end_sftp_daily, get_available_files

source_name = "fra"
table_name = "connected_parties"
sftp_conn_id = "fra"
flow_name = f"{source_name}_{table_name}"

params = set_parameter()
print(params)

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = yesterday_date


batch_start(spark, source_name, table_name)

# Stop Spark when done
spark.stop()