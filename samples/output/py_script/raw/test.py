import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, get_available_files

source_name = "fra"
table_name = "connected_parties"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"
file_pattern = "ConnectedParties_Data_{}.TXT"


# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date


# Stop Spark when done
spark.stop()