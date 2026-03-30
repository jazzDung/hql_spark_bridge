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

schema = StructType([
    StructField("staffname", StringType(), True),
    StructField("staff_nric", StringType(), True),
    StructField("staff_oldic", StringType(), True),
    StructField("cp_name", StringType(), True),
    StructField("cp_nric", StringType(), True),
    StructField("cp_oldic", StringType(), True),
    StructField("cp_relationship", StringType(), True),
    StructField("actual_status", StringType(), True),
    StructField("define_status", StringType(), True),
    StructField("processdate", TimestampType(), True)
])

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

base_path = params["cluster_mount_point"]+params["itl_data_path"]

# housekeep partition if required
drop_partition_day(spark, batch_date, params["raw_schema"], hive_table_name, partition_col, params["retention_raw_delta"])


min_date, max_date, file_list = get_available_files(base_path, ext_start_time, ext_end_time, file_pattern)

print(f"min_date         : {min_date}")
print(f"max_date         : {max_date}")
print(f"Files to process : {file_list}")

spark.sql(f"""
alter table {params["raw_schema"]}.{hive_table_name} drop if exists partition ({partition_col}='{batch_date}')
""")

for file_path in file_list:
  file_path_mapr = file_path.replace(params["cluster_mount_point"],'')
  filename = os.path.basename(file_path)
  print(f"Ingesting file {file_path_mapr} ...")
    
  df = (
      spark.read
      .format("csv")
      .option("sep", "|")
      .option("header", True)  # skips first row
      .schema(schema)
      .load(file_path_mapr)
  )
  
  df = df.withColumn("filename", lit(filename))
  df = df.withColumn("etl_timestamp", current_timestamp().cast("string"))
  df = df.withColumn("etl_dt", lit(batch_date))
  
  target_table = f"{params['raw_schema']}.{hive_table_name}"
  target_cols = spark.table(target_table).columns
  df = df.select(*target_cols)
  
  (
      df.write
      .mode("append")
      .insertInto(f"{params['raw_schema']}.{hive_table_name}")
  )


# Stop Spark when done
spark.stop()