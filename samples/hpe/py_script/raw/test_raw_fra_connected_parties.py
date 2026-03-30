import os
import re
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, batch_start, batch_end_sftp_daily, get_available_files

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter()

source_name = "fra"
table_name = "connected_parties"
file_pattern = "ConnectedParties_Data_{}.TXT"
base_path = params["cluster_mount_point"]+params["itl_data_path"]

# spark session
spark, batch_date_start, batch_date_end, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# batch start
batch_start(spark, source_name, table_name)

# housekeep partition if required
drop_partition_day(spark, batch_date, params["raw_schema"], "fra_connected_parties", "etl_dt", params["retention_raw_delta"])

# eg: if file name = ConnectedParties_Data_20260307.TXT, prefix = ConnectedParties_Data_, ext = .TXT
min_date, max_date, file_list = get_available_files(base_path, batch_date_start, batch_date, file_pattern)

print(f"min_date         : {min_date}")
print(f"max_date         : {max_date}")
print(f"Files to process : {file_list}")

spark.sql(f"""
alter table {params["raw_schema"]}.fra_connected_parties drop if exists partition (etl_dt='{batch_date}')
""")

for file_path in file_list:
  file_path_mapr = file_path.replace(params["cluster_mount_point"],'')
  filename = os.path.basename(file_path)
  print(f"Ingesting file {file_path_mapr} ...")
  
  spark.sql(f"""
  drop table if exists {params["raw_schema"]}.fra_connected_parties_et
  """)
  
  spark.sql(f"""
  create external table if not exists {params["raw_schema"]}.fra_connected_parties_et(
      staffname string -- 
      ,staff_nric string -- 
      ,staff_oldic string -- 
      ,cp_name string -- 
      ,cp_nric string -- 
      ,cp_oldic string -- 
      ,cp_relationship string -- 
      ,actual_status string -- 
      ,define_status string -- 
      ,processdate timestamp -- 
  )
  row format delimited
  fields terminated by '|'
  lines terminated by '\n'
  stored as textfile
  location '{file_path_mapr}'
  tblproperties (
      "skip.header.line.count"="1"
  )
  """)
  
  spark.sql(f"""
  insert into table {params["raw_schema"]}.fra_connected_parties partition ( etl_dt = '{batch_date}' )
  select
      staffname -- 
      ,staff_nric -- 
      ,staff_oldic -- 
      ,cp_name -- 
      ,cp_nric -- 
      ,cp_oldic -- 
      ,cp_relationship -- 
      ,actual_status -- 
      ,define_status -- 
      ,processdate -- 
      ,'{filename}'
      ,current_timestamp() as etl_timestamp --ETL_processing time
  from {params["raw_schema"]}.fra_connected_parties_et
  where staffname <> 'StaffName'
  """)

# update control table end, last task in the flow only
#batch_end_sftp_daily(spark, batch_date, source_name, table_name)

# Stop Spark when done
spark.stop()