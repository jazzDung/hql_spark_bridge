"""
Purpose:    RAW-DML,Load the data file into the target table's same-day partition
Author:     zjj
Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_fra_connected_parties
CreateDate: 20230809
FileType:   DML
Logs:
1.for hive 3.x on cdp 7.1.5
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

source_name = "fra"
table_name = "connected_parties"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
last_date = yesterday_date
batch_yyyymm = batch_date[:-2]

# ext_start_time = datetime.strptime(ext_start_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# ext_end_time = datetime.strptime(ext_end_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)



spark.sql(rf"""
/* 1.1 drop external table partition */
DROP TABLE IF EXISTS {params["raw_schema"]}.fra_connected_parties_et
""")

spark.sql(rf"""
CREATE EXTERNAL TABLE IF NOT EXISTS {params["raw_schema"]}.fra_connected_parties_et (
  staffname STRING, /* */
  staff_nric STRING, /* */
  staff_oldic STRING, /* */
  cp_name STRING, /* */
  cp_nric STRING, /* */
  cp_oldic STRING, /* */
  cp_relationship STRING, /* */
  actual_status STRING, /* */
  define_status STRING, /* */
  processdate TIMESTAMP /* */
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{itl_data_path}/fra/ConnectedParties_Data_{batch_date}.TXT'
TBLPROPERTIES (
  'skip.header.line.count'='1'
)
""")

spark.sql(rf"""
/* 2.2 insert data to target table */
INSERT INTO {params["raw_schema"]}.fra_connected_parties PARTITION(etl_dt = '{batch_date}')
SELECT
  staffname, /* */
  staff_nric, /* */
  staff_oldic, /* */
  cp_name, /* */
  cp_nric, /* */
  cp_oldic, /* */
  cp_relationship, /* */
  actual_status, /* */
  define_status, /* */
  processdate, /* */
  CURRENT_TIMESTAMP() AS etl_timestamp /* ETL_processing time */
FROM {params["raw_schema"]}.fra_connected_parties_et
""")


# Stop Spark when done
spark.stop()