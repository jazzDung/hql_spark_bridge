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

import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, batch_start, batch_end

source_name = "fra"
table_name = "connected_parties"

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter()

# spark session
spark, batch_date = run_etl(source_name, table_name)

# update control table start, first task in the flow only
batch_start(spark, source_name, table_name)

# add queries here
spark.sql(f"""
/* 1.1 drop external table partition */
DROP TABLE IF EXISTS {params["raw_schema"]}.fra_connected_parties_et
""")

spark.sql(f"""
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
USING TEXTFILE
LOCATION '${itl_data_path}/fra_connected_parties_i.{batch_date}.dat'
TBLPROPERTIES (
  'skip.header.line.count'='1'
)
""")

spark.sql(f"""
ALTER TABLE {params["raw_schema"]}.fra_connected_parties DROP IF EXISTS   PARTITION(etl_dt = '{params["retention_raw_delta"]}')
""")

spark.sql(f"""
ALTER TABLE {params["raw_schema"]}.fra_connected_parties DROP IF EXISTS   PARTITION(etl_dt = '{batch_date}')
""")

spark.sql(f"""
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


# update control table end, last task in the flow only
batch_end(spark, source_name, table_name)

# Stop Spark when done
spark.stop()