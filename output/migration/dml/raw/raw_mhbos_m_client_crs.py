"""
Purpose:    RAW-DML,Load the data file into the target table's same-day partition
Author:     zjj
Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_mhbos_m_client_crs
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

source_name = "mhbos"
table_name = "m_client_crs"
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
DROP TABLE IF EXISTS {params["raw_schema"]}.mhbos_m_client_crs_et
""")

spark.sql(rf"""
CREATE TABLE IF NOT EXISTS {params["raw_schema"]}.mhbos_m_client_crs_et (
  client_no STRING, /* */
  controlling_name STRING, /* */
  country_tax_residence STRING, /* */
  tax_identification_no STRING, /* */
  reason STRING, /* */
  reason_remarks STRING, /* */
  entity_type STRING, /* */
  crs_tax_type STRING /* */
)
STORED AS PARQUET
TBLPROPERTIES (
  'PARQUET.COMPRESSION'='SNAPPY',
  'EXTERNAL.TABLE.PURGE'='TRUE'
)
""")

# -- JDBC Read Optimization cho bảng dbo.m_client_crs --
jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

query = """
SELECT client_no
,controlling_name
,country_tax_residence
,tax_identification_no
,reason
,reason_remarks
,entity_type
,crs_tax_type
FROM dbo.m_client_crs (nolock) 
WHERE 1=1
"""

# Read from MSSQL
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", query)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)

# df = df.withColumn("etl_timestamp", current_timestamp().cast("string"))
# df = df.withColumn("etl_dt", lit(batch_date))


# Write directly to Hive
et_table = f"{params['raw_schema']}.mhbos_m_client_crs_et"
target_cols = spark.table(et_table).columns
df = df.select(*target_cols)

(
    df.write
    .mode("overwrite")
    .insertInto(et_table)
)


# add queries here

spark.sql(rf"""
/* 2.2 insert data to target table */
INSERT INTO {params["raw_schema"]}.mhbos_m_client_crs PARTITION(etl_dt = '{batch_date}')
SELECT
  client_no, /* */
  controlling_name, /* */
  country_tax_residence, /* */
  tax_identification_no, /* */
  reason, /* */
  reason_remarks, /* */
  entity_type, /* */
  crs_tax_type, /* */
  CURRENT_TIMESTAMP() AS etl_timestamp /* ETL_processing time */
FROM {params["raw_schema"]}.mhbos_m_client_crs_et
""")


# Stop Spark when done
spark.stop()