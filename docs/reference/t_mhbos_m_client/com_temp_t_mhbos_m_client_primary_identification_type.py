
import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timedelta


source_name = "mhbos"
table_name = "m_client"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
last_date = (datetime.strptime(batch_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')


# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)



spark.sql(rf"""
/* 2.2 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification type. */
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type (
  `client_no` STRING,
  `id_type` STRING,
  `secondary_id_type` STRING,
  `primary_identification_type` STRING,
  `primary_identification_type_flag` STRING
)
""")

spark.sql(rf"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type
""")

spark.sql(rf"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type
SELECT
  mmc.client_no,
  mmc.id_type,
  mmc.secondary_id_type,
  (
    CASE
      WHEN mmc.id_type = '1'
      THEN '1'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN '1'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN '3'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN '2'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN '6'
      WHEN mmc.id_type = '2'
      THEN '3'
      WHEN mmc.id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '4'
      THEN '5'
      WHEN mmc.id_type = '5'
      THEN '2'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '6'
      THEN '6'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
      THEN '1'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
      THEN '3'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
      THEN '5'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
      THEN '2'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
      THEN '6'
      ELSE '@[' || mmc.id_type || ']'
    END
  ) AS primary_identification_type,
  (
    CASE
      WHEN mmc.id_type = '1'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN '0'
      WHEN mmc.id_type = '2'
      THEN '0'
      WHEN mmc.id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '6'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
      THEN '0'
      ELSE '1'
    END
  ) AS primary_identification_type_flag
FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
