
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
/* 2.3 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification number. */
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no (
  `client_no` STRING,
  `id_type` STRING,
  `ic_no_new` STRING,
  `ic_no_old` STRING,
  `secondary_id_type` STRING,
  `secondary_id_no` STRING,
  `primary_identification_no` STRING,
  `primary_identification_no_flag` STRING
)
""")

spark.sql(rf"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no
""")

spark.sql(rf"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no
SELECT
  mmc.client_no,
  mmc.id_type,
  mmc.ic_no_new,
  mmc.ic_no_old,
  mmc.secondary_id_type,
  mmc.secondary_id_no,
  (
    CASE
      WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '3'
      AND LENGTH(mmc.ic_no_new) < 12
      AND LENGTH(mmc.secondary_id_no) >= 12
      AND LENGTH(mmc.secondary_id_no) <= 15
      THEN mmc.secondary_id_no /* updated 20250502 */
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') <> ''
      AND LENGTH(mmc.secondary_id_no) < 12
      THEN mmc.ic_no_new /* updated 20250502 */
      WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
      THEN mmc.ic_no_new /* updated 20250502 */
      WHEN mmc.id_type = '4'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '5'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '6'
      THEN mmc.ic_no_new
      WHEN COALESCE(mmc.id_type, '') = ''
      AND COALESCE(mmc.secondary_id_type, '') <> ''
      AND COALESCE(mmc.secondary_id_no, '') <> ''
      THEN mmc.secondary_id_no
      ELSE '@[' || mmc.ic_no_new || ']'
    END
  ) AS primary_identification_no,
  (
    CASE
      WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
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
      WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN '0'
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
      THEN '0'
      WHEN mmc.id_type = '3'
      AND LENGTH(mmc.ic_no_new) < 12
      AND LENGTH(mmc.secondary_id_no) >= 12
      AND LENGTH(mmc.secondary_id_no) <= 15
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') <> ''
      AND LENGTH(mmc.secondary_id_no) < 12
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '6'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = ''
      AND COALESCE(mmc.secondary_id_type, '') <> ''
      AND COALESCE(mmc.secondary_id_no, '') <> ''
      THEN '0'
      ELSE '1'
    END
  ) AS primary_identification_no_flag
FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
