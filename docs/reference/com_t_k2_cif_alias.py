import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day

source_name = "k2"
table_name = "cif_alias"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
batch_yyyymm = batch_date[:6]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_k2_cif_alias_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated (
  cifaliasid string
    ,cifid string
    ,aliastype string
    ,aliasvalue string
    ,effectivefrom timestamp
    ,effectiveto timestamp
    ,updateuser bigint
    ,updatets timestamp   
    ,record_status      VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
  ,'external.table.purge'='true'
)
""")

spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated
SELECT
     cifaliasid
    ,cifid
    ,aliastype
    ,aliasvalue
    ,effectivefrom
    ,effectiveto
    ,updateuser
    ,updatets
   ,'A' as record_status
   ,record_created_date
   ,record_updated_date
FROM {params["com_schema"]}.t_k2_cif_alias com
WHERE NOT EXISTS 
    (SELECT 1 FROM {params["raw_schema"]}.k2_cif_alias r
   WHERE r.etl_dt = '{batch_date}'
      AND r.cifaliasid = com.cifaliasid
)
""")

spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated
SELECT
     r.cifaliasid
    ,r.cifid
    ,r.aliastype
    ,r.aliasvalue
    ,r.effectivefrom
    ,r.effectiveto
    ,r.updateuser
    ,r.updatets
   ,'A' as record_status
   ,CASE 
        WHEN com.cifaliasid IS NOT NULL 
        THEN com.record_created_date 
        ELSE current_timestamp()
    END AS record_created_date
   ,current_timestamp() AS record_updated_date

FROM {params["raw_schema"]}.k2_cif_alias r
LEFT JOIN {params["com_schema"]}.t_k2_cif_alias com
    ON r.cifaliasid = com.cifaliasid
WHERE r.etl_dt = '{batch_date}'
AND
      com.cifaliasid IS NULL
   OR nvl(r.cifid,'')      <> nvl(com.cifid,'')
   OR nvl(r.aliastype,'')  <> nvl(com.aliastype,'')
   OR nvl(r.aliasvalue,'') <> nvl(com.aliasvalue,'')
   OR nvl(r.effectivefrom,'')   <> nvl(com.effectivefrom,'')
   OR nvl(r.effectiveto,'')  <> nvl(com.effectiveto,'')
   OR nvl(r.updateuser,'')   <> nvl(com.updateuser,'')
   OR nvl(r.updatets,'')  <> nvl(com.updatets,'')

""")

spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.t_k2_cif_alias
SELECT
     cifaliasid
    ,cifid
    ,aliastype
    ,aliasvalue
    ,effectivefrom
    ,effectiveto
    ,updateuser
    ,updatets
   ,record_status 
   ,record_created_date
   ,record_updated_date
   ,'{batch_date}'      AS etl_dt
   ,current_timestamp() AS etl_timestamp
FROM {params["com_schema"]}.temp_t_k2_cif_alias_updated

""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.t_k2_cif_alias COMPUTE STATISTICS""")

spark.stop()
