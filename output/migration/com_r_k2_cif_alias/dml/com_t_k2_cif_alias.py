##  File Name   : com_t_k2_cif_alias
##  File Type   : DML
##  Model       : 3
##  Generated   : 2026-05-06 00:24:50
##  Source      : com_r_k2_cif_alias (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "k2"
table_name  = "cif_alias"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_k2_cif_alias_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated (
    cifaliasid STRING,
    cifid STRING,
    aliastype STRING,
    aliasvalue STRING,
    effectivefrom TIMESTAMP,
    effectiveto TIMESTAMP,
    updateuser BIGINT,
    updatets TIMESTAMP,
    id_mark STRING
    ,record_status       VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated
SELECT
    cifaliasid,
    cifid,
    aliastype,
    aliasvalue,
    effectivefrom,
    effectiveto,
    updateuser,
    updatets,
    id_mark
    ,'A' AS record_status
    ,record_created_date
    ,record_updated_date
FROM {params["com_schema"]}.t_k2_cif_alias com
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.k2_cif_alias r
    WHERE r.etl_dt = '{batch_date}'
      AND r.cifaliasid = com.cifaliasid
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_k2_cif_alias_updated
SELECT
    r.cifaliasid,
    r.cifid,
    r.aliastype,
    r.aliasvalue,
    r.effectivefrom,
    r.effectiveto,
    r.updateuser,
    r.updatets,
    r.id_mark
    ,'A' AS record_status
    ,CASE WHEN com.cifaliasid IS NOT NULL THEN com.record_created_date
          ELSE current_timestamp() END AS record_created_date
    ,current_timestamp() AS record_updated_date
FROM {params["raw_schema"]}.k2_cif_alias r
LEFT JOIN {params["com_schema"]}.t_k2_cif_alias com
    ON r.cifaliasid = com.cifaliasid
WHERE r.etl_dt = '{batch_date}'
  AND (
      com.cifaliasid IS NULL
      OR nvl(r.cifid, '') <> nvl(com.cifid, '')
      OR nvl(r.aliastype, '') <> nvl(com.aliastype, '')
      OR nvl(r.aliasvalue, '') <> nvl(com.aliasvalue, '')
      OR nvl(r.effectivefrom, '') <> nvl(com.effectivefrom, '')
      OR nvl(r.effectiveto, '') <> nvl(com.effectiveto, '')
      OR nvl(r.updateuser, '') <> nvl(com.updateuser, '')
      OR nvl(r.updatets, '') <> nvl(com.updatets, '')
      OR nvl(r.id_mark, '') <> nvl(com.id_mark, '')
  )
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.t_k2_cif_alias
SELECT
    cifaliasid,
    cifid,
    aliastype,
    aliasvalue,
    effectivefrom,
    effectiveto,
    updateuser,
    updatets,
    id_mark
    ,record_status
    ,record_created_date
    ,record_updated_date
    ,md5(concat_ws('|'
        , nvl(cast(cifid AS STRING), '')
        , nvl(cast(aliastype AS STRING), '')
        , nvl(cast(aliasvalue AS STRING), '')
        , nvl(cast(effectivefrom AS STRING), '')
        , nvl(cast(effectiveto AS STRING), '')
        , nvl(cast(updateuser AS STRING), '')
        , nvl(cast(updatets AS STRING), '')
        , nvl(cast(id_mark AS STRING), '')
    )) AS hash_value
    ,'{batch_date}'       AS etl_dt
    ,current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_t_k2_cif_alias_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.t_k2_cif_alias COMPUTE STATISTICS""")
spark.stop()