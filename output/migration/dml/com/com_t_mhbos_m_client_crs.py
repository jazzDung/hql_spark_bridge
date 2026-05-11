##  File Name   : com_t_mhbos_m_client_crs
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-11 08:24:47
##  Source      : com_r_mhbos_m_client_crs (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "mhbos"
table_name  = "m_client_crs"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_crs_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_crs_updated (
    client_no STRING
    , controlling_name STRING
    , country_tax_residence STRING
    , tax_identification_no STRING
    , reason STRING
    , reason_remarks STRING
    , entity_type STRING
    , crs_tax_type STRING
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_crs_updated
SELECT
    client_no
    , controlling_name
    , country_tax_residence
    , tax_identification_no
    , reason
    , reason_remarks
    , entity_type
    , crs_tax_type
    , 'A' AS dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
FROM {params["com_schema"]}.t_mhbos_m_client_crs com_t
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.mhbos_m_client_crs r
    WHERE r.etl_dt = '{batch_date}'
      AND r.client_no = com_t.client_no
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_crs_updated
SELECT
    r.client_no
    , r.controlling_name
    , r.country_tax_residence
    , r.tax_identification_no
    , r.reason
    , r.reason_remarks
    , r.entity_type
    , r.crs_tax_type
    , 'A' AS dl_record_status
    , CASE
        WHEN com_t.client_no IS NOT NULL THEN com_t.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
FROM {params["raw_schema"]}.mhbos_m_client_crs r
LEFT JOIN {params["com_schema"]}.t_mhbos_m_client_crs com_t
    ON r.client_no = com_t.client_no
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.t_mhbos_m_client_crs
SELECT
    client_no
    , controlling_name
    , country_tax_residence
    , tax_identification_no
    , reason
    , reason_remarks
    , entity_type
    , crs_tax_type
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , '{batch_date}'       AS etl_dt
    , current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_t_mhbos_m_client_crs_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.t_mhbos_m_client_crs COMPUTE STATISTICS""")

spark.stop()