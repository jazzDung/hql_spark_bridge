##  File Name   : com_None_dim_customer
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-18 08:07:28
##  Source      : cur_dim_customer_employment (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "cur"
table_name  = "dim_customer_employment_mhbos"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
# batch_date = today_date
batch_date = '20260513'
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_customer_employment_mhbos
""")

spark.sql(f"""
/* Create temp table */
    CREATE TABLE {params["cur_schema"]}.temp_dim_customer_employment_mhbos (
      CUSTOMER_ID VARCHAR(20) COMMENT '',
      CUSTOMER_EMPLOYER_NAME VARCHAR(100) COMMENT '',
      CUSTOMER_EMPLOYER_INDUSTRY VARCHAR(100) COMMENT '',
      CUSTOMER_EMPLOYER_TYPE VARCHAR(100) COMMENT '',
      CUSTOMER_AMLA_OCCUPATION VARCHAR(50) COMMENT '',
      CUSTOMER_CCRIS_OCCUPATION VARCHAR(10) COMMENT '',
      SOURCE_NAME VARCHAR(10) COMMENT '',
      SOURCE_RECORD_ID VARCHAR(20) COMMENT '',
      ETL_TIMESTAMP STRING COMMENT 'ETL_PROCESSING_TIME',
      SOURCE_UPDATE_DATE TIMESTAMP COMMENT '',
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS VARCHAR(200) COMMENT ''
    )
""")

spark.sql(f"""
/* ==============[Group.1]============== */
    INSERT INTO {params["cur_schema"]}.temp_dim_customer_employment_mhbos (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      T2.EMPLOYER_NAME AS CUSTOMER_EMPLOYER_NAME, /* None */
      T2.EMP_SECTOR AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      T2.EMP_TYPE AS CUSTOMER_EMPLOYER_TYPE, /* None */
      UPPER(T1.OCCUPATION) AS CUSTOMER_AMLA_OCCUPATION, /* None */
      T2.CCRIS_OCC_SUB AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      COALESCE(T1.DATE_CHANGE, T1.DATE_CREATED) AS SOURCE_UPDATE_DATE, /* 20251028 */
      UPPER(T2.TYPE_OF_BUSINESS) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND TO_DATE(T2.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    /* LEFT JOIN ${cur_schema}.REF_LOOKUP AS T3
        ON T3.SOURCE_KEY = 'MHBOS_M_PARA'
        AND T3.REFERENCE_TYPE = 'TYPE_OF_BUSINESS'
        AND T3.REFERENCE_CODE = T2.TYPE_OF_BUSINESS
      LEFT JOIN ${cur_schema}.REF_LOOKUP AS T4
        ON T3.SOURCE_KEY = 'MHBOS_M_PARA'
        AND T3.REFERENCE_TYPE = 'OCCUPATION'
        AND T3.REFERENCE_CODE = T1.OCCUPATION */
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T2.EMPLOYER_NAME, '')) <> ''
        OR COALESCE(TRIM(T2.EMP_SECTOR), '') <> ''
        OR COALESCE(TRIM(T2.EMP_TYPE), '') <> ''
        OR COALESCE(TRIM(T1.OCCUPATION), '') <> ''
        OR COALESCE(TRIM(T2.CCRIS_OCC_SUB), '') <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated (
    customer_id VARCHAR(20)
    , customer_employer_name VARCHAR(100)
    , customer_employer_industry VARCHAR(100)
    , customer_employer_type VARCHAR(100)
    , customer_amla_occupation VARCHAR(50)
    , customer_ccris_occupation VARCHAR(10)
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
    , customer_employer_type_of_business VARCHAR(200)
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")


spark.sql(f"""
WITH CUSTOMER_EMPLOYMENT_ROW_NUM AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
      FROM {params["cur_schema"]}.temp_dim_customer_employment_mhbos
    )
    INSERT INTO {params["cur_schema"]}.temp_dim_customer_employment_mhbos_consolidated (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM CUSTOMER_EMPLOYMENT_ROW_NUM
    WHERE
      RN = 1
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated (
    customer_id VARCHAR(20)
    , customer_employer_name VARCHAR(100)
    , customer_employer_industry VARCHAR(100)
    , customer_employer_type VARCHAR(100)
    , customer_amla_occupation VARCHAR(50)
    , customer_ccris_occupation VARCHAR(10)
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
    , customer_employer_type_of_business VARCHAR(200)
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated
SELECT
    customer_id
    , customer_employer_name
    , customer_employer_industry
    , customer_employer_type
    , customer_amla_occupation
    , customer_ccris_occupation
    , source_name
    , source_record_id
    , source_update_date
    , customer_employer_type_of_business
    , 'A' AS dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
FROM {params["cur_schema"]}.dim_customer_employment cur
WHERE 
    cur.source_name = 'MHBOS'
    AND NOT EXISTS (
    SELECT 1 FROM {params["cur_schema"]}.temp_dim_customer_employment_mhbos_consolidated r
    WHERE r.etl_dt = '{batch_date}'
      AND r.customer_id = cur.customer_id
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated
SELECT
    r.customer_id
    , r.customer_employer_name
    , r.customer_employer_industry
    , r.customer_employer_type
    , r.customer_amla_occupation
    , r.customer_ccris_occupation
    , r.source_name
    , r.source_record_id
    , r.source_update_date
    , r.customer_employer_type_of_business
    , 'A' AS dl_record_status
    , CASE
        WHEN cur.customer_id IS NOT NULL THEN cur.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
FROM {params["cur_schema"]}.temp_dim_customer_employment_mhbos_consolidated r
LEFT JOIN {params["cur_schema"]}.dim_customer_employment cur
    ON cur.source_name = 'MHBOS'
    AND r.customer_id = cur.customer_id
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_customer_employment PARTITION(source_name = 'MHBOS')
SELECT
    customer_id
    , customer_employer_name
    , customer_employer_industry
    , customer_employer_type
    , customer_amla_occupation
    , customer_ccris_occupation
    , source_record_id
    , source_update_date
    , customer_employer_type_of_business
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , '{batch_date}'       AS etl_dt
    , current_timestamp()  AS etl_timestamp
    , source_name
FROM {params["com_schema"]}.temp_dim_customer_employment_mhbos_consolidated
""")

spark.sql(f"""ANALYZE TABLE {params["cur_schema"]}.dim_customer_employment COMPUTE STATISTICS""")

spark.stop()