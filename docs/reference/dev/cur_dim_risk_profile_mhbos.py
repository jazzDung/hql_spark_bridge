##  File Name   : com_None_dim_risk
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-15 08:21:55
##  Source      : cur_dim_risk_profile (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "cur"
table_name  = "dim_risk_profile_mhbos"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_risk_profile_mhbos
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_risk_profile_mhbos (
      CUSTOMER_ID VARCHAR(20) COMMENT '',
      AMLA_RISK_PROFILE_DATE DATE COMMENT '',
      AMLATF_RISK VARCHAR(1) COMMENT '',
      ESTIMATED_NETWORTH VARCHAR(100) COMMENT '',
      ANNUAL_INCOME INT COMMENT '',
      SOURCE_NAME VARCHAR(10) COMMENT '',
      SOURCE_RECORD_ID VARCHAR(20) COMMENT '',
      ETL_TIMESTAMP STRING COMMENT 'ETL_PROCESSING_TIME',
      SOURCE_UPDATE_DATE TIMESTAMP COMMENT ''
    )
""")

spark.sql(f"""
/* ==============[Group.1]============== */
    INSERT INTO {params["cur_schema"]}.temp_dim_risk_profile_mhbos (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP) AS AMLA_RISK_PROFILE_DATE, /* None */
      T1.RISK AS AMLATF_RISK, /* None */
      T2.ESTIMATED_NETWORTH AS ESTIMATED_NETWORTH, /* None */
      CASE
        WHEN COALESCE(T1.MARGIN, '') = 'M'
        THEN CAST(T2.ANNUAL_INCOME AS INT)
        WHEN T2.MONTHLY_INCOME = '1'
        THEN 12000
        WHEN T2.MONTHLY_INCOME = '2'
        THEN 30000
        WHEN T2.MONTHLY_INCOME = '3'
        THEN 60000
        WHEN T2.MONTHLY_INCOME = '4'
        THEN 90000
        WHEN T2.MONTHLY_INCOME = '5'
        THEN 120000
        WHEN T2.MONTHLY_INCOME = '6'
        THEN 240000
        WHEN T2.MONTHLY_INCOME = '7'
        THEN 252000
        ELSE NULL
      END AS ANNUAL_INCOME, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(COALESCE(T1.DATE_CHANGE, T1.DATE_CREATED) AS TIMESTAMP) AS SOURCE_UPDATE_DATE /* 20251028 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND TO_DATE(T2.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3
      ON T2.ESTIMATED_NETWORTH = T3.REFERENCE_CODE
      AND T3.SOURCE_NAME = 'AML'
      AND T3.REFERENCE_TYPE = 'ESTIMATEDNETWORTH'
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND (
        TRIM(COALESCE(T1.RISK, '')) <> ''
        OR TRIM(COALESCE(T2.ESTIMATED_NETWORTH, '')) <> ''
        OR NOT T2.ANNUAL_INCOME IS NULL
        OR COALESCE(T2.MONTHLY_INCOME, '') <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_risk_profile_mhbos_consolidated""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_risk_profile_mhbos_consolidated (
    customer_id VARCHAR(20)
    , amla_risk_profile_date DATE
    , amlatf_risk VARCHAR(1)
    , estimated_networth VARCHAR(100)
    , annual_income INT
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")


spark.sql(f"""
/* Insert into curated table */
    WITH RISK_PROFILE_ROW_NUM AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
      FROM {params["cur_schema"]}.temp_dim_risk_profile_mhbos
    )
    INSERT INTO {params["cur_schema"]}.temp_dim_risk_profile_mhbos_consolidated (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      CUSTOMER_ID,
      AMLA_RISK_PROFILE_DATE,
      AMLATF_RISK,
      ESTIMATED_NETWORTH,
      ANNUAL_INCOME,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      SOURCE_UPDATE_DATE
    FROM RISK_PROFILE_ROW_NUM
    WHERE
      RN = 1
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_risk_profile_mhbos_updated""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_risk_profile_mhbos_updated (
    customer_id VARCHAR(20)
    , amla_risk_profile_date DATE
    , amlatf_risk VARCHAR(1)
    , estimated_networth VARCHAR(100)
    , annual_income INT
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["cur_schema"]}.temp_dim_risk_profile_mhbos_updated
SELECT
    customer_id
    , amla_risk_profile_date
    , amlatf_risk
    , estimated_networth
    , annual_income
    , source_name
    , source_record_id
    , source_update_date
    , 'A' AS dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
FROM {params["cur_schema"]}.dim_risk_profile cur
WHERE 
    cur.source_name = 'MHBOS'
    AND NOT EXISTS (
    SELECT 1 FROM {params["cur_schema"]}.temp_dim_risk_profile_mhbos_consolidated r
    WHERE r.customer_id = cur.customer_id
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["cur_schema"]}.temp_dim_risk_profile_mhbos_updated
SELECT
    r.customer_id
    , r.amla_risk_profile_date
    , r.amlatf_risk
    , r.estimated_networth
    , r.annual_income
    , r.source_record_id
    , r.source_update_date
    , 'A' AS dl_record_status
    , CASE
        WHEN cur.customer_id IS NOT NULL THEN cur.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
    , r.source_name
FROM {params["cur_schema"]}.temp_dim_risk_profile_mhbos_consolidated r
LEFT JOIN {params["cur_schema"]}.dim_risk_profile cur
    ON cur.source_name = 'MHBOS'
    AND r.customer_id = cur.customer_id
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_risk_profile PARTITION(source_name = 'MHBOS')
SELECT
    customer_id
    , amla_risk_profile_date
    , amlatf_risk
    , estimated_networth
    , annual_income
    , source_record_id
    , source_update_date
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , '{batch_date}'       AS etl_dt
    , current_timestamp()  AS etl_timestamp
FROM {params["cur_schema"]}.temp_dim_risk_profile_mhbos_updated
""")

spark.sql(f"""ANALYZE TABLE {params["cur_schema"]}.dim_risk_profile COMPUTE STATISTICS""")

spark.stop()