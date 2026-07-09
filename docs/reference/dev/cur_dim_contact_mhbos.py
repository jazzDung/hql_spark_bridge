##  File Name   : com_None_dim_contact
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-14 09:44:45
##  Source      : cur_dim_contact (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter, run_etl_cur
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "cur"
table_name  = "dim_contact"

spark, today_date = run_etl_cur(source_name, table_name)
batch_date = today_date
# batch_date = '20260513'
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────

spark.sql(f"""
-- ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID VARCHAR(50), /* None */
      CONTACT_OWNER_TYPE VARCHAR(20), /* None */
      CONTACT_TYPE VARCHAR(15), /* None */
      CONTACT_VALUE VARCHAR(150), /* None */
      CONTACT_NAME VARCHAR(100), /* None */
      CONTACT_CREATE_DATE DATE, /* None */
      CONTACT_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50), /* None */
      SEQUENCE_NO INT /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      T1.MOBILE_NO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.MOBILE_NO, '')) <> ''
      AND NOT T1.MOBILE_NO LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.2]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      T1.TEL_NO_HOME AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.TEL_NO_HOME, '')) <> ''
      AND NOT T1.TEL_NO_HOME LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.3]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.TEL_NO_OFFICE AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.TEL_NO_OFFICE, '')) <> ''
      AND NOT T1.TEL_NO_OFFICE LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.4]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'FAX' AS CONTACT_TYPE, /* None */
      T1.FAX_NO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
      AND NOT T1.FAX_NO LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.5]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        CLIENT_NO,
        EMAIL_1 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        1 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_1, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_1 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_2 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        2 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_2, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_2 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_3 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        3 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_3, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_3 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_4 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        4 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_4, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_4 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_5 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        5 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_5, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_5 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_6 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        6 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_6, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_6 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_7 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        7 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_7, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_7 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_8 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        8 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_8, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_8 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_9 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        9 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_9, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_9 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        CLIENT_NO,
        EMAIL_10 AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        10 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EMAIL_10, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EMAIL_10 RLIKE '^\\@\\[.*\\]$'
    ) AS T1 /* None */
    WHERE
      1 = 1
""")

spark.sql(f"""
-- ==============EINVOICE_EMAIL============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL_EINV' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        CLIENT_NO,
        EINVOICE_EMAIL AS EMAIL,
        DATE_CREATED,
        DATE_CHANGE,
        1 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
      WHERE
        TRIM(COALESCE(EINVOICE_EMAIL, '')) <> ''
        AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND NOT EINVOICE_EMAIL RLIKE '^\\@\\[.*\\]$'
    ) AS T1 /* None */
    WHERE
      1 = 1
""")



spark.sql(f"""
-- ==============[Group.6]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID VARCHAR(50), /* None */
      CONTACT_OWNER_TYPE VARCHAR(20), /* None */
      CONTACT_TYPE VARCHAR(15), /* None */
      CONTACT_VALUE VARCHAR(150), /* None */
      CONTACT_NAME VARCHAR(100), /* None */
      CONTACT_CREATE_DATE DATE, /* None */
      CONTACT_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50), /* None */
      SEQUENCE_NO INT /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      T1.TEL_NO_HP AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.TEL_NO_HP, '')) <> ''
      AND NOT T1.TEL_NO_HP LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.7]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      (
        CASE
          WHEN T1.SOURCE_TEL_NO_OFF2 LIKE 'EXT%'
          THEN T1.TEL_NO_OFF1 || T1.TEL_NO_OFF2
          ELSE T1.TEL_NO_OFF1
        END
      ) AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND COALESCE(T1.TEL_NO_OFF1, '') <> ''
      AND NOT T1.TEL_NO_OFF1 LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.7.2]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.TEL_NO_OFF2 AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
      2 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND COALESCE(T1.TEL_NO_OFF2, '') <> ''
      AND NOT COALESCE(T1.SOURCE_TEL_NO_OFF2, '') LIKE 'EXT%'
      AND NOT T1.TEL_NO_OFF2 LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.8]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1
    WHERE
      TRIM(COALESCE(T1.EMAIL, '')) <> ''
      AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND NOT T1.EMAIL LIKE '@[%]'
""")




spark.sql(f"""
-- ==============[Group.34]==============
-- ADDED ON 2024-06-06 
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      T1.TEL_NO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
      AND NOT T1.TEL_NO LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.55]============== */ /* ADDED ON 2025-01-28 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
      'AGENT_ASSISTANT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      COALESCE(NULLIF(T1.MOBILE_NO, ''), NULLIF(T1.TEL_NO, '')) AS CONTACT_VALUE, /* None */
      T1.TDR_NAME AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
      WHERE
        TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND LICENCE_TYPE IN ('08', '09', '10', '11')
        AND (
          TRIM(COALESCE(T1.MOBILE_NO, '')) <> '' OR TRIM(COALESCE(T1.TEL_NO, '')) <> ''
        )
    ) AS T1
    WHERE
      RN = 1
""")

spark.sql(f"""
-- ==============[Group.48]============== */ /* ADDED ON 2025-01-28 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
      'AGENT_ASSISTANT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      T1.TDR_NAME AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
      WHERE
        TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
        AND LICENCE_TYPE IN ('08', '09', '10', '11')
        AND TRIM(COALESCE(T1.EMAIL, '')) <> ''
    ) AS T1
    WHERE
      RN = 1
""")

spark.sql(f"""
-- ==============[Group.9]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS (
      OWNER_ID VARCHAR(50), /* None */
      CONTACT_OWNER_TYPE VARCHAR(20), /* None */
      CONTACT_TYPE VARCHAR(15), /* None */
      CONTACT_VALUE VARCHAR(150), /* None */
      CONTACT_NAME VARCHAR(100), /* None */
      CONTACT_CREATE_DATE DATE, /* None */
      CONTACT_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50), /* None */
      SEQUENCE_NO INT /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
      'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.TEL_NO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_BRANCH AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
      AND NOT T1.TEL_NO LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.10]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
      'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
      'FAX' AS CONTACT_TYPE, /* None */
      T1.FAX_NO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_BRANCH AS T1 /* None */
    WHERE
      TO_DATE(T1.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
      AND NOT T1.FAX_NO LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.11]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
      'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_BRANCH AS T1
    WHERE
      TRIM(COALESCE(T1.EMAIL, '')) <> ''
      AND TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      AND NOT T1.EMAIL LIKE '@[%]'
""")

spark.sql(f"""
-- ==============[Group.49]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID VARCHAR(50), /* None */
      CONTACT_OWNER_TYPE VARCHAR(20), /* None */
      CONTACT_TYPE VARCHAR(15), /* None */
      CONTACT_VALUE VARCHAR(150), /* None */
      CONTACT_NAME VARCHAR(100), /* None */
      CONTACT_CREATE_DATE DATE, /* None */
      CONTACT_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50), /* None */
      SEQUENCE_NO INT /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      T1.CUSTOMER_ID AS OWNER_ID, /* None */
      'CUSTOMER' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      NULL AS LINE_OF_BUSINESS, /* None */
      NULL AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        DC.CUSTOMER_ID,
        DAC.*,
        ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC) AS ROWNUM
      FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
      INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND TO_DATE(ACC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID
        AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
        AND DAC.CONTACT_TYPE = 'MOBILE'
        AND DAC.SEQUENCE_NO = 1
      WHERE
        TO_DATE(DC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    ) AS T1 /* None */
    WHERE
      T1.ROWNUM = 1
""")

spark.sql(f"""
-- ==============[Group.50]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      T1.CUSTOMER_ID AS OWNER_ID, /* None */
      'CUSTOMER' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      NULL AS LINE_OF_BUSINESS, /* None */
      NULL AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        DC.CUSTOMER_ID,
        DAC.*,
        ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC) AS ROWNUM
      FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
      INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND TO_DATE(ACC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID
        AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
        AND DAC.CONTACT_TYPE = 'HOME'
        AND DAC.SEQUENCE_NO = 1
      WHERE
        TO_DATE(DC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    ) AS T1 /* None */
    WHERE
      T1.ROWNUM = 1
""")

spark.sql(f"""
-- ==============[Group.51]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      T1.CUSTOMER_ID AS OWNER_ID, /* None */
      'CUSTOMER' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      NULL AS LINE_OF_BUSINESS, /* None */
      NULL AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        DC.CUSTOMER_ID,
        DAC.*,
        ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC) AS ROWNUM
      FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
      INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND TO_DATE(ACC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID
        AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
        AND DAC.CONTACT_TYPE = 'OFFICE'
        AND DAC.SEQUENCE_NO = 1
      WHERE
        TO_DATE(DC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    ) AS T1 /* None */
    WHERE
      T1.ROWNUM = 1
""")

spark.sql(f"""
-- ==============[Group.52]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      T1.CUSTOMER_ID AS OWNER_ID, /* None */
      'CUSTOMER' AS CONTACT_OWNER_TYPE, /* None */
      'FAX' AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      NULL AS LINE_OF_BUSINESS, /* None */
      NULL AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        DC.CUSTOMER_ID,
        DAC.*,
        ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC) AS ROWNUM
      FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
      INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND TO_DATE(ACC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID
        AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
        AND DAC.CONTACT_TYPE = 'FAX'
        AND DAC.SEQUENCE_NO = 1
      WHERE
        TO_DATE(DC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    ) AS T1 /* None */
    WHERE
      T1.ROWNUM = 1
""")

spark.sql(f"""
-- ==============[Group.53]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO /* None */
    )
    SELECT
      T1.CUSTOMER_ID AS OWNER_ID, /* None */
      'CUSTOMER' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      NULL AS LINE_OF_BUSINESS, /* None */
      NULL AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        DC.CUSTOMER_ID,
        DAC.*,
        ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC) AS ROWNUM
      FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
      INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND TO_DATE(ACC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
      INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID
        AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
        AND DAC.CONTACT_TYPE IN ('EMAIL', 'EMAIL_EINV')
        AND DAC.SEQUENCE_NO = 1
      WHERE
        TO_DATE(DC.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    ) AS T1 /* None */
    WHERE
      T1.ROWNUM = 1
""")


spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated;""")


spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated (
      OWNER_ID VARCHAR(50), /* None */
      CONTACT_OWNER_TYPE VARCHAR(20), /* None */
      CONTACT_TYPE VARCHAR(15), /* None */
      CONTACT_VALUE VARCHAR(150), /* None */
      CONTACT_NAME VARCHAR(100), /* None */
      CONTACT_CREATE_DATE DATE, /* None */
      CONTACT_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50), /* None */
      SEQUENCE_NO INT,
      SOURCE_KEY VARCHAR(50)
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")



spark.sql(f"""
-- ==============[Group.54]============== */
    INSERT INTO {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated (
      OWNER_ID, /* None */
      CONTACT_OWNER_TYPE, /* None */
      CONTACT_TYPE, /* None */
      CONTACT_VALUE, /* None */
      CONTACT_NAME, /* None */
      CONTACT_CREATE_DATE, /* None */
      CONTACT_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      SEQUENCE_NO, /* None */
      SOURCE_KEY
    )
    SELECT
      T1.OWNER_ID AS OWNER_ID, /* None */
      T1.CONTACT_OWNER_TYPE AS CONTACT_OWNER_TYPE, /* None */
      T1.CONTACT_TYPE AS CONTACT_TYPE, /* None */
      T1.CONTACT_VALUE AS CONTACT_VALUE, /* None */
      T1.CONTACT_NAME AS CONTACT_NAME, /* None */
      T1.CONTACT_CREATE_DATE AS CONTACT_CREATE_DATE, /* None */
      T1.CONTACT_UPDATE_DATE AS CONTACT_UPDATE_DATE, /* None */
      T1.LINE_OF_BUSINESS AS LINE_OF_BUSINESS, /* None */
      T1.SOURCE_NAME AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      T1.SEQUENCE_NO AS SEQUENCE_NO, /* None */
      T1.source_key
    FROM (
      SELECT
        *
        , SOURCE_NAME || '_' || CONTACT_OWNER_TYPE AS source_key
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS
      UNION ALL
      SELECT
        *
        , SOURCE_NAME || '_' || CONTACT_OWNER_TYPE AS source_key
      FROM {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS
      UNION ALL
      SELECT
        *
        , SOURCE_NAME || '_' || CONTACT_OWNER_TYPE AS source_key
      FROM {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS
      UNION ALL
      SELECT
        *
        , CONTACT_OWNER_TYPE AS source_key
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS
    ) AS T1 /* None */
    WHERE
      1 = 1
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_contact_mhbos_updated""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_contact_mhbos_updated (
    owner_id VARCHAR(50)
    , contact_owner_type VARCHAR(20)
    , contact_type VARCHAR(10)
    , contact_value VARCHAR(150)
    , contact_name VARCHAR(100)
    , contact_create_date DATE
    , contact_update_date DATE
    , line_of_business VARCHAR(20)
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(50)
    , sequence_no INT
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
    , SOURCE_KEY VARCHAR(50)
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["cur_schema"]}.temp_dim_contact_mhbos_updated
SELECT
    cur.owner_id
    , cur.contact_owner_type
    , cur.contact_type
    , cur.contact_value
    , cur.contact_name
    , cur.contact_create_date
    , cur.contact_update_date
    , cur.line_of_business
    , cur.source_name
    , cur.source_record_id
    , cur.sequence_no
    , 'A' AS dl_record_status
    , cur.dl_record_created_date
    , cur.dl_record_updated_date
    , cur.source_key
FROM {params["cur_schema"]}.dim_contact cur
WHERE 
    cur.source_key in ('CUSTOMER', 'MHBOS_ACCOUNT', 'MHBOS_AGENT', 'MHBOS_AGENT_ASSISTANT', 'MHBOS_BRANCH')
    AND NOT EXISTS (
    SELECT 1 FROM {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated r
    WHERE r.owner_id = cur.owner_id
      AND r.contact_owner_type = cur.contact_owner_type
      and r.source_key = cur.source_key
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["cur_schema"]}.temp_dim_contact_mhbos_updated
SELECT
    r.owner_id
    , r.contact_owner_type
    , r.contact_type
    , r.contact_value
    , r.contact_name
    , r.contact_create_date
    , r.contact_update_date
    , r.line_of_business
    , r.source_name
    , r.source_record_id
    , r.sequence_no
    , 'A' AS dl_record_status
    , CASE
        WHEN cur.owner_id IS NOT NULL THEN cur.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
    , r.source_key
FROM {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated r
LEFT JOIN {params["cur_schema"]}.dim_contact cur
    ON cur.source_key in ('CUSTOMER', 'MHBOS_ACCOUNT', 'MHBOS_AGENT', 'MHBOS_AGENT_ASSISTANT', 'MHBOS_BRANCH')
    AND r.owner_id = cur.owner_id
    AND r.contact_owner_type = cur.contact_owner_type
    and r.contact_type = cur.contact_type
    and r.source_key = cur.source_key
    and r.sequence_no = cur.sequence_no
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_contact PARTITION (source_key) 
SELECT
    owner_id
    , contact_owner_type
    , contact_type
    , contact_value
    , contact_name
    , contact_create_date
    , contact_update_date
    , line_of_business
    , source_name
    , source_record_id
    , sequence_no
    , current_timestamp()  AS etl_timestamp
    , '{batch_date}'       AS etl_dt
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , source_key
FROM {params["cur_schema"]}.temp_dim_contact_mhbos_updated
""")

spark.sql(f"""ANALYZE TABLE {params["cur_schema"]}.dim_contact COMPUTE STATISTICS""")

spark.stop()