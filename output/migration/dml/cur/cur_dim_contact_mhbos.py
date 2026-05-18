##  File Name   : cur_dim_contact_mhbos
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-14 09:44:45
##  Source      : cur_dim_contact (migrated from Datalake Old)
##  Description : This script populates contact information from the MHBOS source.
##                It has been refactored for performance, correctness, and readability.

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp

# --- ETL JOB SETUP ---
source_name = "cur"
table_name = "dim_contact_mhbos" # Descriptive name for logging
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# --- CONFIGURATION FOR DYNAMIC PARTITION OVERWRITE ---
# This ensures that INSERT OVERWRITE only affects the target partition(s)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────

# --- 1. TEMP_DIM_ACCOUNT_CONTACT_MHBOS: Consolidate all account-level contacts ---
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS (
    OWNER_ID VARCHAR(50), CONTACT_OWNER_TYPE VARCHAR(20), CONTACT_TYPE VARCHAR(15),
    CONTACT_VALUE VARCHAR(150), CONTACT_NAME VARCHAR(100), CONTACT_CREATE_DATE DATE,
    CONTACT_UPDATE_DATE DATE, LINE_OF_BUSINESS VARCHAR(20), SOURCE_NAME VARCHAR(10),
    SOURCE_RECORD_ID VARCHAR(50), SEQUENCE_NO INT
) USING PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS
SELECT
    'MHBOS_' || CLIENT_NO AS OWNER_ID, 'ACCOUNT' AS CONTACT_OWNER_TYPE, CONTACT_TYPE,
    CONTACT_VALUE, NULL AS CONTACT_NAME, DATE_CREATED AS CONTACT_CREATE_DATE,
    DATE_CHANGE AS CONTACT_UPDATE_DATE, 'EB' AS LINE_OF_BUSINESS, 'MHBOS' AS SOURCE_NAME,
    CLIENT_NO AS SOURCE_RECORD_ID, SEQUENCE_NO
FROM (
    -- Mobile, Home, Office, Fax contacts
    SELECT CLIENT_NO, DATE_CREATED, DATE_CHANGE, 'MOBILE' AS CONTACT_TYPE, MOBILE_NO AS CONTACT_VALUE, 1 AS SEQUENCE_NO FROM {params["com_schema"]}.T_MHBOS_M_CLIENT WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(MOBILE_NO, '')) <> '' AND NOT MOBILE_NO LIKE '@[%]'
    UNION ALL
    SELECT CLIENT_NO, DATE_CREATED, DATE_CHANGE, 'HOME' AS CONTACT_TYPE, TEL_NO_HOME AS CONTACT_VALUE, 1 AS SEQUENCE_NO FROM {params["com_schema"]}.T_MHBOS_M_CLIENT WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(TEL_NO_HOME, '')) <> '' AND NOT TEL_NO_HOME LIKE '@[%]'
    UNION ALL
    SELECT CLIENT_NO, DATE_CREATED, DATE_CHANGE, 'OFFICE' AS CONTACT_TYPE, TEL_NO_OFFICE AS CONTACT_VALUE, 1 AS SEQUENCE_NO FROM {params["com_schema"]}.T_MHBOS_M_CLIENT WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(TEL_NO_OFFICE, '')) <> '' AND NOT TEL_NO_OFFICE LIKE '@[%]'
    UNION ALL
    SELECT CLIENT_NO, DATE_CREATED, DATE_CHANGE, 'FAX' AS CONTACT_TYPE, FAX_NO AS CONTACT_VALUE, 1 AS SEQUENCE_NO FROM {params["com_schema"]}.T_MHBOS_M_CLIENT WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(FAX_NO, '')) <> '' AND NOT FAX_NO LIKE '@[%]'
)
UNION ALL
-- Email contacts (Unpivoted from EMAIL_1 to EMAIL_10)
SELECT
    'MHBOS_' || CLIENT_NO, 'ACCOUNT', 'EMAIL', EMAIL, NULL, DATE_CREATED, DATE_CHANGE,
    'EB', 'MHBOS', CLIENT_NO, SEQUENCE_NO
FROM (
    SELECT CLIENT_NO, DATE_CREATED, DATE_CHANGE, stacked.EMAIL, stacked.SEQUENCE_NO
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
    LATERAL VIEW stack(10, 1, EMAIL_1, 2, EMAIL_2, 3, EMAIL_3, 4, EMAIL_4, 5, EMAIL_5, 6, EMAIL_6, 7, EMAIL_7, 8, EMAIL_8, 9, EMAIL_9, 10, EMAIL_10) stacked AS SEQUENCE_NO, EMAIL
    WHERE ETL_DT = '{batch_date}'
)
WHERE TRIM(COALESCE(EMAIL, '')) <> '' AND NOT EMAIL RLIKE '^\\\\@\\\\[.*\\\\]$'
UNION ALL
-- E-Invoice Email contact
SELECT
    'MHBOS_' || CLIENT_NO, 'ACCOUNT', 'EMAIL_EINV', EINVOICE_EMAIL, NULL, DATE_CREATED,
    DATE_CHANGE, 'EB', 'MHBOS', CLIENT_NO, 1
FROM {params["com_schema"]}.T_MHBOS_M_CLIENT
WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(EINVOICE_EMAIL, '')) <> '' AND NOT EINVOICE_EMAIL RLIKE '^\\\\@\\\\[.*\\\\]$'
""")

# --- 2. TEMP_DIM_TRADER_CONTACT_MHBOS: Consolidate all trader-level contacts ---
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS (
    OWNER_ID VARCHAR(50), CONTACT_OWNER_TYPE VARCHAR(20), CONTACT_TYPE VARCHAR(15),
    CONTACT_VALUE VARCHAR(150), CONTACT_NAME VARCHAR(100), CONTACT_CREATE_DATE DATE,
    CONTACT_UPDATE_DATE DATE, LINE_OF_BUSINESS VARCHAR(20), SOURCE_NAME VARCHAR(10),
    SOURCE_RECORD_ID VARCHAR(50), SEQUENCE_NO INT
) USING PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS
-- AGENT contacts
SELECT
    'MHBOS_' || TDR_CODE AS OWNER_ID, 'AGENT' AS CONTACT_OWNER_TYPE, CONTACT_TYPE,
    CONTACT_VALUE, NULL AS CONTACT_NAME, DATE_CREATED AS CONTACT_CREATE_DATE,
    DATE_CHANGE AS CONTACT_UPDATE_DATE, 'EB' AS LINE_OF_BUSINESS, 'MHBOS' AS SOURCE_NAME,
    TDR_CODE AS SOURCE_RECORD_ID, SEQUENCE_NO
FROM (
    SELECT TDR_CODE, DATE_CREATED, DATE_CHANGE, 'MOBILE' AS CONTACT_TYPE, TEL_NO_HP AS CONTACT_VALUE, 1 AS SEQUENCE_NO FROM {params["com_schema"]}.T_MHBOS_M_TRADER WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(TEL_NO_HP, '')) <> '' AND NOT TEL_NO_HP LIKE '@[%]'
    UNION ALL
    SELECT TDR_CODE, DATE_CREATED, DATE_CHANGE, 'OFFICE' AS CONTACT_TYPE, CASE WHEN SOURCE_TEL_NO_OFF2 LIKE 'EXT%' THEN TEL_NO_OFF1 || TEL_NO_OFF2 ELSE TEL_NO_OFF1 END, 1 FROM {params["com_schema"]}.T_MHBOS_M_TRADER WHERE ETL_DT = '{batch_date}' AND COALESCE(TEL_NO_OFF1, '') <> '' AND NOT TEL_NO_OFF1 LIKE '@[%]'
    UNION ALL
    SELECT TDR_CODE, DATE_CREATED, DATE_CHANGE, 'OFFICE' AS CONTACT_TYPE, TEL_NO_OFF2, 2 FROM {params["com_schema"]}.T_MHBOS_M_TRADER WHERE ETL_DT = '{batch_date}' AND COALESCE(TEL_NO_OFF2, '') <> '' AND NOT COALESCE(SOURCE_TEL_NO_OFF2, '') LIKE 'EXT%' AND NOT TEL_NO_OFF2 LIKE '@[%]'
    UNION ALL
    SELECT TDR_CODE, DATE_CREATED, DATE_CHANGE, 'EMAIL' AS CONTACT_TYPE, EMAIL, 1 FROM {params["com_schema"]}.T_MHBOS_M_TRADER WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(EMAIL, '')) <> '' AND NOT EMAIL LIKE '@[%]'
    UNION ALL
    SELECT TDR_CODE, DATE_CREATED, DATE_CHANGE, 'HOME' AS CONTACT_TYPE, TEL_NO, 1 FROM {params["com_schema"]}.T_MHBOS_M_TRADER WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(TEL_NO, '')) <> '' AND NOT TEL_NO LIKE '@[%]'
)
UNION ALL
-- AGENT_ASSISTANT contacts
SELECT
    'MHBOS_' || IC_NO_NEW, 'AGENT_ASSISTANT', CONTACT_TYPE, CONTACT_VALUE, TDR_NAME,
    NULL, NULL, 'EB', 'MHBOS', IC_NO_NEW, 1
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW, CONTACT_TYPE ORDER BY DATE_CREATED DESC) AS RN
    FROM (
        SELECT IC_NO_NEW, TDR_NAME, DATE_CREATED, 'MOBILE' AS CONTACT_TYPE, COALESCE(NULLIF(MOBILE_NO, ''), NULLIF(TEL_NO, '')) AS CONTACT_VALUE FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL WHERE ETL_DT = '{batch_date}' AND LICENCE_TYPE IN ('08', '09', '10', '11') AND (TRIM(COALESCE(MOBILE_NO, '')) <> '' OR TRIM(COALESCE(TEL_NO, '')) <> '')
        UNION ALL
        SELECT IC_NO_NEW, TDR_NAME, DATE_CREATED, 'EMAIL' AS CONTACT_TYPE, EMAIL FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL WHERE ETL_DT = '{batch_date}' AND LICENCE_TYPE IN ('08', '09', '10', '11') AND TRIM(COALESCE(EMAIL, '')) <> ''
    )
)
WHERE RN = 1
""")

# --- 3. TEMP_DIM_BRANCH_CONTACT_MHBOS: Consolidate all branch-level contacts ---
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS (
    OWNER_ID VARCHAR(50), CONTACT_OWNER_TYPE VARCHAR(20), CONTACT_TYPE VARCHAR(15),
    CONTACT_VALUE VARCHAR(150), CONTACT_NAME VARCHAR(100), CONTACT_CREATE_DATE DATE,
    CONTACT_UPDATE_DATE DATE, LINE_OF_BUSINESS VARCHAR(20), SOURCE_NAME VARCHAR(10),
    SOURCE_RECORD_ID VARCHAR(50), SEQUENCE_NO INT
) USING PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS
SELECT
    'MHBOS_' || BRANCH_ID, 'BRANCH', CONTACT_TYPE, CONTACT_VALUE, NULL, NULL, NULL,
    'EB', 'MHBOS', BRANCH_ID, 1
FROM (
    SELECT BRANCH_ID, 'OFFICE' AS CONTACT_TYPE, TEL_NO AS CONTACT_VALUE FROM {params["com_schema"]}.T_MHBOS_M_BRANCH WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(TEL_NO, '')) <> '' AND NOT TEL_NO LIKE '@[%]'
    UNION ALL
    SELECT BRANCH_ID, 'FAX' AS CONTACT_TYPE, FAX_NO FROM {params["com_schema"]}.T_MHBOS_M_BRANCH WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(FAX_NO, '')) <> '' AND NOT FAX_NO LIKE '@[%]'
    UNION ALL
    SELECT BRANCH_ID, 'EMAIL' AS CONTACT_TYPE, EMAIL FROM {params["com_schema"]}.T_MHBOS_M_BRANCH WHERE ETL_DT = '{batch_date}' AND TRIM(COALESCE(EMAIL, '')) <> '' AND NOT EMAIL LIKE '@[%]'
)
""")

# --- 4. TEMP_DIM_CUSTOMER_CONTACT_MHBOS: Derive customer contacts from account contacts ---
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS (
    OWNER_ID VARCHAR(50), CONTACT_OWNER_TYPE VARCHAR(20), CONTACT_TYPE VARCHAR(15),
    CONTACT_VALUE VARCHAR(150), CONTACT_NAME VARCHAR(100), CONTACT_CREATE_DATE DATE,
    CONTACT_UPDATE_DATE DATE, LINE_OF_BUSINESS VARCHAR(20), SOURCE_NAME VARCHAR(10),
    SOURCE_RECORD_ID VARCHAR(50), SEQUENCE_NO INT
) USING PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS
SELECT
    CUSTOMER_ID, 'CUSTOMER', CONTACT_TYPE, CONTACT_VALUE, CONTACT_NAME,
    CONTACT_CREATE_DATE, CONTACT_UPDATE_DATE, NULL, SOURCE_NAME, SOURCE_RECORD_ID, 1
FROM (
    SELECT
        DC.CUSTOMER_ID, DAC.CONTACT_TYPE, DAC.CONTACT_VALUE, DAC.CONTACT_NAME,
        DAC.CONTACT_CREATE_DATE, DAC.CONTACT_UPDATE_DATE, DAC.SOURCE_NAME, DAC.SOURCE_RECORD_ID,
        ROW_NUMBER() OVER (
            PARTITION BY DC.CUSTOMER_ID, DAC.CONTACT_TYPE
            ORDER BY COALESCE(DAC.CONTACT_UPDATE_DATE, DAC.CONTACT_CREATE_DATE) DESC, DAC.SEQUENCE_NO ASC
        ) AS ROWNUM
    FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
    INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
        ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
        AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
        AND ACC.ETL_DT = '{batch_date}'
    INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS AS DAC
        ON DAC.OWNER_ID = ACC.ACCOUNT_ID AND DAC.CONTACT_OWNER_TYPE = 'ACCOUNT'
    WHERE DC.ETL_DT = '{batch_date}'
)
WHERE ROWNUM = 1
""")

# --- 5. CONSOLIDATE ALL CONTACTS ---
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated AS
SELECT * FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS
UNION ALL
SELECT * FROM {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS
UNION ALL
SELECT * FROM {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS
UNION ALL
SELECT * FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS
""")

# --- 6. FINAL LOAD: DYNAMICALLY OVERWRITE MHBOS PARTITION ---
spark.sql(f"""
INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_contact PARTITION(source_name)
SELECT
    s.owner_id,
    s.contact_owner_type,
    s.contact_type,
    s.contact_value,
    s.contact_name,
    s.contact_create_date,
    s.contact_update_date,
    s.line_of_business,
    s.source_record_id,
    s.sequence_no,
    'A' AS dl_record_status,
    COALESCE(t.dl_record_created_date, current_timestamp()) AS dl_record_created_date,
    current_timestamp() AS dl_record_updated_date,
    '{batch_date}' AS etl_dt,
    current_timestamp() AS etl_timestamp,
    s.source_name
FROM {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated s
LEFT JOIN {params["cur_schema"]}.dim_contact t
    ON s.owner_id = t.owner_id
    AND s.contact_owner_type = t.contact_owner_type
    AND s.contact_type = t.contact_type
    AND s.sequence_no = t.sequence_no
    AND t.source_name = 'MHBOS'
""")

# --- 7. CLEANUP AND ANALYSIS ---
spark.sql(f"""ANALYZE TABLE {params["cur_schema"]}.dim_contact COMPUTE STATISTICS FOR COLUMNS owner_id, contact_owner_type, contact_type, source_name""")

spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_MHBOS""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT_MHBOS""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT_MHBOS""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT_MHBOS""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_contact_mhbos_consolidated""")

spark.stop()
