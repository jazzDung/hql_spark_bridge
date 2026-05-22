
"""
Purpose:    curated - Snapshot table script
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py yyyymmdd [file_name]
CreateDate: 2023-08-18 00:00:00
FileType:   DML
Logs:
Table name: DIM_CONTACT
Table comment: DIM_CONTACT
Creation date: 2023-08-18 00:00:00
Primary key field: OWNER_ID,CONTACT_OWNER_TYPE,CONTACT_TYPE
Attribution hierarchy: curated
Attribution subject: cust
Main application: None
Analyst: zhairuoping
Time granularity: None
Retention period: None
Descriptive information: None
log:  chenguanhong  20240731    add smf/sbl/lms source (uat)
log:  davidyip      20230903    add T1.CLEAN_RULE_FLAG filter for lms and sbl (uat)
log:  marcoong      20250128    add agent_assistant from MHBOS
log:  marcoong      20250326    add new source: sbl/lms/kdi (prod)
log:  marcoong      20250519    convert com_r_mhbos_m_trader, mhbos_m_trader_cmsrl, m_branch to com_t;
log:  marcoong      20250519    update filter
log:  syhmi         20250812    fix [Group.47] FROM statement to use ROW_NUMBER() to remove duplicates
log:  syhmi         20250903    added [Group.55] for EINV_EMAIL query
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp

source_name = "SBL"
table_name  = "DIM_CONTACT_SBL"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE (
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
      SEQUENCE_NO_CONSTANT INT, /* None */
      SEQUENCE_NO INT /* None */
    )
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* Insert TOMS_ECORPORATE office phone */
    WITH cte_toms_ecorp_ofcphone AS (
      SELECT
        T1.ACCOUNTNO,
        T1.OFCPHONENO AS OFCPHONENO,
        NULL AS contact_name,
        T1.SYDTC,
        T1.SYDTU,
        1 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.OFCPHONENO, '')) <> ''
        AND NOT T1.OFCPHONENO LIKE '@[%]'
      UNION ALL
      SELECT
        T1.ACCOUNTNO,
        T1.OFCPHONENO1 AS OFCPHONENO,
        T1.personname1 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        2 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.OFCPHONENO1, '')) <> ''
        AND NOT T1.OFCPHONENO1 LIKE '@[%]'
      UNION ALL
      SELECT
        T1.ACCOUNTNO,
        T1.OFCPHONENO2 AS OFCPHONENO,
        T1.personname2 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        3 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.OFCPHONENO2, '')) <> ''
        AND NOT T1.OFCPHONENO2 LIKE '@[%]'
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE (
      OWNER_ID,
      CONTACT_OWNER_TYPE,
      CONTACT_TYPE,
      CONTACT_VALUE,
      CONTACT_NAME,
      CONTACT_CREATE_DATE,
      CONTACT_UPDATE_DATE,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      SEQUENCE_NO_CONSTANT,
      SEQUENCE_NO
    )
    SELECT
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID,
      'ACCOUNT' AS CONTACT_OWNER_TYPE,
      'OFFICE' AS CONTACT_TYPE,
      T1.OFCPHONENO AS CONTACT_VALUE,
      T1.CONTACT_NAME AS CONTACT_NAME,
      T1.SYDTC AS CONTACT_CREATE_DATE,
      T1.SYDTU AS CONTACT_UPDATE_DATE,
      'UT' AS LINE_OF_BUSINESS,
      'TOMS' AS SOURCE_NAME,
      T1.ACCOUNTNO AS SOURCE_RECORD_ID,
      T1.SEQUENCE_NO_CONSTANT AS SEQUENCE_NO_CONSTANT,
      ROW_NUMBER() OVER (PARTITION BY T1.ACCOUNTNO ORDER BY T1.SEQUENCE_NO_CONSTANT ASC) AS SEQUENCE_NO
    FROM cte_toms_ecorp_ofcphone AS T1
""")
spark.sql(f"""
/* Insert TOMS_ECORPORATE mobile phone */
    WITH cte_toms_ecorp_mobilephone AS (
      SELECT
        T1.ACCOUNTNO,
        T1.mobileno1 AS MOBILENO,
        T1.personname1 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        1 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.mobileno1, '')) <> ''
        AND NOT T1.mobileno1 LIKE '@[%]'
      UNION ALL
      SELECT
        T1.ACCOUNTNO,
        T1.mobileno2 AS MOBILENO,
        T1.personname2 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        2 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.mobileno2, '')) <> ''
        AND NOT T1.mobileno2 LIKE '@[%]'
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE (
      OWNER_ID,
      CONTACT_OWNER_TYPE,
      CONTACT_TYPE,
      CONTACT_VALUE,
      CONTACT_NAME,
      CONTACT_CREATE_DATE,
      CONTACT_UPDATE_DATE,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      SEQUENCE_NO_CONSTANT,
      SEQUENCE_NO
    )
    SELECT
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID,
      'ACCOUNT' AS CONTACT_OWNER_TYPE,
      'MOBILE' AS CONTACT_TYPE,
      T1.MOBILENO AS CONTACT_VALUE,
      T1.CONTACT_NAME AS CONTACT_NAME,
      T1.SYDTC AS CONTACT_CREATE_DATE,
      T1.SYDTU AS CONTACT_UPDATE_DATE,
      'UT' AS LINE_OF_BUSINESS,
      'TOMS' AS SOURCE_NAME,
      T1.ACCOUNTNO AS SOURCE_RECORD_ID,
      T1.SEQUENCE_NO_CONSTANT AS SEQUENCE_NO_CONSTANT,
      ROW_NUMBER() OVER (PARTITION BY T1.ACCOUNTNO ORDER BY T1.SEQUENCE_NO_CONSTANT ASC) AS SEQUENCE_NO
    FROM cte_toms_ecorp_mobilephone AS T1
""")
spark.sql(f"""
/* Insert TOMS_ECORPORATE email */
    WITH cte_toms_ecorp_email AS (
      SELECT
        T1.ACCOUNTNO,
        T1.email AS email,
        NULL AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        1 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.email, '')) <> ''
        AND NOT T1.email RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        T1.ACCOUNTNO,
        T1.email1 AS email,
        T1.personname1 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        2 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.email1, '')) <> ''
        AND NOT T1.email1 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        T1.ACCOUNTNO,
        T1.email2 AS email,
        T1.personname2 AS CONTACT_NAME,
        T1.SYDTC,
        T1.SYDTU,
        3 AS SEQUENCE_NO_CONSTANT
      FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(T1.email2, '')) <> ''
        AND NOT T1.email2 RLIKE '^\\@\\[.*\\]$'
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE (
      OWNER_ID,
      CONTACT_OWNER_TYPE,
      CONTACT_TYPE,
      CONTACT_VALUE,
      CONTACT_NAME,
      CONTACT_CREATE_DATE,
      CONTACT_UPDATE_DATE,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      SEQUENCE_NO_CONSTANT,
      SEQUENCE_NO
    )
    SELECT
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID,
      'ACCOUNT' AS CONTACT_OWNER_TYPE,
      'EMAIL' AS CONTACT_TYPE,
      T1.EMAIL AS CONTACT_VALUE,
      T1.CONTACT_NAME AS CONTACT_NAME,
      T1.SYDTC AS CONTACT_CREATE_DATE,
      T1.SYDTU AS CONTACT_UPDATE_DATE,
      'UT' AS LINE_OF_BUSINESS,
      'TOMS' AS SOURCE_NAME,
      T1.ACCOUNTNO AS SOURCE_RECORD_ID,
      T1.SEQUENCE_NO_CONSTANT AS SEQUENCE_NO_CONSTANT,
      ROW_NUMBER() OVER (PARTITION BY T1.ACCOUNTNO ORDER BY T1.SEQUENCE_NO_CONSTANT ASC) AS SEQUENCE_NO
    FROM cte_toms_ecorp_email AS T1
""")
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.6]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.23 - TOMS_ECORPORATE: Officephone, mobilephone, email]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
    FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT_TOMS_ECORPORATE AS T1 /* None */
""")
spark.sql(f"""
/* ==============[Group.24 - TOMS_ECORPORATE: Fax]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'FAX' AS CONTACT_TYPE, /* None */
      T1.FAXNO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.FAXNO, '')) <> ''
      AND NOT T1.FAXNO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.25]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.OFCPHONENO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.OFCPHONENO, '')) <> ''
      AND NOT T1.OFCPHONENO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.26]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      T1.HOMEPHONE AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.HOMEPHONE, '')) <> ''
      AND NOT T1.HOMEPHONE LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.27]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'FAX' AS CONTACT_TYPE, /* None */
      T1.FAXNO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.FAXNO, '')) <> ''
      AND NOT T1.FAXNO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.28]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      T1.MOBILENO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.MOBILENO, '')) <> ''
      AND NOT T1.MOBILENO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.29]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.EMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID, /* None */
      T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        ACCOUNTNO,
        EMAIL_1 AS EMAIL,
        SYDTC,
        SYDTU,
        1 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_1, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_1 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_2 AS EMAIL,
        SYDTC,
        SYDTU,
        2 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_2, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_2 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_3 AS EMAIL,
        SYDTC,
        SYDTU,
        3 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_3, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_3 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_4 AS EMAIL,
        SYDTC,
        SYDTU,
        4 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_4, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_4 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_5 AS EMAIL,
        SYDTC,
        SYDTU,
        5 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_5, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_5 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_6 AS EMAIL,
        SYDTC,
        SYDTU,
        6 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_6, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_6 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_7 AS EMAIL,
        SYDTC,
        SYDTU,
        7 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_7, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_7 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_8 AS EMAIL,
        SYDTC,
        SYDTU,
        8 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_8, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_8 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_9 AS EMAIL,
        SYDTC,
        SYDTU,
        9 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_9, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_9 RLIKE '^\\@\\[.*\\]$'
      UNION ALL
      SELECT
        ACCOUNTNO,
        EMAIL_10 AS EMAIL,
        SYDTC,
        SYDTU,
        10 AS SEQUENCE_NO
      FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT
      WHERE
        TRIM(COALESCE(EMAIL_10, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_10 RLIKE '^\\@\\[.*\\]$'
    ) AS T1 /* None */
    WHERE
      TRIM(COALESCE(T1.EMAIL, '')) <> '' AND NOT T1.EMAIL LIKE '@[%'
""")
spark.sql(f"""
/* ==============[Group.30]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      T1.AGENTTELNO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_EAGENTDETAILS AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.AGENTTELNO, '') <> ''
      AND NOT T1.AGENTTELNO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.31]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      T1.AGENTHOMENO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_EAGENTDETAILS AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.AGENTHOMENO, '')) <> ''
""")
spark.sql(f"""
/* ==============[Group.32]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      T1.AGENTMOBILENO AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_EAGENTDETAILS AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.AGENTMOBILENO, '')) <> ''
""")
spark.sql(f"""
/* ==============[Group.33]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
      'AGENT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.AGENTEMAIL AS CONTACT_VALUE, /* None */
      NULL AS CONTACT_NAME, /* None */
      T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
      T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_TOMS_EAGENTDETAILS AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.AGENTEMAIL, '')) <> ''
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT
""")
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.MOBILE_NO, '')) <> ''
      AND NOT T1.MOBILE_NO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.2]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TEL_NO_HOME, '')) <> ''
      AND NOT T1.TEL_NO_HOME LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.3]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TEL_NO_OFFICE, '')) <> ''
      AND NOT T1.TEL_NO_OFFICE LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.4]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
      AND NOT T1.FAX_NO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.5]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EMAIL_10 RLIKE '^\\@\\[.*\\]$'
    ) AS T1 /* None */
    WHERE
      1 = 1
""")
spark.sql(f"""
/* ==============[Group.55]============== 
     ==============EINVOICE_EMAIL============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT EINVOICE_EMAIL RLIKE '^\\@\\[.*\\]$'
    ) AS T1 /* None */
    WHERE
      1 = 1
""")
spark.sql(f"""
/* ==============[Group.6]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TEL_NO_HP, '')) <> ''
      AND NOT T1.TEL_NO_HP LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.7]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.TEL_NO_OFF1, '') <> ''
      AND NOT T1.TEL_NO_OFF1 LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.7.2]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.TEL_NO_OFF2, '') <> ''
      AND NOT COALESCE(T1.SOURCE_TEL_NO_OFF2, '') LIKE 'EXT%'
      AND NOT T1.TEL_NO_OFF2 LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.8]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.EMAIL LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.9]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
      AND NOT T1.TEL_NO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.10]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
      AND NOT T1.FAX_NO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.11]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT (
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
      AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.EMAIL LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.34]============== 
     ADDED ON 2024-06-06 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
      AND NOT T1.TEL_NO LIKE '@[%]'
""")
spark.sql(f"""
/* ==============[Group.55]============== 
     ADDED ON 2025-01-28 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND LICENCE_TYPE IN ('08', '09', '10', '11')
        AND (
          TRIM(COALESCE(T1.MOBILE_NO, '')) <> '' OR TRIM(COALESCE(T1.TEL_NO, '')) <> ''
        )
    ) AS T1
    WHERE
      RN = 1
""")
spark.sql(f"""
/* ==============[Group.48]============== 
     ADDED ON 2025-01-28 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT (
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
      IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND LICENCE_TYPE IN ('08', '09', '10', '11')
        AND TRIM(COALESCE(T1.EMAIL, '')) <> ''
    ) AS T1
    WHERE
      RN = 1
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT
""")
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
    STORED AS PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.39]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'MOBILE' AS CONTACT_TYPE, /* None */
      TRIM(T1.CUSTOMER_MOBILE_PHONE) AS CONTACT_VALUE, /* None */
      UPPER(T1.CUSTOMER_NAME) AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'SBL' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.CUSTOMER_MOBILE_PHONE, '') <> ''
      AND NOT T1.CUSTOMER_MOBILE_PHONE LIKE '@[%]'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Group.41]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'HOME' AS CONTACT_TYPE, /* None */
      TRIM(T1.CUSTOMER_HOME_PHONE) AS CONTACT_VALUE, /* None */
      UPPER(T1.CUSTOMER_NAME) AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'SBL' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.CUSTOMER_HOME_PHONE, '') <> ''
      AND NOT T1.CUSTOMER_HOME_PHONE LIKE '@[%]'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Group.43]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'OFFICE' AS CONTACT_TYPE, /* None */
      TRIM(T1.CUSTOMER_OFFICE_PHONE) AS CONTACT_VALUE, /* None */
      UPPER(T1.CUSTOMER_NAME) AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'SBL' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND COALESCE(T1.CUSTOMER_OFFICE_PHONE, '') <> ''
      AND NOT T1.CUSTOMER_OFFICE_PHONE LIKE '@[%]'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Group.45]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT (
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
      'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
      'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
      'EMAIL' AS CONTACT_TYPE, /* None */
      T1.CUSTOMER_EMAIL_ADDRESS AS CONTACT_VALUE, /* None */
      UPPER(T1.CUSTOMER_NAME) AS CONTACT_NAME, /* None */
      NULL AS CONTACT_CREATE_DATE, /* None */
      NULL AS CONTACT_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'SBL' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
      1 AS SEQUENCE_NO /* None */
    FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
      AND TRIM(COALESCE(T1.CUSTOMER_EMAIL_ADDRESS, '')) <> ''
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta (
        owner_id VARCHAR(50)
        , contact_owner_type VARCHAR(20)
        , contact_type VARCHAR(10)
        , contact_value VARCHAR(150)
        , contact_name VARCHAR(100)
        , contact_create_date DATE
        , contact_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , sequence_no INT
        , dl_record_status       VARCHAR(10)
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# main_processing_sqls should include WHERE DATE_FORMAT(record_updated_date, 'yyyyMMdd') = '{batch_date}' logic
# ─── INSERT INTO CONSOLIDATED TABLE ────────────────────────────────────────────────────────

spark.sql(f"""
ALTER TABLE {params["cur_schema"]}.temp_DIM_CONTACT_main_consolidated
    DROP IF EXISTS
""")

spark.sql(f"""
/* ==============[Group.54]============== */
    INSERT INTO {params["cur_schema"]}.temp_DIM_CONTACT_main_consolidated (
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
      ETL_TIMESTAMP
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
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
    FROM (
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_CONTACT
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_TRADER_CONTACT
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_BRANCH_CONTACT
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_CONTACT
    ) AS T1 /* None */
    WHERE
      1 = 1
""")


# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_DIM_CONTACT_SBL_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_DIM_CONTACT_SBL_updated (
        owner_id VARCHAR(50)
        , contact_owner_type VARCHAR(20)
        , contact_type VARCHAR(10)
        , contact_value VARCHAR(150)
        , contact_name VARCHAR(100)
        , contact_create_date DATE
        , contact_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , sequence_no INT
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from IMPACTED PARTITIONS ONLY ───────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_DIM_CONTACT_SBL_updated
    SELECT
        cur.owner_id,
        cur.contact_owner_type,
        cur.contact_type,
        cur.contact_value,
        cur.contact_name,
        cur.contact_create_date,
        cur.contact_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.sequence_no,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
        , cur.<<partition_column>>
    FROM {params["cur_schema"]}.DIM_CONTACT_SBL cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.['source_key'] = 'SBL' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_DIM_CONTACT_SBL_updated
    SELECT
        delta.owner_id,
        delta.contact_owner_type,
        delta.contact_type,
        delta.contact_value,
        delta.contact_name,
        delta.contact_create_date,
        delta.contact_update_date,
        delta.line_of_business,
        delta.source_record_id,
        delta.sequence_no,
        COALESCE(delta.dl_record_status, 'A') AS dl_record_status,
        CASE
            WHEN cur.OWNER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE current_timestamp() END AS dl_record_created_date,
        current_timestamp() AS dl_record_updated_date
        , delta.<<partition_column>>
    FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta delta
    LEFT JOIN {params["cur_schema"]}.DIM_CONTACT_SBL cur
        ON cur.['source_key'] = 'SBL' AND delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE""")

# ─── STEP 3: Insert previous state of changed records into History table ─────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.DIM_CONTACT_SBL_h PARTITION (['source_key'], <<partition_column>>, hist_year)
    SELECT
        cur.owner_id,
        cur.contact_owner_type,
        cur.contact_type,
        cur.contact_value,
        cur.contact_name,
        cur.contact_create_date,
        cur.contact_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.sequence_no,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'SBL' AS ['source_key']        , cur.<<partition_column>>
        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.DIM_CONTACT_SBL cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.['source_key'] = 'SBL' AND         EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE        )
""")

# ─── STEP 4: Overwrite CUR table impacted partitions dynamically ─────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.DIM_CONTACT_SBL PARTITION (['source_key'] = 'SBL', <<partition_column>>)
    SELECT
        owner_id,
        contact_owner_type,
        contact_type,
        contact_value,
        contact_name,
        contact_create_date,
        contact_update_date,
        line_of_business,
        source_record_id,
        sequence_no,
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
        , <<partition_column>>
    FROM {params["com_schema"]}.temp_DIM_CONTACT_SBL_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.DIM_CONTACT_SBL COMPUTE STATISTICS
""")

spark.stop()