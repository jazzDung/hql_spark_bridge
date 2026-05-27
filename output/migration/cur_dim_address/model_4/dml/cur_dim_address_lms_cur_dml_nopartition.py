
"""
Purpose:    curated - Snapshot table script
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py yyyymmdd [file_name]
CreateDate: 2023-08-18 00:00:00
FileType:   DML
Logs:
Table name: DIM_ADDRESS
Table comment: DIM_ADDRESS
Creation date: 2023-08-18 00:00:00
Primary key field: OWNER_ID,ADDRESS_OWNER_TYPE,ADDRESS_TYPE
Attribution hierarchy: curated
Attribution subject: cust
Main application: None
Analyst: zhairuoping
Time granularity: None
Retention period: None
Descriptive information: None
version2
chenguanhong  20240801    add sbl/lms source (UAT)
Sharon        20250123    Updated REF_LOOKUP joining to include field SOURCE_KEY and remove ETL_DT filter (UAT)
marcoong      20250128    add agent assistant from MHBOS
marcoong      20250326    add new source kdi/sbl/lms
marcoong      20250423    Updated REF_LOOKUP joining to include field SOURCE_KEY and remove ETL_DT filter
marcoong      20250519    update table com_r_mhbos_trader, com_r_mhbos_m_trader_cmsrl, com_r_mhbos_m_branch, com_r_m21_ACCOUNTEXECUTIVE to com_t
marcoong      20250715    added new source field perm_country for MHBOS
marcoong      20251015    update state definition logic for MHBOS's Account mailing address (Group 1)
Afiq Azizi    20260223    change the source for KDI from KDI Customer to KDI Clientreport
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp

source_name = "LMS"
table_name  = "dim_address"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites for history table
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Drop temp table */
    DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_address_lms_tbl_einvoicing_clientdata
""")
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS (
      OWNER_ID VARCHAR(50), /* None */
      ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
      ADDRESS_TYPE VARCHAR(10), /* None */
      ADDRESS_LINE_1 VARCHAR(100), /* None */
      ADDRESS_LINE_2 VARCHAR(100), /* None */
      ADDRESS_LINE_3 VARCHAR(100), /* None */
      ADDRESS_LINE_4 VARCHAR(100), /* None */
      CITY VARCHAR(255), /* None */
      STATE VARCHAR(50), /* None */
      POSTCODE VARCHAR(5), /* None */
      COUNTRY VARCHAR(3), /* None */
      ADDRESS_CREATE_DATE DATE, /* None */
      ADDRESS_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50) /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.18]============== */
    CREATE TABLE IF NOT EXISTS {params["cur_schema"]}.temp_dim_address_lms_tbl_einvoicing_clientdata (
      owner_id VARCHAR(50) COMMENT '',
      address_owner_type VARCHAR(20) COMMENT '',
      address_type VARCHAR(10) COMMENT '',
      address_line_1 VARCHAR(100) COMMENT '',
      address_line_2 VARCHAR(100) COMMENT '',
      address_line_3 VARCHAR(100) COMMENT '',
      address_line_4 VARCHAR(100) COMMENT '',
      city VARCHAR(255) COMMENT '',
      state VARCHAR(50) COMMENT '',
      postcode VARCHAR(5) COMMENT '',
      country VARCHAR(3) COMMENT '',
      address_create_date DATE COMMENT '',
      address_update_date DATE COMMENT '',
      line_of_business VARCHAR(20) COMMENT '',
      source_name VARCHAR(10) COMMENT '',
      source_record_id VARCHAR(50) COMMENT '',
      etl_timestamp STRING COMMENT 'ETL_processing_time'
    )
""")
spark.sql(f"""
/* ==============[Group.20]============== */
    INSERT INTO {params["cur_schema"]}.temp_dim_address_lms_tbl_einvoicing_clientdata (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      'LMS_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
      'REGISTERED' AS ADDRESS_TYPE, /* NONE */
      UPPER(TRIM(T1.CUSTOMER_ADDRESS_LINE_1)) AS ADDRESS_LINE_1, /* NONE */
      UPPER(TRIM(T1.CUSTOMER_ADDRESS_LINE_2)) AS ADDRESS_LINE_2, /* NONE */
      UPPER(TRIM(T1.CUSTOMER_ADDRESS_LINE_3)) AS ADDRESS_LINE_3, /* NONE */
      NULL AS ADDRESS_LINE_4, /* NONE */
      UPPER(TRIM(T1.CUSTOMER_CITY)) AS CITY, /* NONE */
      UPPER(TRIM(T2.REFERENCE_VALUE_2)) AS STATE, /* NONE */
      UPPER(TRIM(T1.CUSTOMER_POSTAL_CODE)) AS POSTCODE, /* NONE */
      UPPER(TRIM(T3.COUNTRY_CODE_3_DIGITS)) AS COUNTRY, /* NONE */
      COALESCE(T4.ADDRESS_CREATE_DATE, CURRENT_TIMESTAMP()) AS ADDRESS_CREATE_DATE, /* NONE */
      COALESCE(T1.LAST_GENERATED_DATETIME, '1900-01-01') AS ADDRESS_UPDATE_DATE, /* NONE */
      'CB' AS LINE_OF_BUSINESS, /* NONE */
      'LMS' AS SOURCE_NAME, /* NONE */
      T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
    FROM {params["com_schema"]}.T_LMS_TBL_EINVOICING_CLIENTDATA AS T1 /* NONE */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2
      ON T1.CUSTOMER_STATE_CODE = T2.REFERENCE_CODE
      AND T2.REFERENCE_TYPE = 'STATE'
      AND T2.SOURCE_NAME = 'LMS'
      AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
    /* AND T2.ETL_DT = '{batch_date}' */
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* None */
      ON T1.CUSTOMER_COUNTRY = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T4
      ON DATE_FORMAT(T4.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T4.SOURCE_NAME = 'LMS'
      AND T4.ADDRESS_OWNER_TYPE = 'ACCOUNT'
      AND T4.ADDRESS_TYPE = 'REGISTERED'
      AND T1.ACCOUNT_NUMBER = T4.SOURCE_RECORD_ID
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_1, '')) <> ''
        OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_2, '')) <> ''
        OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_3, '')) <> ''
        OR TRIM(COALESCE(T1.CUSTOMER_CITY, '')) <> ''
        OR TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
        OR TRIM(COALESCE(T1.CUSTOMER_POSTAL_CODE, '')) <> ''
        OR TRIM(COALESCE(T3.COUNTRY_CODE_3_DIGITS, '')) <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Source 19a: LMS Counterparty Permanent]============== */
    CREATE TABLE IF NOT EXISTS {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty (
      owner_id VARCHAR(50) COMMENT '',
      address_owner_type VARCHAR(20) COMMENT '',
      address_type VARCHAR(10) COMMENT '',
      address_line_1 VARCHAR(100) COMMENT '',
      address_line_2 VARCHAR(100) COMMENT '',
      address_line_3 VARCHAR(100) COMMENT '',
      address_line_4 VARCHAR(100) COMMENT '',
      city VARCHAR(255) COMMENT '',
      state VARCHAR(50) COMMENT '',
      postcode VARCHAR(5) COMMENT '',
      country VARCHAR(3) COMMENT '',
      address_create_date DATE COMMENT '',
      address_update_date DATE COMMENT '',
      line_of_business VARCHAR(20) COMMENT '',
      source_name VARCHAR(10) COMMENT '',
      source_record_id VARCHAR(50) COMMENT '',
      etl_timestamp STRING COMMENT 'ETL_processing_time'
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      'LMS_' || T0.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
      'REGISTERED' AS ADDRESS_TYPE, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_1)) AS ADDRESS_LINE_1, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_2)) AS ADDRESS_LINE_2, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_3)) AS ADDRESS_LINE_3, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_4)) AS ADDRESS_LINE_4, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_CITY)) AS CITY, /* NONE */
      UPPER(TRIM(T3.REFERENCE_VALUE_2)) AS STATE, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_POSTAL_CODE)) AS POSTCODE, /* NONE */
      UPPER(TRIM(T4.COUNTRY_CODE_3_DIGITS)) AS COUNTRY, /* NONE */
      COALESCE(T5.ADDRESS_CREATE_DATE, CURRENT_TIMESTAMP()) AS ADDRESS_CREATE_DATE, /* NONE */
      GREATEST(
        COALESCE(T1.SYSTEM_UPDATED_DATETIME, '1900-01-01'),
        COALESCE(T1.LAST_ACTION_DATETIME, '1900-01-01')
      ) AS ADDRESS_UPDATE_DATE, /* NONE */
      'CB' AS LINE_OF_BUSINESS, /* NONE */
      'LMS' AS SOURCE_NAME, /* NONE */
      T0.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONEtbl_JobParameter */
    FROM {params["com_schema"]}.T_LMSKIBB2_TBL_FACILITY AS T0
    INNER JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTERPARTY AS T1
      ON T0.COUNTERPARTY_ID = T1.COUNTERPARTY_ID
      AND DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
      AND (
        TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_1, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_2, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_3, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_POSTAL_CODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_CITY, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_STATE_CODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_COUNTRY_CODE, '')) <> ''
      )
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3
      ON T1.COUNTERPARTY_ADDRESS_STATE_CODE = T3.REFERENCE_CODE
      AND T3.REFERENCE_TYPE = 'STATE'
      AND T3.SOURCE_NAME = 'LMS'
      AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T4 /* None */
      ON T1.COUNTERPARTY_ADDRESS_COUNTRY_CODE = T4.COUNTRY_CODE_CCRIS AND T4.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T5
      ON DATE_FORMAT(T5.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T5.SOURCE_NAME = 'LMS'
      AND T5.ADDRESS_OWNER_TYPE = 'ACCOUNT'
      AND T5.ADDRESS_TYPE = 'REGISTERED'
      AND T0.ACCOUNT_NUMBER = T5.SOURCE_RECORD_ID
    WHERE
      DATE_FORMAT(T0.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND /* exclude collateral party */ NOT T0.FACILITY_TYPE_ID IN (7, 8)
""")
spark.sql(f"""
/* ==============LMS consolidation ============== 
     Insert */
    WITH COMBINED_ADDRESS AS (
      SELECT
        *
      FROM {params["cur_schema"]}.temp_dim_address_lms_tbl_einvoicing_clientdata
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty
    ), RANKED_ADDRESS_LMS /* Prioritize value from latest update record */ AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY OWNER_ID ORDER BY ADDRESS_UPDATE_DATE DESC) AS RN
      FROM COMBINED_ADDRESS
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    FROM RANKED_ADDRESS_LMS
    WHERE
      RN = 1
""")
spark.sql(f"""
/* ==============[Group 19b: LMS counterparty mailing]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      'LMS_' || T0.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
      'MAILING' AS ADDRESS_TYPE, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_1)) AS ADDRESS_LINE_1, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_2)) AS ADDRESS_LINE_2, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_3)) AS ADDRESS_LINE_3, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_4)) AS ADDRESS_LINE_4, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_CITY)) AS CITY, /* NONE */
      UPPER(TRIM(T3.REFERENCE_VALUE_2)) AS STATE, /* NONE */
      UPPER(TRIM(T1.COUNTERPARTY_ADDRESS_POSTAL_CODE)) AS POSTCODE, /* NONE */
      UPPER(TRIM(T4.COUNTRY_CODE_3_DIGITS)) AS COUNTRY, /* NONE */
      COALESCE(T5.ADDRESS_CREATE_DATE, CURRENT_TIMESTAMP()) AS ADDRESS_CREATE_DATE, /* NONE */
      GREATEST(
        COALESCE(T1.SYSTEM_UPDATED_DATETIME, '1900-01-01'),
        COALESCE(T1.LAST_ACTION_DATETIME, '1900-01-01')
      ) AS ADDRESS_UPDATE_DATE, /* NONE */
      'CB' AS LINE_OF_BUSINESS, /* NONE */
      'LMS' AS SOURCE_NAME, /* NONE */
      T0.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
    FROM {params["com_schema"]}.T_LMSKIBB2_TBL_FACILITY AS T0
    INNER JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTERPARTY AS T1
      ON T0.COUNTERPARTY_ID = T1.COUNTERPARTY_ID
      AND DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
      AND (
        TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_1, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_2, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_3, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_POSTAL_CODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_CITY, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_STATE_CODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTERPARTY_ADDRESS_COUNTRY_CODE, '')) <> ''
      )
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3
      ON T1.CORRESPONDENCE_ADDRESS_STATE_CODE = T3.REFERENCE_CODE
      AND T3.REFERENCE_TYPE = 'STATE'
      AND T3.SOURCE_NAME = 'LMS'
      AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T4 /* None */
      ON T1.CORRESPONDENCE_ADDRESS_COUNTRY_CODE = T4.COUNTRY_CODE_CCRIS AND T4.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T5
      ON DATE_FORMAT(T5.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T5.SOURCE_NAME = 'LMS'
      AND T5.ADDRESS_OWNER_TYPE = 'ACCOUNT'
      AND T5.ADDRESS_TYPE = 'MAILING'
      AND T0.ACCOUNT_NUMBER = T5.SOURCE_RECORD_ID
    WHERE
      DATE_FORMAT(T0.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND /* exclude collateral party */ NOT T0.FACILITY_TYPE_ID IN (7, 8)
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
# Identifies delta from COM_T based on record_updated_date
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_lms_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_lms_delta (
        owner_id VARCHAR(50)
        , address_owner_type VARCHAR(20)
        , address_type VARCHAR(10)
        , address_line_1 VARCHAR(100)
        , address_line_2 VARCHAR(100)
        , address_line_3 VARCHAR(100)
        , address_line_4 VARCHAR(100)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , postcode VARCHAR(5)
        , country VARCHAR(3)
        , address_create_date DATE
        , address_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , dl_record_status       VARCHAR(10)
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# main_processing_sqls should include the WHERE DATE_FORMAT(record_updated_date, 'yyyyMMdd') = '{batch_date}' logic
# ─── INSERT INTO CONSOLIDATED TABLE ────────────────────────────────────────────────────────

spark.sql(f"""
ALTER TABLE {params["cur_schema"]}.temp_DIM_ADDRESS_main_consolidated DROP IF EXISTS
""")

spark.sql(f"""
/* ==============[Group.23]============== */
    INSERT INTO {params["cur_schema"]}.temp_DIM_ADDRESS_main_consolidated (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP
    )
    SELECT
      T1.OWNER_ID AS OWNER_ID, /* None */
      T1.ADDRESS_OWNER_TYPE AS ADDRESS_OWNER_TYPE, /* None */
      T1.ADDRESS_TYPE AS ADDRESS_TYPE, /* None */
      T1.ADDRESS_LINE_1 AS ADDRESS_LINE_1, /* None */
      T1.ADDRESS_LINE_2 AS ADDRESS_LINE_2, /* None */
      T1.ADDRESS_LINE_3 AS ADDRESS_LINE_3, /* None */
      T1.ADDRESS_LINE_4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.STATE AS STATE, /* None */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      T1.ADDRESS_CREATE_DATE AS ADDRESS_CREATE_DATE, /* None */
      T1.ADDRESS_UPDATE_DATE AS ADDRESS_UPDATE_DATE, /* None */
      T1.LINE_OF_BUSINESS AS LINE_OF_BUSINESS, /* None */
      T1.SOURCE_NAME AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
    FROM (
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS
    ) AS T1 /* None */
    WHERE
      1 = 1
""")


# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_lms_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_lms_updated (
        owner_id VARCHAR(50)
        , address_owner_type VARCHAR(20)
        , address_type VARCHAR(10)
        , address_line_1 VARCHAR(100)
        , address_line_2 VARCHAR(100)
        , address_line_3 VARCHAR(100)
        , address_line_4 VARCHAR(100)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , postcode VARCHAR(5)
        , country VARCHAR(3)
        , address_create_date DATE
        , address_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Insert existing unchanged records from CUR ──────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_lms_updated
    SELECT
        cur.owner_id,
        cur.address_owner_type,
        cur.address_type,
        cur.address_line_1,
        cur.address_line_2,
        cur.address_line_3,
        cur.address_line_4,
        cur.city,
        cur.state,
        cur.postcode,
        cur.country,
        cur.address_create_date,
        cur.address_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_address_lms cur
    WHERE
cur.source_key = 'LMS' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_address_lms_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_lms_updated
    SELECT
        delta.owner_id,
        delta.address_owner_type,
        delta.address_type,
        delta.address_line_1,
        delta.address_line_2,
        delta.address_line_3,
        delta.address_line_4,
        delta.city,
        delta.state,
        delta.postcode,
        delta.country,
        delta.address_create_date,
        delta.address_update_date,
        delta.line_of_business,
        delta.source_record_id,
        COALESCE(delta.dl_record_status, 'A') AS dl_record_status,
        CASE
            WHEN cur.OWNER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE current_timestamp() END AS dl_record_created_date,
        current_timestamp() AS dl_record_updated_date
    FROM {params["com_schema"]}.temp_dim_address_lms_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_address_lms cur
        ON cur.source_key = 'LMS' AND delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE""")

# ─── STEP 3: Insert previous state of changed records into History table ─────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_address_lms_h PARTITION (source_key, hist_year)
    SELECT
        cur.owner_id,
        cur.address_owner_type,
        cur.address_type,
        cur.address_line_1,
        cur.address_line_2,
        cur.address_line_3,
        cur.address_line_4,
        cur.city,
        cur.state,
        cur.postcode,
        cur.country,
        cur.address_create_date,
        cur.address_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'LMS' AS source_key        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.dim_address_lms cur
    WHERE
cur.source_key = 'LMS' AND         EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_address_lms_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE        )
""")

# ─── STEP 4: Overwrite CUR table ─────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_lms PARTITION (source_key = 'LMS')    SELECT
        owner_id,
        address_owner_type,
        address_type,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        city,
        state,
        postcode,
        country,
        address_create_date,
        address_update_date,
        line_of_business,
        source_record_id,
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["com_schema"]}.temp_dim_address_lms_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_lms PARTITION (source_key = 'LMS') COMPUTE STATISTICS
""")

spark.stop()