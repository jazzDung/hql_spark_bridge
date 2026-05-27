
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
from pyspark.sql.functions import current_timestamp, md5, concat_ws, coalesce, lit

source_name = "TOMS"
table_name  = "dim_address"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites if there is a partition key
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── SOURCE PROCESSING (Temp tables logic from legacy script) ───────────────────
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
/* ==============[Group.3]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS (
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
/* ==============[Group.11]============== */
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.PADD1 AS ADDRESS_LINE_1, /* None */
      T1.PADD2 AS ADDRESS_LINE_2, /* None */
      T1.PADD3 AS ADDRESS_LINE_3, /* None */
      T1.PADD4 AS ADDRESS_LINE_4, /* None */
      T1.PTOWN AS CITY, /* None */
      COALESCE(T4.REFERENCE_VALUE_2, T1.PSTATE) AS STATE, /* None */
      T1.PPOSTCODE AS POSTCODE, /* None */
      COALESCE(T3.COUNTRY_CODE_3_DIGITS, T1.PCOUNTRY) AS COUNTRY, /* None */
      T1.SYDTC AS ADDRESS_CREATE_DATE, /* None */
      T1.SYDTU AS ADDRESS_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.PCOUNTRY = T2.REFERENCE_CODE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
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
      ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
      ON T1.PSTATE = T4.REFERENCE_CODE
      AND /* AND T4.ETL_DT = '{batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.SOURCE_NAME = 'TOMS'
      AND T4.REFERENCE_TYPE = 'STATE'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.PADD1, '')) <> ''
        OR TRIM(COALESCE(T1.PADD2, '')) <> ''
        OR TRIM(COALESCE(T1.PADD3, '')) <> ''
        OR TRIM(COALESCE(T1.PADD4, '')) <> ''
        OR TRIM(COALESCE(T1.PTOWN, '')) <> ''
        OR NOT T1.PSTATE IS NULL
        OR TRIM(COALESCE(T1.PPOSTCODE, '')) <> ''
        OR NOT T1.PCOUNTRY IS NULL
      )
""")
spark.sql(f"""
/* ==============[Group.12]============== */
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.CADD1 AS ADDRESS_LINE_1, /* None */
      T1.CADD2 AS ADDRESS_LINE_2, /* None */
      T1.CADD3 AS ADDRESS_LINE_3, /* None */
      T1.CADD4 AS ADDRESS_LINE_4, /* None */
      T1.CTOWN AS CITY, /* None */
      (
        CASE WHEN T1.CSTATE = 0 THEN NULL ELSE COALESCE(T4.REFERENCE_VALUE_2, T1.CSTATE) END
      ) AS STATE, /* None */
      T1.CPOSTCODE AS POSTCODE, /* None */
      (
        CASE
          WHEN T1.CCOUNTRY = 0
          THEN NULL
          ELSE COALESCE(T3.COUNTRY_CODE_3_DIGITS, T1.CCOUNTRY)
        END
      ) AS COUNTRY, /* None */
      T1.SYDTC AS ADDRESS_CREATE_DATE, /* None */
      T1.SYDTU AS ADDRESS_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_TOMS_ECORPORATE_ACCOUNT AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.CCOUNTRY = T2.REFERENCE_CODE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
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
      ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
      ON T1.CSTATE = T4.REFERENCE_CODE
      AND /* AND T4.ETL_DT = '{batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.SOURCE_NAME = 'TOMS'
      AND T4.REFERENCE_TYPE = 'STATE'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.CADD1, '')) <> ''
        OR TRIM(COALESCE(T1.CADD2, '')) <> ''
        OR TRIM(COALESCE(T1.CADD3, '')) <> ''
        OR TRIM(COALESCE(T1.CADD4, '')) <> ''
        OR TRIM(COALESCE(T1.CTOWN, '')) <> ''
        OR NOT T1.CSTATE IS NULL
        OR TRIM(COALESCE(T1.CPOSTCODE, '')) <> ''
        OR NOT T1.CCOUNTRY IS NULL
      )
""")
spark.sql(f"""
/* ==============[Group.13]============== */
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.PADD1 AS ADDRESS_LINE_1, /* None */
      T1.PADD2 AS ADDRESS_LINE_2, /* None */
      T1.PADD3 AS ADDRESS_LINE_3, /* None */
      T1.PADD4 AS ADDRESS_LINE_4, /* None */
      T1.PTOWN AS CITY, /* None */
      (
        CASE WHEN T1.PSTATE = 0 THEN NULL ELSE COALESCE(T4.REFERENCE_VALUE_2, T1.PSTATE) END
      ) AS STATE, /* None */
      T1.PPOSTCODE AS POSTCODE, /* None */
      (
        CASE
          WHEN T1.PCOUNTRY = 0
          THEN NULL
          ELSE COALESCE(T3.COUNTRY_CODE_3_DIGITS, T1.PCOUNTRY)
        END
      ) AS COUNTRY, /* None */
      T1.SYDTC AS ADDRESS_CREATE_DATE, /* None */
      T1.SYDTU AS ADDRESS_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.PCOUNTRY = T2.REFERENCE_CODE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
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
      ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
      ON T1.PSTATE = T4.REFERENCE_CODE
      AND /* AND T4.ETL_DT = '{batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.SOURCE_NAME = 'TOMS'
      AND T4.REFERENCE_TYPE = 'STATE'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.PADD1, '')) <> ''
        OR TRIM(COALESCE(T1.PADD2, '')) <> ''
        OR TRIM(COALESCE(T1.PADD3, '')) <> ''
        OR TRIM(COALESCE(T1.PADD4, '')) <> ''
        OR TRIM(COALESCE(T1.PTOWN, '')) <> ''
        OR NOT T1.PSTATE IS NULL
        OR TRIM(COALESCE(T1.PPOSTCODE, '')) <> ''
        OR NOT T1.PCOUNTRY IS NULL
      )
""")
spark.sql(f"""
/* ==============[Group.14]============== */
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
      'TOMS_' || T1.ACCOUNTNO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.CADD1 AS ADDRESS_LINE_1, /* None */
      T1.CADD2 AS ADDRESS_LINE_2, /* None */
      T1.CADD3 AS ADDRESS_LINE_3, /* None */
      T1.CADD4 AS ADDRESS_LINE_4, /* None */
      T1.CTOWN AS CITY, /* None */
      (
        CASE WHEN T1.CSTATE = 0 THEN NULL ELSE COALESCE(T4.REFERENCE_VALUE_2, T1.CSTATE) END
      ) AS STATE, /* None */
      T1.CPOSTCODE AS POSTCODE, /* None */
      (
        CASE
          WHEN T1.CCOUNTRY = 0
          THEN NULL
          ELSE COALESCE(T3.COUNTRY_CODE_3_DIGITS, T1.CCOUNTRY)
        END
      ) AS COUNTRY, /* None */
      T1.SYDTC AS ADDRESS_CREATE_DATE, /* None */
      T1.SYDTU AS ADDRESS_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.ACCOUNTNO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_TOMS_ERETAIL_ACCOUNT AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.CCOUNTRY = T2.REFERENCE_CODE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
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
      ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
      ON T1.CSTATE = T4.REFERENCE_CODE
      AND /* AND T4.ETL_DT = '{batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.SOURCE_NAME = 'TOMS'
      AND T4.REFERENCE_TYPE = 'STATE'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.CADD1, '')) <> ''
        OR TRIM(COALESCE(T1.CADD2, '')) <> ''
        OR TRIM(COALESCE(T1.CADD3, '')) <> ''
        OR TRIM(COALESCE(T1.CADD4, '')) <> ''
        OR TRIM(COALESCE(T1.CTOWN, '')) <> ''
        OR NOT T1.CSTATE IS NULL
        OR TRIM(COALESCE(T1.CPOSTCODE, '')) <> ''
        OR NOT T1.CCOUNTRY IS NULL
      )
""")
spark.sql(f"""
/* ==============[Group.15]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS (
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
      'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
      'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.CADD1 AS ADDRESS_LINE_1, /* None */
      T1.CADD2 AS ADDRESS_LINE_2, /* None */
      T1.CADD3 AS ADDRESS_LINE_3, /* None */
      T1.CADD4 AS ADDRESS_LINE_4, /* None */
      T1.CTOWN AS CITY, /* None */
      T4.REFERENCE_VALUE_2 AS STATE, /* None */
      T1.CPOSTCODE AS POSTCODE, /* None */
      T3.COUNTRY_CODE_3_DIGITS AS COUNTRY, /* None */
      T1.SYDTC AS ADDRESS_CREATE_DATE, /* None */
      T1.SYDTU AS ADDRESS_UPDATE_DATE, /* None */
      'UT' AS LINE_OF_BUSINESS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.AGENTCODE AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_TOMS_EAGENTDETAILS AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.CCOUNTRY = T2.REFERENCE_VALUE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
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
      ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
    LEFT JOIN (
      SELECT DISTINCT
        REFERENCE_VALUE,
        REFERENCE_VALUE_2
      FROM {params["cur_schema"]}.REF_LOOKUP
      WHERE
        SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
        AND SOURCE_NAME = 'TOMS'
        AND REFERENCE_TYPE = 'STATE'
    ) AS T4 /* None */
      ON T1.CSTATE = T4.REFERENCE_VALUE
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.CADD1, '')) <> ''
        OR TRIM(COALESCE(T1.CADD2, '')) <> ''
        OR TRIM(COALESCE(T1.CADD3, '')) <> ''
        OR TRIM(COALESCE(T1.CADD4, '')) <> ''
        OR TRIM(COALESCE(T1.CTOWN, '')) <> ''
        OR NOT T1.CSTATE IS NULL
        OR TRIM(COALESCE(T1.CPOSTCODE, '')) <> ''
        OR NOT T1.CCOUNTRY IS NULL
      )
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
# This table will hold the transformed delta records from the COM layer.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_address_toms_delta""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_address_toms_delta (
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
        , hash_value             STRING
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── MAIN PROCESSING (Populate Delta Table) ───────────────────────────────────
# The main_processing_sqls should contain the logic to transform and insert data
# from com_t into the temp delta table, filtering for the current batch_date,
# and calculating the hash_value.
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
# This table will hold the final, merged dataset.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_address_toms_updated""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_address_toms_updated (
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
        , hash_value             STRING
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from CUR ──────────────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_address_toms_updated
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
        cur.hash_value,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_address_toms cur
    WHERE cur.source_key = 'TOMS' AND         NOT EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_address_toms_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_address_toms_updated
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
        delta.dl_record_status,
        delta.hash_value,
        CASE
            WHEN cur.OWNER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE delta.dl_record_created_date
        END AS dl_record_created_date,
        CASE
            WHEN cur.hash_value = delta.hash_value THEN cur.dl_record_updated_date
            ELSE delta.dl_record_updated_date
        END AS dl_record_updated_date
    FROM {params["tmp_schema"]}.temp_dim_address_toms_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_address_toms cur
        ON cur.source_key = 'TOMS' AND delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE""")

# ─── STEP 3: Insert outdated CUR records into History table ──────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_address_toms_h PARTITION (source_key, hist_year)
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
        cur.hash_value,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'TOMS' AS source_key        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.dim_address_toms cur
    INNER JOIN {params["tmp_schema"]}.temp_dim_address_toms_delta delta
        ON delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE    WHERE cur.source_key = 'TOMS' AND         cur.hash_value <> delta.hash_value
""")

# ─── STEP 4: Overwrite CUR table ──────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_toms PARTITION (source_key = 'TOMS')    SELECT
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
        hash_value,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["tmp_schema"]}.temp_dim_address_toms_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_toms PARTITION (source_key = 'TOMS') COMPUTE STATISTICS
""")

spark.stop()