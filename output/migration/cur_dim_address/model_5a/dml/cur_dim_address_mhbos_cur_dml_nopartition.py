
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

source_name = "MHBOS"
table_name  = "dim_address"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
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
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.ADDR1 AS ADDRESS_LINE_1, /* None */
      T1.ADDR2 AS ADDRESS_LINE_2, /* None */
      T1.ADDR3 AS ADDRESS_LINE_3, /* None */
      T1.ADDR4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      COALESCE(T2.REFERENCE_VALUE_2, T1.STATE) AS STATE, /* 20251013 */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      T1.DATE_CREATED AS ADDRESS_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS ADDRESS_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.STATE = T2.REFERENCE_CODE
      AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'MHBOS'
      AND T2.REFERENCE_TYPE = 'STATE_CODE'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.ADDR1, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR4, '')) <> ''
        OR TRIM(COALESCE(T1.CITY, '')) <> ''
        OR TRIM(COALESCE(T1.STATE, '')) <> ''
        OR TRIM(COALESCE(T1.POSTCODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group.2]============== */
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
      'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.PERM_ADDR1 AS ADDRESS_LINE_1, /* None */
      T1.PERM_ADDR2 AS ADDRESS_LINE_2, /* None */
      T1.PERM_ADDR3 AS ADDRESS_LINE_3, /* None */
      T1.PERM_ADDR4 AS ADDRESS_LINE_4, /* None */
      T1.PERM_CITY AS CITY, /* None */
      COALESCE(T3.REFERENCE_VALUE_2, T1.PERM_STATE) AS STATE, /* None */
      T1.PERM_POSTCODE AS POSTCODE, /* None */
      T1.PERM_COUNTRY AS COUNTRY, /* None */
      T1.DATE_CREATED AS ADDRESS_CREATE_DATE, /* None */
      T1.DATE_CHANGE AS ADDRESS_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3 /* None */
      ON T1.PERM_STATE = T3.REFERENCE_CODE
      AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T3.SOURCE_NAME = 'MHBOS'
      AND T3.REFERENCE_TYPE = 'STATE_CODE'
      AND TRIM(COALESCE(T3.REFERENCE_VALUE_2, '')) <> ''
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.PERM_ADDR1, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_ADDR2, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_ADDR3, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_ADDR4, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_CITY, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_STATE, '')) <> ''
        OR TRIM(COALESCE(T1.PERM_POSTCODE, '')) <> ''
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
      'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
      'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.ADDR1 AS ADDRESS_LINE_1, /* None */
      T1.ADDR2 AS ADDRESS_LINE_2, /* None */
      T1.ADDR3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T2.REFERENCE_VALUE_2 AS STATE, /* None */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_CREATE_DATE, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.TDR_CODE AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_TRADER AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.STATE = T2.REFERENCE_CODE
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'MHBOS'
      AND T2.REFERENCE_TYPE = 'STATE_CODE'
      AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.ADDR1, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group.4]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS (
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
INSERT INTO {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS (
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
      'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
      'BRANCH' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.ADDR1 AS ADDRESS_LINE_1, /* None */
      T1.ADDR2 AS ADDRESS_LINE_2, /* None */
      T1.ADDR3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      NULL AS CITY, /* None */
      NULL AS STATE, /* None */
      T1.POST_CODE AS POSTCODE, /* None */
      NULL AS COUNTRY, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_CREATE_DATE, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.BRANCH_ID AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.T_MHBOS_M_BRANCH AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.ADDR1, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
        OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
        OR TRIM(COALESCE(T1.POST_CODE, '')) <> ''
      )
""")
spark.sql(f"""
/* behind TEMP_DIM_ACCOUNT_ADDRESS 
     ==============[Group.20]============== 
     added 20250128 */
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
      'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
      'AGENT_ASSISTANT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.ADDR1 AS ADDRESS_LINE_1, /* None */
      T1.ADDR2 AS ADDRESS_LINE_2, /* None */
      T1.ADDR3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      NULL AS CITY, /* None */
      NULL AS STATE, /* None */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_CREATE_DATE, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS ADDRESS_UPDATE_DATE, /* None */
      'EB' AS LINE_OF_BUSINESS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      IC_NO_NEW AS SOURCE_RECORD_ID /* None */
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND LICENCE_TYPE IN ('08', '09', '10', '11')
        AND (
          TRIM(COALESCE(T1.ADDR1, '')) <> ''
          OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
          OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
        )
    ) AS T1
    WHERE
      RN = 1
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
""")

# ─── DELTA TABLE SETUP (EXTRACT IMPACTED DATES FROM COM_T) ──────────────────
# Find all distinct datecolumn2 where records were updated in the current batch
# Then pull ALL active records from COM_T for those dates.
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_mhbos_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_mhbos_delta (
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
        , raw_incremental_etl_dt STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# Note: key_date_column needs to be provided in the template variables
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_mhbos_delta
    SELECT
        com_t.owner_id,
        com_t.address_owner_type,
        com_t.address_type,
        com_t.address_line_1,
        com_t.address_line_2,
        com_t.address_line_3,
        com_t.address_line_4,
        com_t.city,
        com_t.state,
        com_t.postcode,
        com_t.country,
        com_t.address_create_date,
        com_t.address_update_date,
        com_t.line_of_business,
        com_t.source_record_id,
        com_t.raw_incremental_etl_dt
    FROM {params["com_schema"]}.dim_address_mhbos com_t
    INNER JOIN (
        SELECT DISTINCT <<key_date_column>>
        FROM {params["com_schema"]}.dim_address_mhbos
        WHERE DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) delta ON com_t.<<key_date_column>> = delta.<<key_date_column>>
    WHERE com_t.dl_record_status = 'A'
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_mhbos_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_mhbos_updated (
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
        , raw_incremental_etl_dt STRING
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep existing records from CUR that are NOT in impacted dates ──
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_mhbos_updated
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
        cur.raw_incremental_etl_dt,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_address_mhbos cur
    WHERE
cur.source_key = 'MHBOS' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_address_mhbos_delta delta
            WHERE delta.<<key_date_column>> = cur.<<key_date_column>>
        )
""")

# ─── STEP 2: Insert transformed delta into temp table ────────────────────────
# main_processing_sqls is expected to transform data from temp_{target_table_name}_delta
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


# ─── STEP 3: Overwrite CUR table ─────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_mhbos PARTITION (source_key = 'MHBOS')    SELECT
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
        raw_incremental_etl_dt,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["com_schema"]}.temp_dim_address_mhbos_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_mhbos PARTITION (source_key = 'MHBOS') COMPUTE STATISTICS
""")

spark.stop()