
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

source_name = "GUAVA"
table_name  = "dim_address"

# spark session
spark, today_date = run_etl_cur(source_name, table_name)
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
/* ==============[Group.5 + 6: GUAVA Customer address]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA (
      OWNER_ID VARCHAR(50), /* None */
      ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
      ADDRESS_TYPE VARCHAR(20), /* None */
      ADDRESS_LINE_1 VARCHAR(100), /* None */
      ADDRESS_LINE_2 VARCHAR(100), /* None */
      ADDRESS_LINE_3 VARCHAR(100), /* None */
      ADDRESS_LINE_4 VARCHAR(100), /* None */
      CITY VARCHAR(255), /* None */
      STATE VARCHAR(50), /* None */
      POSTCODE VARCHAR(5), /* None */
      COUNTRY VARCHAR(3), /* None */
      ADDRESS_CREATE_DATE TIMESTAMP, /* None */
      ADDRESS_UPDATE_DATE TIMESTAMP, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(20), /* NONE */
      PRIORITY INT /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.5]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA (
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
      PRIORITY /* None */
    )
    SELECT
      'GUAVA_' || T1.ACCOUNT_NO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      T1.CUSTOMER_ADDRESS_TYPE AS ADDRESS_TYPE, /* None */
      TRIM(T1.CUSTOMER_ADDRESS_LINE1) AS ADDRESS_LINE_1, /* NONE */
      TRIM(T1.CUSTOMER_ADDRESS_LINE2) AS ADDRESS_LINE_2, /* NONE */
      TRIM(T1.CUSTOMER_ADDRESS_LINE3) AS ADDRESS_LINE_3, /* NONE */
      TRIM(T1.CUSTOMER_ADDRESS_LINE4) AS ADDRESS_LINE_4, /* NONE */
      TRIM(T1.CITY) AS CITY, /* NONE */
      CASE
        WHEN COALESCE(T1.STATE, '') <> ''
        THEN T2.REFERENCE_VALUE_2
        WHEN COALESCE(T1.STATE, '') = '' AND T1.COUNTRY <> 'MYS'
        THEN 'NOT APPLICABLE'
        ELSE NULL
      END AS STATE, /* None */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_DATE, /* None */
      NULL AS ADDRESS_UPDATE_DATE, /* None */
      'TR' AS LINE_OF_BUSINESS, /* None */
      'GUAVA' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
      1 AS PRIORITY /* None */
    FROM {params["com_schema"]}.T_GUAVA_CUSTOMER AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2
      ON T1.STATE = T2.REFERENCE_CODE
      AND T2.REFERENCE_TYPE = 'STATE'
      AND T2.SOURCE_NAME = 'GUAVA'
      AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
      AND (
        TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE1, '')) <> ''
        OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE2, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group.6a]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA (
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
      PRIORITY /* None */
    )
    SELECT
      'GUAVA_' || T1.ATTACHMENT_TRANSACTION_INFO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T2.PHYSICALADDRESS_STREET1 AS ADDRESS_LINE_1, /* None */
      T2.PHYSICALADDRESS_STREET2 AS ADDRESS_LINE_2, /* None */
      NULL AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T2.PHYSICALADDRESS_CITY AS CITY, /* None */
      T2.PHYSICALADDRESS_STATE AS STATE, /* None */
      T2.PHYSICALADDRESS_POSTCODE AS POSTCODE, /* None */
      T2.PHYSICALADDRESS_COUNTRY AS COUNTRY, /* None */
      T1.ATTACHMENT_OWNER_DATE_CREATION AS ADDRESS_CREATE_DATE, /* None */
      T2.ATTACHMENT_ADDRESS_LAST_MAIN_DATE AS ADDRESS_UPDATE_DATE, /* None */
      'TR' AS LINE_OF_BUSINESS, /* None */
      'GUAVA' AS SOURCE_NAME, /* None */
      T1.ATTACHMENT_TRANSACTION_INFO AS SOURCE_RECORD_ID, /* None */
      2 AS PRIORITY /* None */
    FROM {params["com_schema"]}.T_GUAVA_COMPANY AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_GUAVA_BRANCH AS T2 /* None */
      ON T1.ATTACHMENT_TRANSACTION_INFO = T2.ATTACHMENT_TRANSACTION_INFO
      AND T1.BRANCH_NAME = T2.BRANCH_NAME
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T1.IC_SERIAL_NUMBER = 1
      AND NOT T2.ATTACHMENT_TRANSACTION_INFO LIKE '@[%'
      AND (
        TRIM(COALESCE(T2.PHYSICALADDRESS_STREET1, '')) <> ''
        OR TRIM(COALESCE(T2.PHYSICALADDRESS_STREET2, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group.6b]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA (
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
      PRIORITY /* None */
    )
    SELECT
      'GUAVA_' || T1.ATTACHMENT_TRANSACTION_INFO AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T2.CORRESPONDENCE_STREET1 AS ADDRESS_LINE_1, /* None */
      T2.CORRESPONDENCE_STREET2 AS ADDRESS_LINE_2, /* None */
      NULL AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T2.CORRESPONDENCE_CITY AS CITY, /* None */
      T2.CORRESPONDENCE_STATE AS STATE, /* None */
      T2.CORRESPONDENCE_POSTCODE AS POSTCODE, /* None */
      T2.CORRESPONDENCE_COUNTRY AS COUNTRY, /* None */
      T1.ATTACHMENT_OWNER_DATE_CREATION AS ADDRESS_CREATE_DATE, /* None */
      T2.ATTACHMENT_ADDRESS_LAST_MAIN_DATE AS ADDRESS_UPDATE_DATE, /* None */
      'TR' AS LINE_OF_BUSINESS, /* None */
      'GUAVA' AS SOURCE_NAME, /* None */
      T1.ATTACHMENT_TRANSACTION_INFO AS SOURCE_RECORD_ID, /* None */
      2 AS PRIORITY /* None */
    FROM {params["com_schema"]}.T_GUAVA_COMPANY AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_GUAVA_BRANCH AS T2 /* None */
      ON T1.ATTACHMENT_TRANSACTION_INFO = T2.ATTACHMENT_TRANSACTION_INFO
      AND T1.BRANCH_NAME = T2.BRANCH_NAME
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T1.IC_SERIAL_NUMBER = 1
      AND NOT T2.ATTACHMENT_TRANSACTION_INFO LIKE '@[%'
      AND (
        TRIM(COALESCE(T2.CORRESPONDENCE_STREET1, '')) <> ''
        OR TRIM(COALESCE(T2.CORRESPONDENCE_STREET2, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group.5 + 6: GUAVA Customer address]============== */
    WITH TEMP_DIM_ACCOUNT_ADDRESS_GUAVA_ROW_NUM AS (
      SELECT
        OWNER_ID, /* None */
        ADDRESS_OWNER_TYPE, /* None */
        ADDRESS_TYPE, /* None */
        UPPER(ADDRESS_LINE_1) AS ADDRESS_LINE_1,
        UPPER(ADDRESS_LINE_2) AS ADDRESS_LINE_2,
        UPPER(ADDRESS_LINE_3) AS ADDRESS_LINE_3,
        UPPER(ADDRESS_LINE_4) AS ADDRESS_LINE_4,
        UPPER(CITY) AS CITY,
        UPPER(STATE) AS STATE,
        UPPER(POSTCODE) AS POSTCODE,
        UPPER(COUNTRY) AS COUNTRY,
        DATE_FORMAT(CAST(ADDRESS_CREATE_DATE AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_CREATE_DATE, /* None */
        DATE_FORMAT(CAST(ADDRESS_UPDATE_DATE AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_UPDATE_DATE, /* None */
        LINE_OF_BUSINESS, /* None */
        SOURCE_NAME, /* None */
        SOURCE_RECORD_ID, /* None */
        ROW_NUMBER() OVER (PARTITION BY OWNER_ID, ADDRESS_OWNER_TYPE, ADDRESS_TYPE ORDER BY PRIORITY) AS rn
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA
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
    FROM TEMP_DIM_ACCOUNT_ADDRESS_GUAVA_ROW_NUM
    WHERE
      rn = 1
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")

# ─── DELTA TABLE SETUP ───────────────────────────────────────────────────────
# This table holds the transformed dataset from COM_T
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_guava_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_guava_delta (
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
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_guava_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_guava_updated (
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

# ─── STEP 1: Upsert new and updated records from COM_T ───────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_guava_updated
    SELECT
        delta.owner_id
        , delta.address_owner_type
        , delta.address_type
        , delta.address_line_1
        , delta.address_line_2
        , delta.address_line_3
        , delta.address_line_4
        , delta.city
        , delta.state
        , delta.postcode
        , delta.country
        , delta.address_create_date
        , delta.address_update_date
        , delta.line_of_business
        , delta.source_record_id
        , delta.dl_record_status
        , CASE
            WHEN cur.OWNER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE current_timestamp() END AS dl_record_created_date
        , current_timestamp() AS dl_record_updated_date
    FROM {params["com_schema"]}.temp_dim_address_guava_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_address_guava cur
        ON cur.source_key = 'GUAVA' AND delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE""")

# ─── STEP 2: Mark removed CUR records as deleted ('X') ───────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_address_guava_updated
    SELECT
        cur.owner_id
        , cur.address_owner_type
        , cur.address_type
        , cur.address_line_1
        , cur.address_line_2
        , cur.address_line_3
        , cur.address_line_4
        , cur.city
        , cur.state
        , cur.postcode
        , cur.country
        , cur.address_create_date
        , cur.address_update_date
        , cur.line_of_business
        , cur.source_record_id
        , 'X' AS dl_record_status
        , cur.dl_record_created_date
        , current_timestamp() AS dl_record_updated_date
    FROM {params["cur_schema"]}.dim_address_guava cur
    WHERE cur.source_key = 'GUAVA' AND NOT EXISTS (
        SELECT 1 FROM {params["com_schema"]}.temp_dim_address_guava_delta delta
        WHERE cur.OWNER_ID = delta.OWNER_ID AND cur.ADDRESS_OWNER_TYPE = delta.ADDRESS_OWNER_TYPE AND cur.ADDRESS_TYPE = delta.ADDRESS_TYPE    )
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_guava PARTITION (source_key = 'GUAVA')    SELECT
        owner_id
        , address_owner_type
        , address_type
        , address_line_1
        , address_line_2
        , address_line_3
        , address_line_4
        , city
        , state
        , postcode
        , country
        , address_create_date
        , address_update_date
        , line_of_business
        , source_record_id
        , dl_record_status
        , dl_record_created_date
        , dl_record_updated_date
        , '{batch_date}'       AS etl_dt
        , current_timestamp()  AS etl_timestamp
    FROM {params["com_schema"]}.temp_dim_address_guava_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_guava PARTITION (source_key = 'GUAVA') COMPUTE STATISTICS
""")

spark.stop()