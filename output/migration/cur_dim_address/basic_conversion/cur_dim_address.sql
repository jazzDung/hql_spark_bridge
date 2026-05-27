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

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

source_name = "dim"
table_name = "address"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
last_date = yesterday_date
batch_yyyymm = batch_date[:-2]

# ext_start_time = datetime.strptime(ext_start_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# ext_end_time = datetime.strptime(ext_end_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)



spark.sql(rf"""
/* Drop temp table */
DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty
""")

spark.sql(rf"""
DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_address_lms_tbl_einvoicing_clientdata
""")

spark.sql(rf"""
/* ==============[Group.1]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")

spark.sql(rf"""
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
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
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
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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
  ON T1.CLIENT_NO = T2.CLIENT_NO AND T2.PART_ID = '{batch_date}'
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3 /* None */
  ON T1.PERM_STATE = T3.REFERENCE_CODE
  AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T3.SOURCE_NAME = 'MHBOS'
  AND T3.REFERENCE_TYPE = 'STATE_CODE'
  AND TRIM(COALESCE(T3.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
/* ==============[Group.3]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")

spark.sql(rf"""
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
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T2.SOURCE_NAME = 'MHBOS'
  AND T2.REFERENCE_TYPE = 'STATE_CODE'
  AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
  )
""")

spark.sql(rf"""
/* ==============[Group.4]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
""")

spark.sql(rf"""
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
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
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
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
    OR TRIM(COALESCE(T1.POST_CODE, '')) <> ''
  )
""")

spark.sql(rf"""
/* ==============[Group.5 + 6: GUAVA Customer address]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_GUAVA
""")

spark.sql(rf"""
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
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
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
  T1.ETL_DT = '{batch_date}'
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
  AND (
    TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE1, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE2, '')) <> ''
  )
""")

spark.sql(rf"""
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
  AND T2.ETL_DT = '{batch_date}'
WHERE
  T1.ETL_DT = '{batch_date}'
  AND T1.IC_SERIAL_NUMBER = 1
  AND NOT T2.ATTACHMENT_TRANSACTION_INFO LIKE '@[%'
  AND (
    TRIM(COALESCE(T2.PHYSICALADDRESS_STREET1, '')) <> ''
    OR TRIM(COALESCE(T2.PHYSICALADDRESS_STREET2, '')) <> ''
  )
""")

spark.sql(rf"""
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
  AND T2.ETL_DT = '{batch_date}'
WHERE
  T1.ETL_DT = '{batch_date}'
  AND T1.IC_SERIAL_NUMBER = 1
  AND NOT T2.ATTACHMENT_TRANSACTION_INFO LIKE '@[%'
  AND (
    TRIM(COALESCE(T2.CORRESPONDENCE_STREET1, '')) <> ''
    OR TRIM(COALESCE(T2.CORRESPONDENCE_STREET2, '')) <> ''
  )
""")

spark.sql(rf"""
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

spark.sql(rf"""
/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21
""")

spark.sql(rf"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
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
  ADDRESS_CREATE_TIME TIMESTAMP, /* None */
  ADDRESS_UPDATE_TIME TIMESTAMP, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(20), /* NONE */
  ETL_TIMESTAMP STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
/* ==============[Group.7: M21 Customer address: (account, business), (account, REGISTERED), (account, MAILING)]============== */
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account Business address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'BUSINESS' AS ADDRESS_TYPE, /* None */
  T1.BUSINESSADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.BUSINESSADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.BUSINESSADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.BUSINESSCITY AS CITY, /* None */
  T1.BUSINESSCOUNTRYSTATE AS STATE, /* None */
  T1.BUSINESSPOSTALCODE AS POSTCODE, /* None */
  COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.BUSINESSCOUNTRY) AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM {params["com_schema"]}.T_M21_CUSTOMER AS T1 /* None */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM {params["cur_schema"]}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T2 /* None */
  ON T1.BUSINESSCOUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.BUSINESSADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCITY, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCOUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSPOSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCOUNTRY, '')) <> ''
  )
UNION ALL
/* Account registered address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.ADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.ADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.ADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.COUNTRYSTATE AS STATE, /* None */
  T1.POSTALCODE AS POSTCODE, /* None */
  COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.COUNTRY) AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM {params["com_schema"]}.T_M21_CUSTOMER AS T1 /* None */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM {params["cur_schema"]}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T2 /* None */
  ON T1.COUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.POSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  )
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.MAILINGADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.MAILINGADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.MAILINGADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.COUNTRYSTATE AS STATE, /* None */
  T1.MAILINGPOSTALCODE AS POSTCODE, /* None */
  T1.COUNTRY AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM {params["com_schema"]}.R_M21_STATEMENTRECIPIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.MAILINGADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGPOSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  )
""")

spark.sql(rf"""
/* ==============[Group 8: M21_A Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING), (ACCOUNT, EMPLOYER) ]============== */
WITH M21_A_CUSTOMER AS (
  SELECT
    T1.account_number,
    T1.customer_address_type,
    T1.customer_address_line1,
    T1.customer_address_line2,
    T1.customer_address_line3,
    T1.customer_address_line4,
    T1.CITY,
    T2.reference_value AS state,
    T1.postcode,
    T1.country,
    T1.etl_timestamp
  FROM {params["com_schema"]}.T_M21_A_CUSTOMER AS T1 /* None */
  LEFT JOIN {params["cur_schema"]}.ref_lookup AS T2
    ON T1.state = T2.reference_code
    AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
    AND T2.reference_type = 'IRBSTATE'
  WHERE
    T1.ETL_DT = '{batch_date}'
    AND (
      TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
      OR TRIM(COALESCE(T1.CITY, '')) <> ''
      OR TRIM(COALESCE(T1.state, '')) <> ''
      OR TRIM(COALESCE(T1.postcode, '')) <> ''
      OR TRIM(COALESCE(T1.country, '')) <> ''
    )
)
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account registered address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
UNION ALL
/* Account Employer address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'EMPLOYER' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER'
""")

spark.sql(rf"""
/* ==============[Group 9: M21_O Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING)]============== */
WITH M21_O_CUSTOMER AS (
  SELECT
    T1.account_number,
    T1.customer_address_type,
    T1.customer_address_line1,
    T1.customer_address_line2,
    T1.customer_address_line3,
    T1.customer_address_line4,
    T1.CITY,
    T2.reference_value AS state,
    T1.postcode,
    T1.country,
    T1.etl_timestamp
  FROM {params["com_schema"]}.T_M21_O_CUSTOMER AS T1 /* None */
  LEFT JOIN {params["cur_schema"]}.ref_lookup AS T2
    ON T1.state = T2.reference_code
    AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
    AND T2.reference_type = 'IRBSTATE'
  WHERE
    T1.ETL_DT = '{batch_date}'
    AND (
      TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
      OR TRIM(COALESCE(T1.CITY, '')) <> ''
      OR TRIM(COALESCE(T1.state, '')) <> ''
      OR TRIM(COALESCE(T1.postcode, '')) <> ''
      OR TRIM(COALESCE(T1.country, '')) <> ''
    )
)
INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account registered address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
UNION ALL
/* Account Employer address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'EMPLOYER' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER'
""")

spark.sql(rf"""
/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
WITH TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM AS (
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
    DATE_FORMAT(CAST(ADDRESS_CREATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_CREATE_DATE, /* None */
    DATE_FORMAT(CAST(ADDRESS_UPDATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_UPDATE_DATE, /* None */
    LINE_OF_BUSINESS, /* None */
    SOURCE_NAME, /* None */
    SOURCE_RECORD_ID, /* None */
    ROW_NUMBER() OVER (PARTITION BY OWNER_ID, ADDRESS_OWNER_TYPE, ADDRESS_TYPE ORDER BY ADDRESS_UPDATE_TIME DESC) AS rn
  FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21
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
FROM TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM
WHERE
  rn = 1
""")

spark.sql(rf"""
/* ==============[Group.10]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.LINE1 AS ADDRESS_LINE_1, /* None */
  T1.LINE2 AS ADDRESS_LINE_2, /* None */
  T1.LINE3 AS ADDRESS_LINE_3, /* None */
  T1.LINE4 AS ADDRESS_LINE_4, /* None */
  NULL AS CITY, /* None */
  NULL AS STATE, /* None */
  NULL AS POSTCODE, /* None */
  NULL AS COUNTRY, /* None */
  T2.CREATEDATE AS ADDRESS_CREATE_DATE, /* None */
  T2.MODIFYDATE AS ADDRESS_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID /* None */
FROM {params["com_schema"]}.R_M21_ACCOUNTEXECUTIVEADDRESS AS T1 /* None */
LEFT JOIN {params["com_schema"]}.T_M21_ACCOUNTEXECUTIVE AS T2 /* None */
  ON T1.CODE = T2.CODE AND T2.ETL_DT = '{batch_date}'
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.LINE1, '')) <> ''
    OR TRIM(COALESCE(T1.LINE2, '')) <> ''
    OR TRIM(COALESCE(T1.LINE3, '')) <> ''
    OR TRIM(COALESCE(T1.LINE4, '')) <> ''
  )
""")

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
  ON T1.PSTATE = T4.REFERENCE_CODE
  AND /* AND T4.ETL_DT = '${batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T4.SOURCE_NAME = 'TOMS'
  AND T4.REFERENCE_TYPE = 'STATE'
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
  ON T1.CSTATE = T4.REFERENCE_CODE
  AND /* AND T4.ETL_DT = '${batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T4.SOURCE_NAME = 'TOMS'
  AND T4.REFERENCE_TYPE = 'STATE'
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
  ON T1.PSTATE = T4.REFERENCE_CODE
  AND /* AND T4.ETL_DT = '${batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T4.SOURCE_NAME = 'TOMS'
  AND T4.REFERENCE_TYPE = 'STATE'
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T2.REFERENCE_VALUE_2 = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4 /* None */
  ON T1.CSTATE = T4.REFERENCE_CODE
  AND /* AND T4.ETL_DT = '${batch_date}' */ T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T4.SOURCE_NAME = 'TOMS'
  AND T4.REFERENCE_TYPE = 'STATE'
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
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
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
/* ==============[Group.16]============== */
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
  'KDI_' || T1.CLIENT_ID AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  T1.ADDRESS_TYPE AS ADDRESS_TYPE, /* None */
  TRIM(T1.ADDRESS_LINE_1) AS ADDRESS_LINE_1, /* NONE */
  TRIM(T1.ADDRESS_LINE_2) AS ADDRESS_LINE_2, /* NONE */
  TRIM(T1.ADDRESS_LINE_3) AS ADDRESS_LINE_3, /* NONE */
  TRIM(T1.ADDRESS_LINE_4) AS ADDRESS_LINE_4, /* NONE */
  TRIM(T1.CITY) AS CITY, /* NONE */
  CASE
    WHEN COALESCE(T1.STATE, '') <> ''
    THEN COALESCE(NULLIF(TRIM(T2.REFERENCE_VALUE_2), ''), TRIM(T1.STATE))
    WHEN COALESCE(T1.STATE, '') = '' AND T1.COUNTRY <> 'MYS'
    THEN 'NOT APPLICABLE'
    ELSE NULL
  END AS STATE, /* None */
  T1.POSTCODE AS POSTCODE, /* None */
  T1.country_code AS COUNTRY, /* None */
  T1.Record_Created_Date AS ADDRESS_CREATE_DATE, /* None */
  T1.Record_Updated_Date AS ADDRESS_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'KDI' AS SOURCE_NAME, /* None */
  T1.Client_ID AS SOURCE_RECORD_ID /* None */
FROM {params["com_schema"]}.T_KDI_CLIENTREPORT AS T1 /* None */
LEFT JOIN (
  SELECT DISTINCT
    REFERENCE_VALUE,
    REFERENCE_VALUE_2
  FROM {params["cur_schema"]}.REF_LOOKUP
  WHERE
    REFERENCE_TYPE = 'STATE'
    AND SOURCE_NAME = 'KDI'
    AND SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
) AS T2
  ON T1.STATE = T2.REFERENCE_VALUE
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDRESS_LINE_1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESS_LINE_2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESS_LINE_3, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESS_LINE_4, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
    OR TRIM(COALESCE(T1.postcode, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  )
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(rf"""
/* ==============[Group.17]============== */
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
  'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
  'REGISTERED' AS ADDRESS_TYPE, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_1) AS ADDRESS_LINE_1, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_2) AS ADDRESS_LINE_2, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_3) AS ADDRESS_LINE_3, /* NONE */
  NULL AS ADDRESS_LINE_4, /* NONE */
  TRIM(T1.CUSTOMER_CITY) AS CITY, /* NONE */
  IF(NOT T2.REFERENCE_CODE IS NULL, T2.REFERENCE_VALUE_2, 'NOT APPLICABLE') AS STATE, /* NONE */
  T1.CUSTOMER_POSTAL_CODE AS POSTCODE, /* NONE */
  T3.COUNTRY_CODE_3_DIGITS AS COUNTRY, /* NONE */
  NULL AS ADDRESS_CREATE_DATE, /* NONE */
  NULL AS ADDRESS_UPDATE_DATE, /* NONE */
  'EB' AS LINE_OF_BUSINESS, /* NONE */
  'SBL' AS SOURCE_NAME, /* NONE */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* NONE */
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2
  ON T1.CUSTOMER_STATE_CODE = T2.REFERENCE_CODE
  AND T2.REFERENCE_TYPE = 'STATE'
  AND T2.SOURCE_NAME = 'SBL'
  AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
/* AND T2.ETL_DT = '${batch_date}' */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM {params["cur_schema"]}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T1.CUSTOMER_COUNTRY = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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

spark.sql(rf"""
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
/* AND T2.ETL_DT = '${batch_date}' */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM {params["cur_schema"]}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T3 /* None */
  ON T1.CUSTOMER_COUNTRY = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T4
  ON T4.ETL_DT = '{batch_date}'
  AND T4.SOURCE_NAME = 'LMS'
  AND T4.ADDRESS_OWNER_TYPE = 'ACCOUNT'
  AND T4.ADDRESS_TYPE = 'REGISTERED'
  AND T1.ACCOUNT_NUMBER = T4.SOURCE_RECORD_ID
WHERE
  T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
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

spark.sql(rf"""
TRUNCATE TABLE   {params["cur_schema"]}.temp_dim_address_lmskibb2_tbl_counterparty
""")

spark.sql(rf"""
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
  AND T1.ETL_DT = '{batch_date}'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T4 /* None */
  ON T1.COUNTERPARTY_ADDRESS_COUNTRY_CODE = T4.COUNTRY_CODE_CCRIS AND T4.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T5
  ON T5.ETL_DT = '{batch_date}'
  AND T5.SOURCE_NAME = 'LMS'
  AND T5.ADDRESS_OWNER_TYPE = 'ACCOUNT'
  AND T5.ADDRESS_TYPE = 'REGISTERED'
  AND T0.ACCOUNT_NUMBER = T5.SOURCE_RECORD_ID
WHERE
  T0.ETL_DT = '{batch_date}'
  AND /* exclude collateral party */ NOT T0.FACILITY_TYPE_ID IN (7, 8)
""")

spark.sql(rf"""
/* ==============LMS consolidation ============== */
/* Insert */
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

spark.sql(rf"""
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
  AND T1.ETL_DT = '{batch_date}'
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
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '{batch_date}'
) AS T4 /* None */
  ON T1.CORRESPONDENCE_ADDRESS_COUNTRY_CODE = T4.COUNTRY_CODE_CCRIS AND T4.ROW_NUM = 1
LEFT JOIN {params["cur_schema"]}.DIM_ADDRESS AS T5
  ON T5.ETL_DT = '{batch_date}'
  AND T5.SOURCE_NAME = 'LMS'
  AND T5.ADDRESS_OWNER_TYPE = 'ACCOUNT'
  AND T5.ADDRESS_TYPE = 'MAILING'
  AND T0.ACCOUNT_NUMBER = T5.SOURCE_RECORD_ID
WHERE
  T0.ETL_DT = '{batch_date}'
  AND /* exclude collateral party */ NOT T0.FACILITY_TYPE_ID IN (7, 8)
""")

spark.sql(rf"""
/* ==============[Group 20: SBL counterparty mailing]============== */
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
  'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
  'MAILING' AS ADDRESS_TYPE, /* NONE */
  UPPER(TRIM(T4.mailing_address_1)) AS ADDRESS_LINE_1, /* NONE */
  UPPER(TRIM(T4.mailing_address_2)) AS ADDRESS_LINE_2, /* NONE */
  UPPER(TRIM(T4.mailing_address_3)) AS ADDRESS_LINE_3, /* NONE */
  NULL AS ADDRESS_LINE_4, /* NONE */
  UPPER(TRIM(T4.Mailing_Address_City)) AS CITY, /* NONE */
  T7.reference_value_2 AS STATE, /* NONE */
  UPPER(TRIM(T4.Mailing_Address_Postal_Code)) AS POSTCODE, /* NONE */
  T5.Country_Code_A3 AS COUNTRY, /* NONE */
  NULL AS ADDRESS_CREATE_DATE, /* NONE */
  GREATEST(
    COALESCE(T4.SYSTEM_UPDATED_DATETIME, '1900-01-01'),
    COALESCE(T4.LAST_ACTION_DATETIME, '1900-01-01')
  ) AS ADDRESS_UPDATE_DATE, /* NONE */
  'EB' AS LINE_OF_BUSINESS, /* NONE */
  'SBL' AS SOURCE_NAME, /* NONE */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
FROM {params["com_schema"]}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
INNER JOIN {params["com_schema"]}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T2 /* None */
  ON T1.PRIMARY_IDENTIFICATION_NO = T2.PRIMARY_IDENTIFICATION_NO
  AND T1.CUSTOMER_NAME = T2.CUSTOMER_NAME
  AND T2.ETL_DT = '{batch_date}'
  AND NOT T2.CLEAN_RULE_FLAG LIKE '%1%'
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_account AS T3
  ON T1.account_number = T3.account_number
  AND T3.etl_dt = '{batch_date}'
  AND T3.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T3.etl_dt, 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_counterparty AS T4
  ON T3.counterparty_id_1 = T4.counterparty_id
  AND T4.etl_dt = '{batch_date}'
  AND T4.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T4.etl_dt, 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_country AS T5
  ON T4.Mailing_Address_Country_Id = T5.country_id
  AND T5.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T5.etl_dt, 'yyyyMMdd')) BETWEEN T5.effective_from AND T5.effective_to
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_countrystate AS T6
  ON T4.Mailing_Address_Country_State_Id = T6.Country_State_Id
  AND T6.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T6.etl_dt, 'yyyyMMdd')) BETWEEN T6.effective_from AND T6.effective_to
LEFT JOIN {params["cur_schema"]}.ref_lookup AS T7
  ON T6.Country_State_Code = T7.reference_code
  AND T7.source_key = 'GENERAL_REFERENCE_LOOKUP'
  AND T7.source_name = 'SBL'
  AND T7.reference_type = 'STATE'
WHERE
  T1.etl_dt = '{batch_date}'
  AND (
    TRIM(COALESCE(T4.mailing_address_1, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_2, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_3, '')) <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_Postal_Code, '')) /*       TRIM(NVL(T3.Mailing_Address_4, '')) <> '' OR */ <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_City, '')) <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_Country_State_Id, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_country_id, '')) <> ''
  )
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(rf"""
/* ==============[Group.19] rak account_address ============== */
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
  T1.SOURCE_SYSTEM || '_' || T1.account_no AS OWNER_ID, /* NONE */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
  T1.customer_address_type AS ADDRESS_TYPE, /* NONE */
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
  T1.postcode AS POSTCODE, /* NONE */
  T1.COUNTRY AS COUNTRY, /* NONE */
  NULL AS ADDRESS_CREATE_DATE, /* NONE */
  NULL AS ADDRESS_UPDATE_DATE, /* NONE */
  NULL AS LINE_OF_BUSINESS, /* NONE */
  T1.SOURCE_SYSTEM AS SOURCE_NAME, /* NONE */
  T1.account_no AS SOURCE_RECORD_ID /* NONE */
FROM {params["com_schema"]}.T_RAK_CUSTOMER AS T1 /* NONE */
LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
  ON T1.STATE = T2.REFERENCE_CODE
  AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T2.SOURCE_NAME = 'RAK'
  AND T2.REFERENCE_TYPE = 'STATE'
  AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '{batch_date}'
  AND (
    TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE1, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE2, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE3, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE4, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T2.REFERENCE_VALUE, '')) <> ''
    OR TRIM(COALESCE(T1.postcode, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  )
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(rf"""
/* behind TEMP_DIM_ACCOUNT_ADDRESS */
/* ==============[Group.20]============== */
/* added 20250128 */
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
    T1.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
/* ==============[Group.21]============== */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS
""")

spark.sql(rf"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS (
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
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
)
""")

spark.sql(rf"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS (
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
  T1.CUSTOMER_ID AS OWNER_ID, /* None */
  'CUSTOMER' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
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
  NULL AS LINE_OF_BUSINESS, /* None */
  T1.SOURCE_NAME AS SOURCE_NAME, /* None */
  T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID /* None */
FROM (
  SELECT
    DC.CUSTOMER_ID,
    DA.*,
    ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DA.ADDRESS_UPDATE_DATE, DA.ADDRESS_CREATE_DATE) DESC) AS ROWNUM
  FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
  INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
    ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
    AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
    AND ACC.ETL_DT = '{batch_date}'
  INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS AS DA
    ON DA.OWNER_ID = ACC.ACCOUNT_ID
    AND DA.ADDRESS_OWNER_TYPE = 'ACCOUNT'
    AND DA.ADDRESS_TYPE = 'MAILING'
  WHERE
    DC.ETL_DT = '{batch_date}'
) AS T1 /* None */
WHERE
  T1.ROWNUM = 1
""")

spark.sql(rf"""
/* behind TEMP_DIM_ACCOUNT_ADDRESS */
/* ==============[Group.22]============== */
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS (
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
  T1.CUSTOMER_ID AS OWNER_ID, /* None */
  'CUSTOMER' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
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
  NULL AS LINE_OF_BUSINESS, /* None */
  T1.SOURCE_NAME AS SOURCE_NAME, /* None */
  T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID /* None */
FROM (
  SELECT
    DC.CUSTOMER_ID,
    DA.*,
    ROW_NUMBER() OVER (PARTITION BY DC.CUSTOMER_ID ORDER BY COALESCE(DA.ADDRESS_UPDATE_DATE, DA.ADDRESS_CREATE_DATE) DESC) AS ROWNUM
  FROM {params["cur_schema"]}.DIM_CUSTOMER AS DC
  INNER JOIN {params["cur_schema"]}.DIM_ACCOUNT AS ACC
    ON DC.CUSTOMER_ID = ACC.CUSTOMER_ID
    AND COALESCE(ACC.SUB_ACCOUNT_FLAG, 'N') <> 'Y'
    AND ACC.ETL_DT = '{batch_date}'
  INNER JOIN {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS AS DA
    ON DA.OWNER_ID = ACC.ACCOUNT_ID
    AND DA.ADDRESS_OWNER_TYPE = 'ACCOUNT'
    AND DA.ADDRESS_TYPE = 'REGISTERED'
  WHERE
    DC.ETL_DT = '{batch_date}'
) AS T1 /* None */
WHERE
  T1.ROWNUM = 1
""")

spark.sql(rf"""
/* ==============[Group.23]============== */
INSERT INTO {params["cur_schema"]}.DIM_ADDRESS PARTITION(etl_dt = '{batch_date}') (
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

spark.sql(rf"""
/* Delete all temporary tables */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")

spark.sql(rf"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")

spark.sql(rf"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
""")

spark.sql(rf"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS
""")


# Stop Spark when done
spark.stop()