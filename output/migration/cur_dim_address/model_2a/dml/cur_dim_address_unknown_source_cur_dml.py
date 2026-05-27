
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

source_name = "UNKNOWN_SOURCE"
table_name  = "dim_address"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)


# ─── DELTA TABLE SETUP (TRANSFORMATIONS) ─────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_address_unknown_source_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_address_unknown_source_delta (
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
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# The main_processing_sqls for Model 2A cur is expected to read from com_schema and perform transformations
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


# ─── STEP 1: Overwrite target table with transformed data ───────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_unknown_source PARTITION (source_key = 'UNKNOWN_SOURCE')    SELECT
        t.owner_id
        , t.address_owner_type
        , t.address_type
        , t.address_line_1
        , t.address_line_2
        , t.address_line_3
        , t.address_line_4
        , t.city
        , t.state
        , t.postcode
        , t.country
        , t.address_create_date
        , t.address_update_date
        , t.line_of_business
        , t.source_record_id
        , current_timestamp() AS dl_record_created_date
        , current_timestamp() AS dl_record_updated_date
        , '{batch_date}'      AS etl_dt
        , current_timestamp() AS etl_timestamp
    FROM {params["com_schema"]}.temp_dim_address_unknown_source_delta t
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_unknown_source PARTITION (source_key = 'UNKNOWN_SOURCE') COMPUTE STATISTICS
""")

spark.stop()