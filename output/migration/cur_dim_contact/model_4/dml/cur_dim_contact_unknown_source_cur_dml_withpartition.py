
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

source_name = "UNKNOWN_SOURCE"
table_name  = "DIM_CONTACT_UNKNOWN_SOURCE"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")


# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta (
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
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_updated (
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
    INSERT INTO TABLE {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_updated
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
    FROM {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.['source_key'] = 'UNKNOWN_SOURCE' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_updated
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
    FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta delta
    LEFT JOIN {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE cur
        ON cur.['source_key'] = 'UNKNOWN_SOURCE' AND delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE""")

# ─── STEP 3: Insert previous state of changed records into History table ─────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE_h PARTITION (['source_key'], <<partition_column>>, hist_year)
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
, 'UNKNOWN_SOURCE' AS ['source_key']        , cur.<<partition_column>>
        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.['source_key'] = 'UNKNOWN_SOURCE' AND         EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.CONTACT_OWNER_TYPE = cur.CONTACT_OWNER_TYPE AND delta.CONTACT_TYPE = cur.CONTACT_TYPE        )
""")

# ─── STEP 4: Overwrite CUR table impacted partitions dynamically ─────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE PARTITION (['source_key'] = 'UNKNOWN_SOURCE', <<partition_column>>)
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
    FROM {params["com_schema"]}.temp_DIM_CONTACT_UNKNOWN_SOURCE_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.DIM_CONTACT_UNKNOWN_SOURCE COMPUTE STATISTICS
""")

spark.stop()