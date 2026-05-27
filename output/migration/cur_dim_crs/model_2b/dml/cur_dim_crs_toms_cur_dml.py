
"""
Purpose:    curated - Snapshot table script
Purpose:    curated - Snapshot table script
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py yyyymmdd [file_name]
CreateDate: 2023-08-18 00:00:00
FileType:   DML
Logs:
Table name: DIM_CRS
Table comment: DIM_CRS
Creation date: 2023-08-18 00:00:00
Primary key field: CUSTOMER_ID
Attribution hierarchy: curated
Attribution subject: cust
Main application: None
Analyst: zhairuoping
Time granularity: None
Retention period: None
Descriptive information: None
lixiaotian      20240616     add Group.2.TOMS
version2:
lixiaotian       20240718        add Group.3, Group.4, Group.5
lixiaotian       20240626        add Group.6, Group.7, Group.8
marcoong         20250326        add new source: toms_eretail
gia dung         20251030        add cust_id + source name + tin_no consolidation logic
afiq azizi		 20260225        change KDI source from KDI Customer to KDI Clientreport
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, lit, to_date

source_name = "TOMS"
table_name  = "dim_crs"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Define snapshot-specific variables
snapshot_date_str = batch_date # Or yesterday_date, depending on logic
snapshot_year_month = snapshot_date_str[:6]

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CRS
""")
spark.sql(f"""
/* Create temp table */
    CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CRS (
      CUSTOMER_ID VARCHAR(20) COMMENT '',
      CRS_ENTITY_TYPE VARCHAR(2) COMMENT '',
      NAME_OF_CONTROLLING_PERSON_1 VARCHAR(100) COMMENT '',
      NAME_OF_CONTROLLING_PERSON_2 VARCHAR(100) COMMENT '',
      CRS_COUNTRY VARCHAR(2) COMMENT '',
      CRS_TAX_RESIDENCE VARCHAR(2) COMMENT '',
      TAXPAYER_IDENTIFICATION_NO_1 VARCHAR(20) COMMENT '',
      TAXPAYER_IDENTIFICATION_NO_2 VARCHAR(20) COMMENT '',
      TIN_UNAVAILABLE_REASON VARCHAR(2) COMMENT '',
      TIN_REMARKS VARCHAR(200) COMMENT '',
      SOURCE_NAME VARCHAR(10) COMMENT '',
      SOURCE_RECORD_ID VARCHAR(20) COMMENT '',
      ETL_TIMESTAMP STRING COMMENT 'ETL_PROCESSING_TIME',
      PRIORITY_LEVEL INT COMMENT '',
      SOURCE_UPDATE_DATE TIMESTAMP
    )
""")
spark.sql(f"""
/* ==============[Group.2 TOMS eCorporate]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CRS (
      CUSTOMER_ID, /* None */
      CRS_ENTITY_TYPE, /* None */
      NAME_OF_CONTROLLING_PERSON_1, /* None */
      NAME_OF_CONTROLLING_PERSON_2, /* None */
      CRS_COUNTRY, /* None */
      CRS_TAX_RESIDENCE, /* None */
      TAXPAYER_IDENTIFICATION_NO_1, /* None */
      TAXPAYER_IDENTIFICATION_NO_2, /* None */
      TIN_UNAVAILABLE_REASON, /* None */
      TIN_REMARKS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      PRIORITY_LEVEL, /* None */
      SOURCE_UPDATE_DATE /* 20251030 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS CRS_ENTITY_TYPE, /* None */
      NULL AS NAME_OF_CONTROLLING_PERSON_1, /* None */
      NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
      NULL AS CRS_COUNTRY, /* None */
      NULL AS CRS_TAX_RESIDENCE, /* None */
      T1.TINNO AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
      NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
      NULL AS TIN_UNAVAILABLE_REASON, /* None */
      NULL AS TIN_REMARKS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      NULL AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      5 AS PRIORITY_LEVEL,
      CAST(GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_TOMS_ECORPORATE_CIF AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TINNO, '')) <> ''
""")
spark.sql(f"""
/* ==============[Group.3 TOMS eRetail]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CRS (
      CUSTOMER_ID, /* None */
      CRS_ENTITY_TYPE, /* None */
      NAME_OF_CONTROLLING_PERSON_1, /* None */
      NAME_OF_CONTROLLING_PERSON_2, /* None */
      CRS_COUNTRY, /* None */
      CRS_TAX_RESIDENCE, /* None */
      TAXPAYER_IDENTIFICATION_NO_1, /* None */
      TAXPAYER_IDENTIFICATION_NO_2, /* None */
      TIN_UNAVAILABLE_REASON, /* None */
      TIN_REMARKS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      PRIORITY_LEVEL, /* None */
      SOURCE_UPDATE_DATE /* 20251030 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS CRS_ENTITY_TYPE, /* None */
      NULL AS NAME_OF_CONTROLLING_PERSON_1, /* None */
      NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
      NULL AS CRS_COUNTRY, /* None */
      NULL AS CRS_TAX_RESIDENCE, /* None */
      T1.TINNO AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
      NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
      NULL AS TIN_UNAVAILABLE_REASON, /* None */
      NULL AS TIN_REMARKS, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      NULL AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      5 AS PRIORITY_LEVEL,
      CAST(GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_TOMS_ERETAIL_CIF AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.TINNO, '')) <> ''
""")

# ─── TRANSFORMED SNAPSHOT TABLE ──────────────────────────────────────────────
# This table holds the new snapshot data, transformed from COM_T
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_toms_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_toms_delta (
        customer_id VARCHAR(20)
        , crs_entity_type VARCHAR(2)
        , name_of_controlling_person_1 VARCHAR(100)
        , name_of_controlling_person_2 VARCHAR(100)
        , crs_country VARCHAR(2)
        , crs_tax_residence VARCHAR(2)
        , taxpayer_identification_no_1 VARCHAR(20)
        , taxpayer_identification_no_2 VARCHAR(20)
        , tin_unavailable_reason VARCHAR(2)
        , tin_remarks VARCHAR(200)
        , source_record_id VARCHAR(20)
        , priority_level INT
        , source_update_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# The main_processing_sqls for Model 2B cur is expected to read from com_schema and perform transformations
# ─── INSERT INTO CONSOLIDATED TABLE ────────────────────────────────────────────────────────

spark.sql(f"""
ALTER TABLE {params["cur_schema"]}.temp_DIM_CRS_main_consolidated DROP IF EXISTS
""")

spark.sql(f"""
/* Insert into curated */
    WITH CRS_ROW_NUM AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID, TAXPAYER_IDENTIFICATION_NO_1 ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
      FROM {params["cur_schema"]}.TEMP_DIM_CRS
    )
    INSERT INTO {params["cur_schema"]}.temp_DIM_CRS_main_consolidated (
      CUSTOMER_ID, /* None */
      CRS_ENTITY_TYPE, /* None */
      NAME_OF_CONTROLLING_PERSON_1, /* None */
      NAME_OF_CONTROLLING_PERSON_2, /* None */
      CRS_COUNTRY, /* None */
      CRS_TAX_RESIDENCE, /* None */
      TAXPAYER_IDENTIFICATION_NO_1, /* None */
      TAXPAYER_IDENTIFICATION_NO_2, /* None */
      TIN_UNAVAILABLE_REASON, /* None */
      TIN_REMARKS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      PRIORITY_LEVEL, /* None */
      SOURCE_UPDATE_DATE /* 20251030 */
    )
    SELECT
      CUSTOMER_ID, /* None */
      CRS_ENTITY_TYPE, /* None */
      NAME_OF_CONTROLLING_PERSON_1, /* None */
      NAME_OF_CONTROLLING_PERSON_2, /* None */
      CRS_COUNTRY, /* None */
      CRS_TAX_RESIDENCE, /* None */
      TAXPAYER_IDENTIFICATION_NO_1, /* None */
      TAXPAYER_IDENTIFICATION_NO_2, /* None */
      TIN_UNAVAILABLE_REASON, /* None */
      TIN_REMARKS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      PRIORITY_LEVEL, /* None */
      SOURCE_UPDATE_DATE /* 20251030 */
    FROM CRS_ROW_NUM
    WHERE
      RN = 1
""")


# ─── FINAL UPDATED TABLE SETUP ───────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_toms_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_toms_updated (
        snapshot_date DATE,
        customer_id VARCHAR(20),
        crs_entity_type VARCHAR(2),
        name_of_controlling_person_1 VARCHAR(100),
        name_of_controlling_person_2 VARCHAR(100),
        crs_country VARCHAR(2),
        crs_tax_residence VARCHAR(2),
        taxpayer_identification_no_1 VARCHAR(20),
        taxpayer_identification_no_2 VARCHAR(20),
        tin_unavailable_reason VARCHAR(2),
        tin_remarks VARCHAR(200),
        source_record_id VARCHAR(20),
        priority_level INT,
        source_update_date TIMESTAMP,
        dl_record_created_date TIMESTAMP,
        dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep records from other snapshot dates within the same month ───
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_toms_updated
    SELECT
        snapshot_date,
        customer_id,
        crs_entity_type,
        name_of_controlling_person_1,
        name_of_controlling_person_2,
        crs_country,
        crs_tax_residence,
        taxpayer_identification_no_1,
        taxpayer_identification_no_2,
        tin_unavailable_reason,
        tin_remarks,
        source_record_id,
        priority_level,
        source_update_date,
        dl_record_created_date,
        dl_record_updated_date
    FROM {params["cur_schema"]}.dim_crs_toms
    WHERE year_month = '{snapshot_year_month}'
      AND snapshot_date != to_date('{snapshot_date_str}', 'yyyyMMdd')
AND source_key = 'TOMS'""")

# ─── STEP 2: Insert the new snapshot data from COM_T ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_toms_updated
    SELECT
        to_date('{snapshot_date_str}', 'yyyyMMdd') AS snapshot_date,
        delta.customer_id,
        delta.crs_entity_type,
        delta.name_of_controlling_person_1,
        delta.name_of_controlling_person_2,
        delta.crs_country,
        delta.crs_tax_residence,
        delta.taxpayer_identification_no_1,
        delta.taxpayer_identification_no_2,
        delta.tin_unavailable_reason,
        delta.tin_remarks,
        delta.source_record_id,
        delta.priority_level,
        delta.source_update_date,
        current_timestamp() AS dl_record_created_date,
        current_timestamp() AS dl_record_updated_date
    FROM {params["com_schema"]}.temp_dim_crs_toms_delta delta
""")

# ─── STEP 3: Overwrite the year_month partition with the updated data ───────
spark.sql(f"""
    SET spark.sql.sources.partitionOverwriteMode=dynamic
""")
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_crs_toms
    PARTITION (year_month, source_key)
    SELECT
        snapshot_date,
        customer_id,
        crs_entity_type,
        name_of_controlling_person_1,
        name_of_controlling_person_2,
        crs_country,
        crs_tax_residence,
        taxpayer_identification_no_1,
        taxpayer_identification_no_2,
        tin_unavailable_reason,
        tin_remarks,
        source_record_id,
        priority_level,
        source_update_date,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp,
        '{snapshot_year_month}' AS year_month
,'TOMS' AS source_key    FROM {params["com_schema"]}.temp_dim_crs_toms_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_crs_toms COMPUTE STATISTICS
""")

spark.stop()