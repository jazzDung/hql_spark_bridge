
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
from pyspark.sql.functions import current_timestamp

source_name = "SBL"
table_name  = "dim_crs"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites if there is a partition key
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── SOURCE PROCESSING (Temp tables logic from legacy script) ───────────────────
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
/* ==============[Group.7 SBL]============== */
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
      T1.cust_id AS CUSTOMER_ID,
      NULL AS CRS_ENTITY_TYPE,
      NULL AS NAME_OF_CONTROLLING_PERSON_1,
      NULL AS NAME_OF_CONTROLLING_PERSON_2,
      T4.Country_Code AS CRS_COUNTRY,
      CASE
        WHEN T3.Country_Jurisdiction_Of_Resident_Declaration_Id < 10
        THEN LPAD(CAST(T3.Country_Jurisdiction_Of_Resident_Declaration_Id AS STRING), 2, '0')
        ELSE CAST(T3.Country_Jurisdiction_Of_Resident_Declaration_Id AS STRING)
      END AS CRS_TAX_RESIDENCE,
      T1.customer_tin AS TAXPAYER_IDENTIFICATION_NO_1,
      NULL AS TAXPAYER_IDENTIFICATION_NO_2,
      NULL AS TIN_UNAVAILABLE_REASON,
      NULL AS tin_remarks,
      'SBL' AS source_name,
      T1.account_number AS source_record_id,
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      9 AS priority_level,
      COALESCE(T1.LAST_GENERATED_DATETIME, '1900-01-01') AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T1
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_account AS T2
      ON T1.account_number = T2.account_number
      AND T2.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T2.effective_from AND T2.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_counterparty AS T3
      ON T2.counterparty_id_1 = T3.counterparty_id
      AND T3.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T3.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_country AS T4
      ON T3.Country_Of_Birth_Country_Id = T4.country_id
      AND T4.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T4.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
      AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
      AND TRIM(COALESCE(T1.PRIMARY_IDENTIFICATION_TYPE, '')) <> ''
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
# This table will hold the transformed delta records from the COM layer.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_crs_sbl_delta""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_crs_sbl_delta (
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
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── MAIN PROCESSING (Populate Delta Table) ───────────────────────────────────
# The main_processing_sqls should contain the logic to transform and insert data
# from com_t into the temp delta table, filtering for the current batch_date.
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

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
# This table will hold the final, merged dataset.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_crs_sbl_updated""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_crs_sbl_updated (
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
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from CUR ──────────────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_crs_sbl_updated
    SELECT
        cur.customer_id,
        cur.crs_entity_type,
        cur.name_of_controlling_person_1,
        cur.name_of_controlling_person_2,
        cur.crs_country,
        cur.crs_tax_residence,
        cur.taxpayer_identification_no_1,
        cur.taxpayer_identification_no_2,
        cur.tin_unavailable_reason,
        cur.tin_remarks,
        cur.source_record_id,
        cur.priority_level,
        cur.source_update_date,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_crs_sbl cur
    WHERE cur.source_key = 'SBL' AND         NOT EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_crs_sbl_delta delta
            WHERE delta.SOURCE_NAME = cur.SOURCE_NAME AND delta.CUSTOMER_ID = cur.CUSTOMER_ID AND delta.CRS_COUNTRY = cur.CRS_COUNTRY AND delta.TAXPAYER_IDENTIFICATION_NO_1 = cur.TAXPAYER_IDENTIFICATION_NO_1        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_crs_sbl_updated
    SELECT
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
        delta.dl_record_status,
        CASE
            WHEN cur.SOURCE_NAME IS NOT NULL THEN cur.dl_record_created_date
            ELSE delta.dl_record_created_date
        END AS dl_record_created_date,
        delta.dl_record_updated_date
    FROM {params["tmp_schema"]}.temp_dim_crs_sbl_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_crs_sbl cur
        ON cur.source_key = 'SBL' AND delta.SOURCE_NAME = cur.SOURCE_NAME AND delta.CUSTOMER_ID = cur.CUSTOMER_ID AND delta.CRS_COUNTRY = cur.CRS_COUNTRY AND delta.TAXPAYER_IDENTIFICATION_NO_1 = cur.TAXPAYER_IDENTIFICATION_NO_1""")

# ─── STEP 3: Insert previous state of changed records into History table ─────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_crs_sbl_h PARTITION (source_key, hist_year)
    SELECT
        cur.customer_id,
        cur.crs_entity_type,
        cur.name_of_controlling_person_1,
        cur.name_of_controlling_person_2,
        cur.crs_country,
        cur.crs_tax_residence,
        cur.taxpayer_identification_no_1,
        cur.taxpayer_identification_no_2,
        cur.tin_unavailable_reason,
        cur.tin_remarks,
        cur.source_record_id,
        cur.priority_level,
        cur.source_update_date,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'SBL' AS source_key        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.dim_crs_sbl cur
    WHERE cur.source_key = 'SBL' AND         EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_crs_sbl_delta delta
            WHERE delta.SOURCE_NAME = cur.SOURCE_NAME AND delta.CUSTOMER_ID = cur.CUSTOMER_ID AND delta.CRS_COUNTRY = cur.CRS_COUNTRY AND delta.TAXPAYER_IDENTIFICATION_NO_1 = cur.TAXPAYER_IDENTIFICATION_NO_1        )
""")

# ─── STEP 4: Overwrite CUR table ──────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_crs_sbl PARTITION (source_key = 'SBL')    SELECT
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
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["tmp_schema"]}.temp_dim_crs_sbl_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_crs_sbl PARTITION (source_key = 'SBL') COMPUTE STATISTICS
""")

spark.stop()