
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

source_name = "MHBOS"
table_name  = "dim_crs"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

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
/* ==============[Group.1]============== */
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
    SELECT DISTINCT
      a.CUST_ID AS CUSTOMER_ID, /* None */
      b.CRS_TAX_TYPE AS CRS_ENTITY_TYPE, /* None */
      b.CONTROLLING_NAME AS NAME_OF_CONTROLLING_PERSON_1, /* None */
      NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
      b.COUNTRY_TAX_RESIDENCE AS CRS_COUNTRY, /* None */
      b.CRS_TAX_TYPE AS CRS_TAX_RESIDENCE, /* None */
      b.TAX_IDENTIFICATION_NO AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
      NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
      b.REASON AS TIN_UNAVAILABLE_REASON, /* None */
      b.REASON_REMARKS AS TIN_REMARKS, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      a.client_no AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      1 AS PRIORITY_LEVEL,
      CAST(COALESCE(a.DATE_CHANGE, '1900-01-01') AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM (
      SELECT
        T1.client_no,
        T2.cust_id,
        T1.DATE_CHANGE,
        ROW_NUMBER() OVER (PARTITION BY T2.cust_id ORDER BY CASE WHEN T1.type_of_account <> 'F' THEN 0 ELSE 1 END, GREATEST(COALESCE(T1.date_created, '1900-01-01'), COALESCE(T1.DATE_CHANGE, '1900-01-01')) DESC) AS RN
      FROM {params["com_schema"]}.T_MHBOS_M_CLIENT AS T1
      INNER JOIN {params["com_schema"]}.m_customer_id_mapping AS T2
        ON T1.client_no = T2.source_owner_id
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
    ) AS a
    JOIN (
      SELECT
        crs.*
      FROM {params["com_schema"]}.t_mhbos_m_client_crs AS crs
      WHERE
        NOT EXISTS(
          SELECT
            client_no,
            country_tax_residence,
            COUNT(*)
          FROM {params["com_schema"]}.t_mhbos_m_client_crs AS dup_crs
          WHERE
            crs.client_no = dup_crs.client_no
            AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
            AND TRIM(COALESCE(crs_tax_type, '')) <> ''
            AND TRIM(COALESCE(tax_identification_no, '')) <> ''
            AND TRIM(COALESCE(country_tax_residence, '')) <> ''
          GROUP BY
            client_no,
            country_tax_residence
          HAVING
            COUNT(*) > 1
        )
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND TRIM(COALESCE(crs.crs_tax_type, '')) <> ''
        AND TRIM(COALESCE(crs.tax_identification_no, '')) <> ''
        AND TRIM(COALESCE(crs.country_tax_residence, '')) <> ''
    ) AS b
      ON b.client_no = a.client_no
    WHERE
      RN = 1
""")

# ─── DELTA TABLE SETUP (EXTRACT IMPACTED DATES FROM COM_T) ──────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_mhbos_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_mhbos_delta (
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
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_mhbos_delta
    SELECT
        com_t.customer_id,
        com_t.crs_entity_type,
        com_t.name_of_controlling_person_1,
        com_t.name_of_controlling_person_2,
        com_t.crs_country,
        com_t.crs_tax_residence,
        com_t.taxpayer_identification_no_1,
        com_t.taxpayer_identification_no_2,
        com_t.tin_unavailable_reason,
        com_t.tin_remarks,
        com_t.source_record_id,
        com_t.priority_level,
        com_t.source_update_date,
        com_t.<<partition_column>>
    FROM {params["com_schema"]}.dim_crs_mhbos com_t
    INNER JOIN (
        SELECT DISTINCT <<key_date_column>>
        FROM {params["com_schema"]}.dim_crs_mhbos
        WHERE DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) delta_date ON com_t.<<key_date_column>> = delta_date.<<key_date_column>>
    WHERE com_t.dl_record_status = 'A'
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_mhbos_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_mhbos_updated (
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
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep existing records from CUR from IMPACTED PARTITIONS ONLY ───
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_mhbos_updated
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
        cur.dl_record_created_date,
        cur.dl_record_updated_date
        , cur.<<partition_column>>
    FROM {params["cur_schema"]}.dim_crs_mhbos cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_dim_crs_mhbos_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.source_key = 'MHBOS' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_crs_mhbos_delta delta
            WHERE delta.<<key_date_column>> = cur.<<key_date_column>>
 AND delta.<<partition_column>> = cur.<<partition_column>>        )
""")

# ─── STEP 2: Insert transformed delta into temp table ────────────────────────
# main_processing_sqls is expected to transform data from temp_{target_table_name}_delta
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


# ─── STEP 3: Overwrite CUR table dynamically ─────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_crs_mhbos PARTITION (source_key = 'MHBOS', <<partition_column>>)
    SELECT
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
        current_timestamp() AS etl_timestamp
        , <<partition_column>>
    FROM {params["com_schema"]}.temp_dim_crs_mhbos_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_crs_mhbos COMPUTE STATISTICS
""")

spark.stop()