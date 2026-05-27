
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

source_name = "KDI"
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
/* ==============[Group.4 KDI]============== */
    WITH m_base AS (
      SELECT
        m.*,
        REGEXP_REPLACE(COALESCE(m.TIN_NUMBERS, ''), '\\s*,\\s*', ',') AS tin_norm,
        REGEXP_REPLACE(COALESCE(m.country_of_tax_residences_code, ''), '\\s*,\\s*', ',') AS ctry_code_norm
      FROM {params["com_schema"]}.M_KDI_CLIENTREPORT AS m
      WHERE
        DATE_FORMAT(m.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ), m_arr AS (
      SELECT
        b.*,
        SPLIT(b.tin_norm, ',') AS tin_arr,
        SPLIT(b.ctry_code_norm, ',') AS ctry_arr,
        SIZE(SPLIT(b.tin_norm, ',')) AS tin_len,
        SIZE(SPLIT(b.ctry_code_norm, ',')) AS ctry_len
      FROM m_base AS b
    ), m_expanded AS (
      SELECT
        a.ACCOUNT_NO,
        a.CLIENT_ID,
        a.NAME,
        a.IDENTIFICATION_TYPE,
        a.NRIC_PASSPORT,
        a.SECONDARY_ID_NO_TYPE,
        a.SECONDARY_ID_NO,
        a.NATIONALITY,
        a.NATIONALITY_CODE,
        a.COUNTRY_OF_RESIDENCE,
        a.COUNTRY_OF_RESIDENCE_CODE,
        a.COMPANY_PLACE_INCORPORATION,
        a.COMPANY_PLACE_BUSINESS,
        a.OCCUPATION,
        a.BUSINESS_TYPE_INDUSTRY,
        a.NET_WORTH,
        a.SST_REGISTRATION_NUMBER,
        a.Record_Updated_Date,
        tin_lv.pos AS pos_tin,
        TRIM(tin_lv.tin) AS tin_raw,
        REGEXP_REPLACE(TRIM(tin_lv.tin), '[^0-9A-Za-z]', '') AS tin_clean,
        UPPER(TRIM(a.ctry_arr[tin_lv.pos])) AS country_code_raw,
        UPPER(TRIM(a.ctry_arr[tin_lv.pos])) AS country_code_upper,
        a.ETL_TIMESTAMP
      FROM m_arr AS a
      LATERAL VIEW
      POSEXPLODE(a.tin_arr) tin_lv AS pos, tin
      WHERE
        tin_lv.pos < a.ctry_len
        AND TRIM(COALESCE(tin_lv.tin, '')) <> ''
        AND TRIM(COALESCE(a.ctry_arr[tin_lv.pos], '')) <> ''
        AND REGEXP_REPLACE(TRIM(tin_lv.tin), '[^0-9A-Za-z]', '') <> ''
    ), ref_ccris AS (
      SELECT
        UPPER(TRIM(y.country_code_ccris)) AS country_code_ccris,
        y.country_name,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(y.country_code_ccris)) ORDER BY y.country_name) AS rn
      FROM {params["cur_schema"]}.ref_country AS y
      WHERE
        DATE_FORMAT(y.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ), ref_ccris_dedup AS (
      SELECT
        country_code_ccris,
        country_name
      FROM ref_ccris
      WHERE
        rn = 1
    )
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
      m.CUST_ID AS CUSTOMER_ID,
      NULL AS CRS_ENTITY_TYPE,
      NULL AS NAME_OF_CONTROLLING_PERSON_1,
      NULL AS NAME_OF_CONTROLLING_PERSON_2,
      COALESCE(rA.country_code_ccris, me.country_code_upper) AS CRS_COUNTRY,
      NULL AS CRS_TAX_RESIDENCE,
      me.tin_clean AS TAXPAYER_IDENTIFICATION_NO_1,
      NULL AS TAXPAYER_IDENTIFICATION_NO_2,
      NULL AS TIN_UNAVAILABLE_REASON,
      NULL AS TIN_REMARKS,
      'KDI' AS SOURCE_NAME,
      m.CLIENT_ID AS SOURCE_RECORD_ID,
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      6 AS PRIORITY_LEVEL,
      m.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
    FROM m_base AS m
    JOIN m_expanded AS me
      ON m.CLIENT_ID = me.CLIENT_ID
    LEFT JOIN ref_ccris_dedup AS rA
      ON me.country_code_upper = rA.country_code_ccris
    WHERE
      NOT m.IDENTIFICATION_TYPE LIKE '@[%'
      AND TRIM(COALESCE(m.IDENTIFICATION_TYPE, '')) <> ''
""")

# ─── DELTA TABLE SETUP (EXTRACT IMPACTED primary_key FROM COM_T) ───────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_kdi_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_kdi_delta (
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
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_kdi_delta
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
    FROM {params["com_schema"]}.dim_crs_kdi com_t
    INNER JOIN (
        SELECT DISTINCT SOURCE_NAME,CUSTOMER_ID,CRS_COUNTRY,TAXPAYER_IDENTIFICATION_NO_1        FROM {params["com_schema"]}.dim_crs_kdi
        WHERE DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) delta_key
    ON com_t.SOURCE_NAME = delta_key.SOURCE_NAME AND com_t.CUSTOMER_ID = delta_key.CUSTOMER_ID AND com_t.CRS_COUNTRY = delta_key.CRS_COUNTRY AND com_t.TAXPAYER_IDENTIFICATION_NO_1 = delta_key.TAXPAYER_IDENTIFICATION_NO_1    WHERE com_t.dl_record_status = 'A'
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_kdi_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_kdi_updated (
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
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_kdi_updated
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
    FROM {params["cur_schema"]}.dim_crs_kdi cur
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_dim_crs_kdi_delta
    ) impacted_partitions
    ON cur.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
cur.source_key = 'KDI' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_crs_kdi_delta delta
            WHERE delta.SOURCE_NAME = cur.SOURCE_NAME AND delta.CUSTOMER_ID = cur.CUSTOMER_ID AND delta.CRS_COUNTRY = cur.CRS_COUNTRY AND delta.TAXPAYER_IDENTIFICATION_NO_1 = cur.TAXPAYER_IDENTIFICATION_NO_1 AND delta.<<partition_column>> = cur.<<partition_column>>        )
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
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_crs_kdi PARTITION (source_key = 'KDI', <<partition_column>>)
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
    FROM {params["com_schema"]}.temp_dim_crs_kdi_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_crs_kdi COMPUTE STATISTICS
""")

spark.stop()