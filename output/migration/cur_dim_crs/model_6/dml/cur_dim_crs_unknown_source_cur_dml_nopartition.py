
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

source_name = "UNKNOWN_SOURCE"
table_name  = "dim_crs"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)


# ─── DELTA TABLE SETUP (EXTRACT IMPACTED primary_key FROM COM_T) ───────────────────
# Find all distinct key columns where records were updated in the current batch
# Then pull ALL active records from COM_T for those primary_key.
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_unknown_source_delta""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_unknown_source_delta (
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

spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_unknown_source_delta
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
        com_t.source_update_date
    FROM {params["com_schema"]}.dim_crs_unknown_source com_t
    INNER JOIN (
        SELECT DISTINCT SOURCE_NAME,CUSTOMER_ID,CRS_COUNTRY,TAXPAYER_IDENTIFICATION_NO_1        FROM {params["com_schema"]}.dim_crs_unknown_source
        WHERE DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) delta_key
    ON com_t.SOURCE_NAME = delta_key.SOURCE_NAME AND com_t.CUSTOMER_ID = delta_key.CUSTOMER_ID AND com_t.CRS_COUNTRY = delta_key.CRS_COUNTRY AND com_t.TAXPAYER_IDENTIFICATION_NO_1 = delta_key.TAXPAYER_IDENTIFICATION_NO_1    WHERE com_t.dl_record_status = 'A'
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_dim_crs_unknown_source_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_dim_crs_unknown_source_updated (
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
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep existing records from CUR that are NOT in impacted primary_key ──
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_dim_crs_unknown_source_updated
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
    FROM {params["cur_schema"]}.dim_crs_unknown_source cur
    WHERE
cur.source_key = 'UNKNOWN_SOURCE' AND         NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_dim_crs_unknown_source_delta delta
            WHERE delta.SOURCE_NAME = cur.SOURCE_NAME AND delta.CUSTOMER_ID = cur.CUSTOMER_ID AND delta.CRS_COUNTRY = cur.CRS_COUNTRY AND delta.TAXPAYER_IDENTIFICATION_NO_1 = cur.TAXPAYER_IDENTIFICATION_NO_1        )
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


# ─── STEP 3: Overwrite CUR table ─────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_crs_unknown_source PARTITION (source_key = 'UNKNOWN_SOURCE')    SELECT
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
    FROM {params["com_schema"]}.temp_dim_crs_unknown_source_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_crs_unknown_source PARTITION (source_key = 'UNKNOWN_SOURCE') COMPUTE STATISTICS
""")

spark.stop()