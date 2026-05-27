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

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

source_name = "dim"
table_name = "crs"
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
/* Delete all temporary tables */
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CRS
""")

spark.sql(rf"""
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

spark.sql(rf"""
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
    T1.etl_dt = '{batch_date}' AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
) AS a
JOIN (
  SELECT
    crs.*
  FROM {params["com_schema"]}.r_mhbos_m_client_crs AS crs
  WHERE
    NOT EXISTS(
      SELECT
        client_no,
        country_tax_residence,
        COUNT(*)
      FROM {params["com_schema"]}.r_mhbos_m_client_crs AS dup_crs
      WHERE
        crs.client_no = dup_crs.client_no
        AND etl_dt = '{batch_date}'
        AND TRIM(COALESCE(crs_tax_type, '')) <> ''
        AND TRIM(COALESCE(tax_identification_no, '')) <> ''
        AND TRIM(COALESCE(country_tax_residence, '')) <> ''
      GROUP BY
        client_no,
        country_tax_residence
      HAVING
        COUNT(*) > 1
    )
    AND etl_dt = '{batch_date}'
    AND TRIM(COALESCE(crs.crs_tax_type, '')) <> ''
    AND TRIM(COALESCE(crs.tax_identification_no, '')) <> ''
    AND TRIM(COALESCE(crs.country_tax_residence, '')) <> ''
) AS b
  ON b.client_no = a.client_no
WHERE
  RN = 1
""")

spark.sql(rf"""
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
  T1.ETL_DT = '{batch_date}' AND TRIM(COALESCE(T1.TINNO, '')) <> ''
""")

spark.sql(rf"""
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
  T1.ETL_DT = '{batch_date}' AND TRIM(COALESCE(T1.TINNO, '')) <> ''
""")

spark.sql(rf"""
/* ==============[Group.4 KDI]============== */
WITH m_base AS (
  SELECT
    m.*,
    REGEXP_REPLACE(COALESCE(m.TIN_NUMBERS, ''), '\\s*,\\s*', ',') AS tin_norm,
    REGEXP_REPLACE(COALESCE(m.country_of_tax_residences_code, ''), '\\s*,\\s*', ',') AS ctry_code_norm
  FROM {params["com_schema"]}.M_KDI_CLIENTREPORT AS m
  WHERE
    m.ETL_DT = '{batch_date}'
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
    y.ETL_DT = '{batch_date}'
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

spark.sql(rf"""
/* /*==============[Group.6 SMF]==============*/ */
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
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  'SMF' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  8 AS PRIORITY_LEVEL,
  COALESCE(T1.LAST_GENERATED_DATETIME, '1900-01-01') AS SOURCE_UPDATE_DATE
FROM {params["com_schema"]}.M_SMF_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")

spark.sql(rf"""
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
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T2.etl_dt, 'yyyyMMdd')) BETWEEN T2.effective_from AND T2.effective_to
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_counterparty AS T3
  ON T2.counterparty_id_1 = T3.counterparty_id
  AND T3.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T3.etl_dt, 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_country AS T4
  ON T3.Country_Of_Birth_Country_Id = T4.country_id
  AND T4.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T4.etl_dt, 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND TRIM(COALESCE(T1.PRIMARY_IDENTIFICATION_TYPE, '')) <> ''
""")

spark.sql(rf"""
/* ==============[Group.8 LMS]============== */
WITH COUNTERPARTYCHILD2 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY PARENT_FUNCTION_RECORD_ID ORDER BY RECORD_ID) AS RN
  FROM {params["com_schema"]}.t_lmskibb2_tbl_counterpartychild2
  WHERE
    ETL_DT = '{batch_date}'
), LMS_TBL_EINVOICING_CLIENTDATA_CRS AS (
  SELECT
    T1.CUST_ID AS CUSTOMER_ID, /* None */
    NULL AS CRS_ENTITY_TYPE, /* None */
    NULL AS NAME_OF_CONTROLLING_PERSON_1, /* None */
    NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
    NULL AS CRS_COUNTRY, /* None */
    NULL AS CRS_TAX_RESIDENCE, /* None */
    T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
    NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
    NULL AS TIN_UNAVAILABLE_REASON, /* None */
    NULL AS TIN_REMARKS, /* None */
    'LMS' AS SOURCE_NAME, /* None */
    T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
    CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
    10 AS PRIORITY_LEVEL,
    COALESCE(T1.LAST_GENERATED_DATETIME, '1900-01-01') AS SOURCE_UPDATE_DATE
  FROM {params["com_schema"]}.M_LMS_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
  WHERE
    T1.ETL_DT = '{batch_date}'
    AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
    AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%]'
    AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
), LMS_COUNTERPARTYCHILD2_CRS AS (
  SELECT
    T2.CUST_ID AS CUSTOMER_ID, /* None */
    NULL AS CRS_ENTITY_TYPE, /* None */
    NULL AS NAME_OF_CONTROLLING_PERSON_1, /* None */
    NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
    T1.CHILD2_TAX_RESIDENCE_COUNTRY_CODE AS CRS_COUNTRY, /* None */
    T2.COUNTRY_OF_RESIDENT_DECLARATION_ID AS CRS_TAX_RESIDENCE, /* None */
    T1.CHILD2_TAX_IDENTIFICATION_NUMBER AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
    NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
    T1.CHILD2_TIN_UNAVAILABLE_REASON_ID AS TIN_UNAVAILABLE_REASON, /* None */
    T1.CHILD2_TIN_UNAVAILABLE_REMARKS AS TIN_REMARKS, /* None */
    'LMS' AS SOURCE_NAME, /* None */
    CAST(T2.RECORD_ID AS STRING) AS SOURCE_RECORD_ID, /* None */
    CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
    10 AS PRIORITY_LEVEL,
    GREATEST(
      COALESCE(T2.LAST_ACTION_DATETIME, '1900-01-01'),
      COALESCE(T2.SYSTEM_UPDATED_DATETIME, '1900-01-01')
    ) AS SOURCE_UPDATE_DATE
  FROM COUNTERPARTYCHILD2 AS T1 /* None */
  LEFT JOIN {params["com_schema"]}.M_LMSKIBB2_TBL_COUNTERPARTY AS T2
    ON T2.ETL_DT = '{batch_date}'
    AND TRIM(COALESCE(T1.CHILD2_TAX_IDENTIFICATION_NUMBER, '')) <> ''
    AND NOT T2.CLEAN_RULE_FLAG LIKE '%1%'
    AND T1.PARENT_FUNCTION_RECORD_ID = T2.RECORD_ID
  WHERE
    T1.RN = 1 AND NOT T2.CUST_ID IS NULL
), LMS_CRS_COMBINE AS (
  SELECT
    *,
    1 AS LMS_PRIORITY_LEVEL /* Prioritize non-null value */
  FROM LMS_COUNTERPARTYCHILD2_CRS
  UNION ALL
  SELECT
    *,
    2 AS LMS_PRIORITY_LEVEL
  FROM LMS_TBL_EINVOICING_CLIENTDATA_CRS
), LMS_CRS_ROW_NUM AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY LMS_PRIORITY_LEVEL) AS RN
  FROM LMS_CRS_COMBINE
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
FROM LMS_CRS_ROW_NUM
WHERE
  RN = 1
""")

spark.sql(rf"""
/* ==============[Group.9 rak]============== */
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
  SOURCE_UPDATE_DATE /* 20251030 */
)
SELECT
  T1.CUST_ID AS CUSTOMER_ID, /* None */
  NULL AS CRS_ENTITY_TYPE, /* None */
  NULL AS NAME_OF_CONTROLLING_PERSON_1, /* None */
  NULL AS NAME_OF_CONTROLLING_PERSON_2, /* None */
  NULL AS CRS_COUNTRY, /* None */
  NULL AS CRS_TAX_RESIDENCE, /* None */
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  T1.SOURCE_SYSTEM AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  T1.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
FROM {params["com_schema"]}.M_rak_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND NOT T1.clean_rule_flag LIKE '%1%'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
""")

spark.sql(rf"""
/* ==============[Group.10 GUAVA]============== */
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
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  'GUAVA' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  4 AS PRIORITY_LEVEL,
  T1.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
FROM {params["com_schema"]}.M_GUAVA_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")

spark.sql(rf"""
/* ==============[Group.11 SUNGL]============== */
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
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  'SUNGL' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  7 AS PRIORITY_LEVEL,
  NULL AS SOURCE_UPDATE_DATE /* 20251030 */
FROM {params["com_schema"]}.M_SUNGL_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")

spark.sql(rf"""
/* ==============[Group.10 M21]============== */
/* ==============[Group.10.1 M21_A (ALGO file)]============== */
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
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  2 AS PRIORITY_LEVEL,
  T1.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
FROM {params["com_schema"]}.M_M21_A_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")

spark.sql(rf"""
/* ==============[Group.10.1 M21_O (Ovex file)]============== */
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
  T1.CUSTOMER_TIN AS TAXPAYER_IDENTIFICATION_NO_1, /* None */
  NULL AS TAXPAYER_IDENTIFICATION_NO_2, /* None */
  NULL AS TIN_UNAVAILABLE_REASON, /* None */
  NULL AS TIN_REMARKS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
  2 AS PRIORITY_LEVEL,
  T1.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
FROM {params["com_schema"]}.M_M21_O_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '{batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")

spark.sql(rf"""
/* Insert into curated */
WITH CRS_ROW_NUM AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID, TAXPAYER_IDENTIFICATION_NO_1 ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
  FROM {params["cur_schema"]}.TEMP_DIM_CRS
)
INSERT INTO {params["cur_schema"]}.DIM_CRS PARTITION(etl_dt = '{batch_date}') (
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


# Stop Spark when done
spark.stop()