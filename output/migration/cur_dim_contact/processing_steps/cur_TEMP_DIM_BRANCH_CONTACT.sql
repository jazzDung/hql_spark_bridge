/* ==============[Group.9]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_BRANCH_CONTACT;

CREATE TABLE ${cur_schema}.TEMP_DIM_BRANCH_CONTACT (
  OWNER_ID VARCHAR(50), /* None */
  CONTACT_OWNER_TYPE VARCHAR(20), /* None */
  CONTACT_TYPE VARCHAR(15), /* None */
  CONTACT_VALUE VARCHAR(150), /* None */
  CONTACT_NAME VARCHAR(100), /* None */
  CONTACT_CREATE_DATE DATE, /* None */
  CONTACT_UPDATE_DATE DATE, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(50), /* None */
  SEQUENCE_NO INT /* None */
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

INSERT INTO ${cur_schema}.TEMP_DIM_BRANCH_CONTACT (
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
  SEQUENCE_NO /* None */
)
SELECT
  'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
  'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  T1.TEL_NO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_BRANCH AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
  AND NOT T1.TEL_NO LIKE '@[%]';

/* ==============[Group.10]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_BRANCH_CONTACT (
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
  SEQUENCE_NO /* None */
)
SELECT
  'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
  'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
  'FAX' AS CONTACT_TYPE, /* None */
  T1.FAX_NO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_BRANCH AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
  AND NOT T1.FAX_NO LIKE '@[%]';

/* ==============[Group.11]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_BRANCH_CONTACT (
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
  SEQUENCE_NO /* None */
)
SELECT
  'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
  'BRANCH' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.BRANCH_ID AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_BRANCH AS T1
WHERE
  TRIM(COALESCE(T1.EMAIL, '')) <> ''
  AND ETL_DT = '${batch_date}'
  AND NOT T1.EMAIL LIKE '@[%]';

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_BRANCH_CONTACT;