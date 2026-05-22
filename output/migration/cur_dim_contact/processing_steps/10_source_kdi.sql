/* ==============[Group.1]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT;

CREATE TABLE ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT (
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

/* ==============[Group.35]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT (
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
  'KDI_' || T1.ACCOUNT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  T1.CUSTOMER_CONTACT_NUMBER_TYPE AS CONTACT_TYPE, /* None */
  T1.CUSTOMER_CONTACT_NUMBER AS CONTACT_VALUE, /* None */
  T1.CUSTOMER_NAME AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'KDI' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_KDI_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND NOT T1.clean_rule_flag LIKE '%1%'
  AND COALESCE(T1.CUSTOMER_CONTACT_NUMBER, '') <> ''
  AND NOT T1.CUSTOMER_CONTACT_NUMBER LIKE '@[%]';

/* ==============[Group.36]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT (
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
  'KDI_' || T1.ACCOUNT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.CUSTOMER_E_MAIL AS CONTACT_VALUE, /* None */
  T1.CUSTOMER_NAME AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'KDI' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_KDI_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND NOT T1.clean_rule_flag LIKE '%1%'
  AND NOT T1.CUSTOMER_E_MAIL IS NULL
  AND T1.CUSTOMER_E_MAIL <> '';

/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT;