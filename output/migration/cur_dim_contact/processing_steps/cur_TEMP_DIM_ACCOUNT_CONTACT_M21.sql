/* ==============[Group.15 + 16 + 17: M21, M21_A, M21_O Account Contact]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21;

CREATE TABLE ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21 (
  OWNER_ID VARCHAR(50), /* None */
  CONTACT_OWNER_TYPE VARCHAR(20), /* None */
  CONTACT_TYPE VARCHAR(15), /* None */
  CONTACT_VALUE VARCHAR(150), /* None */
  CONTACT_NAME VARCHAR(100), /* None */
  CONTACT_CREATE_TIME TIMESTAMP, /* None */
  CONTACT_UPDATE_TIME TIMESTAMP, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(50), /* None */
  SEQUENCE_NO INT, /* None */
  ETL_TIMESTAMP STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

/* ==============[Group.15: M21 Account Contact: (ACCOUNT, MOBILE), (ACCOUNT, HOME), (ACCOUNT, OFFICE), (ACCOUNT, FAX), (ACCOUNT, EMAIL)]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21 (
  OWNER_ID,
  CONTACT_OWNER_TYPE,
  CONTACT_TYPE,
  CONTACT_VALUE,
  CONTACT_NAME,
  CONTACT_CREATE_TIME,
  CONTACT_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  SEQUENCE_NO,
  ETL_TIMESTAMP
)
/* Account Mobile Contact */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.MOBILEPHONE AS CONTACT_VALUE, /* None */
  T1.CONTACTPERSON AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_TIME, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.MOBILEPHONE, '')) <> ''
  AND NOT T1.MOBILEPHONE LIKE '@[%]'
UNION ALL
/* Account Home And Office Contact */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  (
    CASE
      WHEN T1.CLIENTTYPE IN ('CORP', 'EAO', 'FCORP', 'INST', 'INSTLO', 'POC', 'POCFR')
      THEN 'OFFICE'
      WHEN T1.CLIENTTYPE IN ('DUAL', 'FOIND', 'INDIVID', 'LOCAL', 'RETAIL', 'SINGLE')
      THEN 'HOME'
      ELSE 'HOME'
    END
  ) AS CONTACT_TYPE, /* None */
  T1.TELEPHONE AS CONTACT_VALUE, /* None */
  T1.CONTACTPERSON AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_TIME, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TELEPHONE, '')) <> ''
  AND NOT T1.TELEPHONE LIKE '@[%]'
UNION ALL
/* Account Fax Contact */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'FAX' AS CONTACT_TYPE, /* None */
  T1.FAX AS CONTACT_VALUE, /* None */
  T1.CONTACTPERSON AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_TIME, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.FAX, '')) <> ''
  AND NOT T1.FAX LIKE '@[%]'
UNION ALL
/* Account Email Contact */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL_1 AS CONTACT_VALUE, /* None */
  T1.CONTACTPERSON AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_TIME, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1
WHERE
  TRIM(COALESCE(EMAIL_1, '')) <> ''
  AND ETL_DT = '${batch_date}'
  AND NOT EMAIL_1 RLIKE '^\\@\\[.*\\]$';

/* ==============[Group.16: M21_A Account Contact: (ACCOUNT, MOBILE), (ACCOUNT, HOME), (ACCOUNT, OFFICE), (ACCOUNT, EMAIL)]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21 (
  OWNER_ID,
  CONTACT_OWNER_TYPE,
  CONTACT_TYPE,
  CONTACT_VALUE,
  CONTACT_NAME,
  CONTACT_CREATE_TIME,
  CONTACT_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  SEQUENCE_NO,
  ETL_TIMESTAMP
)
/* Account Mobile Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.customer_contact_number AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_A_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND UPPER(T1.customer_contact_number_type) = 'MOBILE'
  AND TRIM(COALESCE(T1.customer_contact_number, '')) <> ''
  AND NOT T1.customer_contact_number LIKE '@[%]'
UNION ALL
/* Account Home And Office Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  (
    CASE
      WHEN T1.primary_identification_type IN ('1', '2', '3', '5')
      THEN 'HOME'
      WHEN T1.primary_identification_type IN ('4', '6')
      THEN 'OFFICE'
      ELSE 'HOME'
    END
  ) AS CONTACT_TYPE,
  T1.customer_contact_number AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_A_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND UPPER(T1.customer_contact_number_type) = 'HOME/OFFICE'
  AND TRIM(COALESCE(T1.customer_contact_number, '')) <> ''
  AND NOT T1.customer_contact_number LIKE '@[%]'
UNION ALL
/* Account Email Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.customer_e_mail AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_A_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.customer_e_mail, '')) <> ''
  AND NOT T1.customer_e_mail LIKE '@[%]';

/* ==============[Group.17: M21_O Account Contact: (ACCOUNT, MOBILE), (ACCOUNT, HOME), (ACCOUNT, OFFICE), (ACCOUNT, EMAIL)]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21 (
  OWNER_ID,
  CONTACT_OWNER_TYPE,
  CONTACT_TYPE,
  CONTACT_VALUE,
  CONTACT_NAME,
  CONTACT_CREATE_TIME,
  CONTACT_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  SEQUENCE_NO,
  ETL_TIMESTAMP
)
/* Account Mobile Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.customer_contact_number AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_O_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND UPPER(T1.customer_contact_number_type) = 'MOBILE'
  AND TRIM(COALESCE(T1.customer_contact_number, '')) <> ''
  AND NOT T1.customer_contact_number LIKE '@[%]'
UNION ALL
/* Account Home And Office Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  (
    CASE
      WHEN T1.primary_identification_type IN ('1', '2', '3', '5')
      THEN 'HOME'
      WHEN T1.primary_identification_type IN ('4', '6')
      THEN 'OFFICE'
      ELSE 'HOME'
    END
  ) AS CONTACT_TYPE,
  T1.customer_contact_number AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_O_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND UPPER(T1.customer_contact_number_type) = 'HOME/OFFICE'
  AND TRIM(COALESCE(T1.customer_contact_number, '')) <> ''
  AND NOT T1.customer_contact_number LIKE '@[%]'
UNION ALL
/* Account Email Contact */
SELECT
  'M21_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.customer_e_mail AS CONTACT_VALUE, /* None */
  T1.customer_name AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_TIME, /* None */
  T1.etl_timestamp AS CONTACT_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_O_CUSTOMER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.customer_e_mail, '')) <> ''
  AND NOT T1.customer_e_mail LIKE '@[%]';