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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.MOBILE_NO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.MOBILE_NO, '')) <> ''
  AND NOT T1.MOBILE_NO LIKE '@[%]';

/* ==============[Group.2]============== */
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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'HOME' AS CONTACT_TYPE, /* None */
  T1.TEL_NO_HOME AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TEL_NO_HOME, '')) <> ''
  AND NOT T1.TEL_NO_HOME LIKE '@[%]';

/* ==============[Group.3]============== */
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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  T1.TEL_NO_OFFICE AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TEL_NO_OFFICE, '')) <> ''
  AND NOT T1.TEL_NO_OFFICE LIKE '@[%]';

/* ==============[Group.4]============== */
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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'FAX' AS CONTACT_TYPE, /* None */
  T1.FAX_NO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.FAX_NO, '')) <> ''
  AND NOT T1.FAX_NO LIKE '@[%]';

/* ==============[Group.5]============== */
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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
FROM (
  SELECT
    CLIENT_NO,
    EMAIL_1 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    1 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_1, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_1 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_2 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    2 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_2, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_2 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_3 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    3 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_3, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_3 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_4 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    4 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_4, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_4 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_5 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    5 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_5, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_5 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_6 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    6 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_6, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_6 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_7 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    7 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_7, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_7 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_8 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    8 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_8, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_8 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_9 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    9 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_9, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_9 RLIKE '^\\@\\[.*\\]$'
  UNION ALL
  SELECT
    CLIENT_NO,
    EMAIL_10 AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    10 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EMAIL_10, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EMAIL_10 RLIKE '^\\@\\[.*\\]$'
) AS T1 /* None */
WHERE
  1 = 1;

/* ==============[Group.55]============== */ /* ==============EINVOICE_EMAIL============== */
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
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL_EINV' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
  T1.SEQUENCE_NO AS SEQUENCE_NO /* None */
FROM (
  SELECT
    CLIENT_NO,
    EINVOICE_EMAIL AS EMAIL,
    DATE_CREATED,
    DATE_CHANGE,
    1 AS SEQUENCE_NO
  FROM ${com_schema}.T_MHBOS_M_CLIENT
  WHERE
    TRIM(COALESCE(EINVOICE_EMAIL, '')) <> ''
    AND ETL_DT = '${batch_date}'
    AND NOT EINVOICE_EMAIL RLIKE '^\\@\\[.*\\]$'
) AS T1 /* None */
WHERE
  1 = 1;

/* ==============[Group.6]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_CONTACT;

CREATE TABLE ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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

INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.TEL_NO_HP AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TEL_NO_HP, '')) <> ''
  AND NOT T1.TEL_NO_HP LIKE '@[%]';

/* ==============[Group.7]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  (
    CASE
      WHEN T1.SOURCE_TEL_NO_OFF2 LIKE 'EXT%'
      THEN T1.TEL_NO_OFF1 || T1.TEL_NO_OFF2
      ELSE T1.TEL_NO_OFF1
    END
  ) AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND COALESCE(T1.TEL_NO_OFF1, '') <> ''
  AND NOT T1.TEL_NO_OFF1 LIKE '@[%]';

/* ==============[Group.7.2]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  T1.TEL_NO_OFF2 AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
  2 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND COALESCE(T1.TEL_NO_OFF2, '') <> ''
  AND NOT COALESCE(T1.SOURCE_TEL_NO_OFF2, '') LIKE 'EXT%'
  AND NOT T1.TEL_NO_OFF2 LIKE '@[%]';

/* ==============[Group.8]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1
WHERE
  TRIM(COALESCE(T1.EMAIL, '')) <> ''
  AND ETL_DT = '${batch_date}'
  AND NOT T1.EMAIL LIKE '@[%]';

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

/* ==============[Group.34]============== */ /* ADDED ON 2024-06-06 */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'HOME' AS CONTACT_TYPE, /* None */
  T1.TEL_NO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.DATE_CREATED AS CONTACT_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TEL_NO, '')) <> ''
  AND NOT T1.TEL_NO LIKE '@[%]';

/* ==============[Group.55]============== */ /* ADDED ON 2025-01-28 */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
  'AGENT_ASSISTANT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  COALESCE(NULLIF(T1.MOBILE_NO, ''), NULLIF(T1.TEL_NO, '')) AS CONTACT_VALUE, /* None */
  T1.TDR_NAME AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
  FROM ${com_schema}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
  WHERE
    T1.ETL_DT = '${batch_date}'
    AND LICENCE_TYPE IN ('08', '09', '10', '11')
    AND (
      TRIM(COALESCE(T1.MOBILE_NO, '')) <> '' OR TRIM(COALESCE(T1.TEL_NO, '')) <> ''
    )
) AS T1
WHERE
  RN = 1;

/* ==============[Group.48]============== */ /* ADDED ON 2025-01-28 */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT (
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
  'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
  'AGENT_ASSISTANT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  T1.TDR_NAME AS CONTACT_NAME, /* None */
  NULL AS CONTACT_CREATE_DATE, /* None */
  NULL AS CONTACT_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  IC_NO_NEW AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
  FROM ${com_schema}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
  WHERE
    T1.ETL_DT = '${batch_date}'
    AND LICENCE_TYPE IN ('08', '09', '10', '11')
    AND TRIM(COALESCE(T1.EMAIL, '')) <> ''
) AS T1
WHERE
  RN = 1;

/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT;

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_CONTACT;

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_BRANCH_CONTACT;