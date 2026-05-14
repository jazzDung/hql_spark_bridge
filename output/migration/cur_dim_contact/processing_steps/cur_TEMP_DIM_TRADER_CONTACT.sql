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

/* ==============[Group.19]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.MOBILEPHONE AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_DATE, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.MOBILEPHONE, '')) <> ''
  AND NOT T1.MOBILEPHONE LIKE '@[%]';

/* ==============[Group.20]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  T1.TELEPHONE AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_DATE, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.TELEPHONE, '')) <> ''
  AND NOT T1.TELEPHONE LIKE '@[%]';

/* ==============[Group.21]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'FAX' AS CONTACT_TYPE, /* None */
  T1.FAX AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_DATE, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.FAX, '')) <> ''
  AND NOT T1.FAX LIKE '@[%]';

/* ==============[Group.22]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.EMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.CREATEDATE AS CONTACT_CREATE_DATE, /* None */
  T1.MODIFYDATE AS CONTACT_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM (
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[0] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    1 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[0]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[1] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    2 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[1]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[2] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    3 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[2]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[3] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    4 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[3]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[4] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    5 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[4]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[5] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    6 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[5]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[6] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    7 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[6]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[7] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    8 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[7]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[8] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    9 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[8]), '') <> '' AND ETL_DT = '${batch_date}'
  UNION ALL
  SELECT
    CODE,
    SPLIT(EMAIL, ';')[9] AS EMAIL,
    CREATEDATE,
    MODIFYDATE,
    10 AS SEQUENCE_NO
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE
  WHERE
    COALESCE(TRIM(SPLIT(EMAIL, ';')[9]), '') <> '' AND ETL_DT = '${batch_date}'
) AS T1 /* None */
WHERE
  1 = 1;

/* ==============[Group.30]============== */
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
  'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'OFFICE' AS CONTACT_TYPE, /* None */
  T1.AGENTTELNO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
  T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'TOMS' AS SOURCE_NAME, /* None */
  T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_TOMS_EAGENTDETAILS AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND COALESCE(T1.AGENTTELNO, '') <> ''
  AND NOT T1.AGENTTELNO LIKE '@[%]';

/* ==============[Group.31]============== */
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
  'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'HOME' AS CONTACT_TYPE, /* None */
  T1.AGENTHOMENO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
  T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'TOMS' AS SOURCE_NAME, /* None */
  T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_TOMS_EAGENTDETAILS AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}' AND TRIM(COALESCE(T1.AGENTHOMENO, '')) <> '';

/* ==============[Group.32]============== */
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
  'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'MOBILE' AS CONTACT_TYPE, /* None */
  T1.AGENTMOBILENO AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
  T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'TOMS' AS SOURCE_NAME, /* None */
  T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_TOMS_EAGENTDETAILS AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}' AND TRIM(COALESCE(T1.AGENTMOBILENO, '')) <> '';

/* ==============[Group.33]============== */
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
  'TOMS_' || T1.AGENTCODE AS OWNER_ID, /* None */
  'AGENT' AS CONTACT_OWNER_TYPE, /* None */
  'EMAIL' AS CONTACT_TYPE, /* None */
  T1.AGENTEMAIL AS CONTACT_VALUE, /* None */
  NULL AS CONTACT_NAME, /* None */
  T1.SYDTC AS CONTACT_CREATE_DATE, /* None */
  T1.SYDTU AS CONTACT_UPDATE_DATE, /* None */
  'UT' AS LINE_OF_BUSINESS, /* None */
  'TOMS' AS SOURCE_NAME, /* None */
  T1.AGENTCODE AS SOURCE_RECORD_ID, /* None */
  1 AS SEQUENCE_NO /* None */
FROM ${com_schema}.T_TOMS_EAGENTDETAILS AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}' AND TRIM(COALESCE(T1.AGENTEMAIL, '')) <> '';

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

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_CONTACT;