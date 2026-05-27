/* ==============[Group.1]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS;

CREATE TABLE ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS (
  OWNER_ID VARCHAR(50), /* None */
  ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
  ADDRESS_TYPE VARCHAR(10), /* None */
  ADDRESS_LINE_1 VARCHAR(100), /* None */
  ADDRESS_LINE_2 VARCHAR(100), /* None */
  ADDRESS_LINE_3 VARCHAR(100), /* None */
  ADDRESS_LINE_4 VARCHAR(100), /* None */
  CITY VARCHAR(255), /* None */
  STATE VARCHAR(50), /* None */
  POSTCODE VARCHAR(5), /* None */
  COUNTRY VARCHAR(3), /* None */
  ADDRESS_CREATE_DATE DATE, /* None */
  ADDRESS_UPDATE_DATE DATE, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(50) /* None */
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS (
  OWNER_ID, /* None */
  ADDRESS_OWNER_TYPE, /* None */
  ADDRESS_TYPE, /* None */
  ADDRESS_LINE_1, /* None */
  ADDRESS_LINE_2, /* None */
  ADDRESS_LINE_3, /* None */
  ADDRESS_LINE_4, /* None */
  CITY, /* None */
  STATE, /* None */
  POSTCODE, /* None */
  COUNTRY, /* None */
  ADDRESS_CREATE_DATE, /* None */
  ADDRESS_UPDATE_DATE, /* None */
  LINE_OF_BUSINESS, /* None */
  SOURCE_NAME, /* None */
  SOURCE_RECORD_ID /* None */
)
SELECT
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.ADDR1 AS ADDRESS_LINE_1, /* None */
  T1.ADDR2 AS ADDRESS_LINE_2, /* None */
  T1.ADDR3 AS ADDRESS_LINE_3, /* None */
  T1.ADDR4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  COALESCE(T2.REFERENCE_VALUE_2, T1.STATE) AS STATE, /* 20251013 */
  T1.POSTCODE AS POSTCODE, /* None */
  T1.COUNTRY AS COUNTRY, /* None */
  T1.DATE_CREATED AS ADDRESS_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS ADDRESS_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
LEFT JOIN ${cur_schema}.REF_LOOKUP AS T2 /* None */
  ON T1.STATE = T2.REFERENCE_CODE
  AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T2.SOURCE_NAME = 'MHBOS'
  AND T2.REFERENCE_TYPE = 'STATE_CODE'
  AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR4, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T1.STATE, '')) <> ''
    OR TRIM(COALESCE(T1.POSTCODE, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  );

/* ==============[Group.2]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS (
  OWNER_ID, /* None */
  ADDRESS_OWNER_TYPE, /* None */
  ADDRESS_TYPE, /* None */
  ADDRESS_LINE_1, /* None */
  ADDRESS_LINE_2, /* None */
  ADDRESS_LINE_3, /* None */
  ADDRESS_LINE_4, /* None */
  CITY, /* None */
  STATE, /* None */
  POSTCODE, /* None */
  COUNTRY, /* None */
  ADDRESS_CREATE_DATE, /* None */
  ADDRESS_UPDATE_DATE, /* None */
  LINE_OF_BUSINESS, /* None */
  SOURCE_NAME, /* None */
  SOURCE_RECORD_ID /* None */
)
SELECT
  'MHBOS_' || T1.CLIENT_NO AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.PERM_ADDR1 AS ADDRESS_LINE_1, /* None */
  T1.PERM_ADDR2 AS ADDRESS_LINE_2, /* None */
  T1.PERM_ADDR3 AS ADDRESS_LINE_3, /* None */
  T1.PERM_ADDR4 AS ADDRESS_LINE_4, /* None */
  T1.PERM_CITY AS CITY, /* None */
  COALESCE(T3.REFERENCE_VALUE_2, T1.PERM_STATE) AS STATE, /* None */
  T1.PERM_POSTCODE AS POSTCODE, /* None */
  T1.PERM_COUNTRY AS COUNTRY, /* None */
  T1.DATE_CREATED AS ADDRESS_CREATE_DATE, /* None */
  T1.DATE_CHANGE AS ADDRESS_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.CLIENT_NO AS SOURCE_RECORD_ID /* None */
FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 /* None */
LEFT JOIN ${com_schema}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
  ON T1.CLIENT_NO = T2.CLIENT_NO AND T2.PART_ID = '${batch_date}'
LEFT JOIN ${cur_schema}.REF_LOOKUP AS T3 /* None */
  ON T1.PERM_STATE = T3.REFERENCE_CODE
  AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T3.SOURCE_NAME = 'MHBOS'
  AND T3.REFERENCE_TYPE = 'STATE_CODE'
  AND TRIM(COALESCE(T3.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.PERM_ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_ADDR3, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_ADDR4, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_CITY, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_STATE, '')) <> ''
    OR TRIM(COALESCE(T1.PERM_POSTCODE, '')) <> ''
  );

/* ==============[Group.3]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_ADDRESS;

CREATE TABLE ${cur_schema}.TEMP_DIM_TRADER_ADDRESS (
  OWNER_ID VARCHAR(50), /* None */
  ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
  ADDRESS_TYPE VARCHAR(10), /* None */
  ADDRESS_LINE_1 VARCHAR(100), /* None */
  ADDRESS_LINE_2 VARCHAR(100), /* None */
  ADDRESS_LINE_3 VARCHAR(100), /* None */
  ADDRESS_LINE_4 VARCHAR(100), /* None */
  CITY VARCHAR(255), /* None */
  STATE VARCHAR(50), /* None */
  POSTCODE VARCHAR(5), /* None */
  COUNTRY VARCHAR(3), /* None */
  ADDRESS_CREATE_DATE DATE, /* None */
  ADDRESS_UPDATE_DATE DATE, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(50) /* None */
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_ADDRESS (
  OWNER_ID, /* None */
  ADDRESS_OWNER_TYPE, /* None */
  ADDRESS_TYPE, /* None */
  ADDRESS_LINE_1, /* None */
  ADDRESS_LINE_2, /* None */
  ADDRESS_LINE_3, /* None */
  ADDRESS_LINE_4, /* None */
  CITY, /* None */
  STATE, /* None */
  POSTCODE, /* None */
  COUNTRY, /* None */
  ADDRESS_CREATE_DATE, /* None */
  ADDRESS_UPDATE_DATE, /* None */
  LINE_OF_BUSINESS, /* None */
  SOURCE_NAME, /* None */
  SOURCE_RECORD_ID /* None */
)
SELECT
  'MHBOS_' || T1.TDR_CODE AS OWNER_ID, /* None */
  'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.ADDR1 AS ADDRESS_LINE_1, /* None */
  T1.ADDR2 AS ADDRESS_LINE_2, /* None */
  T1.ADDR3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T2.REFERENCE_VALUE_2 AS STATE, /* None */
  T1.POSTCODE AS POSTCODE, /* None */
  T1.COUNTRY AS COUNTRY, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_CREATE_DATE, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.TDR_CODE AS SOURCE_RECORD_ID /* None */
FROM ${com_schema}.T_MHBOS_M_TRADER AS T1 /* None */
LEFT JOIN ${cur_schema}.REF_LOOKUP AS T2 /* None */
  ON T1.STATE = T2.REFERENCE_CODE
  AND /* AND T2.ETL_DT = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
  AND T2.SOURCE_NAME = 'MHBOS'
  AND T2.REFERENCE_TYPE = 'STATE_CODE'
  AND TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
  );

/* ==============[Group.4]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_BRANCH_ADDRESS;

CREATE TABLE ${cur_schema}.TEMP_DIM_BRANCH_ADDRESS (
  OWNER_ID VARCHAR(50), /* None */
  ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
  ADDRESS_TYPE VARCHAR(10), /* None */
  ADDRESS_LINE_1 VARCHAR(100), /* None */
  ADDRESS_LINE_2 VARCHAR(100), /* None */
  ADDRESS_LINE_3 VARCHAR(100), /* None */
  ADDRESS_LINE_4 VARCHAR(100), /* None */
  CITY VARCHAR(255), /* None */
  STATE VARCHAR(50), /* None */
  POSTCODE VARCHAR(5), /* None */
  COUNTRY VARCHAR(3), /* None */
  ADDRESS_CREATE_DATE DATE, /* None */
  ADDRESS_UPDATE_DATE DATE, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(50) /* None */
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

INSERT INTO ${cur_schema}.TEMP_DIM_BRANCH_ADDRESS (
  OWNER_ID, /* None */
  ADDRESS_OWNER_TYPE, /* None */
  ADDRESS_TYPE, /* None */
  ADDRESS_LINE_1, /* None */
  ADDRESS_LINE_2, /* None */
  ADDRESS_LINE_3, /* None */
  ADDRESS_LINE_4, /* None */
  CITY, /* None */
  STATE, /* None */
  POSTCODE, /* None */
  COUNTRY, /* None */
  ADDRESS_CREATE_DATE, /* None */
  ADDRESS_UPDATE_DATE, /* None */
  LINE_OF_BUSINESS, /* None */
  SOURCE_NAME, /* None */
  SOURCE_RECORD_ID /* None */
)
SELECT
  'MHBOS_' || T1.BRANCH_ID AS OWNER_ID, /* None */
  'BRANCH' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.ADDR1 AS ADDRESS_LINE_1, /* None */
  T1.ADDR2 AS ADDRESS_LINE_2, /* None */
  T1.ADDR3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  NULL AS CITY, /* None */
  NULL AS STATE, /* None */
  T1.POST_CODE AS POSTCODE, /* None */
  NULL AS COUNTRY, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_CREATE_DATE, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  T1.BRANCH_ID AS SOURCE_RECORD_ID /* None */
FROM ${com_schema}.T_MHBOS_M_BRANCH AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDR1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
    OR TRIM(COALESCE(T1.POST_CODE, '')) <> ''
  );

/* behind TEMP_DIM_ACCOUNT_ADDRESS */ /* ==============[Group.20]============== */ /* added 20250128 */
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_ADDRESS (
  OWNER_ID, /* None */
  ADDRESS_OWNER_TYPE, /* None */
  ADDRESS_TYPE, /* None */
  ADDRESS_LINE_1, /* None */
  ADDRESS_LINE_2, /* None */
  ADDRESS_LINE_3, /* None */
  ADDRESS_LINE_4, /* None */
  CITY, /* None */
  STATE, /* None */
  POSTCODE, /* None */
  COUNTRY, /* None */
  ADDRESS_CREATE_DATE, /* None */
  ADDRESS_UPDATE_DATE, /* None */
  LINE_OF_BUSINESS, /* None */
  SOURCE_NAME, /* None */
  SOURCE_RECORD_ID /* None */
)
SELECT
  'MHBOS_' || T1.IC_NO_NEW AS OWNER_ID, /* None */
  'AGENT_ASSISTANT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.ADDR1 AS ADDRESS_LINE_1, /* None */
  T1.ADDR2 AS ADDRESS_LINE_2, /* None */
  T1.ADDR3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  NULL AS CITY, /* None */
  NULL AS STATE, /* None */
  T1.POSTCODE AS POSTCODE, /* None */
  T1.COUNTRY AS COUNTRY, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_CREATE_DATE, /* None */
  DATE_FORMAT(
    CAST(SUBSTRING('${batch_date}', 1, 4) || '-' || SUBSTRING('${batch_date}', 5, 2) || '-' || SUBSTRING('${batch_date}', 7, 2) AS TIMESTAMP),
    'yyyy-MM-dd'
  ) AS ADDRESS_UPDATE_DATE, /* None */
  'EB' AS LINE_OF_BUSINESS, /* None */
  'MHBOS' AS SOURCE_NAME, /* None */
  IC_NO_NEW AS SOURCE_RECORD_ID /* None */
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY IC_NO_NEW ORDER BY DATE_CREATED DESC) AS RN
  FROM ${com_schema}.T_MHBOS_M_TRADER_CMSRL AS T1 /* None */
  WHERE
    T1.ETL_DT = '${batch_date}'
    AND LICENCE_TYPE IN ('08', '09', '10', '11')
    AND (
      TRIM(COALESCE(T1.ADDR1, '')) <> ''
      OR TRIM(COALESCE(T1.ADDR2, '')) <> ''
      OR TRIM(COALESCE(T1.ADDR3, '')) <> ''
    )
) AS T1
WHERE
  RN = 1;

/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS;

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_ADDRESS;

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_BRANCH_ADDRESS;