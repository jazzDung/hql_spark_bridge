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

/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21;

CREATE TABLE ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
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
  ADDRESS_CREATE_TIME TIMESTAMP, /* None */
  ADDRESS_UPDATE_TIME TIMESTAMP, /* None */
  LINE_OF_BUSINESS VARCHAR(20), /* None */
  SOURCE_NAME VARCHAR(10), /* None */
  SOURCE_RECORD_ID VARCHAR(20), /* NONE */
  ETL_TIMESTAMP STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
);

/* ==============[Group.7: M21 Customer address: (account, business), (account, REGISTERED), (account, MAILING)]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account Business address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'BUSINESS' AS ADDRESS_TYPE, /* None */
  T1.BUSINESSADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.BUSINESSADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.BUSINESSADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.BUSINESSCITY AS CITY, /* None */
  T1.BUSINESSCOUNTRYSTATE AS STATE, /* None */
  T1.BUSINESSPOSTALCODE AS POSTCODE, /* None */
  COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.BUSINESSCOUNTRY) AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1 /* None */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM ${cur_schema}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '${batch_date}'
) AS T2 /* None */
  ON T1.BUSINESSCOUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.BUSINESSADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCITY, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCOUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSPOSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.BUSINESSCOUNTRY, '')) <> ''
  )
UNION ALL
/* Account registered address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.ADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.ADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.ADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.COUNTRYSTATE AS STATE, /* None */
  T1.POSTALCODE AS POSTCODE, /* None */
  COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.COUNTRY) AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.T_M21_CUSTOMER AS T1 /* None */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM ${cur_schema}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '${batch_date}'
) AS T2 /* None */
  ON T1.COUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.ADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.ADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.POSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  )
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.MAILINGADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
  T1.MAILINGADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
  T1.MAILINGADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
  NULL AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.COUNTRYSTATE AS STATE, /* None */
  T1.MAILINGPOSTALCODE AS POSTCODE, /* None */
  T1.COUNTRY AS COUNTRY, /* None */
  T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
  T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM ${com_schema}.R_M21_STATEMENTRECIPIENT AS T1 /* None */
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.MAILINGADDRESSLINE1, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGADDRESSLINE2, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGADDRESSLINE3, '')) <> ''
    OR TRIM(COALESCE(T1.CITY, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
    OR TRIM(COALESCE(T1.MAILINGPOSTALCODE, '')) <> ''
    OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
  );

/* ==============[Group 8: M21_A Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING), (ACCOUNT, EMPLOYER) ]============== */
WITH M21_A_CUSTOMER AS (
  SELECT
    T1.account_number,
    T1.customer_address_type,
    T1.customer_address_line1,
    T1.customer_address_line2,
    T1.customer_address_line3,
    T1.customer_address_line4,
    T1.CITY,
    T2.reference_value AS state,
    T1.postcode,
    T1.country,
    T1.etl_timestamp
  FROM ${com_schema}.T_M21_A_CUSTOMER AS T1 /* None */
  LEFT JOIN ${cur_schema}.ref_lookup AS T2
    ON T1.state = T2.reference_code
    AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
    AND T2.reference_type = 'IRBSTATE'
  WHERE
    T1.ETL_DT = '${batch_date}'
    AND (
      TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
      OR TRIM(COALESCE(T1.CITY, '')) <> ''
      OR TRIM(COALESCE(T1.state, '')) <> ''
      OR TRIM(COALESCE(T1.postcode, '')) <> ''
      OR TRIM(COALESCE(T1.country, '')) <> ''
    )
)
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account registered address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
UNION ALL
/* Account Employer address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'EMPLOYER' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_A_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER';

/* ==============[Group 9: M21_O Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING)]============== */
WITH M21_O_CUSTOMER AS (
  SELECT
    T1.account_number,
    T1.customer_address_type,
    T1.customer_address_line1,
    T1.customer_address_line2,
    T1.customer_address_line3,
    T1.customer_address_line4,
    T1.CITY,
    T2.reference_value AS state,
    T1.postcode,
    T1.country,
    T1.etl_timestamp
  FROM ${com_schema}.T_M21_O_CUSTOMER AS T1 /* None */
  LEFT JOIN ${cur_schema}.ref_lookup AS T2
    ON T1.state = T2.reference_code
    AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
    AND T2.reference_type = 'IRBSTATE'
  WHERE
    T1.ETL_DT = '${batch_date}'
    AND (
      TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
      OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
      OR TRIM(COALESCE(T1.CITY, '')) <> ''
      OR TRIM(COALESCE(T1.state, '')) <> ''
      OR TRIM(COALESCE(T1.postcode, '')) <> ''
      OR TRIM(COALESCE(T1.country, '')) <> ''
    )
)
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
  OWNER_ID,
  ADDRESS_OWNER_TYPE,
  ADDRESS_TYPE,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  ADDRESS_LINE_3,
  ADDRESS_LINE_4,
  CITY,
  STATE,
  POSTCODE,
  COUNTRY,
  ADDRESS_CREATE_TIME,
  ADDRESS_UPDATE_TIME,
  LINE_OF_BUSINESS,
  SOURCE_NAME,
  SOURCE_RECORD_ID,
  ETL_TIMESTAMP
)
/* Account registered address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
UNION ALL
/* Account Mailing address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'MAILING' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
UNION ALL
/* Account Employer address */
SELECT
  'M21_' || T1.account_number AS OWNER_ID, /* None */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
  'EMPLOYER' AS ADDRESS_TYPE, /* None */
  T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
  T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
  T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
  T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
  T1.CITY AS CITY, /* None */
  T1.state AS STATE, /* None */
  T1.postcode AS POSTCODE, /* None */
  T1.country AS COUNTRY, /* None */
  NULL AS ADDRESS_CREATE_TIME, /* None */
  T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.account_number AS SOURCE_RECORD_ID, /* None */
  '${batch_timestamp}' AS etl_timestamp
FROM M21_O_CUSTOMER AS T1 /* None */
WHERE
  UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER';

/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
WITH TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM AS (
  SELECT
    OWNER_ID, /* None */
    ADDRESS_OWNER_TYPE, /* None */
    ADDRESS_TYPE, /* None */
    UPPER(ADDRESS_LINE_1) AS ADDRESS_LINE_1,
    UPPER(ADDRESS_LINE_2) AS ADDRESS_LINE_2,
    UPPER(ADDRESS_LINE_3) AS ADDRESS_LINE_3,
    UPPER(ADDRESS_LINE_4) AS ADDRESS_LINE_4,
    UPPER(CITY) AS CITY,
    UPPER(STATE) AS STATE,
    UPPER(POSTCODE) AS POSTCODE,
    UPPER(COUNTRY) AS COUNTRY,
    DATE_FORMAT(CAST(ADDRESS_CREATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_CREATE_DATE, /* None */
    DATE_FORMAT(CAST(ADDRESS_UPDATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_UPDATE_DATE, /* None */
    LINE_OF_BUSINESS, /* None */
    SOURCE_NAME, /* None */
    SOURCE_RECORD_ID, /* None */
    ROW_NUMBER() OVER (PARTITION BY OWNER_ID, ADDRESS_OWNER_TYPE, ADDRESS_TYPE ORDER BY ADDRESS_UPDATE_TIME DESC) AS rn
  FROM ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS_M21
)
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
FROM TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM
WHERE
  rn = 1;

/* ==============[Group.10]============== */
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
  'M21_' || T1.CODE AS OWNER_ID, /* None */
  'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
  'REGISTERED' AS ADDRESS_TYPE, /* None */
  T1.LINE1 AS ADDRESS_LINE_1, /* None */
  T1.LINE2 AS ADDRESS_LINE_2, /* None */
  T1.LINE3 AS ADDRESS_LINE_3, /* None */
  T1.LINE4 AS ADDRESS_LINE_4, /* None */
  NULL AS CITY, /* None */
  NULL AS STATE, /* None */
  NULL AS POSTCODE, /* None */
  NULL AS COUNTRY, /* None */
  T2.CREATEDATE AS ADDRESS_CREATE_DATE, /* None */
  T2.MODIFYDATE AS ADDRESS_UPDATE_DATE, /* None */
  'FT' AS LINE_OF_BUSINESS, /* None */
  'M21' AS SOURCE_NAME, /* None */
  T1.CODE AS SOURCE_RECORD_ID /* None */
FROM ${com_schema}.R_M21_ACCOUNTEXECUTIVEADDRESS AS T1 /* None */
LEFT JOIN ${com_schema}.T_M21_ACCOUNTEXECUTIVE AS T2 /* None */
  ON T1.CODE = T2.CODE AND T2.ETL_DT = '${batch_date}'
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.LINE1, '')) <> ''
    OR TRIM(COALESCE(T1.LINE2, '')) <> ''
    OR TRIM(COALESCE(T1.LINE3, '')) <> ''
    OR TRIM(COALESCE(T1.LINE4, '')) <> ''
  );

/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS;

DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_TRADER_ADDRESS;