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

/* ==============[Group.17]============== */
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
  'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
  'REGISTERED' AS ADDRESS_TYPE, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_1) AS ADDRESS_LINE_1, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_2) AS ADDRESS_LINE_2, /* NONE */
  TRIM(T1.CUSTOMER_ADDRESS_LINE_3) AS ADDRESS_LINE_3, /* NONE */
  NULL AS ADDRESS_LINE_4, /* NONE */
  TRIM(T1.CUSTOMER_CITY) AS CITY, /* NONE */
  IF(NOT T2.REFERENCE_CODE IS NULL, T2.REFERENCE_VALUE_2, 'NOT APPLICABLE') AS STATE, /* NONE */
  T1.CUSTOMER_POSTAL_CODE AS POSTCODE, /* NONE */
  T3.COUNTRY_CODE_3_DIGITS AS COUNTRY, /* NONE */
  NULL AS ADDRESS_CREATE_DATE, /* NONE */
  NULL AS ADDRESS_UPDATE_DATE, /* NONE */
  'EB' AS LINE_OF_BUSINESS, /* NONE */
  'SBL' AS SOURCE_NAME, /* NONE */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
FROM ${com_schema}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* NONE */
LEFT JOIN ${cur_schema}.REF_LOOKUP AS T2
  ON T1.CUSTOMER_STATE_CODE = T2.REFERENCE_CODE
  AND T2.REFERENCE_TYPE = 'STATE'
  AND T2.SOURCE_NAME = 'SBL'
  AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
/* AND T2.ETL_DT = '${batch_date}' */
LEFT JOIN (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
    COUNTRY_CODE_3_DIGITS,
    COUNTRY_CODE_CCRIS
  FROM ${cur_schema}.REF_COUNTRY
  WHERE
    TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> '' AND ETL_DT = '${batch_date}'
) AS T3 /* None */
  ON T1.CUSTOMER_COUNTRY = T3.COUNTRY_CODE_CCRIS AND T3.ROW_NUM = 1
WHERE
  T1.ETL_DT = '${batch_date}'
  AND (
    TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_1, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_2, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_ADDRESS_LINE_3, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_CITY, '')) <> ''
    OR TRIM(COALESCE(T2.REFERENCE_VALUE_2, '')) <> ''
    OR TRIM(COALESCE(T1.CUSTOMER_POSTAL_CODE, '')) <> ''
    OR TRIM(COALESCE(T3.COUNTRY_CODE_3_DIGITS, '')) <> ''
  )
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%';

/* ==============[Group 20: SBL counterparty mailing]============== */
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
  'SBL_' || T1.ACCOUNT_NUMBER AS OWNER_ID, /* NONE */
  'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* NONE */
  'MAILING' AS ADDRESS_TYPE, /* NONE */
  UPPER(TRIM(T4.mailing_address_1)) AS ADDRESS_LINE_1, /* NONE */
  UPPER(TRIM(T4.mailing_address_2)) AS ADDRESS_LINE_2, /* NONE */
  UPPER(TRIM(T4.mailing_address_3)) AS ADDRESS_LINE_3, /* NONE */
  NULL AS ADDRESS_LINE_4, /* NONE */
  UPPER(TRIM(T4.Mailing_Address_City)) AS CITY, /* NONE */
  T7.reference_value_2 AS STATE, /* NONE */
  UPPER(TRIM(T4.Mailing_Address_Postal_Code)) AS POSTCODE, /* NONE */
  T5.Country_Code_A3 AS COUNTRY, /* NONE */
  NULL AS ADDRESS_CREATE_DATE, /* NONE */
  GREATEST(
    COALESCE(T4.SYSTEM_UPDATED_DATETIME, '1900-01-01'),
    COALESCE(T4.LAST_ACTION_DATETIME, '1900-01-01')
  ) AS ADDRESS_UPDATE_DATE, /* NONE */
  'EB' AS LINE_OF_BUSINESS, /* NONE */
  'SBL' AS SOURCE_NAME, /* NONE */
  T1.ACCOUNT_NUMBER AS SOURCE_RECORD_ID /* NONE */
FROM ${com_schema}.T_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
INNER JOIN ${com_schema}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T2 /* None */
  ON T1.PRIMARY_IDENTIFICATION_NO = T2.PRIMARY_IDENTIFICATION_NO
  AND T1.CUSTOMER_NAME = T2.CUSTOMER_NAME
  AND T2.ETL_DT = '${batch_date}'
  AND NOT T2.CLEAN_RULE_FLAG LIKE '%1%'
LEFT JOIN ${com_schema}.t_sblkibb_tbl_account AS T3
  ON T1.account_number = T3.account_number
  AND T3.etl_dt = '${batch_date}'
  AND T3.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T3.etl_dt, 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
LEFT JOIN ${com_schema}.t_sblkibb_tbl_counterparty AS T4
  ON T3.counterparty_id_1 = T4.counterparty_id
  AND T4.etl_dt = '${batch_date}'
  AND T4.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T4.etl_dt, 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
LEFT JOIN ${com_schema}.t_sblkibb_tbl_country AS T5
  ON T4.Mailing_Address_Country_Id = T5.country_id
  AND T5.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T5.etl_dt, 'yyyyMMdd')) BETWEEN T5.effective_from AND T5.effective_to
LEFT JOIN ${com_schema}.t_sblkibb_tbl_countrystate AS T6
  ON T4.Mailing_Address_Country_State_Id = T6.Country_State_Id
  AND T6.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T6.etl_dt, 'yyyyMMdd')) BETWEEN T6.effective_from AND T6.effective_to
LEFT JOIN ${cur_schema}.ref_lookup AS T7
  ON T6.Country_State_Code = T7.reference_code
  AND T7.source_key = 'GENERAL_REFERENCE_LOOKUP'
  AND T7.source_name = 'SBL'
  AND T7.reference_type = 'STATE'
WHERE
  T1.etl_dt = '${batch_date}'
  AND (
    TRIM(COALESCE(T4.mailing_address_1, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_2, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_3, '')) <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_Postal_Code, '')) /*       TRIM(NVL(T3.Mailing_Address_4, '')) <> '' OR */ <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_City, '')) <> ''
    OR TRIM(COALESCE(T4.Mailing_Address_Country_State_Id, '')) <> ''
    OR TRIM(COALESCE(T4.mailing_address_country_id, '')) <> ''
  )
  AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%';

/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_ACCOUNT_ADDRESS;