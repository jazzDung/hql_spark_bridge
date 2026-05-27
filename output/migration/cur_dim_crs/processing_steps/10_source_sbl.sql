/* Delete all temporary tables */
DROP TABLE IF EXISTS ${cur_schema}.TEMP_DIM_CRS;

/* Create temp table */
CREATE TABLE ${cur_schema}.TEMP_DIM_CRS (
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
);

/* ==============[Group.7 SBL]============== */
INSERT INTO ${cur_schema}.TEMP_DIM_CRS (
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
  '${batch_timestamp}' AS ETL_TIMESTAMP,
  9 AS priority_level,
  COALESCE(T1.LAST_GENERATED_DATETIME, '1900-01-01') AS SOURCE_UPDATE_DATE
FROM ${com_schema}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T1
LEFT JOIN ${com_schema}.t_sblkibb_tbl_account AS T2
  ON T1.account_number = T2.account_number
  AND T2.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T2.etl_dt, 'yyyyMMdd')) BETWEEN T2.effective_from AND T2.effective_to
LEFT JOIN ${com_schema}.t_sblkibb_tbl_counterparty AS T3
  ON T2.counterparty_id_1 = T3.counterparty_id
  AND T3.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T3.etl_dt, 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
LEFT JOIN ${com_schema}.t_sblkibb_tbl_country AS T4
  ON T3.Country_Of_Birth_Country_Id = T4.country_id
  AND T4.record_status_id = 3
  AND FROM_UNIXTIME(UNIX_TIMESTAMP(T4.etl_dt, 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
WHERE
  T1.ETL_DT = '${batch_date}'
  AND TRIM(COALESCE(T1.CUSTOMER_TIN, '')) <> ''
  AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
  AND TRIM(COALESCE(T1.PRIMARY_IDENTIFICATION_TYPE, '')) <> '';