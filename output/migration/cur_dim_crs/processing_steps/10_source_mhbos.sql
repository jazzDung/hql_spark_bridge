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

/* ==============[Group.1]============== */
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
  '${batch_timestamp}' AS ETL_TIMESTAMP,
  1 AS PRIORITY_LEVEL,
  CAST(COALESCE(a.DATE_CHANGE, '1900-01-01') AS TIMESTAMP) AS SOURCE_UPDATE_DATE
FROM (
  SELECT
    T1.client_no,
    T2.cust_id,
    T1.DATE_CHANGE,
    ROW_NUMBER() OVER (PARTITION BY T2.cust_id ORDER BY CASE WHEN T1.type_of_account <> 'F' THEN 0 ELSE 1 END, GREATEST(COALESCE(T1.date_created, '1900-01-01'), COALESCE(T1.DATE_CHANGE, '1900-01-01')) DESC) AS RN
  FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1
  INNER JOIN ${com_schema}.m_customer_id_mapping AS T2
    ON T1.client_no = T2.source_owner_id
  WHERE
    T1.etl_dt = '${batch_date}' AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
) AS a
JOIN (
  SELECT
    crs.*
  FROM ${com_schema}.r_mhbos_m_client_crs AS crs
  WHERE
    NOT EXISTS(
      SELECT
        client_no,
        country_tax_residence,
        COUNT(*)
      FROM ${com_schema}.r_mhbos_m_client_crs AS dup_crs
      WHERE
        crs.client_no = dup_crs.client_no
        AND etl_dt = '${batch_date}'
        AND TRIM(COALESCE(crs_tax_type, '')) <> ''
        AND TRIM(COALESCE(tax_identification_no, '')) <> ''
        AND TRIM(COALESCE(country_tax_residence, '')) <> ''
      GROUP BY
        client_no,
        country_tax_residence
      HAVING
        COUNT(*) > 1
    )
    AND etl_dt = '${batch_date}'
    AND TRIM(COALESCE(crs.crs_tax_type, '')) <> ''
    AND TRIM(COALESCE(crs.tax_identification_no, '')) <> ''
    AND TRIM(COALESCE(crs.country_tax_residence, '')) <> ''
) AS b
  ON b.client_no = a.client_no
WHERE
  RN = 1;