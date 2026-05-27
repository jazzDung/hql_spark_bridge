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

/* ==============[Group.4 KDI]============== */
WITH m_base AS (
  SELECT
    m.*,
    REGEXP_REPLACE(COALESCE(m.TIN_NUMBERS, ''), '\\s*,\\s*', ',') AS tin_norm,
    REGEXP_REPLACE(COALESCE(m.country_of_tax_residences_code, ''), '\\s*,\\s*', ',') AS ctry_code_norm
  FROM ${com_schema}.M_KDI_CLIENTREPORT AS m
  WHERE
    m.ETL_DT = '${batch_date}'
), m_arr AS (
  SELECT
    b.*,
    SPLIT(b.tin_norm, ',') AS tin_arr,
    SPLIT(b.ctry_code_norm, ',') AS ctry_arr,
    SIZE(SPLIT(b.tin_norm, ',')) AS tin_len,
    SIZE(SPLIT(b.ctry_code_norm, ',')) AS ctry_len
  FROM m_base AS b
), m_expanded AS (
  SELECT
    a.ACCOUNT_NO,
    a.CLIENT_ID,
    a.NAME,
    a.IDENTIFICATION_TYPE,
    a.NRIC_PASSPORT,
    a.SECONDARY_ID_NO_TYPE,
    a.SECONDARY_ID_NO,
    a.NATIONALITY,
    a.NATIONALITY_CODE,
    a.COUNTRY_OF_RESIDENCE,
    a.COUNTRY_OF_RESIDENCE_CODE,
    a.COMPANY_PLACE_INCORPORATION,
    a.COMPANY_PLACE_BUSINESS,
    a.OCCUPATION,
    a.BUSINESS_TYPE_INDUSTRY,
    a.NET_WORTH,
    a.SST_REGISTRATION_NUMBER,
    a.Record_Updated_Date,
    tin_lv.pos AS pos_tin,
    TRIM(tin_lv.tin) AS tin_raw,
    REGEXP_REPLACE(TRIM(tin_lv.tin), '[^0-9A-Za-z]', '') AS tin_clean,
    UPPER(TRIM(a.ctry_arr[tin_lv.pos])) AS country_code_raw,
    UPPER(TRIM(a.ctry_arr[tin_lv.pos])) AS country_code_upper,
    a.ETL_TIMESTAMP
  FROM m_arr AS a
  LATERAL VIEW
  POSEXPLODE(a.tin_arr) tin_lv AS pos, tin
  WHERE
    tin_lv.pos < a.ctry_len
    AND TRIM(COALESCE(tin_lv.tin, '')) <> ''
    AND TRIM(COALESCE(a.ctry_arr[tin_lv.pos], '')) <> ''
    AND REGEXP_REPLACE(TRIM(tin_lv.tin), '[^0-9A-Za-z]', '') <> ''
), ref_ccris AS (
  SELECT
    UPPER(TRIM(y.country_code_ccris)) AS country_code_ccris,
    y.country_name,
    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(y.country_code_ccris)) ORDER BY y.country_name) AS rn
  FROM ${cur_schema}.ref_country AS y
  WHERE
    y.ETL_DT = '${batch_date}'
), ref_ccris_dedup AS (
  SELECT
    country_code_ccris,
    country_name
  FROM ref_ccris
  WHERE
    rn = 1
)
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
  m.CUST_ID AS CUSTOMER_ID,
  NULL AS CRS_ENTITY_TYPE,
  NULL AS NAME_OF_CONTROLLING_PERSON_1,
  NULL AS NAME_OF_CONTROLLING_PERSON_2,
  COALESCE(rA.country_code_ccris, me.country_code_upper) AS CRS_COUNTRY,
  NULL AS CRS_TAX_RESIDENCE,
  me.tin_clean AS TAXPAYER_IDENTIFICATION_NO_1,
  NULL AS TAXPAYER_IDENTIFICATION_NO_2,
  NULL AS TIN_UNAVAILABLE_REASON,
  NULL AS TIN_REMARKS,
  'KDI' AS SOURCE_NAME,
  m.CLIENT_ID AS SOURCE_RECORD_ID,
  '${batch_timestamp}' AS ETL_TIMESTAMP,
  6 AS PRIORITY_LEVEL,
  m.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE
FROM m_base AS m
JOIN m_expanded AS me
  ON m.CLIENT_ID = me.CLIENT_ID
LEFT JOIN ref_ccris_dedup AS rA
  ON me.country_code_upper = rA.country_code_ccris
WHERE
  NOT m.IDENTIFICATION_TYPE LIKE '@[%'
  AND TRIM(COALESCE(m.IDENTIFICATION_TYPE, '')) <> '';