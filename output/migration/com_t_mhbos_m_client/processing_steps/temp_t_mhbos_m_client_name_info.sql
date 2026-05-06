DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_name_info

/* create temporary table temp_t_mhbos_m_client_name_info 				Add 20240321 */
DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_name_info

CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_name_info (
  `client_no` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `customer_name_concatenate` STRING,
  `org_customer_name` STRING,
  `customer_name` STRING,
  `customer_name_flag` STRING,
  `org_primary_identification_type` STRING,
  `primary_identification_type` STRING,
  `org_primary_identification_no` STRING,
  `primary_identification_no` STRING,
  `org_secondary_identification_type` STRING,
  `secondary_identification_type` STRING,
  `org_secondary_identification_no` STRING,
  `secondary_identification_no` STRING,
  `org_principal_name` STRING,
  `principal_name` STRING,
  `org_beneficiary_name` STRING,
  `beneficiary_name` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_name_info /* trunate temporary table temp_t_mhbos_m_client_name_info */

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info		add 20240321 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_name_info
SELECT
  t2.client_no,
  t2.client_name,
  t2.client_name1,
  t2.client_name2,
  t2.client_name3,
  t2.customer_name_concatenate,
  t2.org_customer_name, /* 2025/09/08: Add customer_name_flag */
  CASE
    WHEN TRIM(COALESCE(t2.Customer_Name, '')) <> ''
    THEN TRIM(COALESCE(t2.Customer_Name, ''))
    ELSE '@[' || TRIM(COALESCE(t2.Customer_Name, '')) || ']'
  END AS customer_name, /* 2025/09/08: Add customer_name_flag */
  CASE WHEN TRIM(COALESCE(t2.Customer_Name, '')) <> '' THEN '0' ELSE '1' END AS customer_name_flag,
  t2.primary_identification_type AS org_primary_identification_type, /* added 20250313 */
  CASE
    WHEN NOT l1.primary_id_type IS NULL
    THEN l1.primary_id_type /* added 20250313 */
    ELSE t2.primary_identification_type
  END AS primary_identification_type,
  t2.org_primary_identification_no,
  CASE
    WHEN NOT l1.primary_id_no IS NULL
    THEN l1.primary_id_no
    ELSE t2.primary_identification_no
  END AS primary_identification_no,
  t2.secondary_identification_type AS org_secondary_identification_type, /* added 20250620 */
  CASE
    WHEN NOT l1.secondary_id_type IS NULL
    THEN l1.secondary_id_type /* added 20250620 */
    ELSE t2.secondary_identification_type
  END AS secondary_identification_type,
  t2.secondary_identification_no AS org_secondary_identification_no, /* added 20250620 */
  CASE
    WHEN NOT l1.secondary_id_no IS NULL
    THEN l1.secondary_id_no /* added 20250620 */
    ELSE t2.secondary_identification_no
  END AS secondary_identification_no,
  t2.org_principal_name,
  t2.principal_name,
  t2.org_beneficiary_name,
  t2.beneficiary_name
FROM (
  SELECT
    t1.client_no,
    t1.client_name,
    t1.client_name1,
    t1.client_name2,
    t1.client_name3,
    t1.customer_name_concatenate,
    t1.customer_name AS org_customer_name,
    TRIM(
      CASE
        WHEN NOT l2.cust_name IS NULL
        THEN l2.cust_name /* add new mapping 20240828 */
        WHEN NOT l3.new_cust_name IS NULL
        THEN l3.new_cust_name /* add new mapping 20240828 */
        WHEN NOT l4.new_cust_name IS NULL
        THEN l4.new_cust_name /* add new mapping 20250313 */
        WHEN NOT l5.cust_name IS NULL
        THEN l5.cust_name /* add new mapping 20250620 */
        WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KIBB%'
        THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240815 */
        WHEN t1.customer_name_concatenate LIKE '%RSS/ SBL FOR KIBB%'
        THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240828 */
        WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%'
        THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240828 */
        WHEN t1.customer_name RLIKE '.+\\(.*\\)$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(t1.customer_name, '\\s*\\([^)]*\\)$', ''),
          '^(SMT\\s+|INTRADAY A/C\\s+)',
          ''
        ) /* add 20240518 --20241118 changes */
        WHEN t1.customer_name_concatenate LIKE '%SHARE BUY%'
        THEN REGEXP_REPLACE(t1.customer_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') /* added logic 20240828 */
        ELSE REGEXP_REPLACE(t1.customer_name, '^(SMT\\s+|INTRADAY A/C\\s+)', '')
      END
    ) AS customer_name,
    t1.primary_identification_type, /* added 20250313 */
    t1.primary_identification_no AS org_primary_identification_no,
    t1.primary_identification_no,
    t1.secondary_identification_type, /* added 20250620 */
    t1.secondary_identification_no, /* added 20250620 */
    t1.principal_name AS org_principal_name,
    TRIM(
      CASE
        WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KIBB%'
        THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240815 */
        WHEN t1.customer_name_concatenate LIKE '%RSS/ SBL FOR KIBB%'
        THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240828 */
        WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%'
        THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240828 */
        WHEN t1.principal_name RLIKE '.+\\(.*\\)$'
        THEN REGEXP_REPLACE(t1.principal_name, '\\s*\\([^)]*\\)$', '') /* add 20240518 --20241118 changes */
        WHEN t1.customer_name_concatenate LIKE '%SHARE BUY%'
        THEN REGEXP_REPLACE(t1.principal_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') /* added logic 20240828 */
        ELSE t1.principal_name
      END
    ) AS principal_name,
    t1.beneficiary_name AS org_beneficiary_name,
    CASE
      WHEN t1.beneficiary_name RLIKE '.+\\(.*\\)$'
      THEN REGEXP_REPLACE(t1.beneficiary_name, '\\s*\\([^)]*\\)$', '')
      ELSE t1.beneficiary_name
    END AS beneficiary_name /* add 20240518 --20241118 changes */
  FROM (
    SELECT
      mmca.client_no,
      cn.client_name,
      cn.client_name1,
      cn.client_name2,
      cn.client_name3,
      cn.customer_name_concatenate,
      cn.customer_name AS org_customer_name, /*             ,case when nvl(nom.noms_ind, 'Y') = 'Y' and nvl(trim(nom.principal_name), '') <> '' then nom.principal_name */
      CASE
        WHEN COALESCE(TRIM(nom.principal_name), '') <> ''
        THEN nom.principal_name
        WHEN COALESCE(TRIM(cn.customer_name), '') = ''
        AND COALESCE(TRIM(cn.client_name), '') <> ''
        THEN cn.client_name
        WHEN COALESCE(TRIM(cn.customer_name), '') = ''
        AND COALESCE(TRIM(cn.client_name), '') = ''
        THEN '@[]'
        ELSE cn.customer_name
      END AS customer_name,
      id.primary_identification_type, /* added 20250313 */
      id.primary_identification_no,
      id.secondary_identification_type, /* added 20250620 */
      id.secondary_identification_no, /* added 20250620 */
      nom.principal_name,
      nom.beneficiary_name
    FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmca
    LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_customer_name_2 AS cn
      ON mmca.client_no = cn.client_no
    LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_nominees_info AS nom
      ON mmca.client_no = nom.client_no
    LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_identification_info AS id
      ON mmca.client_no = id.client_no
  ) AS t1
  LEFT JOIN ${com_schema}.r_lookup_clientno_custname AS l2
    ON t1.client_no = l2.client_no AND l2.source_system = 'MHBOS'
  LEFT JOIN ${com_schema}.r_lookup_custname_custname AS l3
    ON t1.customer_name = l3.cust_name AND l3.source_system = 'MHBOS'
  LEFT JOIN ${com_schema}.r_lookup_primaryidno_newcustname AS l4 /* added 20250313 */
    ON t1.primary_identification_no = l4.primary_id_no AND l4.source_system = 'MHBOS'
  LEFT JOIN ${com_schema}.r_lookup_custname_primaryidno AS l5 /* added 20250620 */
    ON t1.primary_identification_no = l5.primary_id_no AND l5.source_system = 'MHBOS'
) AS t2
LEFT JOIN ${com_schema}.r_lookup_custname_primaryidno AS l1
  ON t2.customer_name = l1.cust_name AND l1.source_system = 'MHBOS'