DROP TABLE IF EXISTS ${com_schema}.temp_m_mhbos_m_client_new_cust_id

/*
===================================== GENERATE NEW CUST_ID =====================================
*/
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_m_mhbos_m_client_new_cust_id (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `primary_identification_type_consolidated` VARCHAR(10),
  `primary_identification_no_consolidated` VARCHAR(60),
  `secondary_identification_type_consolidated` VARCHAR(10),
  `secondary_identification_no_consolidated` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` TIMESTAMP
)

/* Step 2: Generate New Cust ID */
/* Generate new cust_id
Step 1: Get customer records that
    - Don't have existing cust_id
    - Don't have exceptions
Step 2: Generate New Cust ID

Note: cust_id format
    - Non-Rakuten format: `CUS000000000`.
    - Rakuten format: `RC000000000`.
 */
WITH missing_cust_id /* Step 1: Get customer records that don't have existing cust_id AND don't have exceptions */ AS (
  SELECT
    mg.client_no,
    mg.clean_rule_flag,
    mg.primary_identification_type,
    mg.primary_identification_no,
    mg.secondary_identification_type,
    mg.secondary_identification_no,
    mg.primary_identification_type_consolidated,
    mg.primary_identification_no_consolidated,
    mg.secondary_identification_type_consolidated,
    mg.secondary_identification_no_consolidated,
    mg.customer_name,
    mg.date_created,
    mg.date_closed,
    mg.date_change,
    mg.etl_timestamp,
    t0.max_cust_id_number
  FROM ${com_schema}.temp_m_mhbos_m_client_merge AS mg
  CROSS JOIN (
    /* Minor readability improvement to max cust_id number logic */
    SELECT
      COALESCE(MAX(CAST(SUBSTRING(cust_id, 4) AS INT)), 0) AS max_cust_id_number
    FROM ${com_schema}.m_customer_id_mapping
  ) AS t0
  /* Criteria 1: Records that did NOT find cust_id FROM mapping */
  LEFT JOIN ${com_schema}.temp_m_mhbos_m_client_match_cust_id AS matched
    ON mg.client_no = matched.client_no
  /* Criteria 2: Records that did NOT have an exception */
  LEFT JOIN ${com_schema}.temp_m_mhbos_m_client_exception AS exception
    ON mg.client_no = exception.client_no
  WHERE
    matched.cust_id IS NULL AND exception.client_no IS NULL
)
INSERT INTO ${com_schema}.temp_m_mhbos_m_client_new_cust_id
SELECT
  IF(
    date_closed IS NULL,
    'CUS' || LPAD(
      max_cust_id_number + ROW_NUMBER() OVER (
        ORDER BY GREATEST(COALESCE(date_created, '1900-01-01'), COALESCE(date_change, '1900-01-01')), client_no
      ),
      8,
      0
    ),
    NULL
  ) AS cust_id,
  client_no, /*
     New flag to depict closed account without cust_id assigned -> Do not insert to mapping / curated tables
     */
  clean_rule_flag || '0' || IF(date_closed IS NULL, '0', '1') AS clean_rule_flag,
  primary_identification_type,
  primary_identification_no,
  secondary_identification_type,
  secondary_identification_no,
  primary_identification_type_consolidated,
  primary_identification_no_consolidated,
  secondary_identification_type_consolidated,
  secondary_identification_no_consolidated,
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM missing_cust_id