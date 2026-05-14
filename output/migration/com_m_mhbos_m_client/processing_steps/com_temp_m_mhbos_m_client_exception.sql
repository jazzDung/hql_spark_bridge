DROP TABLE IF EXISTS ${com_schema}.temp_m_mhbos_m_client_exception;

/*
===================================== GET EXCEPTION RECORDS =====================================
*/
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_m_mhbos_m_client_exception (
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
  `customer_name_original` VARCHAR(250),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` TIMESTAMP
);

/* Step 3: Get all records WITH exception */
/* Find exception records

Step 1: Find records WHERE cust_id does NOT match WITH mapping
Step 2: Flag exception for Primary ID WITH > 1 distinct customer name
    - Update clean_rule_flag
    - Put Customer name inside '@[]'
Step 3: Get all records WITH exception in
    - primary_identification_no
    - primary_identification_type
    - customer_name
*/
WITH missing_cust_id /* Step 1: Find records WHERE cust_id does NOT match WITH mapping */ AS (
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
    mg.etl_timestamp
  FROM ${com_schema}.temp_m_mhbos_m_client_merge AS mg
  LEFT JOIN ${com_schema}.temp_m_mhbos_m_client_match_cust_id AS mmc
    ON mg.client_no = mmc.client_no
  WHERE
    mmc.cust_id IS NULL
), customer_name_exception /* Step 2: Flag exception for Primary ID WITH > 1 distinct customer name */ AS (
  SELECT
    t0.client_no, /* Update clean_rule_flag */
    t0.clean_rule_flag || IF(t1.primary_identification_no IS NULL, '0', '1') || '0' AS clean_rule_flag,
    t0.primary_identification_type,
    t0.primary_identification_no,
    t0.secondary_identification_type,
    t0.secondary_identification_no,
    t0.primary_identification_type_consolidated,
    t0.primary_identification_no_consolidated,
    t0.secondary_identification_type_consolidated,
    t0.secondary_identification_no_consolidated, /* Backup the original customer_name */
    t0.customer_name AS customer_name_original, /* Put Customer name inside '@[]' */
    IF(
      t1.primary_identification_no IS NULL OR t0.customer_name LIKE '@[%]',
      t0.customer_name,
      '@[' || t0.customer_name || ']'
    ) AS customer_name,
    t0.date_created,
    t0.date_closed,
    t0.date_change,
    t0.etl_timestamp
  FROM missing_cust_id AS t0
  LEFT JOIN (
    /* Find Primary ID WITH > 1 distinct customer name */
    SELECT
      primary_identification_no
    FROM ${com_schema}.temp_m_mhbos_m_client_merge
    /* Only mark customer name exception for active records */
    WHERE
      date_closed IS NULL
    GROUP BY
      primary_identification_no
    HAVING
      COUNT(*) > 1
  ) AS t1
    ON t0.primary_identification_no = t1.primary_identification_no
)
INSERT INTO ${com_schema}.temp_m_mhbos_m_client_exception
SELECT
  NULL AS cust_id,
  client_no,
  clean_rule_flag,
  primary_identification_type,
  primary_identification_no,
  secondary_identification_type,
  secondary_identification_no,
  primary_identification_type_consolidated,
  primary_identification_no_consolidated,
  secondary_identification_type_consolidated,
  secondary_identification_no_consolidated,
  customer_name_original,
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM customer_name_exception
WHERE
  customer_name LIKE '@[%]'
  OR primary_identification_no LIKE '@[%]'
  OR primary_identification_type LIKE '@[%]';