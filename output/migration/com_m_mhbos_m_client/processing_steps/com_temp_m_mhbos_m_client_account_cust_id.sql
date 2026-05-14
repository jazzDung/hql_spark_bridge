DROP TABLE IF EXISTS ${com_schema}.temp_m_mhbos_m_client_account_cust_id;

/*
===================================== GET FULL ACCOUNT LIST WITH ASSOCIATED CUST ID =====================================
*/
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_m_mhbos_m_client_account_cust_id (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `etl_timestamp` TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transactional'='true',
  'transactional_properties'='default'
);

/*
JOIN dataset 'Unique Customer WITH Cust ID' WITH t
using JOIN key Primary ID + Cust Name to get full list of accounts WITH respective cust id
*/
WITH unique_customer_with_cust_id AS (
  SELECT
    *
  FROM ${com_schema}.temp_m_mhbos_m_client_match_cust_id
  UNION ALL
  SELECT
    *
  FROM ${com_schema}.temp_m_mhbos_m_client_new_cust_id
), t AS (
  SELECT
    *
  FROM ${com_schema}.t_mhbos_m_client
  WHERE
    etl_dt = '${batch_date}'
    AND /* Remove accounts WITH exception in Primary ID type */ NOT PRIMARY_IDENTIFICATION_TYPE LIKE '@[%]'
    AND /* Remove accounts WITH exception in Primary ID number */ NOT PRIMARY_IDENTIFICATION_NO LIKE '@[%]'
    AND /* 2025/09/08: Remove accounts with exception in account number */ NOT CLIENT_NO LIKE '@[%]'
    AND /* 2025/09/08: Remove accounts WITH exception in customer name */ NOT CUSTOMER_NAME LIKE '@[%]'
)
INSERT INTO ${com_schema}.temp_m_mhbos_m_client_account_cust_id
SELECT
  COALESCE(cust1.cust_id, cust2.cust_id) AS cust_id,
  t.client_no,
  COALESCE(cust1.clean_rule_flag, cust2.clean_rule_flag) AS clean_rule_flag,
  t.primary_identification_type,
  t.primary_identification_no,
  t.secondary_identification_type,
  t.secondary_identification_no,
  t.customer_name,
  t.etl_timestamp
FROM t
LEFT JOIN unique_customer_with_cust_id AS cust1
  ON t.primary_identification_no = cust1.primary_identification_no
  AND /* Remove accounts WITH exception in customer name */ t.customer_name = cust1.customer_name
LEFT JOIN (
  SELECT
    cust_id,
    clean_rule_flag,
    secondary_identification_no,
    customer_name,
    ROW_NUMBER() OVER (PARTITION BY secondary_identification_no, customer_name ORDER BY cust_id DESC) AS rn
  FROM unique_customer_with_cust_id
  WHERE
    TRIM(COALESCE(secondary_identification_no, '')) <> ''
) AS cust2
  ON t.primary_identification_no = cust2.secondary_identification_no
  AND /* Remove accounts WITH exception in customer name */ t.customer_name = cust2.customer_name
  AND cust2.rn = 1
WHERE
  NOT COALESCE(cust1.cust_id, cust2.cust_id) IS NULL;