/* Purpose:    Customer information merge program */ /* Author:     Sunline */ /* Usage:      python $ETL_HOME/script/main.py 20230809 com_m_mhbos_m_client */ /* CreateDate: 20230816 */ /* Logs:       zhairp 20230816 create script. */ /* Logs        lixiaotian 20240702 annotating code(like 'TOMS%'/ not like 'TOMS%') */ /* 1.0 set parameter */ /* source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql; */ /* DROP all temporary tables */
DROP TABLE IF EXISTS ${com_schema}.temp_m_mhbos_m_client_merge;

/*
===================================== GET UNIQUE CUSTOMER =====================================
*/
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_m_mhbos_m_client_merge (
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
);

/*
Get Unique Customer:
Step 1: CREATE indicator for accounts WHERE Account No already exist in Mapping TABLE
Step 2: PARTITION data BY Primary ID No + Customer Name. ORDER BY Account CREATE Date (DESC) AND Account No (DESC).
 */
WITH current_date_account AS (
  SELECT
    *
  FROM ${com_schema}.t_mhbos_m_client
  WHERE
    etl_dt = '${batch_date}'
), customer_row_number AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY t.primary_identification_no, t.customer_name ORDER BY IF(NOT mapping.source_owner_id IS NULL, 1, 0) DESC /*
                Step 2: Prioritize active records, since when insert to temp_m_mhbos_m_client_new_cust_id
                We only assign cust_id for active records, closed account will be marked with closed account exception and
                won't be inserted to mapping / curated tables.
                 */, IF(date_closed IS NULL, 1, 0) DESC /* Step 3: ORDER BY Account CREATE Date (DESC) AND Account No (DESC). */, GREATEST(COALESCE(t.date_created, '1900-01-01'), COALESCE(t.date_change, '1900-01-01')) DESC, t.client_no DESC) AS rn,
    t.client_no,
    t.clean_rule_flag,
    t.primary_identification_type,
    t.primary_identification_no,
    t.secondary_identification_type,
    t.secondary_identification_no,
    t.customer_name,
    t.date_created,
    t.date_closed,
    t.date_change,
    t.etl_timestamp
  FROM current_date_account AS t
  /* Prioritize accounts WHERE Account No already exist in Mapping TABLE */
  LEFT JOIN ${com_schema}.m_customer_id_mapping AS mapping
    ON t.client_no = mapping.source_owner_id
), consolidated_id AS (
  SELECT
    m1.client_no,
    m1.clean_rule_flag,
    m1.primary_identification_type AS primary_identification_type,
    m1.primary_identification_no AS primary_identification_no,
    m1.secondary_identification_type AS secondary_identification_type,
    m1.secondary_identification_no AS secondary_identification_no,
    COALESCE(m2.primary_identification_type, m1.primary_identification_type) AS primary_identification_type_consolidated,
    COALESCE(m2.primary_identification_no, m1.primary_identification_no) AS primary_identification_no_consolidated,
    COALESCE(m2.secondary_identification_type, m1.secondary_identification_type) AS secondary_identification_type_consolidated,
    COALESCE(m2.secondary_identification_no, m1.secondary_identification_no) AS secondary_identification_no_consolidated,
    m1.customer_name,
    m1.date_created,
    m1.date_closed,
    m1.date_change,
    m1.etl_timestamp
  FROM (
    SELECT
      *
    FROM customer_row_number
    WHERE
      rn = 1
  ) AS m1
  LEFT JOIN (
    SELECT
      *
    FROM customer_row_number
    WHERE
      rn = 1
  ) AS m2
    ON m1.primary_identification_no = m2.secondary_identification_no
    AND m1.primary_identification_type = m2.secondary_identification_type
    AND m1.customer_name = m2.customer_name
    AND m1.client_no <> m2.client_no
    AND COALESCE(m2.secondary_identification_no, '') <> ''
)
INSERT INTO ${com_schema}.temp_m_mhbos_m_client_merge
SELECT
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
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM consolidated_id
WHERE
  primary_identification_no = primary_identification_no_consolidated;