DROP TABLE IF EXISTS ${com_schema}.temp_m_mhbos_m_client_match_cust_id

/*
===================================== LOOKUP EXISTING CUST ID FROM MAPPING =====================================
*/
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_m_mhbos_m_client_match_cust_id (
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

/*
Step 1: Get latest mapping based ON
1. primary_identification_no, exclude exception for
    - primary_identification_no
    - customer_name
2. secondary_identification_no, exclude exception for
    - secondary_identification_no
    - customer_name
3. source_owner_id, exclude exception for
    - primary_identification_no
    - customer_name

Step 2: JOIN WITH latest mapping, matched cust_id will be selected based ON JOIN priority:
1. primary_identification_no = primary_identification_no
2. primary_identification_no = secondary_identification_no
3. secondary_identification_no = primary_identification_no
4. secondary_identification_no = secondary_identification_no
5. source_owner_id = source_owner_id
*/
WITH latest_primary_identification_no /* latest mapping based ON primary_identification_no */ AS (
  SELECT
    COALESCE(t.primary_identification_no, mapping.primary_identification_no) AS primary_identification_no,
    COALESCE(t.customer_name, mapping.customer_name) AS customer_name,
    mapping.cust_id,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(t.primary_identification_no, mapping.primary_identification_no), COALESCE(t.customer_name, mapping.customer_name)
      ORDER BY mapping.cust_id, mapping.priority_level
    ) AS rn
  FROM ${com_schema}.m_customer_id_mapping AS mapping
  LEFT JOIN ${com_schema}.t_mhbos_m_client AS t
    ON mapping.source_owner_id = t.client_no AND t.etl_dt = '${batch_date}'
  WHERE
    TRIM(COALESCE(t.primary_identification_no, mapping.primary_identification_no, '')) /* exclude exception for primary_identification_no */ <> ''
    AND NOT TRIM(COALESCE(t.primary_identification_no, mapping.primary_identification_no, '')) LIKE '@[%]'
    AND /* exclude exception for customer_name */ TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) <> ''
    AND NOT TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) LIKE '@[%]'
), latest_secondary_identification_no /* latest mapping based ON secondary_identification_no */ AS (
  SELECT
    COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) AS secondary_identification_no,
    COALESCE(t.customer_name, mapping.customer_name) AS customer_name,
    mapping.cust_id,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
        NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
      ), COALESCE(t.customer_name, mapping.customer_name)
      ORDER BY mapping.cust_id, mapping.priority_level
    ) AS rn
  FROM ${com_schema}.m_customer_id_mapping AS mapping
  LEFT JOIN ${com_schema}.t_mhbos_m_client AS t
    ON mapping.source_owner_id = t.client_no AND t.etl_dt = '${batch_date}'
  WHERE
    COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) /* exclude exception for secondary_identification_no */ <> ''
    AND NOT COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) LIKE '@[%]'
    AND /* exclude exception for customer_name */ TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) <> ''
    AND NOT TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) LIKE '@[%]'
), latest_source_owner_id /* latest mapping based ON source_owner_id */ AS (
  SELECT
    source_owner_id,
    cust_id,
    ROW_NUMBER() OVER (PARTITION BY source_owner_id ORDER BY cust_id, priority_level) AS rn
  FROM ${com_schema}.m_customer_id_mapping
  WHERE
    TRIM(COALESCE(source_owner_id, '')) /* exclude exception for primary_identification_no */ <> ''
)
INSERT INTO ${com_schema}.temp_m_mhbos_m_client_match_cust_id
SELECT
  COALESCE(mmc1.cust_id, mmc2.cust_id, mmc3.cust_id, mmc4.cust_id, mmc5.cust_id) AS cust_id,
  mg.client_no, /* 2025/06/12: add another '0' for closed account */
  mg.clean_rule_flag || '00' AS clean_rule_flag,
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
/*
    cust_id will be get based ON JOIN priority:
        1. primary_identification_no = primary_identification_no
        2. primary_identification_no = secondary_identification_no
        3. secondary_identification_no = primary_identification_no
        4. secondary_identification_no = secondary_identification_no
        5. source_owner_id = source_owner_id
     */
LEFT JOIN latest_primary_identification_no AS mmc1
  ON mg.primary_identification_no_consolidated = mmc1.primary_identification_no
  AND mg.customer_name = mmc1.customer_name
  AND mmc1.rn = 1
LEFT JOIN latest_secondary_identification_no AS mmc2
  ON mg.primary_identification_no_consolidated = mmc2.secondary_identification_no
  AND mg.customer_name = mmc2.customer_name
  AND mmc2.rn = 1
LEFT JOIN latest_primary_identification_no AS mmc3
  ON mg.secondary_identification_no_consolidated = mmc3.primary_identification_no
  AND mg.customer_name = mmc3.customer_name
  AND mmc3.rn = 1
LEFT JOIN latest_secondary_identification_no AS mmc4
  ON mg.secondary_identification_no_consolidated = mmc4.secondary_identification_no
  AND mg.customer_name = mmc4.customer_name
  AND mmc4.rn = 1
LEFT JOIN latest_source_owner_id AS mmc5
  ON mg.client_no = mmc5.source_owner_id AND mmc5.rn = 1
WHERE
  NOT COALESCE(mmc1.cust_id, mmc2.cust_id, mmc3.cust_id, mmc4.cust_id, mmc5.cust_id) IS NULL