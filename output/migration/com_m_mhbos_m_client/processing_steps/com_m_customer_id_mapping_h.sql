/*
===================================== LOOKUP SOURCE_OWNER_ID IN MAPPING =====================================
*/ /*
IF these conditions happen
1. source_owner_id exist in mapping
2. Source_owner_id belong to mapping WITH same source name
3. Existing mapping require update in at least 1 of these fields
    - cust_id
    - primary_identification_no
    - secondary_identification_no
    - customer_name
*/ /* Step 1: Insert existing mapping to history TABLE */
INSERT INTO ${com_schema}.m_customer_id_mapping_h
SELECT
  'MHBOS_M_CLIENT' AS update_source,
  mapping.cust_id,
  mapping.source_owner_id,
  mapping.primary_identification_no,
  mapping.secondary_identification_no,
  mapping.customer_name,
  mapping.source_name,
  mapping.priority_level,
  mapping.etl_timestamp AS start_timestamp,
  '${batch_timestamp}' AS end_timestamp,
  mapping.customer_type AS customer_type
FROM ${com_schema}.m_customer_id_mapping AS mapping
JOIN ${com_schema}.temp_m_mhbos_m_client_account_cust_id AS acc
  ON mapping.source_owner_id = acc.client_no
WHERE
  mapping.source_name = 'MHBOS_M_CLIENT'
  AND /* But value must be updated ON at least 1 column */ (
    mapping.cust_id <> acc.cust_id
    OR mapping.primary_identification_no <> acc.primary_identification_no
    OR mapping.secondary_identification_no <> acc.secondary_identification_no
    OR mapping.customer_name <> acc.customer_name
  );