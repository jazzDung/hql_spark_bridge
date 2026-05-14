/* 2.1 insert data to target table */
INSERT INTO ${com_schema}.r_mhbos_m_client_crs PARTITION(part_id = '${batch_yyyymm}')
SELECT
  client_no,
  controlling_name,
  country_tax_residence,
  tax_identification_no,
  reason,
  reason_remarks,
  entity_type,
  crs_tax_type,
  '${batch_timestamp}' AS etl_timestamp,
  '${batch_date}' AS etl_dt
FROM ${raw_schema}.mhbos_m_client_crs
WHERE
  etl_dt = '${batch_date}'
UNION ALL
SELECT
  client_no,
  controlling_name,
  country_tax_residence,
  tax_identification_no,
  reason,
  reason_remarks,
  entity_type,
  crs_tax_type,
  etl_timestamp,
  etl_dt
FROM ${com_schema}.r_mhbos_m_client_crs_bf;