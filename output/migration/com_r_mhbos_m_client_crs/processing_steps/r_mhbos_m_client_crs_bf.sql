/* 1.0 backup month data without batch_date */
CREATE TABLE IF NOT EXISTS ${com_schema}.r_mhbos_m_client_crs_bf
STORED AS PARQUET
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'external.table.purge'='true'
) AS
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
  etl_dt,
  part_id
FROM ${com_schema}.r_mhbos_m_client_crs
WHERE
  etl_dt <> '${batch_date}' AND part_id = '${batch_yyyymm}';

/* 3.1 truncate temp table */
DROP TABLE ${com_schema}.r_mhbos_m_client_crs_bf;