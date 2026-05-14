DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_primary_identification_type;

/* 2.2 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification type. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_primary_identification_type (
  `client_no` STRING,
  `id_type` STRING,
  `secondary_id_type` STRING,
  `primary_identification_type` STRING,
  `primary_identification_type_flag` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_primary_identification_type;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_primary_identification_type
SELECT
  mmc.client_no,
  mmc.id_type,
  mmc.secondary_id_type,
  (
    CASE
      WHEN mmc.id_type = '1'
      THEN '1'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN '1'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN '3'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN '2'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN '6'
      WHEN mmc.id_type = '2'
      THEN '3'
      WHEN mmc.id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '4'
      THEN '5'
      WHEN mmc.id_type = '5'
      THEN '2'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN mmc.id_type = '6'
      THEN '6'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
      THEN '1'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
      THEN '3'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
      THEN '4'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
      THEN '5'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
      THEN '2'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
      THEN '6'
      ELSE '@[' || mmc.id_type || ']'
    END
  ) AS primary_identification_type,
  (
    CASE
      WHEN mmc.id_type = '1'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN '0'
      WHEN mmc.id_type = '2'
      THEN '0'
      WHEN mmc.id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '6'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
      THEN '0'
      ELSE '1'
    END
  ) AS primary_identification_type_flag
FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmc;