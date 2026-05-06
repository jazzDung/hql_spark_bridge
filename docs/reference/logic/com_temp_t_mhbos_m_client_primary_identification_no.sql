

/* 2.3 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification number. */
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no (
  `client_no` STRING,
  `id_type` STRING,
  `ic_no_new` STRING,
  `ic_no_old` STRING,
  `secondary_id_type` STRING,
  `secondary_id_no` STRING,
  `primary_identification_no` STRING,
  `primary_identification_no_flag` STRING
)


TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no


INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no
SELECT
  mmc.client_no,
  mmc.id_type,
  mmc.ic_no_new,
  mmc.ic_no_old,
  mmc.secondary_id_type,
  mmc.secondary_id_no,
  (
    CASE
      WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '3'
      AND LENGTH(mmc.ic_no_new) < 12
      AND LENGTH(mmc.secondary_id_no) >= 12
      AND LENGTH(mmc.secondary_id_no) <= 15
      THEN mmc.secondary_id_no /* updated 20250502 */
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') <> ''
      AND LENGTH(mmc.secondary_id_no) < 12
      THEN mmc.ic_no_new /* updated 20250502 */
      WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
      THEN mmc.ic_no_new /* updated 20250502 */
      WHEN mmc.id_type = '4'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '5'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '6'
      THEN mmc.ic_no_new
      WHEN COALESCE(mmc.id_type, '') = ''
      AND COALESCE(mmc.secondary_id_type, '') <> ''
      AND COALESCE(mmc.secondary_id_no, '') <> ''
      THEN mmc.secondary_id_no
      ELSE '@[' || mmc.ic_no_new || ']'
    END
  ) AS primary_identification_no,
  (
    CASE
      WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
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
      WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
      THEN '0'
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
      THEN '0'
      WHEN mmc.id_type = '3'
      AND LENGTH(mmc.ic_no_new) < 12
      AND LENGTH(mmc.secondary_id_no) >= 12
      AND LENGTH(mmc.secondary_id_no) <= 15
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') <> ''
      AND LENGTH(mmc.secondary_id_no) < 12
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
      THEN '0' /* updated 20250502 */
      WHEN mmc.id_type = '4'
      THEN '0'
      WHEN mmc.id_type = '5'
      THEN '0'
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN '0'
      WHEN mmc.id_type = '6'
      THEN '0'
      WHEN COALESCE(mmc.id_type, '') = ''
      AND COALESCE(mmc.secondary_id_type, '') <> ''
      AND COALESCE(mmc.secondary_id_no, '') <> ''
      THEN '0'
      ELSE '1'
    END
  ) AS primary_identification_no_flag
FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
