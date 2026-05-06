



/* 2.5 Create a temporary table temp_mhbos_m_client_secondary_identification_no to store the cleaned secondary identification number. */
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no (
  `client_no` STRING,
  `id_type` STRING,
  `ic_no_new` STRING,
  `ic_no_old` STRING,
  `secondary_id_type` STRING,
  `secondary_id_no` STRING,
  `secondary_identification_no` STRING,
  `secondary_identification_no_flag` STRING
)


TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no


INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no
SELECT
  mmc.client_no,
  mmc.id_type,
  mmc.ic_no_new,
  mmc.ic_no_old,
  mmc.secondary_id_type,
  mmc.secondary_id_no,
  (
    CASE
      WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_old, '') <> ''
      THEN mmc.ic_no_old
      WHEN mmc.id_type = '1' AND mmc.secondary_id_type = '1'
      THEN ''
      WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '2'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '4'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '5'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '6'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2'
      AND mmc.secondary_id_type = '1'
      AND COALESCE(mmc.ic_no_old, '') <> ''
      THEN mmc.ic_no_old
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '2'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
      THEN mmc.ic_no_new
      WHEN mmc.id_type = '3'
      AND mmc.secondary_id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
      THEN ''
      WHEN mmc.id_type = '3'
      AND mmc.secondary_id_type = '3'
      AND LENGTH(mmc.ic_no_new) < 12
      AND LENGTH(mmc.secondary_id_no) >= 12
      AND LENGTH(mmc.secondary_id_no) <= 15
      THEN mmc.ic_no_new /* updated 20250502 */
      WHEN mmc.id_type = '3'
      AND mmc.secondary_id_type = '3'
      AND COALESCE(mmc.ic_no_new, '') <> COALESCE(mmc.secondary_id_no, '')
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '3' AND mmc.secondary_id_type <> '3'
      THEN mmc.secondary_id_no
      WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
      THEN mmc.ic_no_new
      WHEN COALESCE(mmc.id_type, '') = ''
      AND COALESCE(mmc.secondary_id_type, '') <> ''
      AND COALESCE(mmc.secondary_id_no, '') <> ''
      THEN ''
      WHEN COALESCE(TRIM(mmc.secondary_id_no), '') = ''
      THEN ''
      ELSE mmc.secondary_id_no
    END
  ) AS secondary_identification_no,
  '0' /*
       (case when mmc.id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0' 
             when mmc.id_type = '1' and mmc.secondary_id_type = '1' then '0'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '2' then '0'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '4' then '0'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '5' then '0'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '6' then '0'
			 when mmc.id_type = '2' and mmc.secondary_id_type = '2' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '0'
			 when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '0'
             when mmc.id_type = '3' then '0'
			 when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '0'
			 when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then '0'
			 when nvl(trim(mmc.secondary_id_no), '') = '' then '0'
             else '0'
         end )
      */ AS secondary_identification_no_flag /* 20250620 */
FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
