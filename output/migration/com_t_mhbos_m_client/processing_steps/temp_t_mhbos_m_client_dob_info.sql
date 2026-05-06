DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_dob_info

/* 2.7 Create a temporary table temp_mhbos_m_client_dob_info to store the cleaned date of birth. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_dob_info (
  `client_no` STRING,
  `primary_identification_type` STRING,
  `primary_identification_no` STRING,
  `primary_identification_type_flag` STRING,
  `primary_identification_no_flag` STRING,
  `source_date_of_birth` TIMESTAMP,
  `date_of_birth` STRING,
  `date_of_birth_flag` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_dob_info /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_dob_info
SELECT
  id.client_no,
  id.primary_identification_type,
  id.primary_identification_no,
  id.primary_identification_type_flag,
  id.primary_identification_no_flag,
  mmce.date_of_birth AS source_date_of_birth,
  (
    CASE
      WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
      THEN DATE_FORMAT(
        FROM_UNIXTIME(
          UNIX_TIMESTAMP(SUBSTRING(id.primary_identification_no, 1, 6), 'yymmdd'),
          'yyyy-mm-dd'
        ),
        'yyyy-MM-dd'
      )
      WHEN NOT mmce.date_of_birth IS NULL
      THEN mmce.date_of_birth
      ELSE mmce.date_of_birth
    END
  ) AS date_of_birth,
  (
    CASE
      WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
      THEN '0'
      WHEN NOT mmce.date_of_birth IS NULL
      THEN '0'
      ELSE '0'
    END
  ) AS date_of_birth_flag
FROM ${com_schema}.temp_t_mhbos_m_client_identification_info AS id
CROSS JOIN ${com_schema}.temp_t_mhbos_m_client_all AS mmce
WHERE
  id.client_no = mmce.client_no