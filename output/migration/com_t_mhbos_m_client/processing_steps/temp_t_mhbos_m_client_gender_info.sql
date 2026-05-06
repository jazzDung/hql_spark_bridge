DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_gender_info

/* 2.8 Create a temporary table temp_mhbos_m_client_gender_info to store the cleaned gender. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_gender_info (
  `client_no` STRING,
  `primary_identification_type` STRING,
  `primary_identification_no` STRING,
  `primary_identification_type_flag` STRING,
  `primary_identification_no_flag` STRING,
  `source_sex` STRING,
  `sex` STRING,
  `sex_flag` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_gender_info /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_gender_info
SELECT
  id.client_no,
  id.primary_identification_type,
  id.primary_identification_no,
  id.primary_identification_type_flag,
  id.primary_identification_no_flag,
  mmca.sex AS source_sex,
  (
    CASE
      WHEN COALESCE(TRIM(mmca.sex), '') <> ''
      THEN (
        CASE
          WHEN mmca.sex = 'F'
          THEN 'FEMALE'
          WHEN mmca.sex = 'M'
          THEN 'MALE'
          ELSE '@[' || mmca.sex || ']'
        END
      )
      WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
      THEN (
        CASE
          WHEN SUBSTRING(id.primary_identification_no, 12, 1) IN ('1', '3', '5', '7', '9')
          THEN 'MALE'
          ELSE 'FEMALE'
        END
      )
      ELSE ''
    END
  ) AS sex,
  (
    CASE
      WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
      THEN '0'
      WHEN COALESCE(TRIM(mmca.sex), '') <> ''
      THEN (
        CASE WHEN mmca.sex = 'F' THEN '0' WHEN mmca.sex = 'M' THEN '0' ELSE '1' END
      )
      ELSE '0'
    END
  ) AS sex_flag
FROM ${com_schema}.temp_t_mhbos_m_client_identification_info AS id
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_all AS mmca
  ON id.client_no = mmca.client_no