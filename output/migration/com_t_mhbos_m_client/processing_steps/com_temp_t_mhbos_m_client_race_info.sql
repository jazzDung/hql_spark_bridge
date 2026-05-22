DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_race_info;

/* 2.11 Create a temporary table temp_mhbos_m_client_race_info to store the cleaned race information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_race_info (
  `client_no` STRING,
  `source_race` STRING,
  `race` STRING,
  `race_flag` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_race_info /* 2.1.1 ddl-insert-sundexin */;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_race_info
SELECT
  mmca.client_no,
  mmca.race AS source_race,
  (
    CASE
      WHEN mmca.race = 'C'
      THEN 'CHINESE'
      WHEN mmca.race = 'F'
      THEN 'FOREIGNER'
      WHEN mmca.race = 'I'
      THEN 'INDIAN'
      WHEN mmca.race = 'M'
      THEN 'MALAY'
      WHEN mmca.race = 'O'
      THEN 'OTHERS'
      WHEN mmca.race = 'B'
      THEN 'BUMIPUTRA'
      WHEN cn2.customer_name RLIKE '(^| )A/P( |$)'
      THEN 'INDIAN'
      WHEN cn2.customer_name RLIKE '(^| )A/L( |$)'
      THEN 'INDIAN'
      WHEN cn2.customer_name RLIKE '(^| )S/O( |$)'
      THEN 'INDIAN'
      WHEN cn2.customer_name RLIKE '(^| )D/O( |$)'
      THEN 'INDIAN'
      ELSE NULL
    END
  ) AS race,
  '0' AS race_flag
FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmca
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_customer_name_2 AS cn2
  ON mmca.client_no = cn2.client_no;