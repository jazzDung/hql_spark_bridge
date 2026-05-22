DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_email_info;

/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_info to store the cleaned email information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_email_info (
  `client_no` STRING,
  `source_email` STRING,
  `einvoice_email` STRING, /* 20250903 einvoice_email */
  `email_1` STRING,
  `email_2` STRING,
  `email_3` STRING,
  `email_4` STRING,
  `email_5` STRING,
  `email_6` STRING,
  `email_7` STRING,
  `email_8` STRING,
  `email_9` STRING,
  `email_10` STRING,
  `email_flag` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_email_info /* 2.1.1 ddl-insert-sundexin */;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_email_info
SELECT
  t.client_no,
  t.source_email,
  t.einvoice_email, /* 20250903 einvoice_email */
  t.email_1,
  t.email_2,
  t.email_3,
  t.email_4,
  t.email_5,
  t.email_6,
  t.email_7,
  t.email_8,
  t.email_9,
  t.email_10,
  (
    CASE
      WHEN (
        t.email_flag_1 || t.email_flag_2 || t.email_flag_3 || t.email_flag_4 || t.email_flag_5 || t.email_flag_6 || t.email_flag_7 || t.email_flag_8 || t.email_flag_9 || t.email_flag_10
      ) LIKE '%1%'
      THEN '1'
      ELSE '0'
    END
  ) AS email_flag
FROM ${com_schema}.temp_t_mhbos_m_client_email_info_1 AS t
WHERE
  1 = 1;