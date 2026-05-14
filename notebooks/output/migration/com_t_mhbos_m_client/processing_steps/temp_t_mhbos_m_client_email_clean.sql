DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_email_clean;

/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_clean to store the cleaned email information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_email_clean (
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
  `email_10` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_email_clean;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_email_clean
SELECT
  t.client_no,
  t.source_email,
  t.einvoice_email, /* 20250903 einvoice_email */
  TRIM(SPLIT(t.email, ';')[0]) AS email_1,
  TRIM(SPLIT(t.email, ';')[1]) AS email_2,
  TRIM(SPLIT(t.email, ';')[2]) AS email_3,
  TRIM(SPLIT(t.email, ';')[3]) AS email_4,
  TRIM(SPLIT(t.email, ';')[4]) AS email_5,
  TRIM(SPLIT(t.email, ';')[5]) AS email_6,
  TRIM(SPLIT(t.email, ';')[6]) AS email_7,
  TRIM(SPLIT(t.email, ';')[7]) AS email_8,
  TRIM(SPLIT(t.email, ';')[8]) AS email_9,
  TRIM(SPLIT(t.email, ';')[9]) AS email_10
FROM (
  SELECT
    mmce.client_no,
    REPLACE(LOWER(mmce.einvoice_email), ' ', '') AS einvoice_email, /* 20250903 einvoice_email */
    mmce.email AS source_email,
    REPLACE(LOWER(mmce.email), ' ', '') AS email
  FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmce
) AS t;