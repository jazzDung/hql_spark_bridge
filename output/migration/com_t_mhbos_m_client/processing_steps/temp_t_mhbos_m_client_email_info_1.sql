DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_email_info_1

/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_info_1 to store the cleaned email information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_email_info_1 (
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
  `email_flag_1` STRING,
  `email_flag_2` STRING,
  `email_flag_3` STRING,
  `email_flag_4` STRING,
  `email_flag_5` STRING,
  `email_flag_6` STRING,
  `email_flag_7` STRING,
  `email_flag_8` STRING,
  `email_flag_9` STRING,
  `email_flag_10` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_email_info_1 /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_email_info_1
SELECT
  t.client_no,
  t.source_email,
  (
    CASE
      WHEN t.einvoice_email RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.einvoice_email) <= 254
      THEN t.einvoice_email
      WHEN COALESCE(TRIM(t.einvoice_email), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS einvoice_email, /* 20250903 einvoice_email */
  (
    CASE
      WHEN t.email_1 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_1) <= 254
      THEN t.email_1
      WHEN COALESCE(TRIM(t.email_1), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_1,
  (
    CASE
      WHEN t.email_2 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_2) <= 254
      THEN t.email_2
      WHEN COALESCE(TRIM(t.email_2), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_2,
  (
    CASE
      WHEN t.email_3 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_3) <= 254
      THEN t.email_3
      WHEN COALESCE(TRIM(t.email_3), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_3,
  (
    CASE
      WHEN t.email_4 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_4) <= 254
      THEN t.email_4
      WHEN COALESCE(TRIM(t.email_4), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_4,
  (
    CASE
      WHEN t.email_5 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_5) <= 254
      THEN t.email_5
      WHEN COALESCE(TRIM(t.email_5), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_5,
  (
    CASE
      WHEN t.email_6 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_6) <= 254
      THEN t.email_6
      WHEN COALESCE(TRIM(t.email_6), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_6,
  (
    CASE
      WHEN t.email_7 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_7) <= 254
      THEN t.email_7
      WHEN COALESCE(TRIM(t.email_7), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_7,
  (
    CASE
      WHEN t.email_8 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_8) <= 254
      THEN t.email_8
      WHEN COALESCE(TRIM(t.email_8), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_8,
  (
    CASE
      WHEN t.email_9 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_9) <= 254
      THEN t.email_9
      WHEN COALESCE(TRIM(t.email_9), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_9,
  (
    CASE
      WHEN t.email_10 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_10) <= 254
      THEN t.email_10
      WHEN COALESCE(TRIM(t.email_10), '') = ''
      THEN ''
      ELSE ''
    END
  ) AS email_10,
  '0' AS email_flag_1, /*       (case when t.email_1 rlike '^.{1,64}\\@.+\\..+' and length(t.email_1) <= 254 then '0'
	         when nvl(trim(t.email_1), '') = '' then '0'
             else '1' end) as email_flag_1,
*/
  '0' AS email_flag_2, /*       (case when t.email_2 rlike '^.{1,64}\\@.+\\..+' and length(t.email_2) <= 254 then '0'
	         when nvl(trim(t.email_2), '') = '' then '0'
             else '1' end) as email_flag_2,
*/
  '0' AS email_flag_3, /*       (case when t.email_3 rlike '^.{1,64}\\@.+\\..+' and length(t.email_3) <= 254 then '0'
	         when nvl(trim(t.email_3), '') = '' then '0'
             else '1' end) as email_flag_3,
*/
  '0' AS email_flag_4, /*       (case when t.email_4 rlike '^.{1,64}\\@.+\\..+' and length(t.email_4) <= 254 then '0'
	         when nvl(trim(t.email_4), '') = '' then '0'
             else '1' end) as email_flag_4,
*/
  '0' AS email_flag_5, /*       (case when t.email_5 rlike '^.{1,64}\\@.+\\..+' and length(t.email_5) <= 254 then '0'
	         when nvl(trim(t.email_5), '') = '' then '0'
             else '1' end) as email_flag_5,
*/
  '0' AS email_flag_6, /*       (case when t.email_6 rlike '^.{1,64}\\@.+\\..+' and length(t.email_6) <= 254 then '0'
	         when nvl(trim(t.email_6), '') = '' then '0'
             else '1' end) as email_flag_6,
*/
  '0' AS email_flag_7, /*       (case when t.email_7 rlike '^.{1,64}\\@.+\\..+' and length(t.email_7) <= 254 then '0'
	         when nvl(trim(t.email_7), '') = '' then '0'
             else '1' end) as email_flag_7,
*/
  '0' AS email_flag_8, /*       (case when t.email_8 rlike '^.{1,64}\\@.+\\..+' and length(t.email_8) <= 254 then '0'
	         when nvl(trim(t.email_8), '') = '' then '0'
             else '1' end) as email_flag_8,
*/
  '0' AS email_flag_9, /*       (case when t.email_9 rlike '^.{1,64}\\@.+\\..+' and length(t.email_9) <= 254 then '0'
	         when nvl(trim(t.email_9), '') = '' then '0'
             else '1' end) as email_flag_9,
*/
  '0' AS email_flag_10 /*       (case when t.email_10 rlike '^.{1,64}\\@.+\\..+' and length(t.email_10) <= 254 then '0'
	         when nvl(trim(t.email_10), '') = '' then '0'
             else '1' end) as email_flag_10
*/
FROM (
  SELECT
    e.client_no,
    e.source_email,
    (
      CASE
        WHEN NOT e.einvoice_email LIKE '%@%'
        THEN ''
        WHEN e.einvoice_email LIKE '%@hotmail'
        THEN e.einvoice_email || '.com'
        WHEN e.einvoice_email LIKE '%@gmail'
        THEN e.einvoice_email || '.com'
        WHEN e.einvoice_email LIKE '%@yahoo'
        THEN e.einvoice_email || '.com'
        WHEN e.einvoice_email RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.einvoice_email, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.einvoice_email RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.einvoice_email, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.einvoice_email RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.einvoice_email, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.einvoice_email
      END
    ) AS einvoice_email, /* 20250903 einvoice_email */
    (
      CASE
        WHEN NOT e.email_1 LIKE '%@%'
        THEN ''
        WHEN e.email_1 LIKE '%@hotmail'
        THEN e.email_1 || '.com'
        WHEN e.email_1 LIKE '%@gmail'
        THEN e.email_1 || '.com'
        WHEN e.email_1 LIKE '%@yahoo'
        THEN e.email_1 || '.com'
        WHEN e.email_1 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_1, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_1 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_1, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_1 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_1, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_1
      END
    ) AS email_1,
    (
      CASE
        WHEN NOT e.email_2 LIKE '%@%'
        THEN ''
        WHEN e.email_2 LIKE '%@hotmail'
        THEN e.email_2 || '.com'
        WHEN e.email_2 LIKE '%@gmail'
        THEN e.email_2 || '.com'
        WHEN e.email_2 LIKE '%@yahoo'
        THEN e.email_2 || '.com'
        WHEN e.email_2 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_2, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_2 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_2, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_2 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_2, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_2
      END
    ) AS email_2,
    (
      CASE
        WHEN NOT e.email_3 LIKE '%@%'
        THEN ''
        WHEN e.email_3 LIKE '%@hotmail'
        THEN e.email_3 || '.com'
        WHEN e.email_3 LIKE '%@gmail'
        THEN e.email_3 || '.com'
        WHEN e.email_3 LIKE '%@yahoo'
        THEN e.email_3 || '.com'
        WHEN e.email_3 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_3, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_3 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_3, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_3 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_3, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_3
      END
    ) AS email_3,
    (
      CASE
        WHEN NOT e.email_4 LIKE '%@%'
        THEN ''
        WHEN e.email_4 LIKE '%@hotmail'
        THEN e.email_4 || '.com'
        WHEN e.email_4 LIKE '%@gmail'
        THEN e.email_4 || '.com'
        WHEN e.email_4 LIKE '%@yahoo'
        THEN e.email_4 || '.com'
        WHEN e.email_4 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_4, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_4 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_4, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_4 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_4, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_4
      END
    ) AS email_4,
    (
      CASE
        WHEN NOT e.email_5 LIKE '%@%'
        THEN ''
        WHEN e.email_5 LIKE '%@hotmail'
        THEN e.email_5 || '.com'
        WHEN e.email_5 LIKE '%@gmail'
        THEN e.email_5 || '.com'
        WHEN e.email_5 LIKE '%@yahoo'
        THEN e.email_5 || '.com'
        WHEN e.email_5 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_5, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_5 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_5, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_5 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_5, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_5
      END
    ) AS email_5,
    (
      CASE
        WHEN NOT e.email_6 LIKE '%@%'
        THEN ''
        WHEN e.email_6 LIKE '%@hotmail'
        THEN e.email_6 || '.com'
        WHEN e.email_6 LIKE '%@gmail'
        THEN e.email_6 || '.com'
        WHEN e.email_6 LIKE '%@yahoo'
        THEN e.email_6 || '.com'
        WHEN e.email_6 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_6, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_6 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_6, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_6 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_6, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_6
      END
    ) AS email_6,
    (
      CASE
        WHEN NOT e.email_7 LIKE '%@%'
        THEN ''
        WHEN e.email_7 LIKE '%@hotmail'
        THEN e.email_7 || '.com'
        WHEN e.email_7 LIKE '%@gmail'
        THEN e.email_7 || '.com'
        WHEN e.email_7 LIKE '%@yahoo'
        THEN e.email_7 || '.com'
        WHEN e.email_7 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_7, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_7 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_7, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_7 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_7, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_7
      END
    ) AS email_7,
    (
      CASE
        WHEN NOT e.email_8 LIKE '%@%'
        THEN ''
        WHEN e.email_8 LIKE '%@hotmail'
        THEN e.email_8 || '.com'
        WHEN e.email_8 LIKE '%@gmail'
        THEN e.email_8 || '.com'
        WHEN e.email_8 LIKE '%@yahoo'
        THEN e.email_8 || '.com'
        WHEN e.email_8 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_8, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_8 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_8, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_8 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_8, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_8
      END
    ) AS email_8,
    (
      CASE
        WHEN NOT e.email_9 LIKE '%@%'
        THEN ''
        WHEN e.email_9 LIKE '%@hotmail'
        THEN e.email_9 || '.com'
        WHEN e.email_9 LIKE '%@gmail'
        THEN e.email_9 || '.com'
        WHEN e.email_9 LIKE '%@yahoo'
        THEN e.email_9 || '.com'
        WHEN e.email_9 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_9, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_9 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_9, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_9 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_9, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_9
      END
    ) AS email_9,
    (
      CASE
        WHEN NOT e.email_10 LIKE '%@%'
        THEN ''
        WHEN e.email_10 LIKE '%@hotmail'
        THEN e.email_10 || '.com'
        WHEN e.email_10 LIKE '%@gmail'
        THEN e.email_10 || '.com'
        WHEN e.email_10 LIKE '%@yahoo'
        THEN e.email_10 || '.com'
        WHEN e.email_10 RLIKE '\\@hotmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_10, '\\@hotmail[^\\.]+', '@hotmail.com')
        WHEN e.email_10 RLIKE '\\@gmail[^\\.]+'
        THEN REGEXP_REPLACE(e.email_10, '\\@gmail[^\\.]+', '@gmail.com')
        WHEN e.email_10 RLIKE '\\@yahoo[^\\.]+'
        THEN REGEXP_REPLACE(e.email_10, '\\@yahoo[^\\.]+', '@yahoo.com')
        ELSE e.email_10
      END
    ) AS email_10
  FROM ${com_schema}.temp_t_mhbos_m_client_email_clean AS e
) AS t