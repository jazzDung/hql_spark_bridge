DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info

/* create temporary table temp_t_mhbos_m_client_nominees_info */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info (
  `client_no` STRING,
  `client_type` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `customer_name_concatenate` STRING,
  `noms_ind` STRING,
  `nominees_type` STRING,
  `pledged_securities_flag` STRING,
  `remove_kenanga_nominees_name` STRING,
  `remove_pledged_name` STRING,
  `replace_dsal_handling` STRING,
  `cleaned_nominees_name` STRING,
  `principal_name` STRING,
  `intermediary_name` STRING,
  `beneficiary_name` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info /* trunate temporary table temp_t_mhbos_m_client_nominees_info_5 */

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info
SELECT
  t.client_no,
  t.client_type,
  t.client_name,
  t.client_name1,
  t.client_name2,
  t.client_name3,
  t.customer_name_concatenate,
  t.noms_ind,
  t.nominees_type,
  t.pledged_securities_flag,
  t.remove_kenanga_nominees_name,
  t.remove_pledged_name,
  t.replace_dsal_handling,
  t.cleaned_nominees_name,
  (
    CASE
      WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
      THEN t.cleaned_nominees_name
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
      THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$'))
      WHEN t.cleaned_nominees_name RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
      THEN t.cleaned_nominees_name
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+)$'
      THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+)$'))
      ELSE t.cleaned_nominees_name
    END
  ) AS principal_name,
  (
    CASE
      WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
      THEN NULL
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR[ ]+([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
      THEN NULL
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
      THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 2))
      ELSE NULL
    END
  ) AS intermediary_name,
  (
    CASE
      WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
      THEN NULL
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$'
      THEN TRIM(
        REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$', 2)
      )
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
      THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 3))
      WHEN t.cleaned_nominees_name RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
      THEN NULL
      WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+)$'
      THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+)$', 2))
      ELSE NULL
    END
  ) AS beneficiary_name
FROM (
  SELECT
    client_no,
    client_type,
    client_name,
    client_name1,
    client_name2,
    client_name3,
    customer_name_concatenate,
    noms_ind,
    nominees_type,
    pledged_securities_flag,
    remove_kenanga_nominees_name,
    remove_pledged_name,
    replace_dsal_handling,
    (
      CASE
        WHEN cleaned_nominees_name LIKE 'FOR %'
        THEN SUBSTRING(cleaned_nominees_name, 5)
        ELSE cleaned_nominees_name
      END
    ) AS cleaned_nominees_name
  FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_5
) AS t /* where t.noms_ind = 'Y' */