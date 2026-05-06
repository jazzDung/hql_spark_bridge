DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_5

/* create temporary table temp_t_mhbos_m_client_nominees_info_5 */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_5 (
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
  `cleaned_nominees_name` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info_5 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_5 */

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_5 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info_5
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
  REGEXP_REPLACE(t.remove_pledged_name, '(^| )DSAL( |$)', ' ') AS replace_dsal_handling,
  TRIM(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(REGEXP_REPLACE(TRIM(t.remove_pledged_name), '^FOR ', ''), 'FOR EXEMPT AN FOR', 'FOR'),
          'EXEMPT AN FOR',
          'FOR'
        ),
        'RSS/SBL EXEMPT AN FOR',
        'FOR'
      ),
      'RSS/SBL FOR',
      'FOR'
    )
  ) AS cleaned_nominees_name
FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_4 AS t /* where t.noms_ind = 'Y' */