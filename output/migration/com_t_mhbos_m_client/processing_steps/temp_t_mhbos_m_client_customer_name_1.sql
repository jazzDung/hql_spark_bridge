DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_customer_name_1

/* 2.9 Create a temporary table temp_mhbos_m_client_customer_name_1 to store the cleaned customer name. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_customer_name_1 (
  `client_no` STRING,
  `client_type` STRING,
  `source_client_name` STRING,
  `source_client_name1` STRING,
  `source_client_name2` STRING,
  `source_client_name3` STRING,
  `primary_identification_type` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `client_name_flag` STRING,
  `client_name1_flag` STRING,
  `client_name2_flag` STRING,
  `client_name3_flag` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_customer_name_1 /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_customer_name_1
SELECT
  mmca.client_no,
  mmca.client_type,
  mmca.client_name AS source_client_name,
  mmca.client_name1 AS source_client_name1,
  mmca.client_name2 AS source_client_name2,
  mmca.client_name3 AS source_client_name3,
  tmmcpit.primary_identification_type,
  (
    CASE
      WHEN (
        tmmcpit.primary_identification_type = '1'
        AND COALESCE(mmca.client_name, '') <> ''
        AND COALESCE(mmca.client_name1, '') = ''
        AND COALESCE(mmca.client_name2, '') = ''
        AND COALESCE(mmca.client_name3, '') = ''
      )
      THEN TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(UPPER(TRIM(mmca.client_name)), '\\s*\\([^)]*\\)$', '') /* 20240723 changed from \\(.+\\) to \\(.*\\)$  --20241118 changes */,
                      ' MR$',
                      ''
                    ),
                    ' MDM$',
                    ''
                  ),
                  ' MS$',
                  ''
                ),
                ' PUAN$',
                ''
              ),
              '^MR ',
              ''
            ),
            '^DR ',
            ''
          ),
          '^DR. ',
          ''
        )
      )
      ELSE UPPER(TRIM(mmca.client_name))
    END
  ) AS client_name,
  UPPER(TRIM(mmca.client_name1)) AS client_name1, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name1 like '%CONDO%' or 
                   mmca.client_name1 like '%NO 1%' or
                   mmca.client_name1 like '%NO 2%' or
                   mmca.client_name1 like '%OFFICE%' or
                   mmca.client_name1 like '%FLR%') then 
               '@[' || upper(mmca.client_name1) || ']'
             else upper(trim(mmca.client_name1)) end) as client_name1,
        */ /* 20250620 */
  UPPER(TRIM(mmca.client_name2)) AS client_name2, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name2 like '%CONDO%' or 
                   mmca.client_name2 like '%NO 1%' or
                   mmca.client_name2 like '%NO 2%' or
                   mmca.client_name2 like '%OFFICE%' or
                   mmca.client_name2 like '%FLR%') then 
               '@[' || upper(mmca.client_name2) || ']'
             else upper(trim(mmca.client_name2)) end)  as client_name2,
       */ /* 20250620 */
  UPPER(TRIM(mmca.client_name3)) AS client_name3, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name3 like '%CONDO%' or 
                   mmca.client_name3 like '%NO 1%' or
                   mmca.client_name3 like '%NO 2%' or
                   mmca.client_name3 like '%OFFICE%' or
                   mmca.client_name3 like '%FLR%') then 
               '@[' || upper(mmca.client_name3) || ']'
             else upper(trim(mmca.client_name3)) end) as client_name3,
       */ /* 20250620 */
  '0' AS client_name_flag, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name like '%CONDO%' or 
                   mmca.client_name like '%NO 1%' or
                   mmca.client_name like '%NO 2%' or
                   mmca.client_name like '%OFFICE%' or
                   mmca.client_name like '%FLR%') then 
               '1'
             else '0' end) as client_name_flag,
        */ /* 20250620 */
  '0' AS client_name1_flag, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name1 like '%CONDO%' or 
                   mmca.client_name1 like '%NO 1%' or
                   mmca.client_name1 like '%NO 2%' or
                   mmca.client_name1 like '%OFFICE%' or
                   mmca.client_name1 like '%FLR%') then '1'
             else '0' end) as client_name1_flag,
        */ /* 20250620 */
  '0' AS client_name2_flag, /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name2 like '%CONDO%' or 
                   mmca.client_name2 like '%NO 1%' or
                   mmca.client_name2 like '%NO 2%' or
                   mmca.client_name2 like '%OFFICE%' or
                   mmca.client_name2 like '%FLR%') then '1'
              else '0' end)  as client_name2_flag,
       */ /* 20250620 */
  '0' /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name3 like '%CONDO%' or 
                   mmca.client_name3 like '%NO 1%' or
                   mmca.client_name3 like '%NO 2%' or
                   mmca.client_name3 like '%OFFICE%' or
                   mmca.client_name3 like '%FLR%') then '1'
              else '0' end) as client_name3_flag
      */ AS client_name3_flag /* 20250620 */
FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmca
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_primary_identification_type AS tmmcpit
  ON mmca.client_no = tmmcpit.client_no