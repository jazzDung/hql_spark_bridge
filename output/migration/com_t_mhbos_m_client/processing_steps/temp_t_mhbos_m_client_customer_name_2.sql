DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_customer_name_2

/* 2.10 Create a temporary table temp_mhbos_m_client_customer_name_2 to store the second time cleaned customer name. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_customer_name_2 (
  `client_no` STRING,
  `client_type` STRING,
  `customer_name_concatenate` STRING,
  `source_customer_name_concatenate` STRING,
  `customer_name` STRING,
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

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_customer_name_2 /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_customer_name_2
SELECT
  t.client_no,
  t.client_type,
  t.customer_name_concatenate,
  t.source_customer_name_concatenate,
  (
    CASE
      WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
      THEN t.customer_name_concatenate
      WHEN t.customer_name_concatenate RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
      THEN t.customer_name_concatenate
      WHEN t.customer_name_concatenate RLIKE '.*(^| )FOR( +)(.+)$'
      THEN REGEXP_EXTRACT(t.customer_name_concatenate, '.*(^| )FOR( +)(.+)$', 3)
      WHEN t.customer_name_concatenate RLIKE '.*(^| )FOR$'
      THEN t.client_name
      WHEN (
        t.customer_name_concatenate RLIKE '.*(^| )NOMINEES( |$)'
        OR t.customer_name_concatenate RLIKE '.*(^| )PLEDGED( |$)'
        OR t.customer_name_concatenate RLIKE '.*(^| )TEMPATAN( |$)'
        OR t.customer_name_concatenate RLIKE '.*(^| )ASING( |$)'
      )
      AND COALESCE(TRIM(t.client_name1), '') <> ''
      THEN COALESCE(TRIM(t.client_name1), t.client_name)
      ELSE t.customer_name_concatenate
    END
  ) AS customer_name,
  t.source_client_name,
  t.source_client_name1,
  t.source_client_name2,
  t.source_client_name3,
  t.primary_identification_type,
  t.client_name,
  t.client_name1,
  t.client_name2,
  t.client_name3,
  t.client_name_flag,
  t.client_name1_flag,
  t.client_name2_flag,
  t.client_name3_flag
FROM (
  SELECT
    cn.*,
    TRIM(cn.client_name) || (
      CASE
        WHEN COALESCE(cn.client_name1, '') <> ''
        THEN (
          ' ' || cn.client_name1
        )
        ELSE ''
      END
    ) || (
      CASE
        WHEN COALESCE(cn.client_name2, '') <> ''
        THEN (
          ' ' || cn.client_name2
        )
        ELSE ''
      END
    ) || (
      CASE
        WHEN COALESCE(cn.client_name3, '') <> ''
        THEN (
          ' ' || cn.client_name3
        )
        ELSE ''
      END
    ) AS customer_name_concatenate,
    TRIM(cn.source_client_name) || (
      CASE
        WHEN COALESCE(cn.source_client_name1, '') <> ''
        THEN (
          ' ' || cn.source_client_name1
        )
        ELSE ''
      END
    ) || (
      CASE
        WHEN COALESCE(cn.source_client_name2, '') <> ''
        THEN (
          ' ' || cn.source_client_name2
        )
        ELSE ''
      END
    ) || (
      CASE
        WHEN COALESCE(cn.source_client_name3, '') <> ''
        THEN (
          ' ' || cn.source_client_name3
        )
        ELSE ''
      END
    ) AS source_customer_name_concatenate
  FROM ${com_schema}.temp_t_mhbos_m_client_customer_name_1 AS cn
) AS t