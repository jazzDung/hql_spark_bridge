/* 2.12 Create a temporary table temp_t_mhbos_m_client_telephone_clean to store the cleaned telephone number information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_telephone_clean (
  `client_no` STRING,
  `source_mobile_no` STRING,
  `source_fax_no` STRING,
  `source_tel_no_home` STRING,
  `source_tel_no_office` STRING,
  `mobile_no` STRING,
  `fax_no` STRING,
  `tel_no_home` STRING,
  `tel_no_office` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_telephone_clean /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_telephone_clean /* modify 20250502 */
SELECT
  t.client_no,
  t.source_mobile_no,
  t.source_fax_no,
  t.source_tel_no_home,
  t.source_tel_no_office,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(t.mobile_no, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
            '[^0-9/()+]',
            ''
          ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
          '\\((?![0]\\))',
          ''
        ) /* Remove '(' only if it's not part of "(0)" pattern */,
        '(?<!\\(0)\\)',
        ''
      ) /* Remove ')' only if it's not part of "(0)" pattern */,
      '/(?!([0-9]|\\(0\\)))',
      ''
    ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
    '(?<=.)(\\+)',
    ''
  ) AS mobile_no, /* Keep '+' sign if it's the first character, remove otherwise */
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(t.fax_no, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
            '[^0-9/()+]',
            ''
          ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
          '\\((?![0]\\))',
          ''
        ) /* Remove '(' only if it's not part of "(0)" pattern */,
        '(?<!\\(0)\\)',
        ''
      ) /* Remove ')' only if it's not part of "(0)" pattern */,
      '/(?!([0-9]|\\(0\\)))',
      ''
    ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
    '(?<=.)(\\+)',
    ''
  ) AS fax_no, /* Keep '+' sign if it's the first character, remove otherwise */
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(t.tel_no_home, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
            '[^0-9/()+]',
            ''
          ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
          '\\((?![0]\\))',
          ''
        ) /* Remove '(' only if it's not part of "(0)" pattern */,
        '(?<!\\(0)\\)',
        ''
      ) /* Remove ')' only if it's not part of "(0)" pattern */,
      '/(?!([0-9]|\\(0\\)))',
      ''
    ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
    '(?<=.)(\\+)',
    ''
  ) AS tel_no_home, /* Keep '+' sign if it's the first character, remove otherwise */
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(t.tel_no_office, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
            '[^0-9/()+]',
            ''
          ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
          '\\((?![0]\\))',
          ''
        ) /* Remove '(' only if it's not part of "(0)" pattern */,
        '(?<!\\(0)\\)',
        ''
      ) /* Remove ')' only if it's not part of "(0)" pattern */,
      '/(?!([0-9]|\\(0\\)))',
      ''
    ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
    '(?<=.)(\\+)',
    ''
  ) AS tel_no_office /* Keep '+' sign if it's the first character, remove otherwise */
FROM (
  SELECT
    mmca.client_no,
    (
      mmca.mobile_prefix || mmca.mobile_no
    ) AS source_mobile_no,
    mmca.fax_no AS source_fax_no,
    mmca.tel_no_home AS source_tel_no_home,
    mmca.tel_no_office AS source_tel_no_office,
    (
      mmca.mobile_prefix || mmca.mobile_no
    ) AS mobile_no,
    mmca.fax_no AS fax_no,
    mmca.tel_no_home AS tel_no_home,
    mmca.tel_no_office AS tel_no_office
  FROM ${com_schema}.temp_t_mhbos_m_client_all AS mmca
) AS t
WHERE
  1 = 1