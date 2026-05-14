DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_telephone_number_info;

/* 2.12 Create a temporary table temp_mhbos_m_client_telephone_number_info to store the cleaned telephone number information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_telephone_number_info (
  `client_no` STRING,
  `source_mobile_no` STRING,
  `source_fax_no` STRING,
  `source_tel_no_home` STRING,
  `source_tel_no_office` STRING,
  `mobile_no` STRING,
  `fax_no` STRING,
  `tel_no_home` STRING,
  `tel_no_office` STRING,
  `mobile_no_flag` STRING,
  `fax_no_flag` STRING,
  `tel_no_home_flag` STRING,
  `tel_no_office_flag` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_telephone_number_info;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_telephone_number_info /* modify 20250502 removed flag logic */
SELECT
  t.client_no,
  t.source_mobile_no,
  t.source_fax_no,
  t.source_tel_no_home,
  t.source_tel_no_office,
  t.mobile_no, /*  (case when t.mobile_no rlike '^[0-9+]+$' then t.mobile_no */ /*        when t.mobile_no like '%(0)%' then t.mobile_no */ /*        when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then t.mobile_no */ /*      when nvl(trim(t.mobile_no), '') = '' then '' */ /*        else '@[' || t.mobile_no || ']' end) as mobile_no, */
  t.fax_no, /*  (case when t.fax_no rlike '^[0-9+]+$' then t.fax_no */ /*        when t.fax_no like '%(0)%' then t.fax_no */ /*        when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then t.fax_no */ /*      when nvl(trim(t.fax_no), '') = '' then '' */ /*        else '@[' || t.fax_no || ']' end) as fax_no, */
  t.tel_no_home, /*  (case when t.tel_no_home rlike '^[0-9+]+$' then t.tel_no_home */ /*        when t.tel_no_home like '%(0)%' then t.tel_no_home */ /*        when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_home */ /*      when nvl(trim(t.tel_no_home), '') = '' then '' */ /*        else '@[' || t.tel_no_home || ']' end) as tel_no_home, */
  t.tel_no_office, /*  (case when t.tel_no_office rlike '^[0-9+]+$' then t.tel_no_office */ /*        when t.tel_no_office like '%(0)%' then t.tel_no_office */ /*        when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_office */ /*      when nvl(trim(t.tel_no_office), '') = '' then '' */ /*        else '@[' || t.tel_no_office || ']' end) as tel_no_office, */
  '0' AS mobile_no_flag, /*  (case when t.mobile_no rlike '^[0-9+]+$' then '0' */ /*        when t.mobile_no like '%(0)%' then '0' */ /*        when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then '0' */ /*      when nvl(trim(t.mobile_no), '') = '' then '0' */ /*        else '1' end) as mobile_no_flag, */
  '0' AS fax_no_flag, /*  (case when t.fax_no rlike '^[0-9+]+$' then '0' */ /*        when t.fax_no like '%(0)%' then '0' */ /*        when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then '0' */ /*      when nvl(trim(t.fax_no), '') = '' then '0' */ /*        else '1' end) as fax_no_flag, */
  '0' AS tel_no_home_flag, /*  (case when t.tel_no_home rlike '^[0-9+]+$' then '0' */ /*        when t.tel_no_home like '%(0)%' then '0' */ /*        when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then '0' */ /*      when nvl(trim(t.tel_no_home), '') = '' then '0' */ /*        else '1' end) as tel_no_home_flag, */
  '0' AS tel_no_office_flag /*  (case when t.tel_no_office rlike '^[0-9+]+$' then '0' */ /*        when t.tel_no_office like '%(0)%' then '0' */ /*        when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then '0' */ /*      when nvl(trim(t.tel_no_office), '') = '' then '0' */ /*        else '1' end) as tel_no_office_flag */
FROM ${com_schema}.temp_t_mhbos_m_client_telephone_clean AS t;