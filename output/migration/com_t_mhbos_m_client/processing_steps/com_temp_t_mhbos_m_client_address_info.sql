DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_address_info;

/* 2.15 Create a temporary table temp_t_mhbos_m_client_address_info to store the cleaned cityinformation. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_address_info (
  `client_no` STRING,
  `addr1` STRING,
  `addr2` STRING,
  `addr3` STRING,
  `addr4` STRING,
  `postcode` STRING,
  `mailing_addr` STRING,
  `city` STRING,
  `state` STRING,
  `perm_addr1` STRING,
  `perm_addr2` STRING,
  `perm_addr3` STRING,
  `perm_addr4` STRING,
  `perm_postcode` STRING,
  `registered_addr` STRING,
  `perm_city` STRING,
  `perm_state` STRING,
  `perm_country` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_address_info;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_address_info
SELECT
  addr.client_no,
  addr.addr1,
  addr.addr2,
  addr.addr3,
  addr.addr4,
  addr.postcode,
  addr.mailing_addr,
  addr.city,
  addr.state, /*       (case when nvl(trim(addr.state), '') = '' then mp1.target_cd_val
	         when trim(addr.state) = 'MALACCA' then 'Melaka'
			 when trim(addr.state) = 'WP' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
			 else addr.state end) as state,
*/ /* 20251013 */
  addr.perm_addr1,
  addr.perm_addr2,
  addr.perm_addr3,
  addr.perm_addr4,
  addr.perm_postcode,
  addr.registered_addr,
  addr.perm_city,
  addr.perm_state, /*       (case when nvl(trim(addr.perm_state), '') = '' then mp2.target_cd_val
	         when trim(addr.perm_state) = 'MALACCA' then 'Melaka'
			 when trim(addr.perm_state) = 'WP' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
			 else addr.perm_state end) as perm_state,
*/ /* 20251013 */
  addr.perm_country
FROM ${com_schema}.temp_t_mhbos_m_client_address_city AS addr
LEFT JOIN ${com_schema}.t_ref_pub_cd_map AS mp1
  ON addr.city = mp1.src_code_val
  AND mp1.subj = 'com'
  AND mp1.src_sys_cd = 'mhbos'
  AND mp1.src_tab_en_name = 'r_mhbos_m_client'
  AND mp1.src_field_en_name = 'city'
  AND mp1.valid_flag = 'Y'
LEFT JOIN ${com_schema}.t_ref_pub_cd_map AS mp2
  ON addr.perm_city = mp2.src_code_val
  AND mp2.subj = 'com'
  AND mp2.src_sys_cd = 'mhbos'
  AND mp2.src_tab_en_name = 'r_mhbos_m_client'
  AND mp2.src_field_en_name = 'city'
  AND mp2.valid_flag = 'Y';