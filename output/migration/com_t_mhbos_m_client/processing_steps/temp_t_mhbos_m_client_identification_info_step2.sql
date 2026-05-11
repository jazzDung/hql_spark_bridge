/* 3.2.2 step2 */
DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_identification_info_step2;

CREATE TABLE ${com_schema}.temp_t_mhbos_m_client_identification_info_step2 AS
SELECT
  step1.client_no, /* 2025/09/08: Add customer_name_flag */
  step1.clean_rule_flag || mn.customer_name_flag AS clean_rule_flag,
  mn.primary_identification_type, /* modify 20250313 */
  mn.primary_identification_no,
  mn.secondary_identification_type, /* modify from step1. to mn. 20250620 */
  mn.secondary_identification_no, /* modify from step1. to mn. 20250620 */
  mn.customer_name,
  step1.customer_name_concatenate,
  step1.client_name,
  step1.client_name1,
  step1.client_name2,
  step1.client_name3,
  step1.mobile_no,
  step1.fax_no,
  step1.tel_no_home,
  step1.tel_no_office,
  step1.date_of_birth,
  step1.race,
  step1.email_1,
  step1.email_2,
  step1.email_3,
  step1.email_4,
  step1.email_5,
  step1.email_6,
  step1.email_7,
  step1.email_8,
  step1.email_9,
  step1.email_10,
  step1.sex,
  ad.addr1,
  ad.addr2,
  ad.addr3,
  ad.addr4,
  ad.postcode,
  ad.city,
  ad.state,
  ad.perm_addr1,
  ad.perm_addr2,
  ad.perm_addr3,
  ad.perm_addr4,
  ad.perm_postcode,
  ad.perm_city,
  ad.perm_state,
  ad.perm_country, /* added new field 20250715 */
  COALESCE(nom.noms_ind, 'N') AS noms_ind,
  nom.cleaned_nominees_name,
  mn.principal_name, /* modify 20240518 */
  nom.intermediary_name,
  mn.beneficiary_name, /* modify 20240518 */
  nom.nominees_type,
  nom.pledged_securities_flag,
  step1.einvoice_email /* 20250903 einvoice_email */
FROM ${com_schema}.temp_t_mhbos_m_client_identification_info_step1 AS step1
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_address_info AS ad
  ON step1.client_no = ad.client_no
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_nominees_info AS nom
  ON step1.client_no = nom.client_no
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_name_info AS mn
  ON step1.client_no = mn.client_no;