DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_3;

/* create temporary table temp_t_mhbos_m_client_nominees_info_3 */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_3 (
  `client_no` STRING,
  `client_type` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `customer_name_concatenate` STRING,
  `noms_ind` STRING,
  `nominees_type` STRING,
  `remove_kenanga_nominees_name` STRING,
  `pledged_name_list_1` STRING,
  `pledged_name_list_2` STRING,
  `pledged_name_list_39` STRING, /* add_20240611 */
  `pledged_name_list_3` STRING,
  `pledged_name_list_4` STRING,
  `pledged_name_list_5` STRING,
  `pledged_name_list_6` STRING,
  `pledged_name_list_7` STRING,
  `pledged_name_list_8` STRING,
  `pledged_name_list_9` STRING,
  `pledged_name_list_42` STRING, /* add_20240620 */
  `pledged_name_list_10` STRING,
  `pledged_name_list_11` STRING,
  `pledged_name_list_12` STRING,
  `pledged_name_list_13` STRING,
  `pledged_name_list_14` STRING,
  `pledged_name_list_15` STRING,
  `pledged_name_list_16` STRING,
  `pledged_name_list_17` STRING,
  `pledged_name_list_18` STRING,
  `pledged_name_list_41` STRING, /* add_20240828 */
  `pledged_name_list_19` STRING,
  `pledged_name_list_20` STRING,
  `pledged_name_list_21` STRING,
  `pledged_name_list_22` STRING,
  `pledged_name_list_23` STRING,
  `pledged_name_list_24` STRING,
  `pledged_name_list_25` STRING,
  `pledged_name_list_26` STRING,
  `pledged_name_list_27` STRING,
  `pledged_name_list_28` STRING,
  `pledged_name_list_29` STRING,
  `pledged_name_list_30` STRING,
  `pledged_name_list_31` STRING,
  `pledged_name_list_32` STRING,
  `pledged_name_list_33` STRING,
  `pledged_name_list_34` STRING,
  `pledged_name_list_35` STRING,
  `pledged_name_list_36` STRING, /* add_20240321 */
  `pledged_name_list_37` STRING, /* add_20240321 */
  `pledged_name_list_38` STRING, /* add_20240518 */
  `pledged_name_list_40` STRING /* add_20240828 */
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info_3;

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_3 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info_3
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
  t.remove_kenanga_nominees_name,
  REPLACE(t.remove_kenanga_nominees_name, '(PLEDGED)', '') AS pledged_name_list_1,
  REPLACE(t.remove_kenanga_nominees_name, '(PLEDGE)', '') AS pledged_name_list_2,
  REPLACE(
    t.remove_kenanga_nominees_name,
    'PLEDGED SECURITIES A/C FOR PLEDGED SECURITIES ACCOUNT',
    ''
  ) AS pledged_name_list_39, /* add_20240611 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLEGDED SECURITIES ACCOUNT', '') AS pledged_name_list_3,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITISED ACCOUNT', '') AS pledged_name_list_4,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SDN BHD', '') AS pledged_name_list_5,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SA/C', '') AS pledged_name_list_6,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') AS pledged_name_list_7,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') AS pledged_name_list_8,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') AS pledged_name_list_9,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT - ', '') AS pledged_name_list_42, /* add_20250620 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') AS pledged_name_list_10,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') AS pledged_name_list_11,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT ', '') AS pledged_name_list_12,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCCOUNT', '') AS pledged_name_list_13,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACC', '') AS pledged_name_list_14,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES A/C', '') AS pledged_name_list_15,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES  ACCOUNT', '') AS pledged_name_list_16,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') AS pledged_name_list_17,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITES ACCOUNT', '') AS pledged_name_list_18,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURUTIES ACCOUNT', '') AS pledged_name_list_41, /* add_20240828 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECS ACCOUNT', '') AS pledged_name_list_19,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECS A/C', '') AS pledged_name_list_20,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC. A/C', '') AS pledged_name_list_21,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC', '') AS pledged_name_list_22,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC A/C ', '') AS pledged_name_list_23,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC  A/C', '') AS pledged_name_list_24,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED', '') AS pledged_name_list_25,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES ACCOUNT', '') AS pledged_name_list_26,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES A/C', '') AS pledged_name_list_27,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SEC A/C', '') AS pledged_name_list_28,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE', '') AS pledged_name_list_29,
  REPLACE(t.remove_kenanga_nominees_name, 'PLED GED SECURITIES ACCOUNT', '') AS pledged_name_list_30,
  REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC', '') AS pledged_name_list_31,
  REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC AC ', '') AS pledged_name_list_32,
  REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC A/C ', '') AS pledged_name_list_33,
  REPLACE(t.remove_kenanga_nominees_name, 'PLDG A/C', '') AS pledged_name_list_34,
  REPLACE(t.remove_kenanga_nominees_name, 'PL SEC A/C - ', '') AS pledged_name_list_35,
  REPLACE(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES ACCOUNT', '') AS pledged_name_list_36, /* add_20240321 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES A/C', '') AS pledged_name_list_37, /* add_20240321 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLEDEGD SECURITIES ACCOUNT', '') AS pledged_name_list_38, /* add_20240518 */
  REPLACE(t.remove_kenanga_nominees_name, 'PLGD SEC ACC', '') AS pledged_name_list_40 /* add_20240828 */
FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_2 AS t /* where t.noms_ind = 'Y' */;