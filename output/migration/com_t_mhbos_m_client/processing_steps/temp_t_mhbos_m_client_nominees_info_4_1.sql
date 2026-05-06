DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1

/* create temporary table temp_t_mhbos_m_client_nominees_info_4_1 */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1 (
  `client_no` STRING,
  `client_type` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `customer_name_concatenate` STRING,
  `noms_ind` STRING,
  `nominees_type` STRING,
  `remove_pledged_name_length` INT
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_4_1 */

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4_1 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1
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
  LEAST(
    LENGTH(pledged_name_list_1),
    LENGTH(pledged_name_list_2),
    LENGTH(pledged_name_list_3),
    LENGTH(pledged_name_list_4),
    LENGTH(pledged_name_list_5),
    LENGTH(pledged_name_list_6),
    LENGTH(pledged_name_list_7),
    LENGTH(pledged_name_list_8),
    LENGTH(pledged_name_list_9),
    LENGTH(pledged_name_list_10),
    LENGTH(pledged_name_list_11),
    LENGTH(pledged_name_list_12),
    LENGTH(pledged_name_list_13),
    LENGTH(pledged_name_list_14),
    LENGTH(pledged_name_list_15),
    LENGTH(pledged_name_list_16),
    LENGTH(pledged_name_list_17),
    LENGTH(pledged_name_list_18),
    LENGTH(pledged_name_list_19),
    LENGTH(pledged_name_list_20),
    LENGTH(pledged_name_list_21),
    LENGTH(pledged_name_list_22),
    LENGTH(pledged_name_list_23),
    LENGTH(pledged_name_list_24),
    LENGTH(pledged_name_list_25),
    LENGTH(pledged_name_list_26),
    LENGTH(pledged_name_list_27),
    LENGTH(pledged_name_list_28),
    LENGTH(pledged_name_list_29),
    LENGTH(pledged_name_list_30),
    LENGTH(pledged_name_list_31),
    LENGTH(pledged_name_list_32),
    LENGTH(pledged_name_list_33),
    LENGTH(pledged_name_list_34),
    LENGTH(pledged_name_list_35),
    LENGTH(pledged_name_list_36) /* add_20240321 */,
    LENGTH(pledged_name_list_37) /* add_20240321 */,
    LENGTH(pledged_name_list_38) /* add_20240518    */,
    LENGTH(pledged_name_list_39) /* add_20240611 */,
    LENGTH(pledged_name_list_40) /* add_20240828      */,
    LENGTH(pledged_name_list_41) /* add_20240828         */,
    LENGTH(pledged_name_list_42) /* add_20250620                     */
  ) AS remove_pledged_name_length
FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_3 AS t /* where t.noms_ind = 'Y' */