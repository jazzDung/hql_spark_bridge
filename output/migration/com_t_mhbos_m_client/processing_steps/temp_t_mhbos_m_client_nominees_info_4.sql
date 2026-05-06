DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_4

/* create temporary table temp_t_mhbos_m_client_nominees_info_4 */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_4 (
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
  `remove_pledged_name` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info_4 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_4 */

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info_4
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
  (
    CASE
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_1)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_2)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_3)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_4)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_5)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_6)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_7)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_8)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_9)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_10)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_11)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_12)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_13)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_14)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_15)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_16)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_17)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_18)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_19)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_20)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_21)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_22)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_23)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_24)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_25)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_26)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_27)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_28)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_29)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_30)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_31)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_32)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_33)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_34)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_35)
      THEN 'Y'
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_36)
      THEN 'Y' /* add_20240321 */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_37)
      THEN 'Y' /* add_20240321 */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_38)
      THEN 'Y' /* add_20240518  */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_39)
      THEN 'Y' /* add_20240611 */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_40)
      THEN 'Y' /* add_20240828   */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_41)
      THEN 'Y' /* add_20240828          */
      WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_42)
      THEN 'Y' /* add_20250620   */
      ELSE 'N'
    END
  ) AS pledged_securities_flag,
  t.remove_kenanga_nominees_name,
  (
    CASE
      WHEN LENGTH(t.pledged_name_list_1) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_1)
      WHEN LENGTH(t.pledged_name_list_2) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_2)
      WHEN LENGTH(t.pledged_name_list_3) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_3)
      WHEN LENGTH(t.pledged_name_list_4) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_4)
      WHEN LENGTH(t.pledged_name_list_5) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_5)
      WHEN LENGTH(t.pledged_name_list_6) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_6)
      WHEN LENGTH(t.pledged_name_list_7) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_7)
      WHEN LENGTH(t.pledged_name_list_8) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_8)
      WHEN LENGTH(t.pledged_name_list_9) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_9)
      WHEN LENGTH(t.pledged_name_list_10) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_10)
      WHEN LENGTH(t.pledged_name_list_11) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_11)
      WHEN LENGTH(t.pledged_name_list_12) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_12)
      WHEN LENGTH(t.pledged_name_list_13) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_13)
      WHEN LENGTH(t.pledged_name_list_14) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_14)
      WHEN LENGTH(t.pledged_name_list_15) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_15)
      WHEN LENGTH(t.pledged_name_list_16) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_16)
      WHEN LENGTH(t.pledged_name_list_17) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_17)
      WHEN LENGTH(t.pledged_name_list_18) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_18)
      WHEN LENGTH(t.pledged_name_list_19) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_19)
      WHEN LENGTH(t.pledged_name_list_20) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_20)
      WHEN LENGTH(t.pledged_name_list_21) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_21)
      WHEN LENGTH(t.pledged_name_list_22) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_22)
      WHEN LENGTH(t.pledged_name_list_23) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_23)
      WHEN LENGTH(t.pledged_name_list_24) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_24)
      WHEN LENGTH(t.pledged_name_list_25) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_25)
      WHEN LENGTH(t.pledged_name_list_26) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_26)
      WHEN LENGTH(t.pledged_name_list_27) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_27)
      WHEN LENGTH(t.pledged_name_list_28) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_28)
      WHEN LENGTH(t.pledged_name_list_29) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_29)
      WHEN LENGTH(t.pledged_name_list_30) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_30)
      WHEN LENGTH(t.pledged_name_list_31) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_31)
      WHEN LENGTH(t.pledged_name_list_32) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_32)
      WHEN LENGTH(t.pledged_name_list_33) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_33)
      WHEN LENGTH(t.pledged_name_list_34) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_34)
      WHEN LENGTH(t.pledged_name_list_35) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_35)
      WHEN LENGTH(t.pledged_name_list_36) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_36) /* add_20240321 */
      WHEN LENGTH(t.pledged_name_list_37) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_37) /* add_20240321 */
      WHEN LENGTH(t.pledged_name_list_38) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_38) /* add_20240518 */
      WHEN LENGTH(t.pledged_name_list_39) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_39) /* add_20240611 */
      WHEN LENGTH(t.pledged_name_list_40) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_40) /* add_20240828 */
      WHEN LENGTH(t.pledged_name_list_41) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_41) /* add_20240828 */
      WHEN LENGTH(t.pledged_name_list_42) = t1.remove_pledged_name_length
      THEN TRIM(t.pledged_name_list_42) /* add_20250620 */
      ELSE TRIM(t.remove_kenanga_nominees_name)
    END
  ) AS remove_pledged_name
FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_3 AS t
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1 AS t1
  ON t.client_no = t1.client_no /* where t.noms_ind = 'Y' */