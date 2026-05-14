DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_2;

/* create temporary table temp_t_mhbos_m_client_nominees_info_2 */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_nominees_info_2 (
  `client_no` STRING,
  `client_type` STRING,
  `client_name` STRING,
  `client_name1` STRING,
  `client_name2` STRING,
  `client_name3` STRING,
  `customer_name_concatenate` STRING,
  `noms_ind` STRING,
  `nominees_type` STRING,
  `remove_kenanga_nominees_name` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_nominees_info_2;

/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_2 */
INSERT INTO ${com_schema}.temp_t_mhbos_m_client_nominees_info_2
SELECT
  t.client_no,
  t.client_type,
  t.client_name,
  t.client_name1,
  t.client_name2,
  t.client_name3,
  t.customer_name_concatenate,
  t.noms_ind,
  t1.nominees_type,
  (
    CASE
      WHEN LENGTH(t.kenanga_noms_list_1) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_1)
      WHEN LENGTH(t.kenanga_noms_list_2) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_2)
      WHEN LENGTH(t.kenanga_noms_list_3) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_3)
      WHEN LENGTH(t.kenanga_noms_list_4) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_4)
      WHEN LENGTH(t.kenanga_noms_list_5) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_5)
      WHEN LENGTH(t.kenanga_noms_list_6) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_6)
      WHEN LENGTH(t.kenanga_noms_list_7) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_7)
      WHEN LENGTH(t.kenanga_noms_list_8) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_8)
      WHEN LENGTH(t.kenanga_noms_list_9) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_9)
      WHEN LENGTH(t.kenanga_noms_list_10) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_10)
      WHEN LENGTH(t.kenanga_noms_list_11) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_11)
      WHEN LENGTH(t.kenanga_noms_list_12) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_12)
      WHEN LENGTH(t.kenanga_noms_list_13) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_13)
      WHEN LENGTH(t.kenanga_noms_list_14) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_14)
      WHEN LENGTH(t.kenanga_noms_list_15) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_15)
      WHEN LENGTH(t.kenanga_noms_list_16) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_16)
      WHEN LENGTH(t.kenanga_noms_list_17) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_17)
      WHEN LENGTH(t.kenanga_noms_list_18) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_18)
      WHEN LENGTH(t.kenanga_noms_list_19) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_19)
      WHEN LENGTH(t.kenanga_noms_list_20) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_20)
      WHEN LENGTH(t.kenanga_noms_list_21) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_21)
      WHEN LENGTH(t.kenanga_noms_list_22) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_22)
      WHEN LENGTH(t.kenanga_noms_list_23) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_23)
      WHEN LENGTH(t.kenanga_noms_list_24) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_24)
      WHEN LENGTH(t.kenanga_noms_list_25) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_25)
      WHEN LENGTH(t.kenanga_noms_list_26) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_26)
      WHEN LENGTH(t.kenanga_noms_list_27) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_27)
      WHEN LENGTH(t.kenanga_noms_list_28) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_28)
      WHEN LENGTH(t.kenanga_noms_list_29) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_29)
      WHEN LENGTH(t.kenanga_noms_list_30) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_30)
      WHEN LENGTH(t.kenanga_noms_list_31) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_31)
      WHEN LENGTH(t.kenanga_noms_list_32) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_32)
      WHEN LENGTH(t.kenanga_noms_list_33) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_33)
      WHEN LENGTH(t.kenanga_noms_list_34) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_34)
      WHEN LENGTH(t.kenanga_noms_list_35) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_35)
      WHEN LENGTH(t.kenanga_noms_list_36) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_36)
      WHEN LENGTH(t.kenanga_noms_list_37) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_37)
      WHEN LENGTH(t.kenanga_noms_list_38) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_38)
      WHEN LENGTH(t.kenanga_noms_list_39) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_39)
      WHEN LENGTH(t.kenanga_noms_list_40) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_40)
      WHEN LENGTH(t.kenanga_noms_list_41) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_41)
      WHEN LENGTH(t.kenanga_noms_list_42) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_42)
      WHEN LENGTH(t.kenanga_noms_list_43) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_43)
      WHEN LENGTH(t.kenanga_noms_list_44) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_44)
      WHEN LENGTH(t.kenanga_noms_list_45) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_45)
      WHEN LENGTH(t.kenanga_noms_list_46) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_46)
      WHEN LENGTH(t.kenanga_noms_list_47) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_47)
      WHEN LENGTH(t.kenanga_noms_list_48) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_48)
      WHEN LENGTH(t.kenanga_noms_list_49) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_49)
      WHEN LENGTH(t.kenanga_noms_list_50) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_50)
      WHEN LENGTH(t.kenanga_noms_list_51) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_51)
      WHEN LENGTH(t.kenanga_noms_list_52) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_52)
      WHEN LENGTH(t.kenanga_noms_list_53) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_53)
      WHEN LENGTH(t.kenanga_noms_list_54) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_54)
      WHEN LENGTH(t.kenanga_noms_list_55) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_55)
      WHEN LENGTH(t.kenanga_noms_list_56) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_56)
      WHEN LENGTH(t.kenanga_noms_list_57) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_57)
      WHEN LENGTH(t.kenanga_noms_list_58) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_58)
      WHEN LENGTH(t.kenanga_noms_list_59) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_59)
      WHEN LENGTH(t.kenanga_noms_list_60) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_60)
      WHEN LENGTH(t.kenanga_noms_list_61) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_61)
      WHEN LENGTH(t.kenanga_noms_list_62) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_62)
      WHEN LENGTH(t.kenanga_noms_list_63) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_63)
      WHEN LENGTH(t.kenanga_noms_list_64) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_64)
      WHEN LENGTH(t.kenanga_noms_list_65) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_65)
      WHEN LENGTH(t.kenanga_noms_list_66) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_66)
      WHEN LENGTH(t.kenanga_noms_list_67) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_67)
      WHEN LENGTH(t.kenanga_noms_list_68) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_68)
      WHEN LENGTH(t.kenanga_noms_list_69) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_69)
      WHEN LENGTH(t.kenanga_noms_list_70) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_70)
      WHEN LENGTH(t.kenanga_noms_list_71) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_71)
      WHEN LENGTH(t.kenanga_noms_list_72) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_72)
      WHEN LENGTH(t.kenanga_noms_list_73) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_73)
      WHEN LENGTH(t.kenanga_noms_list_74) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_74)
      WHEN LENGTH(t.kenanga_noms_list_75) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_75)
      WHEN LENGTH(t.kenanga_noms_list_76) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_76)
      WHEN LENGTH(t.kenanga_noms_list_77) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_77)
      WHEN LENGTH(t.kenanga_noms_list_78) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_78)
      WHEN LENGTH(t.kenanga_noms_list_79) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_79)
      WHEN LENGTH(t.kenanga_noms_list_80) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_80)
      WHEN LENGTH(t.kenanga_noms_list_81) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_81)
      WHEN LENGTH(t.kenanga_noms_list_82) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_82)
      WHEN LENGTH(t.kenanga_noms_list_83) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_83)
      WHEN LENGTH(t.kenanga_noms_list_84) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_84)
      WHEN LENGTH(t.kenanga_noms_list_85) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_85)
      WHEN LENGTH(t.kenanga_noms_list_86) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_86)
      WHEN LENGTH(t.kenanga_noms_list_87) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_87)
      WHEN LENGTH(t.kenanga_noms_list_88) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_88)
      WHEN LENGTH(t.kenanga_noms_list_89) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_89)
      WHEN LENGTH(t.kenanga_noms_list_90) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_90)
      WHEN LENGTH(t.kenanga_noms_list_91) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_91)
      WHEN LENGTH(t.kenanga_noms_list_92) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_92)
      WHEN LENGTH(t.kenanga_noms_list_93) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_93)
      WHEN LENGTH(t.kenanga_noms_list_94) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_94)
      WHEN LENGTH(t.kenanga_noms_list_95) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_95)
      WHEN LENGTH(t.kenanga_noms_list_96) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_96)
      WHEN LENGTH(t.kenanga_noms_list_97) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_97)
      WHEN LENGTH(t.kenanga_noms_list_98) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_98)
      WHEN LENGTH(t.kenanga_noms_list_99) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_99)
      WHEN LENGTH(t.kenanga_noms_list_100) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_100)
      WHEN LENGTH(t.kenanga_noms_list_101) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_101)
      WHEN LENGTH(t.kenanga_noms_list_102) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_102)
      WHEN LENGTH(t.kenanga_noms_list_103) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_103)
      WHEN LENGTH(t.kenanga_noms_list_104) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_104)
      WHEN LENGTH(t.kenanga_noms_list_105) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_105)
      WHEN LENGTH(t.kenanga_noms_list_106) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_106)
      WHEN LENGTH(t.kenanga_noms_list_107) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_107)
      WHEN LENGTH(t.kenanga_noms_list_108) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_108)
      WHEN LENGTH(t.kenanga_noms_list_109) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_109)
      WHEN LENGTH(t.kenanga_noms_list_110) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_110)
      WHEN LENGTH(t.kenanga_noms_list_111) = t1.replace_field_length
      THEN TRIM(t.kenanga_noms_list_111)
      ELSE TRIM(t.customer_name_concatenate)
    END
  ) AS remove_kenanga_nominees_name
FROM ${com_schema}.temp_t_mhbos_m_client_nominees_info_1 AS t
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1 AS t1
  ON t.client_no = t1.client_no /* where t.noms_ind = 'Y' */;