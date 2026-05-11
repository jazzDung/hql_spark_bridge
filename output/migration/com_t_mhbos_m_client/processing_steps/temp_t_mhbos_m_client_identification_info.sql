DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_identification_info;

/* 2.6 Create a temporary table temp_mhbos_m_client_identification_info to store the cleaned identification information. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_identification_info (
  `client_no` STRING,
  `id_type` STRING,
  `ic_no_new` STRING,
  `ic_no_old` STRING,
  `secondary_id_type` STRING,
  `secondary_id_no` STRING,
  `primary_identification_type` STRING,
  `primary_identification_type_flag` STRING,
  `secondary_identification_type` STRING,
  `secondary_identification_type_flag` STRING,
  `primary_identification_no` STRING,
  `primary_identification_no_flag` STRING,
  `secondary_identification_no` STRING,
  `secondary_identification_no_flag` STRING
);

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_identification_info /* 2.1.1 ddl-insert-sundexin */;

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_identification_info
SELECT
  p_no.client_no,
  p_no.id_type,
  p_no.ic_no_new,
  p_no.ic_no_old,
  p_no.secondary_id_type,
  p_no.secondary_id_no,
  p_type.primary_identification_type,
  p_type.primary_identification_type_flag,
  s_type.secondary_identification_type,
  s_type.secondary_identification_type_flag,
  (
    CASE
      WHEN p_type.primary_identification_type = '1'
      THEN (
        CASE
          WHEN p_no.primary_identification_no RLIKE '^\\d+$'
          AND /* Determine whether the ID number is all numbers */ LENGTH(p_no.primary_identification_no) = 12
          AND /* Determine whether the ID number is 12 digits long */ (
            INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) >= 1
            AND INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) <= 12
          ) /* 1st till 6 digit represent date of birth in YYMMDD format */
          AND (
            INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) >= 1
            AND INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) <= 31
          )
          AND SUBSTRING(p_no.primary_identification_no, 7, 2) /* At 7th and 8th digit referring to the Place of Birth */ IN (
            '01',
            '21',
            '22',
            '23',
            '24',
            '02',
            '25',
            '26',
            '27',
            '03',
            '28',
            '29',
            '04',
            '30',
            '05',
            '31',
            '59',
            '06',
            '32',
            '33',
            '07',
            '34',
            '35',
            '08',
            '36',
            '37',
            '38',
            '39',
            '09',
            '40',
            '10',
            '41',
            '42',
            '43',
            '44',
            '11',
            '45',
            '46',
            '12',
            '47',
            '48',
            '49',
            '13',
            '50',
            '51',
            '52',
            '53',
            '14',
            '54',
            '55',
            '56',
            '57',
            '15',
            '58',
            '16',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '71',
            '72',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87',
            '88',
            '89',
            '90',
            '91',
            '92',
            '93',
            '98',
            '99'
          )
          THEN p_no.primary_identification_no
          ELSE '@[' || p_no.primary_identification_no || ']'
        END
      )
      ELSE p_no.primary_identification_no
    END
  ) AS primary_identification_no,
  (
    CASE
      WHEN p_type.primary_identification_type = '1'
      THEN (
        CASE
          WHEN p_no.primary_identification_no RLIKE '^\\d+$'
          AND /* Determine whether the ID number is all numbers */ LENGTH(p_no.primary_identification_no) = 12
          AND /* Determine whether the ID number is 12 digits long */ (
            INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) >= 1
            AND INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) <= 12
          ) /* 1st till 6 digit represent date of birth in YYMMDD format */
          AND (
            INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) >= 1
            AND INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) <= 31
          )
          AND SUBSTRING(p_no.primary_identification_no, 7, 2) /* At 7th and 8th digit referring to the Place of Birth */ IN (
            '01',
            '21',
            '22',
            '23',
            '24',
            '02',
            '25',
            '26',
            '27',
            '03',
            '28',
            '29',
            '04',
            '30',
            '05',
            '31',
            '59',
            '06',
            '32',
            '33',
            '07',
            '34',
            '35',
            '08',
            '36',
            '37',
            '38',
            '39',
            '09',
            '40',
            '10',
            '41',
            '42',
            '43',
            '44',
            '11',
            '45',
            '46',
            '12',
            '47',
            '48',
            '49',
            '13',
            '50',
            '51',
            '52',
            '53',
            '14',
            '54',
            '55',
            '56',
            '57',
            '15',
            '58',
            '16',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '71',
            '72',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87',
            '88',
            '89',
            '90',
            '91',
            '92',
            '93',
            '98',
            '99'
          )
          THEN '0'
          ELSE '1'
        END
      )
      ELSE p_no.primary_identification_no_flag
    END
  ) AS primary_identification_no_flag,
  s_no.secondary_identification_no AS secondary_identification_no, /*
       (case when s_type.secondary_identification_type  = '1' then -- identification type is '1'
               (case when s_no.secondary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                          length(s_no.secondary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                          -- 1st till 6 digit represent date of birth in YYMMDD format
                          (int(substr(s_no.secondary_identification_no, 3, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 3, 2)) <= 12 ) and 
						  (int(substr(s_no.secondary_identification_no, 5, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 5, 2)) <= 31 ) and  
                          -- At 7th and 8th digit referring to the Place of Birth
                          substr(s_no.secondary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                 '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                 '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                            s_no.secondary_identification_no
			         when nvl(trim(s_no.secondary_identification_no), '') = '' then ''
                     else '@[' || s_no.secondary_identification_no || ']'
                end)
             else s_no.secondary_identification_no
         end ) as secondary_identification_no,
        */ /* 20250620 */
  s_no.secondary_identification_no_flag /*
       (case when s_type.secondary_identification_type  = '1' then -- identification type is '1'
               (case when s_no.secondary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                          length(s_no.secondary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                          -- 1st till 6 digit represent date of birth in YYMMDD format
                          (int(substr(s_no.secondary_identification_no, 3, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 3, 2)) <= 12 ) and 
						  (int(substr(s_no.secondary_identification_no, 5, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 5, 2)) <= 31 ) and  
                          -- At 7th and 8th digit referring to the Place of Birth
                          substr(s_no.secondary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                 '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                 '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                            '0'
			         when nvl(trim(s_no.secondary_identification_no), '') = '' then '0'
                     else '1'
                end)
             else s_no.secondary_identification_no_flag 
         end ) as secondary_identification_no_flag
        */ AS secondary_identification_no_flag /* 20250620 */
FROM ${com_schema}.temp_t_mhbos_m_client_primary_identification_no AS p_no
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_primary_identification_type AS p_type
  ON p_no.client_no = p_type.client_no
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no AS s_no
  ON p_no.client_no = s_no.client_no
LEFT JOIN ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type AS s_type
  ON p_no.client_no = s_type.client_no;