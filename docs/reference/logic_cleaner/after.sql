
SELECT 'MHBOS_' || T1.CLIENT_NO AS OWNER_ID -- None
       ,'ACCOUNT' AS CONTACT_OWNER_TYPE -- None
       ,'MOBILE' AS CONTACT_TYPE -- None
       ,T1.MOBILE_NO AS CONTACT_VALUE -- None
       ,NULL AS CONTACT_NAME -- None
       ,T1.DATE_CREATED AS CONTACT_CREATE_DATE -- None
       ,T1.DATE_CHANGE AS CONTACT_UPDATE_DATE -- None
       ,'EB' AS LINE_OF_BUSINESS -- None
       ,'MHBOS' AS SOURCE_NAME -- None
       ,T1.CLIENT_NO AS SOURCE_RECORD_ID -- None
       ,1 AS SEQUENCE_NO -- None
  FROM ${com_schema}.T_MHBOS_M_CLIENT AS T1 --None
  JOIN ${com_schema}.T_MHBOS_M_CLIENT_EXT AS T2 --None
    ON DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{last_date}'
    AND DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd')  = DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd')
 WHERE
     DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{last_date}'
    AND DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    AND T1.test_date = '${batch_date}'
    AND DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') IN (SELECT DISTINCT DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') FROM ${com_schema}.temp_table)
;