/*==============[Group.19]==============*/
INSERT INTO ${cur_schema}.TEMP_DIM_TRADER_CONTACT(
    OWNER_ID -- None
    ,CONTACT_OWNER_TYPE -- None
    ,CONTACT_TYPE -- None
    ,CONTACT_VALUE -- None
    ,CONTACT_NAME -- None
    ,CONTACT_CREATE_DATE -- None
    ,CONTACT_UPDATE_DATE -- None
    ,LINE_OF_BUSINESS -- None
    ,SOURCE_NAME -- None
    ,SOURCE_RECORD_ID -- None
    ,SEQUENCE_NO -- None
)
SELECT 'M21_' || T1.CODE AS OWNER_ID -- None
       ,'AGENT' AS CONTACT_OWNER_TYPE -- None
       ,'MOBILE' AS CONTACT_TYPE -- None
       ,T1.MOBILEPHONE AS CONTACT_VALUE -- None
       ,NULL AS CONTACT_NAME -- None
       ,T1.CREATEDATE AS CONTACT_CREATE_DATE -- None
       ,T1.MODIFYDATE AS CONTACT_UPDATE_DATE -- None
       ,'FT' AS LINE_OF_BUSINESS -- None
       ,'M21' AS SOURCE_NAME -- None
       ,T1.CODE AS SOURCE_RECORD_ID -- None
       ,1 AS SEQUENCE_NO -- None
  FROM ${com_schema}.T_M21_ACCOUNTEXECUTIVE AS T1 --None
 WHERE T1.ETL_DT = '${batch_date}'
   AND TRIM(NVL(T1.MOBILEPHONE, '')) <> ''
   and T1.MOBILEPHONE not like '@[%]'
;