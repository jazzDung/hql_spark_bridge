/*==============[Group.29]==============*/
INSERT INTO ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT(
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
SELECT 'TOMS_' || T1.ACCOUNTNO AS OWNER_ID -- None
       ,'ACCOUNT' AS CONTACT_OWNER_TYPE -- None
       ,'EMAIL' AS CONTACT_TYPE -- None
       ,T1.EMAIL AS CONTACT_VALUE -- None
       ,NULL AS CONTACT_NAME -- None
       ,T1.SYDTC AS CONTACT_CREATE_DATE -- None
       ,T1.SYDTU AS CONTACT_UPDATE_DATE -- None
       ,'UT' AS LINE_OF_BUSINESS -- None
       ,T1.SOURCE_NAME AS SOURCE_NAME -- None
       ,T1.ACCOUNTNO AS SOURCE_RECORD_ID -- None
       ,T1.SEQUENCE_NO AS SEQUENCE_NO -- None
 FROM (SELECT ACCOUNTNO, EMAIL_1 AS EMAIL, SYDTC, SYDTU, 1 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_1, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_1 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_2 AS EMAIL, SYDTC, SYDTU, 2 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_2, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_2 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_3 AS EMAIL, SYDTC, SYDTU, 3 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_3, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_3 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_4 AS EMAIL, SYDTC, SYDTU, 4 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_4, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_4 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_5 AS EMAIL, SYDTC, SYDTU, 5 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_5, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_5 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_6 AS EMAIL, SYDTC, SYDTU, 6 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_6, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_6 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_7 AS EMAIL, SYDTC, SYDTU, 7 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_7, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_7 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_8 AS EMAIL, SYDTC, SYDTU, 8 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_8, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_8 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_9 AS EMAIL, SYDTC, SYDTU, 9 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_9, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_9 NOT RLIKE '^\\@\\[.*\\]$'
        UNION ALL
       SELECT ACCOUNTNO, EMAIL_10 AS EMAIL, SYDTC, SYDTU, 10 AS SEQUENCE_NO
         FROM ${com_schema}.T_TOMS_ERETAIL_ACCOUNT
        WHERE TRIM(NVL(EMAIL_10, '')) <> ''
          AND ETL_DT = '${batch_date}'
          AND EMAIL_10 NOT RLIKE '^\\@\\[.*\\]$'
      ) AS T1 --None
 WHERE TRIM(NVL(T1.EMAIL, '')) <> ''
   AND T1.EMAIL NOT LIKE '@[%'
;