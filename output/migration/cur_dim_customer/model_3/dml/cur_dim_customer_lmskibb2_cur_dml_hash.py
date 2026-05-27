
"""
Purpose:    curated - Snapshot table script
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py yyyymmdd [file_name]
CreateDate: 2023-08-17 00:00:00
FileType:   DML
Logs:
Table name: DIM_CUSTOMER
Table comment: DIM_CUSTOMER
Creation date: 2023-08-17 00:00:00
Primary key field: CUSTOMER_ID
Attribution hierarchy: curated
Attribution subject: cust
Main application: None
Analyst: zhairuoping
Time granularity: None
Retention period: None
Descriptive information: None
marcoong 20250326  add new source: sbl/lms/kdi, add new field CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
marcoong 20250409  update update_date field, update logic for CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION for MHBOS
marcoong 20250423  include primary_id_no = 6 for Corporate, and update REF_LOOKUP joining to include field SOURCE_KEY and remove ETL_DT filter
marcoong 20250715  add new field vulnerable_flag
syhmi    20251014  updated CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS logic for MHBOS
syhmi    20251016  updated CUSTOMER_COMPANY_TYPE_OF_BUSINESS logic for TOMS | updated CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION logic for M21
syhmi    20251017  uupdated CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS & CUSTOMER_COMPANY_TYPE_OF_BUSINESS logic for MHBOS
marcoong 20251017  updated CUSTOMER_COUNTRY_OF_RESIDENCE, CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS for GUAVA_CUSTOMER
marcoong 20251024  updated CUSTOMER_COUNTRY_OF_RESIDENCE, CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS for KDI_CUSTOMER
syhmi    20251024  updated CUSTOMER_COUNTRY_OF_RESIDENCE, CUSTOMER_COMPANY_TYPE_OF_BUSINESS for M21
syhmi    20251028  updated CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION logic
afiq     20260223  change sources from kdi customer to kdi clientreport
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws, coalesce, lit

source_name = "LMSKIBB2"
table_name  = "dim_customer_lmskibb2"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites if there is a partition key
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── SOURCE PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MHBOS (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5')
          THEN 'INDIVIDUAL'
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
          THEN 'CORPORATE'
          ELSE NULL
        END
      ) AS CUSTOMER_TYPE, /* None */
      T1.TITLE AS CUSTOMER_TITLE, /* None */
      (
        CASE WHEN T1.NOMS_IND = 'Y' THEN T1.PRINCIPAL_NAME ELSE T1.CUSTOMER_NAME END
      ) AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      (
        CASE
          WHEN TRIM(COALESCE(T1.PRIMARY_IDENTIFICATION_NO, '')) = ''
          THEN T4.BRN
          ELSE T1.PRIMARY_IDENTIFICATION_NO
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      T1.PRIMARY_ID_EXPIRY_DATE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      T1.SECONDARY_ID_EXPIRY_DATE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T2.NATIONALITY AS CUSTOMER_NATIONALITY, /* None */
      T1.RESI_CODE AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      T2.DATE_OF_BIRTH AS CUSTOMER_DATE_OF_BIRTH, /* None */
      (
        CASE
          WHEN T2.BUMI_STATUS = '1'
          THEN 'Y'
          WHEN T2.BUMI_STATUS = '2'
          THEN 'N'
          ELSE NULL
        END
      ) AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      T1.RACE AS CUSTOMER_RACE, /* None */
      T1.SEX AS CUSTOMER_GENDER, /* None */
      T2.MARITAL_STATUS AS CUSTOMER_MARITAL_STATUS, /* None */
      T1.CONTACT_PERSON AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      T4.COMPANY_OWNERSHIP AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CASE
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
        THEN T2.NATIONALITY
        ELSE NULL
      END AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251028 */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      T4.COMPANY_DATE_OF_INCORPORATION AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      T4.COMPANY_WEBSITE AS CUSTOMER_COMPANY_WEBSITE, /* None */
      T4.TYPE_OF_ORGANIZATION AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      T5.DECLARATION_FLAG AS PDPA_FLAG, /* None */
      T6.DECLARATION_FLAG AS CONNECTED_PARTY_FLAG, /* None */
      T7.DECLARATION_FLAG AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      T2.DISCLOSURE_OF_INFO AS CROSS_SELLING_CONSENT_FLAG, /* None */
      T8.DECLARATION_FLAG AS DCF_FLAG, /* None */
      T2.MTA_IND AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      (
        CASE WHEN NOT T9.CUSTOMER_ID IS NULL THEN 'Y' ELSE 'N' END
      ) AS FATCA_FLAG, /* None */
      (
        CASE WHEN NOT T3.CLIENT_NO IS NULL THEN 'Y' ELSE 'N' END
      ) AS CRS_FLAG, /* None */
      T4.PERSONNEL_DESIGNATION AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      T2.RESIDENCY_STATUS AS CUSTOMER_RESIDENCY_STATUS, /* None */
      GREATEST(COALESCE(T1.DATE_CREATED, '1900-01-01'), COALESCE(T1.DATE_CHANGE, '1900-01-01')) AS UPDATE_DATE, /* None */
      T2.EINVOICE AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T2.SST_NO AS SST_REGISTRATION_NO, /* NONE */
      T12.COUNTRY_CODE_CCRIS AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      T11.DECLARATION_FLAG AS VULNERABLE_FLAG /* 20250715 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN (
      SELECT DISTINCT
        CLIENT_NO
      FROM {params["com_schema"]}.t_MHBOS_M_CLIENT_CRS
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* None */
      ON T1.CLIENT_NO = T3.CLIENT_NO
    LEFT JOIN (
      SELECT
        A.ACCOUNTNO AS ACCOUNTNO,
        D.ALIASVALUE AS BRN,
        E.DOB AS COMPANY_DATE_OF_INCORPORATION,
        F.FIELDVALUE AS COMPANY_WEBSITE,
        G.FIELDVALUE AS COMPANY_OWNERSHIP,
        H.FIELDVALUE AS TYPE_OF_BUSINESS,
        I.FIELDVALUE AS TYPE_OF_ORGANIZATION,
        J.FIELDVALUE AS PERSONNEL_DESIGNATION
      FROM {params["com_schema"]}.t_K2_ACCOUNT AS A
      LEFT JOIN {params["com_schema"]}.t_K2_CIF_ACCOUNT AS B
        ON A.ACCOUNTID = B.ACCOUNTID
        AND B.RECSTATUS = 'AA'
        AND B.ISPRIMARY = '1'
        AND B.START_DT <= '{batch_date}'
        AND B.END_DT > '{batch_date}'
      LEFT JOIN (
        SELECT DISTINCT
          CIFID,
          ALIASVALUE
        FROM {params["com_schema"]}.t_K2_CIF_ALIAS
        WHERE
          ALIASTYPE = 'BUSREGNO'
          AND LENGTH(ALIASVALUE) = 12
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS D
        ON B.CIFID = D.CIFID
      LEFT JOIN {params["com_schema"]}.t_K2_CIF_EXT AS E
        ON B.CIFID = E.CIFID
        AND E.RECSTATUS = 'AA'
        AND E.START_DT <= '{batch_date}'
        AND E.END_DT > '{batch_date}'
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          REFERENCETYPE = 'ACCOUNT'
          AND RULEGROUPCODE = 'MHBOS'
          AND FIELDID = 'COMPWEB'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS F
        ON B.ACCOUNTID = F.REFERENCEID AND F.RN = 1
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          REFERENCETYPE = 'CIF'
          AND RULEGROUPCODE = 'INSTITUTION'
          AND FIELDID = 'OWNERSHIP'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS G
        ON B.CIFID = G.REFERENCEID AND G.RN = 1
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          REFERENCETYPE = 'CIF'
          AND RULEGROUPCODE = 'EMPLOYMENT'
          AND FIELDID = 'BUSINESSTYPE'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS H
        ON B.CIFID = H.REFERENCEID AND H.RN = 1
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          REFERENCETYPE = 'CIF'
          AND RULEGROUPCODE = 'CIF-INSTITUTION'
          AND FIELDID = 'ORIGANIZATIONTYPE'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS I
        ON B.CIFID = I.REFERENCEID AND I.RN = 1
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          REFERENCETYPE = 'CIF'
          AND RULEGROUPCODE = 'EMPLOYMENT'
          AND FIELDID = 'OCCUPATION'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS J
        ON B.CIFID = J.REFERENCEID AND J.RN = 1
      WHERE
        A.RECSTATUS = 'AA' AND A.START_DT <= '{batch_date}' AND A.END_DT > '{batch_date}'
    ) AS T4 /* None */
      ON T1.CLIENT_NO = T4.ACCOUNTNO
    LEFT JOIN {params["cur_schema"]}.DIM_DECLARATION AS T5 /* None */
      ON T1.CUST_ID = T5.CUSTOMER_ID
      AND T5.DECLARATION_TYPE = 'PDPA'
      AND T5.SOURCE_NAME = 'MHBOS'
      AND DATE_FORMAT(T5.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN {params["cur_schema"]}.DIM_DECLARATION AS T6 /* None */
      ON T1.CUST_ID = T6.CUSTOMER_ID
      AND T6.DECLARATION_TYPE = 'CONNECTED_PARTY'
      AND T6.SOURCE_NAME = 'FRA'
      AND DATE_FORMAT(T6.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN {params["cur_schema"]}.DIM_DECLARATION AS T7 /* None */
      ON T1.CUST_ID = T7.CUSTOMER_ID
      AND T7.DECLARATION_TYPE = 'POLITICALLY EXPOSED PERSON'
      AND T7.SOURCE_NAME = 'MHBOS'
      AND DATE_FORMAT(T7.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN {params["cur_schema"]}.DIM_DECLARATION AS T8 /* None */
      ON T1.CUST_ID = T8.CUSTOMER_ID
      AND T8.DECLARATION_TYPE = 'DCF'
      AND T8.SOURCE_NAME = 'MHBOS'
      AND DATE_FORMAT(T8.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN {params["cur_schema"]}.DIM_FATCA AS T9 /* None */
      ON T1.CUST_ID = T9.CUSTOMER_ID
      AND T9.SOURCE_NAME = 'MHBOS'
      AND DATE_FORMAT(T9.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        country_code_ccris
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T10 /* None */
      ON T1.COUNTRY = T10.COUNTRY_CODE_3_DIGITS
    LEFT JOIN {params["cur_schema"]}.DIM_DECLARATION AS T11 /* added new lookup for vulnerable value 20250715 */
      ON T1.CUST_ID = T11.CUSTOMER_ID
      AND T11.DECLARATION_TYPE = 'VULNERABLE'
      AND T11.SOURCE_NAME = 'MHBOS'
      AND DATE_FORMAT(T11.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T12 /* 20251014 added for CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS */
      ON T1.COUNTRY = T12.COUNTRY_CODE_3_DIGITS
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_COMPANY (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN COALESCE(T1.ATTACHMENT_ID_NO, '') = ''
          THEN 'INDIVIDUAL'
          ELSE T1.ATTACHMENT_CUSTOMER_TYPE
        END
      ) AS CUSTOMER_TYPE, /* None */
      T1.ATTACHMENT_TITLE AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      (
        CASE
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'CORPORATE'
          THEN '4'
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'INDIVIDUAL'
          AND LENGTH(T1.PRIMARY_IDENTIFICATION_NO) = 12
          THEN '1'
          ELSE NULL
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      (
        CASE WHEN COALESCE(T1.SECONDARY_IDENTIFICATION_NO, '') <> '' THEN '2' ELSE NULL END
      ) AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      (
        CASE
          WHEN LENGTH(T1.ATTACHMENT_NATIONALITY) = 2
          THEN T1.ATTACHMENT_NATIONALITY
          ELSE T3.COUNTRY_CODE_CCRIS
        END
      ) AS CUSTOMER_NATIONALITY, /* None */
      COALESCE(T4.COUNTRY_CODE_CCRIS, T2.CORRESPONDENCE_COUNTRY) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      T1.ATTACHMENT_DATE_OF_BIRTH_REGISTRATION AS CUSTOMER_DATE_OF_BIRTH, /* None */
      (
        CASE
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'INDIVIDUAL' AND T1.ATTACHMENT_BUMI_NONBUMI = '1'
          THEN 'Y'
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'INDIVIDUAL' AND T1.ATTACHMENT_BUMI_NONBUMI = '2'
          THEN 'N'
          ELSE NULL
        END
      ) AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      T1.ATTACHMENT_GENDER AS CUSTOMER_GENDER, /* None */
      T1.ATTACHMENT_MARITAL_STATUS AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      (
        CASE
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'CORPORATE'
          THEN T1.ATTACHMENT_BUMI_NONBUMI
          ELSE NULL
        END
      ) AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      (
        CASE
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'CORPORATE'
          THEN CASE
            WHEN LENGTH(T1.ATTACHMENT_NATIONALITY) = 2
            THEN T1.ATTACHMENT_NATIONALITY
            ELSE T3.COUNTRY_CODE_CCRIS
          END
        END
      ) AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      TRIM(T5.FIELDVALUE) AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.UPDATE_DATE AS UPDATE_DATE, /* None */
      NULL AS AUTO_EINVOICE_INDICATOR, /* None */
      NULL AS SST_REGISTRATION_NO, /* None */
      (
        CASE
          WHEN T1.ATTACHMENT_CUSTOMER_TYPE = 'CORPORATE'
          THEN COALESCE(T4.COUNTRY_CODE_CCRIS, T2.CORRESPONDENCE_COUNTRY)
        END
      ) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_GUAVA_COMPANY AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_GUAVA_BRANCH AS T2 /* None */
      ON T1.ATTACHMENT_TRANSACTION_INFO = T2.ATTACHMENT_TRANSACTION_INFO
      AND T1.BRANCH_NAME = T2.BRANCH_NAME
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY NATIONALITY ORDER BY COUNTRY_CODE_CCRIS DESC) AS ROW_NUM,
        COUNTRY_CODE_CCRIS,
        NATIONALITY
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(NATIONALITY, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* None */
      ON T1.ATTACHMENT_NATIONALITY = T3.NATIONALITY AND T3.ROW_NUM = 1
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_NAME ORDER BY COUNTRY_CODE_CCRIS DESC) AS ROW_NUM,
        COUNTRY_NAME,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T4 /* None */
      ON T2.CORRESPONDENCE_COUNTRY = T4.COUNTRY_NAME AND T4.ROW_NUM = 1
    LEFT JOIN (
      SELECT
        A.ACCOUNTNO AS ACCOUNTNO,
        TRIM(C.FIELDVALUE) AS FIELDVALUE
      FROM {params["com_schema"]}.t_K2_ACCOUNT AS A
      LEFT JOIN {params["com_schema"]}.t_K2_CIF_ACCOUNT AS B
        ON A.ACCOUNTID = B.ACCOUNTID
        AND B.RECSTATUS = 'AA'
        AND B.ISPRIMARY = 1
        AND B.START_DT <= '{batch_date}'
        AND B.END_DT > '{batch_date}'
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID ORDER BY EFFECTIVEFROM DESC) AS RN,
          REFERENCEID,
          FIELDVALUE,
          EFFECTIVEFROM
        FROM {params["com_schema"]}.t_K2_RULE_VALUE AS RV
        WHERE
          TRIM(REFERENCETYPE) = 'CIF'
          AND TRIM(RULEGROUPCODE) = 'EMPLOYMENT'
          AND TRIM(FIELDID) = 'BUSINESSTYPE'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS C
        ON B.CIFID = C.REFERENCEID AND C.RN = 1
      WHERE
        A.RECSTATUS = 'AA'
        AND A.ACCOUNTTYPE = 'TREASURY'
        AND A.START_DT <= '{batch_date}'
        AND A.END_DT > '{batch_date}'
    ) AS T5 /* None */
      ON T1.ATTACHMENT_TRANSACTION_INFO = T5.ACCOUNTNO
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CUSTOMER_NAME LIKE '@[%'
      AND NOT T1.CUST_ID IS NULL
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_SBL (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      CASE
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5')
        THEN 'INDIVIDUAL'
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
        THEN 'CORPORATE'
      END AS CUSTOMER_TYPE,
      UPPER(TRIM(T4.TITLE)) AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      TRIM(T1.PRIMARY_IDENTIFICATION_NO) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CASE
        WHEN UPPER(TRIM(T1.NATIONALITY)) IN ('MALAYSIAN CITIZEN')
        THEN 'MY'
        WHEN UPPER(TRIM(T1.NATIONALITY)) IN ('NON-MALAYSIAN CITIZEN')
        THEN ''
        ELSE UPPER(TRIM(T1.NATIONALITY))
      END AS CUSTOMER_NATIONALITY,
      T1.COUNTRY_RESIDENCY AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      T5.country_code AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      DATE_FORMAT(CAST(T3.birth_date AS TIMESTAMP), 'yyyy-MM-dd') AS CUSTOMER_DATE_OF_BIRTH, /* None */
      CASE
        WHEN T6.BUMI_Status_Code IN (1)
        THEN 'Y'
        WHEN T6.BUMI_Status_Code IN (2, 4)
        THEN 'N'
        ELSE NULL
      END AS CUSTOMER_BUMIPUTRA_STATUS,
      UPPER(T7.race_description) AS CUSTOMER_RACE, /* None */
      CASE
        WHEN T3.gender = 'F'
        THEN 'FEMALE'
        WHEN T3.gender = 'M'
        THEN 'MALE'
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE = '1'
        THEN (
          CASE
            WHEN SUBSTRING(T1.primary_identification_no, 12, 1) IN ('1', '3', '5', '7', '9')
            THEN 'MALE'
            ELSE 'FEMALE'
          END
        )
        ELSE NULL
      END AS CUSTOMER_GENDER,
      T8.Marital_Status_Code AS CUSTOMER_MARITAL_STATUS, /* None */
      UPPER(T3.contact_person_name) AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T1.PLACE_OF_INCORPORATION AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      UPPER(T9.type_of_business_code) AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      T3.date_of_incorporation AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None -- Need Mapping */
      CASE WHEN T3.is_pdpa = TRUE THEN 'Y' WHEN T3.is_pdpa = FALSE THEN 'N' ELSE NULL END AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      CASE WHEN T3.Is_FATCA = TRUE THEN 'Y' WHEN T3.Is_FATCA = FALSE THEN 'N' ELSE NULL END AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      CASE
        WHEN T3.Is_Politically_Exposed_Person = TRUE
        THEN 'Y'
        WHEN T3.Is_Politically_Exposed_Person = FALSE
        THEN 'N'
        ELSE NULL
      END AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      T10.Residency_Status_Code AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.LAST_GENERATED_DATETIME AS UPDATE_DATE, /* None */
      T1.CUSTOMER_E_INVOICING_FLAG AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T1.CUSTOMER_SST_REGISTRATION_NUMBER AS SST_REGISTRATION_NO, /* NONE */
      T1.PLACE_OF_BUSINESS AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* NONE */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_account AS T2
      ON T1.account_number = T2.account_number
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T2.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T2.effective_from AND T2.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_counterparty AS T3
      ON T2.counterparty_id_1 = T3.counterparty_id
      AND DATE_FORMAT(T3.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND T3.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T3.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_title AS T4
      ON T3.title_id = T4.title_id
      AND T4.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T4.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_country AS T5
      ON T3.Country_Of_Birth_Country_Id = T5.country_id
      AND T5.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T5.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T5.effective_from AND T5.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_bumistatus AS T6
      ON T3.bumi_status_id = T6.bumi_status_id
      AND T6.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T6.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T6.effective_from AND T6.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_race AS T7
      ON T3.race_id = T7.race_id
      AND T7.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T7.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T7.effective_from AND T7.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_maritalstatus AS T8
      ON T3.marital_status_id = T8.marital_status_id AND T8.record_status_id = 3
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_typeofbusiness AS T9
      ON T3.type_of_business_id = T9.type_of_business_id
      AND T9.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T9.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T9.effective_from AND T9.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_residencystatus AS T10
      ON T3.Residency_Status_Id = T10.Residency_Status_Id
      AND T10.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_FORMAT(T10.dl_record_updated_date, 'yyyyMMdd'), 'yyyyMMdd')) BETWEEN T10.effective_from AND T10.effective_to
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      PRIORITY_LEVEL, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUSTOMER_ID AS CUSTOMER_ID, /* None */
      T1.CUSTOMER_TYPE AS CUSTOMER_TYPE, /* None */
      T1.CUSTOMER_TITLE AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.CUSTOMER_NATIONALITY AS CUSTOMER_NATIONALITY, /* None */
      T1.CUSTOMER_COUNTRY_OF_RESIDENCE AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      T1.CUSTOMER_COUNTRY_OF_BIRTH AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      T1.CUSTOMER_DATE_OF_BIRTH AS CUSTOMER_DATE_OF_BIRTH, /* None */
      T1.CUSTOMER_BUMIPUTRA_STATUS AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      T1.CUSTOMER_RACE AS CUSTOMER_RACE, /* None */
      T1.CUSTOMER_GENDER AS CUSTOMER_GENDER, /* None */
      T1.CUSTOMER_MARITAL_STATUS AS CUSTOMER_MARITAL_STATUS, /* None */
      T1.CUSTOMER_COMPANY_CONTACT_PERSON AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      T1.CUSTOMER_COMPANY_OWNERSHIP AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T1.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      T1.CUSTOMER_COMPANY_TYPE_OF_BUSINESS AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      T1.CUSTOMER_COMPANY_DATE_OF_INCORPORATION AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      T1.CUSTOMER_COMPANY_WEBSITE AS CUSTOMER_COMPANY_WEBSITE, /* None */
      T1.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      T1.PDPA_FLAG AS PDPA_FLAG, /* None */
      T1.CONNECTED_PARTY_FLAG AS CONNECTED_PARTY_FLAG, /* None */
      T1.POLITICALLY_EXPOSED_PERSON_FLAG AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      T1.CROSS_SELLING_CONSENT_FLAG AS CROSS_SELLING_CONSENT_FLAG, /* None */
      T1.DCF_FLAG AS DCF_FLAG, /* None */
      T1.MULTI_TRADING_ACCOUNT_FLAG AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      T1.FATCA_FLAG AS FATCA_FLAG, /* None */
      T1.CRS_FLAG AS CRS_FLAG, /* None */
      T1.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      T1.CUSTOMER_RESIDENCY_STATUS AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.UPDATE_DATE AS UPDATE_DATE, /* None */
      ROW_NUMBER() OVER (PARTITION BY T1.CUSTOMER_ID ORDER BY T1.UPDATE_DATE DESC, T1.SYSTEM_NUM ASC) AS PRIORITY_LEVEL, /* None */
      T1.AUTO_EINVOICE_INDICATOR /* None */,
      T1.SST_REGISTRATION_NO /* None */,
      T1.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS /* None */,
      T1.VULNERABLE_FLAG /* 20250715 */
    FROM (
      SELECT
        MHBOS.*,
        1 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MHBOS AS MHBOS
      UNION ALL
      SELECT
        M21.*,
        2 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21 AS M21
      UNION ALL
      SELECT
        M21A.*,
        3 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_A AS M21A
      UNION ALL
      SELECT
        M21O.*,
        4 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_O AS M21O
      UNION ALL
      SELECT
        COM.*,
        5 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_COMPANY AS COM
      UNION ALL
      SELECT
        GUAVA.*,
        6 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_CUSTOMER AS GUAVA
      UNION ALL
      SELECT
        TOMS.*,
        7 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_MERGE AS TOMS
      UNION ALL
      SELECT
        KDI.*,
        8 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_KDI_CLIENTREPORT AS KDI
      UNION ALL
      SELECT
        SBL.*,
        9 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_SBL AS SBL
      UNION ALL
      SELECT
        LMS_COUNTERPARTY.*,
        10 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_COUNTERPARTY AS LMS_COUNTERPARTY
      UNION ALL
      SELECT
        LMS_CLIENTDATA.*,
        11 AS SYSTEM_NUM
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_CLIENTDATA AS LMS_CLIENTDATA
    ) AS T1 /* None */
    WHERE
      1 = 1
""")
spark.sql(f"""
/* ==============[Group.3]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_CUSTOMER
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_CUSTOMER (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_CUSTOMER (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5')
          THEN 'INDIVIDUAL'
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
          THEN 'CORPORATE'
          ELSE NULL
        END
      ) AS CUSTOMER_TYPE, /* None */
      NULL AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.CUSTOMER_NATIONALITY) AS CUSTOMER_NATIONALITY, /* None */
      COALESCE(T3.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COUNTRY_RESIDENCY) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* 20251017 */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      NULL AS CUSTOMER_DATE_OF_BIRTH, /* None */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      NULL AS CUSTOMER_GENDER, /* None */
      NULL AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      COALESCE(T4.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_INCORPORATION) AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251017 */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.ETL_TIMESTAMP AS UPDATE_DATE, /* None */
      T1.e_inv_flag AS AUTO_EINVOICE_INDICATOR, /* None */
      T1.customer_sst_registration_number AS SST_REGISTRATION_NO, /* None */
      COALESCE(T5.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* 20251017 */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_GUAVA_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T2
      ON T1.CUSTOMER_NATIONALITY = T2.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T3
      ON T1.CUSTOMER_COUNTRY_RESIDENCY = T3.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T4
      ON T1.CUSTOMER_COMPANY_PLACE_OF_INCORPORATION = T4.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T5
      ON T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS = T5.COUNTRY_CODE_3_DIGITS
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.PRIMARY_IDENTIFICATION_TYPE LIKE '@[%'
      AND T1.PRIMARY_IDENTIFICATION_TYPE <> ''
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_GUAVA_CUSTOMER
""")
spark.sql(f"""
/* ==============[Group.4 - M21: ALGO DB ]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21 (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21 (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN T1.CLIENTTYPE IN ('CORP', 'EAO', 'FCORP', 'INST', 'INSTLO', 'POC', 'POCFR')
          THEN 'CORPORATE'
          WHEN T1.CLIENTTYPE IN ('DUAL', 'FOIND', 'INDIVID', 'LOCAL', 'RETAIL', 'SINGLE')
          THEN 'INDIVIDUAL'
          ELSE T1.CLIENTTYPE
        END
      ) AS CUSTOMER_TYPE, /* None */
      T1.SALUTATION AS CUSTOMER_TITLE, /* None */
      T1.FULLNAME AS CUSTOMER_NAME, /* None */
      (
        CASE
          WHEN T1.CLIENTTYPE IN ('CORP', 'EAO', 'FCORP', 'INST', 'INSTLO', 'POC', 'POCFR')
          THEN '4'
          WHEN T1.CLIENTTYPE IN ('DUAL', 'FOIND', 'INDIVID', 'LOCAL', 'RETAIL', 'SINGLE')
          AND LENGTH(T1.NRICNUMBER) = 12
          THEN '1'
          ELSE NULL
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.NRICNUMBER AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      (
        CASE
          WHEN T1.CLIENTTYPE IN ('CORP', 'EAO', 'FCORP', 'INST', 'INSTLO', 'POC', 'POCFR')
          AND COALESCE(T1.OLDNRICNUMBER, '') <> ''
          THEN '4'
          ELSE NULL
        END
      ) AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.OLDNRICNUMBER AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.NATIONALITY AS CUSTOMER_NATIONALITY, /* None */
      UPPER(T6.COUNTRY) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* 20251024 */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      (
        CASE WHEN T1.DATEOFBIRTH = '2088-08-08' THEN NULL ELSE T1.DATEOFBIRTH END
      ) AS CUSTOMER_DATE_OF_BIRTH, /* None */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      T1.GENDER AS CUSTOMER_GENDER, /* None */
      (
        CASE WHEN T1.MARITALSTATUS = 'N' THEN 'U' ELSE T1.MARITALSTATUS END
      ) AS CUSTOMER_MARITAL_STATUS, /* None */
      T1.CONTACTPERSON AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CASE
        WHEN T1.CLIENTTYPE IN ('CORP', 'EAO', 'FCORP', 'INST', 'INSTLO', 'POC', 'POCFR')
        THEN T1.NATIONALITY
      END AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251016 */
      T1.NATUREOFBUSINESS AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      T2.DECLARATION_FLAG AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      T3.DECLARATION_FLAG AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      (
        CASE WHEN NOT T4.CUSTOMER_ID IS NULL THEN 'Y' ELSE NULL END
      ) AS FATCA_FLAG, /* None */
      (
        CASE WHEN NOT T5.CUSTOMER_ID IS NULL THEN 'Y' ELSE NULL END
      ) AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      GREATEST(COALESCE(T1.CREATEDATE, '1900-01-01'), COALESCE(T1.MODIFYDATE, '1900-01-01')) AS UPDATE_DATE, /* None */
      NULL AS AUTO_EINVOICE_INDICATOR, /* None */
      NULL AS SST_REGISTRATION_NO, /* None */
      UPPER(T6.COUNTRY) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* 20251024 */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_M21_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT
        CUSTOMER_ID,
        DECLARATION_FLAG,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY SOURCE_NAME) AS ROW_NUM
      FROM {params["cur_schema"]}.DIM_DECLARATION
      WHERE
        DECLARATION_TYPE = 'PDPA'
        AND SOURCE_NAME = 'M21'
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T2 /* None */
      ON T1.CUST_ID = T2.CUSTOMER_ID AND T2.ROW_NUM = 1
    LEFT JOIN (
      SELECT
        CUSTOMER_ID,
        DECLARATION_FLAG,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY SOURCE_NAME) AS ROW_NUM
      FROM {params["cur_schema"]}.DIM_DECLARATION
      WHERE
        DECLARATION_TYPE = 'POLITICALLY EXPOSED PERSON'
        AND SOURCE_NAME = 'M21'
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* None */
      ON T1.CUST_ID = T3.CUSTOMER_ID AND T3.ROW_NUM = 1
    LEFT JOIN (
      SELECT DISTINCT
        CUSTOMER_ID
      FROM {params["cur_schema"]}.DIM_FATCA
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND SOURCE_NAME = 'M21'
    ) AS T4 /* None */
      ON T1.CUST_ID = T4.CUSTOMER_ID
    LEFT JOIN (
      SELECT DISTINCT
        CUSTOMER_ID
      FROM {params["cur_schema"]}.DIM_CRS
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND SOURCE_NAME = 'M21'
    ) AS T5 /* None */
      ON T1.CUST_ID = T5.CUSTOMER_ID
    LEFT JOIN {params["com_schema"]}.T_M21_STATEMENTRECIPIENT AS T6
      ON T6.CODE = T1.CODE
      AND DATE_FORMAT(T6.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Group.5 - M21: ALGO File ]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_A
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_A (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_A (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5', '01', '02', '03', '05')
          THEN 'INDIVIDUAL'
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6', '04', '06')
          THEN 'CORPORATE'
          ELSE NULL
        END
      ) AS CUSTOMER_TYPE, /* None */
      NULL AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.customer_nationality AS CUSTOMER_NATIONALITY, /* None */
      COALESCE(T3.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COUNTRY_RESIDENCY) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* 20251029 */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      NULL AS CUSTOMER_DATE_OF_BIRTH, /* None */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      NULL AS CUSTOMER_GENDER, /* None */
      NULL AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_INCORPORATION) AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251016 */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.ETL_TIMESTAMP AS UPDATE_DATE, /* None */
      T1.e_inv_flag AS AUTO_EINVOICE_INDICATOR, /* None */
      T1.customer_sst_registration_number AS SST_REGISTRATION_NO, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_M21_A_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_CCRIS,
        COUNTRY_CODE_3_DIGITS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_3_DIGITS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T2 /* None */
      ON T2.COUNTRY_CODE_3_DIGITS = T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_CCRIS,
        COUNTRY_CODE_3_DIGITS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_3_DIGITS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* 20251029 */
      ON T3.COUNTRY_CODE_3_DIGITS = T1.CUSTOMER_COUNTRY_RESIDENCY
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.clean_rule_flag LIKE '%1%'
""")
spark.sql(f"""
/* ==============[Group.6 - M21: OVEX File ]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_O
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_O (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21_O (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5', '01', '02', '03', '05')
          THEN 'INDIVIDUAL'
          WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6', '04', '06')
          THEN 'CORPORATE'
          ELSE NULL
        END
      ) AS CUSTOMER_TYPE, /* None */
      NULL AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.customer_nationality AS CUSTOMER_NATIONALITY, /* None */
      COALESCE(T3.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COUNTRY_RESIDENCY) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* 20251029 */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      NULL AS CUSTOMER_DATE_OF_BIRTH, /* None */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      NULL AS CUSTOMER_GENDER, /* None */
      NULL AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_INCORPORATION) AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251016 */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.ETL_TIMESTAMP AS UPDATE_DATE, /* None */
      T1.e_inv_flag AS AUTO_EINVOICE_INDICATOR, /* None */
      T1.customer_sst_registration_number AS SST_REGISTRATION_NO, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_M21_O_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T2
      ON T2.COUNTRY_CODE_3_DIGITS = T1.CUSTOMER_COMPANY_PLACE_OF_BUSINESS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_CCRIS,
        COUNTRY_CODE_3_DIGITS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_3_DIGITS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T3 /* 20251029 */
      ON T3.COUNTRY_CODE_3_DIGITS = T1.CUSTOMER_COUNTRY_RESIDENCY
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.clean_rule_flag LIKE '%1%'
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_M21
""")
spark.sql(f"""
/* ==============[Group.7]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ECORPORATE_CIF
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ECORPORATE_CIF (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ECORPORATE_CIF (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* NONE */
      'CORPORATE' AS CUSTOMER_TYPE, /* NONE */
      NULL AS CUSTOMER_TITLE, /* NONE */
      T1.NM AS CUSTOMER_NAME, /* NONE */
      '4' AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* NONE */
      T1.BUSREG AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* NONE */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* NONE */
      NULL AS CUSTOMER_NATIONALITY, /* NONE */
      NULL AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* NONE AS CUSTOMER_COUNTRY_OF_RESIDENCE -- NONE */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* NONE */
      NULL AS CUSTOMER_DATE_OF_BIRTH, /* NONE */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* NONE */
      NULL AS CUSTOMER_RACE, /* NONE */
      NULL AS CUSTOMER_GENDER, /* NONE */
      NULL AS CUSTOMER_MARITAL_STATUS, /* NONE */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* NONE */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* NONE */
      T2.REFERENCE_VALUE_2 AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* NONE */
      UPPER(T3.REFERENCE_VALUE) AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* NONE */
      T1.DOC AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* NONE */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* NONE */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* NONE */
      NULL AS PDPA_FLAG, /* NONE */
      NULL AS CONNECTED_PARTY_FLAG, /* NONE */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* NONE */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* NONE */
      NULL AS DCF_FLAG, /* NONE */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* NONE */
      NULL AS FATCA_FLAG, /* NONE */
      NULL AS CRS_FLAG, /* NONE */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* NONE */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* NONE */
      GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS UPDATE_DATE, /* NONE */
      T1.EINVIND AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T1.SSTREGNO AS SST_REGISTRATION_NO, /* NONE */
      UPPER(T2.REFERENCE_VALUE) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_TOMS_ECORPORATE_CIF AS T1 /* NONE */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T2.REFERENCE_CODE = T1.COUNTRYOFINCORP
      AND /* AND T2.ETL_DT = '{batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.SOURCE_NAME = 'TOMS'
      AND T2.REFERENCE_TYPE = 'COUNTRY'
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3 /* None */
      ON T3.REFERENCE_CODE = T1.NATUREOFBUSINESS
      AND /* AND T2.ETL_DT = '{batch_date}' */ T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T3.SOURCE_NAME = 'TOMS'
      AND T3.REFERENCE_TYPE = 'TOMSBIZTYPEMAP'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
""")
spark.sql(f"""
/* ==============[Group.7]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ERETAIL_CIF
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ERETAIL_CIF (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
WITH ref_lookup AS (
      SELECT
        reference_type,
        reference_code,
        reference_value,
        reference_value_2,
        source_name
      FROM {params["cur_schema"]}.REF_LOOKUP
      WHERE
        SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' AND source_name = 'TOMS'
    ), t_toms_eretail_account AS (
      SELECT
        idref,
        ccountry,
        ROW_NUMBER() OVER (PARTITION BY idref ORDER BY GREATEST(COALESCE(sydtc, '1900-01-01'), COALESCE(sydtu, '1900-01-01')) DESC, accountno DESC) AS rn
      FROM {params["com_schema"]}.t_toms_eretail_account
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ERETAIL_CIF (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* NONE */
      'INDIVIDUAL' AS CUSTOMER_TYPE, /* NONE */
      T2.REFERENCE_VALUE_2 AS CUSTOMER_TITLE, /* NONE */
      T1.NM AS CUSTOMER_NAME, /* NONE */
      T1.IDTYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* NONE */
      T1.IDREF AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* NONE */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* NONE */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* NONE */
      T4.REFERENCE_VALUE_2 AS CUSTOMER_NATIONALITY, /* NONE */
      T7.REFERENCE_VALUE_2 AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* NONE */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* NONE */
      T1.DOB AS CUSTOMER_DATE_OF_BIRTH, /* NONE */
      CASE
        WHEN T1.RACE IN ('13', '16', '17', '20', '21')
        THEN 'Y'
        WHEN T1.RACE IN ('14', '15', '18', '22')
        THEN 'N'
        ELSE NULL
      END AS CUSTOMER_BUMIPUTRA_STATUS, /* NONE */
      T3.REFERENCE_VALUE_2 AS CUSTOMER_RACE, /* NONE */
      CASE WHEN T1.SEX = '0' THEN 'MALE' WHEN T1.SEX = '1' THEN 'FEMALE' ELSE NULL END AS CUSTOMER_GENDER, /* NONE */
      T5.REFERENCE_VALUE_2 AS CUSTOMER_MARITAL_STATUS, /* NONE */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* NONE */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* NONE */
      NULL AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* NONE */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* NONE */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* NONE */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* NONE */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* NONE */
      NULL AS PDPA_FLAG, /* NONE */
      NULL AS CONNECTED_PARTY_FLAG, /* NONE */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* NONE */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* NONE */
      NULL AS DCF_FLAG, /* NONE */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* NONE */
      NULL AS FATCA_FLAG, /* NONE */
      NULL AS CRS_FLAG, /* NONE */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* NONE */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* NONE */
      GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS UPDATE_DATE, /* NONE */
      T1.EINVIND AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T1.SSTREGNO AS SST_REGISTRATION_NO, /* NONE */
      NULL AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_TOMS_ERETAIL_CIF AS T1 /* NONE */
    LEFT JOIN ref_lookup AS T2 /* None */
      ON T2.REFERENCE_CODE = T1.TITLE
      AND /* AND T2.ETL_DT = '{batch_date}' 
          AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' 
          AND T2.SOURCE_NAME = 'TOMS' */ T2.REFERENCE_TYPE = 'SALUTATION'
    LEFT JOIN ref_lookup AS T3 /* None */
      ON T3.REFERENCE_CODE = T1.RACE
      AND /* AND T3.ETL_DT = '{batch_date}' 
          AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' 
          AND T3.SOURCE_NAME = 'TOMS' */ T3.REFERENCE_TYPE = 'RACE'
    LEFT JOIN ref_lookup AS T4 /* None */
      ON T4.REFERENCE_CODE = T1.NATIONALITY
      AND /* AND T4.ETL_DT = '{batch_date}' 
          AND T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' 
          AND T4.SOURCE_NAME = 'TOMS' */ T4.REFERENCE_TYPE = 'COUNTRY'
    LEFT JOIN ref_lookup AS T5 /* None */
      ON T5.REFERENCE_CODE = T1.MARITALSTATUS
      AND /* AND T5.ETL_DT = '{batch_date}' 
          AND T5.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' 
          AND T5.SOURCE_NAME = 'TOMS' */ T5.REFERENCE_TYPE = 'MARITAL STATUS'
    LEFT JOIN t_toms_eretail_account AS T6
      ON T1.idref = T6.idref AND T6.rn = 1
    LEFT JOIN ref_lookup AS T7 /* None */
      ON T6.ccountry = T7.REFERENCE_CODE
      AND /*    and T7.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP' 
          and T7.source_name = 'TOMS' */ T7.REFERENCE_TYPE = 'COUNTRY'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
""")
spark.sql(f"""
/* ==============[Group.7]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_MERGE
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_MERGE (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_MERGE (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUSTOMER_ID AS CUSTOMER_ID, /* None */
      T1.CUSTOMER_TYPE AS CUSTOMER_TYPE, /* None */
      T1.CUSTOMER_TITLE AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.CUSTOMER_NATIONALITY AS CUSTOMER_NATIONALITY, /* None */
      T1.CUSTOMER_COUNTRY_OF_RESIDENCE AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      T1.CUSTOMER_COUNTRY_OF_BIRTH AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      T1.CUSTOMER_DATE_OF_BIRTH AS CUSTOMER_DATE_OF_BIRTH, /* None */
      T1.CUSTOMER_BUMIPUTRA_STATUS AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      T1.CUSTOMER_RACE AS CUSTOMER_RACE, /* None */
      T1.CUSTOMER_GENDER AS CUSTOMER_GENDER, /* None */
      T1.CUSTOMER_MARITAL_STATUS AS CUSTOMER_MARITAL_STATUS, /* None */
      T1.CUSTOMER_COMPANY_CONTACT_PERSON AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      T1.CUSTOMER_COMPANY_OWNERSHIP AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T1.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      T1.CUSTOMER_COMPANY_TYPE_OF_BUSINESS AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      T1.CUSTOMER_COMPANY_DATE_OF_INCORPORATION AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      T1.CUSTOMER_COMPANY_WEBSITE AS CUSTOMER_COMPANY_WEBSITE, /* None */
      T1.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      T1.PDPA_FLAG AS PDPA_FLAG, /* None */
      T1.CONNECTED_PARTY_FLAG AS CONNECTED_PARTY_FLAG, /* None */
      T1.POLITICALLY_EXPOSED_PERSON_FLAG AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      T1.CROSS_SELLING_CONSENT_FLAG AS CROSS_SELLING_CONSENT_FLAG, /* None */
      T1.DCF_FLAG AS DCF_FLAG, /* None */
      T1.MULTI_TRADING_ACCOUNT_FLAG AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      T1.FATCA_FLAG AS FATCA_FLAG, /* None */
      T1.CRS_FLAG AS CRS_FLAG, /* None */
      T1.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      T1.CUSTOMER_RESIDENCY_STATUS AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.UPDATE_DATE AS UPDATE_DATE, /* None */
      T1.AUTO_EINVOICE_INDICATOR /* None */,
      T1.SST_REGISTRATION_NO /* None */,
      T1.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS /* None */,
      T1.VULNERABLE_FLAG /* 20250715 */
    FROM (
      SELECT
        EC.*
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ECORPORATE_CIF AS EC
      UNION ALL
      SELECT
        ER.*
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ERETAIL_CIF AS ER
    ) AS T1 /* None */
    WHERE
      1 = 1
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_ERETAIL_CIF
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_TOMS_MERGE
""")
spark.sql(f"""
/* ==============[Group.8]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_KDI_CLIENTREPORT
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_KDI_CLIENTREPORT (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_KDI_CLIENTREPORT (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      CASE CAST(T1.IDENTIFICATION_TYPE AS STRING)
        WHEN '1'
        THEN 'INDIVIDUAL'
        WHEN '2'
        THEN 'INDIVIDUAL'
        WHEN '3'
        THEN 'INDIVIDUAL'
        WHEN '5'
        THEN 'INDIVIDUAL'
        WHEN '4'
        THEN 'CORPORATE'
        WHEN '6'
        THEN 'CORPORATE'
        ELSE NULL
      END AS CUSTOMER_TYPE, /* None */
      NULL AS CUSTOMER_TITLE, /* None */
      T1.NAME AS CUSTOMER_NAME, /* None */
      T1.IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.NRIC_PASSPORT AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.SECONDARY_ID_NO_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T1.SECONDARY_ID_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      COALESCE(T2.COUNTRY_CODE_CCRIS, T1.COUNTRY_CODE) AS CUSTOMER_NATIONALITY, /* 20260203 */
      COALESCE(T3.COUNTRY_CODE_CCRIS, T1.COUNTRY_OF_RESIDENCE_CODE) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* 20251024 */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH,
      COALESCE(
        CAST(T1.Date_of_Birth AS DATE),
        CASE
          WHEN REGEXP_REPLACE(T1.nric_passport, '[^0-9]', '') RLIKE '^[0-9]{12}$'
          THEN CAST(FROM_UNIXTIME(
            UNIX_TIMESTAMP(
              CONCAT(
                CASE
                  WHEN CAST(SUBSTRING(REGEXP_REPLACE(T1.nric_passport, '[^0-9]', ''), 1, 2) AS INT) BETWEEN 0 AND 29
                  THEN '20'
                  ELSE '19'
                END,
                SUBSTRING(REGEXP_REPLACE(T1.nric_passport, '[^0-9]', ''), 1, 2),
                '-' /* YY */,
                SUBSTRING(REGEXP_REPLACE(T1.nric_passport, '[^0-9]', ''), 3, 2),
                '-' /* MM */,
                SUBSTRING(REGEXP_REPLACE(T1.nric_passport, '[^0-9]', ''), 5, 2) /* DD */
              ),
              'yyyy-MM-dd'
            )
          ) AS DATE)
          ELSE NULL
        END
      ) AS CUSTOMER_DATE_OF_BIRTH, /* None */
      CASE
        WHEN T1.nationality_code <> 'MYS'
        THEN 'N'
        WHEN T1.nationality_code = 'MYS' AND T1.ethnicity = 'MALAY'
        THEN 'Y'
        WHEN T1.nationality_code = 'MYS' AND T1.ethnicity IN ('CHINESE', 'INDIAN')
        THEN 'N'
        ELSE NULL
      END AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CASE
        WHEN UPPER(TRIM(ethnicity)) LIKE '%BUMI%'
        THEN 'BUMIPUTRA'
        WHEN UPPER(TRIM(ethnicity)) IN ('CHINESE', 'INDIAN', 'MALAY', 'FOREIGNER')
        THEN UPPER(TRIM(ethnicity))
        ELSE 'OTHERS'
      END AS CUSTOMER_RACE, /* None */
      CASE
        WHEN T1.IDENTIFICATION_TYPE = 1
        AND NOT T1.NRIC_PASSPORT IS NULL
        AND LENGTH(REGEXP_REPLACE(T1.NRIC_PASSPORT, '[^0-9]', '')) = 12
        AND REGEXP_REPLACE(T1.NRIC_PASSPORT, '[^0-9]', '') RLIKE '^[0-9]{12}$'
        THEN CASE
          WHEN CAST(SUBSTRING(REGEXP_REPLACE(T1.NRIC_PASSPORT, '[^0-9]', ''), -1) AS INT) % 2 = 0
          THEN 'FEMALE'
          ELSE 'MALE'
        END
        WHEN NOT T1.GENDER IS NULL
        THEN T1.GENDER
        ELSE NULL
      END AS CUSTOMER_GENDER,
      CASE
        WHEN T1.married_or_single = 0
        THEN 'S'
        WHEN T1.married_or_single = 1
        THEN 'M'
        ELSE NULL
      END AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T1.COMPANY_PLACE_INCORPORATION AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* 20251024 */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      CASE WHEN T1.pep_dim = 'YES' THEN 'Y' WHEN T1.pep_dim = 'NO' THEN 'N' ELSE NULL END AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.ETL_TIMESTAMP AS UPDATE_DATE, /* None */
      T1.e_inv_flag AS AUTO_EINVOICE_INDICATOR, /* None */
      T1.sst_registration_number AS SST_REGISTRATION_NO, /* None */
      T1.COMPANY_PLACE_BUSINESS AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* 20251024 
     ,NULL AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_KDI_CLIENTREPORT AS T1 /* None */
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T2
      ON T1.nationality_code = T2.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T3
      ON T1.country_of_residence_code = T3.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T4
      ON T1.country_of_residence_code = T4.COUNTRY_CODE_3_DIGITS
    LEFT JOIN (
      SELECT DISTINCT
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND COALESCE(COUNTRY_CODE_3_DIGITS, '') <> ''
    ) AS T5
      ON T1.country_of_residence_code = T5.COUNTRY_CODE_3_DIGITS
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}' /* -- AND T1.clean_rule_flag not like '%1%' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_KDI_CLIENTREPORT
""")
spark.sql(f"""
/* ==============[Group.10]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_CLIENTDATA
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_CLIENTDATA (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(50), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_CLIENTDATA (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      CASE
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5')
        THEN 'INDIVIDUAL'
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
        THEN 'CORPORATE'
      END AS CUSTOMER_TYPE,
      NULL AS CUSTOMER_TITLE, /* None */
      T1.CUSTOMER_NAME AS CUSTOMER_NAME, /* None */
      T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      TRIM(T1.PRIMARY_IDENTIFICATION_NO) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T1.NATIONALITY AS CUSTOMER_NATIONALITY, /* None */
      T1.COUNTRY_RESIDENCY AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None AS CUSTOMER_COUNTRY_OF_RESIDENCE -- None */
      NULL AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      NULL AS CUSTOMER_DATE_OF_BIRTH, /* None */
      NULL AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      NULL AS CUSTOMER_RACE, /* None */
      NULL AS CUSTOMER_GENDER, /* None */
      NULL AS CUSTOMER_MARITAL_STATUS, /* None */
      NULL AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T1.PLACE_OF_INCORPORATION AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      NULL AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      NULL AS CUSTOMER_COMPANY_WEBSITE, /* None */
      NULL AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      NULL AS PDPA_FLAG, /* None */
      NULL AS CONNECTED_PARTY_FLAG, /* None */
      NULL AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      NULL AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      NULL AS CUSTOMER_RESIDENCY_STATUS, /* None */
      T1.LAST_GENERATED_DATETIME AS UPDATE_DATE, /* None */
      T1.CUSTOMER_E_INVOICING_FLAG AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T1.CUSTOMER_SST_REGISTRATION_NUMBER AS SST_REGISTRATION_NO, /* NONE */
      T1.PLACE_OF_BUSINESS AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* NONE */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_LMS_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_CLIENTDATA
""")
spark.sql(f"""
/* ==============[Source 10: LMS Counterparty]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_COUNTERPARTY
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_COUNTERPARTY (
      CUSTOMER_ID VARCHAR(20), /* None */
      CUSTOMER_TYPE VARCHAR(10), /* None */
      CUSTOMER_TITLE VARCHAR(20), /* None */
      CUSTOMER_NAME VARCHAR(250), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO VARCHAR(20), /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE, /* None */
      CUSTOMER_NATIONALITY VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE VARCHAR(2), /* None */
      CUSTOMER_COUNTRY_OF_BIRTH VARCHAR(2), /* None */
      CUSTOMER_DATE_OF_BIRTH DATE, /* None */
      CUSTOMER_BUMIPUTRA_STATUS VARCHAR(1), /* None */
      CUSTOMER_RACE VARCHAR(20), /* None */
      CUSTOMER_GENDER VARCHAR(10), /* None */
      CUSTOMER_MARITAL_STATUS VARCHAR(20), /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON VARCHAR(100), /* None */
      CUSTOMER_COMPANY_OWNERSHIP VARCHAR(5), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION VARCHAR(2), /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS VARCHAR(200), /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE, /* None */
      CUSTOMER_COMPANY_WEBSITE VARCHAR(100), /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION VARCHAR(100), /* None */
      PDPA_FLAG VARCHAR(1), /* None */
      CONNECTED_PARTY_FLAG VARCHAR(2), /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG VARCHAR(3), /* None */
      CROSS_SELLING_CONSENT_FLAG VARCHAR(4), /* None */
      DCF_FLAG VARCHAR(5), /* None */
      MULTI_TRADING_ACCOUNT_FLAG VARCHAR(6), /* None */
      FATCA_FLAG VARCHAR(7), /* None */
      CRS_FLAG VARCHAR(8), /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION VARCHAR(150), /* None */
      CUSTOMER_RESIDENCY_STATUS VARCHAR(1), /* None */
      UPDATE_DATE TIMESTAMP, /* None */
      AUTO_EINVOICE_INDICATOR VARCHAR(1), /* None */
      SST_REGISTRATION_NO VARCHAR(20), /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS VARCHAR(2), /* None */
      VULNERABLE_FLAG VARCHAR(1) /* 20250715 */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_COUNTERPARTY (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      UPDATE_DATE, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG /* 20250715 */
    )
    SELECT
      T0.CUST_ID AS CUSTOMER_ID, /* None */
      CASE
        WHEN T0.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5')
        THEN 'INDIVIDUAL'
        WHEN T0.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6')
        THEN 'CORPORATE'
      END AS CUSTOMER_TYPE,
      T0.TITLE AS CUSTOMER_TITLE, /* None */
      T0.COUNTERPARTY_NAME AS CUSTOMER_NAME, /* None */
      T0.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      TRIM(T0.PRIMARY_IDENTIFICATION_NO) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      T0.PASSPORT_EXPIRY_DATE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T0.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      T0.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      NULL AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      T0.NATIONALITY_COUNTRY_CODE AS CUSTOMER_NATIONALITY, /* None */
      T0.COUNTERPARTY_COUNTRY_CODE AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None AS CUSTOMER_COUNTRY_OF_RESIDENCE -- None */
      T0.COUNTRY_OF_BIRTH_COUNTRY_CODE AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      T0.DATE_OF_BIRTH AS CUSTOMER_DATE_OF_BIRTH, /* None */
      T0.BUMIPUTERA_STATUS AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      T0.RACE_CODE AS CUSTOMER_RACE, /* None */
      T0.GENDER_CODE AS CUSTOMER_GENDER, /* None */
      T0.MARITAL_STATUS_CODE AS CUSTOMER_MARITAL_STATUS, /* None */
      T0.CONTACT_PERSON_NAME AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      NULL AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      T0.COUNTRY_OF_INCORPORATION_COUNTRY_CODE AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      T0.BUSINESS_CODE AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      T0.DATE_OF_INCORPORATION AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      T0.WEBSITE AS CUSTOMER_COMPANY_WEBSITE, /* None */
      T0.COMPANY_TYPE AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      T0.PDPA AS PDPA_FLAG, /* None */
      T0.PDPA_CONNECTED_PARTY AS CONNECTED_PARTY_FLAG, /* None */
      T0.IS_POLITICALLY_EXPOSED_PERSON AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      NULL AS CROSS_SELLING_CONSENT_FLAG, /* None */
      NULL AS DCF_FLAG, /* None */
      NULL AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      T0.IS_FATCA AS FATCA_FLAG, /* None */
      NULL AS CRS_FLAG, /* None */
      NULL AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      T0.RESIDENCY_STATUS_CODE AS CUSTOMER_RESIDENCY_STATUS, /* None */
      GREATEST(
        COALESCE(T0.SYSTEM_UPDATED_DATETIME, '1900-01-01'),
        COALESCE(T0.LAST_ACTION_DATETIME, '1900-01-01')
      ) AS UPDATE_DATE, /* None */
      T0.E_INVOICE_INDICATOR AS AUTO_EINVOICE_INDICATOR, /* NONE */
      T0.CUSTOMER_SST_REGISTRATION_NUMBER AS SST_REGISTRATION_NO, /* NONE */
      T0.COUNTRY_OF_OPERATION_COUNTRY_CODE AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* NONE */
      NULL AS VULNERABLE_FLAG
    FROM {params["com_schema"]}.M_LMSKIBB2_TBL_COUNTERPARTY AS T0 /* None */
    WHERE
      DATE_FORMAT(T0.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND NOT T0.CLEAN_RULE_FLAG LIKE '%1%'
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_LMS_COUNTERPARTY
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
# This table will hold the transformed delta records from the COM layer.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_customer_lmskibb2_delta""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_customer_lmskibb2_delta (
        customer_id VARCHAR(20)
        , customer_type VARCHAR(10)
        , customer_title VARCHAR(20)
        , customer_name VARCHAR(250)
        , customer_primary_identification_no_type VARCHAR(20)
        , customer_primary_identification_no VARCHAR(20)
        , customer_primary_identification_no_expiry_date DATE
        , customer_secondary_identification_no_type VARCHAR(20)
        , customer_secondary_identification_no VARCHAR(20)
        , customer_secondary_identification_no_expiry_date DATE
        , customer_nationality VARCHAR(2)
        , customer_country_of_residence VARCHAR(2)
        , customer_country_of_birth VARCHAR(2)
        , customer_date_of_birth DATE
        , customer_bumiputra_status VARCHAR(1)
        , customer_race VARCHAR(20)
        , customer_gender VARCHAR(10)
        , customer_marital_status VARCHAR(20)
        , customer_company_contact_person VARCHAR(100)
        , customer_company_ownership VARCHAR(5)
        , customer_company_country_of_registration VARCHAR(2)
        , customer_company_type_of_business VARCHAR(200)
        , customer_company_date_of_incorporation DATE
        , customer_company_website VARCHAR(100)
        , customer_company_type_of_organization VARCHAR(100)
        , pdpa_flag VARCHAR(1)
        , connected_party_flag VARCHAR(1)
        , politically_exposed_person_flag VARCHAR(3)
        , cross_selling_consent_flag VARCHAR(4)
        , dcf_flag VARCHAR(5)
        , multi_trading_account_flag VARCHAR(6)
        , fatca_flag VARCHAR(7)
        , crs_flag VARCHAR(8)
        , customer_company_personnel_designation VARCHAR(150)
        , customer_residency_status VARCHAR(1)
        , auto_einvoice_indicator VARCHAR(1)
        , sst_registration_no VARCHAR(20)
        , customer_company_country_of_business VARCHAR(2)
        , vulnerable_flag VARCHAR(1)
        , dl_record_status       VARCHAR(10)
        , hash_value             STRING
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── MAIN PROCESSING (Populate Delta Table) ───────────────────────────────────
# The main_processing_sqls should contain the logic to transform and insert data
# from com_t into the temp delta table, filtering for the current batch_date,
# and calculating the hash_value.
spark.sql(f"""
ALTER TABLE {params["cur_schema"]}.temp_DIM_CUSTOMER_main_consolidated DROP IF EXISTS
""")
spark.sql(f"""
/* ==============[Group.11]============== */
    INSERT INTO {params["cur_schema"]}.temp_DIM_CUSTOMER_main_consolidated (
      CUSTOMER_ID, /* None */
      CUSTOMER_TYPE, /* None */
      CUSTOMER_TITLE, /* None */
      CUSTOMER_NAME, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      CUSTOMER_NATIONALITY, /* None */
      CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      CUSTOMER_DATE_OF_BIRTH, /* None */
      CUSTOMER_BUMIPUTRA_STATUS, /* None */
      CUSTOMER_RACE, /* None */
      CUSTOMER_GENDER, /* None */
      CUSTOMER_MARITAL_STATUS, /* None */
      CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      CUSTOMER_COMPANY_OWNERSHIP, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      CUSTOMER_COMPANY_WEBSITE, /* None */
      CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      PDPA_FLAG, /* None */
      CONNECTED_PARTY_FLAG, /* None */
      POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      CROSS_SELLING_CONSENT_FLAG, /* None */
      DCF_FLAG, /* None */
      MULTI_TRADING_ACCOUNT_FLAG, /* None */
      FATCA_FLAG, /* None */
      CRS_FLAG, /* None */
      CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      CUSTOMER_RESIDENCY_STATUS, /* None */
      AUTO_EINVOICE_INDICATOR, /* None */
      SST_REGISTRATION_NO, /* None */
      CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      VULNERABLE_FLAG, /* 20250715 */
      ETL_TIMESTAMP
    )
    SELECT
      T1.CUSTOMER_ID AS CUSTOMER_ID, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_TYPE, '') <> ''
          THEN T1.CUSTOMER_TYPE
          WHEN COALESCE(T2.CUSTOMER_TYPE, '') <> ''
          THEN T2.CUSTOMER_TYPE
          WHEN COALESCE(T3.CUSTOMER_TYPE, '') <> ''
          THEN T3.CUSTOMER_TYPE
          WHEN COALESCE(T4.CUSTOMER_TYPE, '') <> ''
          THEN T4.CUSTOMER_TYPE
          WHEN COALESCE(T5.CUSTOMER_TYPE, '') <> ''
          THEN T5.CUSTOMER_TYPE
          WHEN COALESCE(T6.CUSTOMER_TYPE, '') <> ''
          THEN T6.CUSTOMER_TYPE
          WHEN COALESCE(T7.CUSTOMER_TYPE, '') <> ''
          THEN T7.CUSTOMER_TYPE
          WHEN COALESCE(T8.CUSTOMER_TYPE, '') <> ''
          THEN T8.CUSTOMER_TYPE
          WHEN COALESCE(T9.CUSTOMER_TYPE, '') <> ''
          THEN T9.CUSTOMER_TYPE
          WHEN COALESCE(T10.CUSTOMER_TYPE, '') <> ''
          THEN T10.CUSTOMER_TYPE
          WHEN COALESCE(T11.CUSTOMER_TYPE, '') <> ''
          THEN T11.CUSTOMER_TYPE
          ELSE COALESCE(T1.CUSTOMER_TYPE, '')
        END
      ) AS CUSTOMER_TYPE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_TITLE, '') <> ''
          THEN T1.CUSTOMER_TITLE
          WHEN COALESCE(T2.CUSTOMER_TITLE, '') <> ''
          THEN T2.CUSTOMER_TITLE
          WHEN COALESCE(T3.CUSTOMER_TITLE, '') <> ''
          THEN T3.CUSTOMER_TITLE
          WHEN COALESCE(T4.CUSTOMER_TITLE, '') <> ''
          THEN T4.CUSTOMER_TITLE
          WHEN COALESCE(T5.CUSTOMER_TITLE, '') <> ''
          THEN T5.CUSTOMER_TITLE
          WHEN COALESCE(T6.CUSTOMER_TITLE, '') <> ''
          THEN T6.CUSTOMER_TITLE
          WHEN COALESCE(T7.CUSTOMER_TITLE, '') <> ''
          THEN T7.CUSTOMER_TITLE
          WHEN COALESCE(T8.CUSTOMER_TITLE, '') <> ''
          THEN T8.CUSTOMER_TITLE
          WHEN COALESCE(T9.CUSTOMER_TITLE, '') <> ''
          THEN T9.CUSTOMER_TITLE
          WHEN COALESCE(T10.CUSTOMER_TITLE, '') <> ''
          THEN T10.CUSTOMER_TITLE
          WHEN COALESCE(T11.CUSTOMER_TITLE, '') <> ''
          THEN T11.CUSTOMER_TITLE
          ELSE COALESCE(T1.CUSTOMER_TITLE, '')
        END
      ) AS CUSTOMER_TITLE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_NAME, '') <> ''
          THEN T1.CUSTOMER_NAME
          WHEN COALESCE(T2.CUSTOMER_NAME, '') <> ''
          THEN T2.CUSTOMER_NAME
          WHEN COALESCE(T3.CUSTOMER_NAME, '') <> ''
          THEN T3.CUSTOMER_NAME
          WHEN COALESCE(T4.CUSTOMER_NAME, '') <> ''
          THEN T4.CUSTOMER_NAME
          WHEN COALESCE(T5.CUSTOMER_NAME, '') <> ''
          THEN T5.CUSTOMER_NAME
          WHEN COALESCE(T6.CUSTOMER_NAME, '') <> ''
          THEN T6.CUSTOMER_NAME
          WHEN COALESCE(T7.CUSTOMER_NAME, '') <> ''
          THEN T7.CUSTOMER_NAME
          WHEN COALESCE(T8.CUSTOMER_NAME, '') <> ''
          THEN T8.CUSTOMER_NAME
          WHEN COALESCE(T9.CUSTOMER_NAME, '') <> ''
          THEN T9.CUSTOMER_NAME
          WHEN COALESCE(T10.CUSTOMER_NAME, '') <> ''
          THEN T10.CUSTOMER_NAME
          WHEN COALESCE(T11.CUSTOMER_NAME, '') <> ''
          THEN T11.CUSTOMER_NAME
          ELSE COALESCE(T1.CUSTOMER_NAME, '')
        END
      ) AS CUSTOMER_NAME, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE
          ELSE COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, '')
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          WHEN COALESCE(T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '') <> ''
          THEN T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO
          ELSE COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO, '')
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T2.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T3.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T4.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T5.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T6.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T7.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T8.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T9.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T10.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T11.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE
          ELSE COALESCE(T1.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, '')
        END
      ) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          WHEN COALESCE(T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '') <> ''
          THEN T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE
          ELSE COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, '')
        END
      ) AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          WHEN COALESCE(T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '') <> ''
          THEN T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO
          ELSE COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO, '')
        END
      ) AS CUSTOMER_SECONDARY_IDENTIFICATION_NO, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T2.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T3.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T4.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T5.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T6.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T7.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T8.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T9.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T10.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          WHEN COALESCE(T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '') <> ''
          THEN T11.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE
          ELSE COALESCE(T1.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, '')
        END
      ) AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_NATIONALITY, '') <> ''
          THEN T1.CUSTOMER_NATIONALITY
          WHEN COALESCE(T2.CUSTOMER_NATIONALITY, '') <> ''
          THEN T2.CUSTOMER_NATIONALITY
          WHEN COALESCE(T3.CUSTOMER_NATIONALITY, '') <> ''
          THEN T3.CUSTOMER_NATIONALITY
          WHEN COALESCE(T4.CUSTOMER_NATIONALITY, '') <> ''
          THEN T4.CUSTOMER_NATIONALITY
          WHEN COALESCE(T5.CUSTOMER_NATIONALITY, '') <> ''
          THEN T5.CUSTOMER_NATIONALITY
          WHEN COALESCE(T6.CUSTOMER_NATIONALITY, '') <> ''
          THEN T6.CUSTOMER_NATIONALITY
          WHEN COALESCE(T7.CUSTOMER_NATIONALITY, '') <> ''
          THEN T7.CUSTOMER_NATIONALITY
          WHEN COALESCE(T8.CUSTOMER_NATIONALITY, '') <> ''
          THEN T8.CUSTOMER_NATIONALITY
          WHEN COALESCE(T9.CUSTOMER_NATIONALITY, '') <> ''
          THEN T9.CUSTOMER_NATIONALITY
          WHEN COALESCE(T10.CUSTOMER_NATIONALITY, '') <> ''
          THEN T10.CUSTOMER_NATIONALITY
          WHEN COALESCE(T11.CUSTOMER_NATIONALITY, '') <> ''
          THEN T11.CUSTOMER_NATIONALITY
          ELSE COALESCE(T1.CUSTOMER_NATIONALITY, '')
        END
      ) AS CUSTOMER_NATIONALITY, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T1.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T2.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T2.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T3.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T3.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T4.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T4.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T5.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T5.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T6.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T6.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T7.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T7.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T8.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T8.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T9.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T9.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T10.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T10.CUSTOMER_COUNTRY_OF_RESIDENCE
          WHEN COALESCE(T11.CUSTOMER_COUNTRY_OF_RESIDENCE, '') <> ''
          THEN T11.CUSTOMER_COUNTRY_OF_RESIDENCE
          ELSE COALESCE(T1.CUSTOMER_COUNTRY_OF_RESIDENCE, '')
        END
      ) AS CUSTOMER_COUNTRY_OF_RESIDENCE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T1.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T2.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T2.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T3.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T3.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T4.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T4.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T5.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T5.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T6.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T6.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T7.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T7.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T8.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T8.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T9.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T9.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T10.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T10.CUSTOMER_COUNTRY_OF_BIRTH
          WHEN COALESCE(T11.CUSTOMER_COUNTRY_OF_BIRTH, '') <> ''
          THEN T11.CUSTOMER_COUNTRY_OF_BIRTH
          ELSE COALESCE(T1.CUSTOMER_COUNTRY_OF_BIRTH, '')
        END
      ) AS CUSTOMER_COUNTRY_OF_BIRTH, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T1.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T2.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T2.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T3.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T3.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T4.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T4.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T5.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T5.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T6.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T6.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T7.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T7.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T8.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T8.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T9.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T9.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T10.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T10.CUSTOMER_DATE_OF_BIRTH
          WHEN COALESCE(T11.CUSTOMER_DATE_OF_BIRTH, '') <> ''
          THEN T11.CUSTOMER_DATE_OF_BIRTH
          ELSE COALESCE(T1.CUSTOMER_DATE_OF_BIRTH, '')
        END
      ) AS CUSTOMER_DATE_OF_BIRTH, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T1.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T2.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T2.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T3.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T3.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T4.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T4.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T5.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T5.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T6.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T6.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T7.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T7.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T8.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T8.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T9.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T9.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T10.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T10.CUSTOMER_BUMIPUTRA_STATUS
          WHEN COALESCE(T11.CUSTOMER_BUMIPUTRA_STATUS, '') <> ''
          THEN T11.CUSTOMER_BUMIPUTRA_STATUS
          ELSE COALESCE(T1.CUSTOMER_BUMIPUTRA_STATUS, '')
        END
      ) AS CUSTOMER_BUMIPUTRA_STATUS, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_RACE, '') <> ''
          THEN T1.CUSTOMER_RACE
          WHEN COALESCE(T2.CUSTOMER_RACE, '') <> ''
          THEN T2.CUSTOMER_RACE
          WHEN COALESCE(T3.CUSTOMER_RACE, '') <> ''
          THEN T3.CUSTOMER_RACE
          WHEN COALESCE(T4.CUSTOMER_RACE, '') <> ''
          THEN T4.CUSTOMER_RACE
          WHEN COALESCE(T5.CUSTOMER_RACE, '') <> ''
          THEN T5.CUSTOMER_RACE
          WHEN COALESCE(T6.CUSTOMER_RACE, '') <> ''
          THEN T6.CUSTOMER_RACE
          WHEN COALESCE(T7.CUSTOMER_RACE, '') <> ''
          THEN T7.CUSTOMER_RACE
          WHEN COALESCE(T8.CUSTOMER_RACE, '') <> ''
          THEN T8.CUSTOMER_RACE
          WHEN COALESCE(T9.CUSTOMER_RACE, '') <> ''
          THEN T9.CUSTOMER_RACE
          WHEN COALESCE(T10.CUSTOMER_RACE, '') <> ''
          THEN T10.CUSTOMER_RACE
          WHEN COALESCE(T11.CUSTOMER_RACE, '') <> ''
          THEN T11.CUSTOMER_RACE
          ELSE COALESCE(T1.CUSTOMER_RACE, '')
        END
      ) AS CUSTOMER_RACE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_GENDER, '') <> ''
          THEN T1.CUSTOMER_GENDER
          WHEN COALESCE(T2.CUSTOMER_GENDER, '') <> ''
          THEN T2.CUSTOMER_GENDER
          WHEN COALESCE(T3.CUSTOMER_GENDER, '') <> ''
          THEN T3.CUSTOMER_GENDER
          WHEN COALESCE(T4.CUSTOMER_GENDER, '') <> ''
          THEN T4.CUSTOMER_GENDER
          WHEN COALESCE(T5.CUSTOMER_GENDER, '') <> ''
          THEN T5.CUSTOMER_GENDER
          WHEN COALESCE(T6.CUSTOMER_GENDER, '') <> ''
          THEN T6.CUSTOMER_GENDER
          WHEN COALESCE(T7.CUSTOMER_GENDER, '') <> ''
          THEN T7.CUSTOMER_GENDER
          WHEN COALESCE(T8.CUSTOMER_GENDER, '') <> ''
          THEN T8.CUSTOMER_GENDER
          WHEN COALESCE(T9.CUSTOMER_GENDER, '') <> ''
          THEN T9.CUSTOMER_GENDER
          WHEN COALESCE(T10.CUSTOMER_GENDER, '') <> ''
          THEN T10.CUSTOMER_GENDER
          WHEN COALESCE(T11.CUSTOMER_GENDER, '') <> ''
          THEN T11.CUSTOMER_GENDER
          ELSE COALESCE(T1.CUSTOMER_GENDER, '')
        END
      ) AS CUSTOMER_GENDER, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T1.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T2.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T2.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T3.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T3.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T4.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T4.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T5.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T5.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T6.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T6.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T7.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T7.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T8.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T8.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T9.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T9.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T10.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T10.CUSTOMER_MARITAL_STATUS
          WHEN COALESCE(T11.CUSTOMER_MARITAL_STATUS, '') <> ''
          THEN T11.CUSTOMER_MARITAL_STATUS
          ELSE COALESCE(T1.CUSTOMER_MARITAL_STATUS, '')
        END
      ) AS CUSTOMER_MARITAL_STATUS, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T1.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T2.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T2.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T3.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T3.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T4.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T4.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T5.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T5.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T6.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T6.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T7.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T7.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T8.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T8.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T9.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T9.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T10.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T10.CUSTOMER_COMPANY_CONTACT_PERSON
          WHEN COALESCE(T11.CUSTOMER_COMPANY_CONTACT_PERSON, '') <> ''
          THEN T11.CUSTOMER_COMPANY_CONTACT_PERSON
          ELSE COALESCE(T1.CUSTOMER_COMPANY_CONTACT_PERSON, '')
        END
      ) AS CUSTOMER_COMPANY_CONTACT_PERSON, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T1.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T2.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T2.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T3.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T3.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T4.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T4.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T5.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T5.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T6.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T6.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T7.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T7.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T8.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T8.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T9.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T9.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T10.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T10.CUSTOMER_COMPANY_OWNERSHIP
          WHEN COALESCE(T11.CUSTOMER_COMPANY_OWNERSHIP, '') <> ''
          THEN T11.CUSTOMER_COMPANY_OWNERSHIP
          ELSE COALESCE(T1.CUSTOMER_COMPANY_OWNERSHIP, '')
        END
      ) AS CUSTOMER_COMPANY_OWNERSHIP, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T1.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T2.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T2.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T3.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T3.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T4.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T4.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T5.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T5.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T6.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T6.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T7.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T7.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T8.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T8.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T9.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T9.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T10.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T10.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          WHEN COALESCE(T11.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '') <> ''
          THEN T11.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION
          ELSE COALESCE(T1.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, '')
        END
      ) AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T1.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T2.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T2.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T3.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T3.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T4.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T4.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T5.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T5.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T6.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T6.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T7.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T7.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T8.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T8.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T9.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T9.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T10.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T10.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          WHEN COALESCE(T11.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '') <> ''
          THEN T11.CUSTOMER_COMPANY_TYPE_OF_BUSINESS
          ELSE COALESCE(T1.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, '')
        END
      ) AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T1.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T2.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T2.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T3.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T3.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T4.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T4.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T5.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T5.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T6.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T6.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T7.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T7.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T8.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T8.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T9.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T9.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T10.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T10.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          WHEN COALESCE(T11.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '') <> ''
          THEN T11.CUSTOMER_COMPANY_DATE_OF_INCORPORATION
          ELSE COALESCE(T1.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, '')
        END
      ) AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T1.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T2.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T2.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T3.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T3.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T4.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T4.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T5.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T5.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T6.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T6.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T7.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T7.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T8.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T8.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T9.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T9.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T10.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T10.CUSTOMER_COMPANY_WEBSITE
          WHEN COALESCE(T11.CUSTOMER_COMPANY_WEBSITE, '') <> ''
          THEN T11.CUSTOMER_COMPANY_WEBSITE
          ELSE COALESCE(T1.CUSTOMER_COMPANY_WEBSITE, '')
        END
      ) AS CUSTOMER_COMPANY_WEBSITE, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T1.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T2.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T2.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T3.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T3.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T4.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T4.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T5.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T5.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T6.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T6.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T7.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T7.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T8.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T8.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T9.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T9.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T10.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T10.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          WHEN COALESCE(T11.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '') <> ''
          THEN T11.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION
          ELSE COALESCE(T1.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, '')
        END
      ) AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, /* None */
      (
        CASE
          WHEN COALESCE(T1.PDPA_FLAG, '') <> ''
          THEN T1.PDPA_FLAG
          WHEN COALESCE(T2.PDPA_FLAG, '') <> ''
          THEN T2.PDPA_FLAG
          WHEN COALESCE(T3.PDPA_FLAG, '') <> ''
          THEN T3.PDPA_FLAG
          WHEN COALESCE(T4.PDPA_FLAG, '') <> ''
          THEN T4.PDPA_FLAG
          WHEN COALESCE(T5.PDPA_FLAG, '') <> ''
          THEN T5.PDPA_FLAG
          WHEN COALESCE(T6.PDPA_FLAG, '') <> ''
          THEN T6.PDPA_FLAG
          WHEN COALESCE(T7.PDPA_FLAG, '') <> ''
          THEN T7.PDPA_FLAG
          WHEN COALESCE(T8.PDPA_FLAG, '') <> ''
          THEN T8.PDPA_FLAG
          WHEN COALESCE(T9.PDPA_FLAG, '') <> ''
          THEN T9.PDPA_FLAG
          WHEN COALESCE(T10.PDPA_FLAG, '') <> ''
          THEN T10.PDPA_FLAG
          WHEN COALESCE(T11.PDPA_FLAG, '') <> ''
          THEN T11.PDPA_FLAG
          ELSE COALESCE(T1.PDPA_FLAG, '')
        END
      ) AS PDPA_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T1.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T2.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T2.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T3.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T3.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T4.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T4.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T5.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T5.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T6.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T6.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T7.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T7.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T8.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T8.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T9.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T9.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T10.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T10.CONNECTED_PARTY_FLAG
          WHEN COALESCE(T11.CONNECTED_PARTY_FLAG, '') <> ''
          THEN T11.CONNECTED_PARTY_FLAG
          ELSE COALESCE(T1.CONNECTED_PARTY_FLAG, '')
        END
      ) AS CONNECTED_PARTY_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T1.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T2.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T2.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T3.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T3.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T4.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T4.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T5.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T5.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T6.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T6.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T7.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T7.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T8.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T8.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T9.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T9.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T10.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T10.POLITICALLY_EXPOSED_PERSON_FLAG
          WHEN COALESCE(T11.POLITICALLY_EXPOSED_PERSON_FLAG, '') <> ''
          THEN T11.POLITICALLY_EXPOSED_PERSON_FLAG
          ELSE COALESCE(T1.POLITICALLY_EXPOSED_PERSON_FLAG, '')
        END
      ) AS POLITICALLY_EXPOSED_PERSON_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T1.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T2.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T2.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T3.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T3.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T4.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T4.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T5.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T5.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T6.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T6.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T7.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T7.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T8.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T8.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T9.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T9.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T10.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T10.CROSS_SELLING_CONSENT_FLAG
          WHEN COALESCE(T11.CROSS_SELLING_CONSENT_FLAG, '') <> ''
          THEN T11.CROSS_SELLING_CONSENT_FLAG
          ELSE COALESCE(T1.CROSS_SELLING_CONSENT_FLAG, '')
        END
      ) AS CROSS_SELLING_CONSENT_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.DCF_FLAG, '') <> ''
          THEN T1.DCF_FLAG
          WHEN COALESCE(T2.DCF_FLAG, '') <> ''
          THEN T2.DCF_FLAG
          WHEN COALESCE(T3.DCF_FLAG, '') <> ''
          THEN T3.DCF_FLAG
          WHEN COALESCE(T4.DCF_FLAG, '') <> ''
          THEN T4.DCF_FLAG
          WHEN COALESCE(T5.DCF_FLAG, '') <> ''
          THEN T5.DCF_FLAG
          WHEN COALESCE(T6.DCF_FLAG, '') <> ''
          THEN T6.DCF_FLAG
          WHEN COALESCE(T7.DCF_FLAG, '') <> ''
          THEN T7.DCF_FLAG
          WHEN COALESCE(T8.DCF_FLAG, '') <> ''
          THEN T8.DCF_FLAG
          WHEN COALESCE(T9.DCF_FLAG, '') <> ''
          THEN T9.DCF_FLAG
          WHEN COALESCE(T10.DCF_FLAG, '') <> ''
          THEN T10.DCF_FLAG
          WHEN COALESCE(T11.DCF_FLAG, '') <> ''
          THEN T11.DCF_FLAG
          ELSE COALESCE(T1.DCF_FLAG, '')
        END
      ) AS DCF_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T1.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T2.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T2.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T3.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T3.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T4.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T4.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T5.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T5.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T6.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T6.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T7.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T7.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T8.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T8.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T9.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T9.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T10.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T10.MULTI_TRADING_ACCOUNT_FLAG
          WHEN COALESCE(T11.MULTI_TRADING_ACCOUNT_FLAG, '') <> ''
          THEN T11.MULTI_TRADING_ACCOUNT_FLAG
          ELSE COALESCE(T1.MULTI_TRADING_ACCOUNT_FLAG, '')
        END
      ) AS MULTI_TRADING_ACCOUNT_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.FATCA_FLAG, '') <> ''
          THEN T1.FATCA_FLAG
          WHEN COALESCE(T2.FATCA_FLAG, '') <> ''
          THEN T2.FATCA_FLAG
          WHEN COALESCE(T3.FATCA_FLAG, '') <> ''
          THEN T3.FATCA_FLAG
          WHEN COALESCE(T4.FATCA_FLAG, '') <> ''
          THEN T4.FATCA_FLAG
          WHEN COALESCE(T5.FATCA_FLAG, '') <> ''
          THEN T5.FATCA_FLAG
          WHEN COALESCE(T6.FATCA_FLAG, '') <> ''
          THEN T6.FATCA_FLAG
          WHEN COALESCE(T7.FATCA_FLAG, '') <> ''
          THEN T7.FATCA_FLAG
          WHEN COALESCE(T8.FATCA_FLAG, '') <> ''
          THEN T8.FATCA_FLAG
          WHEN COALESCE(T9.FATCA_FLAG, '') <> ''
          THEN T9.FATCA_FLAG
          WHEN COALESCE(T10.FATCA_FLAG, '') <> ''
          THEN T10.FATCA_FLAG
          WHEN COALESCE(T11.FATCA_FLAG, '') <> ''
          THEN T11.FATCA_FLAG
          ELSE COALESCE(T1.FATCA_FLAG, '')
        END
      ) AS FATCA_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.CRS_FLAG, '') <> ''
          THEN T1.CRS_FLAG
          WHEN COALESCE(T2.CRS_FLAG, '') <> ''
          THEN T2.CRS_FLAG
          WHEN COALESCE(T3.CRS_FLAG, '') <> ''
          THEN T3.CRS_FLAG
          WHEN COALESCE(T4.CRS_FLAG, '') <> ''
          THEN T4.CRS_FLAG
          WHEN COALESCE(T5.CRS_FLAG, '') <> ''
          THEN T5.CRS_FLAG
          WHEN COALESCE(T6.CRS_FLAG, '') <> ''
          THEN T6.CRS_FLAG
          WHEN COALESCE(T7.CRS_FLAG, '') <> ''
          THEN T7.CRS_FLAG
          WHEN COALESCE(T8.CRS_FLAG, '') <> ''
          THEN T8.CRS_FLAG
          WHEN COALESCE(T9.CRS_FLAG, '') <> ''
          THEN T9.CRS_FLAG
          WHEN COALESCE(T10.CRS_FLAG, '') <> ''
          THEN T10.CRS_FLAG
          WHEN COALESCE(T11.CRS_FLAG, '') <> ''
          THEN T11.CRS_FLAG
          ELSE COALESCE(T1.CRS_FLAG, '')
        END
      ) AS CRS_FLAG, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T1.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T2.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T2.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T3.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T3.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T4.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T4.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T5.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T5.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T6.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T6.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T7.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T7.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T8.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T8.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T9.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T9.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T10.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T10.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          WHEN COALESCE(T11.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '') <> ''
          THEN T11.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION
          ELSE COALESCE(T1.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, '')
        END
      ) AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T1.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T2.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T2.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T3.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T3.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T4.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T4.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T5.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T5.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T6.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T6.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T7.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T7.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T8.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T8.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T9.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T9.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T10.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T10.CUSTOMER_RESIDENCY_STATUS
          WHEN COALESCE(T11.CUSTOMER_RESIDENCY_STATUS, '') <> ''
          THEN T11.CUSTOMER_RESIDENCY_STATUS
          ELSE COALESCE(T1.CUSTOMER_RESIDENCY_STATUS, '')
        END
      ) AS CUSTOMER_RESIDENCY_STATUS, /* None */
      (
        CASE
          WHEN COALESCE(T1.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T1.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T2.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T2.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T3.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T3.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T4.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T4.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T5.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T5.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T6.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T6.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T7.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T7.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T8.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T8.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T9.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T9.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T10.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T10.AUTO_EINVOICE_INDICATOR
          WHEN COALESCE(T11.AUTO_EINVOICE_INDICATOR, '') <> ''
          THEN T11.AUTO_EINVOICE_INDICATOR
          ELSE COALESCE(T1.AUTO_EINVOICE_INDICATOR, '')
        END
      ) AS AUTO_EINVOICE_INDICATOR, /* None */
      (
        CASE
          WHEN COALESCE(T1.SST_REGISTRATION_NO, '') <> ''
          THEN T1.SST_REGISTRATION_NO
          WHEN COALESCE(T2.SST_REGISTRATION_NO, '') <> ''
          THEN T2.SST_REGISTRATION_NO
          WHEN COALESCE(T3.SST_REGISTRATION_NO, '') <> ''
          THEN T3.SST_REGISTRATION_NO
          WHEN COALESCE(T4.SST_REGISTRATION_NO, '') <> ''
          THEN T4.SST_REGISTRATION_NO
          WHEN COALESCE(T5.SST_REGISTRATION_NO, '') <> ''
          THEN T5.SST_REGISTRATION_NO
          WHEN COALESCE(T6.SST_REGISTRATION_NO, '') <> ''
          THEN T6.SST_REGISTRATION_NO
          WHEN COALESCE(T7.SST_REGISTRATION_NO, '') <> ''
          THEN T7.SST_REGISTRATION_NO
          WHEN COALESCE(T8.SST_REGISTRATION_NO, '') <> ''
          THEN T8.SST_REGISTRATION_NO
          WHEN COALESCE(T9.SST_REGISTRATION_NO, '') <> ''
          THEN T9.SST_REGISTRATION_NO
          WHEN COALESCE(T10.SST_REGISTRATION_NO, '') <> ''
          THEN T10.SST_REGISTRATION_NO
          WHEN COALESCE(T11.SST_REGISTRATION_NO, '') <> ''
          THEN T11.SST_REGISTRATION_NO
          ELSE COALESCE(T1.SST_REGISTRATION_NO, '')
        END
      ) AS SST_REGISTRATION_NO, /* None */
      (
        CASE
          WHEN COALESCE(T1.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T1.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T2.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T2.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T3.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T3.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T4.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T4.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T5.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T5.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T6.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T6.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T7.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T7.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T8.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T8.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T9.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T9.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T10.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T10.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          WHEN COALESCE(T11.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '') <> ''
          THEN T11.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
          ELSE COALESCE(T1.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, '')
        END
      ) AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, /* None */
      (
        CASE
          WHEN COALESCE(T1.VULNERABLE_FLAG, '') <> ''
          THEN T1.VULNERABLE_FLAG
          WHEN COALESCE(T2.VULNERABLE_FLAG, '') <> ''
          THEN T2.VULNERABLE_FLAG
          WHEN COALESCE(T3.VULNERABLE_FLAG, '') <> ''
          THEN T3.VULNERABLE_FLAG
          WHEN COALESCE(T4.VULNERABLE_FLAG, '') <> ''
          THEN T4.VULNERABLE_FLAG
          WHEN COALESCE(T5.VULNERABLE_FLAG, '') <> ''
          THEN T5.VULNERABLE_FLAG
          WHEN COALESCE(T6.VULNERABLE_FLAG, '') <> ''
          THEN T6.VULNERABLE_FLAG
          WHEN COALESCE(T7.VULNERABLE_FLAG, '') <> ''
          THEN T7.VULNERABLE_FLAG
          WHEN COALESCE(T8.VULNERABLE_FLAG, '') <> ''
          THEN T8.VULNERABLE_FLAG
          WHEN COALESCE(T9.VULNERABLE_FLAG, '') <> ''
          THEN T9.VULNERABLE_FLAG
          WHEN COALESCE(T10.VULNERABLE_FLAG, '') <> ''
          THEN T10.VULNERABLE_FLAG
          WHEN COALESCE(T11.VULNERABLE_FLAG, '') <> ''
          THEN T11.VULNERABLE_FLAG
          ELSE COALESCE(T1.VULNERABLE_FLAG, '')
        END
      ) AS VULNERABLE_FLAG, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
    FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T1 /* MHBOS */
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T2 /* M21 ALGO DB */
      ON T1.CUSTOMER_ID = T2.CUSTOMER_ID AND T2.PRIORITY_LEVEL = 2
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T3 /* M21 ALGO File */
      ON T1.CUSTOMER_ID = T3.CUSTOMER_ID AND T3.PRIORITY_LEVEL = 3
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T4 /* M21 OVEX File */
      ON T1.CUSTOMER_ID = T4.CUSTOMER_ID AND T4.PRIORITY_LEVEL = 4
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T5 /* Guava company */
      ON T1.CUSTOMER_ID = T5.CUSTOMER_ID AND T5.PRIORITY_LEVEL = 5
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T6 /* Guava customer */
      ON T1.CUSTOMER_ID = T6.CUSTOMER_ID AND T6.PRIORITY_LEVEL = 6
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T7 /* Toms */
      ON T1.CUSTOMER_ID = T7.CUSTOMER_ID AND T7.PRIORITY_LEVEL = 7
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T8 /* KDI */
      ON T1.CUSTOMER_ID = T8.CUSTOMER_ID AND T8.PRIORITY_LEVEL = 8
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T9 /* SBL */
      ON T1.CUSTOMER_ID = T9.CUSTOMER_ID AND T9.PRIORITY_LEVEL = 9
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T10 /* LMS_COUNTERPARTY */
      ON T1.CUSTOMER_ID = T10.CUSTOMER_ID AND T10.PRIORITY_LEVEL = 10
    LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T11 /* LMS_CLIENTDATA */
      ON T1.CUSTOMER_ID = T11.CUSTOMER_ID AND T11.PRIORITY_LEVEL = 11
    /*   LEFT JOIN {params["cur_schema"]}.TEMP_DIM_CUSTOMER_MERGE AS T12 
        ON T1.CUSTOMER_ID = T12.CUSTOMER_ID 
       AND T12.PRIORITY_LEVEL = 12 */
    WHERE
      T1.PRIORITY_LEVEL = 1
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
# This table will hold the final, merged dataset.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_customer_lmskibb2_updated""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_customer_lmskibb2_updated (
        customer_id VARCHAR(20)
        , customer_type VARCHAR(10)
        , customer_title VARCHAR(20)
        , customer_name VARCHAR(250)
        , customer_primary_identification_no_type VARCHAR(20)
        , customer_primary_identification_no VARCHAR(20)
        , customer_primary_identification_no_expiry_date DATE
        , customer_secondary_identification_no_type VARCHAR(20)
        , customer_secondary_identification_no VARCHAR(20)
        , customer_secondary_identification_no_expiry_date DATE
        , customer_nationality VARCHAR(2)
        , customer_country_of_residence VARCHAR(2)
        , customer_country_of_birth VARCHAR(2)
        , customer_date_of_birth DATE
        , customer_bumiputra_status VARCHAR(1)
        , customer_race VARCHAR(20)
        , customer_gender VARCHAR(10)
        , customer_marital_status VARCHAR(20)
        , customer_company_contact_person VARCHAR(100)
        , customer_company_ownership VARCHAR(5)
        , customer_company_country_of_registration VARCHAR(2)
        , customer_company_type_of_business VARCHAR(200)
        , customer_company_date_of_incorporation DATE
        , customer_company_website VARCHAR(100)
        , customer_company_type_of_organization VARCHAR(100)
        , pdpa_flag VARCHAR(1)
        , connected_party_flag VARCHAR(1)
        , politically_exposed_person_flag VARCHAR(3)
        , cross_selling_consent_flag VARCHAR(4)
        , dcf_flag VARCHAR(5)
        , multi_trading_account_flag VARCHAR(6)
        , fatca_flag VARCHAR(7)
        , crs_flag VARCHAR(8)
        , customer_company_personnel_designation VARCHAR(150)
        , customer_residency_status VARCHAR(1)
        , auto_einvoice_indicator VARCHAR(1)
        , sst_registration_no VARCHAR(20)
        , customer_company_country_of_business VARCHAR(2)
        , vulnerable_flag VARCHAR(1)
        , dl_record_status       VARCHAR(10)
        , hash_value             STRING
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from CUR ──────────────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_customer_lmskibb2_updated
    SELECT
        cur.customer_id,
        cur.customer_type,
        cur.customer_title,
        cur.customer_name,
        cur.customer_primary_identification_no_type,
        cur.customer_primary_identification_no,
        cur.customer_primary_identification_no_expiry_date,
        cur.customer_secondary_identification_no_type,
        cur.customer_secondary_identification_no,
        cur.customer_secondary_identification_no_expiry_date,
        cur.customer_nationality,
        cur.customer_country_of_residence,
        cur.customer_country_of_birth,
        cur.customer_date_of_birth,
        cur.customer_bumiputra_status,
        cur.customer_race,
        cur.customer_gender,
        cur.customer_marital_status,
        cur.customer_company_contact_person,
        cur.customer_company_ownership,
        cur.customer_company_country_of_registration,
        cur.customer_company_type_of_business,
        cur.customer_company_date_of_incorporation,
        cur.customer_company_website,
        cur.customer_company_type_of_organization,
        cur.pdpa_flag,
        cur.connected_party_flag,
        cur.politically_exposed_person_flag,
        cur.cross_selling_consent_flag,
        cur.dcf_flag,
        cur.multi_trading_account_flag,
        cur.fatca_flag,
        cur.crs_flag,
        cur.customer_company_personnel_designation,
        cur.customer_residency_status,
        cur.auto_einvoice_indicator,
        cur.sst_registration_no,
        cur.customer_company_country_of_business,
        cur.vulnerable_flag,
        cur.dl_record_status,
        cur.hash_value,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_customer_lmskibb2 cur
    WHERE cur.source_key = 'LMSKIBB2' AND         NOT EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_customer_lmskibb2_delta delta
            WHERE delta.CUSTOMER_ID = cur.CUSTOMER_ID        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_customer_lmskibb2_updated
    SELECT
        delta.customer_id,
        delta.customer_type,
        delta.customer_title,
        delta.customer_name,
        delta.customer_primary_identification_no_type,
        delta.customer_primary_identification_no,
        delta.customer_primary_identification_no_expiry_date,
        delta.customer_secondary_identification_no_type,
        delta.customer_secondary_identification_no,
        delta.customer_secondary_identification_no_expiry_date,
        delta.customer_nationality,
        delta.customer_country_of_residence,
        delta.customer_country_of_birth,
        delta.customer_date_of_birth,
        delta.customer_bumiputra_status,
        delta.customer_race,
        delta.customer_gender,
        delta.customer_marital_status,
        delta.customer_company_contact_person,
        delta.customer_company_ownership,
        delta.customer_company_country_of_registration,
        delta.customer_company_type_of_business,
        delta.customer_company_date_of_incorporation,
        delta.customer_company_website,
        delta.customer_company_type_of_organization,
        delta.pdpa_flag,
        delta.connected_party_flag,
        delta.politically_exposed_person_flag,
        delta.cross_selling_consent_flag,
        delta.dcf_flag,
        delta.multi_trading_account_flag,
        delta.fatca_flag,
        delta.crs_flag,
        delta.customer_company_personnel_designation,
        delta.customer_residency_status,
        delta.auto_einvoice_indicator,
        delta.sst_registration_no,
        delta.customer_company_country_of_business,
        delta.vulnerable_flag,
        delta.dl_record_status,
        delta.hash_value,
        CASE
            WHEN cur.CUSTOMER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE delta.dl_record_created_date
        END AS dl_record_created_date,
        CASE
            WHEN cur.hash_value = delta.hash_value THEN cur.dl_record_updated_date
            ELSE delta.dl_record_updated_date
        END AS dl_record_updated_date
    FROM {params["tmp_schema"]}.temp_dim_customer_lmskibb2_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_customer_lmskibb2 cur
        ON cur.source_key = 'LMSKIBB2' AND delta.CUSTOMER_ID = cur.CUSTOMER_ID""")

# ─── STEP 3: Insert outdated CUR records into History table ──────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_customer_lmskibb2_h PARTITION (source_key, hist_year)
    SELECT
        cur.customer_id,
        cur.customer_type,
        cur.customer_title,
        cur.customer_name,
        cur.customer_primary_identification_no_type,
        cur.customer_primary_identification_no,
        cur.customer_primary_identification_no_expiry_date,
        cur.customer_secondary_identification_no_type,
        cur.customer_secondary_identification_no,
        cur.customer_secondary_identification_no_expiry_date,
        cur.customer_nationality,
        cur.customer_country_of_residence,
        cur.customer_country_of_birth,
        cur.customer_date_of_birth,
        cur.customer_bumiputra_status,
        cur.customer_race,
        cur.customer_gender,
        cur.customer_marital_status,
        cur.customer_company_contact_person,
        cur.customer_company_ownership,
        cur.customer_company_country_of_registration,
        cur.customer_company_type_of_business,
        cur.customer_company_date_of_incorporation,
        cur.customer_company_website,
        cur.customer_company_type_of_organization,
        cur.pdpa_flag,
        cur.connected_party_flag,
        cur.politically_exposed_person_flag,
        cur.cross_selling_consent_flag,
        cur.dcf_flag,
        cur.multi_trading_account_flag,
        cur.fatca_flag,
        cur.crs_flag,
        cur.customer_company_personnel_designation,
        cur.customer_residency_status,
        cur.auto_einvoice_indicator,
        cur.sst_registration_no,
        cur.customer_company_country_of_business,
        cur.vulnerable_flag,
        cur.dl_record_status,
        cur.hash_value,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'LMSKIBB2' AS source_key        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.dim_customer_lmskibb2 cur
    INNER JOIN {params["tmp_schema"]}.temp_dim_customer_lmskibb2_delta delta
        ON delta.CUSTOMER_ID = cur.CUSTOMER_ID    WHERE cur.source_key = 'LMSKIBB2' AND         cur.hash_value <> delta.hash_value
""")

# ─── STEP 4: Overwrite CUR table ──────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_customer_lmskibb2 PARTITION (source_key = 'LMSKIBB2')    SELECT
        customer_id,
        customer_type,
        customer_title,
        customer_name,
        customer_primary_identification_no_type,
        customer_primary_identification_no,
        customer_primary_identification_no_expiry_date,
        customer_secondary_identification_no_type,
        customer_secondary_identification_no,
        customer_secondary_identification_no_expiry_date,
        customer_nationality,
        customer_country_of_residence,
        customer_country_of_birth,
        customer_date_of_birth,
        customer_bumiputra_status,
        customer_race,
        customer_gender,
        customer_marital_status,
        customer_company_contact_person,
        customer_company_ownership,
        customer_company_country_of_registration,
        customer_company_type_of_business,
        customer_company_date_of_incorporation,
        customer_company_website,
        customer_company_type_of_organization,
        pdpa_flag,
        connected_party_flag,
        politically_exposed_person_flag,
        cross_selling_consent_flag,
        dcf_flag,
        multi_trading_account_flag,
        fatca_flag,
        crs_flag,
        customer_company_personnel_designation,
        customer_residency_status,
        auto_einvoice_indicator,
        sst_registration_no,
        customer_company_country_of_business,
        vulnerable_flag,
        dl_record_status,
        hash_value,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["tmp_schema"]}.temp_dim_customer_lmskibb2_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_customer_lmskibb2 PARTITION (source_key = 'LMSKIBB2') COMPUTE STATISTICS
""")

spark.stop()