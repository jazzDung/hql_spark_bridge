import sys

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl_cur, set_parameter

source_name = "cur"
table_name = "dim_customer"
hive_table_name = source_name + "_" + table_name

# spark session
spark, today_date = run_etl_cur(source_name, table_name)
# batch_date = today_date
batch_date = "20260513"

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

# print(batch_date)c
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_customer_mhbos""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_customer_mhbos_delta""")
spark.sql(f"""DROP TABLE IF EXISTS {params["cur_schema"]}.temp_dim_customer_mhbos_updated""")

# ─── SOURCE PROCESSING MHBOS (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
    create table if not exists {params["cur_schema"]}.temp_dim_customer_mhbos(
        CUSTOMER_ID STRING,
        CUSTOMER_TYPE STRING,
        CUSTOMER_TITLE STRING,
        CUSTOMER_NAME STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_NATIONALITY STRING,
        CUSTOMER_COUNTRY_OF_RESIDENCE STRING,
        CUSTOMER_COUNTRY_OF_BIRTH STRING,
        CUSTOMER_DATE_OF_BIRTH DATE,
        CUSTOMER_BUMIPUTRA_STATUS STRING,
        CUSTOMER_RACE STRING,
        CUSTOMER_GENDER STRING,
        CUSTOMER_MARITAL_STATUS STRING,
        CUSTOMER_COMPANY_CONTACT_PERSON STRING,
        CUSTOMER_COMPANY_OWNERSHIP STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION STRING,
        CUSTOMER_COMPANY_TYPE_OF_BUSINESS STRING,
        CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE,
        CUSTOMER_COMPANY_WEBSITE STRING,
        CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION STRING,
        PDPA_FLAG STRING,
        CONNECTED_PARTY_FLAG STRING,
        POLITICALLY_EXPOSED_PERSON_FLAG STRING,
        CROSS_SELLING_CONSENT_FLAG STRING,
        DCF_FLAG STRING,
        MULTI_TRADING_ACCOUNT_FLAG STRING,
        FATCA_FLAG STRING,
        CRS_FLAG STRING,
        CUSTOMER_COMPANY_PERSONNEL_DESIGNATION STRING,
        CUSTOMER_RESIDENCY_STATUS STRING,
        UPDATE_DATE TIMESTAMP,
        AUTO_EINVOICE_INDICATOR STRING,
        SST_REGISTRATION_NO STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS STRING,
        VULNERABLE_FLAG STRING
        ,dl_record_status string
        ,dl_record_created_date timestamp
        ,dl_record_updated_date timestamp
        ,dl_last_updated_source_name varchar(50)
    )
STORED AS PARQUET
""")

spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.temp_dim_customer_mhbos
    SELECT T1.CUST_ID AS CUSTOMER_ID -- None
           ,(CASE WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('1', '2', '3', '5') THEN 'INDIVIDUAL'
                  WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6') THEN 'CORPORATE'
                  ELSE NULL
              END) AS CUSTOMER_TYPE -- None
           ,T1.TITLE AS CUSTOMER_TITLE -- None
           ,(CASE WHEN T1.NOMS_IND = 'Y' THEN T1.PRINCIPAL_NAME ELSE T1.CUSTOMER_NAME END) AS CUSTOMER_NAME -- None
           ,T1.PRIMARY_IDENTIFICATION_TYPE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE -- None
           ,(CASE WHEN TRIM(NVL(T1.PRIMARY_IDENTIFICATION_NO, '')) = '' THEN T4.BRN ELSE T1.PRIMARY_IDENTIFICATION_NO END) AS CUSTOMER_PRIMARY_IDENTIFICATION_NO -- None
           ,T1.PRIMARY_ID_EXPIRY_DATE AS CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE -- None
           ,T1.SECONDARY_IDENTIFICATION_TYPE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE -- None
           ,T1.SECONDARY_IDENTIFICATION_NO AS CUSTOMER_SECONDARY_IDENTIFICATION_NO -- None
           ,T1.SECONDARY_ID_EXPIRY_DATE AS CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE -- None
           ,T2.NATIONALITY AS CUSTOMER_NATIONALITY -- None
           ,T1.RESI_CODE AS CUSTOMER_COUNTRY_OF_RESIDENCE -- None
           ,NULL AS CUSTOMER_COUNTRY_OF_BIRTH -- None
           ,T2.DATE_OF_BIRTH AS CUSTOMER_DATE_OF_BIRTH -- None
           ,(CASE WHEN T2.BUMI_STATUS = '1' THEN 'Y' WHEN T2.BUMI_STATUS = '2' THEN 'N' ELSE NULL END) AS CUSTOMER_BUMIPUTRA_STATUS -- None
           ,T1.RACE AS CUSTOMER_RACE -- None
           ,T1.SEX AS CUSTOMER_GENDER -- None
           ,T2.MARITAL_STATUS AS CUSTOMER_MARITAL_STATUS -- None
           ,T1.CONTACT_PERSON AS CUSTOMER_COMPANY_CONTACT_PERSON -- None
           ,T4.COMPANY_OWNERSHIP AS CUSTOMER_COMPANY_OWNERSHIP -- None
           ,CASE WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6') THEN T2.NATIONALITY 
              ELSE NULL
              END AS CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION -- 20251028
           ,NULL AS CUSTOMER_COMPANY_TYPE_OF_BUSINESS -- None
           -- ,T4.COMPANY_DATE_OF_INCORPORATION AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION -- None
           ,CASE 
                 WHEN T1.PRIMARY_IDENTIFICATION_TYPE IN ('4', '6') 
                 THEN T4.COMPANY_DATE_OF_INCORPORATION
                 ELSE NULL
               END AS CUSTOMER_COMPANY_DATE_OF_INCORPORATION
           ,T4.COMPANY_WEBSITE AS CUSTOMER_COMPANY_WEBSITE -- None
           ,T4.TYPE_OF_ORGANIZATION AS CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION -- None
           ,T5.DECLARATION_FLAG AS PDPA_FLAG -- None
           ,T6.DECLARATION_FLAG AS CONNECTED_PARTY_FLAG -- None
           ,T7.DECLARATION_FLAG AS POLITICALLY_EXPOSED_PERSON_FLAG -- None
           ,T2.DISCLOSURE_OF_INFO AS CROSS_SELLING_CONSENT_FLAG -- None
           ,T8.DECLARATION_FLAG AS DCF_FLAG -- None
           ,T2.MTA_IND AS MULTI_TRADING_ACCOUNT_FLAG -- None
           ,(CASE WHEN T9.CUSTOMER_ID IS NOT NULL THEN 'Y' ELSE 'N' END) AS FATCA_FLAG -- None
           ,(CASE WHEN T3.CLIENT_NO IS NOT NULL THEN 'Y' ELSE 'N' END) AS CRS_FLAG -- None*/
           ,T4.PERSONNEL_DESIGNATION AS CUSTOMER_COMPANY_PERSONNEL_DESIGNATION -- None
           ,T2.RESIDENCY_STATUS AS CUSTOMER_RESIDENCY_STATUS -- None
           ,GREATEST(
              COALESCE(T1.DATE_CREATED, TIMESTAMP '1900-01-01 00:00:00'),
              COALESCE(T1.DATE_CHANGE,  TIMESTAMP '1900-01-01 00:00:00')
            ) AS UPDATE_DATE-- None
             ,T2.EINVOICE AS AUTO_EINVOICE_INDICATOR -- NONE
             ,T2.SST_NO AS SST_REGISTRATION_NO  -- NONE
                 ,NULL AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS
           /*,T12.COUNTRY_CODE_CCRIS AS CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS*/
           ,T11.DECLARATION_FLAG AS VULNERABLE_FLAG -- 20250715
        , T1.dl_record_status 
        , T1.dl_record_created_date AS dl_record_created_date 
        , T1.dl_record_updated_date AS dl_record_updated_date 
        , "MHBOS" as dl_last_updated_source_name
      FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 --None

    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY CLIENT_NO 
                       ORDER BY dl_record_updated_date DESC
                   ) AS rn
            FROM {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT
        ) t
        WHERE rn = 1
    ) T2
    ON T1.CLIENT_NO = T2.CLIENT_NO


      LEFT JOIN (SELECT DISTINCT CLIENT_NO FROM {params["com_schema"]}.T_MHBOS_M_CLIENT_CRS) AS T3 --None*/
        ON T1.CLIENT_NO = T3.CLIENT_NO
      LEFT JOIN (SELECT A.ACCOUNTNO AS ACCOUNTNO
                        ,D.ALIASVALUE AS BRN
                        ,E.DOB AS COMPANY_DATE_OF_INCORPORATION
                        ,F.FIELDVALUE AS COMPANY_WEBSITE
                        ,G.FIELDVALUE AS COMPANY_OWNERSHIP
                        ,H.FIELDVALUE AS TYPE_OF_BUSINESS
                        ,I.FIELDVALUE AS TYPE_OF_ORGANIZATION
                        ,J.FIELDVALUE AS PERSONNEL_DESIGNATION
                   FROM {params["com_schema"]}.T_K2_ACCOUNT A
                   LEFT JOIN {params["com_schema"]}.T_K2_CIF_ACCOUNT B
                     ON A.ACCOUNTID = B.ACCOUNTID
                    AND B.RECSTATUS = 'AA'
                    AND B.ISPRIMARY = '1'
                   LEFT JOIN (SELECT DISTINCT CIFID, ALIASVALUE
                                FROM {params["com_schema"]}.T_K2_CIF_ALIAS 
                               WHERE ALIASTYPE = 'BUSREGNO'
                                 AND LENGTH(ALIASVALUE) = 12 ) D
                                 /*AND ETL_DT > '{batch_date}') D*/
                     ON B.CIFID = D.CIFID
                   LEFT JOIN {params["com_schema"]}.T_K2_CIF_EXT E
                     ON B.CIFID = E.CIFID
                    AND E.RECSTATUS = 'AA'
                   LEFT JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
                                     RV.*
                                FROM {params["com_schema"]}.T_K2_RULE_VALUE RV
                               WHERE REFERENCETYPE = 'ACCOUNT'
                                 AND RULEGROUPCODE = 'MHBOS'
                                 AND FIELDID = 'COMPWEB'
                                 AND RECSTATUS = 'AA') F
                     ON B.ACCOUNTID = F.REFERENCEID
                    AND F.RN = 1
                   LEFT JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
                                     RV.*
                                FROM {params["com_schema"]}.T_K2_RULE_VALUE RV
                               WHERE REFERENCETYPE = 'CIF'
                                 AND RULEGROUPCODE = 'INSTITUTION'
                                 AND FIELDID = 'OWNERSHIP'
                                 AND RECSTATUS = 'AA') G
                     ON B.CIFID = G.REFERENCEID
                    AND G.RN = 1
                   LEFT JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
                                     RV.*
                                FROM {params["com_schema"]}.T_K2_RULE_VALUE RV
                               WHERE REFERENCETYPE = 'CIF'
                                 AND RULEGROUPCODE = 'EMPLOYMENT'
                                 AND FIELDID = 'BUSINESSTYPE'
                                 AND RECSTATUS = 'AA') H
                     ON B.CIFID = H.REFERENCEID
                    AND H.RN = 1
                   LEFT JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
                                     RV.*
                                FROM {params["com_schema"]}.T_K2_RULE_VALUE RV
                               WHERE REFERENCETYPE = 'CIF'
                                 AND RULEGROUPCODE = 'CIF-INSTITUTION'
                                 AND FIELDID = 'ORIGANIZATIONTYPE'
                                 AND RECSTATUS = 'AA') I
                     ON B.CIFID = I.REFERENCEID
                    AND I.RN = 1
                    LEFT JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
                                     RV.*
                                FROM {params["com_schema"]}.T_K2_RULE_VALUE RV
                               WHERE REFERENCETYPE = 'CIF'
                                 AND RULEGROUPCODE = 'EMPLOYMENT'
                                 AND FIELDID = 'OCCUPATION'
                                 AND RECSTATUS = 'AA') J
                     ON B.CIFID = J.REFERENCEID
                    AND J.RN = 1
                  WHERE A.RECSTATUS = 'AA') as T4
                 ON T1.CLIENT_NO = T4.ACCOUNTNO

        LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, DECLARATION_TYPE ORDER BY dl_record_updated_date DESC) rn
            FROM {params["cur_schema"]}.DIM_DECLARATION
            WHERE SOURCE_KEY = 'MHBOS'
        ) t
        WHERE rn = 1
    ) T5
    ON T1.CUST_ID = T5.CUSTOMER_ID
    AND T5.DECLARATION_TYPE = 'PDPA'
      LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, DECLARATION_TYPE ORDER BY dl_record_updated_date DESC) rn
            FROM {params["cur_schema"]}.DIM_DECLARATION
            WHERE SOURCE_KEY = 'FRA'
        ) t
        WHERE rn = 1
    ) T6
    ON T1.CUST_ID = T6.CUSTOMER_ID
    AND T6.DECLARATION_TYPE = 'CONNECTED_PARTY'


    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, DECLARATION_TYPE ORDER BY dl_record_updated_date DESC) rn
            FROM {params["cur_schema"]}.DIM_DECLARATION
            WHERE SOURCE_KEY = 'MHBOS'
        ) t
        WHERE rn = 1
    ) T7
    ON T1.CUST_ID = T7.CUSTOMER_ID
    AND T7.DECLARATION_TYPE = 'POLITICALLY EXPOSED PERSON'


    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, DECLARATION_TYPE ORDER BY dl_record_updated_date DESC) rn
            FROM {params["cur_schema"]}.DIM_DECLARATION
            WHERE SOURCE_KEY = 'MHBOS'
        ) t
        WHERE rn = 1
    ) T8
    ON T1.CUST_ID = T8.CUSTOMER_ID
    AND T8.DECLARATION_TYPE = 'DCF'

    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CUSTOMER_ID) rn
            FROM {params["cur_schema"]}.DIM_FATCA
            WHERE SOURCE_KEY = 'MHBOS'
        ) t
        WHERE rn = 1
    ) T9
    ON T1.CUST_ID = T9.CUSTOMER_ID

    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, DECLARATION_TYPE ORDER BY dl_record_updated_date DESC) rn
            FROM {params["cur_schema"]}.DIM_DECLARATION
            WHERE SOURCE_KEY = 'MHBOS'
        ) t
        WHERE rn = 1
    ) T11
    ON T1.CUST_ID = T11.CUSTOMER_ID
    AND T11.DECLARATION_TYPE = 'VULNERABLE'
    WHERE T1.CUST_ID IS NOT NULL
""")

# ─── END OF SOURCE PROCESSING MHBOS ───────────────────
# ─── BELOW HERE ARE MERGING LOGIC / HISTORY LOGGING LOGIC, APPLICABLE FOR ALL SOURCES ───────────────────


# ─── 1. DELTA PROCESSING ───────────────────
spark.sql(f"""
    create table if not exists {params["cur_schema"]}.temp_dim_customer_mhbos_delta(
        CUSTOMER_ID STRING,
        CUSTOMER_TYPE STRING,
        CUSTOMER_TITLE STRING,
        CUSTOMER_NAME STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_NATIONALITY STRING,
        CUSTOMER_COUNTRY_OF_RESIDENCE STRING,
        CUSTOMER_COUNTRY_OF_BIRTH STRING,
        CUSTOMER_DATE_OF_BIRTH DATE,
        CUSTOMER_BUMIPUTRA_STATUS STRING,
        CUSTOMER_RACE STRING,
        CUSTOMER_GENDER STRING,
        CUSTOMER_MARITAL_STATUS STRING,
        CUSTOMER_COMPANY_CONTACT_PERSON STRING,
        CUSTOMER_COMPANY_OWNERSHIP STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION STRING,
        CUSTOMER_COMPANY_TYPE_OF_BUSINESS STRING,
        CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE,
        CUSTOMER_COMPANY_WEBSITE STRING,
        CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION STRING,
        PDPA_FLAG STRING,
        CONNECTED_PARTY_FLAG STRING,
        POLITICALLY_EXPOSED_PERSON_FLAG STRING,
        CROSS_SELLING_CONSENT_FLAG STRING,
        DCF_FLAG STRING,
        MULTI_TRADING_ACCOUNT_FLAG STRING,
        FATCA_FLAG STRING,
        CRS_FLAG STRING,
        CUSTOMER_COMPANY_PERSONNEL_DESIGNATION STRING,
        CUSTOMER_RESIDENCY_STATUS STRING,
        UPDATE_DATE TIMESTAMP,
        AUTO_EINVOICE_INDICATOR STRING,
        SST_REGISTRATION_NO STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS STRING,
        VULNERABLE_FLAG STRING
        ,dl_record_status string
        ,dl_record_created_date timestamp
        ,dl_record_updated_date timestamp
        ,dl_last_updated_source_name varchar(50)
    )
STORED AS PARQUET
""")

spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.temp_dim_customer_mhbos_delta
    SELECT
        CUSTOMER_ID,
        CUSTOMER_TYPE,
        CUSTOMER_TITLE,
        CUSTOMER_NAME,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE,
        CUSTOMER_NATIONALITY,
        CUSTOMER_COUNTRY_OF_RESIDENCE,
        CUSTOMER_COUNTRY_OF_BIRTH,
        CUSTOMER_DATE_OF_BIRTH,
        CUSTOMER_BUMIPUTRA_STATUS,
        CUSTOMER_RACE,
        CUSTOMER_GENDER,
        CUSTOMER_MARITAL_STATUS,
        CUSTOMER_COMPANY_CONTACT_PERSON,
        CUSTOMER_COMPANY_OWNERSHIP,
        CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION,
        CUSTOMER_COMPANY_TYPE_OF_BUSINESS,
        CUSTOMER_COMPANY_DATE_OF_INCORPORATION,
        CUSTOMER_COMPANY_WEBSITE,
        CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION,
        PDPA_FLAG,
        CONNECTED_PARTY_FLAG,
        POLITICALLY_EXPOSED_PERSON_FLAG,
        CROSS_SELLING_CONSENT_FLAG,
        DCF_FLAG,
        MULTI_TRADING_ACCOUNT_FLAG,
        FATCA_FLAG,
        CRS_FLAG,
        CUSTOMER_COMPANY_PERSONNEL_DESIGNATION,
        CUSTOMER_RESIDENCY_STATUS,
        UPDATE_DATE,
        AUTO_EINVOICE_INDICATOR,
        SST_REGISTRATION_NO,
        CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
        VULNERABLE_FLAG,
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        dl_last_updated_source_name
    FROM (
        SELECT
            *
            , ROW_NUMBER() OVER(PARTITION BY CUSTOMER_ID ORDER BY update_date DESC) AS RN
        FROM {params["cur_schema"]}.temp_dim_customer_mhbos
    ) 
    WHERE RN = 1
;
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
# This table will hold the final, merged dataset.


# Updated table create
spark.sql(f"""
    create table if not exists {params["cur_schema"]}.temp_dim_customer_mhbos_updated(
        CUSTOMER_ID STRING,
        CUSTOMER_TYPE STRING,
        CUSTOMER_TITLE STRING,
        CUSTOMER_NAME STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO STRING,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO STRING,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE DATE,
        CUSTOMER_NATIONALITY STRING,
        CUSTOMER_COUNTRY_OF_RESIDENCE STRING,
        CUSTOMER_COUNTRY_OF_BIRTH STRING,
        CUSTOMER_DATE_OF_BIRTH DATE,
        CUSTOMER_BUMIPUTRA_STATUS STRING,
        CUSTOMER_RACE STRING,
        CUSTOMER_GENDER STRING,
        CUSTOMER_MARITAL_STATUS STRING,
        CUSTOMER_COMPANY_CONTACT_PERSON STRING,
        CUSTOMER_COMPANY_OWNERSHIP STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION STRING,
        CUSTOMER_COMPANY_TYPE_OF_BUSINESS STRING,
        CUSTOMER_COMPANY_DATE_OF_INCORPORATION DATE,
        CUSTOMER_COMPANY_WEBSITE STRING,
        CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION STRING,
        PDPA_FLAG STRING,
        CONNECTED_PARTY_FLAG STRING,
        POLITICALLY_EXPOSED_PERSON_FLAG STRING,
        CROSS_SELLING_CONSENT_FLAG STRING,
        DCF_FLAG STRING,
        MULTI_TRADING_ACCOUNT_FLAG STRING,
        FATCA_FLAG STRING,
        CRS_FLAG STRING,
        CUSTOMER_COMPANY_PERSONNEL_DESIGNATION STRING,
        CUSTOMER_RESIDENCY_STATUS STRING,
        UPDATE_DATE TIMESTAMP,
        AUTO_EINVOICE_INDICATOR STRING,
        SST_REGISTRATION_NO STRING,
        CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS STRING,
        VULNERABLE_FLAG STRING
        ,dl_record_status string
        ,dl_record_created_date timestamp
        ,dl_record_updated_date timestamp
        ,dl_last_updated_source_name varchar(50)
        ,etl_dt string
        ,etl_timestamp timestamp

    )STORED AS PARQUET
""")

# Insert new incoming data from delta temp table
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.temp_dim_customer_mhbos_updated
    SELECT 
        delta.CUSTOMER_ID, 
        coalesce(delta.CUSTOMER_TYPE, cur.CUSTOMER_TYPE) as CUSTOMER_TYPE,
        coalesce(delta.CUSTOMER_TITLE, cur.CUSTOMER_TITLE) as CUSTOMER_TITLE,
        coalesce(delta.CUSTOMER_NAME, cur.CUSTOMER_NAME) as CUSTOMER_NAME,
        coalesce(delta.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE, cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE) as CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE,
        coalesce(delta.CUSTOMER_PRIMARY_IDENTIFICATION_NO, cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO) as CUSTOMER_PRIMARY_IDENTIFICATION_NO,
        coalesce(delta.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE, cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE) as CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE,
        coalesce(delta.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE, cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE) as CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE,
        coalesce(delta.CUSTOMER_SECONDARY_IDENTIFICATION_NO, cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO) as CUSTOMER_SECONDARY_IDENTIFICATION_NO,
        coalesce(delta.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE, cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE) as CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE,
        coalesce(delta.CUSTOMER_NATIONALITY, cur.CUSTOMER_NATIONALITY) as CUSTOMER_NATIONALITY,
        coalesce(delta.CUSTOMER_COUNTRY_OF_RESIDENCE, cur.CUSTOMER_COUNTRY_OF_RESIDENCE) as CUSTOMER_COUNTRY_OF_RESIDENCE,
        coalesce(delta.CUSTOMER_COUNTRY_OF_BIRTH, cur.CUSTOMER_COUNTRY_OF_BIRTH) as CUSTOMER_COUNTRY_OF_BIRTH,
        coalesce(delta.CUSTOMER_DATE_OF_BIRTH, cur.CUSTOMER_DATE_OF_BIRTH) as CUSTOMER_DATE_OF_BIRTH,
        coalesce(delta.CUSTOMER_BUMIPUTRA_STATUS, cur.CUSTOMER_BUMIPUTRA_STATUS) as CUSTOMER_BUMIPUTRA_STATUS,
        coalesce(delta.CUSTOMER_RACE, cur.CUSTOMER_RACE) as CUSTOMER_RACE,
        coalesce(delta.CUSTOMER_GENDER, cur.CUSTOMER_GENDER) as CUSTOMER_GENDER,
        coalesce(delta.CUSTOMER_MARITAL_STATUS, cur.CUSTOMER_MARITAL_STATUS) as CUSTOMER_MARITAL_STATUS,
        coalesce(delta.CUSTOMER_COMPANY_CONTACT_PERSON, cur.CUSTOMER_COMPANY_CONTACT_PERSON) as CUSTOMER_COMPANY_CONTACT_PERSON,
        coalesce(delta.CUSTOMER_COMPANY_OWNERSHIP, cur.CUSTOMER_COMPANY_OWNERSHIP) as CUSTOMER_COMPANY_OWNERSHIP,
        coalesce(delta.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION, cur.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION) as CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION,
        coalesce(delta.CUSTOMER_COMPANY_TYPE_OF_BUSINESS, cur.CUSTOMER_COMPANY_TYPE_OF_BUSINESS) as CUSTOMER_COMPANY_TYPE_OF_BUSINESS,
        coalesce(delta.CUSTOMER_COMPANY_DATE_OF_INCORPORATION, cur.CUSTOMER_COMPANY_DATE_OF_INCORPORATION) as CUSTOMER_COMPANY_DATE_OF_INCORPORATION,
        coalesce(delta.CUSTOMER_COMPANY_WEBSITE, cur.CUSTOMER_COMPANY_WEBSITE) as CUSTOMER_COMPANY_WEBSITE,
        coalesce(delta.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION, cur.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION) as CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION,
        coalesce(delta.PDPA_FLAG, cur.PDPA_FLAG) as PDPA_FLAG,
        coalesce(delta.CONNECTED_PARTY_FLAG, cur.CONNECTED_PARTY_FLAG) as CONNECTED_PARTY_FLAG,
        coalesce(delta.POLITICALLY_EXPOSED_PERSON_FLAG, cur.POLITICALLY_EXPOSED_PERSON_FLAG) as POLITICALLY_EXPOSED_PERSON_FLAG,
        coalesce(delta.CROSS_SELLING_CONSENT_FLAG, cur.CROSS_SELLING_CONSENT_FLAG) as CROSS_SELLING_CONSENT_FLAG,
        coalesce(delta.DCF_FLAG, cur.DCF_FLAG) as DCF_FLAG,
        coalesce(delta.MULTI_TRADING_ACCOUNT_FLAG, cur.MULTI_TRADING_ACCOUNT_FLAG) as MULTI_TRADING_ACCOUNT_FLAG,
        coalesce(delta.FATCA_FLAG, cur.FATCA_FLAG) as FATCA_FLAG,
        coalesce(delta.CRS_FLAG, cur.CRS_FLAG) as CRS_FLAG,
        coalesce(delta.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION, cur.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION) as CUSTOMER_COMPANY_PERSONNEL_DESIGNATION,
        coalesce(delta.CUSTOMER_RESIDENCY_STATUS, cur.CUSTOMER_RESIDENCY_STATUS) as CUSTOMER_RESIDENCY_STATUS,
        coalesce(delta.UPDATE_DATE, cur.UPDATE_DATE) as UPDATE_DATE,
        coalesce(delta.AUTO_EINVOICE_INDICATOR, cur.AUTO_EINVOICE_INDICATOR) as AUTO_EINVOICE_INDICATOR,
        coalesce(delta.SST_REGISTRATION_NO, cur.SST_REGISTRATION_NO) as SST_REGISTRATION_NO,
        coalesce(delta.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS, cur.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS) as CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
        coalesce(delta.VULNERABLE_FLAG, cur.VULNERABLE_FLAG) as VULNERABLE_FLAG,
        delta.dl_record_status 
        , IF(cur.CUSTOMER_ID IS NOT NULL, cur.dl_record_created_date, delta.dl_record_created_date) AS dl_record_created_date 
        , delta.dl_record_updated_date    
        , delta.dl_last_updated_source_name
        ,'{batch_date}'      AS etl_dt
        ,current_timestamp() AS etl_timestamp
    FROM {params["cur_schema"]}.temp_dim_customer_mhbos_delta delta 
    LEFT JOIN {params["cur_schema"]}.dim_customer cur  
        ON delta.CUSTOMER_ID  = cur.CUSTOMER_ID  
    WHERE 
        cur.CUSTOMER_ID IS NULL
        OR COALESCE(delta.update_date, TIMESTAMP '1900-01-01') > COALESCE(cur.update_date, TIMESTAMP '1900-01-01')
""")

# Insert existing, unchanged records from main table
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.temp_dim_customer_mhbos_updated
    SELECT 
         cur.CUSTOMER_ID,
         cur.CUSTOMER_TYPE,
         cur.CUSTOMER_TITLE,
         cur.CUSTOMER_NAME,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE,
         cur.CUSTOMER_NATIONALITY,
         cur.CUSTOMER_COUNTRY_OF_RESIDENCE,
         cur.CUSTOMER_COUNTRY_OF_BIRTH,
         cur.CUSTOMER_DATE_OF_BIRTH,
         cur.CUSTOMER_BUMIPUTRA_STATUS,
         cur.CUSTOMER_RACE,
         cur.CUSTOMER_GENDER,
         cur.CUSTOMER_MARITAL_STATUS,
         cur.CUSTOMER_COMPANY_CONTACT_PERSON,
         cur.CUSTOMER_COMPANY_OWNERSHIP,
         cur.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION,
         cur.CUSTOMER_COMPANY_TYPE_OF_BUSINESS,
         cur.CUSTOMER_COMPANY_DATE_OF_INCORPORATION,
         cur.CUSTOMER_COMPANY_WEBSITE,
         cur.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION,
         cur.PDPA_FLAG,
         cur.CONNECTED_PARTY_FLAG,
         cur.POLITICALLY_EXPOSED_PERSON_FLAG,
         cur.CROSS_SELLING_CONSENT_FLAG,
         cur.DCF_FLAG,
         cur.MULTI_TRADING_ACCOUNT_FLAG,
         cur.FATCA_FLAG,
         cur.CRS_FLAG,
         cur.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION,
         cur.CUSTOMER_RESIDENCY_STATUS,
         cur.UPDATE_DATE,
         cur.AUTO_EINVOICE_INDICATOR,
         cur.SST_REGISTRATION_NO,
         cur.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
         cur.VULNERABLE_FLAG
        , cur.dl_record_status as dl_record_status 
        , cur.dl_record_created_date 
        , cur.dl_record_updated_date
        , cur.dl_last_updated_source_name
        , '{batch_date}'      AS etl_dt
        , current_timestamp() AS etl_timestamp
    FROM {params["cur_schema"]}.dim_customer cur   
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {params["cur_schema"]}.temp_dim_customer_mhbos_delta delta 
        WHERE cur.CUSTOMER_ID = delta.CUSTOMER_ID
        AND COALESCE(delta.update_date, TIMESTAMP '1900-01-01') > COALESCE(cur.update_date, TIMESTAMP '1900-01-01')
    )
""")

# ─── Logging into history table ───────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_customer_h
    SELECT 
         cur.CUSTOMER_ID,
         cur.CUSTOMER_TYPE,
         cur.CUSTOMER_TITLE,
         cur.CUSTOMER_NAME,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO,
         cur.CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO,
         cur.CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE,
         cur.CUSTOMER_NATIONALITY,
         cur.CUSTOMER_COUNTRY_OF_RESIDENCE,
         cur.CUSTOMER_COUNTRY_OF_BIRTH,
         cur.CUSTOMER_DATE_OF_BIRTH,
         cur.CUSTOMER_BUMIPUTRA_STATUS,
         cur.CUSTOMER_RACE,
         cur.CUSTOMER_GENDER,
         cur.CUSTOMER_MARITAL_STATUS,
         cur.CUSTOMER_COMPANY_CONTACT_PERSON,
         cur.CUSTOMER_COMPANY_OWNERSHIP,
         cur.CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION,
         cur.CUSTOMER_COMPANY_TYPE_OF_BUSINESS,
         cur.CUSTOMER_COMPANY_DATE_OF_INCORPORATION,
         cur.CUSTOMER_COMPANY_WEBSITE,
         cur.CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION,
         cur.PDPA_FLAG,
         cur.CONNECTED_PARTY_FLAG,
         cur.POLITICALLY_EXPOSED_PERSON_FLAG,
         cur.CROSS_SELLING_CONSENT_FLAG,
         cur.DCF_FLAG,
         cur.MULTI_TRADING_ACCOUNT_FLAG,
         cur.FATCA_FLAG,
         cur.CRS_FLAG,
         cur.CUSTOMER_COMPANY_PERSONNEL_DESIGNATION,
         cur.CUSTOMER_RESIDENCY_STATUS,
         cur.UPDATE_DATE,
         cur.AUTO_EINVOICE_INDICATOR,
         cur.SST_REGISTRATION_NO,
         cur.CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
         cur.VULNERABLE_FLAG
        , cur.dl_record_status
        , cur.dl_last_updated_source_name
        , cur.etl_dt as etl_dt
        , cur.dl_record_created_date as start_timestamp
        , current_timestamp() as end_timestamp
        , date_format(current_timestamp(), 'yyyyMM') as log_period
    FROM {params["cur_schema"]}.dim_customer cur   
    WHERE EXISTS (
        SELECT 1 
        FROM {params["cur_schema"]}.temp_dim_customer_mhbos_delta delta 
        WHERE cur.CUSTOMER_ID = delta.CUSTOMER_ID
        AND COALESCE(delta.update_date, TIMESTAMP '1900-01-01') > COALESCE(cur.update_date, TIMESTAMP '1900-01-01')
    )
""")

# Insert into main table
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_customer
    SELECT
        CUSTOMER_ID,
        CUSTOMER_TYPE,
        CUSTOMER_TITLE,
        CUSTOMER_NAME,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_TYPE,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO,
        CUSTOMER_PRIMARY_IDENTIFICATION_NO_EXPIRY_DATE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_TYPE,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO,
        CUSTOMER_SECONDARY_IDENTIFICATION_NO_EXPIRY_DATE,
        CUSTOMER_NATIONALITY,
        CUSTOMER_COUNTRY_OF_RESIDENCE,
        CUSTOMER_COUNTRY_OF_BIRTH,
        CUSTOMER_DATE_OF_BIRTH,
        CUSTOMER_BUMIPUTRA_STATUS,
        CUSTOMER_RACE,
        CUSTOMER_GENDER,
        CUSTOMER_MARITAL_STATUS,
        CUSTOMER_COMPANY_CONTACT_PERSON,
        CUSTOMER_COMPANY_OWNERSHIP,
        CUSTOMER_COMPANY_COUNTRY_OF_REGISTRATION,
        CUSTOMER_COMPANY_TYPE_OF_BUSINESS,
        CUSTOMER_COMPANY_DATE_OF_INCORPORATION,
        CUSTOMER_COMPANY_WEBSITE,
        CUSTOMER_COMPANY_TYPE_OF_ORGANIZATION,
        PDPA_FLAG,
        CONNECTED_PARTY_FLAG,
        POLITICALLY_EXPOSED_PERSON_FLAG,
        CROSS_SELLING_CONSENT_FLAG,
        DCF_FLAG,
        MULTI_TRADING_ACCOUNT_FLAG,
        FATCA_FLAG,
        CRS_FLAG,
        CUSTOMER_COMPANY_PERSONNEL_DESIGNATION,
        CUSTOMER_RESIDENCY_STATUS,
        UPDATE_DATE,
        AUTO_EINVOICE_INDICATOR,
        SST_REGISTRATION_NO,
        CUSTOMER_COMPANY_COUNTRY_OF_BUSINESS,
        VULNERABLE_FLAG
        , dl_record_status 
        , dl_record_created_date 
        , dl_record_updated_date 
        , dl_last_updated_source_name
        , '{batch_date}' as etl_dt
        , current_timestamp() as etl_timestamp
    FROM {params["cur_schema"]}.temp_dim_customer_mhbos_updated
""")

spark.sql(f"""analyze table {params["cur_schema"]}.dim_customer COMPUTE STATISTICS""")

# Stop Spark when done
spark.stop()
