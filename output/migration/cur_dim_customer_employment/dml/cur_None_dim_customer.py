##  File Name   : com_None_dim_customer
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-18 08:07:28
##  Source      : cur_dim_customer_employment (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "dim"
table_name  = "None_dim_customer"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT
""")

spark.sql(f"""
/* Create temp table */
    CREATE TABLE {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID VARCHAR(20) COMMENT '',
      CUSTOMER_EMPLOYER_NAME VARCHAR(100) COMMENT '',
      CUSTOMER_EMPLOYER_INDUSTRY VARCHAR(100) COMMENT '',
      CUSTOMER_EMPLOYER_TYPE VARCHAR(100) COMMENT '',
      CUSTOMER_AMLA_OCCUPATION VARCHAR(50) COMMENT '',
      CUSTOMER_CCRIS_OCCUPATION VARCHAR(10) COMMENT '',
      SOURCE_NAME VARCHAR(10) COMMENT '',
      SOURCE_RECORD_ID VARCHAR(20) COMMENT '',
      ETL_TIMESTAMP STRING COMMENT 'ETL_PROCESSING_TIME',
      SOURCE_UPDATE_DATE TIMESTAMP COMMENT '',
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS VARCHAR(200) COMMENT ''
    )
""")

spark.sql(f"""
/* ==============[Group.1]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      T2.EMPLOYER_NAME AS CUSTOMER_EMPLOYER_NAME, /* None */
      T2.EMP_SECTOR AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      T2.EMP_TYPE AS CUSTOMER_EMPLOYER_TYPE, /* None */
      UPPER(T1.OCCUPATION) AS CUSTOMER_AMLA_OCCUPATION, /* None */
      T2.CCRIS_OCC_SUB AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      COALESCE(T1.DATE_CHANGE, T1.DATE_CREATED) AS SOURCE_UPDATE_DATE, /* 20251028 */
      UPPER(T2.TYPE_OF_BUSINESS) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND TO_DATE(T2.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    /* LEFT JOIN ${cur_schema}.REF_LOOKUP AS T3
        ON T3.SOURCE_KEY = 'MHBOS_M_PARA'
        AND T3.REFERENCE_TYPE = 'TYPE_OF_BUSINESS'
        AND T3.REFERENCE_CODE = T2.TYPE_OF_BUSINESS
      LEFT JOIN ${cur_schema}.REF_LOOKUP AS T4
        ON T3.SOURCE_KEY = 'MHBOS_M_PARA'
        AND T3.REFERENCE_TYPE = 'OCCUPATION'
        AND T3.REFERENCE_CODE = T1.OCCUPATION */
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T2.EMPLOYER_NAME, '')) <> ''
        OR COALESCE(TRIM(T2.EMP_SECTOR), '') <> ''
        OR COALESCE(TRIM(T2.EMP_TYPE), '') <> ''
        OR COALESCE(TRIM(T1.OCCUPATION), '') <> ''
        OR COALESCE(TRIM(T2.CCRIS_OCC_SUB), '') <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.2]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      UPPER(T1.ATTACHMENT_EMPLOYER_NAME) AS CUSTOMER_EMPLOYER_NAME, /* None */
      NULL AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      NULL AS CUSTOMER_EMPLOYER_TYPE, /* None */
      UPPER(T1.ATTACHMENT_OCCUPATION) AS CUSTOMER_AMLA_OCCUPATION, /* 20251017 */
      NULL AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'GUAVA' AS SOURCE_NAME, /* None */
      T1.ATTACHMENT_TRANSACTION_INFO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      T2.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE, /* 20251028 */
      TRIM(T3.FIELDVALUE) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_GUAVA_COMPANY AS T1 /* None */
    LEFT JOIN (
      SELECT
        ATTACHMENT_TRANSACTION_INFO,
        MAX(ETL_TIMESTAMP) AS ETL_TIMESTAMP
      FROM {params["com_schema"]}.R_GUAVA_COMPANY
      WHERE
        START_DT <= '{batch_date}' AND END_DT > '{batch_date}'
      GROUP BY
        ATTACHMENT_TRANSACTION_INFO
    ) AS T2
      ON T1.ATTACHMENT_TRANSACTION_INFO = T2.ATTACHMENT_TRANSACTION_INFO
    LEFT JOIN (
      SELECT
        A.ACCOUNTNO AS ACCOUNTNO,
        TRIM(C.FIELDVALUE) AS FIELDVALUE
      FROM {params["com_schema"]}.R_K2_ACCOUNT AS A
      LEFT JOIN {params["com_schema"]}.R_K2_CIF_ACCOUNT AS B
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
        FROM {params["com_schema"]}.R_K2_RULE_VALUE AS RV
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
    ) AS T3 /* None */
      ON T1.ATTACHMENT_TRANSACTION_INFO = T3.ACCOUNTNO
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.ATTACHMENT_EMPLOYER_NAME, '')) <> ''
        OR TRIM(COALESCE(T1.ATTACHMENT_OCCUPATION, '')) <> ''
        OR TRIM(COALESCE(T3.FIELDVALUE, '')) <> ''
      )
      AND NOT T1.CUSTOMER_NAME LIKE '@[%'
""")

spark.sql(f"""
/* ==============[Group.3]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      T1.EMPLOYER AS CUSTOMER_EMPLOYER_NAME, /* None */
      NULL AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      NULL AS CUSTOMER_EMPLOYER_TYPE, /* None */
      T1.OCCUPATION AS CUSTOMER_AMLA_OCCUPATION, /* None */
      NULL AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      T1.MODIFYDATE AS SOURCE_UPDATE_DATE,
      UPPER(T1.NATUREOFBUSINESS) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_M21_CUSTOMER AS T1 /* None */
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.EMPLOYER, '')) <> '' OR COALESCE(TRIM(T1.OCCUPATION), '') <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.4]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS CUSTOMER_EMPLOYER_NAME, /* None */
      (
        CASE
          WHEN T1.OCCUPATIONSECTOR = 0
          THEN NULL
          ELSE COALESCE(T2.REFERENCE_VALUE, T1.OCCUPATIONSECTOR)
        END
      ) AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      NULL AS CUSTOMER_EMPLOYER_TYPE, /* None */
      T5.REFERENCE_VALUE AS CUSTOMER_AMLA_OCCUPATION, /* None */
      NULL AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.IDREF AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS TIMESTAMP) AS SOURCE_UPDATE_DATE,
      T4.REFERENCE_VALUE AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_TOMS_ERETAIL_CIF AS T1 /* None */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T2 /* None */
      ON T1.OCCUPATIONSECTOR = T2.REFERENCE_CODE
      AND /* AND T2.etl_dt = '${batch_date}' */ T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T2.source_name = 'TOMS'
      AND T2.REFERENCE_TYPE = 'OCCUPATION SECTOR'
    /* LEFT JOIN ${cur_schema}.REF_LOOKUP AS T3 --None
        ON T1.OCCUPATION = T3.REFERENCE_CODE
        --AND T3.etl_dt = '${batch_date}'
        AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
        AND T3.source_name = 'TOMS'
        AND T3.REFERENCE_TYPE = 'OCCUPATION' */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4
      ON T4.REFERENCE_CODE = T1.BUSINESSTYPE
      AND T4.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.REFERENCE_TYPE = 'TOMSBIZTYPEMAP'
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T5
      ON T5.REFERENCE_CODE = T1.OCCUPATION
      AND T5.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T5.REFERENCE_TYPE = 'TOMSOCCUMAP'
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        NOT T1.OCCUPATIONSECTOR IS NULL OR NOT T1.OCCUPATION IS NULL
      )
""")

spark.sql(f"""
/* ==============[source 5 (LMS Counterparty)]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      UPPER(NAME_OF_COMPANY) AS CUSTOMER_EMPLOYER_NAME, /* NONE */
      UPPER(T1.CCRIS_EMPLOYMENT_SECTOR_CODE) AS CUSTOMER_EMPLOYER_INDUSTRY, /* NONE */
      UPPER(T1.CCRIS_EMPLOYMENT_TYPE_CODE) AS CUSTOMER_EMPLOYER_TYPE, /* NONE */
      T2.REFERENCE_VALUE AS CUSTOMER_AMLA_OCCUPATION, /* 20260212 */
      UPPER(T1.CCRIS_OCCUPATION_CODE) AS CUSTOMER_CCRIS_OCCUPATION, /* NONE */
      'LMS' AS SOURCE_NAME, /* None */
      UPPER(T1.RECORD_ID) AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(
        COALESCE(T1.LAST_ACTION_DATETIME, '1900-01-01'),
        COALESCE(T1.SYSTEM_UPDATED_DATETIME, '1900-01-01')
      ) AS TIMESTAMP) AS SOURCE_UPDATE_DATE,
      UPPER(T1.EMPLOYER_BUSINESS_CODE) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_LMSKIBB2_TBL_COUNTERPARTY AS T1
    LEFT JOIN {params["cur_schema"]}.ref_lookup AS T2
      ON T2.REFERENCE_CODE = T1.AML_OCCUPATION_ID
      AND T2.SOURCE_NAME = 'LMS'
      AND T2.REFERENCE_TYPE = 'AMLOCCUMAP'
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND NOT T1.CUST_ID IS NULL
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[source 6 (SBL Counterparty)]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      UPPER(T3.Employer_Name) AS CUSTOMER_EMPLOYER_NAME, /* NONE */
      UPPER(T4.Employment_Sector_Code) AS CUSTOMER_EMPLOYER_INDUSTRY, /* NONE */
      UPPER(T5.CCRIS_Code) AS CUSTOMER_EMPLOYER_TYPE, /* NONE */
      UPPER(T6.Occupation_Code) AS CUSTOMER_AMLA_OCCUPATION, /* NONE */
      NULL AS CUSTOMER_CCRIS_OCCUPATION, /* NONE */
      'SBL' AS SOURCE_NAME, /* None */
      UPPER(T3.RECORD_ID) AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(
        COALESCE(T3.LAST_ACTION_DATETIME, '1900-01-01'),
        COALESCE(T3.SYSTEM_UPDATED_DATETIME, '1900-01-01')
      ) AS TIMESTAMP) AS SOURCE_UPDATE_DATE,
      UPPER(T7.Type_Of_Business_Code) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM {params["com_schema"]}.M_SBL_TBL_EINVOICING_CLIENTDATA AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_account AS T2
      ON T1.account_number = T2.account_number
      AND T2.etl_dt = '{batch_date}'
      AND T2.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T2.etl_dt, 'yyyyMMdd')) BETWEEN T2.effective_from AND T2.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_counterparty AS T3
      ON T2.counterparty_id_1 = T3.counterparty_id
      AND T3.etl_dt = '{batch_date}'
      AND T3.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T3.etl_dt, 'yyyyMMdd')) BETWEEN T3.effective_from AND T3.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_employmentsector AS T4
      ON T3.Employment_Sector_Id = T4.Employment_Sector_Id
      AND T4.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T4.etl_dt, 'yyyyMMdd')) BETWEEN T4.effective_from AND T4.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_employmentstatustype AS T5
      ON T3.Employment_Status_Type_Id = T5.Employment_Status_Type_Id
      AND T5.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T5.etl_dt, 'yyyyMMdd')) BETWEEN T5.effective_from AND T5.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_occupation AS T6
      ON T3.Occupation_Id = T6.Occupation_Id
      AND T6.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T6.etl_dt, 'yyyyMMdd')) BETWEEN T6.effective_from AND T6.effective_to
    LEFT JOIN {params["com_schema"]}.t_sblkibb_tbl_typeofbusiness AS T7
      ON T3.Type_Of_Business_Id = T7.Type_Of_Business_Id
      AND T7.record_status_id = 3
      AND FROM_UNIXTIME(UNIX_TIMESTAMP(T7.etl_dt, 'yyyyMMdd')) BETWEEN T7.effective_from AND T7.effective_to
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
      AND NOT T1.cust_id IS NULL
""")

spark.sql(f"""
/* ==============[Group.7 KDI Report]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* None */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* None */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID,
      name_of_employer AS CUSTOMER_EMPLOYER_NAME, /* None */
      NULL AS CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      NULL AS CUSTOMER_EMPLOYER_TYPE, /* None */
      UPPER(COALESCE(T3.REFERENCE_CODE, T1.OCCUPATION)) AS CUSTOMER_AMLA_OCCUPATION, /* None */
      NULL AS CUSTOMER_CCRIS_OCCUPATION, /* None */
      'KDI' AS SOURCE_NAME, /* None */
      T1.client_id AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      T1.Record_Updated_Date AS SOURCE_UPDATE_DATE, /*       ,T2.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE */
      UPPER(COALESCE(T4.REFERENCE_CODE, T1.BUSINESS_TYPE_INDUSTRY)) AS CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* None */
    FROM {params["com_schema"]}.M_KDI_CLIENTREPORT AS T1 /* None */
    /* LEFT JOIN ( */ /*    SELECT */ /*        ACCOUNT_NO */ /*        , MAX(ETL_TIMESTAMP) AS ETL_TIMESTAMP */ /*    FROM ${raw_schema}.KDI_REPORT */ /*    GROUP BY ACCOUNT_NO */ /* ) AS T2 */ /* ON T1.ACCOUNT_NO = T2.ACCOUNT_NO */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3
      ON T1.OCCUPATION = T3.REFERENCE_VALUE
      AND T3.source_key = 'GENERAL_REFERENCE_LOOKUP'
      AND T3.SOURCE_NAME = 'AML'
      AND T3.REFERENCE_TYPE = 'OCCUPATION'
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4
      ON T1.BUSINESS_TYPE_INDUSTRY = T4.REFERENCE_VALUE
      AND T4.source_key = 'GENERAL_REFERENCE_LOOKUP'
      AND T4.SOURCE_NAME = 'AML'
      AND T4.REFERENCE_TYPE = 'TYPEOFBUSINESS'
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.OCCUPATION, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESS_TYPE_INDUSTRY, '')) <> ''
        OR TRIM(COALESCE(T1.NAME_OF_EMPLOYER, '')) <> ''
      )
""")

spark.sql(f"""
WITH CUSTOMER_EMPLOYMENT_ROW_NUM AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_EMPLOYMENT
    )
    INSERT INTO {params["cur_schema"]}.DIM_CUSTOMER_EMPLOYMENT PARTITION(etl_dt = '{batch_date}') (
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    )
    SELECT
      CUSTOMER_ID, /* None */
      CUSTOMER_EMPLOYER_NAME, /* None */
      CUSTOMER_EMPLOYER_INDUSTRY, /* None */
      CUSTOMER_EMPLOYER_TYPE, /* None */
      CUSTOMER_AMLA_OCCUPATION, /* None */
      CUSTOMER_CCRIS_OCCUPATION, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE, /* 20251014 */
      CUSTOMER_EMPLOYER_TYPE_OF_BUSINESS /* 20251016 */
    FROM CUSTOMER_EMPLOYMENT_ROW_NUM
    WHERE
      RN = 1
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_None_dim_customer_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_None_dim_customer_updated (
    customer_id VARCHAR(20)
    , customer_employer_name VARCHAR(100)
    , customer_employer_industry VARCHAR(100)
    , customer_employer_type VARCHAR(100)
    , customer_amla_occupation VARCHAR(50)
    , customer_ccris_occupation VARCHAR(10)
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
    , customer_employer_type_of_business VARCHAR(200)
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_None_dim_customer_updated
SELECT
    customer_id
    , customer_employer_name
    , customer_employer_industry
    , customer_employer_type
    , customer_amla_occupation
    , customer_ccris_occupation
    , source_name
    , source_record_id
    , source_update_date
    , customer_employer_type_of_business
    , 'A' AS dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
FROM {params["com_schema"]}.None_dim_customer com_t
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.dim_None_dim_customer r
    WHERE r.etl_dt = '{batch_date}'
      AND r.customer_id = com_t.customer_id
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_None_dim_customer_updated
SELECT
    r.customer_id
    , r.customer_employer_name
    , r.customer_employer_industry
    , r.customer_employer_type
    , r.customer_amla_occupation
    , r.customer_ccris_occupation
    , r.source_name
    , r.source_record_id
    , r.source_update_date
    , r.customer_employer_type_of_business
    , 'A' AS dl_record_status
    , CASE
        WHEN com_t.customer_id IS NOT NULL THEN com_t.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
FROM {params["raw_schema"]}.dim_None_dim_customer r
LEFT JOIN {params["com_schema"]}.None_dim_customer com_t
    ON r.customer_id = com_t.customer_id
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.None_dim_customer
SELECT
    customer_id
    , customer_employer_name
    , customer_employer_industry
    , customer_employer_type
    , customer_amla_occupation
    , customer_ccris_occupation
    , source_name
    , source_record_id
    , source_update_date
    , customer_employer_type_of_business
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , '{batch_date}'       AS etl_dt
    , current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_None_dim_customer_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.None_dim_customer COMPUTE STATISTICS""")

spark.stop()