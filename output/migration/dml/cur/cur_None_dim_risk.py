##  File Name   : com_None_dim_risk
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-15 08:21:55
##  Source      : cur_dim_risk_profile (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "dim"
table_name  = "None_dim_risk"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE
""")

spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID VARCHAR(20) COMMENT '',
      AMLA_RISK_PROFILE_DATE DATE COMMENT '',
      AMLATF_RISK VARCHAR(1) COMMENT '',
      ESTIMATED_NETWORTH VARCHAR(100) COMMENT '',
      ANNUAL_INCOME INT COMMENT '',
      SOURCE_NAME VARCHAR(10) COMMENT '',
      SOURCE_RECORD_ID VARCHAR(20) COMMENT '',
      ETL_TIMESTAMP STRING COMMENT 'ETL_PROCESSING_TIME',
      SOURCE_UPDATE_DATE TIMESTAMP COMMENT ''
    )
""")

spark.sql(f"""
/* ==============[Group.1]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS AMLA_RISK_PROFILE_DATE, /* None */
      T1.RISK AS AMLATF_RISK, /* None */
      T2.ESTIMATED_NETWORTH AS ESTIMATED_NETWORTH, /* None */
      CASE
        WHEN COALESCE(T1.MARGIN, '') = 'M'
        THEN T2.ANNUAL_INCOME
        WHEN T2.MONTHLY_INCOME = '1'
        THEN 12000
        WHEN T2.MONTHLY_INCOME = '2'
        THEN 30000
        WHEN T2.MONTHLY_INCOME = '3'
        THEN 60000
        WHEN T2.MONTHLY_INCOME = '4'
        THEN 90000
        WHEN T2.MONTHLY_INCOME = '5'
        THEN 120000
        WHEN T2.MONTHLY_INCOME = '6'
        THEN 240000
        WHEN T2.MONTHLY_INCOME = '7'
        THEN 252000
        ELSE NULL
      END AS ANNUAL_INCOME, /* None */
      'MHBOS' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(COALESCE(T1.DATE_CHANGE, T1.DATE_CREATED) AS TIMESTAMP) AS SOURCE_UPDATE_DATE /* 20251028 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_MHBOS_M_CLIENT_EXT AS T2 /* None */
      ON T1.CLIENT_NO = T2.CLIENT_NO
      AND TO_DATE(T2.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3
      ON T2.ESTIMATED_NETWORTH = T3.REFERENCE_CODE
      AND T3.SOURCE_NAME = 'AML'
      AND T3.REFERENCE_TYPE = 'ESTIMATEDNETWORTH'
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.RISK, '')) <> ''
        OR TRIM(COALESCE(T2.ESTIMATED_NETWORTH, '')) <> ''
        OR NOT T2.ANNUAL_INCOME IS NULL
        OR COALESCE(T2.MONTHLY_INCOME, '') <> ''
      )
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.2]============== */ /* CTE for ESTIMATE_NETWORTH */
    WITH networth_step1 AS (
      SELECT
        *,
        CASE
          WHEN T1.EXTREF LIKE '%:%'
          THEN UPPER(TRIM(REGEXP_EXTRACT(T1.EXTREF, '.*:(.*)')))
          WHEN T1.EXTREF LIKE '%;%'
          THEN UPPER(TRIM(REGEXP_EXTRACT(T1.EXTREF, '.*;(.*)')))
          WHEN UPPER(T1.EXTREF) LIKE '%ASSETS%'
          THEN UPPER(TRIM(REGEXP_EXTRACT(UPPER(T1.EXTREF), '.*ASSETS\\s*(.*)')))
          WHEN UPPER(T1.EXTREF) LIKE '%WORTH%'
          THEN UPPER(TRIM(REGEXP_EXTRACT(UPPER(T1.EXTREF), '.*WORTH\\s*(.*)')))
          ELSE NULL
        END AS trim_networth /* trim out unnecessary string for networth */
      FROM {params["com_schema"]}.M_M21_CUSTOMER AS T1 /* None */
      WHERE
        T1.ETL_DT = '{batch_date}'
        AND TRIM(COALESCE(T1.RACE, '')) <> ''
        AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
    ), networth_step2 AS (
      SELECT
        *,
        CASE
          WHEN (
            trim_networth LIKE '<%' OR trim_networth LIKE '>%'
          )
          THEN SUBSTRING(trim_networth, 1, 1)
        END AS networth_operator, /* split the networth into Operator only */
        TRIM(
          CASE
            WHEN trim_networth LIKE '<%' OR trim_networth LIKE '>%'
            THEN SUBSTRING(trim_networth, 2, LENGTH(trim_networth))
            ELSE trim_networth
          END
        ) AS networh_value /* split the networth into value only */
      FROM networth_step1
    ), networth_step3 AS (
      SELECT
        *,
        CAST(CAST(REGEXP_EXTRACT(UPPER(networh_value), '(\\d+(?:\\.\\d+)?)') AS INT) * CASE
          WHEN UPPER(networh_value) LIKE '%K'
          THEN 1000
          WHEN UPPER(networh_value) RLIKE '(?i)\\d\\s*(M|MI|MIL|MN|MILLION)\\b'
          THEN 1000000
          ELSE 1
        END AS INT) AS total_networth_value
      FROM networth_step2
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS AMLA_RISK_PROFILE_DATE, /* None */
      (
        CASE
          WHEN T1.RACE LIKE 'LOW%'
          THEN 'L'
          WHEN T1.RACE LIKE 'MEDIUM%'
          THEN 'M'
          WHEN T1.RACE LIKE 'HIGH%'
          THEN 'H'
          WHEN T1.RACE LIKE 'PEP%'
          THEN 'H'
          ELSE NULL
        END
      ) AS AMLATF_RISK, /* None */
      CASE
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 50000
        THEN 1
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 100000
        THEN 2
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 200000
        THEN 3
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 500000
        THEN 4
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 1000000
        THEN 5
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE <= 3000000
        THEN 6
        WHEN (
          NETWORTH_OPERATOR = '<' OR NETWORTH_OPERATOR IS NULL
        )
        AND TOTAL_NETWORTH_VALUE > 3000000
        THEN 7
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 50000
        THEN 1
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 100000
        THEN 2
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 200000
        THEN 3
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 500000
        THEN 4
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 1000000
        THEN 5
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE < 3000000
        THEN 6
        WHEN NETWORTH_OPERATOR = '>' AND TOTAL_NETWORTH_VALUE >= 3000000
        THEN 7
      END AS ESTIMATED_NETWORTH, /* 20260204 */
      T1.ANNUALINCOME AS ANNUAL_INCOME, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      COALESCE(T1.MODIFYDATE, '1900-01-01') AS SOURCE_UPDATE_DATE
    FROM networth_step3 AS T1
""")

spark.sql(f"""
/* ==============[Group.3]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      DATE_FORMAT(
        CAST(SUBSTRING('{batch_date}', 1, 4) || '-' || SUBSTRING('{batch_date}', 5, 2) || '-' || SUBSTRING('{batch_date}', 7, 2) AS TIMESTAMP),
        'yyyy-MM-dd'
      ) AS AMLA_RISK_PROFILE_DATE, /* None */
      T2.AMLA AS AMLATF_RISK, /* None */
      NULL AS ESTIMATED_NETWORTH, /* None */
      NULL AS ANNUAL_INCOME, /* None */
      'K2' AS SOURCE_NAME, /* None */
      T1.CLIENT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(COALESCE(T1.DATE_CHANGE, '1900-01-01') AS TIMESTAMP) AS SOURCE_UPDATE_DATE /* 20251028 */
    FROM {params["com_schema"]}.M_MHBOS_M_CLIENT AS T1 /* None */
    LEFT JOIN (
      SELECT
        A.ACCOUNTNO AS ACCOUNTNO,
        CASE WHEN C.FIELDVALUE <> '' THEN C.FIELDVALUE ELSE NULL END AS AMLA
      FROM {params["com_schema"]}.R_K2_ACCOUNT AS A
      LEFT JOIN {params["com_schema"]}.R_K2_CIF_ACCOUNT AS B
        ON A.ACCOUNTID = B.ACCOUNTID
        AND B.RECSTATUS = 'AA'
        AND B.ISPRIMARY = 1
        AND B.START_DT <= '{batch_date}'
        AND B.END_DT > '{batch_date}'
      LEFT JOIN (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY REFERENCEID, REFERENCETYPE, RULEGROUPCODE, FIELDID, RECSTATUS ORDER BY EFFECTIVEFROM DESC) AS RN,
          RV.*
        FROM {params["com_schema"]}.R_K2_RULE_VALUE AS RV
        WHERE
          TRIM(REFERENCETYPE) = 'CIF'
          AND TRIM(RULEGROUPCODE) = 'RISK'
          AND TRIM(FIELDID) = 'AMLA'
          AND RECSTATUS = 'AA'
          AND START_DT <= '{batch_date}'
          AND END_DT > '{batch_date}'
      ) AS C
        ON B.CIFID = C.REFERENCEID AND C.RN = 1
      WHERE
        A.RECSTATUS = 'AA' AND A.START_DT <= '{batch_date}' AND A.END_DT > '{batch_date}'
    ) AS T2 /* None */
      ON T1.CLIENT_NO = T2.ACCOUNTNO
    WHERE
      T1.ETL_DT = '{batch_date}' AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.4 TOMS eCorporate_CIF]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS AMLA_RISK_PROFILE_DATE, /* None */
      NULL AS AMLATF_RISK, /* None */
      CASE WHEN T1.NETWORTH = 8 THEN 'N/A' ELSE T1.NETWORTH END AS ESTIMATED_NETWORTH, /* 20251024 */
      NULL AS ANNUAL_INCOME, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      NULL AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_TOMS_ECORPORATE_CIF AS T1 /* None */
    /* LEFT JOIN ${cur_schema}.REF_LOOKUP AS T2 --None
        ON T1.NETWORTH = T2.REFERENCE_CODE
        --AND T2.ETL_DT = '${batch_date}'
        AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
        AND T2.SOURCE_NAME = 'TOMS'
        AND T2.REFERENCE_TYPE = 'NETWORTH' */ /* 20251016 remove join as it is not being used for ESTIMATED_NETWORTH */
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND /* filter to be updated with clean_rule_flag when com_t / com_m updated with clean_rule_flag logic */ NOT T1.CUST_ID IS NULL
""")

spark.sql(f"""
/* ==============[Group.5 TOMS eRetail_CIF]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS AMLA_RISK_PROFILE_DATE, /* None */
      NULL AS AMLATF_RISK, /* None */
      CASE WHEN T1.NETWORTH = 8 THEN 'N/A' ELSE T1.NETWORTH END AS ESTIMATED_NETWORTH, /* 20251024 */
      NULL AS ANNUAL_INCOME, /* None */
      'TOMS' AS SOURCE_NAME, /* None */
      T1.idref AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(COALESCE(T1.SYDTC, '1900-01-01'), COALESCE(T1.SYDTU, '1900-01-01')) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_TOMS_ERETAIL_CIF AS T1 /* None */
    /* LEFT JOIN ${cur_schema}.REF_LOOKUP AS T2 --None
        ON T1.NETWORTH = T2.REFERENCE_CODE
        --AND T2.ETL_DT = '${batch_date}'
        AND T2.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
        AND T2.SOURCE_NAME = 'TOMS'
        AND T2.REFERENCE_TYPE = 'NETWORTH' */ /* 20251016 remove join as it is not being used for ESTIMATED_NETWORTH */
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T3 /* None */
      ON T1.occupation = T3.reference_code
      AND T3.SOURCE_KEY = 'GENERAL_REFERENCE_LOOKUP'
      AND T3.SOURCE_NAME = 'TOMS'
      AND T3.REFERENCE_TYPE = 'OCCUPATION'
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND /* filter to be updated with clean_rule_flag when com_t / com_m updated with clean_rule_flag logic */ NOT T1.CUST_ID IS NULL
""")

spark.sql(f"""
/* ==============[Group.6 (LMS counterparty)]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd') AS AMLA_RISK_PROFILE_DATE, /* None */
      NULL AS AMLATF_RISK, /* None */
      T1.CIF_NET_WORTH_ID AS ESTIMATED_NETWORTH, /* None */
      CAST(T1.GROSS_ANNUAL_INCOME_AMOUNT AS INT) AS ANNUAL_INCOME, /* None */
      'LMS' AS SOURCE_NAME, /* None */
      T1.RECORD_ID AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(
        COALESCE(T1.LAST_ACTION_DATETIME, '1900-01-01'),
        COALESCE(T1.SYSTEM_UPDATED_DATETIME, '1900-01-01')
      ) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.M_LMSKIBB2_TBL_COUNTERPARTY AS T1
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND NOT T1.CUST_ID IS NULL
      AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.7 Guava Company]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS AMLA_RISK_PROFILE_DATE, /* None */
      NULL AS AMLATF_RISK, /* None */
      TRIM(T3.FIELDVALUE) AS ESTIMATED_NETWORTH, /* None */
      NULL AS ANNUAL_INCOME, /* None */
      'GUAVA' AS SOURCE_NAME, /* None */
      T1.ATTACHMENT_TRANSACTION_INFO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP, /* None */
      T2.ETL_TIMESTAMP AS SOURCE_UPDATE_DATE /* 20251028 */
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
          AND TRIM(FIELDID) = 'ESTINETWORTH'
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
      AND NOT T1.CUST_ID IS NULL
      AND TRIM(COALESCE(T3.FIELDVALUE, '')) <> ''
""")

spark.sql(f"""
/* ==============[source 8 SBL counterparty)]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      T1.cust_id AS customer_id,
      FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd') AS AMLA_RISK_PROFILE_DATE, /* NONE */
      NULL AS amlaf_risk,
      T3.Investment_Profiling_Estimated_Net_Worth_Id AS ESTIMATED_NETWORTH, /* ,  CAST(T3.Gross_Annual_Income_Amount AS INT) as ANNUAL_INCOME */
      CASE
        WHEN t3.Investment_Profiling_Annual_Income_Id = 1
        THEN 12000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 2
        THEN 30000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 3
        THEN 60000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 4
        THEN 90000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 5
        THEN 120000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 6
        THEN 240000
        WHEN t3.Investment_Profiling_Annual_Income_Id = 7
        THEN 252000
      END AS ANNUAL_INCOME,
      'SBL' AS SOURCE_NAME,
      T3.record_id AS SOURCE_RECORD_ID,
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP,
      CAST(GREATEST(
        COALESCE(T3.LAST_ACTION_DATETIME, '1900-01-01'),
        COALESCE(T3.SYSTEM_UPDATED_DATETIME, '1900-01-01')
      ) AS TIMESTAMP) AS SOURCE_UPDATE_DATE
    FROM {params["com_schema"]}.m_sbl_tbl_einvoicing_clientdata AS T1
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
    LEFT JOIN {params["cur_schema"]}.REF_LOOKUP AS T4
      ON T3.Investment_Profiling_Estimated_Net_Worth_Id = T4.REFERENCE_CODE
      AND T4.SOURCE_NAME = 'AML'
      AND T4.REFERENCE_TYPE = 'ESTIMATEDNETWORTH'
    WHERE
      T1.ETL_DT = '{batch_date}' AND NOT T1.CLEAN_RULE_FLAG LIKE '%1%'
""")

spark.sql(f"""
/* ==============[Group.9 KDI Customer]============== */ /* 20251029 */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* None */
    )
    SELECT
      T1.CUST_ID AS CUSTOMER_ID, /* None */
      NULL AS AMLA_RISK_PROFILE_DATE, /* None */
      T1.risk_level AS AMLATF_RISK, /* None */
      TRIM(T1.NET_WORTH) AS ESTIMATED_NETWORTH, /* None */ /* , T1.Monthly_income                            as MONTHLY_INCOME         -- to check annual income */
      CASE
        WHEN UPPER(T1.NET_WORTH) RLIKE '^[\\s\\u00A0]*(LESS[\\s\\u00A0]+THAN|UNDER|BELOW)[\\s\\u00A0]+RM[\\s\\u00A0]*[0-9.,]+[\\s\\u00A0]*[KM]?[\\s\\u00A0]*$'
        THEN CAST((
          CAST(REGEXP_REPLACE(
            REGEXP_EXTRACT(
              UPPER(T1.NET_WORTH),
              '^(?:[\\s\\u00A0]*(?:LESS[\\s\\u00A0]+THAN|UNDER|BELOW)[\\s\\u00A0]+RM[\\s\\u00A0]*)([0-9.,]+)[\\s\\u00A0]*([KM]?)'
            ),
            ',',
            ''
          ) AS DOUBLE) /* Extract number and suffix; allow NBSP */ * CASE UPPER(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                '^(?:[\\s\\u00A0]*(?:LESS[\\s\\u00A0]+THAN|UNDER|BELOW)[\\s\\u00A0]+RM[\\s\\u00A0]*)([0-9.,]+)[\\s\\u00A0]*([KM]?)',
                2
              )
            )
            WHEN 'M'
            THEN 1000000
            WHEN 'K'
            THEN 1000
            ELSE 1
          END * 12
        ) AS BIGINT)
        WHEN UPPER(T1.NET_WORTH) RLIKE '^[\\s\\u00A0]*(MORE[\\s\\u00A0]+THAN|ABOVE|OVER)[\\s\\u00A0]+RM[\\s\\u00A0]*[0-9.,]+[\\s\\u00A0]*[KM]?[\\s\\u00A0]*$'
        THEN CAST((
          CAST(REGEXP_REPLACE(
            REGEXP_EXTRACT(
              UPPER(T1.NET_WORTH),
              '^(?:[\\s\\u00A0]*(?:MORE[\\s\\u00A0]+THAN|ABOVE|OVER)[\\s\\u00A0]+RM[\\s\\u00A0]*)([0-9.,]+)[\\s\\u00A0]*([KM]?)'
            ),
            ',',
            ''
          ) AS DOUBLE) * CASE UPPER(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                '^(?:[\\s\\u00A0]*(?:MORE[\\s\\u00A0]+THAN|ABOVE|OVER)[\\s\\u00A0]+RM[\\s\\u00A0]*)([0-9.,]+)[\\s\\u00A0]*([KM]?)',
                2
              )
            )
            WHEN 'M'
            THEN 1000000
            WHEN 'K'
            THEN 1000
            ELSE 1
          END * 12
        ) AS BIGINT)
        WHEN UPPER(T1.NET_WORTH) RLIKE 'RM[\\s\\u00A0]*[0-9.,]+[\\s\\u00A0]*[KM]?[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*[0-9.,]+[\\s\\u00A0]*[KM]?'
        THEN CAST((
          GREATEST(
            CAST(REGEXP_REPLACE(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                'RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)'
              ),
              ',',
              ''
            ) AS DOUBLE) /* First bound */ * CASE UPPER(
                REGEXP_EXTRACT(
                  UPPER(T1.NET_WORTH),
                  'RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)',
                  2
                )
              )
              WHEN 'M'
              THEN 1000000
              WHEN 'K'
              THEN 1000
              ELSE 1
            END,
            CAST(REGEXP_REPLACE(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                'RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)',
                3
              ),
              ',',
              ''
            ) AS DOUBLE) /* Second bound */ * CASE UPPER(
                REGEXP_EXTRACT(
                  UPPER(T1.NET_WORTH),
                  'RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)',
                  4
                )
              )
              WHEN 'M'
              THEN 1000000
              WHEN 'K'
              THEN 1000
              ELSE 1
            END
          ) * 12
        ) AS BIGINT)
        WHEN UPPER(T1.NET_WORTH) RLIKE '^[\\s\\u00A0]*RM[\\s\\u00A0]*[0-9.,]+[\\s\\u00A0]*[KM]?[\\s\\u00A0]*$'
        THEN CAST((
          CAST(REGEXP_REPLACE(
            REGEXP_EXTRACT(
              UPPER(T1.NET_WORTH),
              '^[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*$'
            ),
            ',',
            ''
          ) AS DOUBLE) * CASE UPPER(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                '^[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9.,]+)[\\s\\u00A0]*([KM]?)[\\s\\u00A0]*$',
                2
              )
            )
            WHEN 'M'
            THEN 1000000
            WHEN 'K'
            THEN 1000
            ELSE 1
          END * 12
        ) AS BIGINT)
        WHEN UPPER(T1.NET_WORTH) RLIKE 'RM[0-9,]+[\\s\\u00A0]*-[\\s\\u00A0]*RM[0-9,]+'
        THEN CAST((
          GREATEST(
            CAST(REGEXP_REPLACE(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                'RM[\\s\\u00A0]*([0-9,]+)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9,]+)'
              ),
              ',',
              ''
            ) AS DOUBLE),
            CAST(REGEXP_REPLACE(
              REGEXP_EXTRACT(
                UPPER(T1.NET_WORTH),
                'RM[\\s\\u00A0]*([0-9,]+)[\\s\\u00A0]*-[\\s\\u00A0]*RM[\\s\\u00A0]*([0-9,]+)',
                2
              ),
              ',',
              ''
            ) AS DOUBLE)
          ) * 12
        ) AS BIGINT)
        ELSE NULL
      END AS ANNUAL_INCOME,
      'KDI' AS SOURCE_NAME, /* None */
      T1.ACCOUNT_NO AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP, /* None */
      T1.record_updated_date AS SOURCE_UPDATE_DATE /* None */
    FROM {params["com_schema"]}.M_KDI_CLIENTREPORT AS T1
    /* LEFT JOIN ( */ /* SELECT */ /* ACCOUNT_NO */ /* , MAX(ETL_TIMESTAMP) AS ETL_TIMESTAMP */ /* FROM ${com_schema}.R_KDI_CUSTOMER */ /* GROUP BY ACCOUNT_NO */ /* ) AS T2 */ /* ON T1.ACCOUNT_NO = T2.ACCOUNT_NO */
    WHERE
      T1.ETL_DT = '{batch_date}'
      AND NOT T1.CUST_ID IS NULL
      AND TRIM(COALESCE(T1.NET_WORTH, '')) <> ''
""")

spark.sql(f"""
/* Insert into curated table */
    WITH RISK_PROFILE_ROW_NUM AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME, CUSTOMER_ID ORDER BY SOURCE_UPDATE_DATE DESC) AS RN
      FROM {params["cur_schema"]}.TEMP_DIM_RISK_PROFILE
    )
    INSERT INTO {params["cur_schema"]}.DIM_RISK_PROFILE PARTITION(etl_dt = '{batch_date}') (
      CUSTOMER_ID, /* None */
      AMLA_RISK_PROFILE_DATE, /* None */
      AMLATF_RISK, /* None */
      ESTIMATED_NETWORTH, /* None */
      ANNUAL_INCOME, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE /* 20251028 */
    )
    SELECT
      CUSTOMER_ID,
      AMLA_RISK_PROFILE_DATE,
      AMLATF_RISK,
      ESTIMATED_NETWORTH,
      ANNUAL_INCOME,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      ETL_TIMESTAMP,
      SOURCE_UPDATE_DATE
    FROM RISK_PROFILE_ROW_NUM
    WHERE
      RN = 1
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_None_dim_risk_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_None_dim_risk_updated (
    customer_id VARCHAR(20)
    , amla_risk_profile_date DATE
    , amlatf_risk VARCHAR(1)
    , estimated_networth VARCHAR(100)
    , annual_income INT
    , source_name VARCHAR(10)
    , source_record_id VARCHAR(20)
    , source_update_date TIMESTAMP
    , dl_record_status       VARCHAR(10)
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_None_dim_risk_updated
SELECT
    customer_id
    , amla_risk_profile_date
    , amlatf_risk
    , estimated_networth
    , annual_income
    , source_name
    , source_record_id
    , source_update_date
    , 'A' AS dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
FROM {params["com_schema"]}.None_dim_risk com_t
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.dim_None_dim_risk r
    WHERE r.etl_dt = '{batch_date}'
      AND r.customer_id = com_t.customer_id
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_None_dim_risk_updated
SELECT
    r.customer_id
    , r.amla_risk_profile_date
    , r.amlatf_risk
    , r.estimated_networth
    , r.annual_income
    , r.source_name
    , r.source_record_id
    , r.source_update_date
    , 'A' AS dl_record_status
    , CASE
        WHEN com_t.customer_id IS NOT NULL THEN com_t.dl_record_created_date
        ELSE current_timestamp() END AS dl_record_created_date
    , current_timestamp() AS dl_record_updated_date
FROM {params["raw_schema"]}.dim_None_dim_risk r
LEFT JOIN {params["com_schema"]}.None_dim_risk com_t
    ON r.customer_id = com_t.customer_id
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.None_dim_risk
SELECT
    customer_id
    , amla_risk_profile_date
    , amlatf_risk
    , estimated_networth
    , annual_income
    , source_name
    , source_record_id
    , source_update_date
    , dl_record_status
    , dl_record_created_date
    , dl_record_updated_date
    , '{batch_date}'       AS etl_dt
    , current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_None_dim_risk_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.None_dim_risk COMPUTE STATISTICS""")

spark.stop()