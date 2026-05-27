
"""
Purpose:    curated - Snapshot table script
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py yyyymmdd [file_name]
CreateDate: 2023-08-18 00:00:00
FileType:   DML
Logs:
Table name: DIM_ADDRESS
Table comment: DIM_ADDRESS
Creation date: 2023-08-18 00:00:00
Primary key field: OWNER_ID,ADDRESS_OWNER_TYPE,ADDRESS_TYPE
Attribution hierarchy: curated
Attribution subject: cust
Main application: None
Analyst: zhairuoping
Time granularity: None
Retention period: None
Descriptive information: None
version2
chenguanhong  20240801    add sbl/lms source (UAT)
Sharon        20250123    Updated REF_LOOKUP joining to include field SOURCE_KEY and remove ETL_DT filter (UAT)
marcoong      20250128    add agent assistant from MHBOS
marcoong      20250326    add new source kdi/sbl/lms
marcoong      20250423    Updated REF_LOOKUP joining to include field SOURCE_KEY and remove ETL_DT filter
marcoong      20250519    update table com_r_mhbos_trader, com_r_mhbos_m_trader_cmsrl, com_r_mhbos_m_branch, com_r_m21_ACCOUNTEXECUTIVE to com_t
marcoong      20250715    added new source field perm_country for MHBOS
marcoong      20251015    update state definition logic for MHBOS's Account mailing address (Group 1)
Afiq Azizi    20260223    change the source for KDI from KDI Customer to KDI Clientreport
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp

source_name = "M21"
table_name  = "dim_address"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites if there is a partition key
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── SOURCE PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* ==============[Group.1]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS (
      OWNER_ID VARCHAR(50), /* None */
      ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
      ADDRESS_TYPE VARCHAR(10), /* None */
      ADDRESS_LINE_1 VARCHAR(100), /* None */
      ADDRESS_LINE_2 VARCHAR(100), /* None */
      ADDRESS_LINE_3 VARCHAR(100), /* None */
      ADDRESS_LINE_4 VARCHAR(100), /* None */
      CITY VARCHAR(255), /* None */
      STATE VARCHAR(50), /* None */
      POSTCODE VARCHAR(5), /* None */
      COUNTRY VARCHAR(3), /* None */
      ADDRESS_CREATE_DATE DATE, /* None */
      ADDRESS_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50) /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.3]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS (
      OWNER_ID VARCHAR(50), /* None */
      ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
      ADDRESS_TYPE VARCHAR(10), /* None */
      ADDRESS_LINE_1 VARCHAR(100), /* None */
      ADDRESS_LINE_2 VARCHAR(100), /* None */
      ADDRESS_LINE_3 VARCHAR(100), /* None */
      ADDRESS_LINE_4 VARCHAR(100), /* None */
      CITY VARCHAR(255), /* None */
      STATE VARCHAR(50), /* None */
      POSTCODE VARCHAR(5), /* None */
      COUNTRY VARCHAR(3), /* None */
      ADDRESS_CREATE_DATE DATE, /* None */
      ADDRESS_UPDATE_DATE DATE, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(50) /* None */
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21
""")
spark.sql(f"""
CREATE TABLE {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
      OWNER_ID VARCHAR(50), /* None */
      ADDRESS_OWNER_TYPE VARCHAR(20), /* None */
      ADDRESS_TYPE VARCHAR(10), /* None */
      ADDRESS_LINE_1 VARCHAR(100), /* None */
      ADDRESS_LINE_2 VARCHAR(100), /* None */
      ADDRESS_LINE_3 VARCHAR(100), /* None */
      ADDRESS_LINE_4 VARCHAR(100), /* None */
      CITY VARCHAR(255), /* None */
      STATE VARCHAR(50), /* None */
      POSTCODE VARCHAR(5), /* None */
      COUNTRY VARCHAR(3), /* None */
      ADDRESS_CREATE_TIME TIMESTAMP, /* None */
      ADDRESS_UPDATE_TIME TIMESTAMP, /* None */
      LINE_OF_BUSINESS VARCHAR(20), /* None */
      SOURCE_NAME VARCHAR(10), /* None */
      SOURCE_RECORD_ID VARCHAR(20), /* NONE */
      ETL_TIMESTAMP STRING
    )
    USING PARQUET
    TBLPROPERTIES (
      'parquet.compression'='SNAPPY',
      'external.table.purge'='true'
    )
""")
spark.sql(f"""
/* ==============[Group.7: M21 Customer address: (account, business), (account, REGISTERED), (account, MAILING)]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
      OWNER_ID,
      ADDRESS_OWNER_TYPE,
      ADDRESS_TYPE,
      ADDRESS_LINE_1,
      ADDRESS_LINE_2,
      ADDRESS_LINE_3,
      ADDRESS_LINE_4,
      CITY,
      STATE,
      POSTCODE,
      COUNTRY,
      ADDRESS_CREATE_TIME,
      ADDRESS_UPDATE_TIME,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      ETL_TIMESTAMP
    )
    /* Account Business address */
    SELECT
      'M21_' || T1.CODE AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'BUSINESS' AS ADDRESS_TYPE, /* None */
      T1.BUSINESSADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
      T1.BUSINESSADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
      T1.BUSINESSADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T1.BUSINESSCITY AS CITY, /* None */
      T1.BUSINESSCOUNTRYSTATE AS STATE, /* None */
      T1.BUSINESSPOSTALCODE AS POSTCODE, /* None */
      COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.BUSINESSCOUNTRY) AS COUNTRY, /* None */
      T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
      T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM {params["com_schema"]}.T_M21_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T2 /* None */
      ON T1.BUSINESSCOUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.BUSINESSADDRESSLINE1, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE2, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSADDRESSLINE3, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSCITY, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSCOUNTRYSTATE, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSPOSTALCODE, '')) <> ''
        OR TRIM(COALESCE(T1.BUSINESSCOUNTRY, '')) <> ''
      )
    UNION ALL
    /* Account registered address */
    SELECT
      'M21_' || T1.CODE AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.ADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
      T1.ADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
      T1.ADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.COUNTRYSTATE AS STATE, /* None */
      T1.POSTALCODE AS POSTCODE, /* None */
      COALESCE(T2.COUNTRY_CODE_3_DIGITS, T1.COUNTRY) AS COUNTRY, /* None */
      T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
      T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM {params["com_schema"]}.T_M21_CUSTOMER AS T1 /* None */
    LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY COUNTRY_CODE_CCRIS ORDER BY COUNTRY_CODE_3_DIGITS DESC) AS ROW_NUM,
        COUNTRY_CODE_3_DIGITS,
        COUNTRY_CODE_CCRIS
      FROM {params["cur_schema"]}.REF_COUNTRY
      WHERE
        TRIM(COALESCE(COUNTRY_CODE_CCRIS, '')) <> ''
        AND DATE_FORMAT(dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    ) AS T2 /* None */
      ON T1.COUNTRY = T2.COUNTRY_CODE_CCRIS AND T2.ROW_NUM = 1
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.ADDRESSLINE1, '')) <> ''
        OR TRIM(COALESCE(T1.ADDRESSLINE2, '')) <> ''
        OR TRIM(COALESCE(T1.ADDRESSLINE3, '')) <> ''
        OR TRIM(COALESCE(T1.CITY, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
        OR TRIM(COALESCE(T1.POSTALCODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
      )
    UNION ALL
    /* Account Mailing address */
    SELECT
      'M21_' || T1.CODE AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.MAILINGADDRESSLINE1 AS ADDRESS_LINE_1, /* None */
      T1.MAILINGADDRESSLINE2 AS ADDRESS_LINE_2, /* None */
      T1.MAILINGADDRESSLINE3 AS ADDRESS_LINE_3, /* None */
      NULL AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.COUNTRYSTATE AS STATE, /* None */
      T1.MAILINGPOSTALCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      T1.CREATEDATE AS ADDRESS_CREATE_TIME, /* None */
      T1.MODIFYDATE AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM {params["com_schema"]}.t_M21_STATEMENTRECIPIENT AS T1 /* None */
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.MAILINGADDRESSLINE1, '')) <> ''
        OR TRIM(COALESCE(T1.MAILINGADDRESSLINE2, '')) <> ''
        OR TRIM(COALESCE(T1.MAILINGADDRESSLINE3, '')) <> ''
        OR TRIM(COALESCE(T1.CITY, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTRYSTATE, '')) <> ''
        OR TRIM(COALESCE(T1.MAILINGPOSTALCODE, '')) <> ''
        OR TRIM(COALESCE(T1.COUNTRY, '')) <> ''
      )
""")
spark.sql(f"""
/* ==============[Group 8: M21_A Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING), (ACCOUNT, EMPLOYER) ]============== */
    WITH M21_A_CUSTOMER AS (
      SELECT
        T1.account_number,
        T1.customer_address_type,
        T1.customer_address_line1,
        T1.customer_address_line2,
        T1.customer_address_line3,
        T1.customer_address_line4,
        T1.CITY,
        T2.reference_value AS state,
        T1.postcode,
        T1.country,
        T1.etl_timestamp
      FROM {params["com_schema"]}.T_M21_A_CUSTOMER AS T1 /* None */
      LEFT JOIN {params["cur_schema"]}.ref_lookup AS T2
        ON T1.state = T2.reference_code
        AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
        AND T2.reference_type = 'IRBSTATE'
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND (
          TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
          OR TRIM(COALESCE(T1.CITY, '')) <> ''
          OR TRIM(COALESCE(T1.state, '')) <> ''
          OR TRIM(COALESCE(T1.postcode, '')) <> ''
          OR TRIM(COALESCE(T1.country, '')) <> ''
        )
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
      OWNER_ID,
      ADDRESS_OWNER_TYPE,
      ADDRESS_TYPE,
      ADDRESS_LINE_1,
      ADDRESS_LINE_2,
      ADDRESS_LINE_3,
      ADDRESS_LINE_4,
      CITY,
      STATE,
      POSTCODE,
      COUNTRY,
      ADDRESS_CREATE_TIME,
      ADDRESS_UPDATE_TIME,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      ETL_TIMESTAMP
    )
    /* Account registered address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_A_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
    UNION ALL
    /* Account Mailing address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_A_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
    UNION ALL
    /* Account Employer address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'EMPLOYER' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_A_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER'
""")
spark.sql(f"""
/* ==============[Group 9: M21_O Customer address: (ACCOUNT, REGISTERED), (ACCOUNT, MAILING)]============== */
    WITH M21_O_CUSTOMER AS (
      SELECT
        T1.account_number,
        T1.customer_address_type,
        T1.customer_address_line1,
        T1.customer_address_line2,
        T1.customer_address_line3,
        T1.customer_address_line4,
        T1.CITY,
        T2.reference_value AS state,
        T1.postcode,
        T1.country,
        T1.etl_timestamp
      FROM {params["com_schema"]}.T_M21_O_CUSTOMER AS T1 /* None */
      LEFT JOIN {params["cur_schema"]}.ref_lookup AS T2
        ON T1.state = T2.reference_code
        AND T2.source_key = 'GENERAL_REFERENCE_LOOKUP'
        AND T2.reference_type = 'IRBSTATE'
      WHERE
        DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
        AND (
          TRIM(COALESCE(T1.customer_address_line1, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line2, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line3, '')) <> ''
          OR TRIM(COALESCE(T1.customer_address_line4, '')) <> ''
          OR TRIM(COALESCE(T1.CITY, '')) <> ''
          OR TRIM(COALESCE(T1.state, '')) <> ''
          OR TRIM(COALESCE(T1.postcode, '')) <> ''
          OR TRIM(COALESCE(T1.country, '')) <> ''
        )
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21 (
      OWNER_ID,
      ADDRESS_OWNER_TYPE,
      ADDRESS_TYPE,
      ADDRESS_LINE_1,
      ADDRESS_LINE_2,
      ADDRESS_LINE_3,
      ADDRESS_LINE_4,
      CITY,
      STATE,
      POSTCODE,
      COUNTRY,
      ADDRESS_CREATE_TIME,
      ADDRESS_UPDATE_TIME,
      LINE_OF_BUSINESS,
      SOURCE_NAME,
      SOURCE_RECORD_ID,
      ETL_TIMESTAMP
    )
    /* Account registered address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_O_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'REGISTERED'
    UNION ALL
    /* Account Mailing address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'MAILING' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_O_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'MAILING'
    UNION ALL
    /* Account Employer address */
    SELECT
      'M21_' || T1.account_number AS OWNER_ID, /* None */
      'ACCOUNT' AS ADDRESS_OWNER_TYPE, /* None */
      'EMPLOYER' AS ADDRESS_TYPE, /* None */
      T1.customer_address_line1 AS ADDRESS_LINE_1, /* None */
      T1.customer_address_line2 AS ADDRESS_LINE_2, /* None */
      T1.customer_address_line3 AS ADDRESS_LINE_3, /* None */
      T1.customer_address_line4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.state AS STATE, /* None */
      T1.postcode AS POSTCODE, /* None */
      T1.country AS COUNTRY, /* None */
      NULL AS ADDRESS_CREATE_TIME, /* None */
      T1.etl_timestamp AS ADDRESS_UPDATE_TIME, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.account_number AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS etl_timestamp
    FROM M21_O_CUSTOMER AS T1 /* None */
    WHERE
      UPPER(TRIM(T1.customer_address_type)) = 'EMPLOYER'
""")
spark.sql(f"""
/* ==============[Group.7 + 8 + 9: M21 Customer address]============== */
    WITH TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM AS (
      SELECT
        OWNER_ID, /* None */
        ADDRESS_OWNER_TYPE, /* None */
        ADDRESS_TYPE, /* None */
        UPPER(ADDRESS_LINE_1) AS ADDRESS_LINE_1,
        UPPER(ADDRESS_LINE_2) AS ADDRESS_LINE_2,
        UPPER(ADDRESS_LINE_3) AS ADDRESS_LINE_3,
        UPPER(ADDRESS_LINE_4) AS ADDRESS_LINE_4,
        UPPER(CITY) AS CITY,
        UPPER(STATE) AS STATE,
        UPPER(POSTCODE) AS POSTCODE,
        UPPER(COUNTRY) AS COUNTRY,
        DATE_FORMAT(CAST(ADDRESS_CREATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_CREATE_DATE, /* None */
        DATE_FORMAT(CAST(ADDRESS_UPDATE_TIME AS TIMESTAMP), 'yyyy-MM-dd') AS ADDRESS_UPDATE_DATE, /* None */
        LINE_OF_BUSINESS, /* None */
        SOURCE_NAME, /* None */
        SOURCE_RECORD_ID, /* None */
        ROW_NUMBER() OVER (PARTITION BY OWNER_ID, ADDRESS_OWNER_TYPE, ADDRESS_TYPE ORDER BY ADDRESS_UPDATE_TIME DESC) AS rn
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS_M21
    )
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    FROM TEMP_DIM_ACCOUNT_ADDRESS_M21_ROW_NUM
    WHERE
      rn = 1
""")
spark.sql(f"""
/* ==============[Group.10]============== */
    INSERT INTO {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID /* None */
    )
    SELECT
      'M21_' || T1.CODE AS OWNER_ID, /* None */
      'AGENT' AS ADDRESS_OWNER_TYPE, /* None */
      'REGISTERED' AS ADDRESS_TYPE, /* None */
      T1.LINE1 AS ADDRESS_LINE_1, /* None */
      T1.LINE2 AS ADDRESS_LINE_2, /* None */
      T1.LINE3 AS ADDRESS_LINE_3, /* None */
      T1.LINE4 AS ADDRESS_LINE_4, /* None */
      NULL AS CITY, /* None */
      NULL AS STATE, /* None */
      NULL AS POSTCODE, /* None */
      NULL AS COUNTRY, /* None */
      T2.CREATEDATE AS ADDRESS_CREATE_DATE, /* None */
      T2.MODIFYDATE AS ADDRESS_UPDATE_DATE, /* None */
      'FT' AS LINE_OF_BUSINESS, /* None */
      'M21' AS SOURCE_NAME, /* None */
      T1.CODE AS SOURCE_RECORD_ID /* None */
    FROM {params["com_schema"]}.t_M21_ACCOUNTEXECUTIVEADDRESS AS T1 /* None */
    LEFT JOIN {params["com_schema"]}.T_M21_ACCOUNTEXECUTIVE AS T2 /* None */
      ON T1.CODE = T2.CODE
      AND DATE_FORMAT(T2.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
    WHERE
      DATE_FORMAT(T1.dl_record_updated_date, 'yyyyMMdd') = '{batch_date}'
      AND (
        TRIM(COALESCE(T1.LINE1, '')) <> ''
        OR TRIM(COALESCE(T1.LINE2, '')) <> ''
        OR TRIM(COALESCE(T1.LINE3, '')) <> ''
        OR TRIM(COALESCE(T1.LINE4, '')) <> ''
      )
""")
spark.sql(f"""
/* Delete all temporary tables */
    DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
""")

# ─── DELTA TABLE SETUP (TRANSFORMED COM_T) ──────────────────────────────────
# This table will hold the transformed delta records from the COM layer.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_address_m21_delta""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_address_m21_delta (
        owner_id VARCHAR(50)
        , address_owner_type VARCHAR(20)
        , address_type VARCHAR(10)
        , address_line_1 VARCHAR(100)
        , address_line_2 VARCHAR(100)
        , address_line_3 VARCHAR(100)
        , address_line_4 VARCHAR(100)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , postcode VARCHAR(5)
        , country VARCHAR(3)
        , address_create_date DATE
        , address_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── MAIN PROCESSING (Populate Delta Table) ───────────────────────────────────
# The main_processing_sqls should contain the logic to transform and insert data
# from com_t into the temp delta table, filtering for the current batch_date.
spark.sql(f"""
ALTER TABLE {params["cur_schema"]}.temp_DIM_ADDRESS_main_consolidated DROP IF EXISTS
""")
spark.sql(f"""
/* ==============[Group.23]============== */
    INSERT INTO {params["cur_schema"]}.temp_DIM_ADDRESS_main_consolidated (
      OWNER_ID, /* None */
      ADDRESS_OWNER_TYPE, /* None */
      ADDRESS_TYPE, /* None */
      ADDRESS_LINE_1, /* None */
      ADDRESS_LINE_2, /* None */
      ADDRESS_LINE_3, /* None */
      ADDRESS_LINE_4, /* None */
      CITY, /* None */
      STATE, /* None */
      POSTCODE, /* None */
      COUNTRY, /* None */
      ADDRESS_CREATE_DATE, /* None */
      ADDRESS_UPDATE_DATE, /* None */
      LINE_OF_BUSINESS, /* None */
      SOURCE_NAME, /* None */
      SOURCE_RECORD_ID, /* None */
      ETL_TIMESTAMP
    )
    SELECT
      T1.OWNER_ID AS OWNER_ID, /* None */
      T1.ADDRESS_OWNER_TYPE AS ADDRESS_OWNER_TYPE, /* None */
      T1.ADDRESS_TYPE AS ADDRESS_TYPE, /* None */
      T1.ADDRESS_LINE_1 AS ADDRESS_LINE_1, /* None */
      T1.ADDRESS_LINE_2 AS ADDRESS_LINE_2, /* None */
      T1.ADDRESS_LINE_3 AS ADDRESS_LINE_3, /* None */
      T1.ADDRESS_LINE_4 AS ADDRESS_LINE_4, /* None */
      T1.CITY AS CITY, /* None */
      T1.STATE AS STATE, /* None */
      T1.POSTCODE AS POSTCODE, /* None */
      T1.COUNTRY AS COUNTRY, /* None */
      T1.ADDRESS_CREATE_DATE AS ADDRESS_CREATE_DATE, /* None */
      T1.ADDRESS_UPDATE_DATE AS ADDRESS_UPDATE_DATE, /* None */
      T1.LINE_OF_BUSINESS AS LINE_OF_BUSINESS, /* None */
      T1.SOURCE_NAME AS SOURCE_NAME, /* None */
      T1.SOURCE_RECORD_ID AS SOURCE_RECORD_ID, /* None */
      CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
    FROM (
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_ACCOUNT_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_TRADER_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_BRANCH_ADDRESS
      UNION ALL
      SELECT
        *
      FROM {params["cur_schema"]}.TEMP_DIM_CUSTOMER_ADDRESS
    ) AS T1 /* None */
    WHERE
      1 = 1
""")

# ─── UPDATED TABLE SETUP ─────────────────────────────────────────────────────
# This table will hold the final, merged dataset.
spark.sql(f"""DROP TABLE IF EXISTS {params["tmp_schema"]}.temp_dim_address_m21_updated""")
spark.sql(f"""
    CREATE TABLE {params["tmp_schema"]}.temp_dim_address_m21_updated (
        owner_id VARCHAR(50)
        , address_owner_type VARCHAR(20)
        , address_type VARCHAR(10)
        , address_line_1 VARCHAR(100)
        , address_line_2 VARCHAR(100)
        , address_line_3 VARCHAR(100)
        , address_line_4 VARCHAR(100)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , postcode VARCHAR(5)
        , country VARCHAR(3)
        , address_create_date DATE
        , address_update_date DATE
        , line_of_business VARCHAR(20)
        , source_record_id VARCHAR(50)
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from CUR ──────────────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_address_m21_updated
    SELECT
        cur.owner_id,
        cur.address_owner_type,
        cur.address_type,
        cur.address_line_1,
        cur.address_line_2,
        cur.address_line_3,
        cur.address_line_4,
        cur.city,
        cur.state,
        cur.postcode,
        cur.country,
        cur.address_create_date,
        cur.address_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date
    FROM {params["cur_schema"]}.dim_address_m21 cur
    WHERE cur.source_key = 'M21' AND         NOT EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_address_m21_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE        )
""")

# ─── STEP 2: Upsert transformed delta into temp table ────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["tmp_schema"]}.temp_dim_address_m21_updated
    SELECT
        delta.owner_id,
        delta.address_owner_type,
        delta.address_type,
        delta.address_line_1,
        delta.address_line_2,
        delta.address_line_3,
        delta.address_line_4,
        delta.city,
        delta.state,
        delta.postcode,
        delta.country,
        delta.address_create_date,
        delta.address_update_date,
        delta.line_of_business,
        delta.source_record_id,
        delta.dl_record_status,
        CASE
            WHEN cur.OWNER_ID IS NOT NULL THEN cur.dl_record_created_date
            ELSE delta.dl_record_created_date
        END AS dl_record_created_date,
        delta.dl_record_updated_date
    FROM {params["tmp_schema"]}.temp_dim_address_m21_delta delta
    LEFT JOIN {params["cur_schema"]}.dim_address_m21 cur
        ON cur.source_key = 'M21' AND delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE""")

# ─── STEP 3: Insert previous state of changed records into History table ─────
spark.sql(f"""
    INSERT INTO TABLE {params["cur_schema"]}.dim_address_m21_h PARTITION (source_key, hist_year)
    SELECT
        cur.owner_id,
        cur.address_owner_type,
        cur.address_type,
        cur.address_line_1,
        cur.address_line_2,
        cur.address_line_3,
        cur.address_line_4,
        cur.city,
        cur.state,
        cur.postcode,
        cur.country,
        cur.address_create_date,
        cur.address_update_date,
        cur.line_of_business,
        cur.source_record_id,
        cur.dl_record_status,
        cur.dl_record_created_date,
        cur.dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
, 'M21' AS source_key        , DATE_FORMAT(current_timestamp(), 'yyyy') AS hist_year
    FROM {params["cur_schema"]}.dim_address_m21 cur
    WHERE cur.source_key = 'M21' AND         EXISTS (
            SELECT 1 FROM {params["tmp_schema"]}.temp_dim_address_m21_delta delta
            WHERE delta.OWNER_ID = cur.OWNER_ID AND delta.ADDRESS_OWNER_TYPE = cur.ADDRESS_OWNER_TYPE AND delta.ADDRESS_TYPE = cur.ADDRESS_TYPE        )
""")

# ─── STEP 4: Overwrite CUR table ──────────────────────────────────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["cur_schema"]}.dim_address_m21 PARTITION (source_key = 'M21')    SELECT
        owner_id,
        address_owner_type,
        address_type,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        city,
        state,
        postcode,
        country,
        address_create_date,
        address_update_date,
        line_of_business,
        source_record_id,
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
    FROM {params["tmp_schema"]}.temp_dim_address_m21_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["cur_schema"]}.dim_address_m21 PARTITION (source_key = 'M21') COMPUTE STATISTICS
""")

spark.stop()