##  File Name   : com_m_mhbos_m_client
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-06 08:42:13
##  Source      : com_m_mhbos_m_client (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "mhbos"
table_name  = "m_client"


spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)



# add queries here
spark.sql(rf"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_merge
""")


spark.sql(rf"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id
""")


spark.sql(rf"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_exception
""")


spark.sql(rf"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_new_cust_id
""")


spark.sql(rf"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_account_cust_id
""")



spark.sql(rf"""
/*
===================================== GET UNIQUE CUSTOMER =====================================
*/
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_merge (
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `primary_identification_type_consolidated` VARCHAR(10),
  `primary_identification_no_consolidated` VARCHAR(60),
  `secondary_identification_type_consolidated` VARCHAR(10),
  `secondary_identification_no_consolidated` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` STRING
)
""")


spark.sql(rf"""
/*
Get Unique Customer:
Step 1: CREATE indicator for accounts WHERE Account No already exist in Mapping TABLE
Step 2: PARTITION data BY Primary ID No + Customer Name. ORDER BY Account CREATE Date (DESC) AND Account No (DESC).
 */
WITH current_date_account AS (
  SELECT
    *
  FROM {params["com_schema"]}.t_mhbos_m_client
  WHERE
    TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
), customer_row_number AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY t.primary_identification_no, t.customer_name ORDER BY IF(NOT mapping.source_owner_id IS NULL, 1, 0) DESC /*
                Step 2: Prioritize active records, since when insert to temp_m_mhbos_m_client_new_cust_id
                We only assign cust_id for active records, closed account will be marked with closed account exception and
                won't be inserted to mapping / curated tables.
                 */, IF(date_closed IS NULL, 1, 0) DESC /* Step 3: ORDER BY Account CREATE Date (DESC) AND Account No (DESC). */, GREATEST(COALESCE(t.date_created, '1900-01-01'), COALESCE(t.date_change, '1900-01-01')) DESC, t.client_no DESC) AS rn,
    t.client_no,
    t.clean_rule_flag,
    t.primary_identification_type,
    t.primary_identification_no,
    t.secondary_identification_type,
    t.secondary_identification_no,
    t.customer_name,
    t.date_created,
    t.date_closed,
    t.date_change,
    t.etl_timestamp
  FROM current_date_account AS t
  /* Prioritize accounts WHERE Account No already exist in Mapping TABLE */
  LEFT JOIN {params["com_schema"]}.m_customer_id_mapping AS mapping
    ON t.client_no = mapping.source_owner_id
), consolidated_id AS (
  SELECT
    m1.client_no,
    m1.clean_rule_flag,
    m1.primary_identification_type AS primary_identification_type,
    m1.primary_identification_no AS primary_identification_no,
    m1.secondary_identification_type AS secondary_identification_type,
    m1.secondary_identification_no AS secondary_identification_no,
    COALESCE(m2.primary_identification_type, m1.primary_identification_type) AS primary_identification_type_consolidated,
    COALESCE(m2.primary_identification_no, m1.primary_identification_no) AS primary_identification_no_consolidated,
    COALESCE(m2.secondary_identification_type, m1.secondary_identification_type) AS secondary_identification_type_consolidated,
    COALESCE(m2.secondary_identification_no, m1.secondary_identification_no) AS secondary_identification_no_consolidated,
    m1.customer_name,
    m1.date_created,
    m1.date_closed,
    m1.date_change,
    m1.etl_timestamp
  FROM (
    SELECT
      *
    FROM customer_row_number
    WHERE
      rn = 1
  ) AS m1
  LEFT JOIN (
    SELECT
      *
    FROM customer_row_number
    WHERE
      rn = 1
  ) AS m2
    ON m1.primary_identification_no = m2.secondary_identification_no
    AND m1.primary_identification_type = m2.secondary_identification_type
    AND m1.customer_name = m2.customer_name
    AND m1.client_no <> m2.client_no
    AND COALESCE(m2.secondary_identification_no, '') <> ''
)
INSERT INTO {params["com_schema"]}.temp_m_mhbos_m_client_merge
SELECT
  client_no,
  clean_rule_flag,
  primary_identification_type,
  primary_identification_no,
  secondary_identification_type,
  secondary_identification_no,
  primary_identification_type_consolidated,
  primary_identification_no_consolidated,
  secondary_identification_type_consolidated,
  secondary_identification_no_consolidated,
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM consolidated_id
WHERE
  primary_identification_no = primary_identification_no_consolidated
""")


spark.sql(rf"""
/*
===================================== LOOKUP EXISTING CUST ID FROM MAPPING =====================================
*/
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `primary_identification_type_consolidated` VARCHAR(10),
  `primary_identification_no_consolidated` VARCHAR(60),
  `secondary_identification_type_consolidated` VARCHAR(10),
  `secondary_identification_no_consolidated` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` STRING
)
""")


spark.sql(rf"""
/*
Step 1: Get latest mapping based ON
1. primary_identification_no, exclude exception for
    - primary_identification_no
    - customer_name
2. secondary_identification_no, exclude exception for
    - secondary_identification_no
    - customer_name
3. source_owner_id, exclude exception for
    - primary_identification_no
    - customer_name

Step 2: JOIN WITH latest mapping, matched cust_id will be selected based ON JOIN priority:
1. primary_identification_no = primary_identification_no
2. primary_identification_no = secondary_identification_no
3. secondary_identification_no = primary_identification_no
4. secondary_identification_no = secondary_identification_no
5. source_owner_id = source_owner_id
*/
WITH latest_primary_identification_no /* latest mapping based ON primary_identification_no */ AS (
  SELECT
    COALESCE(t.primary_identification_no, mapping.primary_identification_no) AS primary_identification_no,
    COALESCE(t.customer_name, mapping.customer_name) AS customer_name,
    mapping.cust_id,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(t.primary_identification_no, mapping.primary_identification_no), COALESCE(t.customer_name, mapping.customer_name) ORDER BY mapping.cust_id, mapping.priority_level) AS rn
  FROM {params["com_schema"]}.m_customer_id_mapping AS mapping
  LEFT JOIN {params["com_schema"]}.t_mhbos_m_client AS t
    ON mapping.source_owner_id = t.client_no 
    AND TO_DATE(t.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd'))) 
  WHERE
    TRIM(COALESCE(t.primary_identification_no, mapping.primary_identification_no, '')) /* exclude exception for primary_identification_no */ <> ''
    AND NOT TRIM(COALESCE(t.primary_identification_no, mapping.primary_identification_no, '')) LIKE '@[%]'
    AND /* exclude exception for customer_name */ TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) <> ''
    AND NOT TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) LIKE '@[%]'
), latest_secondary_identification_no /* latest mapping based ON secondary_identification_no */ AS (
  SELECT
    COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) AS secondary_identification_no,
    COALESCE(t.customer_name, mapping.customer_name) AS customer_name,
    mapping.cust_id,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ), COALESCE(t.customer_name, mapping.customer_name) ORDER BY mapping.cust_id, mapping.priority_level) AS rn
  FROM {params["com_schema"]}.m_customer_id_mapping AS mapping
  LEFT JOIN {params["com_schema"]}.t_mhbos_m_client AS t
    ON mapping.source_owner_id = t.client_no 
    AND TO_DATE(t.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd'))) 
  WHERE
    COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) /* exclude exception for secondary_identification_no */ <> ''
    AND NOT COALESCE(
      NULLIF(TRIM(COALESCE(t.secondary_identification_no, '')), ''),
      NULLIF(TRIM(COALESCE(mapping.secondary_identification_no, '')), '')
    ) LIKE '@[%]'
    AND /* exclude exception for customer_name */ TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) <> ''
    AND NOT TRIM(COALESCE(t.customer_name, mapping.customer_name, '')) LIKE '@[%]'
), latest_source_owner_id /* latest mapping based ON source_owner_id */ AS (
  SELECT
    source_owner_id,
    cust_id,
    ROW_NUMBER() OVER (PARTITION BY source_owner_id ORDER BY cust_id, priority_level) AS rn
  FROM {params["com_schema"]}.m_customer_id_mapping
  WHERE
    TRIM(COALESCE(source_owner_id, '')) /* exclude exception for primary_identification_no */ <> ''
)
INSERT INTO {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id
SELECT
  COALESCE(mmc1.cust_id, mmc2.cust_id, mmc3.cust_id, mmc4.cust_id, mmc5.cust_id) AS cust_id,
  mg.client_no, /* 2025/06/12: add another '0' for closed account */
  mg.clean_rule_flag || '00' AS clean_rule_flag,
  mg.primary_identification_type,
  mg.primary_identification_no,
  mg.secondary_identification_type,
  mg.secondary_identification_no,
  mg.primary_identification_type_consolidated,
  mg.primary_identification_no_consolidated,
  mg.secondary_identification_type_consolidated,
  mg.secondary_identification_no_consolidated,
  mg.customer_name,
  mg.date_created,
  mg.date_closed,
  mg.date_change,
  mg.etl_timestamp
FROM {params["com_schema"]}.temp_m_mhbos_m_client_merge AS mg
/*
    cust_id will be get based ON JOIN priority:
        1. primary_identification_no = primary_identification_no
        2. primary_identification_no = secondary_identification_no
        3. secondary_identification_no = primary_identification_no
        4. secondary_identification_no = secondary_identification_no
        5. source_owner_id = source_owner_id
     */
LEFT JOIN latest_primary_identification_no AS mmc1
  ON mg.primary_identification_no_consolidated = mmc1.primary_identification_no
  AND mg.customer_name = mmc1.customer_name
  AND mmc1.rn = 1
LEFT JOIN latest_secondary_identification_no AS mmc2
  ON mg.primary_identification_no_consolidated = mmc2.secondary_identification_no
  AND mg.customer_name = mmc2.customer_name
  AND mmc2.rn = 1
LEFT JOIN latest_primary_identification_no AS mmc3
  ON mg.secondary_identification_no_consolidated = mmc3.primary_identification_no
  AND mg.customer_name = mmc3.customer_name
  AND mmc3.rn = 1
LEFT JOIN latest_secondary_identification_no AS mmc4
  ON mg.secondary_identification_no_consolidated = mmc4.secondary_identification_no
  AND mg.customer_name = mmc4.customer_name
  AND mmc4.rn = 1
LEFT JOIN latest_source_owner_id AS mmc5
  ON mg.client_no = mmc5.source_owner_id AND mmc5.rn = 1
WHERE
  NOT COALESCE(mmc1.cust_id, mmc2.cust_id, mmc3.cust_id, mmc4.cust_id, mmc5.cust_id) IS NULL
""")


spark.sql(rf"""
/*
===================================== GET EXCEPTION RECORDS =====================================
*/
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_exception (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `primary_identification_type_consolidated` VARCHAR(10),
  `primary_identification_no_consolidated` VARCHAR(60),
  `secondary_identification_type_consolidated` VARCHAR(10),
  `secondary_identification_no_consolidated` VARCHAR(60),
  `customer_name_original` VARCHAR(250),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` STRING
)
""")


spark.sql(rf"""
/* Step 3: Get all records WITH exception */
/* Find exception records

Step 1: Find records WHERE cust_id does NOT match WITH mapping
Step 2: Flag exception for Primary ID WITH > 1 distinct customer name
    - Update clean_rule_flag
    - Put Customer name inside '@[]'
Step 3: Get all records WITH exception in
    - primary_identification_no
    - primary_identification_type
    - customer_name
*/
WITH missing_cust_id /* Step 1: Find records WHERE cust_id does NOT match WITH mapping */ AS (
  SELECT
    mg.client_no,
    mg.clean_rule_flag,
    mg.primary_identification_type,
    mg.primary_identification_no,
    mg.secondary_identification_type,
    mg.secondary_identification_no,
    mg.primary_identification_type_consolidated,
    mg.primary_identification_no_consolidated,
    mg.secondary_identification_type_consolidated,
    mg.secondary_identification_no_consolidated,
    mg.customer_name,
    mg.date_created,
    mg.date_closed,
    mg.date_change,
    mg.etl_timestamp
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_merge AS mg
  LEFT JOIN {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id AS mmc
    ON mg.client_no = mmc.client_no
  WHERE
    mmc.cust_id IS NULL
), customer_name_exception /* Step 2: Flag exception for Primary ID WITH > 1 distinct customer name */ AS (
  SELECT
    t0.client_no, /* Update clean_rule_flag */
    t0.clean_rule_flag || IF(t1.primary_identification_no IS NULL, '0', '1') || '0' AS clean_rule_flag,
    t0.primary_identification_type,
    t0.primary_identification_no,
    t0.secondary_identification_type,
    t0.secondary_identification_no,
    t0.primary_identification_type_consolidated,
    t0.primary_identification_no_consolidated,
    t0.secondary_identification_type_consolidated,
    t0.secondary_identification_no_consolidated, /* Backup the original customer_name */
    t0.customer_name AS customer_name_original, /* Put Customer name inside '@[]' */
    IF(
      t1.primary_identification_no IS NULL OR t0.customer_name LIKE '@[%]',
      t0.customer_name,
      '@[' || t0.customer_name || ']'
    ) AS customer_name,
    t0.date_created,
    t0.date_closed,
    t0.date_change,
    t0.etl_timestamp
  FROM missing_cust_id AS t0
  LEFT JOIN (
    /* Find Primary ID WITH > 1 distinct customer name */
    SELECT
      primary_identification_no
    FROM {params["com_schema"]}.temp_m_mhbos_m_client_merge
    /* Only mark customer name exception for active records */
    WHERE
      date_closed IS NULL
    GROUP BY
      primary_identification_no
    HAVING
      COUNT(*) > 1
  ) AS t1
    ON t0.primary_identification_no = t1.primary_identification_no
)
INSERT INTO {params["com_schema"]}.temp_m_mhbos_m_client_exception
SELECT
  NULL AS cust_id,
  client_no,
  clean_rule_flag,
  primary_identification_type,
  primary_identification_no,
  secondary_identification_type,
  secondary_identification_no,
  primary_identification_type_consolidated,
  primary_identification_no_consolidated,
  secondary_identification_type_consolidated,
  secondary_identification_no_consolidated,
  customer_name_original,
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM customer_name_exception
WHERE
  customer_name LIKE '@[%]'
  OR primary_identification_no LIKE '@[%]'
  OR primary_identification_type LIKE '@[%]'
""")


spark.sql(rf"""
/*
===================================== GENERATE NEW CUST_ID =====================================
*/
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_new_cust_id (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `primary_identification_type_consolidated` VARCHAR(10),
  `primary_identification_no_consolidated` VARCHAR(60),
  `secondary_identification_type_consolidated` VARCHAR(10),
  `secondary_identification_no_consolidated` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `date_created` TIMESTAMP,
  `date_closed` TIMESTAMP,
  `date_change` TIMESTAMP,
  `etl_timestamp` STRING
)
""")


spark.sql(rf"""
/* Step 2: Generate New Cust ID */
/* Generate new cust_id
Step 1: Get customer records that
    - Don't have existing cust_id
    - Don't have exceptions
Step 2: Generate New Cust ID

Note: cust_id format
    - Non-Rakuten format: `CUS000000000`.
    - Rakuten format: `RC000000000`.
 */
WITH missing_cust_id /* Step 1: Get customer records that don't have existing cust_id AND don't have exceptions */ AS (
  SELECT
    mg.client_no,
    mg.clean_rule_flag,
    mg.primary_identification_type,
    mg.primary_identification_no,
    mg.secondary_identification_type,
    mg.secondary_identification_no,
    mg.primary_identification_type_consolidated,
    mg.primary_identification_no_consolidated,
    mg.secondary_identification_type_consolidated,
    mg.secondary_identification_no_consolidated,
    mg.customer_name,
    mg.date_created,
    mg.date_closed,
    mg.date_change,
    mg.etl_timestamp,
    t0.max_cust_id_number
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_merge AS mg, (
    /* Minor readability improvement to max cust_id number logic */
    SELECT
      COALESCE(MAX(CAST(SUBSTRING(cust_id, 4) AS INT)), 0) AS max_cust_id_number
    FROM {params["com_schema"]}.m_customer_id_mapping
  ) AS t0
  /* Criteria 1: Records that did NOT find cust_id FROM mapping */
  LEFT JOIN {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id AS matched
    ON mg.client_no = matched.client_no
  /* Criteria 2: Records that did NOT have an exception */
  LEFT JOIN {params["com_schema"]}.temp_m_mhbos_m_client_exception AS exception
    ON mg.client_no = exception.client_no
  WHERE
    matched.cust_id IS NULL AND exception.client_no IS NULL
)
INSERT INTO {params["com_schema"]}.temp_m_mhbos_m_client_new_cust_id
SELECT
  IF(
    date_closed IS NULL,
    'CUS' || LPAD(
      max_cust_id_number + ROW_NUMBER() OVER (ORDER BY GREATEST(COALESCE(date_created, '1900-01-01'), COALESCE(date_change, '1900-01-01')), client_no),
      8,
      0
    ),
    NULL
  ) AS cust_id,
  client_no, /*
     New flag to depict closed account without cust_id assigned -> Do not insert to mapping / curated tables
     */
  clean_rule_flag || '0' || IF(date_closed IS NULL, '0', '1') AS clean_rule_flag,
  primary_identification_type,
  primary_identification_no,
  secondary_identification_type,
  secondary_identification_no,
  primary_identification_type_consolidated,
  primary_identification_no_consolidated,
  secondary_identification_type_consolidated,
  secondary_identification_no_consolidated,
  customer_name,
  date_created,
  date_closed,
  date_change,
  etl_timestamp
FROM missing_cust_id
""")



# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(rf"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_consolidated""")

spark.sql(rf"""
CREATE TABLE {params["com_schema"]}.temp_m_mhbos_m_client_consolidated (
    cust_id STRING
    , client_no STRING
    , clean_rule_flag STRING
    , primary_identification_type STRING
    , primary_identification_no STRING
    , secondary_identification_type STRING
    , secondary_identification_no STRING
    , customer_name STRING
    , customer_name_concatenate STRING
    , client_name STRING
    , client_name1 STRING
    , client_name2 STRING
    , client_name3 STRING
    , mobile_no STRING
    , fax_no STRING
    , tel_no_home STRING
    , tel_no_office STRING
    , date_of_birth TIMESTAMP
    , race STRING
    , email_1 STRING
    , email_2 STRING
    , email_3 STRING
    , email_4 STRING
    , email_5 STRING
    , email_6 STRING
    , email_7 STRING
    , email_8 STRING
    , email_9 STRING
    , email_10 STRING
    , sex STRING
    , addr1 STRING
    , addr2 STRING
    , addr3 STRING
    , addr4 STRING
    , postcode STRING
    , city STRING
    , state STRING
    , perm_addr1 STRING
    , perm_addr2 STRING
    , perm_addr3 STRING
    , perm_addr4 STRING
    , perm_postcode STRING
    , perm_city STRING
    , perm_state STRING
    , noms_ind STRING
    , cleaned_nominees_name STRING
    , principal_name STRING
    , intermediary_name STRING
    , beneficiary_name STRING
    , nominees_type STRING
    , pledged_securities_flag STRING
    , id_type STRING
    , ic_no_new STRING
    , ic_no_old STRING
    , secondary_id_type STRING
    , secondary_id_no STRING
    , source_client_name STRING
    , source_client_name1 STRING
    , source_client_name2 STRING
    , source_client_name3 STRING
    , source_mobile_no STRING
    , source_fax_no STRING
    , source_tel_no_home STRING
    , source_tel_no_office STRING
    , source_date_of_birth TIMESTAMP
    , source_race STRING
    , source_email STRING
    , source_sex STRING
    , client_group STRING
    , cds_acc_no STRING
    , tdr_code STRING
    , client_type STRING
    , margin STRING
    , last_margin_date TIMESTAMP
    , int_rate STRING
    , auto_ded STRING
    , despatch_mode STRING
    , copies STRING
    , prohibit_trade STRING
    , custody_status STRING
    , country STRING
    , margin_limit STRING
    , margin_pct STRING
    , rollover_rate STRING
    , form_completed STRING
    , last_tran_date TIMESTAMP
    , ytd_bvalue STRING
    , ytd_svalue STRING
    , ytd_brokerage STRING
    , os_led_bal STRING
    , title STRING
    , date_created TIMESTAMP
    , stop_payt STRING
    , acc_payee STRING
    , category STRING
    , auto_contra STRING
    , pnl_acc_no STRING
    , cr_limit STRING
    , trust_bal STRING
    , avg_ind STRING
    , remarks STRING
    , contact_person STRING
    , date_closed TIMESTAMP
    , grace_period STRING
    , acct_type STRING
    , assoc_ind STRING
    , short_sell_ind STRING
    , short_name STRING
    , mesdaq_pctlmt STRING
    , date_change TIMESTAMP
    , resi_code STRING
    , charge_int STRING
    , bdebt STRING
    , assets STRING
    , liabilities STRING
    , income STRING
    , expenses STRING
    , bdebt_his_ind STRING
    , rel_ac1 STRING
    , rel_ac2 STRING
    , rel_ac3 STRING
    , rel_ac4 STRING
    , occupation STRING
    , margin_int STRING
    , lst_led_no STRING
    , cur_led_no STRING
    , remarks2 STRING
    , acc_type STRING
    , mas_accno STRING
    , legal STRING
    , sell_limit STRING
    , brk_rate STRING
    , brokerage_type STRING
    , cds_acc_no1 STRING
    , remarks1 STRING
    , payment_bank_code STRING
    , noms STRING
    , dms_date TIMESTAMP
    , violation_date TIMESTAMP
    , mcd_branch STRING
    , home_branch STRING
    , eaf_code STRING
    , call_warrant STRING
    , user_id STRING
    , credit_int_rate STRING
    , min_eligible_amt STRING
    , intraday_flag STRING
    , intraday_rate STRING
    , cta_weight STRING
    , sta_weight STRING
    , bo_cds_acc_no STRING
    , ecos_form STRING
    , custodian_no STRING
    , prin_acc STRING
    , armada_type STRING
    , old_authorisee STRING
    , etrade_rate STRING
    , etf STRING
    , cstamp_client_exempt STRING
    , main_branch STRING
    , prev_client_no STRING
    , web_eds STRING
    , place STRING
    , excl_tdr_deduct STRING
    , excl_auto_susp STRING
    , trust_flag STRING
    , mgn_new_int_rate STRING
    , counter_concentration STRING
    , auto_trust STRING
    , margin_pct2 STRING
    , df_flag STRING
    , mgn_curr_int_rate STRING
    , product_type STRING
    , web_ecos STRING
    , xeye_clt_grp STRING
    , bursa_violation_date TIMESTAMP
    , brokerage_type_etrade STRING
    , brokerage_type_odd_lot STRING
    , omnibus STRING
    , cg_tdr_code STRING
    , limit_foreign STRING
    , limit_bursa STRING
    , brokerage_type_intraday STRING
    , brokerage_type_intraday_etrade STRING
    , bursa_violation_date1 TIMESTAMP
    , cif_no STRING
    , brokerage_type_foreign STRING
    , soft_copy STRING
    , exclude_rollover STRING
    , account_status STRING
    , w8ben STRING
    , ic_no_rel1 STRING
    , ic_no_rel2 STRING
    , ic_no_rel3 STRING
    , ic_no_rel4 STRING
    , ic_no_rel5 STRING
    , rel1 STRING
    , rel2 STRING
    , rel3 STRING
    , rel4 STRING
    , rel5 STRING
    , brokerage_type_etrade_b STRING
    , brokerage_type_odd_lot_b STRING
    , brokerage_type_b STRING
    , brokerage_type_foreign_b STRING
    , brokerage_type_intraday_b STRING
    , brokerage_type_intraday_etrade_b STRING
    , brokerage_type_etrade_s STRING
    , brokerage_type_odd_lot_s STRING
    , brokerage_type_s STRING
    , brokerage_type_foreign_s STRING
    , brokerage_type_intraday_s STRING
    , brokerage_type_intraday_etrade_s STRING
    , no_free_trade STRING
    , sms STRING
    , mobile_prefix STRING
    , foreign_curr_set STRING
    , num_free_trade STRING
    , etrader_type STRING
    , check_limit STRING
    , auto_margin STRING
    , margin_client_no STRING
    , dup_despatch_mode STRING
    , risk STRING
    , exclude_trader_limit STRING
    , sett_mode_date_change TIMESTAMP
    , pick_up_fee_pct STRING
    , e_payment STRING
    , mgn_new_int_rate2 STRING
    , fund_cost_type STRING
    , check_share STRING
    , citibank_changes STRING
    , citibank_charges STRING
    , cq_market STRING
    , exclude_margin_pro_rate STRING
    , brokerage_type_cash_b STRING
    , brokerage_type_etrade_cash_b STRING
    , clt_consent STRING
    , consent_start_date TIMESTAMP
    , portfolio STRING
    , expiry_date TIMESTAMP
    , intraday_auto_contra_option STRING
    , dcf_limit STRING
    , brokerage_type_etb STRING
    , mgn_force_sell_pct STRING
    , mgn_tenure STRING
    , mgn_expiry_date TIMESTAMP
    , loss_gl_acc_no STRING
    , portfolio_date TIMESTAMP
    , day_prior_temp_susp STRING
    , day_prior_perm_susp STRING
    , gst_code STRING
    , match_price_decimal_local STRING
    , match_price_decimal_foreign STRING
    , primary_id_expiry_date TIMESTAMP
    , secondary_id_expiry_date TIMESTAMP
    , mgn_int_tdr_spread_pct STRING
    , mgn_base_int_rate STRING
    , mgn_int_tdr_share STRING
    , islamic_flag STRING
    , mcd_resident_flag STRING
    , chq_charges_flag STRING
    , chq_charges_tdr_pct STRING
    , brokerage_type_foreign_etrade STRING
    , brokerage_type_foreign_etrade_b STRING
    , brokerage_type_foreign_etrade_s STRING
    , grp_exch_code STRING
    , bdebt_ras STRING
    , twse_declaration STRING
    , joint_acc_amt STRING
    , high_risk_market STRING
    , brokerage_type_leap_normal STRING
    , brokerage_type_leap_etrade STRING
    , type_of_account STRING
    ,dl_record_status       VARCHAR(10)
    ,dl_record_created_date TIMESTAMP
    ,dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")


# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(rf"""
WITH unique_customer AS (
  SELECT
    *
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id
  UNION ALL
  SELECT
    *
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_new_cust_id
), last_updated_account AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY primary_identification_no, customer_name ORDER BY CASE WHEN type_of_account = 'F' THEN 1 ELSE 0 END /* set account_type = 'F' rows as lower priority */, GREATEST(COALESCE(date_created, '1900-01-01'), COALESCE(date_change, '1900-01-01')) DESC) AS rn
  FROM {params["com_schema"]}.t_mhbos_m_client
  WHERE
    TO_DATE(dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
)
INSERT INTO TABLE {params["com_schema"]}.temp_m_mhbos_m_client_consolidated
SELECT
  cust.cust_id,
  cust.client_no,
  cust.clean_rule_flag,
  cust.primary_identification_type,
  cust.primary_identification_no,
  cust.secondary_identification_type,
  cust.secondary_identification_no,
  cust.customer_name,
  t.customer_name_concatenate,
  t.client_name,
  t.client_name1,
  t.client_name2,
  t.client_name3,
  t.mobile_no,
  t.fax_no,
  t.tel_no_home,
  t.tel_no_office,
  t.date_of_birth,
  t.race,
  t.email_1,
  t.email_2,
  t.email_3,
  t.email_4,
  t.email_5,
  t.email_6,
  t.email_7,
  t.email_8,
  t.email_9,
  t.email_10,
  t.sex,
  t.addr1,
  t.addr2,
  t.addr3,
  t.addr4,
  t.postcode,
  t.city,
  t.state,
  t.perm_addr1,
  t.perm_addr2,
  t.perm_addr3,
  t.perm_addr4,
  t.perm_postcode,
  t.perm_city,
  t.perm_state,
  t.noms_ind,
  t.cleaned_nominees_name,
  t.principal_name,
  t.intermediary_name,
  t.beneficiary_name,
  t.nominees_type,
  t.pledged_securities_flag,
  t.id_type,
  t.ic_no_new,
  t.ic_no_old,
  t.secondary_id_type,
  t.secondary_id_no,
  t.source_client_name,
  t.source_client_name1,
  t.source_client_name2,
  t.source_client_name3,
  t.source_mobile_no,
  t.source_fax_no,
  t.source_tel_no_home,
  t.source_tel_no_office,
  t.source_date_of_birth,
  t.source_race,
  t.source_email,
  t.source_sex,
  t.client_group,
  t.cds_acc_no,
  t.tdr_code,
  t.client_type,
  t.margin,
  t.last_margin_date,
  t.int_rate,
  t.auto_ded,
  t.despatch_mode,
  t.copies,
  t.prohibit_trade,
  t.custody_status,
  t.country,
  t.margin_limit,
  t.margin_pct,
  t.rollover_rate,
  t.form_completed,
  t.last_tran_date,
  t.ytd_bvalue,
  t.ytd_svalue,
  t.ytd_brokerage,
  t.os_led_bal,
  t.title,
  t.date_created,
  t.stop_payt,
  t.acc_payee,
  t.category,
  t.auto_contra,
  t.pnl_acc_no,
  t.cr_limit,
  t.trust_bal,
  t.avg_ind,
  t.remarks,
  t.contact_person,
  t.date_closed,
  t.grace_period,
  t.acct_type,
  t.assoc_ind,
  t.short_sell_ind,
  t.short_name,
  t.mesdaq_pctlmt,
  t.date_change,
  t.resi_code,
  t.charge_int,
  t.bdebt,
  t.assets,
  t.liabilities,
  t.income,
  t.expenses,
  t.bdebt_his_ind,
  t.rel_ac1,
  t.rel_ac2,
  t.rel_ac3,
  t.rel_ac4,
  t.occupation,
  t.margin_int,
  t.lst_led_no,
  t.cur_led_no,
  t.remarks2,
  t.acc_type,
  t.mas_accno,
  t.legal,
  t.sell_limit,
  t.brk_rate,
  t.brokerage_type,
  t.cds_acc_no1,
  t.remarks1,
  t.payment_bank_code,
  t.noms,
  t.dms_date,
  t.violation_date,
  t.mcd_branch,
  t.home_branch,
  t.eaf_code,
  t.call_warrant,
  t.user_id,
  t.credit_int_rate,
  t.min_eligible_amt,
  t.intraday_flag,
  t.intraday_rate,
  t.cta_weight,
  t.sta_weight,
  t.bo_cds_acc_no,
  t.ecos_form,
  t.custodian_no,
  t.prin_acc,
  t.armada_type,
  t.old_authorisee,
  t.etrade_rate,
  t.etf,
  t.cstamp_client_exempt,
  t.main_branch,
  t.prev_client_no,
  t.web_eds,
  t.place,
  t.excl_tdr_deduct,
  t.excl_auto_susp,
  t.trust_flag,
  t.mgn_new_int_rate,
  t.counter_concentration,
  t.auto_trust,
  t.margin_pct2,
  t.df_flag,
  t.mgn_curr_int_rate,
  t.product_type,
  t.web_ecos,
  t.xeye_clt_grp,
  t.bursa_violation_date,
  t.brokerage_type_etrade,
  t.brokerage_type_odd_lot,
  t.omnibus,
  t.cg_tdr_code,
  t.limit_foreign,
  t.limit_bursa,
  t.brokerage_type_intraday,
  t.brokerage_type_intraday_etrade,
  t.bursa_violation_date1,
  t.cif_no,
  t.brokerage_type_foreign,
  t.soft_copy,
  t.exclude_rollover,
  t.account_status,
  t.w8ben,
  t.ic_no_rel1,
  t.ic_no_rel2,
  t.ic_no_rel3,
  t.ic_no_rel4,
  t.ic_no_rel5,
  t.rel1,
  t.rel2,
  t.rel3,
  t.rel4,
  t.rel5,
  t.brokerage_type_etrade_b,
  t.brokerage_type_odd_lot_b,
  t.brokerage_type_b,
  t.brokerage_type_foreign_b,
  t.brokerage_type_intraday_b,
  t.brokerage_type_intraday_etrade_b,
  t.brokerage_type_etrade_s,
  t.brokerage_type_odd_lot_s,
  t.brokerage_type_s,
  t.brokerage_type_foreign_s,
  t.brokerage_type_intraday_s,
  t.brokerage_type_intraday_etrade_s,
  t.no_free_trade,
  t.sms,
  t.mobile_prefix,
  t.foreign_curr_set,
  t.num_free_trade,
  t.etrader_type,
  t.check_limit,
  t.auto_margin,
  t.margin_client_no,
  t.dup_despatch_mode,
  t.risk,
  t.exclude_trader_limit,
  t.sett_mode_date_change,
  t.pick_up_fee_pct,
  t.e_payment,
  t.mgn_new_int_rate2,
  t.fund_cost_type,
  t.check_share,
  t.citibank_changes,
  t.citibank_charges,
  t.cq_market,
  t.exclude_margin_pro_rate,
  t.brokerage_type_cash_b,
  t.brokerage_type_etrade_cash_b,
  t.clt_consent,
  t.consent_start_date,
  t.portfolio,
  t.expiry_date,
  t.intraday_auto_contra_option,
  t.dcf_limit,
  t.brokerage_type_etb,
  t.mgn_force_sell_pct,
  t.mgn_tenure,
  t.mgn_expiry_date,
  t.loss_gl_acc_no,
  t.portfolio_date,
  t.day_prior_temp_susp,
  t.day_prior_perm_susp,
  t.gst_code,
  t.match_price_decimal_local,
  t.match_price_decimal_foreign,
  t.primary_id_expiry_date,
  t.secondary_id_expiry_date,
  t.mgn_int_tdr_spread_pct,
  t.mgn_base_int_rate,
  t.mgn_int_tdr_share,
  t.islamic_flag,
  t.mcd_resident_flag,
  t.chq_charges_flag,
  t.chq_charges_tdr_pct,
  t.brokerage_type_foreign_etrade,
  t.brokerage_type_foreign_etrade_b,
  t.brokerage_type_foreign_etrade_s,
  t.grp_exch_code,
  t.bdebt_ras,
  t.twse_declaration,
  t.joint_acc_amt,
  t.high_risk_market,
  t.brokerage_type_leap_normal,
  t.brokerage_type_leap_etrade,
  t.type_of_account,
  t.dl_record_status,
  t.dl_record_created_date,
  t.dl_record_updated_date
FROM unique_customer AS cust
JOIN last_updated_account AS t
  ON cust.primary_identification_no = t.primary_identification_no
  AND cust.customer_name = t.customer_name
WHERE
  t.rn = 1
UNION ALL
SELECT
  cust.cust_id,
  cust.client_no,
  cust.clean_rule_flag,
  cust.primary_identification_type,
  cust.primary_identification_no,
  cust.secondary_identification_type,
  cust.secondary_identification_no,
  cust.customer_name,
  t.customer_name_concatenate,
  t.client_name,
  t.client_name1,
  t.client_name2,
  t.client_name3,
  t.mobile_no,
  t.fax_no,
  t.tel_no_home,
  t.tel_no_office,
  t.date_of_birth,
  t.race,
  t.email_1,
  t.email_2,
  t.email_3,
  t.email_4,
  t.email_5,
  t.email_6,
  t.email_7,
  t.email_8,
  t.email_9,
  t.email_10,
  t.sex,
  t.addr1,
  t.addr2,
  t.addr3,
  t.addr4,
  t.postcode,
  t.city,
  t.state,
  t.perm_addr1,
  t.perm_addr2,
  t.perm_addr3,
  t.perm_addr4,
  t.perm_postcode,
  t.perm_city,
  t.perm_state,
  t.noms_ind,
  t.cleaned_nominees_name,
  t.principal_name,
  t.intermediary_name,
  t.beneficiary_name,
  t.nominees_type,
  t.pledged_securities_flag,
  t.id_type,
  t.ic_no_new,
  t.ic_no_old,
  t.secondary_id_type,
  t.secondary_id_no,
  t.source_client_name,
  t.source_client_name1,
  t.source_client_name2,
  t.source_client_name3,
  t.source_mobile_no,
  t.source_fax_no,
  t.source_tel_no_home,
  t.source_tel_no_office,
  t.source_date_of_birth,
  t.source_race,
  t.source_email,
  t.source_sex,
  t.client_group,
  t.cds_acc_no,
  t.tdr_code,
  t.client_type,
  t.margin,
  t.last_margin_date,
  t.int_rate,
  t.auto_ded,
  t.despatch_mode,
  t.copies,
  t.prohibit_trade,
  t.custody_status,
  t.country,
  t.margin_limit,
  t.margin_pct,
  t.rollover_rate,
  t.form_completed,
  t.last_tran_date,
  t.ytd_bvalue,
  t.ytd_svalue,
  t.ytd_brokerage,
  t.os_led_bal,
  t.title,
  t.date_created,
  t.stop_payt,
  t.acc_payee,
  t.category,
  t.auto_contra,
  t.pnl_acc_no,
  t.cr_limit,
  t.trust_bal,
  t.avg_ind,
  t.remarks,
  t.contact_person,
  t.date_closed,
  t.grace_period,
  t.acct_type,
  t.assoc_ind,
  t.short_sell_ind,
  t.short_name,
  t.mesdaq_pctlmt,
  t.date_change,
  t.resi_code,
  t.charge_int,
  t.bdebt,
  t.assets,
  t.liabilities,
  t.income,
  t.expenses,
  t.bdebt_his_ind,
  t.rel_ac1,
  t.rel_ac2,
  t.rel_ac3,
  t.rel_ac4,
  t.occupation,
  t.margin_int,
  t.lst_led_no,
  t.cur_led_no,
  t.remarks2,
  t.acc_type,
  t.mas_accno,
  t.legal,
  t.sell_limit,
  t.brk_rate,
  t.brokerage_type,
  t.cds_acc_no1,
  t.remarks1,
  t.payment_bank_code,
  t.noms,
  t.dms_date,
  t.violation_date,
  t.mcd_branch,
  t.home_branch,
  t.eaf_code,
  t.call_warrant,
  t.user_id,
  t.credit_int_rate,
  t.min_eligible_amt,
  t.intraday_flag,
  t.intraday_rate,
  t.cta_weight,
  t.sta_weight,
  t.bo_cds_acc_no,
  t.ecos_form,
  t.custodian_no,
  t.prin_acc,
  t.armada_type,
  t.old_authorisee,
  t.etrade_rate,
  t.etf,
  t.cstamp_client_exempt,
  t.main_branch,
  t.prev_client_no,
  t.web_eds,
  t.place,
  t.excl_tdr_deduct,
  t.excl_auto_susp,
  t.trust_flag,
  t.mgn_new_int_rate,
  t.counter_concentration,
  t.auto_trust,
  t.margin_pct2,
  t.df_flag,
  t.mgn_curr_int_rate,
  t.product_type,
  t.web_ecos,
  t.xeye_clt_grp,
  t.bursa_violation_date,
  t.brokerage_type_etrade,
  t.brokerage_type_odd_lot,
  t.omnibus,
  t.cg_tdr_code,
  t.limit_foreign,
  t.limit_bursa,
  t.brokerage_type_intraday,
  t.brokerage_type_intraday_etrade,
  t.bursa_violation_date1,
  t.cif_no,
  t.brokerage_type_foreign,
  t.soft_copy,
  t.exclude_rollover,
  t.account_status,
  t.w8ben,
  t.ic_no_rel1,
  t.ic_no_rel2,
  t.ic_no_rel3,
  t.ic_no_rel4,
  t.ic_no_rel5,
  t.rel1,
  t.rel2,
  t.rel3,
  t.rel4,
  t.rel5,
  t.brokerage_type_etrade_b,
  t.brokerage_type_odd_lot_b,
  t.brokerage_type_b,
  t.brokerage_type_foreign_b,
  t.brokerage_type_intraday_b,
  t.brokerage_type_intraday_etrade_b,
  t.brokerage_type_etrade_s,
  t.brokerage_type_odd_lot_s,
  t.brokerage_type_s,
  t.brokerage_type_foreign_s,
  t.brokerage_type_intraday_s,
  t.brokerage_type_intraday_etrade_s,
  t.no_free_trade,
  t.sms,
  t.mobile_prefix,
  t.foreign_curr_set,
  t.num_free_trade,
  t.etrader_type,
  t.check_limit,
  t.auto_margin,
  t.margin_client_no,
  t.dup_despatch_mode,
  t.risk,
  t.exclude_trader_limit,
  t.sett_mode_date_change,
  t.pick_up_fee_pct,
  t.e_payment,
  t.mgn_new_int_rate2,
  t.fund_cost_type,
  t.check_share,
  t.citibank_changes,
  t.citibank_charges,
  t.cq_market,
  t.exclude_margin_pro_rate,
  t.brokerage_type_cash_b,
  t.brokerage_type_etrade_cash_b,
  t.clt_consent,
  t.consent_start_date,
  t.portfolio,
  t.expiry_date,
  t.intraday_auto_contra_option,
  t.dcf_limit,
  t.brokerage_type_etb,
  t.mgn_force_sell_pct,
  t.mgn_tenure,
  t.mgn_expiry_date,
  t.loss_gl_acc_no,
  t.portfolio_date,
  t.day_prior_temp_susp,
  t.day_prior_perm_susp,
  t.gst_code,
  t.match_price_decimal_local,
  t.match_price_decimal_foreign,
  t.primary_id_expiry_date,
  t.secondary_id_expiry_date,
  t.mgn_int_tdr_spread_pct,
  t.mgn_base_int_rate,
  t.mgn_int_tdr_share,
  t.islamic_flag,
  t.mcd_resident_flag,
  t.chq_charges_flag,
  t.chq_charges_tdr_pct,
  t.brokerage_type_foreign_etrade,
  t.brokerage_type_foreign_etrade_b,
  t.brokerage_type_foreign_etrade_s,
  t.grp_exch_code,
  t.bdebt_ras,
  t.twse_declaration,
  t.joint_acc_amt,
  t.high_risk_market,
  t.brokerage_type_leap_normal,
  t.brokerage_type_leap_etrade,
  t.type_of_account,
  t.dl_record_status,
  t.dl_record_created_date,
  t.dl_record_updated_date
FROM {params["com_schema"]}.temp_m_mhbos_m_client_exception AS cust
JOIN last_updated_account AS t
  ON cust.primary_identification_no = t.primary_identification_no
  AND cust.customer_name_original = t.customer_name
WHERE
  t.rn = 1


"""
)


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(rf"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_updated""")

spark.sql(rf"""
CREATE TABLE {params["com_schema"]}.temp_m_mhbos_m_client_updated (
    cust_id STRING
    , client_no STRING
    , clean_rule_flag STRING
    , primary_identification_type STRING
    , primary_identification_no STRING
    , secondary_identification_type STRING
    , secondary_identification_no STRING
    , customer_name STRING
    , customer_name_concatenate STRING
    , client_name STRING
    , client_name1 STRING
    , client_name2 STRING
    , client_name3 STRING
    , mobile_no STRING
    , fax_no STRING
    , tel_no_home STRING
    , tel_no_office STRING
    , date_of_birth TIMESTAMP
    , race STRING
    , email_1 STRING
    , email_2 STRING
    , email_3 STRING
    , email_4 STRING
    , email_5 STRING
    , email_6 STRING
    , email_7 STRING
    , email_8 STRING
    , email_9 STRING
    , email_10 STRING
    , sex STRING
    , addr1 STRING
    , addr2 STRING
    , addr3 STRING
    , addr4 STRING
    , postcode STRING
    , city STRING
    , state STRING
    , perm_addr1 STRING
    , perm_addr2 STRING
    , perm_addr3 STRING
    , perm_addr4 STRING
    , perm_postcode STRING
    , perm_city STRING
    , perm_state STRING
    , noms_ind STRING
    , cleaned_nominees_name STRING
    , principal_name STRING
    , intermediary_name STRING
    , beneficiary_name STRING
    , nominees_type STRING
    , pledged_securities_flag STRING
    , id_type STRING
    , ic_no_new STRING
    , ic_no_old STRING
    , secondary_id_type STRING
    , secondary_id_no STRING
    , source_client_name STRING
    , source_client_name1 STRING
    , source_client_name2 STRING
    , source_client_name3 STRING
    , source_mobile_no STRING
    , source_fax_no STRING
    , source_tel_no_home STRING
    , source_tel_no_office STRING
    , source_date_of_birth TIMESTAMP
    , source_race STRING
    , source_email STRING
    , source_sex STRING
    , client_group STRING
    , cds_acc_no STRING
    , tdr_code STRING
    , client_type STRING
    , margin STRING
    , last_margin_date TIMESTAMP
    , int_rate STRING
    , auto_ded STRING
    , despatch_mode STRING
    , copies STRING
    , prohibit_trade STRING
    , custody_status STRING
    , country STRING
    , margin_limit STRING
    , margin_pct STRING
    , rollover_rate STRING
    , form_completed STRING
    , last_tran_date TIMESTAMP
    , ytd_bvalue STRING
    , ytd_svalue STRING
    , ytd_brokerage STRING
    , os_led_bal STRING
    , title STRING
    , date_created TIMESTAMP
    , stop_payt STRING
    , acc_payee STRING
    , category STRING
    , auto_contra STRING
    , pnl_acc_no STRING
    , cr_limit STRING
    , trust_bal STRING
    , avg_ind STRING
    , remarks STRING
    , contact_person STRING
    , date_closed TIMESTAMP
    , grace_period STRING
    , acct_type STRING
    , assoc_ind STRING
    , short_sell_ind STRING
    , short_name STRING
    , mesdaq_pctlmt STRING
    , date_change TIMESTAMP
    , resi_code STRING
    , charge_int STRING
    , bdebt STRING
    , assets STRING
    , liabilities STRING
    , income STRING
    , expenses STRING
    , bdebt_his_ind STRING
    , rel_ac1 STRING
    , rel_ac2 STRING
    , rel_ac3 STRING
    , rel_ac4 STRING
    , occupation STRING
    , margin_int STRING
    , lst_led_no STRING
    , cur_led_no STRING
    , remarks2 STRING
    , acc_type STRING
    , mas_accno STRING
    , legal STRING
    , sell_limit STRING
    , brk_rate STRING
    , brokerage_type STRING
    , cds_acc_no1 STRING
    , remarks1 STRING
    , payment_bank_code STRING
    , noms STRING
    , dms_date TIMESTAMP
    , violation_date TIMESTAMP
    , mcd_branch STRING
    , home_branch STRING
    , eaf_code STRING
    , call_warrant STRING
    , user_id STRING
    , credit_int_rate STRING
    , min_eligible_amt STRING
    , intraday_flag STRING
    , intraday_rate STRING
    , cta_weight STRING
    , sta_weight STRING
    , bo_cds_acc_no STRING
    , ecos_form STRING
    , custodian_no STRING
    , prin_acc STRING
    , armada_type STRING
    , old_authorisee STRING
    , etrade_rate STRING
    , etf STRING
    , cstamp_client_exempt STRING
    , main_branch STRING
    , prev_client_no STRING
    , web_eds STRING
    , place STRING
    , excl_tdr_deduct STRING
    , excl_auto_susp STRING
    , trust_flag STRING
    , mgn_new_int_rate STRING
    , counter_concentration STRING
    , auto_trust STRING
    , margin_pct2 STRING
    , df_flag STRING
    , mgn_curr_int_rate STRING
    , product_type STRING
    , web_ecos STRING
    , xeye_clt_grp STRING
    , bursa_violation_date TIMESTAMP
    , brokerage_type_etrade STRING
    , brokerage_type_odd_lot STRING
    , omnibus STRING
    , cg_tdr_code STRING
    , limit_foreign STRING
    , limit_bursa STRING
    , brokerage_type_intraday STRING
    , brokerage_type_intraday_etrade STRING
    , bursa_violation_date1 TIMESTAMP
    , cif_no STRING
    , brokerage_type_foreign STRING
    , soft_copy STRING
    , exclude_rollover STRING
    , account_status STRING
    , w8ben STRING
    , ic_no_rel1 STRING
    , ic_no_rel2 STRING
    , ic_no_rel3 STRING
    , ic_no_rel4 STRING
    , ic_no_rel5 STRING
    , rel1 STRING
    , rel2 STRING
    , rel3 STRING
    , rel4 STRING
    , rel5 STRING
    , brokerage_type_etrade_b STRING
    , brokerage_type_odd_lot_b STRING
    , brokerage_type_b STRING
    , brokerage_type_foreign_b STRING
    , brokerage_type_intraday_b STRING
    , brokerage_type_intraday_etrade_b STRING
    , brokerage_type_etrade_s STRING
    , brokerage_type_odd_lot_s STRING
    , brokerage_type_s STRING
    , brokerage_type_foreign_s STRING
    , brokerage_type_intraday_s STRING
    , brokerage_type_intraday_etrade_s STRING
    , no_free_trade STRING
    , sms STRING
    , mobile_prefix STRING
    , foreign_curr_set STRING
    , num_free_trade STRING
    , etrader_type STRING
    , check_limit STRING
    , auto_margin STRING
    , margin_client_no STRING
    , dup_despatch_mode STRING
    , risk STRING
    , exclude_trader_limit STRING
    , sett_mode_date_change TIMESTAMP
    , pick_up_fee_pct STRING
    , e_payment STRING
    , mgn_new_int_rate2 STRING
    , fund_cost_type STRING
    , check_share STRING
    , citibank_changes STRING
    , citibank_charges STRING
    , cq_market STRING
    , exclude_margin_pro_rate STRING
    , brokerage_type_cash_b STRING
    , brokerage_type_etrade_cash_b STRING
    , clt_consent STRING
    , consent_start_date TIMESTAMP
    , portfolio STRING
    , expiry_date TIMESTAMP
    , intraday_auto_contra_option STRING
    , dcf_limit STRING
    , brokerage_type_etb STRING
    , mgn_force_sell_pct STRING
    , mgn_tenure STRING
    , mgn_expiry_date TIMESTAMP
    , loss_gl_acc_no STRING
    , portfolio_date TIMESTAMP
    , day_prior_temp_susp STRING
    , day_prior_perm_susp STRING
    , gst_code STRING
    , match_price_decimal_local STRING
    , match_price_decimal_foreign STRING
    , primary_id_expiry_date TIMESTAMP
    , secondary_id_expiry_date TIMESTAMP
    , mgn_int_tdr_spread_pct STRING
    , mgn_base_int_rate STRING
    , mgn_int_tdr_share STRING
    , islamic_flag STRING
    , mcd_resident_flag STRING
    , chq_charges_flag STRING
    , chq_charges_tdr_pct STRING
    , brokerage_type_foreign_etrade STRING
    , brokerage_type_foreign_etrade_b STRING
    , brokerage_type_foreign_etrade_s STRING
    , grp_exch_code STRING
    , bdebt_ras STRING
    , twse_declaration STRING
    , joint_acc_amt STRING
    , high_risk_market STRING
    , brokerage_type_leap_normal STRING
    , brokerage_type_leap_etrade STRING
    , type_of_account STRING
    ,dl_record_status       VARCHAR(10)
    ,dl_record_created_date TIMESTAMP
    ,dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(rf"""
INSERT INTO TABLE {params["com_schema"]}.temp_m_mhbos_m_client_updated
SELECT
    cust_id
    , client_no
    , clean_rule_flag
    , primary_identification_type
    , primary_identification_no
    , secondary_identification_type
    , secondary_identification_no
    , customer_name
    , customer_name_concatenate
    , client_name
    , client_name1
    , client_name2
    , client_name3
    , mobile_no
    , fax_no
    , tel_no_home
    , tel_no_office
    , date_of_birth
    , race
    , email_1
    , email_2
    , email_3
    , email_4
    , email_5
    , email_6
    , email_7
    , email_8
    , email_9
    , email_10
    , sex
    , addr1
    , addr2
    , addr3
    , addr4
    , postcode
    , city
    , state
    , perm_addr1
    , perm_addr2
    , perm_addr3
    , perm_addr4
    , perm_postcode
    , perm_city
    , perm_state
    , noms_ind
    , cleaned_nominees_name
    , principal_name
    , intermediary_name
    , beneficiary_name
    , nominees_type
    , pledged_securities_flag
    , id_type
    , ic_no_new
    , ic_no_old
    , secondary_id_type
    , secondary_id_no
    , source_client_name
    , source_client_name1
    , source_client_name2
    , source_client_name3
    , source_mobile_no
    , source_fax_no
    , source_tel_no_home
    , source_tel_no_office
    , source_date_of_birth
    , source_race
    , source_email
    , source_sex
    , client_group
    , cds_acc_no
    , tdr_code
    , client_type
    , margin
    , last_margin_date
    , int_rate
    , auto_ded
    , despatch_mode
    , copies
    , prohibit_trade
    , custody_status
    , country
    , margin_limit
    , margin_pct
    , rollover_rate
    , form_completed
    , last_tran_date
    , ytd_bvalue
    , ytd_svalue
    , ytd_brokerage
    , os_led_bal
    , title
    , date_created
    , stop_payt
    , acc_payee
    , category
    , auto_contra
    , pnl_acc_no
    , cr_limit
    , trust_bal
    , avg_ind
    , remarks
    , contact_person
    , date_closed
    , grace_period
    , acct_type
    , assoc_ind
    , short_sell_ind
    , short_name
    , mesdaq_pctlmt
    , date_change
    , resi_code
    , charge_int
    , bdebt
    , assets
    , liabilities
    , income
    , expenses
    , bdebt_his_ind
    , rel_ac1
    , rel_ac2
    , rel_ac3
    , rel_ac4
    , occupation
    , margin_int
    , lst_led_no
    , cur_led_no
    , remarks2
    , acc_type
    , mas_accno
    , legal
    , sell_limit
    , brk_rate
    , brokerage_type
    , cds_acc_no1
    , remarks1
    , payment_bank_code
    , noms
    , dms_date
    , violation_date
    , mcd_branch
    , home_branch
    , eaf_code
    , call_warrant
    , user_id
    , credit_int_rate
    , min_eligible_amt
    , intraday_flag
    , intraday_rate
    , cta_weight
    , sta_weight
    , bo_cds_acc_no
    , ecos_form
    , custodian_no
    , prin_acc
    , armada_type
    , old_authorisee
    , etrade_rate
    , etf
    , cstamp_client_exempt
    , main_branch
    , prev_client_no
    , web_eds
    , place
    , excl_tdr_deduct
    , excl_auto_susp
    , trust_flag
    , mgn_new_int_rate
    , counter_concentration
    , auto_trust
    , margin_pct2
    , df_flag
    , mgn_curr_int_rate
    , product_type
    , web_ecos
    , xeye_clt_grp
    , bursa_violation_date
    , brokerage_type_etrade
    , brokerage_type_odd_lot
    , omnibus
    , cg_tdr_code
    , limit_foreign
    , limit_bursa
    , brokerage_type_intraday
    , brokerage_type_intraday_etrade
    , bursa_violation_date1
    , cif_no
    , brokerage_type_foreign
    , soft_copy
    , exclude_rollover
    , account_status
    , w8ben
    , ic_no_rel1
    , ic_no_rel2
    , ic_no_rel3
    , ic_no_rel4
    , ic_no_rel5
    , rel1
    , rel2
    , rel3
    , rel4
    , rel5
    , brokerage_type_etrade_b
    , brokerage_type_odd_lot_b
    , brokerage_type_b
    , brokerage_type_foreign_b
    , brokerage_type_intraday_b
    , brokerage_type_intraday_etrade_b
    , brokerage_type_etrade_s
    , brokerage_type_odd_lot_s
    , brokerage_type_s
    , brokerage_type_foreign_s
    , brokerage_type_intraday_s
    , brokerage_type_intraday_etrade_s
    , no_free_trade
    , sms
    , mobile_prefix
    , foreign_curr_set
    , num_free_trade
    , etrader_type
    , check_limit
    , auto_margin
    , margin_client_no
    , dup_despatch_mode
    , risk
    , exclude_trader_limit
    , sett_mode_date_change
    , pick_up_fee_pct
    , e_payment
    , mgn_new_int_rate2
    , fund_cost_type
    , check_share
    , citibank_changes
    , citibank_charges
    , cq_market
    , exclude_margin_pro_rate
    , brokerage_type_cash_b
    , brokerage_type_etrade_cash_b
    , clt_consent
    , consent_start_date
    , portfolio
    , expiry_date
    , intraday_auto_contra_option
    , dcf_limit
    , brokerage_type_etb
    , mgn_force_sell_pct
    , mgn_tenure
    , mgn_expiry_date
    , loss_gl_acc_no
    , portfolio_date
    , day_prior_temp_susp
    , day_prior_perm_susp
    , gst_code
    , match_price_decimal_local
    , match_price_decimal_foreign
    , primary_id_expiry_date
    , secondary_id_expiry_date
    , mgn_int_tdr_spread_pct
    , mgn_base_int_rate
    , mgn_int_tdr_share
    , islamic_flag
    , mcd_resident_flag
    , chq_charges_flag
    , chq_charges_tdr_pct
    , brokerage_type_foreign_etrade
    , brokerage_type_foreign_etrade_b
    , brokerage_type_foreign_etrade_s
    , grp_exch_code
    , bdebt_ras
    , twse_declaration
    , joint_acc_amt
    , high_risk_market
    , brokerage_type_leap_normal
    , brokerage_type_leap_etrade
    , type_of_account
    ,'A' AS dl_record_status
    ,dl_record_created_date
    ,dl_record_updated_date
FROM {params["com_schema"]}.m_mhbos_m_client com_m
WHERE NOT EXISTS (
    SELECT 1 FROM {params["com_schema"]}.temp_m_mhbos_m_client_consolidated r
    WHERE TO_DATE(r.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd'))) 
      AND r.cust_id = com_m.cust_id
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(rf"""
INSERT INTO TABLE {params["com_schema"]}.temp_m_mhbos_m_client_updated
SELECT
    r.cust_id
    , r.client_no
    , r.clean_rule_flag
    , r.primary_identification_type
    , r.primary_identification_no
    , r.secondary_identification_type
    , r.secondary_identification_no
    , r.customer_name
    , r.customer_name_concatenate
    , r.client_name
    , r.client_name1
    , r.client_name2
    , r.client_name3
    , r.mobile_no
    , r.fax_no
    , r.tel_no_home
    , r.tel_no_office
    , r.date_of_birth
    , r.race
    , r.email_1
    , r.email_2
    , r.email_3
    , r.email_4
    , r.email_5
    , r.email_6
    , r.email_7
    , r.email_8
    , r.email_9
    , r.email_10
    , r.sex
    , r.addr1
    , r.addr2
    , r.addr3
    , r.addr4
    , r.postcode
    , r.city
    , r.state
    , r.perm_addr1
    , r.perm_addr2
    , r.perm_addr3
    , r.perm_addr4
    , r.perm_postcode
    , r.perm_city
    , r.perm_state
    , r.noms_ind
    , r.cleaned_nominees_name
    , r.principal_name
    , r.intermediary_name
    , r.beneficiary_name
    , r.nominees_type
    , r.pledged_securities_flag
    , r.id_type
    , r.ic_no_new
    , r.ic_no_old
    , r.secondary_id_type
    , r.secondary_id_no
    , r.source_client_name
    , r.source_client_name1
    , r.source_client_name2
    , r.source_client_name3
    , r.source_mobile_no
    , r.source_fax_no
    , r.source_tel_no_home
    , r.source_tel_no_office
    , r.source_date_of_birth
    , r.source_race
    , r.source_email
    , r.source_sex
    , r.client_group
    , r.cds_acc_no
    , r.tdr_code
    , r.client_type
    , r.margin
    , r.last_margin_date
    , r.int_rate
    , r.auto_ded
    , r.despatch_mode
    , r.copies
    , r.prohibit_trade
    , r.custody_status
    , r.country
    , r.margin_limit
    , r.margin_pct
    , r.rollover_rate
    , r.form_completed
    , r.last_tran_date
    , r.ytd_bvalue
    , r.ytd_svalue
    , r.ytd_brokerage
    , r.os_led_bal
    , r.title
    , r.date_created
    , r.stop_payt
    , r.acc_payee
    , r.category
    , r.auto_contra
    , r.pnl_acc_no
    , r.cr_limit
    , r.trust_bal
    , r.avg_ind
    , r.remarks
    , r.contact_person
    , r.date_closed
    , r.grace_period
    , r.acct_type
    , r.assoc_ind
    , r.short_sell_ind
    , r.short_name
    , r.mesdaq_pctlmt
    , r.date_change
    , r.resi_code
    , r.charge_int
    , r.bdebt
    , r.assets
    , r.liabilities
    , r.income
    , r.expenses
    , r.bdebt_his_ind
    , r.rel_ac1
    , r.rel_ac2
    , r.rel_ac3
    , r.rel_ac4
    , r.occupation
    , r.margin_int
    , r.lst_led_no
    , r.cur_led_no
    , r.remarks2
    , r.acc_type
    , r.mas_accno
    , r.legal
    , r.sell_limit
    , r.brk_rate
    , r.brokerage_type
    , r.cds_acc_no1
    , r.remarks1
    , r.payment_bank_code
    , r.noms
    , r.dms_date
    , r.violation_date
    , r.mcd_branch
    , r.home_branch
    , r.eaf_code
    , r.call_warrant
    , r.user_id
    , r.credit_int_rate
    , r.min_eligible_amt
    , r.intraday_flag
    , r.intraday_rate
    , r.cta_weight
    , r.sta_weight
    , r.bo_cds_acc_no
    , r.ecos_form
    , r.custodian_no
    , r.prin_acc
    , r.armada_type
    , r.old_authorisee
    , r.etrade_rate
    , r.etf
    , r.cstamp_client_exempt
    , r.main_branch
    , r.prev_client_no
    , r.web_eds
    , r.place
    , r.excl_tdr_deduct
    , r.excl_auto_susp
    , r.trust_flag
    , r.mgn_new_int_rate
    , r.counter_concentration
    , r.auto_trust
    , r.margin_pct2
    , r.df_flag
    , r.mgn_curr_int_rate
    , r.product_type
    , r.web_ecos
    , r.xeye_clt_grp
    , r.bursa_violation_date
    , r.brokerage_type_etrade
    , r.brokerage_type_odd_lot
    , r.omnibus
    , r.cg_tdr_code
    , r.limit_foreign
    , r.limit_bursa
    , r.brokerage_type_intraday
    , r.brokerage_type_intraday_etrade
    , r.bursa_violation_date1
    , r.cif_no
    , r.brokerage_type_foreign
    , r.soft_copy
    , r.exclude_rollover
    , r.account_status
    , r.w8ben
    , r.ic_no_rel1
    , r.ic_no_rel2
    , r.ic_no_rel3
    , r.ic_no_rel4
    , r.ic_no_rel5
    , r.rel1
    , r.rel2
    , r.rel3
    , r.rel4
    , r.rel5
    , r.brokerage_type_etrade_b
    , r.brokerage_type_odd_lot_b
    , r.brokerage_type_b
    , r.brokerage_type_foreign_b
    , r.brokerage_type_intraday_b
    , r.brokerage_type_intraday_etrade_b
    , r.brokerage_type_etrade_s
    , r.brokerage_type_odd_lot_s
    , r.brokerage_type_s
    , r.brokerage_type_foreign_s
    , r.brokerage_type_intraday_s
    , r.brokerage_type_intraday_etrade_s
    , r.no_free_trade
    , r.sms
    , r.mobile_prefix
    , r.foreign_curr_set
    , r.num_free_trade
    , r.etrader_type
    , r.check_limit
    , r.auto_margin
    , r.margin_client_no
    , r.dup_despatch_mode
    , r.risk
    , r.exclude_trader_limit
    , r.sett_mode_date_change
    , r.pick_up_fee_pct
    , r.e_payment
    , r.mgn_new_int_rate2
    , r.fund_cost_type
    , r.check_share
    , r.citibank_changes
    , r.citibank_charges
    , r.cq_market
    , r.exclude_margin_pro_rate
    , r.brokerage_type_cash_b
    , r.brokerage_type_etrade_cash_b
    , r.clt_consent
    , r.consent_start_date
    , r.portfolio
    , r.expiry_date
    , r.intraday_auto_contra_option
    , r.dcf_limit
    , r.brokerage_type_etb
    , r.mgn_force_sell_pct
    , r.mgn_tenure
    , r.mgn_expiry_date
    , r.loss_gl_acc_no
    , r.portfolio_date
    , r.day_prior_temp_susp
    , r.day_prior_perm_susp
    , r.gst_code
    , r.match_price_decimal_local
    , r.match_price_decimal_foreign
    , r.primary_id_expiry_date
    , r.secondary_id_expiry_date
    , r.mgn_int_tdr_spread_pct
    , r.mgn_base_int_rate
    , r.mgn_int_tdr_share
    , r.islamic_flag
    , r.mcd_resident_flag
    , r.chq_charges_flag
    , r.chq_charges_tdr_pct
    , r.brokerage_type_foreign_etrade
    , r.brokerage_type_foreign_etrade_b
    , r.brokerage_type_foreign_etrade_s
    , r.grp_exch_code
    , r.bdebt_ras
    , r.twse_declaration
    , r.joint_acc_amt
    , r.high_risk_market
    , r.brokerage_type_leap_normal
    , r.brokerage_type_leap_etrade
    , r.type_of_account
    ,'A' AS dl_record_status
    ,CASE WHEN com_m.cust_id IS NOT NULL THEN com_m.dl_record_created_date
          ELSE current_timestamp() END AS dl_record_created_date
    ,current_timestamp() AS dl_record_updated_date
FROM {params["com_schema"]}.temp_m_mhbos_m_client_consolidated r
LEFT JOIN {params["com_schema"]}.m_mhbos_m_client com_m
    ON r.cust_id = com_m.cust_id
WHERE TO_DATE(r.dl_record_updated_date) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))

""")


# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(rf"""
INSERT OVERWRITE TABLE {params["com_schema"]}.m_mhbos_m_client
SELECT
    cust_id
    , client_no
    , clean_rule_flag
    , primary_identification_type
    , primary_identification_no
    , secondary_identification_type
    , secondary_identification_no
    , customer_name
    , customer_name_concatenate
    , client_name
    , client_name1
    , client_name2
    , client_name3
    , mobile_no
    , fax_no
    , tel_no_home
    , tel_no_office
    , date_of_birth
    , race
    , email_1
    , email_2
    , email_3
    , email_4
    , email_5
    , email_6
    , email_7
    , email_8
    , email_9
    , email_10
    , sex
    , addr1
    , addr2
    , addr3
    , addr4
    , postcode
    , city
    , state
    , perm_addr1
    , perm_addr2
    , perm_addr3
    , perm_addr4
    , perm_postcode
    , perm_city
    , perm_state
    , noms_ind
    , cleaned_nominees_name
    , principal_name
    , intermediary_name
    , beneficiary_name
    , nominees_type
    , pledged_securities_flag
    , id_type
    , ic_no_new
    , ic_no_old
    , secondary_id_type
    , secondary_id_no
    , source_client_name
    , source_client_name1
    , source_client_name2
    , source_client_name3
    , source_mobile_no
    , source_fax_no
    , source_tel_no_home
    , source_tel_no_office
    , source_date_of_birth
    , source_race
    , source_email
    , source_sex
    , client_group
    , cds_acc_no
    , tdr_code
    , client_type
    , margin
    , last_margin_date
    , int_rate
    , auto_ded
    , despatch_mode
    , copies
    , prohibit_trade
    , custody_status
    , country
    , margin_limit
    , margin_pct
    , rollover_rate
    , form_completed
    , last_tran_date
    , ytd_bvalue
    , ytd_svalue
    , ytd_brokerage
    , os_led_bal
    , title
    , date_created
    , stop_payt
    , acc_payee
    , category
    , auto_contra
    , pnl_acc_no
    , cr_limit
    , trust_bal
    , avg_ind
    , remarks
    , contact_person
    , date_closed
    , grace_period
    , acct_type
    , assoc_ind
    , short_sell_ind
    , short_name
    , mesdaq_pctlmt
    , date_change
    , resi_code
    , charge_int
    , bdebt
    , assets
    , liabilities
    , income
    , expenses
    , bdebt_his_ind
    , rel_ac1
    , rel_ac2
    , rel_ac3
    , rel_ac4
    , occupation
    , margin_int
    , lst_led_no
    , cur_led_no
    , remarks2
    , acc_type
    , mas_accno
    , legal
    , sell_limit
    , brk_rate
    , brokerage_type
    , cds_acc_no1
    , remarks1
    , payment_bank_code
    , noms
    , dms_date
    , violation_date
    , mcd_branch
    , home_branch
    , eaf_code
    , call_warrant
    , user_id
    , credit_int_rate
    , min_eligible_amt
    , intraday_flag
    , intraday_rate
    , cta_weight
    , sta_weight
    , bo_cds_acc_no
    , ecos_form
    , custodian_no
    , prin_acc
    , armada_type
    , old_authorisee
    , etrade_rate
    , etf
    , cstamp_client_exempt
    , main_branch
    , prev_client_no
    , web_eds
    , place
    , excl_tdr_deduct
    , excl_auto_susp
    , trust_flag
    , mgn_new_int_rate
    , counter_concentration
    , auto_trust
    , margin_pct2
    , df_flag
    , mgn_curr_int_rate
    , product_type
    , web_ecos
    , xeye_clt_grp
    , bursa_violation_date
    , brokerage_type_etrade
    , brokerage_type_odd_lot
    , omnibus
    , cg_tdr_code
    , limit_foreign
    , limit_bursa
    , brokerage_type_intraday
    , brokerage_type_intraday_etrade
    , bursa_violation_date1
    , cif_no
    , brokerage_type_foreign
    , soft_copy
    , exclude_rollover
    , account_status
    , w8ben
    , ic_no_rel1
    , ic_no_rel2
    , ic_no_rel3
    , ic_no_rel4
    , ic_no_rel5
    , rel1
    , rel2
    , rel3
    , rel4
    , rel5
    , brokerage_type_etrade_b
    , brokerage_type_odd_lot_b
    , brokerage_type_b
    , brokerage_type_foreign_b
    , brokerage_type_intraday_b
    , brokerage_type_intraday_etrade_b
    , brokerage_type_etrade_s
    , brokerage_type_odd_lot_s
    , brokerage_type_s
    , brokerage_type_foreign_s
    , brokerage_type_intraday_s
    , brokerage_type_intraday_etrade_s
    , no_free_trade
    , sms
    , mobile_prefix
    , foreign_curr_set
    , num_free_trade
    , etrader_type
    , check_limit
    , auto_margin
    , margin_client_no
    , dup_despatch_mode
    , risk
    , exclude_trader_limit
    , sett_mode_date_change
    , pick_up_fee_pct
    , e_payment
    , mgn_new_int_rate2
    , fund_cost_type
    , check_share
    , citibank_changes
    , citibank_charges
    , cq_market
    , exclude_margin_pro_rate
    , brokerage_type_cash_b
    , brokerage_type_etrade_cash_b
    , clt_consent
    , consent_start_date
    , portfolio
    , expiry_date
    , intraday_auto_contra_option
    , dcf_limit
    , brokerage_type_etb
    , mgn_force_sell_pct
    , mgn_tenure
    , mgn_expiry_date
    , loss_gl_acc_no
    , portfolio_date
    , day_prior_temp_susp
    , day_prior_perm_susp
    , gst_code
    , match_price_decimal_local
    , match_price_decimal_foreign
    , primary_id_expiry_date
    , secondary_id_expiry_date
    , mgn_int_tdr_spread_pct
    , mgn_base_int_rate
    , mgn_int_tdr_share
    , islamic_flag
    , mcd_resident_flag
    , chq_charges_flag
    , chq_charges_tdr_pct
    , brokerage_type_foreign_etrade
    , brokerage_type_foreign_etrade_b
    , brokerage_type_foreign_etrade_s
    , grp_exch_code
    , bdebt_ras
    , twse_declaration
    , joint_acc_amt
    , high_risk_market
    , brokerage_type_leap_normal
    , brokerage_type_leap_etrade
    , type_of_account
    ,dl_record_status
    ,dl_record_created_date
    ,dl_record_updated_date
    ,'{batch_date}'       AS etl_dt
    ,current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_m_mhbos_m_client_updated
""")




spark.sql(rf"""
/*
===================================== GET FULL ACCOUNT LIST WITH ASSOCIATED CUST ID =====================================
*/
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_m_mhbos_m_client_account_cust_id (
  `cust_id` VARCHAR(11),
  `client_no` VARCHAR(20),
  `clean_rule_flag` VARCHAR(60),
  `primary_identification_type` VARCHAR(10),
  `primary_identification_no` VARCHAR(60),
  `secondary_identification_type` VARCHAR(10),
  `secondary_identification_no` VARCHAR(60),
  `customer_name` VARCHAR(250),
  `etl_timestamp` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transactional'='true',
  'transactional_properties'='default'
)
""")


spark.sql(rf"""
/*
JOIN dataset 'Unique Customer WITH Cust ID' WITH t
using JOIN key Primary ID + Cust Name to get full list of accounts WITH respective cust id
*/
WITH unique_customer_with_cust_id AS (
  SELECT
    *
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_match_cust_id
  UNION ALL
  SELECT
    *
  FROM {params["com_schema"]}.temp_m_mhbos_m_client_new_cust_id
), t AS (
  SELECT
    *
  FROM {params["com_schema"]}.t_mhbos_m_client
  WHERE
    etl_dt = '{batch_date}'
    AND /* Remove accounts WITH exception in Primary ID type */ NOT PRIMARY_IDENTIFICATION_TYPE LIKE '@[%]'
    AND /* Remove accounts WITH exception in Primary ID number */ NOT PRIMARY_IDENTIFICATION_NO LIKE '@[%]'
    AND /* 2025/09/08: Remove accounts with exception in account number */ NOT CLIENT_NO LIKE '@[%]'
    AND /* 2025/09/08: Remove accounts WITH exception in customer name */ NOT CUSTOMER_NAME LIKE '@[%]'
)
INSERT INTO {params["com_schema"]}.temp_m_mhbos_m_client_account_cust_id
SELECT
  COALESCE(cust1.cust_id, cust2.cust_id) AS cust_id,
  t.client_no,
  COALESCE(cust1.clean_rule_flag, cust2.clean_rule_flag) AS clean_rule_flag,
  t.primary_identification_type,
  t.primary_identification_no,
  t.secondary_identification_type,
  t.secondary_identification_no,
  t.customer_name,
  t.etl_timestamp
FROM t
LEFT JOIN unique_customer_with_cust_id AS cust1
  ON t.primary_identification_no = cust1.primary_identification_no
  AND /* Remove accounts WITH exception in customer name */ t.customer_name = cust1.customer_name
LEFT JOIN (
  SELECT
    cust_id,
    clean_rule_flag,
    secondary_identification_no,
    customer_name,
    ROW_NUMBER() OVER (PARTITION BY secondary_identification_no, customer_name ORDER BY cust_id DESC) AS rn
  FROM unique_customer_with_cust_id
  WHERE
    TRIM(COALESCE(secondary_identification_no, '')) <> ''
) AS cust2
  ON t.primary_identification_no = cust2.secondary_identification_no
  AND /* Remove accounts WITH exception in customer name */ t.customer_name = cust2.customer_name
  AND cust2.rn = 1
WHERE
  NOT COALESCE(cust1.cust_id, cust2.cust_id) IS NULL
""")


spark.sql(rf"""
/*
===================================== LOOKUP SOURCE_OWNER_ID IN MAPPING =====================================
*/
/*
IF these conditions happen
1. source_owner_id exist in mapping
2. Source_owner_id belong to mapping WITH same source name
3. Existing mapping require update in at least 1 of these fields
    - cust_id
    - primary_identification_no
    - secondary_identification_no
    - customer_name
*/
/* Step 1: Insert existing mapping to history TABLE */
INSERT INTO {params["com_schema"]}.m_customer_id_mapping_h
SELECT
  'MHBOS_M_CLIENT' AS update_source,
  mapping.cust_id,
  mapping.source_owner_id,
  mapping.primary_identification_no,
  mapping.secondary_identification_no,
  mapping.customer_name,
  mapping.source_name,
  mapping.priority_level,
  mapping.etl_timestamp AS start_timestamp,
  CURRENT_TIMESTAMP() AS end_timestamp,
  mapping.customer_type AS customer_type
FROM {params["com_schema"]}.m_customer_id_mapping AS mapping
JOIN {params["com_schema"]}.temp_m_mhbos_m_client_account_cust_id AS acc
  ON mapping.source_owner_id = acc.client_no
WHERE
  mapping.source_name = 'MHBOS_M_CLIENT'
  AND /* But value must be updated ON at least 1 column */ (
    mapping.cust_id <> acc.cust_id
    OR mapping.primary_identification_no <> acc.primary_identification_no
    OR mapping.secondary_identification_no <> acc.secondary_identification_no
    OR mapping.customer_name <> acc.customer_name
  )
""")


spark.sql(rf"""
MERGE INTO {params["com_schema"]}.m_customer_id_mapping AS mapping
USING {params["com_schema"]}.temp_m_mhbos_m_client_account_cust_id AS acc
ON mapping.source_owner_id = acc.client_no
WHEN MATCHED AND (
  mapping.source_name = 'MHBOS_M_CLIENT'
  AND (
    mapping.cust_id <> acc.cust_id
    OR mapping.primary_identification_no <> acc.primary_identification_no
    OR mapping.secondary_identification_no <> acc.secondary_identification_no
    OR mapping.customer_name <> acc.customer_name
  )
) THEN UPDATE SET cust_id = acc.cust_id, primary_identification_no = acc.primary_identification_no, secondary_identification_no = acc.secondary_identification_no, customer_name = acc.customer_name, etl_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    cust_id,
    source_owner_id,
    primary_identification_no,
    secondary_identification_no,
    customer_name,
    source_name,
    priority_level,
    etl_timestamp,
    customer_type
) VALUES (
  acc.cust_id,
  acc.client_no,
  acc.primary_identification_no,
  acc.secondary_identification_no,
  acc.customer_name,
  'MHBOS_M_CLIENT', /* source_name */
  1, /* priority_level */
  CURRENT_TIMESTAMP(),
  ''
)
""")





spark.sql(rf"""ANALYZE TABLE {params["com_schema"]}.m_mhbos_m_client COMPUTE STATISTICS""")

spark.stop()