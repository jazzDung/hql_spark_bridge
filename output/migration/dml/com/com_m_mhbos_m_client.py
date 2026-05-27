"""
Purpose:    Customer information merge program
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py 20230809 com_m_mhbos_m_client
CreateDate: 20230816
Logs:       zhairp 20230816 create script.
Logs        lixiaotian 20240702 annotating code(like 'TOMS%'/ not like 'TOMS%')
1.0 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;
DROP all temporary tables
"""

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
  `etl_timestamp` TIMESTAMP
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
  `etl_timestamp` TIMESTAMP
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
  `etl_timestamp` TIMESTAMP
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
  `etl_timestamp` TIMESTAMP
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
)
INSERT OVERWRITE {params["com_schema"]}.m_mhbos_m_client
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
  'A' as dl_record_status,
  current_timestamp() as dl_record_created_date,
  current_timestamp() as dl_record_updated_date,
  '{batch_date}'       AS etl_dt,
  current_timestamp()  AS etl_timestamp
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
  'A' as dl_record_status,
  current_timestamp() as dl_record_created_date,
  current_timestamp() as dl_record_updated_date,
  '{batch_date}'       AS etl_dt,
  current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_m_mhbos_m_client_exception AS cust
JOIN last_updated_account AS t
  ON cust.primary_identification_no = t.primary_identification_no
  AND cust.customer_name_original = t.customer_name
WHERE
  t.rn = 1
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
  `etl_timestamp` TIMESTAMP
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
    /* Remove accounts WITH exception in Primary ID type */ NOT PRIMARY_IDENTIFICATION_TYPE LIKE '@[%]'
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
  mapping.customer_type AS customer_type,
  date_format(current_timestamp(), 'yyyyMM')
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