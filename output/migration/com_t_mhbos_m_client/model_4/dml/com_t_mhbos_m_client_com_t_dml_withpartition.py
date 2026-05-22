
"""
Purpose:    Customer information cleaning rules implementation program
Author:     Sunline
Usage:      python $ETL_HOME/script/main.py 20230809 com_t_mhbos_m_client
CreateDate: 20230810
Logs:       zhairp 20230810 create script.
Logs:       leext  20240321 modify and add rule.
Logs:       leext  20240518 modify and add rule
Logs:       marco  20241111 modify noms_ind and nominees_type logic
Logs:       marco  20241111 modify customer_name logic to store principal name regardless of nominee criteria
Logs:       marco  20241118 modify remove bracketed text at the end of the customer_name logic (from '\\(.*\\)$' to '\\s*\\([^)]*\\)$')
Logs:       marco  20241120 modify customer_name logic to ignore splitting of "FOR" for client_type in ('#', '0', '1', '6', '8', 'V')
Logs:       marco  20250313 added primary_identification_type conversion logic and new lookup file primaryidno_newcustname
Logs:       marco  20250502 update contact number cleansing logic (temp_t_mhbos_m_client_telephone_clean, temp_t_mhbos_m_client_telephone_number_info)
Logs:       marco  20250502 update BRN number length checking (temp_t_mhbos_m_client_primary_identification_no, temp_t_mhbos_m_client_secondary_identification_no)
Logs:       marco  20250620 update customer_name, secondary_identification_no, secondary_identification_no conversion based on lookup_custname_primaryidno
Logs:       marco  20250715 added new fields from m_client_ext: perm_city, perm_state_code, perm_country and derive address logic
Logs:       syhmi  20250903 added new fields: einvoice_email
Logs:       marco  20250925 optimize whole sript, only do data cleansing for incremental data from com_r_mhbos_m_client and com_r_mhbos_m_client_ext table
Logs:       marco  20251013 add m_client_ext.state_code to determine mailing state, remove the full address scanning logic to define mailing state and registered state
1.0 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp

source_name = "mhbos"
table_name  = "m_client"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# Enable dynamic partition overwrites
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ─── PRE-PROCESSING (Temp tables logic from legacy script) ───────────────────
spark.sql(f"""
/* 1.2 Drop all temporary tables at the start of the program */
    DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_all
""")
spark.sql(f"""
/* 2.1 Create a temporary table temp_com_mhbos_m_client_all to store the current vaild data */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_all (
      `client_no` STRING,
      `client_group` STRING,
      `cds_acc_no` STRING,
      `tdr_code` STRING,
      `client_name` STRING,
      `client_type` STRING,
      `margin` STRING,
      `last_margin_date` TIMESTAMP,
      `int_rate` DECIMAL(5, 2),
      `auto_ded` STRING,
      `despatch_mode` STRING,
      `copies` DECIMAL(2, 0),
      `prohibit_trade` STRING,
      `custody_status` STRING,
      `race` STRING,
      `country` STRING,
      `ic_no_new` STRING,
      `ic_no_old` STRING,
      `margin_limit` DECIMAL(9, 0),
      `margin_pct` DECIMAL(5, 2),
      `rollover_rate` DECIMAL(6, 3),
      `form_completed` STRING,
      `last_tran_date` TIMESTAMP,
      `ytd_bvalue` DECIMAL(12, 2),
      `ytd_svalue` DECIMAL(12, 2),
      `ytd_brokerage` DECIMAL(11, 2),
      `os_led_bal` DECIMAL(12, 2),
      `title` STRING,
      `addr1` STRING,
      `addr2` STRING,
      `addr3` STRING,
      `tel_no_home` STRING,
      `tel_no_office` STRING,
      `date_created` TIMESTAMP,
      `stop_payt` STRING,
      `acc_payee` STRING,
      `category` STRING,
      `fax_no` STRING,
      `auto_contra` STRING,
      `pnl_acc_no` STRING,
      `cr_limit` DECIMAL(9, 0),
      `trust_bal` DECIMAL(12, 2),
      `avg_ind` STRING,
      `remarks` STRING,
      `contact_person` STRING,
      `date_closed` TIMESTAMP,
      `grace_period` DECIMAL(3, 0),
      `acct_type` DECIMAL(2, 0),
      `assoc_ind` STRING,
      `short_sell_ind` STRING,
      `short_name` STRING,
      `mesdaq_pctlmt` DECIMAL(5, 2),
      `date_change` TIMESTAMP,
      `resi_code` STRING,
      `sex` STRING,
      `charge_int` STRING,
      `bdebt` STRING,
      `assets` DECIMAL(12, 2),
      `liabilities` DECIMAL(12, 2),
      `income` DECIMAL(12, 2),
      `expenses` DECIMAL(12, 2),
      `bdebt_his_ind` STRING,
      `rel_ac1` STRING,
      `rel_ac2` STRING,
      `rel_ac3` STRING,
      `rel_ac4` STRING,
      `occupation` STRING,
      `margin_int` DECIMAL(12, 2),
      `lst_led_no` DECIMAL(9, 0),
      `cur_led_no` DECIMAL(9, 0),
      `remarks2` STRING,
      `acc_type` STRING,
      `mas_accno` STRING,
      `legal` STRING,
      `sell_limit` DECIMAL(9, 0),
      `brk_rate` DECIMAL(9, 4),
      `client_name1` STRING,
      `postcode` STRING,
      `brokerage_type` STRING,
      `cds_acc_no1` STRING,
      `remarks1` STRING,
      `payment_bank_code` STRING,
      `noms` STRING,
      `dms_date` TIMESTAMP,
      `violation_date` TIMESTAMP,
      `mcd_branch` STRING,
      `home_branch` STRING,
      `eaf_code` STRING,
      `call_warrant` STRING,
      `user_id` STRING,
      `credit_int_rate` DECIMAL(5, 2),
      `min_eligible_amt` DECIMAL(18, 4),
      `intraday_flag` STRING,
      `intraday_rate` DECIMAL(5, 4),
      `cta_weight` DECIMAL(3, 0),
      `sta_weight` DECIMAL(3, 0),
      `bo_cds_acc_no` STRING,
      `ecos_form` STRING,
      `custodian_no` STRING,
      `prin_acc` STRING,
      `armada_type` STRING,
      `old_authorisee` STRING,
      `etrade_rate` DECIMAL(9, 4),
      `etf` STRING,
      `cstamp_client_exempt` STRING,
      `main_branch` STRING,
      `prev_client_no` STRING,
      `web_eds` STRING,
      `place` STRING,
      `addr4` STRING,
      `excl_tdr_deduct` STRING,
      `excl_auto_susp` STRING,
      `trust_flag` STRING,
      `mgn_new_int_rate` DECIMAL(5, 2),
      `counter_concentration` DECIMAL(5, 2),
      `auto_trust` STRING,
      `margin_pct2` DECIMAL(5, 2),
      `df_flag` STRING,
      `mgn_curr_int_rate` DECIMAL(5, 2),
      `product_type` STRING,
      `web_ecos` STRING,
      `xeye_clt_grp` STRING,
      `bursa_violation_date` TIMESTAMP,
      `client_name2` STRING,
      `brokerage_type_etrade` STRING,
      `brokerage_type_odd_lot` STRING,
      `omnibus` STRING,
      `cg_tdr_code` STRING,
      `limit_foreign` DECIMAL(9, 4),
      `limit_bursa` DECIMAL(9, 4),
      `brokerage_type_intraday` STRING,
      `brokerage_type_intraday_etrade` STRING,
      `bursa_violation_date1` TIMESTAMP,
      `cif_no` STRING,
      `brokerage_type_foreign` STRING,
      `soft_copy` STRING,
      `exclude_rollover` STRING,
      `account_status` STRING,
      `w8ben` STRING,
      `ic_no_rel1` STRING,
      `ic_no_rel2` STRING,
      `ic_no_rel3` STRING,
      `ic_no_rel4` STRING,
      `ic_no_rel5` STRING,
      `rel1` STRING,
      `rel2` STRING,
      `rel3` STRING,
      `rel4` STRING,
      `rel5` STRING,
      `brokerage_type_etrade_b` STRING,
      `brokerage_type_odd_lot_b` STRING,
      `brokerage_type_b` STRING,
      `brokerage_type_foreign_b` STRING,
      `brokerage_type_intraday_b` STRING,
      `brokerage_type_intraday_etrade_b` STRING,
      `brokerage_type_etrade_s` STRING,
      `brokerage_type_odd_lot_s` STRING,
      `brokerage_type_s` STRING,
      `brokerage_type_foreign_s` STRING,
      `brokerage_type_intraday_s` STRING,
      `brokerage_type_intraday_etrade_s` STRING,
      `no_free_trade` DECIMAL(2, 0),
      `sms` STRING,
      `mobile_prefix` STRING,
      `mobile_no` STRING,
      `foreign_curr_set` STRING,
      `num_free_trade` DECIMAL(2, 0),
      `etrader_type` STRING,
      `check_limit` STRING,
      `auto_margin` STRING,
      `margin_client_no` STRING,
      `dup_despatch_mode` STRING,
      `risk` STRING,
      `exclude_trader_limit` STRING,
      `sett_mode_date_change` TIMESTAMP,
      `pick_up_fee_pct` DECIMAL(5, 2),
      `e_payment` STRING,
      `mgn_new_int_rate2` DECIMAL(5, 2),
      `fund_cost_type` STRING,
      `check_share` STRING,
      `citibank_changes` STRING,
      `citibank_charges` STRING,
      `cq_market` STRING,
      `exclude_margin_pro_rate` STRING,
      `brokerage_type_cash_b` STRING,
      `brokerage_type_etrade_cash_b` STRING,
      `clt_consent` STRING,
      `consent_start_date` TIMESTAMP,
      `portfolio` STRING,
      `expiry_date` TIMESTAMP,
      `intraday_auto_contra_option` STRING,
      `dcf_limit` DECIMAL(9, 0),
      `brokerage_type_etb` STRING,
      `mgn_force_sell_pct` DECIMAL(5, 2),
      `mgn_tenure` DECIMAL(3, 0),
      `mgn_expiry_date` TIMESTAMP,
      `loss_gl_acc_no` STRING,
      `portfolio_date` TIMESTAMP,
      `day_prior_temp_susp` DECIMAL(4, 0),
      `day_prior_perm_susp` DECIMAL(4, 0),
      `gst_code` STRING,
      `match_price_decimal_local` DECIMAL(1, 0),
      `match_price_decimal_foreign` DECIMAL(1, 0),
      `id_type` STRING,
      `primary_id_expiry_date` TIMESTAMP,
      `secondary_id_no` STRING,
      `secondary_id_expiry_date` TIMESTAMP,
      `mgn_int_tdr_spread_pct` DECIMAL(5, 2),
      `mgn_base_int_rate` DECIMAL(5, 2),
      `mgn_int_tdr_share` DECIMAL(5, 2),
      `islamic_flag` STRING,
      `mcd_resident_flag` STRING,
      `chq_charges_flag` STRING,
      `chq_charges_tdr_pct` DECIMAL(9, 2),
      `brokerage_type_foreign_etrade` STRING,
      `brokerage_type_foreign_etrade_b` STRING,
      `brokerage_type_foreign_etrade_s` STRING,
      `grp_exch_code` STRING,
      `secondary_id_type` STRING,
      `bdebt_ras` STRING,
      `twse_declaration` STRING,
      `client_name3` STRING,
      `joint_acc_amt` DECIMAL(12, 2),
      `high_risk_market` STRING,
      `brokerage_type_leap_normal` STRING,
      `brokerage_type_leap_etrade` STRING,
      `etl_timestamp` STRING,
      `etl_dt` STRING,
      `date_of_birth` TIMESTAMP,
      `email` STRING,
      `perm_addr1` STRING,
      `perm_addr2` STRING,
      `perm_addr3` STRING,
      `perm_addr4` STRING,
      `perm_postcode` STRING,
      `perm_city` STRING, /* 20250715 */
      `perm_state_code` STRING, /* 20250715 */
      `perm_country` STRING, /* 20250715 */
      `type_of_account` STRING, /* 20250806 */
      `einvoice_email` STRING, /* 20250903 */
      `state_code` STRING /* 20251013 */
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_all /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
WITH filtered_clients AS (
      SELECT DISTINCT
        client_no
      FROM {params["raw_schema"]}.mhbos_m_client
      WHERE
        etl_dt = '{batch_date}'
      UNION
      SELECT DISTINCT
        client_no
      FROM {params["raw_schema"]}.mhbos_m_client_ext
      WHERE
        etl_dt = '{batch_date}'
    ), latest_clients AS (
      SELECT
        mmc.*,
        ROW_NUMBER() OVER (PARTITION BY mmc.client_no ORDER BY mmc.etl_dt DESC) AS rn
      FROM {params["raw_schema"]}.mhbos_m_client AS mmc
      INNER JOIN filtered_clients AS fc
        ON mmc.client_no = fc.client_no
      WHERE
        mmc.etl_dt <= '{batch_date}'
    )
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_all
    SELECT
      mmc.client_no,
      mmc.client_group,
      mmc.cds_acc_no,
      mmc.tdr_code,
      mmc.client_name,
      mmc.client_type,
      mmc.margin,
      mmc.last_margin_date,
      mmc.int_rate,
      mmc.auto_ded,
      mmc.despatch_mode,
      mmc.copies,
      mmc.prohibit_trade,
      mmc.custody_status,
      mmc.race,
      mmc.country,
      REGEXP_REPLACE(mmc.ic_no_new, '-', '') AS ic_no_new,
      REGEXP_REPLACE(mmc.ic_no_old, '-', '') AS ic_no_old,
      mmc.margin_limit,
      mmc.margin_pct,
      mmc.rollover_rate,
      mmc.form_completed,
      mmc.last_tran_date,
      mmc.ytd_bvalue,
      mmc.ytd_svalue,
      mmc.ytd_brokerage,
      mmc.os_led_bal,
      mmc.title,
      mmc.addr1,
      mmc.addr2,
      mmc.addr3,
      mmc.tel_no_home,
      mmc.tel_no_office,
      mmc.date_created,
      mmc.stop_payt,
      mmc.acc_payee,
      mmc.category,
      mmc.fax_no,
      mmc.auto_contra,
      mmc.pnl_acc_no,
      mmc.cr_limit,
      mmc.trust_bal,
      mmc.avg_ind,
      mmc.remarks,
      mmc.contact_person,
      mmc.date_closed,
      mmc.grace_period,
      mmc.acct_type,
      mmc.assoc_ind,
      mmc.short_sell_ind,
      mmc.short_name,
      mmc.mesdaq_pctlmt,
      mmc.date_change,
      mmc.resi_code,
      mmc.sex,
      mmc.charge_int,
      mmc.bdebt,
      mmc.assets,
      mmc.liabilities,
      mmc.income,
      mmc.expenses,
      mmc.bdebt_his_ind,
      mmc.rel_ac1,
      mmc.rel_ac2,
      mmc.rel_ac3,
      mmc.rel_ac4,
      mmc.occupation,
      mmc.margin_int,
      mmc.lst_led_no,
      mmc.cur_led_no,
      mmc.remarks2,
      mmc.acc_type,
      mmc.mas_accno,
      mmc.legal,
      mmc.sell_limit,
      mmc.brk_rate,
      mmc.client_name1,
      mmc.postcode,
      mmc.brokerage_type,
      mmc.cds_acc_no1,
      mmc.remarks1,
      mmc.payment_bank_code,
      mmc.noms,
      mmc.dms_date,
      mmc.violation_date,
      mmc.mcd_branch,
      mmc.home_branch,
      mmc.eaf_code,
      mmc.call_warrant,
      mmc.`user_id`,
      mmc.credit_int_rate,
      mmc.min_eligible_amt,
      mmc.intraday_flag,
      mmc.intraday_rate,
      mmc.cta_weight,
      mmc.sta_weight,
      mmc.bo_cds_acc_no,
      mmc.ecos_form,
      mmc.custodian_no,
      mmc.prin_acc,
      mmc.armada_type,
      mmc.old_authorisee,
      mmc.etrade_rate,
      mmc.etf,
      mmc.cstamp_client_exempt,
      mmc.main_branch,
      mmc.prev_client_no,
      mmc.web_eds,
      mmc.place,
      mmc.addr4,
      mmc.excl_tdr_deduct,
      mmc.excl_auto_susp,
      mmc.trust_flag,
      mmc.mgn_new_int_rate,
      mmc.counter_concentration,
      mmc.auto_trust,
      mmc.margin_pct2,
      mmc.df_flag,
      mmc.mgn_curr_int_rate,
      mmc.product_type,
      mmc.web_ecos,
      mmc.xeye_clt_grp,
      mmc.bursa_violation_date,
      mmc.client_name2,
      mmc.brokerage_type_etrade,
      mmc.brokerage_type_odd_lot,
      mmc.omnibus,
      mmc.cg_tdr_code,
      mmc.limit_foreign,
      mmc.limit_bursa,
      mmc.brokerage_type_intraday,
      mmc.brokerage_type_intraday_etrade,
      mmc.bursa_violation_date1,
      mmc.cif_no,
      mmc.brokerage_type_foreign,
      mmc.soft_copy,
      mmc.exclude_rollover,
      mmc.account_status,
      mmc.w8ben,
      mmc.ic_no_rel1,
      mmc.ic_no_rel2,
      mmc.ic_no_rel3,
      mmc.ic_no_rel4,
      mmc.ic_no_rel5,
      mmc.rel1,
      mmc.rel2,
      mmc.rel3,
      mmc.rel4,
      mmc.rel5,
      mmc.brokerage_type_etrade_b,
      mmc.brokerage_type_odd_lot_b,
      mmc.brokerage_type_b,
      mmc.brokerage_type_foreign_b,
      mmc.brokerage_type_intraday_b,
      mmc.brokerage_type_intraday_etrade_b,
      mmc.brokerage_type_etrade_s,
      mmc.brokerage_type_odd_lot_s,
      mmc.brokerage_type_s,
      mmc.brokerage_type_foreign_s,
      mmc.brokerage_type_intraday_s,
      mmc.brokerage_type_intraday_etrade_s,
      mmc.no_free_trade,
      mmc.sms,
      mmc.mobile_prefix,
      mmc.mobile_no,
      mmc.foreign_curr_set,
      mmc.num_free_trade,
      mmc.etrader_type,
      mmc.check_limit,
      mmc.auto_margin,
      mmc.margin_client_no,
      mmc.dup_despatch_mode,
      mmc.risk,
      mmc.exclude_trader_limit,
      mmc.sett_mode_date_change,
      mmc.pick_up_fee_pct,
      mmc.e_payment,
      mmc.mgn_new_int_rate2,
      mmc.fund_cost_type,
      mmc.check_share,
      mmc.citibank_changes,
      mmc.citibank_charges,
      mmc.cq_market,
      mmc.exclude_margin_pro_rate,
      mmc.brokerage_type_cash_b,
      mmc.brokerage_type_etrade_cash_b,
      mmc.clt_consent,
      mmc.consent_start_date,
      mmc.portfolio,
      mmc.expiry_date,
      mmc.intraday_auto_contra_option,
      mmc.dcf_limit,
      mmc.brokerage_type_etb,
      mmc.mgn_force_sell_pct,
      mmc.mgn_tenure,
      mmc.mgn_expiry_date,
      mmc.loss_gl_acc_no,
      mmc.portfolio_date,
      mmc.day_prior_temp_susp,
      mmc.day_prior_perm_susp,
      mmc.gst_code,
      mmc.match_price_decimal_local,
      mmc.match_price_decimal_foreign,
      mmc.id_type,
      mmc.primary_id_expiry_date,
      REGEXP_REPLACE(mmc.secondary_id_no, '-', '') AS secondary_id_no,
      mmc.secondary_id_expiry_date,
      mmc.mgn_int_tdr_spread_pct,
      mmc.mgn_base_int_rate,
      mmc.mgn_int_tdr_share,
      mmc.islamic_flag,
      mmc.mcd_resident_flag,
      mmc.chq_charges_flag,
      mmc.chq_charges_tdr_pct,
      mmc.brokerage_type_foreign_etrade,
      mmc.brokerage_type_foreign_etrade_b,
      mmc.brokerage_type_foreign_etrade_s,
      mmc.grp_exch_code,
      mmc.secondary_id_type,
      mmc.bdebt_ras,
      mmc.twse_declaration,
      mmc.client_name3,
      mmc.joint_acc_amt,
      mmc.high_risk_market,
      mmc.brokerage_type_leap_normal,
      mmc.brokerage_type_leap_etrade,
      mmc.etl_timestamp,
      mmc.etl_dt,
      mmce.date_of_birth,
      mmce.email,
      mmce.perm_addr1,
      mmce.perm_addr2,
      mmce.perm_addr3,
      mmce.perm_addr4,
      mmce.perm_postcode,
      mmce.perm_city, /* 20250715 */
      mmce.perm_state_code, /* 20250715 */
      mmce.perm_country, /* 20250715 */
      mmc.type_of_account, /* 20250806 */
      mmce.einvoice_email, /* 20250903 */
      mmce.state_code /* 20251013 */
    FROM latest_clients AS mmc
    LEFT JOIN {params["com_schema"]}.t_mhbos_m_client_ext AS mmce
      ON mmc.client_no = mmce.client_no AND mmce.etl_dt = '{batch_date}'
    WHERE
      mmc.rn = 1
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type
""")
spark.sql(f"""
/* 2.2 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification type. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type (
      `client_no` STRING,
      `id_type` STRING,
      `secondary_id_type` STRING,
      `primary_identification_type` STRING,
      `primary_identification_type_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type
    SELECT
      mmc.client_no,
      mmc.id_type,
      mmc.secondary_id_type,
      (
        CASE
          WHEN mmc.id_type = '1'
          THEN '1'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
          THEN '1'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN '4'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN '3'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN '2'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN '6'
          WHEN mmc.id_type = '2'
          THEN '3'
          WHEN mmc.id_type = '3'
          THEN '4'
          WHEN mmc.id_type = '4'
          THEN '5'
          WHEN mmc.id_type = '5'
          THEN '2'
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN '4'
          WHEN mmc.id_type = '6'
          THEN '6'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
          THEN '1'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
          THEN '3'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
          THEN '4'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
          THEN '5'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
          THEN '2'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
          THEN '6'
          ELSE '@[' || mmc.id_type || ']'
        END
      ) AS primary_identification_type,
      (
        CASE
          WHEN mmc.id_type = '1'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN '0'
          WHEN mmc.id_type = '2'
          THEN '0'
          WHEN mmc.id_type = '3'
          THEN '0'
          WHEN mmc.id_type = '4'
          THEN '0'
          WHEN mmc.id_type = '5'
          THEN '0'
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN '0'
          WHEN mmc.id_type = '6'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '1'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '2'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '3'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '4'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '5'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = '' AND mmc.secondary_id_type = '6'
          THEN '0'
          ELSE '1'
        END
      ) AS primary_identification_type_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no
""")
spark.sql(f"""
/* 2.3 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification number. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no (
      `client_no` STRING,
      `id_type` STRING,
      `ic_no_new` STRING,
      `ic_no_old` STRING,
      `secondary_id_type` STRING,
      `secondary_id_no` STRING,
      `primary_identification_no` STRING,
      `primary_identification_no_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no
    SELECT
      mmc.client_no,
      mmc.id_type,
      mmc.ic_no_new,
      mmc.ic_no_old,
      mmc.secondary_id_type,
      mmc.secondary_id_no,
      (
        CASE
          WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '3'
          AND LENGTH(mmc.ic_no_new) < 12
          AND LENGTH(mmc.secondary_id_no) >= 12
          AND LENGTH(mmc.secondary_id_no) <= 15
          THEN mmc.secondary_id_no /* updated 20250502 */
          WHEN mmc.id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') <> ''
          AND LENGTH(mmc.secondary_id_no) < 12
          THEN mmc.ic_no_new /* updated 20250502 */
          WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
          THEN mmc.ic_no_new /* updated 20250502 */
          WHEN mmc.id_type = '4'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '5'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '6'
          THEN mmc.ic_no_new
          WHEN COALESCE(mmc.id_type, '') = ''
          AND COALESCE(mmc.secondary_id_type, '') <> ''
          AND COALESCE(mmc.secondary_id_no, '') <> ''
          THEN mmc.secondary_id_no
          ELSE '@[' || mmc.ic_no_new || ']'
        END
      ) AS primary_identification_no,
      (
        CASE
          WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_new, '') <> ''
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '1'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN '0'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN '0'
          WHEN mmc.id_type = '2' AND COALESCE(mmc.ic_no_new, '') <> ''
          THEN '0'
          WHEN mmc.id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
          THEN '0'
          WHEN mmc.id_type = '3'
          AND LENGTH(mmc.ic_no_new) < 12
          AND LENGTH(mmc.secondary_id_no) >= 12
          AND LENGTH(mmc.secondary_id_no) <= 15
          THEN '0' /* updated 20250502 */
          WHEN mmc.id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') <> ''
          AND LENGTH(mmc.secondary_id_no) < 12
          THEN '0' /* updated 20250502 */
          WHEN mmc.id_type = '3' AND LENGTH(mmc.ic_no_new) >= 12 AND LENGTH(mmc.ic_no_new) <= 15
          THEN '0' /* updated 20250502 */
          WHEN mmc.id_type = '4'
          THEN '0'
          WHEN mmc.id_type = '5'
          THEN '0'
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN '0'
          WHEN mmc.id_type = '6'
          THEN '0'
          WHEN COALESCE(mmc.id_type, '') = ''
          AND COALESCE(mmc.secondary_id_type, '') <> ''
          AND COALESCE(mmc.secondary_id_no, '') <> ''
          THEN '0'
          ELSE '1'
        END
      ) AS primary_identification_no_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_type
""")
spark.sql(f"""
/* 2.4 Create a temporary table temp_mhbos_m_client_secondary_identification_type to store the cleaned secondary identification type. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_type (
      `client_no` STRING,
      `id_type` STRING,
      `ic_no_new` STRING,
      `ic_no_old` STRING,
      `secondary_id_type` STRING,
      `secondary_id_no` STRING,
      `secondary_identification_type` STRING,
      `secondary_identification_type_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_type /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_type
    SELECT
      mmc.client_no,
      mmc.id_type,
      mmc.ic_no_new,
      mmc.ic_no_old,
      mmc.secondary_id_type,
      mmc.secondary_id_no,
      (
        CASE
          WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_old, '') <> ''
          THEN '2'
          WHEN mmc.id_type = '1' AND mmc.secondary_id_type = '1'
          THEN ''
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '2'
          THEN '3'
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '3'
          THEN '4'
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '4'
          THEN '5'
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '5'
          THEN '2'
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '6'
          THEN '6'
          WHEN mmc.id_type = '2'
          AND mmc.secondary_id_type = '1'
          AND COALESCE(mmc.ic_no_old, '') <> ''
          THEN '2'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN '3'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN '5'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN '3'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '2'
          THEN '3'
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN '3'
          WHEN mmc.id_type = '3'
          AND mmc.secondary_id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
          THEN ''
          WHEN mmc.id_type = '3'
          AND mmc.secondary_id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') <> COALESCE(mmc.secondary_id_no, '')
          THEN '4'
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type = '1'
          THEN '1'
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type = '2'
          THEN '3'
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type = '4'
          THEN '5'
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type = '5'
          THEN '2'
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type = '6'
          THEN '6'
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN '6'
          WHEN COALESCE(mmc.id_type, '') = ''
          AND COALESCE(mmc.secondary_id_type, '') <> ''
          AND COALESCE(mmc.secondary_id_no, '') <> ''
          THEN ''
          WHEN COALESCE(TRIM(mmc.secondary_id_type), '') = ''
          THEN ''
          ELSE mmc.secondary_id_type
        END
      ) AS secondary_identification_type,
      '0' /*
           (case when mmc.id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0'
                 when mmc.id_type = '1' and mmc.secondary_id_type = '1' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '2' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '3' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '4' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '5' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '6' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '0'
    			 when mmc.id_type = '2' and mmc.secondary_id_type = '2' then '0'
    			 when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '0'
                 when mmc.id_type = '3' then '0'
    			 when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '0'
    			 when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then '0'
    			 when nvl(trim(mmc.secondary_id_type), '') = '' then '0'
                 else '0'
             end)
            */ AS secondary_identification_type_flag /* 20250620 */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no
""")
spark.sql(f"""
/* 2.5 Create a temporary table temp_mhbos_m_client_secondary_identification_no to store the cleaned secondary identification number. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no (
      `client_no` STRING,
      `id_type` STRING,
      `ic_no_new` STRING,
      `ic_no_old` STRING,
      `secondary_id_type` STRING,
      `secondary_id_no` STRING,
      `secondary_identification_no` STRING,
      `secondary_identification_no_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no
    SELECT
      mmc.client_no,
      mmc.id_type,
      mmc.ic_no_new,
      mmc.ic_no_old,
      mmc.secondary_id_type,
      mmc.secondary_id_no,
      (
        CASE
          WHEN mmc.id_type = '1' AND COALESCE(mmc.ic_no_old, '') <> ''
          THEN mmc.ic_no_old
          WHEN mmc.id_type = '1' AND mmc.secondary_id_type = '1'
          THEN ''
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '2'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '3'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '4'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '5'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type IN ('1', '4', '5') AND mmc.secondary_id_type = '6'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2'
          AND mmc.secondary_id_type = '1'
          AND COALESCE(mmc.ic_no_old, '') <> ''
          THEN mmc.ic_no_old
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '3'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '4'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '5'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '2'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '2' AND mmc.secondary_id_type = '6'
          THEN mmc.ic_no_new
          WHEN mmc.id_type = '3'
          AND mmc.secondary_id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') = COALESCE(mmc.secondary_id_no, '')
          THEN ''
          WHEN mmc.id_type = '3'
          AND mmc.secondary_id_type = '3'
          AND LENGTH(mmc.ic_no_new) < 12
          AND LENGTH(mmc.secondary_id_no) >= 12
          AND LENGTH(mmc.secondary_id_no) <= 15
          THEN mmc.ic_no_new /* updated 20250502 */
          WHEN mmc.id_type = '3'
          AND mmc.secondary_id_type = '3'
          AND COALESCE(mmc.ic_no_new, '') <> COALESCE(mmc.secondary_id_no, '')
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '3' AND mmc.secondary_id_type <> '3'
          THEN mmc.secondary_id_no
          WHEN mmc.id_type = '6' AND mmc.secondary_id_type = '3'
          THEN mmc.ic_no_new
          WHEN COALESCE(mmc.id_type, '') = ''
          AND COALESCE(mmc.secondary_id_type, '') <> ''
          AND COALESCE(mmc.secondary_id_no, '') <> ''
          THEN ''
          WHEN COALESCE(TRIM(mmc.secondary_id_no), '') = ''
          THEN ''
          ELSE mmc.secondary_id_no
        END
      ) AS secondary_identification_no,
      '0' /*
           (case when mmc.id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0' 
                 when mmc.id_type = '1' and mmc.secondary_id_type = '1' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '2' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '3' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '4' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '5' then '0'
                 when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '6' then '0'
    			 when mmc.id_type = '2' and mmc.secondary_id_type = '2' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '0'
                 when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '0'
    			 when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '0'
                 when mmc.id_type = '3' then '0'
    			 when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '0'
    			 when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then '0'
    			 when nvl(trim(mmc.secondary_id_no), '') = '' then '0'
                 else '0'
             end )
          */ AS secondary_identification_no_flag /* 20250620 */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmc
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_identification_info
""")
spark.sql(f"""
/* 2.6 Create a temporary table temp_mhbos_m_client_identification_info to store the cleaned identification information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_identification_info (
      `client_no` STRING,
      `id_type` STRING,
      `ic_no_new` STRING,
      `ic_no_old` STRING,
      `secondary_id_type` STRING,
      `secondary_id_no` STRING,
      `primary_identification_type` STRING,
      `primary_identification_type_flag` STRING,
      `secondary_identification_type` STRING,
      `secondary_identification_type_flag` STRING,
      `primary_identification_no` STRING,
      `primary_identification_no_flag` STRING,
      `secondary_identification_no` STRING,
      `secondary_identification_no_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_identification_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_identification_info
    SELECT
      p_no.client_no,
      p_no.id_type,
      p_no.ic_no_new,
      p_no.ic_no_old,
      p_no.secondary_id_type,
      p_no.secondary_id_no,
      p_type.primary_identification_type,
      p_type.primary_identification_type_flag,
      s_type.secondary_identification_type,
      s_type.secondary_identification_type_flag,
      (
        CASE
          WHEN p_type.primary_identification_type = '1'
          THEN (
            CASE
              WHEN p_no.primary_identification_no RLIKE '^\\d+$'
              AND /* Determine whether the ID number is all numbers */ LENGTH(p_no.primary_identification_no) = 12
              AND /* Determine whether the ID number is 12 digits long */ (
                INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) >= 1
                AND INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) <= 12
              ) /* 1st till 6 digit represent date of birth in YYMMDD format */
              AND (
                INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) >= 1
                AND INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) <= 31
              )
              AND SUBSTRING(p_no.primary_identification_no, 7, 2) /* At 7th and 8th digit referring to the Place of Birth */ IN (
                '01',
                '21',
                '22',
                '23',
                '24',
                '02',
                '25',
                '26',
                '27',
                '03',
                '28',
                '29',
                '04',
                '30',
                '05',
                '31',
                '59',
                '06',
                '32',
                '33',
                '07',
                '34',
                '35',
                '08',
                '36',
                '37',
                '38',
                '39',
                '09',
                '40',
                '10',
                '41',
                '42',
                '43',
                '44',
                '11',
                '45',
                '46',
                '12',
                '47',
                '48',
                '49',
                '13',
                '50',
                '51',
                '52',
                '53',
                '14',
                '54',
                '55',
                '56',
                '57',
                '15',
                '58',
                '16',
                '60',
                '61',
                '62',
                '63',
                '64',
                '65',
                '66',
                '67',
                '68',
                '71',
                '72',
                '74',
                '75',
                '76',
                '77',
                '78',
                '79',
                '82',
                '83',
                '84',
                '85',
                '86',
                '87',
                '88',
                '89',
                '90',
                '91',
                '92',
                '93',
                '98',
                '99'
              )
              THEN p_no.primary_identification_no
              ELSE '@[' || p_no.primary_identification_no || ']'
            END
          )
          ELSE p_no.primary_identification_no
        END
      ) AS primary_identification_no,
      (
        CASE
          WHEN p_type.primary_identification_type = '1'
          THEN (
            CASE
              WHEN p_no.primary_identification_no RLIKE '^\\d+$'
              AND /* Determine whether the ID number is all numbers */ LENGTH(p_no.primary_identification_no) = 12
              AND /* Determine whether the ID number is 12 digits long */ (
                INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) >= 1
                AND INT(SUBSTRING(p_no.primary_identification_no, 3, 2)) <= 12
              ) /* 1st till 6 digit represent date of birth in YYMMDD format */
              AND (
                INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) >= 1
                AND INT(SUBSTRING(p_no.primary_identification_no, 5, 2)) <= 31
              )
              AND SUBSTRING(p_no.primary_identification_no, 7, 2) /* At 7th and 8th digit referring to the Place of Birth */ IN (
                '01',
                '21',
                '22',
                '23',
                '24',
                '02',
                '25',
                '26',
                '27',
                '03',
                '28',
                '29',
                '04',
                '30',
                '05',
                '31',
                '59',
                '06',
                '32',
                '33',
                '07',
                '34',
                '35',
                '08',
                '36',
                '37',
                '38',
                '39',
                '09',
                '40',
                '10',
                '41',
                '42',
                '43',
                '44',
                '11',
                '45',
                '46',
                '12',
                '47',
                '48',
                '49',
                '13',
                '50',
                '51',
                '52',
                '53',
                '14',
                '54',
                '55',
                '56',
                '57',
                '15',
                '58',
                '16',
                '60',
                '61',
                '62',
                '63',
                '64',
                '65',
                '66',
                '67',
                '68',
                '71',
                '72',
                '74',
                '75',
                '76',
                '77',
                '78',
                '79',
                '82',
                '83',
                '84',
                '85',
                '86',
                '87',
                '88',
                '89',
                '90',
                '91',
                '92',
                '93',
                '98',
                '99'
              )
              THEN '0'
              ELSE '1'
            END
          )
          ELSE p_no.primary_identification_no_flag
        END
      ) AS primary_identification_no_flag,
      s_no.secondary_identification_no AS secondary_identification_no, /*
           (case when s_type.secondary_identification_type  = '1' then -- identification type is '1'
                   (case when s_no.secondary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                              length(s_no.secondary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                              -- 1st till 6 digit represent date of birth in YYMMDD format
                              (int(substr(s_no.secondary_identification_no, 3, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 3, 2)) <= 12 ) and 
    						  (int(substr(s_no.secondary_identification_no, 5, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 5, 2)) <= 31 ) and  
                              -- At 7th and 8th digit referring to the Place of Birth
                              substr(s_no.secondary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                     '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                     '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                                s_no.secondary_identification_no
    			         when nvl(trim(s_no.secondary_identification_no), '') = '' then ''
                         else '@[' || s_no.secondary_identification_no || ']'
                    end)
                 else s_no.secondary_identification_no
             end ) as secondary_identification_no,
            
     20250620 */
      s_no.secondary_identification_no_flag /*
           (case when s_type.secondary_identification_type  = '1' then -- identification type is '1'
                   (case when s_no.secondary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                              length(s_no.secondary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                              -- 1st till 6 digit represent date of birth in YYMMDD format
                              (int(substr(s_no.secondary_identification_no, 3, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 3, 2)) <= 12 ) and 
    						  (int(substr(s_no.secondary_identification_no, 5, 2)) >= 1 and int(substr(s_no.secondary_identification_no, 5, 2)) <= 31 ) and  
                              -- At 7th and 8th digit referring to the Place of Birth
                              substr(s_no.secondary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                     '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                     '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                                '0'
    			         when nvl(trim(s_no.secondary_identification_no), '') = '' then '0'
                         else '1'
                    end)
                 else s_no.secondary_identification_no_flag 
             end ) as secondary_identification_no_flag
            */ AS secondary_identification_no_flag /* 20250620 */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_no AS p_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type AS p_type
      ON p_no.client_no = p_type.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_no AS s_no
      ON p_no.client_no = s_no.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_secondary_identification_type AS s_type
      ON p_no.client_no = s_type.client_no
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_dob_info
""")
spark.sql(f"""
/* 2.7 Create a temporary table temp_mhbos_m_client_dob_info to store the cleaned date of birth. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_dob_info (
      `client_no` STRING,
      `primary_identification_type` STRING,
      `primary_identification_no` STRING,
      `primary_identification_type_flag` STRING,
      `primary_identification_no_flag` STRING,
      `source_date_of_birth` TIMESTAMP,
      `date_of_birth` STRING,
      `date_of_birth_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_dob_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_dob_info
    SELECT
      id.client_no,
      id.primary_identification_type,
      id.primary_identification_no,
      id.primary_identification_type_flag,
      id.primary_identification_no_flag,
      mmce.date_of_birth AS source_date_of_birth,
      (
        CASE
          WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
          THEN DATE_FORMAT(
            FROM_UNIXTIME(
              UNIX_TIMESTAMP(SUBSTRING(id.primary_identification_no, 1, 6), 'yymmdd'),
              'yyyy-mm-dd'
            ),
            'yyyy-MM-dd'
          )
          WHEN NOT mmce.date_of_birth IS NULL
          THEN mmce.date_of_birth
          ELSE mmce.date_of_birth
        END
      ) AS date_of_birth,
      (
        CASE
          WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
          THEN '0'
          WHEN NOT mmce.date_of_birth IS NULL
          THEN '0'
          ELSE '0'
        END
      ) AS date_of_birth_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_identification_info AS id
    CROSS JOIN {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmce
    WHERE
      id.client_no = mmce.client_no
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_gender_info
""")
spark.sql(f"""
/* 2.8 Create a temporary table temp_mhbos_m_client_gender_info to store the cleaned gender. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_gender_info (
      `client_no` STRING,
      `primary_identification_type` STRING,
      `primary_identification_no` STRING,
      `primary_identification_type_flag` STRING,
      `primary_identification_no_flag` STRING,
      `source_sex` STRING,
      `sex` STRING,
      `sex_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_gender_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_gender_info
    SELECT
      id.client_no,
      id.primary_identification_type,
      id.primary_identification_no,
      id.primary_identification_type_flag,
      id.primary_identification_no_flag,
      mmca.sex AS source_sex,
      (
        CASE
          WHEN COALESCE(TRIM(mmca.sex), '') <> ''
          THEN (
            CASE
              WHEN mmca.sex = 'F'
              THEN 'FEMALE'
              WHEN mmca.sex = 'M'
              THEN 'MALE'
              ELSE '@[' || mmca.sex || ']'
            END
          )
          WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
          THEN (
            CASE
              WHEN SUBSTRING(id.primary_identification_no, 12, 1) IN ('1', '3', '5', '7', '9')
              THEN 'MALE'
              ELSE 'FEMALE'
            END
          )
          ELSE ''
        END
      ) AS sex,
      (
        CASE
          WHEN id.primary_identification_type = '1' AND id.primary_identification_no_flag = '0'
          THEN '0'
          WHEN COALESCE(TRIM(mmca.sex), '') <> ''
          THEN (
            CASE WHEN mmca.sex = 'F' THEN '0' WHEN mmca.sex = 'M' THEN '0' ELSE '1' END
          )
          ELSE '0'
        END
      ) AS sex_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_identification_info AS id
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
      ON id.client_no = mmca.client_no
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_1
""")
spark.sql(f"""
/* 2.9 Create a temporary table temp_mhbos_m_client_customer_name_1 to store the cleaned customer name. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_1 (
      `client_no` STRING,
      `client_type` STRING,
      `source_client_name` STRING,
      `source_client_name1` STRING,
      `source_client_name2` STRING,
      `source_client_name3` STRING,
      `primary_identification_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `client_name_flag` STRING,
      `client_name1_flag` STRING,
      `client_name2_flag` STRING,
      `client_name3_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_1 /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_1
    SELECT
      mmca.client_no,
      mmca.client_type,
      mmca.client_name AS source_client_name,
      mmca.client_name1 AS source_client_name1,
      mmca.client_name2 AS source_client_name2,
      mmca.client_name3 AS source_client_name3,
      tmmcpit.primary_identification_type,
      (
        CASE
          WHEN (
            tmmcpit.primary_identification_type = '1'
            AND COALESCE(mmca.client_name, '') <> ''
            AND COALESCE(mmca.client_name1, '') = ''
            AND COALESCE(mmca.client_name2, '') = ''
            AND COALESCE(mmca.client_name3, '') = ''
          )
          THEN TRIM(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(UPPER(TRIM(mmca.client_name)), '\\s*\\([^)]*\\)$', '') /* 20240723 changed from \\(.+\\) to \\(.*\\)$  --20241118 changes */,
                          ' MR$',
                          ''
                        ),
                        ' MDM$',
                        ''
                      ),
                      ' MS$',
                      ''
                    ),
                    ' PUAN$',
                    ''
                  ),
                  '^MR ',
                  ''
                ),
                '^DR ',
                ''
              ),
              '^DR. ',
              ''
            )
          )
          ELSE UPPER(TRIM(mmca.client_name))
        END
      ) AS client_name,
      UPPER(TRIM(mmca.client_name1)) AS client_name1, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name1 like '%CONDO%' or 
                       mmca.client_name1 like '%NO 1%' or
                       mmca.client_name1 like '%NO 2%' or
                       mmca.client_name1 like '%OFFICE%' or
                       mmca.client_name1 like '%FLR%') then 
                   '@[' || upper(mmca.client_name1) || ']'
                 else upper(trim(mmca.client_name1)) end) as client_name1,
            
     20250620 */
      UPPER(TRIM(mmca.client_name2)) AS client_name2, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name2 like '%CONDO%' or 
                       mmca.client_name2 like '%NO 1%' or
                       mmca.client_name2 like '%NO 2%' or
                       mmca.client_name2 like '%OFFICE%' or
                       mmca.client_name2 like '%FLR%') then 
                   '@[' || upper(mmca.client_name2) || ']'
                 else upper(trim(mmca.client_name2)) end)  as client_name2,
           
     20250620 */
      UPPER(TRIM(mmca.client_name3)) AS client_name3, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name3 like '%CONDO%' or 
                       mmca.client_name3 like '%NO 1%' or
                       mmca.client_name3 like '%NO 2%' or
                       mmca.client_name3 like '%OFFICE%' or
                       mmca.client_name3 like '%FLR%') then 
                   '@[' || upper(mmca.client_name3) || ']'
                 else upper(trim(mmca.client_name3)) end) as client_name3,
           
     20250620 */
      '0' AS client_name_flag, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name like '%CONDO%' or 
                       mmca.client_name like '%NO 1%' or
                       mmca.client_name like '%NO 2%' or
                       mmca.client_name like '%OFFICE%' or
                       mmca.client_name like '%FLR%') then 
                   '1'
                 else '0' end) as client_name_flag,
            
     20250620 */
      '0' AS client_name1_flag, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name1 like '%CONDO%' or 
                       mmca.client_name1 like '%NO 1%' or
                       mmca.client_name1 like '%NO 2%' or
                       mmca.client_name1 like '%OFFICE%' or
                       mmca.client_name1 like '%FLR%') then '1'
                 else '0' end) as client_name1_flag,
            
     20250620 */
      '0' AS client_name2_flag, /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name2 like '%CONDO%' or 
                       mmca.client_name2 like '%NO 1%' or
                       mmca.client_name2 like '%NO 2%' or
                       mmca.client_name2 like '%OFFICE%' or
                       mmca.client_name2 like '%FLR%') then '1'
                  else '0' end)  as client_name2_flag,
           
     20250620 */
      '0' /*
           (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
    	              (mmca.client_name3 like '%CONDO%' or 
                       mmca.client_name3 like '%NO 1%' or
                       mmca.client_name3 like '%NO 2%' or
                       mmca.client_name3 like '%OFFICE%' or
                       mmca.client_name3 like '%FLR%') then '1'
                  else '0' end) as client_name3_flag
          */ AS client_name3_flag /* 20250620 */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_primary_identification_type AS tmmcpit
      ON mmca.client_no = tmmcpit.client_no
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2
""")
spark.sql(f"""
/* 2.10 Create a temporary table temp_mhbos_m_client_customer_name_2 to store the second time cleaned customer name. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 (
      `client_no` STRING,
      `client_type` STRING,
      `customer_name_concatenate` STRING,
      `source_customer_name_concatenate` STRING,
      `customer_name` STRING,
      `source_client_name` STRING,
      `source_client_name1` STRING,
      `source_client_name2` STRING,
      `source_client_name3` STRING,
      `primary_identification_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `client_name_flag` STRING,
      `client_name1_flag` STRING,
      `client_name2_flag` STRING,
      `client_name3_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2
    SELECT
      t.client_no,
      t.client_type,
      t.customer_name_concatenate,
      t.source_customer_name_concatenate,
      (
        CASE
          WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
          THEN t.customer_name_concatenate
          WHEN t.customer_name_concatenate RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
          THEN t.customer_name_concatenate
          WHEN t.customer_name_concatenate RLIKE '.*(^| )FOR( +)(.+)$'
          THEN REGEXP_EXTRACT(t.customer_name_concatenate, '.*(^| )FOR( +)(.+)$', 3)
          WHEN t.customer_name_concatenate RLIKE '.*(^| )FOR$'
          THEN t.client_name
          WHEN (
            t.customer_name_concatenate RLIKE '.*(^| )NOMINEES( |$)'
            OR t.customer_name_concatenate RLIKE '.*(^| )PLEDGED( |$)'
            OR t.customer_name_concatenate RLIKE '.*(^| )TEMPATAN( |$)'
            OR t.customer_name_concatenate RLIKE '.*(^| )ASING( |$)'
          )
          AND COALESCE(TRIM(t.client_name1), '') <> ''
          THEN COALESCE(TRIM(t.client_name1), t.client_name)
          ELSE t.customer_name_concatenate
        END
      ) AS customer_name,
      t.source_client_name,
      t.source_client_name1,
      t.source_client_name2,
      t.source_client_name3,
      t.primary_identification_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.client_name_flag,
      t.client_name1_flag,
      t.client_name2_flag,
      t.client_name3_flag
    FROM (
      SELECT
        cn.*,
        TRIM(cn.client_name) || (
          CASE
            WHEN COALESCE(cn.client_name1, '') <> ''
            THEN (
              ' ' || cn.client_name1
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(cn.client_name2, '') <> ''
            THEN (
              ' ' || cn.client_name2
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(cn.client_name3, '') <> ''
            THEN (
              ' ' || cn.client_name3
            )
            ELSE ''
          END
        ) AS customer_name_concatenate,
        TRIM(cn.source_client_name) || (
          CASE
            WHEN COALESCE(cn.source_client_name1, '') <> ''
            THEN (
              ' ' || cn.source_client_name1
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(cn.source_client_name2, '') <> ''
            THEN (
              ' ' || cn.source_client_name2
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(cn.source_client_name3, '') <> ''
            THEN (
              ' ' || cn.source_client_name3
            )
            ELSE ''
          END
        ) AS source_customer_name_concatenate
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_1 AS cn
    ) AS t
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_race_info
""")
spark.sql(f"""
/* 2.11 Create a temporary table temp_mhbos_m_client_race_info to store the cleaned race information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_race_info (
      `client_no` STRING,
      `source_race` STRING,
      `race` STRING,
      `race_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_race_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_race_info
    SELECT
      mmca.client_no,
      mmca.race AS source_race,
      (
        CASE
          WHEN mmca.race = 'C'
          THEN 'CHINESE'
          WHEN mmca.race = 'F'
          THEN 'FOREIGNER'
          WHEN mmca.race = 'I'
          THEN 'INDIAN'
          WHEN mmca.race = 'M'
          THEN 'MALAY'
          WHEN mmca.race = 'O'
          THEN 'OTHERS'
          WHEN mmca.race = 'B'
          THEN 'BUMIPUTRA'
          WHEN cn2.customer_name RLIKE '(^| )A/P( |$)'
          THEN 'INDIAN'
          WHEN cn2.customer_name RLIKE '(^| )A/L( |$)'
          THEN 'INDIAN'
          WHEN cn2.customer_name RLIKE '(^| )S/O( |$)'
          THEN 'INDIAN'
          WHEN cn2.customer_name RLIKE '(^| )D/O( |$)'
          THEN 'INDIAN'
          ELSE NULL
        END
      ) AS race,
      '0' AS race_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 AS cn2
      ON mmca.client_no = cn2.client_no
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_clean
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info
""")
spark.sql(f"""
/* 2.12 Create a temporary table temp_mhbos_m_client_telephone_number_info to store the cleaned telephone number information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info (
      `client_no` STRING,
      `source_mobile_no` STRING,
      `source_fax_no` STRING,
      `source_tel_no_home` STRING,
      `source_tel_no_office` STRING,
      `mobile_no` STRING,
      `fax_no` STRING,
      `tel_no_home` STRING,
      `tel_no_office` STRING,
      `mobile_no_flag` STRING,
      `fax_no_flag` STRING,
      `tel_no_home_flag` STRING,
      `tel_no_office_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info /* 2.12.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info /* modify 20250502 removed flag logic */
    SELECT
      t.client_no,
      t.source_mobile_no,
      t.source_fax_no,
      t.source_tel_no_home,
      t.source_tel_no_office,
      t.mobile_no, /*  (case when t.mobile_no rlike '^[0-9+]+$' then t.mobile_no 
            when t.mobile_no like '%(0)%' then t.mobile_no 
            when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then t.mobile_no 
          when nvl(trim(t.mobile_no), '') = '' then '' 
            else '@[' || t.mobile_no || ']' end) as mobile_no, */
      t.fax_no, /*  (case when t.fax_no rlike '^[0-9+]+$' then t.fax_no 
            when t.fax_no like '%(0)%' then t.fax_no 
            when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then t.fax_no 
          when nvl(trim(t.fax_no), '') = '' then '' 
            else '@[' || t.fax_no || ']' end) as fax_no, */
      t.tel_no_home, /*  (case when t.tel_no_home rlike '^[0-9+]+$' then t.tel_no_home 
            when t.tel_no_home like '%(0)%' then t.tel_no_home 
            when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_home 
          when nvl(trim(t.tel_no_home), '') = '' then '' 
            else '@[' || t.tel_no_home || ']' end) as tel_no_home, */
      t.tel_no_office, /*  (case when t.tel_no_office rlike '^[0-9+]+$' then t.tel_no_office 
            when t.tel_no_office like '%(0)%' then t.tel_no_office 
            when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_office 
          when nvl(trim(t.tel_no_office), '') = '' then '' 
            else '@[' || t.tel_no_office || ']' end) as tel_no_office, */
      '0' AS mobile_no_flag, /*  (case when t.mobile_no rlike '^[0-9+]+$' then '0' 
            when t.mobile_no like '%(0)%' then '0' 
            when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then '0' 
          when nvl(trim(t.mobile_no), '') = '' then '0' 
            else '1' end) as mobile_no_flag, */
      '0' AS fax_no_flag, /*  (case when t.fax_no rlike '^[0-9+]+$' then '0' 
            when t.fax_no like '%(0)%' then '0' 
            when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then '0' 
          when nvl(trim(t.fax_no), '') = '' then '0' 
            else '1' end) as fax_no_flag, */
      '0' AS tel_no_home_flag, /*  (case when t.tel_no_home rlike '^[0-9+]+$' then '0' 
            when t.tel_no_home like '%(0)%' then '0' 
            when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then '0' 
          when nvl(trim(t.tel_no_home), '') = '' then '0' 
            else '1' end) as tel_no_home_flag, */
      '0' AS tel_no_office_flag /*  (case when t.tel_no_office rlike '^[0-9+]+$' then '0' 
            when t.tel_no_office like '%(0)%' then '0' 
            when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then '0' 
          when nvl(trim(t.tel_no_office), '') = '' then '0' 
            else '1' end) as tel_no_office_flag */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_telephone_clean AS t
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_clean
""")
spark.sql(f"""
/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_clean to store the cleaned email information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_clean (
      `client_no` STRING,
      `source_email` STRING,
      `einvoice_email` STRING, /* 20250903 einvoice_email */
      `email_1` STRING,
      `email_2` STRING,
      `email_3` STRING,
      `email_4` STRING,
      `email_5` STRING,
      `email_6` STRING,
      `email_7` STRING,
      `email_8` STRING,
      `email_9` STRING,
      `email_10` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_email_clean /* 2.13.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_email_clean
    SELECT
      t.client_no,
      t.source_email,
      t.einvoice_email, /* 20250903 einvoice_email */
      TRIM(SPLIT(t.email, ';')[0]) AS email_1,
      TRIM(SPLIT(t.email, ';')[1]) AS email_2,
      TRIM(SPLIT(t.email, ';')[2]) AS email_3,
      TRIM(SPLIT(t.email, ';')[3]) AS email_4,
      TRIM(SPLIT(t.email, ';')[4]) AS email_5,
      TRIM(SPLIT(t.email, ';')[5]) AS email_6,
      TRIM(SPLIT(t.email, ';')[6]) AS email_7,
      TRIM(SPLIT(t.email, ';')[7]) AS email_8,
      TRIM(SPLIT(t.email, ';')[8]) AS email_9,
      TRIM(SPLIT(t.email, ';')[9]) AS email_10
    FROM (
      SELECT
        mmce.client_no,
        REPLACE(LOWER(mmce.einvoice_email), ' ', '') AS einvoice_email, /* 20250903 einvoice_email */
        mmce.email AS source_email,
        REPLACE(LOWER(mmce.email), ' ', '') AS email
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmce
    ) AS t
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_info_1
""")
spark.sql(f"""
/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_info_1 to store the cleaned email information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_info_1 (
      `client_no` STRING,
      `source_email` STRING,
      `einvoice_email` STRING, /* 20250903 einvoice_email */
      `email_1` STRING,
      `email_2` STRING,
      `email_3` STRING,
      `email_4` STRING,
      `email_5` STRING,
      `email_6` STRING,
      `email_7` STRING,
      `email_8` STRING,
      `email_9` STRING,
      `email_10` STRING,
      `email_flag_1` STRING,
      `email_flag_2` STRING,
      `email_flag_3` STRING,
      `email_flag_4` STRING,
      `email_flag_5` STRING,
      `email_flag_6` STRING,
      `email_flag_7` STRING,
      `email_flag_8` STRING,
      `email_flag_9` STRING,
      `email_flag_10` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_email_info_1 /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_email_info_1
    SELECT
      t.client_no,
      t.source_email,
      (
        CASE
          WHEN t.einvoice_email RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.einvoice_email) <= 254
          THEN t.einvoice_email
          WHEN COALESCE(TRIM(t.einvoice_email), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS einvoice_email, /* 20250903 einvoice_email */
      (
        CASE
          WHEN t.email_1 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_1) <= 254
          THEN t.email_1
          WHEN COALESCE(TRIM(t.email_1), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_1,
      (
        CASE
          WHEN t.email_2 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_2) <= 254
          THEN t.email_2
          WHEN COALESCE(TRIM(t.email_2), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_2,
      (
        CASE
          WHEN t.email_3 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_3) <= 254
          THEN t.email_3
          WHEN COALESCE(TRIM(t.email_3), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_3,
      (
        CASE
          WHEN t.email_4 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_4) <= 254
          THEN t.email_4
          WHEN COALESCE(TRIM(t.email_4), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_4,
      (
        CASE
          WHEN t.email_5 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_5) <= 254
          THEN t.email_5
          WHEN COALESCE(TRIM(t.email_5), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_5,
      (
        CASE
          WHEN t.email_6 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_6) <= 254
          THEN t.email_6
          WHEN COALESCE(TRIM(t.email_6), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_6,
      (
        CASE
          WHEN t.email_7 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_7) <= 254
          THEN t.email_7
          WHEN COALESCE(TRIM(t.email_7), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_7,
      (
        CASE
          WHEN t.email_8 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_8) <= 254
          THEN t.email_8
          WHEN COALESCE(TRIM(t.email_8), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_8,
      (
        CASE
          WHEN t.email_9 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_9) <= 254
          THEN t.email_9
          WHEN COALESCE(TRIM(t.email_9), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_9,
      (
        CASE
          WHEN t.email_10 RLIKE '^.{1,64}\\@.+\\..+' AND LENGTH(t.email_10) <= 254
          THEN t.email_10
          WHEN COALESCE(TRIM(t.email_10), '') = ''
          THEN ''
          ELSE ''
        END
      ) AS email_10,
      '0' AS email_flag_1, /*       (case when t.email_1 rlike '^.{1,64}\\@.+\\..+' and length(t.email_1) <= 254 then '0'
    	         when nvl(trim(t.email_1), '') = '' then '0'
                 else '1' end) as email_flag_1,
    */
      '0' AS email_flag_2, /*       (case when t.email_2 rlike '^.{1,64}\\@.+\\..+' and length(t.email_2) <= 254 then '0'
    	         when nvl(trim(t.email_2), '') = '' then '0'
                 else '1' end) as email_flag_2,
    */
      '0' AS email_flag_3, /*       (case when t.email_3 rlike '^.{1,64}\\@.+\\..+' and length(t.email_3) <= 254 then '0'
    	         when nvl(trim(t.email_3), '') = '' then '0'
                 else '1' end) as email_flag_3,
    */
      '0' AS email_flag_4, /*       (case when t.email_4 rlike '^.{1,64}\\@.+\\..+' and length(t.email_4) <= 254 then '0'
    	         when nvl(trim(t.email_4), '') = '' then '0'
                 else '1' end) as email_flag_4,
    */
      '0' AS email_flag_5, /*       (case when t.email_5 rlike '^.{1,64}\\@.+\\..+' and length(t.email_5) <= 254 then '0'
    	         when nvl(trim(t.email_5), '') = '' then '0'
                 else '1' end) as email_flag_5,
    */
      '0' AS email_flag_6, /*       (case when t.email_6 rlike '^.{1,64}\\@.+\\..+' and length(t.email_6) <= 254 then '0'
    	         when nvl(trim(t.email_6), '') = '' then '0'
                 else '1' end) as email_flag_6,
    */
      '0' AS email_flag_7, /*       (case when t.email_7 rlike '^.{1,64}\\@.+\\..+' and length(t.email_7) <= 254 then '0'
    	         when nvl(trim(t.email_7), '') = '' then '0'
                 else '1' end) as email_flag_7,
    */
      '0' AS email_flag_8, /*       (case when t.email_8 rlike '^.{1,64}\\@.+\\..+' and length(t.email_8) <= 254 then '0'
    	         when nvl(trim(t.email_8), '') = '' then '0'
                 else '1' end) as email_flag_8,
    */
      '0' AS email_flag_9, /*       (case when t.email_9 rlike '^.{1,64}\\@.+\\..+' and length(t.email_9) <= 254 then '0'
    	         when nvl(trim(t.email_9), '') = '' then '0'
                 else '1' end) as email_flag_9,
    */
      '0' AS email_flag_10 /*       (case when t.email_10 rlike '^.{1,64}\\@.+\\..+' and length(t.email_10) <= 254 then '0'
    	         when nvl(trim(t.email_10), '') = '' then '0'
                 else '1' end) as email_flag_10
    */
    FROM (
      SELECT
        e.client_no,
        e.source_email,
        (
          CASE
            WHEN NOT e.einvoice_email LIKE '%@%'
            THEN ''
            WHEN e.einvoice_email LIKE '%@hotmail'
            THEN e.einvoice_email || '.com'
            WHEN e.einvoice_email LIKE '%@gmail'
            THEN e.einvoice_email || '.com'
            WHEN e.einvoice_email LIKE '%@yahoo'
            THEN e.einvoice_email || '.com'
            WHEN e.einvoice_email RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.einvoice_email, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.einvoice_email RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.einvoice_email, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.einvoice_email RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.einvoice_email, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.einvoice_email
          END
        ) AS einvoice_email, /* 20250903 einvoice_email */
        (
          CASE
            WHEN NOT e.email_1 LIKE '%@%'
            THEN ''
            WHEN e.email_1 LIKE '%@hotmail'
            THEN e.email_1 || '.com'
            WHEN e.email_1 LIKE '%@gmail'
            THEN e.email_1 || '.com'
            WHEN e.email_1 LIKE '%@yahoo'
            THEN e.email_1 || '.com'
            WHEN e.email_1 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_1, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_1 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_1, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_1 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_1, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_1
          END
        ) AS email_1,
        (
          CASE
            WHEN NOT e.email_2 LIKE '%@%'
            THEN ''
            WHEN e.email_2 LIKE '%@hotmail'
            THEN e.email_2 || '.com'
            WHEN e.email_2 LIKE '%@gmail'
            THEN e.email_2 || '.com'
            WHEN e.email_2 LIKE '%@yahoo'
            THEN e.email_2 || '.com'
            WHEN e.email_2 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_2, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_2 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_2, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_2 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_2, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_2
          END
        ) AS email_2,
        (
          CASE
            WHEN NOT e.email_3 LIKE '%@%'
            THEN ''
            WHEN e.email_3 LIKE '%@hotmail'
            THEN e.email_3 || '.com'
            WHEN e.email_3 LIKE '%@gmail'
            THEN e.email_3 || '.com'
            WHEN e.email_3 LIKE '%@yahoo'
            THEN e.email_3 || '.com'
            WHEN e.email_3 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_3, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_3 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_3, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_3 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_3, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_3
          END
        ) AS email_3,
        (
          CASE
            WHEN NOT e.email_4 LIKE '%@%'
            THEN ''
            WHEN e.email_4 LIKE '%@hotmail'
            THEN e.email_4 || '.com'
            WHEN e.email_4 LIKE '%@gmail'
            THEN e.email_4 || '.com'
            WHEN e.email_4 LIKE '%@yahoo'
            THEN e.email_4 || '.com'
            WHEN e.email_4 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_4, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_4 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_4, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_4 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_4, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_4
          END
        ) AS email_4,
        (
          CASE
            WHEN NOT e.email_5 LIKE '%@%'
            THEN ''
            WHEN e.email_5 LIKE '%@hotmail'
            THEN e.email_5 || '.com'
            WHEN e.email_5 LIKE '%@gmail'
            THEN e.email_5 || '.com'
            WHEN e.email_5 LIKE '%@yahoo'
            THEN e.email_5 || '.com'
            WHEN e.email_5 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_5, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_5 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_5, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_5 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_5, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_5
          END
        ) AS email_5,
        (
          CASE
            WHEN NOT e.email_6 LIKE '%@%'
            THEN ''
            WHEN e.email_6 LIKE '%@hotmail'
            THEN e.email_6 || '.com'
            WHEN e.email_6 LIKE '%@gmail'
            THEN e.email_6 || '.com'
            WHEN e.email_6 LIKE '%@yahoo'
            THEN e.email_6 || '.com'
            WHEN e.email_6 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_6, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_6 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_6, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_6 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_6, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_6
          END
        ) AS email_6,
        (
          CASE
            WHEN NOT e.email_7 LIKE '%@%'
            THEN ''
            WHEN e.email_7 LIKE '%@hotmail'
            THEN e.email_7 || '.com'
            WHEN e.email_7 LIKE '%@gmail'
            THEN e.email_7 || '.com'
            WHEN e.email_7 LIKE '%@yahoo'
            THEN e.email_7 || '.com'
            WHEN e.email_7 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_7, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_7 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_7, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_7 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_7, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_7
          END
        ) AS email_7,
        (
          CASE
            WHEN NOT e.email_8 LIKE '%@%'
            THEN ''
            WHEN e.email_8 LIKE '%@hotmail'
            THEN e.email_8 || '.com'
            WHEN e.email_8 LIKE '%@gmail'
            THEN e.email_8 || '.com'
            WHEN e.email_8 LIKE '%@yahoo'
            THEN e.email_8 || '.com'
            WHEN e.email_8 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_8, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_8 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_8, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_8 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_8, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_8
          END
        ) AS email_8,
        (
          CASE
            WHEN NOT e.email_9 LIKE '%@%'
            THEN ''
            WHEN e.email_9 LIKE '%@hotmail'
            THEN e.email_9 || '.com'
            WHEN e.email_9 LIKE '%@gmail'
            THEN e.email_9 || '.com'
            WHEN e.email_9 LIKE '%@yahoo'
            THEN e.email_9 || '.com'
            WHEN e.email_9 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_9, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_9 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_9, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_9 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_9, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_9
          END
        ) AS email_9,
        (
          CASE
            WHEN NOT e.email_10 LIKE '%@%'
            THEN ''
            WHEN e.email_10 LIKE '%@hotmail'
            THEN e.email_10 || '.com'
            WHEN e.email_10 LIKE '%@gmail'
            THEN e.email_10 || '.com'
            WHEN e.email_10 LIKE '%@yahoo'
            THEN e.email_10 || '.com'
            WHEN e.email_10 RLIKE '\\@hotmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_10, '\\@hotmail[^\\.]+', '@hotmail.com')
            WHEN e.email_10 RLIKE '\\@gmail[^\\.]+'
            THEN REGEXP_REPLACE(e.email_10, '\\@gmail[^\\.]+', '@gmail.com')
            WHEN e.email_10 RLIKE '\\@yahoo[^\\.]+'
            THEN REGEXP_REPLACE(e.email_10, '\\@yahoo[^\\.]+', '@yahoo.com')
            ELSE e.email_10
          END
        ) AS email_10
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_email_clean AS e
    ) AS t
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_info
""")
spark.sql(f"""
/* 2.13 Create a temporary table temp_t_mhbos_m_client_email_info to store the cleaned email information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_email_info (
      `client_no` STRING,
      `source_email` STRING,
      `einvoice_email` STRING, /* 20250903 einvoice_email */
      `email_1` STRING,
      `email_2` STRING,
      `email_3` STRING,
      `email_4` STRING,
      `email_5` STRING,
      `email_6` STRING,
      `email_7` STRING,
      `email_8` STRING,
      `email_9` STRING,
      `email_10` STRING,
      `email_flag` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_email_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_email_info
    SELECT
      t.client_no,
      t.source_email,
      t.einvoice_email, /* 20250903 einvoice_email */
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
      (
        CASE
          WHEN (
            t.email_flag_1 || t.email_flag_2 || t.email_flag_3 || t.email_flag_4 || t.email_flag_5 || t.email_flag_6 || t.email_flag_7 || t.email_flag_8 || t.email_flag_9 || t.email_flag_10
          ) LIKE '%1%'
          THEN '1'
          ELSE '0'
        END
      ) AS email_flag
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_email_info_1 AS t
    WHERE
      1 = 1
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_address_city
""")
spark.sql(f"""
/* 2.14 Create a temporary table temp_t_mhbos_m_client_address_city to store the cleaned address cityinformation. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_address_city (
      `client_no` STRING,
      `addr1` STRING,
      `addr2` STRING,
      `addr3` STRING,
      `addr4` STRING,
      `postcode` STRING,
      `mailing_addr` STRING,
      `city` STRING,
      `state` STRING,
      `perm_addr1` STRING,
      `perm_addr2` STRING,
      `perm_addr3` STRING,
      `perm_addr4` STRING,
      `perm_postcode` STRING,
      `registered_addr` STRING,
      `perm_city` STRING,
      `perm_state` STRING,
      `perm_country` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_address_city /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_address_city
    SELECT
      addr.client_no,
      COALESCE(addr.addr1, addr.addr2, addr.addr3, addr.addr4, '') AS addr1,
      (
        CASE
          WHEN NOT addr.addr1 IS NULL
          THEN COALESCE(addr.addr2, addr.addr3, addr.addr4, '')
          WHEN addr.addr1 IS NULL AND NOT addr.addr2 IS NULL
          THEN COALESCE(addr.addr3, addr.addr4, '')
          WHEN addr.addr1 IS NULL AND addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
          THEN COALESCE(addr.addr4, '')
          ELSE ''
        END
      ) AS addr2,
      (
        CASE
          WHEN NOT addr.addr1 IS NULL AND NOT addr.addr2 IS NULL
          THEN COALESCE(addr.addr3, addr.addr4, '')
          WHEN addr.addr1 IS NULL AND NOT addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
          THEN COALESCE(addr.addr4, '')
          ELSE ''
        END
      ) AS addr3,
      (
        CASE
          WHEN NOT addr.addr1 IS NULL AND NOT addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
          THEN COALESCE(addr.addr4, '')
          ELSE ''
        END
      ) AS addr4,
      addr.postcode,
      addr.mailing_addr,
      REGEXP_EXTRACT(
        UPPER(addr.mailing_addr),
        '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b'
      ) AS city,
      addr.state_code AS state, /*	   regexp_extract(upper(addr.mailing_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) as state, 
     20251013 */
      COALESCE(addr.perm_addr1, addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '') AS perm_addr1,
      (
        CASE
          WHEN NOT addr.perm_addr1 IS NULL
          THEN COALESCE(addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '')
          WHEN addr.perm_addr1 IS NULL AND NOT addr.perm_addr2 IS NULL
          THEN COALESCE(addr.perm_addr3, addr.perm_addr4, '')
          WHEN addr.perm_addr1 IS NULL
          AND addr.perm_addr2 IS NULL
          AND NOT addr.perm_addr3 IS NULL
          THEN COALESCE(addr.perm_addr4, '')
          ELSE ''
        END
      ) AS perm_addr2,
      (
        CASE
          WHEN NOT addr.perm_addr1 IS NULL AND NOT addr.perm_addr2 IS NULL
          THEN COALESCE(addr.perm_addr3, addr.perm_addr4, '')
          WHEN addr.perm_addr1 IS NULL
          AND NOT addr.perm_addr2 IS NULL
          AND NOT addr.perm_addr3 IS NULL
          THEN COALESCE(addr.perm_addr4, '')
          ELSE ''
        END
      ) AS perm_addr3,
      (
        CASE
          WHEN NOT addr.perm_addr1 IS NULL
          AND NOT addr.perm_addr2 IS NULL
          AND NOT addr.perm_addr3 IS NULL
          THEN COALESCE(addr.perm_addr4, '')
          ELSE ''
        END
      ) AS perm_addr4,
      addr.perm_postcode,
      addr.registered_addr,
      CASE
        WHEN COALESCE(TRIM(addr.perm_city), '') <> ''
        THEN addr.perm_city
        ELSE REGEXP_EXTRACT(
          UPPER(addr.registered_addr),
          '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b'
        )
      END AS perm_city, /* 20250715 */
      addr.perm_state_code AS perm_state, /*	     case when nvl(trim(addr.perm_state_code),'') <> '' then addr.perm_state_code else regexp_extract(upper(addr.registered_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) end as perm_state, -- 20250715 
     20251013 */
      addr.perm_country /* 20250715 */
    FROM (
      SELECT
        ca.client_no,
        (
          CASE WHEN COALESCE(TRIM(ca.addr1), '') = '' THEN NULL ELSE ca.addr1 END
        ) AS addr1,
        (
          CASE WHEN COALESCE(TRIM(ca.addr2), '') = '' THEN NULL ELSE ca.addr2 END
        ) AS addr2,
        (
          CASE WHEN COALESCE(TRIM(ca.addr3), '') = '' THEN NULL ELSE ca.addr3 END
        ) AS addr3,
        (
          CASE WHEN COALESCE(TRIM(ca.addr4), '') = '' THEN NULL ELSE ca.addr4 END
        ) AS addr4,
        ca.postcode,
        (
          COALESCE(ca.addr1, '') || (
            CASE WHEN COALESCE(ca.addr2, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr2, '') END
          ) || (
            CASE WHEN COALESCE(ca.addr3, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr3, '') END
          ) || (
            CASE WHEN COALESCE(ca.addr4, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr4, '') END
          )
        ) AS mailing_addr,
        ca.state_code,
        (
          CASE WHEN COALESCE(TRIM(ca.perm_addr1), '') = '' THEN NULL ELSE ca.perm_addr1 END
        ) AS perm_addr1,
        (
          CASE WHEN COALESCE(TRIM(ca.perm_addr2), '') = '' THEN NULL ELSE ca.perm_addr2 END
        ) AS perm_addr2,
        (
          CASE WHEN COALESCE(TRIM(ca.perm_addr3), '') = '' THEN NULL ELSE ca.perm_addr3 END
        ) AS perm_addr3,
        (
          CASE WHEN COALESCE(TRIM(ca.perm_addr4), '') = '' THEN NULL ELSE ca.perm_addr4 END
        ) AS perm_addr4,
        ca.perm_postcode,
        (
          COALESCE(ca.perm_addr1, '') || (
            CASE
              WHEN COALESCE(ca.perm_addr2, '') = ''
              THEN ''
              ELSE ' ' || COALESCE(ca.perm_addr2, '')
            END
          ) || (
            CASE
              WHEN COALESCE(ca.perm_addr3, '') = ''
              THEN ''
              ELSE ' ' || COALESCE(ca.perm_addr3, '')
            END
          ) || (
            CASE
              WHEN COALESCE(ca.perm_addr4, '') = ''
              THEN ''
              ELSE ' ' || COALESCE(ca.perm_addr4, '')
            END
          )
        ) AS registered_addr,
        ca.perm_city,
        ca.perm_state_code,
        ca.perm_country
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS ca
    ) AS addr
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_address_info
""")
spark.sql(f"""
/* 2.15 Create a temporary table temp_t_mhbos_m_client_address_info to store the cleaned cityinformation. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_address_info (
      `client_no` STRING,
      `addr1` STRING,
      `addr2` STRING,
      `addr3` STRING,
      `addr4` STRING,
      `postcode` STRING,
      `mailing_addr` STRING,
      `city` STRING,
      `state` STRING,
      `perm_addr1` STRING,
      `perm_addr2` STRING,
      `perm_addr3` STRING,
      `perm_addr4` STRING,
      `perm_postcode` STRING,
      `registered_addr` STRING,
      `perm_city` STRING,
      `perm_state` STRING,
      `perm_country` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_address_info /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_address_info
    SELECT
      addr.client_no,
      addr.addr1,
      addr.addr2,
      addr.addr3,
      addr.addr4,
      addr.postcode,
      addr.mailing_addr,
      addr.city,
      addr.state, /*       (case when nvl(trim(addr.state), '') = '' then mp1.target_cd_val
    	         when trim(addr.state) = 'MALACCA' then 'Melaka'
    			 when trim(addr.state) = 'WP' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
    			 else addr.state end) as state,

     20251013 */
      addr.perm_addr1,
      addr.perm_addr2,
      addr.perm_addr3,
      addr.perm_addr4,
      addr.perm_postcode,
      addr.registered_addr,
      addr.perm_city,
      addr.perm_state, /*       (case when nvl(trim(addr.perm_state), '') = '' then mp2.target_cd_val
    	         when trim(addr.perm_state) = 'MALACCA' then 'Melaka'
    			 when trim(addr.perm_state) = 'WP' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.perm_state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.perm_state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
    			 when trim(addr.perm_state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
    			 else addr.perm_state end) as perm_state,

     20251013 */
      addr.perm_country
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_address_city AS addr
    LEFT JOIN {params["com_schema"]}.t_ref_pub_cd_map AS mp1
      ON addr.city = mp1.src_code_val
      AND mp1.subj = 'com'
      AND mp1.src_sys_cd = 'mhbos'
      AND mp1.src_tab_en_name = 'r_mhbos_m_client'
      AND mp1.src_field_en_name = 'city'
      AND mp1.valid_flag = 'Y'
    LEFT JOIN {params["com_schema"]}.t_ref_pub_cd_map AS mp2
      ON addr.perm_city = mp2.src_code_val
      AND mp2.subj = 'com'
      AND mp2.src_sys_cd = 'mhbos'
      AND mp2.src_tab_en_name = 'r_mhbos_m_client'
      AND mp2.src_field_en_name = 'city'
      AND mp2.valid_flag = 'Y'
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1
""")
spark.sql(f"""
/* 2.16 Detect the Kenanga nominees name from account_full_name and remove them 
     create temporary table temp_t_mhbos_m_client_nominees_info_1 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `kenanga_noms_list_1` STRING,
      `kenanga_noms_list_2` STRING,
      `kenanga_noms_list_3` STRING,
      `kenanga_noms_list_4` STRING,
      `kenanga_noms_list_5` STRING,
      `kenanga_noms_list_6` STRING,
      `kenanga_noms_list_7` STRING,
      `kenanga_noms_list_8` STRING,
      `kenanga_noms_list_9` STRING,
      `kenanga_noms_list_10` STRING,
      `kenanga_noms_list_11` STRING,
      `kenanga_noms_list_12` STRING,
      `kenanga_noms_list_13` STRING,
      `kenanga_noms_list_14` STRING,
      `kenanga_noms_list_15` STRING,
      `kenanga_noms_list_16` STRING,
      `kenanga_noms_list_17` STRING,
      `kenanga_noms_list_18` STRING,
      `kenanga_noms_list_19` STRING,
      `kenanga_noms_list_20` STRING,
      `kenanga_noms_list_21` STRING,
      `kenanga_noms_list_22` STRING,
      `kenanga_noms_list_23` STRING,
      `kenanga_noms_list_24` STRING,
      `kenanga_noms_list_25` STRING,
      `kenanga_noms_list_26` STRING,
      `kenanga_noms_list_27` STRING,
      `kenanga_noms_list_28` STRING,
      `kenanga_noms_list_29` STRING,
      `kenanga_noms_list_30` STRING,
      `kenanga_noms_list_31` STRING,
      `kenanga_noms_list_32` STRING,
      `kenanga_noms_list_33` STRING,
      `kenanga_noms_list_34` STRING,
      `kenanga_noms_list_35` STRING,
      `kenanga_noms_list_36` STRING,
      `kenanga_noms_list_37` STRING,
      `kenanga_noms_list_38` STRING,
      `kenanga_noms_list_39` STRING,
      `kenanga_noms_list_40` STRING,
      `kenanga_noms_list_41` STRING,
      `kenanga_noms_list_42` STRING,
      `kenanga_noms_list_43` STRING,
      `kenanga_noms_list_44` STRING,
      `kenanga_noms_list_45` STRING,
      `kenanga_noms_list_46` STRING,
      `kenanga_noms_list_47` STRING,
      `kenanga_noms_list_48` STRING,
      `kenanga_noms_list_49` STRING,
      `kenanga_noms_list_50` STRING,
      `kenanga_noms_list_51` STRING,
      `kenanga_noms_list_52` STRING,
      `kenanga_noms_list_53` STRING,
      `kenanga_noms_list_54` STRING,
      `kenanga_noms_list_55` STRING,
      `kenanga_noms_list_56` STRING,
      `kenanga_noms_list_57` STRING,
      `kenanga_noms_list_58` STRING,
      `kenanga_noms_list_59` STRING,
      `kenanga_noms_list_60` STRING,
      `kenanga_noms_list_61` STRING,
      `kenanga_noms_list_62` STRING,
      `kenanga_noms_list_63` STRING,
      `kenanga_noms_list_64` STRING,
      `kenanga_noms_list_65` STRING,
      `kenanga_noms_list_66` STRING,
      `kenanga_noms_list_67` STRING,
      `kenanga_noms_list_68` STRING,
      `kenanga_noms_list_69` STRING,
      `kenanga_noms_list_70` STRING,
      `kenanga_noms_list_71` STRING,
      `kenanga_noms_list_72` STRING,
      `kenanga_noms_list_73` STRING,
      `kenanga_noms_list_74` STRING,
      `kenanga_noms_list_75` STRING,
      `kenanga_noms_list_76` STRING,
      `kenanga_noms_list_77` STRING,
      `kenanga_noms_list_78` STRING,
      `kenanga_noms_list_79` STRING,
      `kenanga_noms_list_80` STRING,
      `kenanga_noms_list_81` STRING,
      `kenanga_noms_list_82` STRING,
      `kenanga_noms_list_83` STRING,
      `kenanga_noms_list_84` STRING,
      `kenanga_noms_list_85` STRING,
      `kenanga_noms_list_86` STRING,
      `kenanga_noms_list_87` STRING,
      `kenanga_noms_list_88` STRING,
      `kenanga_noms_list_89` STRING,
      `kenanga_noms_list_90` STRING,
      `kenanga_noms_list_91` STRING,
      `kenanga_noms_list_92` STRING,
      `kenanga_noms_list_93` STRING,
      `kenanga_noms_list_94` STRING,
      `kenanga_noms_list_95` STRING,
      `kenanga_noms_list_96` STRING,
      `kenanga_noms_list_97` STRING,
      `kenanga_noms_list_98` STRING,
      `kenanga_noms_list_99` STRING,
      `kenanga_noms_list_100` STRING,
      `kenanga_noms_list_101` STRING,
      `kenanga_noms_list_102` STRING,
      `kenanga_noms_list_103` STRING,
      `kenanga_noms_list_104` STRING,
      `kenanga_noms_list_105` STRING,
      `kenanga_noms_list_106` STRING,
      `kenanga_noms_list_107` STRING,
      `kenanga_noms_list_108` STRING,
      `kenanga_noms_list_109` STRING,
      `kenanga_noms_list_110` STRING,
      `kenanga_noms_list_111` STRING,
      `noms_ind` STRING,
      `noms` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1 /* truncate temporary table */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_1 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1
    SELECT
      t1.client_no,
      t1.client_type,
      t1.client_name,
      t1.client_name1,
      t1.client_name2,
      t1.client_name3,
      t1.customer_name_concatenate,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS(A)SDN BHD FOR', '') AS kenanga_noms_list_1,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (T) S/B', '') AS kenanga_noms_list_2,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (ASING) SDN BHD', '') AS kenanga_noms_list_3,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (A) SDN BHD', '') AS kenanga_noms_list_4,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (A) S/B', '') AS kenanga_noms_list_5,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (A)-', '') AS kenanga_noms_list_6,
      REPLACE(t1.customer_name_concatenate, 'KNK NOMS (A)', '') AS kenanga_noms_list_7,
      REPLACE(t1.customer_name_concatenate, 'KENNAGA NOMINEES (TEMPATAN) SDN BHD', '') AS kenanga_noms_list_8,
      REPLACE(t1.customer_name_concatenate, 'KENNAGA NOMINEES (ASING) SDN BHD', '') AS kenanga_noms_list_9,
      REPLACE(t1.customer_name_concatenate, 'KENANGE NOMINEES (T) SDN BHD', '') AS kenanga_noms_list_10,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS(T) SB FOR', '') AS kenanga_noms_list_11,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS(A) S/B FOR', '') AS kenanga_noms_list_12,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS(A) S/B', '') AS kenanga_noms_list_13,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS(A) FOR', '') AS kenanga_noms_list_14,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T) SDN BHD A/C ', '') AS kenanga_noms_list_15,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T) SDN BHD', '') AS kenanga_noms_list_16,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T) SB FOR', '') AS kenanga_noms_list_17,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T) S/B FOR', '') AS kenanga_noms_list_18,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T) S/B', '') AS kenanga_noms_list_19,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T)  S/B FOR', '') AS kenanga_noms_list_20,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (T)', '') AS kenanga_noms_list_21,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (TEMPATAN) SDN BHD', '') AS kenanga_noms_list_22,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (ASING) SDN BHD', '') AS kenanga_noms_list_23,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (ASING) S/B', '') AS kenanga_noms_list_24,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (A) SDN BHD FOR', '') AS kenanga_noms_list_25,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (A) SDN BHD', '') AS kenanga_noms_list_26,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (A) SB', '') AS kenanga_noms_list_27,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (A) S/B FOR', '') AS kenanga_noms_list_28,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS (A) S/B', '') AS kenanga_noms_list_29,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMS  (T) S/B FOR', '') AS kenanga_noms_list_30,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMNIEES (ASING) SDN BHD', '') AS kenanga_noms_list_31,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINNES (ASING) SDN BHD', '') AS kenanga_noms_list_32,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINNEES (ASING) SDN BHD', '') AS kenanga_noms_list_33,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINIEES (ASING) SDN BHD', '') AS kenanga_noms_list_34,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINESS (TEMPATAN) SDN BHD FOR', '') AS kenanga_noms_list_35,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINESS (TEMPATAN) SDN BHD', '') AS kenanga_noms_list_36,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINESS (ASING) SDN BHD', '') AS kenanga_noms_list_37,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINESS ( ASING) SDN BHD FOR', '') AS kenanga_noms_list_38,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINES (ASING) SDN BHD FOR', '') AS kenanga_noms_list_39,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINES (ASING) SDN BHD', '') AS kenanga_noms_list_40,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN)SDN BHD', '') AS kenanga_noms_list_41,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN) SDN BHD FOR', '') AS kenanga_noms_list_42,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN) SDN BHD', '') AS kenanga_noms_list_43,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING)SDN BHD FOR', '') AS kenanga_noms_list_44,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING)SDN BHD', '') AS kenanga_noms_list_45,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING) SDN BHD FOR', '') AS kenanga_noms_list_46,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING) SDN BHD', '') AS kenanga_noms_list_47,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) SDN BHD FOR', '') AS kenanga_noms_list_48,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) SDN BHD', '') AS kenanga_noms_list_49,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) S/B FOR', '') AS kenanga_noms_list_50,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES TEMPATAN SDN BHD FOR', '') AS kenanga_noms_list_51,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES TEMPATAN SDN BHD', '') AS kenanga_noms_list_52,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD FOR', '') AS kenanga_noms_list_53,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD F0R', '') AS kenanga_noms_list_54,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD BHD FOR', '') AS kenanga_noms_list_55,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD A/C FOR', '') AS kenanga_noms_list_56,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD A/C', '') AS kenanga_noms_list_57,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD  A/C', '') AS kenanga_noms_list_58,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD', '') AS kenanga_noms_list_59,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B FOR', '') AS kenanga_noms_list_60,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B A/C', '') AS kenanga_noms_list_61,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B  A/C', '') AS kenanga_noms_list_62,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B', '') AS kenanga_noms_list_63,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) A/B', '') AS kenanga_noms_list_64,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (T)  SDN BHD FOR', '') AS kenanga_noms_list_65,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN)SDN BHD', '') AS kenanga_noms_list_66,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN.BHD.', '') AS kenanga_noms_list_67,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN. BHD.', '') AS kenanga_noms_list_68,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD FOR', '') AS kenanga_noms_list_69,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD A/C FOR', '') AS kenanga_noms_list_70,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD', '') AS kenanga_noms_list_71,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN SDN BHD FOR', '') AS kenanga_noms_list_72,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN ) SDN BHD', '') AS kenanga_noms_list_73,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)SDN BHD FOR', '') AS kenanga_noms_list_74,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)SDN BHD', '') AS kenanga_noms_list_75,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SND BHD', '') AS kenanga_noms_list_76,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SN BHD', '') AS kenanga_noms_list_77,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN. BHD.', '') AS kenanga_noms_list_78,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD FOR', '') AS kenanga_noms_list_79,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD A/C FOR', '') AS kenanga_noms_list_80,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD A/C', '') AS kenanga_noms_list_81,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD  FOR', '') AS kenanga_noms_list_82,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD  A/C FOR', '') AS kenanga_noms_list_83,
      REGEXP_REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES \\(ASING\\) SDN BHD\'', '') AS kenanga_noms_list_84,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD', '') AS kenanga_noms_list_85,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) S/B FOR', '') AS kenanga_noms_list_86,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) S/B', '') AS kenanga_noms_list_87,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) FOR', '') AS kenanga_noms_list_88,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)', '') AS kenanga_noms_list_89,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING ) SDN BHD', '') AS kenanga_noms_list_90,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASIGN) SDN BHD', '') AS kenanga_noms_list_91,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASIAN) SDN BHD', '') AS kenanga_noms_list_92,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD FOR', '') AS kenanga_noms_list_93,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD  FOR', '') AS kenanga_noms_list_94,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD', '') AS kenanga_noms_list_95,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) S/B FOR', '') AS kenanga_noms_list_96,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) S/B', '') AS kenanga_noms_list_97,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( T) SDN BHD', '') AS kenanga_noms_list_98,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( TEMPATAN) SDN BHD', '') AS kenanga_noms_list_99,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( TEMPATAN ) SDN BHD', '') AS kenanga_noms_list_100,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( ASING) SDN BHD', '') AS kenanga_noms_list_101,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( ASING ) SDN BHD FOR', '') AS kenanga_noms_list_102,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES ( A ) SDN BHD', '') AS kenanga_noms_list_103,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEE (TEMPATAN) SDN BHD FOR', '') AS kenanga_noms_list_104,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMIMEES (T) SDN BHD', '') AS kenanga_noms_list_105,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMIMEES (TEMPATAN) SDN BHD', '') AS kenanga_noms_list_106,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOM (T) SDN BHD FOR', '') AS kenanga_noms_list_107,
      REPLACE(t1.customer_name_concatenate, 'KENANAGA NOMINEES (T) SDN BHD', '') AS kenanga_noms_list_108,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN  BHD', '') AS kenanga_noms_list_109,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD -', '') AS kenanga_noms_list_110,
      REPLACE(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD A/C', '') AS kenanga_noms_list_111,
      CASE WHEN t1.noms = 'T' THEN 'Y' WHEN t1.noms = 'A' THEN 'Y' ELSE 'N' END AS noms_ind,
      t1.noms
    FROM (
      SELECT
        cn.*,
        UPPER(TRIM(cn.client_name)) || (
          CASE
            WHEN COALESCE(UPPER(TRIM(cn.client_name1)), '') <> ''
            THEN (
              ' ' || UPPER(TRIM(cn.client_name1))
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(UPPER(TRIM(cn.client_name2)), '') <> ''
            THEN (
              ' ' || UPPER(TRIM(cn.client_name2))
            )
            ELSE ''
          END
        ) || (
          CASE
            WHEN COALESCE(UPPER(TRIM(cn.client_name3)), '') <> ''
            THEN (
              ' ' || UPPER(TRIM(cn.client_name3))
            )
            ELSE ''
          END
        ) AS customer_name_concatenate,
        mmca.noms_type,
        mmca.bene_type
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS cn
      LEFT JOIN {params["com_schema"]}.t_mhbos_m_mcd_client_addr AS mmca
        ON cn.client_no = mmca.client_no AND mmca.etl_dt = '{batch_date}'
    ) AS t1
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2_1
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_2_1 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2_1 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `replace_field_length` INT
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2_1 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_2_1 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_2_1 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2_1
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      CASE WHEN t.noms = 'T' THEN 'TEMPATAN' WHEN t.noms = 'A' THEN 'ASING' ELSE NULL END AS nominees_type,
      LEAST(
        LENGTH(kenanga_noms_list_1),
        LENGTH(kenanga_noms_list_2),
        LENGTH(kenanga_noms_list_3),
        LENGTH(kenanga_noms_list_4),
        LENGTH(kenanga_noms_list_5),
        LENGTH(kenanga_noms_list_6),
        LENGTH(kenanga_noms_list_7),
        LENGTH(kenanga_noms_list_8),
        LENGTH(kenanga_noms_list_9),
        LENGTH(kenanga_noms_list_10),
        LENGTH(kenanga_noms_list_11),
        LENGTH(kenanga_noms_list_12),
        LENGTH(kenanga_noms_list_13),
        LENGTH(kenanga_noms_list_14),
        LENGTH(kenanga_noms_list_15),
        LENGTH(kenanga_noms_list_16),
        LENGTH(kenanga_noms_list_17),
        LENGTH(kenanga_noms_list_18),
        LENGTH(kenanga_noms_list_19),
        LENGTH(kenanga_noms_list_20),
        LENGTH(kenanga_noms_list_21),
        LENGTH(kenanga_noms_list_22),
        LENGTH(kenanga_noms_list_23),
        LENGTH(kenanga_noms_list_24),
        LENGTH(kenanga_noms_list_25),
        LENGTH(kenanga_noms_list_26),
        LENGTH(kenanga_noms_list_27),
        LENGTH(kenanga_noms_list_28),
        LENGTH(kenanga_noms_list_29),
        LENGTH(kenanga_noms_list_30),
        LENGTH(kenanga_noms_list_31),
        LENGTH(kenanga_noms_list_32),
        LENGTH(kenanga_noms_list_33),
        LENGTH(kenanga_noms_list_34),
        LENGTH(kenanga_noms_list_35),
        LENGTH(kenanga_noms_list_36),
        LENGTH(kenanga_noms_list_37),
        LENGTH(kenanga_noms_list_38),
        LENGTH(kenanga_noms_list_39),
        LENGTH(kenanga_noms_list_40),
        LENGTH(kenanga_noms_list_41),
        LENGTH(kenanga_noms_list_42),
        LENGTH(kenanga_noms_list_43),
        LENGTH(kenanga_noms_list_44),
        LENGTH(kenanga_noms_list_45),
        LENGTH(kenanga_noms_list_46),
        LENGTH(kenanga_noms_list_47),
        LENGTH(kenanga_noms_list_48),
        LENGTH(kenanga_noms_list_49),
        LENGTH(kenanga_noms_list_50),
        LENGTH(kenanga_noms_list_51),
        LENGTH(kenanga_noms_list_52),
        LENGTH(kenanga_noms_list_53),
        LENGTH(kenanga_noms_list_54),
        LENGTH(kenanga_noms_list_55),
        LENGTH(kenanga_noms_list_56),
        LENGTH(kenanga_noms_list_57),
        LENGTH(kenanga_noms_list_58),
        LENGTH(kenanga_noms_list_59),
        LENGTH(kenanga_noms_list_60),
        LENGTH(kenanga_noms_list_61),
        LENGTH(kenanga_noms_list_62),
        LENGTH(kenanga_noms_list_63),
        LENGTH(kenanga_noms_list_64),
        LENGTH(kenanga_noms_list_65),
        LENGTH(kenanga_noms_list_66),
        LENGTH(kenanga_noms_list_67),
        LENGTH(kenanga_noms_list_68),
        LENGTH(kenanga_noms_list_69),
        LENGTH(kenanga_noms_list_70),
        LENGTH(kenanga_noms_list_71),
        LENGTH(kenanga_noms_list_72),
        LENGTH(kenanga_noms_list_73),
        LENGTH(kenanga_noms_list_74),
        LENGTH(kenanga_noms_list_75),
        LENGTH(kenanga_noms_list_76),
        LENGTH(kenanga_noms_list_77),
        LENGTH(kenanga_noms_list_78),
        LENGTH(kenanga_noms_list_79),
        LENGTH(kenanga_noms_list_80),
        LENGTH(kenanga_noms_list_81),
        LENGTH(kenanga_noms_list_82),
        LENGTH(kenanga_noms_list_83),
        LENGTH(kenanga_noms_list_84),
        LENGTH(kenanga_noms_list_85),
        LENGTH(kenanga_noms_list_86),
        LENGTH(kenanga_noms_list_87),
        LENGTH(kenanga_noms_list_88),
        LENGTH(kenanga_noms_list_89),
        LENGTH(kenanga_noms_list_90),
        LENGTH(kenanga_noms_list_91),
        LENGTH(kenanga_noms_list_92),
        LENGTH(kenanga_noms_list_93),
        LENGTH(kenanga_noms_list_94),
        LENGTH(kenanga_noms_list_95),
        LENGTH(kenanga_noms_list_96),
        LENGTH(kenanga_noms_list_97),
        LENGTH(kenanga_noms_list_98),
        LENGTH(kenanga_noms_list_99),
        LENGTH(kenanga_noms_list_100),
        LENGTH(kenanga_noms_list_101),
        LENGTH(kenanga_noms_list_102),
        LENGTH(kenanga_noms_list_103),
        LENGTH(kenanga_noms_list_104),
        LENGTH(kenanga_noms_list_105),
        LENGTH(kenanga_noms_list_106),
        LENGTH(kenanga_noms_list_107),
        LENGTH(kenanga_noms_list_108),
        LENGTH(kenanga_noms_list_109),
        LENGTH(kenanga_noms_list_110),
        LENGTH(kenanga_noms_list_111)
      ) AS replace_field_length
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1 AS t /* where noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_2 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `remove_kenanga_nominees_name` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_2 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_2 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t1.nominees_type,
      (
        CASE
          WHEN LENGTH(t.kenanga_noms_list_1) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_1)
          WHEN LENGTH(t.kenanga_noms_list_2) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_2)
          WHEN LENGTH(t.kenanga_noms_list_3) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_3)
          WHEN LENGTH(t.kenanga_noms_list_4) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_4)
          WHEN LENGTH(t.kenanga_noms_list_5) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_5)
          WHEN LENGTH(t.kenanga_noms_list_6) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_6)
          WHEN LENGTH(t.kenanga_noms_list_7) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_7)
          WHEN LENGTH(t.kenanga_noms_list_8) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_8)
          WHEN LENGTH(t.kenanga_noms_list_9) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_9)
          WHEN LENGTH(t.kenanga_noms_list_10) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_10)
          WHEN LENGTH(t.kenanga_noms_list_11) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_11)
          WHEN LENGTH(t.kenanga_noms_list_12) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_12)
          WHEN LENGTH(t.kenanga_noms_list_13) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_13)
          WHEN LENGTH(t.kenanga_noms_list_14) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_14)
          WHEN LENGTH(t.kenanga_noms_list_15) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_15)
          WHEN LENGTH(t.kenanga_noms_list_16) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_16)
          WHEN LENGTH(t.kenanga_noms_list_17) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_17)
          WHEN LENGTH(t.kenanga_noms_list_18) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_18)
          WHEN LENGTH(t.kenanga_noms_list_19) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_19)
          WHEN LENGTH(t.kenanga_noms_list_20) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_20)
          WHEN LENGTH(t.kenanga_noms_list_21) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_21)
          WHEN LENGTH(t.kenanga_noms_list_22) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_22)
          WHEN LENGTH(t.kenanga_noms_list_23) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_23)
          WHEN LENGTH(t.kenanga_noms_list_24) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_24)
          WHEN LENGTH(t.kenanga_noms_list_25) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_25)
          WHEN LENGTH(t.kenanga_noms_list_26) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_26)
          WHEN LENGTH(t.kenanga_noms_list_27) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_27)
          WHEN LENGTH(t.kenanga_noms_list_28) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_28)
          WHEN LENGTH(t.kenanga_noms_list_29) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_29)
          WHEN LENGTH(t.kenanga_noms_list_30) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_30)
          WHEN LENGTH(t.kenanga_noms_list_31) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_31)
          WHEN LENGTH(t.kenanga_noms_list_32) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_32)
          WHEN LENGTH(t.kenanga_noms_list_33) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_33)
          WHEN LENGTH(t.kenanga_noms_list_34) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_34)
          WHEN LENGTH(t.kenanga_noms_list_35) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_35)
          WHEN LENGTH(t.kenanga_noms_list_36) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_36)
          WHEN LENGTH(t.kenanga_noms_list_37) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_37)
          WHEN LENGTH(t.kenanga_noms_list_38) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_38)
          WHEN LENGTH(t.kenanga_noms_list_39) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_39)
          WHEN LENGTH(t.kenanga_noms_list_40) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_40)
          WHEN LENGTH(t.kenanga_noms_list_41) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_41)
          WHEN LENGTH(t.kenanga_noms_list_42) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_42)
          WHEN LENGTH(t.kenanga_noms_list_43) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_43)
          WHEN LENGTH(t.kenanga_noms_list_44) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_44)
          WHEN LENGTH(t.kenanga_noms_list_45) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_45)
          WHEN LENGTH(t.kenanga_noms_list_46) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_46)
          WHEN LENGTH(t.kenanga_noms_list_47) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_47)
          WHEN LENGTH(t.kenanga_noms_list_48) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_48)
          WHEN LENGTH(t.kenanga_noms_list_49) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_49)
          WHEN LENGTH(t.kenanga_noms_list_50) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_50)
          WHEN LENGTH(t.kenanga_noms_list_51) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_51)
          WHEN LENGTH(t.kenanga_noms_list_52) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_52)
          WHEN LENGTH(t.kenanga_noms_list_53) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_53)
          WHEN LENGTH(t.kenanga_noms_list_54) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_54)
          WHEN LENGTH(t.kenanga_noms_list_55) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_55)
          WHEN LENGTH(t.kenanga_noms_list_56) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_56)
          WHEN LENGTH(t.kenanga_noms_list_57) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_57)
          WHEN LENGTH(t.kenanga_noms_list_58) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_58)
          WHEN LENGTH(t.kenanga_noms_list_59) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_59)
          WHEN LENGTH(t.kenanga_noms_list_60) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_60)
          WHEN LENGTH(t.kenanga_noms_list_61) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_61)
          WHEN LENGTH(t.kenanga_noms_list_62) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_62)
          WHEN LENGTH(t.kenanga_noms_list_63) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_63)
          WHEN LENGTH(t.kenanga_noms_list_64) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_64)
          WHEN LENGTH(t.kenanga_noms_list_65) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_65)
          WHEN LENGTH(t.kenanga_noms_list_66) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_66)
          WHEN LENGTH(t.kenanga_noms_list_67) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_67)
          WHEN LENGTH(t.kenanga_noms_list_68) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_68)
          WHEN LENGTH(t.kenanga_noms_list_69) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_69)
          WHEN LENGTH(t.kenanga_noms_list_70) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_70)
          WHEN LENGTH(t.kenanga_noms_list_71) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_71)
          WHEN LENGTH(t.kenanga_noms_list_72) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_72)
          WHEN LENGTH(t.kenanga_noms_list_73) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_73)
          WHEN LENGTH(t.kenanga_noms_list_74) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_74)
          WHEN LENGTH(t.kenanga_noms_list_75) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_75)
          WHEN LENGTH(t.kenanga_noms_list_76) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_76)
          WHEN LENGTH(t.kenanga_noms_list_77) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_77)
          WHEN LENGTH(t.kenanga_noms_list_78) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_78)
          WHEN LENGTH(t.kenanga_noms_list_79) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_79)
          WHEN LENGTH(t.kenanga_noms_list_80) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_80)
          WHEN LENGTH(t.kenanga_noms_list_81) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_81)
          WHEN LENGTH(t.kenanga_noms_list_82) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_82)
          WHEN LENGTH(t.kenanga_noms_list_83) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_83)
          WHEN LENGTH(t.kenanga_noms_list_84) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_84)
          WHEN LENGTH(t.kenanga_noms_list_85) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_85)
          WHEN LENGTH(t.kenanga_noms_list_86) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_86)
          WHEN LENGTH(t.kenanga_noms_list_87) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_87)
          WHEN LENGTH(t.kenanga_noms_list_88) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_88)
          WHEN LENGTH(t.kenanga_noms_list_89) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_89)
          WHEN LENGTH(t.kenanga_noms_list_90) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_90)
          WHEN LENGTH(t.kenanga_noms_list_91) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_91)
          WHEN LENGTH(t.kenanga_noms_list_92) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_92)
          WHEN LENGTH(t.kenanga_noms_list_93) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_93)
          WHEN LENGTH(t.kenanga_noms_list_94) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_94)
          WHEN LENGTH(t.kenanga_noms_list_95) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_95)
          WHEN LENGTH(t.kenanga_noms_list_96) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_96)
          WHEN LENGTH(t.kenanga_noms_list_97) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_97)
          WHEN LENGTH(t.kenanga_noms_list_98) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_98)
          WHEN LENGTH(t.kenanga_noms_list_99) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_99)
          WHEN LENGTH(t.kenanga_noms_list_100) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_100)
          WHEN LENGTH(t.kenanga_noms_list_101) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_101)
          WHEN LENGTH(t.kenanga_noms_list_102) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_102)
          WHEN LENGTH(t.kenanga_noms_list_103) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_103)
          WHEN LENGTH(t.kenanga_noms_list_104) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_104)
          WHEN LENGTH(t.kenanga_noms_list_105) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_105)
          WHEN LENGTH(t.kenanga_noms_list_106) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_106)
          WHEN LENGTH(t.kenanga_noms_list_107) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_107)
          WHEN LENGTH(t.kenanga_noms_list_108) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_108)
          WHEN LENGTH(t.kenanga_noms_list_109) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_109)
          WHEN LENGTH(t.kenanga_noms_list_110) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_110)
          WHEN LENGTH(t.kenanga_noms_list_111) = t1.replace_field_length
          THEN TRIM(t.kenanga_noms_list_111)
          ELSE TRIM(t.customer_name_concatenate)
        END
      ) AS remove_kenanga_nominees_name
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_1 AS t
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2_1 AS t1
      ON t.client_no = t1.client_no /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_3 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `remove_kenanga_nominees_name` STRING,
      `pledged_name_list_1` STRING,
      `pledged_name_list_2` STRING,
      `pledged_name_list_39` STRING, /* add_20240611 */
      `pledged_name_list_3` STRING,
      `pledged_name_list_4` STRING,
      `pledged_name_list_5` STRING,
      `pledged_name_list_6` STRING,
      `pledged_name_list_7` STRING,
      `pledged_name_list_8` STRING,
      `pledged_name_list_9` STRING,
      `pledged_name_list_42` STRING, /* add_20240620 */
      `pledged_name_list_10` STRING,
      `pledged_name_list_11` STRING,
      `pledged_name_list_12` STRING,
      `pledged_name_list_13` STRING,
      `pledged_name_list_14` STRING,
      `pledged_name_list_15` STRING,
      `pledged_name_list_16` STRING,
      `pledged_name_list_17` STRING,
      `pledged_name_list_18` STRING,
      `pledged_name_list_41` STRING, /* add_20240828 */
      `pledged_name_list_19` STRING,
      `pledged_name_list_20` STRING,
      `pledged_name_list_21` STRING,
      `pledged_name_list_22` STRING,
      `pledged_name_list_23` STRING,
      `pledged_name_list_24` STRING,
      `pledged_name_list_25` STRING,
      `pledged_name_list_26` STRING,
      `pledged_name_list_27` STRING,
      `pledged_name_list_28` STRING,
      `pledged_name_list_29` STRING,
      `pledged_name_list_30` STRING,
      `pledged_name_list_31` STRING,
      `pledged_name_list_32` STRING,
      `pledged_name_list_33` STRING,
      `pledged_name_list_34` STRING,
      `pledged_name_list_35` STRING,
      `pledged_name_list_36` STRING, /* add_20240321 */
      `pledged_name_list_37` STRING, /* add_20240321 */
      `pledged_name_list_38` STRING, /* add_20240518 */
      `pledged_name_list_40` STRING /* add_20240828 */
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_3 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_3 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t.nominees_type,
      t.remove_kenanga_nominees_name,
      REPLACE(t.remove_kenanga_nominees_name, '(PLEDGED)', '') AS pledged_name_list_1,
      REPLACE(t.remove_kenanga_nominees_name, '(PLEDGE)', '') AS pledged_name_list_2,
      REPLACE(
        t.remove_kenanga_nominees_name,
        'PLEDGED SECURITIES A/C FOR PLEDGED SECURITIES ACCOUNT',
        ''
      ) AS pledged_name_list_39, /* add_20240611 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLEGDED SECURITIES ACCOUNT', '') AS pledged_name_list_3,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITISED ACCOUNT', '') AS pledged_name_list_4,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SDN BHD', '') AS pledged_name_list_5,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SA/C', '') AS pledged_name_list_6,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') AS pledged_name_list_7,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') AS pledged_name_list_8,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') AS pledged_name_list_9,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT - ', '') AS pledged_name_list_42, /* add_20250620 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') AS pledged_name_list_10,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') AS pledged_name_list_11,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT ', '') AS pledged_name_list_12,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCCOUNT', '') AS pledged_name_list_13,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACC', '') AS pledged_name_list_14,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES A/C', '') AS pledged_name_list_15,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES  ACCOUNT', '') AS pledged_name_list_16,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') AS pledged_name_list_17,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURITES ACCOUNT', '') AS pledged_name_list_18,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECURUTIES ACCOUNT', '') AS pledged_name_list_41, /* add_20240828 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECS ACCOUNT', '') AS pledged_name_list_19,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SECS A/C', '') AS pledged_name_list_20,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC. A/C', '') AS pledged_name_list_21,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC', '') AS pledged_name_list_22,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC A/C ', '') AS pledged_name_list_23,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED SEC  A/C', '') AS pledged_name_list_24,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGED', '') AS pledged_name_list_25,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES ACCOUNT', '') AS pledged_name_list_26,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES A/C', '') AS pledged_name_list_27,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE SEC A/C', '') AS pledged_name_list_28,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDGE', '') AS pledged_name_list_29,
      REPLACE(t.remove_kenanga_nominees_name, 'PLED GED SECURITIES ACCOUNT', '') AS pledged_name_list_30,
      REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC', '') AS pledged_name_list_31,
      REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC AC ', '') AS pledged_name_list_32,
      REPLACE(t.remove_kenanga_nominees_name, 'PLDG SEC A/C ', '') AS pledged_name_list_33,
      REPLACE(t.remove_kenanga_nominees_name, 'PLDG A/C', '') AS pledged_name_list_34,
      REPLACE(t.remove_kenanga_nominees_name, 'PL SEC A/C - ', '') AS pledged_name_list_35,
      REPLACE(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES ACCOUNT', '') AS pledged_name_list_36, /* add_20240321 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES A/C', '') AS pledged_name_list_37, /* add_20240321 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLEDEGD SECURITIES ACCOUNT', '') AS pledged_name_list_38, /* add_20240518 */
      REPLACE(t.remove_kenanga_nominees_name, 'PLGD SEC ACC', '') AS pledged_name_list_40 /* add_20240828 */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_2 AS t /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4_1
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_4_1 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4_1 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `remove_pledged_name_length` INT
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4_1 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_4_1 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4_1 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4_1
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t.nominees_type,
      LEAST(
        LENGTH(pledged_name_list_1),
        LENGTH(pledged_name_list_2),
        LENGTH(pledged_name_list_3),
        LENGTH(pledged_name_list_4),
        LENGTH(pledged_name_list_5),
        LENGTH(pledged_name_list_6),
        LENGTH(pledged_name_list_7),
        LENGTH(pledged_name_list_8),
        LENGTH(pledged_name_list_9),
        LENGTH(pledged_name_list_10),
        LENGTH(pledged_name_list_11),
        LENGTH(pledged_name_list_12),
        LENGTH(pledged_name_list_13),
        LENGTH(pledged_name_list_14),
        LENGTH(pledged_name_list_15),
        LENGTH(pledged_name_list_16),
        LENGTH(pledged_name_list_17),
        LENGTH(pledged_name_list_18),
        LENGTH(pledged_name_list_19),
        LENGTH(pledged_name_list_20),
        LENGTH(pledged_name_list_21),
        LENGTH(pledged_name_list_22),
        LENGTH(pledged_name_list_23),
        LENGTH(pledged_name_list_24),
        LENGTH(pledged_name_list_25),
        LENGTH(pledged_name_list_26),
        LENGTH(pledged_name_list_27),
        LENGTH(pledged_name_list_28),
        LENGTH(pledged_name_list_29),
        LENGTH(pledged_name_list_30),
        LENGTH(pledged_name_list_31),
        LENGTH(pledged_name_list_32),
        LENGTH(pledged_name_list_33),
        LENGTH(pledged_name_list_34),
        LENGTH(pledged_name_list_35),
        LENGTH(pledged_name_list_36) /* add_20240321 */,
        LENGTH(pledged_name_list_37) /* add_20240321 */,
        LENGTH(pledged_name_list_38) /* add_20240518    */,
        LENGTH(pledged_name_list_39) /* add_20240611 */,
        LENGTH(pledged_name_list_40) /* add_20240828      */,
        LENGTH(pledged_name_list_41) /* add_20240828         */,
        LENGTH(pledged_name_list_42) /* add_20250620                     */
      ) AS remove_pledged_name_length
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3 AS t /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_4 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `pledged_securities_flag` STRING,
      `remove_kenanga_nominees_name` STRING,
      `remove_pledged_name` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_4 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t.nominees_type,
      (
        CASE
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_1)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_2)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_3)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_4)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_5)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_6)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_7)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_8)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_9)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_10)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_11)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_12)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_13)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_14)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_15)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_16)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_17)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_18)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_19)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_20)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_21)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_22)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_23)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_24)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_25)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_26)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_27)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_28)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_29)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_30)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_31)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_32)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_33)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_34)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_35)
          THEN 'Y'
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_36)
          THEN 'Y' /* add_20240321 */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_37)
          THEN 'Y' /* add_20240321 */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_38)
          THEN 'Y' /* add_20240518  */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_39)
          THEN 'Y' /* add_20240611 */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_40)
          THEN 'Y' /* add_20240828   */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_41)
          THEN 'Y' /* add_20240828          */
          WHEN LENGTH(t.remove_kenanga_nominees_name) <> LENGTH(t.pledged_name_list_42)
          THEN 'Y' /* add_20250620   */
          ELSE 'N'
        END
      ) AS pledged_securities_flag,
      t.remove_kenanga_nominees_name,
      (
        CASE
          WHEN LENGTH(t.pledged_name_list_1) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_1)
          WHEN LENGTH(t.pledged_name_list_2) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_2)
          WHEN LENGTH(t.pledged_name_list_3) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_3)
          WHEN LENGTH(t.pledged_name_list_4) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_4)
          WHEN LENGTH(t.pledged_name_list_5) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_5)
          WHEN LENGTH(t.pledged_name_list_6) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_6)
          WHEN LENGTH(t.pledged_name_list_7) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_7)
          WHEN LENGTH(t.pledged_name_list_8) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_8)
          WHEN LENGTH(t.pledged_name_list_9) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_9)
          WHEN LENGTH(t.pledged_name_list_10) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_10)
          WHEN LENGTH(t.pledged_name_list_11) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_11)
          WHEN LENGTH(t.pledged_name_list_12) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_12)
          WHEN LENGTH(t.pledged_name_list_13) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_13)
          WHEN LENGTH(t.pledged_name_list_14) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_14)
          WHEN LENGTH(t.pledged_name_list_15) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_15)
          WHEN LENGTH(t.pledged_name_list_16) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_16)
          WHEN LENGTH(t.pledged_name_list_17) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_17)
          WHEN LENGTH(t.pledged_name_list_18) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_18)
          WHEN LENGTH(t.pledged_name_list_19) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_19)
          WHEN LENGTH(t.pledged_name_list_20) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_20)
          WHEN LENGTH(t.pledged_name_list_21) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_21)
          WHEN LENGTH(t.pledged_name_list_22) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_22)
          WHEN LENGTH(t.pledged_name_list_23) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_23)
          WHEN LENGTH(t.pledged_name_list_24) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_24)
          WHEN LENGTH(t.pledged_name_list_25) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_25)
          WHEN LENGTH(t.pledged_name_list_26) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_26)
          WHEN LENGTH(t.pledged_name_list_27) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_27)
          WHEN LENGTH(t.pledged_name_list_28) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_28)
          WHEN LENGTH(t.pledged_name_list_29) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_29)
          WHEN LENGTH(t.pledged_name_list_30) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_30)
          WHEN LENGTH(t.pledged_name_list_31) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_31)
          WHEN LENGTH(t.pledged_name_list_32) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_32)
          WHEN LENGTH(t.pledged_name_list_33) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_33)
          WHEN LENGTH(t.pledged_name_list_34) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_34)
          WHEN LENGTH(t.pledged_name_list_35) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_35)
          WHEN LENGTH(t.pledged_name_list_36) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_36) /* add_20240321 */
          WHEN LENGTH(t.pledged_name_list_37) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_37) /* add_20240321 */
          WHEN LENGTH(t.pledged_name_list_38) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_38) /* add_20240518 */
          WHEN LENGTH(t.pledged_name_list_39) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_39) /* add_20240611 */
          WHEN LENGTH(t.pledged_name_list_40) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_40) /* add_20240828 */
          WHEN LENGTH(t.pledged_name_list_41) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_41) /* add_20240828 */
          WHEN LENGTH(t.pledged_name_list_42) = t1.remove_pledged_name_length
          THEN TRIM(t.pledged_name_list_42) /* add_20250620 */
          ELSE TRIM(t.remove_kenanga_nominees_name)
        END
      ) AS remove_pledged_name
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_3 AS t
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4_1 AS t1
      ON t.client_no = t1.client_no /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_5
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info_5 */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_5 (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `pledged_securities_flag` STRING,
      `remove_kenanga_nominees_name` STRING,
      `remove_pledged_name` STRING,
      `replace_dsal_handling` STRING,
      `cleaned_nominees_name` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_5 /* trunate temporary table temp_t_mhbos_m_client_nominees_info_5 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_5 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_5
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t.nominees_type,
      t.pledged_securities_flag,
      t.remove_kenanga_nominees_name,
      t.remove_pledged_name,
      REGEXP_REPLACE(t.remove_pledged_name, '(^| )DSAL( |$)', ' ') AS replace_dsal_handling,
      TRIM(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(REGEXP_REPLACE(TRIM(t.remove_pledged_name), '^FOR ', ''), 'FOR EXEMPT AN FOR', 'FOR'),
              'EXEMPT AN FOR',
              'FOR'
            ),
            'RSS/SBL EXEMPT AN FOR',
            'FOR'
          ),
          'RSS/SBL FOR',
          'FOR'
        )
      ) AS cleaned_nominees_name
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_4 AS t /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_nominees_info */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info (
      `client_no` STRING,
      `client_type` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `noms_ind` STRING,
      `nominees_type` STRING,
      `pledged_securities_flag` STRING,
      `remove_kenanga_nominees_name` STRING,
      `remove_pledged_name` STRING,
      `replace_dsal_handling` STRING,
      `cleaned_nominees_name` STRING,
      `principal_name` STRING,
      `intermediary_name` STRING,
      `beneficiary_name` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info /* trunate temporary table temp_t_mhbos_m_client_nominees_info_5 */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info
    SELECT
      t.client_no,
      t.client_type,
      t.client_name,
      t.client_name1,
      t.client_name2,
      t.client_name3,
      t.customer_name_concatenate,
      t.noms_ind,
      t.nominees_type,
      t.pledged_securities_flag,
      t.remove_kenanga_nominees_name,
      t.remove_pledged_name,
      t.replace_dsal_handling,
      t.cleaned_nominees_name,
      (
        CASE
          WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
          THEN t.cleaned_nominees_name
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
          THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$'))
          WHEN t.cleaned_nominees_name RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
          THEN t.cleaned_nominees_name
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+)$'
          THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+)$'))
          ELSE t.cleaned_nominees_name
        END
      ) AS principal_name,
      (
        CASE
          WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
          THEN NULL
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR[ ]+([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
          THEN NULL
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
          THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 2))
          ELSE NULL
        END
      ) AS intermediary_name,
      (
        CASE
          WHEN t.client_type IN ('#', '0', '1', '6', '8', 'V')
          THEN NULL
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$'
          THEN TRIM(
            REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$', 2)
          )
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+) FOR (.+)$'
          THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 3))
          WHEN t.cleaned_nominees_name RLIKE '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$'
          THEN NULL
          WHEN t.cleaned_nominees_name RLIKE '^(.+) FOR (.+)$'
          THEN TRIM(REGEXP_EXTRACT(t.cleaned_nominees_name, '^(.+) FOR (.+)$', 2))
          ELSE NULL
        END
      ) AS beneficiary_name
    FROM (
      SELECT
        client_no,
        client_type,
        client_name,
        client_name1,
        client_name2,
        client_name3,
        customer_name_concatenate,
        noms_ind,
        nominees_type,
        pledged_securities_flag,
        remove_kenanga_nominees_name,
        remove_pledged_name,
        replace_dsal_handling,
        (
          CASE
            WHEN cleaned_nominees_name LIKE 'FOR %'
            THEN SUBSTRING(cleaned_nominees_name, 5)
            ELSE cleaned_nominees_name
          END
        ) AS cleaned_nominees_name
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info_5
    ) AS t /* where t.noms_ind = 'Y' */
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_name_info
""")
spark.sql(f"""
/* create temporary table temp_t_mhbos_m_client_name_info 				Add 20240321 */
    DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_name_info
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_name_info (
      `client_no` STRING,
      `client_name` STRING,
      `client_name1` STRING,
      `client_name2` STRING,
      `client_name3` STRING,
      `customer_name_concatenate` STRING,
      `org_customer_name` STRING,
      `customer_name` STRING,
      `customer_name_flag` STRING,
      `org_primary_identification_type` STRING,
      `primary_identification_type` STRING,
      `org_primary_identification_no` STRING,
      `primary_identification_no` STRING,
      `org_secondary_identification_type` STRING,
      `secondary_identification_type` STRING,
      `org_secondary_identification_no` STRING,
      `secondary_identification_no` STRING,
      `org_principal_name` STRING,
      `principal_name` STRING,
      `org_beneficiary_name` STRING,
      `beneficiary_name` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_name_info /* trunate temporary table temp_t_mhbos_m_client_name_info */
""")
spark.sql(f"""
/* Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info		add 20240321 */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_name_info
    SELECT
      t2.client_no,
      t2.client_name,
      t2.client_name1,
      t2.client_name2,
      t2.client_name3,
      t2.customer_name_concatenate,
      t2.org_customer_name, /* 2025/09/08: Add customer_name_flag */
      CASE
        WHEN TRIM(COALESCE(t2.Customer_Name, '')) <> ''
        THEN TRIM(COALESCE(t2.Customer_Name, ''))
        ELSE '@[' || TRIM(COALESCE(t2.Customer_Name, '')) || ']'
      END AS customer_name, /* 2025/09/08: Add customer_name_flag */
      CASE WHEN TRIM(COALESCE(t2.Customer_Name, '')) <> '' THEN '0' ELSE '1' END AS customer_name_flag,
      t2.primary_identification_type AS org_primary_identification_type, /* added 20250313 */
      CASE
        WHEN NOT l1.primary_id_type IS NULL
        THEN l1.primary_id_type /* added 20250313 */
        ELSE t2.primary_identification_type
      END AS primary_identification_type,
      t2.org_primary_identification_no,
      CASE
        WHEN NOT l1.primary_id_no IS NULL
        THEN l1.primary_id_no
        ELSE t2.primary_identification_no
      END AS primary_identification_no,
      t2.secondary_identification_type AS org_secondary_identification_type, /* added 20250620 */
      CASE
        WHEN NOT l1.secondary_id_type IS NULL
        THEN l1.secondary_id_type /* added 20250620 */
        ELSE t2.secondary_identification_type
      END AS secondary_identification_type,
      t2.secondary_identification_no AS org_secondary_identification_no, /* added 20250620 */
      CASE
        WHEN NOT l1.secondary_id_no IS NULL
        THEN l1.secondary_id_no /* added 20250620 */
        ELSE t2.secondary_identification_no
      END AS secondary_identification_no,
      t2.org_principal_name,
      t2.principal_name,
      t2.org_beneficiary_name,
      t2.beneficiary_name
    FROM (
      SELECT
        t1.client_no,
        t1.client_name,
        t1.client_name1,
        t1.client_name2,
        t1.client_name3,
        t1.customer_name_concatenate,
        t1.customer_name AS org_customer_name,
        TRIM(
          CASE
            WHEN NOT l2.cust_name IS NULL
            THEN l2.cust_name /* add new mapping 20240828 */
            WHEN NOT l3.new_cust_name IS NULL
            THEN l3.new_cust_name /* add new mapping 20240828 */
            WHEN NOT l4.new_cust_name IS NULL
            THEN l4.new_cust_name /* add new mapping 20250313 */
            WHEN NOT l5.cust_name IS NULL
            THEN l5.cust_name /* add new mapping 20250620 */
            WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KIBB%'
            THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240815 */
            WHEN t1.customer_name_concatenate LIKE '%RSS/ SBL FOR KIBB%'
            THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240828 */
            WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%'
            THEN REGEXP_EXTRACT(t1.customer_name, '\\(([^)]+)\\)') /* added logic 20240828 */
            WHEN t1.customer_name RLIKE '.+\\(.*\\)$'
            THEN REGEXP_REPLACE(
              REGEXP_REPLACE(t1.customer_name, '\\s*\\([^)]*\\)$', ''),
              '^(SMT\\s+|INTRADAY A/C\\s+)',
              ''
            ) /* add 20240518 --20241118 changes */
            WHEN t1.customer_name_concatenate LIKE '%SHARE BUY%'
            THEN REGEXP_REPLACE(t1.customer_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') /* added logic 20240828 */
            ELSE REGEXP_REPLACE(t1.customer_name, '^(SMT\\s+|INTRADAY A/C\\s+)', '')
          END
        ) AS customer_name,
        t1.primary_identification_type, /* added 20250313 */
        t1.primary_identification_no AS org_primary_identification_no,
        t1.primary_identification_no,
        t1.secondary_identification_type, /* added 20250620 */
        t1.secondary_identification_no, /* added 20250620 */
        t1.principal_name AS org_principal_name,
        TRIM(
          CASE
            WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KIBB%'
            THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240815 */
            WHEN t1.customer_name_concatenate LIKE '%RSS/ SBL FOR KIBB%'
            THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240828 */
            WHEN t1.customer_name_concatenate LIKE '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%'
            THEN REGEXP_EXTRACT(t1.principal_name, '\\(([^)]+)\\)') /* added logic 20240828 */
            WHEN t1.principal_name RLIKE '.+\\(.*\\)$'
            THEN REGEXP_REPLACE(t1.principal_name, '\\s*\\([^)]*\\)$', '') /* add 20240518 --20241118 changes */
            WHEN t1.customer_name_concatenate LIKE '%SHARE BUY%'
            THEN REGEXP_REPLACE(t1.principal_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') /* added logic 20240828 */
            ELSE t1.principal_name
          END
        ) AS principal_name,
        t1.beneficiary_name AS org_beneficiary_name,
        CASE
          WHEN t1.beneficiary_name RLIKE '.+\\(.*\\)$'
          THEN REGEXP_REPLACE(t1.beneficiary_name, '\\s*\\([^)]*\\)$', '')
          ELSE t1.beneficiary_name
        END AS beneficiary_name /* add 20240518 --20241118 changes */
      FROM (
        SELECT
          mmca.client_no,
          cn.client_name,
          cn.client_name1,
          cn.client_name2,
          cn.client_name3,
          cn.customer_name_concatenate,
          cn.customer_name AS org_customer_name, /*             ,case when nvl(nom.noms_ind, 'Y') = 'Y' and nvl(trim(nom.principal_name), '') <> '' then nom.principal_name */
          CASE
            WHEN COALESCE(TRIM(nom.principal_name), '') <> ''
            THEN nom.principal_name
            WHEN COALESCE(TRIM(cn.customer_name), '') = ''
            AND COALESCE(TRIM(cn.client_name), '') <> ''
            THEN cn.client_name
            WHEN COALESCE(TRIM(cn.customer_name), '') = ''
            AND COALESCE(TRIM(cn.client_name), '') = ''
            THEN '@[]'
            ELSE cn.customer_name
          END AS customer_name,
          id.primary_identification_type, /* added 20250313 */
          id.primary_identification_no,
          id.secondary_identification_type, /* added 20250620 */
          id.secondary_identification_no, /* added 20250620 */
          nom.principal_name,
          nom.beneficiary_name
        FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
        LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 AS cn
          ON mmca.client_no = cn.client_no
        LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info AS nom
          ON mmca.client_no = nom.client_no
        LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_identification_info AS id
          ON mmca.client_no = id.client_no
      ) AS t1
      LEFT JOIN {params["raw_schema"]}.lookup_clientno_custname AS l2
        ON t1.client_no = l2.client_no AND l2.source_system = 'MHBOS'
      LEFT JOIN {params["raw_schema"]}.lookup_custname_custname AS l3
        ON t1.customer_name = l3.cust_name AND l3.source_system = 'MHBOS'
      LEFT JOIN {params["raw_schema"]}.lookup_primaryidno_newcustname AS l4 /* added 20250313 */
        ON t1.primary_identification_no = l4.primary_id_no AND l4.source_system = 'MHBOS'
      LEFT JOIN {params["raw_schema"]}.lookup_custname_primaryidno AS l5 /* added 20250620 */
        ON t1.primary_identification_no = l5.primary_id_no AND l5.source_system = 'MHBOS'
    ) AS t2
    LEFT JOIN {params["raw_schema"]}.lookup_custname_primaryidno AS l1
      ON t2.customer_name = l1.cust_name AND l1.source_system = 'MHBOS'
""")
spark.sql(f"""
DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client
""")
spark.sql(f"""
/* 3.0.1 create temp_t_mhbos_m_client table */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client (
      client_no VARCHAR(9) COMMENT '',
      clean_rule_flag VARCHAR(60) COMMENT '',
      primary_identification_type VARCHAR(10) COMMENT '',
      primary_identification_no VARCHAR(60) COMMENT '',
      secondary_identification_type VARCHAR(10) COMMENT '',
      secondary_identification_no VARCHAR(60) COMMENT '',
      customer_name VARCHAR(250) COMMENT '',
      customer_name_concatenate VARCHAR(250) COMMENT '',
      client_name VARCHAR(60) COMMENT '',
      client_name1 VARCHAR(60) COMMENT '',
      client_name2 VARCHAR(60) COMMENT '',
      client_name3 VARCHAR(60) COMMENT '',
      mobile_no VARCHAR(20) COMMENT '',
      fax_no VARCHAR(20) COMMENT '',
      tel_no_home VARCHAR(20) COMMENT '',
      tel_no_office VARCHAR(20) COMMENT '',
      date_of_birth TIMESTAMP COMMENT '',
      race VARCHAR(30) COMMENT '',
      email_1 VARCHAR(300) COMMENT '',
      email_2 VARCHAR(300) COMMENT '',
      email_3 VARCHAR(300) COMMENT '',
      email_4 VARCHAR(300) COMMENT '',
      email_5 VARCHAR(300) COMMENT '',
      email_6 VARCHAR(300) COMMENT '',
      email_7 VARCHAR(300) COMMENT '',
      email_8 VARCHAR(300) COMMENT '',
      email_9 VARCHAR(300) COMMENT '',
      email_10 VARCHAR(300) COMMENT '',
      sex VARCHAR(10) COMMENT '',
      addr1 VARCHAR(45) COMMENT '',
      addr2 VARCHAR(45) COMMENT '',
      addr3 VARCHAR(45) COMMENT '',
      addr4 VARCHAR(45) COMMENT '',
      postcode VARCHAR(6) COMMENT '',
      city VARCHAR(255) COMMENT '',
      state VARCHAR(50) COMMENT '',
      perm_addr1 VARCHAR(45) COMMENT '',
      perm_addr2 VARCHAR(45) COMMENT '',
      perm_addr3 VARCHAR(45) COMMENT '',
      perm_addr4 VARCHAR(45) COMMENT '',
      perm_postcode VARCHAR(6) COMMENT '',
      perm_city VARCHAR(255) COMMENT '',
      perm_state VARCHAR(50) COMMENT '',
      noms_ind VARCHAR(1) COMMENT '',
      cleaned_nominees_name VARCHAR(300) COMMENT '',
      principal_name VARCHAR(300) COMMENT '',
      intermediary_name VARCHAR(300) COMMENT '',
      beneficiary_name VARCHAR(300) COMMENT '',
      nominees_type VARCHAR(10) COMMENT '',
      pledged_securities_flag VARCHAR(1) COMMENT '',
      id_type VARCHAR(10) COMMENT '',
      ic_no_new VARCHAR(15) COMMENT '',
      ic_no_old VARCHAR(14) COMMENT '',
      secondary_id_type VARCHAR(2) COMMENT '',
      secondary_id_no VARCHAR(15) COMMENT '',
      source_client_name VARCHAR(50) COMMENT '',
      source_client_name1 VARCHAR(50) COMMENT '',
      source_client_name2 VARCHAR(50) COMMENT '',
      source_client_name3 VARCHAR(50) COMMENT '',
      source_mobile_no VARCHAR(15) COMMENT '',
      source_fax_no VARCHAR(15) COMMENT '',
      source_tel_no_home VARCHAR(15) COMMENT '',
      source_tel_no_office VARCHAR(15) COMMENT '',
      source_date_of_birth TIMESTAMP COMMENT '',
      source_race VARCHAR(20) COMMENT '',
      source_email VARCHAR(300) COMMENT '',
      source_sex VARCHAR(10) COMMENT '',
      client_group VARCHAR(14) COMMENT '',
      cds_acc_no VARCHAR(9) COMMENT '',
      tdr_code VARCHAR(5) COMMENT '',
      client_type VARCHAR(3) COMMENT '',
      margin VARCHAR(1) COMMENT '',
      last_margin_date TIMESTAMP COMMENT '',
      int_rate DECIMAL(5, 2) COMMENT '',
      auto_ded VARCHAR(1) COMMENT '',
      despatch_mode VARCHAR(2) COMMENT '',
      copies DECIMAL(2, 0) COMMENT '',
      prohibit_trade VARCHAR(1) COMMENT '',
      custody_status VARCHAR(1) COMMENT '',
      country VARCHAR(3) COMMENT '',
      margin_limit DECIMAL(9, 0) COMMENT '',
      margin_pct DECIMAL(5, 2) COMMENT '',
      rollover_rate DECIMAL(6, 3) COMMENT '',
      form_completed VARCHAR(1) COMMENT '',
      last_tran_date TIMESTAMP COMMENT '',
      ytd_bvalue DECIMAL(12, 2) COMMENT '',
      ytd_svalue DECIMAL(12, 2) COMMENT '',
      ytd_brokerage DECIMAL(11, 2) COMMENT '',
      os_led_bal DECIMAL(12, 2) COMMENT '',
      title VARCHAR(20) COMMENT '',
      date_created TIMESTAMP COMMENT '',
      stop_payt VARCHAR(1) COMMENT '',
      acc_payee VARCHAR(100) COMMENT '',
      category VARCHAR(1) COMMENT '',
      auto_contra VARCHAR(1) COMMENT '',
      pnl_acc_no VARCHAR(18) COMMENT '',
      cr_limit DECIMAL(9, 0) COMMENT '',
      trust_bal DECIMAL(12, 2) COMMENT '',
      avg_ind VARCHAR(8) COMMENT '',
      remarks VARCHAR(60) COMMENT '',
      contact_person VARCHAR(40) COMMENT '',
      date_closed TIMESTAMP COMMENT '',
      grace_period DECIMAL(3, 0) COMMENT '',
      acct_type DECIMAL(2, 0) COMMENT '',
      assoc_ind VARCHAR(1) COMMENT '',
      short_sell_ind VARCHAR(1) COMMENT '',
      short_name VARCHAR(10) COMMENT '',
      mesdaq_pctlmt DECIMAL(5, 2) COMMENT '',
      date_change TIMESTAMP COMMENT '',
      resi_code VARCHAR(5) COMMENT '',
      charge_int VARCHAR(1) COMMENT '',
      bdebt VARCHAR(1) COMMENT '',
      assets DECIMAL(12, 2) COMMENT '',
      liabilities DECIMAL(12, 2) COMMENT '',
      income DECIMAL(12, 2) COMMENT '',
      expenses DECIMAL(12, 2) COMMENT '',
      bdebt_his_ind VARCHAR(3) COMMENT '',
      rel_ac1 VARCHAR(9) COMMENT '',
      rel_ac2 VARCHAR(9) COMMENT '',
      rel_ac3 VARCHAR(9) COMMENT '',
      rel_ac4 VARCHAR(9) COMMENT '',
      occupation VARCHAR(60) COMMENT '',
      margin_int DECIMAL(12, 2) COMMENT '',
      lst_led_no DECIMAL(9, 0) COMMENT '',
      cur_led_no DECIMAL(9, 0) COMMENT '',
      remarks2 VARCHAR(60) COMMENT '',
      acc_type VARCHAR(1) COMMENT '',
      mas_accno VARCHAR(9) COMMENT '',
      legal VARCHAR(1) COMMENT '',
      sell_limit DECIMAL(9, 0) COMMENT '',
      brk_rate DECIMAL(9, 4) COMMENT '',
      brokerage_type VARCHAR(3) COMMENT '',
      cds_acc_no1 VARCHAR(9) COMMENT '',
      remarks1 VARCHAR(60) COMMENT '',
      payment_bank_code VARCHAR(5) COMMENT '',
      noms VARCHAR(1) COMMENT '',
      dms_date TIMESTAMP COMMENT '',
      violation_date TIMESTAMP COMMENT '',
      mcd_branch VARCHAR(3) COMMENT '',
      home_branch VARCHAR(3) COMMENT '',
      eaf_code VARCHAR(1) COMMENT '',
      call_warrant VARCHAR(1) COMMENT '',
      user_id VARCHAR(20) COMMENT '',
      credit_int_rate DECIMAL(5, 2) COMMENT '',
      min_eligible_amt DECIMAL(18, 4) COMMENT '',
      intraday_flag VARCHAR(1) COMMENT '',
      intraday_rate DECIMAL(5, 4) COMMENT '',
      cta_weight DECIMAL(3, 0) COMMENT '',
      sta_weight DECIMAL(3, 0) COMMENT '',
      bo_cds_acc_no VARCHAR(20) COMMENT '',
      ecos_form VARCHAR(1) COMMENT '',
      custodian_no VARCHAR(7) COMMENT '',
      prin_acc VARCHAR(2) COMMENT '',
      armada_type VARCHAR(8) COMMENT '',
      old_authorisee VARCHAR(5) COMMENT '',
      etrade_rate DECIMAL(9, 4) COMMENT '',
      etf VARCHAR(1) COMMENT '',
      cstamp_client_exempt VARCHAR(1) COMMENT '',
      main_branch VARCHAR(3) COMMENT '',
      prev_client_no VARCHAR(9) COMMENT '',
      web_eds VARCHAR(1) COMMENT '',
      place VARCHAR(5) COMMENT '',
      excl_tdr_deduct VARCHAR(1) COMMENT '',
      excl_auto_susp VARCHAR(1) COMMENT '',
      trust_flag VARCHAR(1) COMMENT '',
      mgn_new_int_rate DECIMAL(5, 2) COMMENT '',
      counter_concentration DECIMAL(5, 2) COMMENT '',
      auto_trust VARCHAR(1) COMMENT '',
      margin_pct2 DECIMAL(5, 2) COMMENT '',
      df_flag VARCHAR(1) COMMENT '',
      mgn_curr_int_rate DECIMAL(5, 2) COMMENT '',
      product_type VARCHAR(1) COMMENT '',
      web_ecos VARCHAR(1) COMMENT '',
      xeye_clt_grp VARCHAR(14) COMMENT '',
      bursa_violation_date TIMESTAMP COMMENT '',
      brokerage_type_etrade VARCHAR(3) COMMENT '',
      brokerage_type_odd_lot VARCHAR(3) COMMENT '',
      omnibus VARCHAR(1) COMMENT '',
      cg_tdr_code VARCHAR(7) COMMENT '',
      limit_foreign DECIMAL(9, 4) COMMENT '',
      limit_bursa DECIMAL(9, 4) COMMENT '',
      brokerage_type_intraday VARCHAR(3) COMMENT '',
      brokerage_type_intraday_etrade VARCHAR(3) COMMENT '',
      bursa_violation_date1 TIMESTAMP COMMENT '',
      cif_no VARCHAR(20) COMMENT '',
      brokerage_type_foreign VARCHAR(3) COMMENT '',
      soft_copy VARCHAR(1) COMMENT '',
      exclude_rollover VARCHAR(1) COMMENT '',
      account_status VARCHAR(1) COMMENT '',
      w8ben VARCHAR(1) COMMENT '',
      ic_no_rel1 VARCHAR(15) COMMENT '',
      ic_no_rel2 VARCHAR(15) COMMENT '',
      ic_no_rel3 VARCHAR(15) COMMENT '',
      ic_no_rel4 VARCHAR(15) COMMENT '',
      ic_no_rel5 VARCHAR(15) COMMENT '',
      rel1 VARCHAR(15) COMMENT '',
      rel2 VARCHAR(15) COMMENT '',
      rel3 VARCHAR(15) COMMENT '',
      rel4 VARCHAR(15) COMMENT '',
      rel5 VARCHAR(15) COMMENT '',
      brokerage_type_etrade_b VARCHAR(3) COMMENT '',
      brokerage_type_odd_lot_b VARCHAR(3) COMMENT '',
      brokerage_type_b VARCHAR(3) COMMENT '',
      brokerage_type_foreign_b VARCHAR(3) COMMENT '',
      brokerage_type_intraday_b VARCHAR(3) COMMENT '',
      brokerage_type_intraday_etrade_b VARCHAR(3) COMMENT '',
      brokerage_type_etrade_s VARCHAR(3) COMMENT '',
      brokerage_type_odd_lot_s VARCHAR(3) COMMENT '',
      brokerage_type_s VARCHAR(3) COMMENT '',
      brokerage_type_foreign_s VARCHAR(3) COMMENT '',
      brokerage_type_intraday_s VARCHAR(3) COMMENT '',
      brokerage_type_intraday_etrade_s VARCHAR(3) COMMENT '',
      no_free_trade DECIMAL(2, 0) COMMENT '',
      sms VARCHAR(1) COMMENT '',
      mobile_prefix VARCHAR(5) COMMENT '',
      foreign_curr_set VARCHAR(1) COMMENT '',
      num_free_trade DECIMAL(2, 0) COMMENT '',
      etrader_type VARCHAR(2) COMMENT '',
      check_limit VARCHAR(1) COMMENT '',
      auto_margin VARCHAR(1) COMMENT '',
      margin_client_no VARCHAR(9) COMMENT '',
      dup_despatch_mode VARCHAR(1) COMMENT '',
      risk VARCHAR(1) COMMENT '',
      exclude_trader_limit VARCHAR(1) COMMENT '',
      sett_mode_date_change TIMESTAMP COMMENT '',
      pick_up_fee_pct DECIMAL(5, 2) COMMENT '',
      e_payment VARCHAR(1) COMMENT '',
      mgn_new_int_rate2 DECIMAL(5, 2) COMMENT '',
      fund_cost_type VARCHAR(1) COMMENT '',
      check_share VARCHAR(1) COMMENT '',
      citibank_changes VARCHAR(1) COMMENT '',
      citibank_charges VARCHAR(1) COMMENT '',
      cq_market VARCHAR(1) COMMENT '',
      exclude_margin_pro_rate VARCHAR(1) COMMENT '',
      brokerage_type_cash_b VARCHAR(3) COMMENT '',
      brokerage_type_etrade_cash_b VARCHAR(3) COMMENT '',
      clt_consent VARCHAR(1) COMMENT '',
      consent_start_date TIMESTAMP COMMENT '',
      portfolio VARCHAR(1) COMMENT '',
      expiry_date TIMESTAMP COMMENT '',
      intraday_auto_contra_option VARCHAR(1) COMMENT '',
      dcf_limit DECIMAL(9, 0) COMMENT '',
      brokerage_type_etb VARCHAR(3) COMMENT '',
      mgn_force_sell_pct DECIMAL(5, 2) COMMENT '',
      mgn_tenure DECIMAL(3, 0) COMMENT '',
      mgn_expiry_date TIMESTAMP COMMENT '',
      loss_gl_acc_no VARCHAR(18) COMMENT '',
      portfolio_date TIMESTAMP COMMENT '',
      day_prior_temp_susp DECIMAL(4, 0) COMMENT '',
      day_prior_perm_susp DECIMAL(4, 0) COMMENT '',
      gst_code VARCHAR(3) COMMENT '',
      match_price_decimal_local DECIMAL(1, 0) COMMENT '',
      match_price_decimal_foreign DECIMAL(1, 0) COMMENT '',
      primary_id_expiry_date TIMESTAMP COMMENT '',
      secondary_id_expiry_date TIMESTAMP COMMENT '',
      mgn_int_tdr_spread_pct DECIMAL(5, 2) COMMENT '',
      mgn_base_int_rate DECIMAL(5, 2) COMMENT '',
      mgn_int_tdr_share DECIMAL(5, 2) COMMENT '',
      islamic_flag VARCHAR(1) COMMENT '',
      mcd_resident_flag VARCHAR(1) COMMENT '',
      chq_charges_flag VARCHAR(1) COMMENT '',
      chq_charges_tdr_pct DECIMAL(9, 2) COMMENT '',
      brokerage_type_foreign_etrade VARCHAR(3) COMMENT '',
      brokerage_type_foreign_etrade_b VARCHAR(3) COMMENT '',
      brokerage_type_foreign_etrade_s VARCHAR(3) COMMENT '',
      grp_exch_code VARCHAR(5) COMMENT '',
      bdebt_ras VARCHAR(1) COMMENT '',
      twse_declaration VARCHAR(1) COMMENT '',
      joint_acc_amt DECIMAL(12, 2) COMMENT '',
      high_risk_market VARCHAR(2) COMMENT '',
      brokerage_type_leap_normal VARCHAR(3) COMMENT '',
      brokerage_type_leap_etrade VARCHAR(3) COMMENT '',
      etl_timestamp STRING COMMENT 'ETL_processing_time',
      perm_country VARCHAR(3) COMMENT '',
      type_of_account STRING COMMENT '',
      einvoice_email STRING COMMENT ''
    )
""")
spark.sql(f"""
/* 3.0.2 insert into temp_t_mhbos_m_client */
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client
    SELECT
      step2.client_no,
      step2.clean_rule_flag,
      step2.primary_identification_type,
      step2.primary_identification_no,
      step2.secondary_identification_type,
      step2.secondary_identification_no,
      step2.customer_name,
      step2.customer_name_concatenate,
      step2.client_name,
      step2.client_name1,
      step2.client_name2,
      step2.client_name3,
      step2.mobile_no,
      step2.fax_no,
      step2.tel_no_home,
      step2.tel_no_office,
      step2.date_of_birth,
      step2.race,
      step2.email_1,
      step2.email_2,
      step2.email_3,
      step2.email_4,
      step2.email_5,
      step2.email_6,
      step2.email_7,
      step2.email_8,
      step2.email_9,
      step2.email_10,
      step2.sex,
      step2.addr1,
      step2.addr2,
      step2.addr3,
      step2.addr4,
      step2.postcode,
      step2.city,
      step2.state,
      step2.perm_addr1,
      step2.perm_addr2,
      step2.perm_addr3,
      step2.perm_addr4,
      step2.perm_postcode,
      step2.perm_city,
      step2.perm_state,
      step2.noms_ind,
      step2.cleaned_nominees_name,
      step2.principal_name,
      step2.intermediary_name,
      step2.beneficiary_name,
      step2.nominees_type,
      step2.pledged_securities_flag,
      mmca.id_type,
      mmca.ic_no_new,
      mmca.ic_no_old,
      mmca.secondary_id_type,
      mmca.secondary_id_no,
      mmca.client_name AS source_client_name,
      mmca.client_name1 AS source_client_name1,
      mmca.client_name2 AS source_client_name2,
      mmca.client_name3 AS source_client_name3,
      mmca.mobile_no AS source_mobile_no,
      mmca.fax_no AS source_fax_no,
      mmca.tel_no_home AS source_tel_no_home,
      mmca.tel_no_office AS source_tel_no_office,
      mmca.date_of_birth AS source_date_of_birth,
      mmca.race AS source_race,
      mmca.email AS source_email,
      mmca.sex AS source_sex,
      mmca.client_group,
      mmca.cds_acc_no,
      mmca.tdr_code,
      mmca.client_type,
      mmca.margin,
      mmca.last_margin_date,
      mmca.int_rate,
      mmca.auto_ded,
      mmca.despatch_mode,
      mmca.copies,
      mmca.prohibit_trade,
      mmca.custody_status,
      mmca.country,
      mmca.margin_limit,
      mmca.margin_pct,
      mmca.rollover_rate,
      mmca.form_completed,
      mmca.last_tran_date,
      mmca.ytd_bvalue,
      mmca.ytd_svalue,
      mmca.ytd_brokerage,
      mmca.os_led_bal,
      mmca.title,
      mmca.date_created,
      mmca.stop_payt,
      mmca.acc_payee,
      mmca.category,
      mmca.auto_contra,
      mmca.pnl_acc_no,
      mmca.cr_limit,
      mmca.trust_bal,
      mmca.avg_ind,
      mmca.remarks,
      mmca.contact_person,
      mmca.date_closed,
      mmca.grace_period,
      mmca.acct_type,
      mmca.assoc_ind,
      mmca.short_sell_ind,
      mmca.short_name,
      mmca.mesdaq_pctlmt,
      mmca.date_change,
      mmca.resi_code,
      mmca.charge_int,
      mmca.bdebt,
      mmca.assets,
      mmca.liabilities,
      mmca.income,
      mmca.expenses,
      mmca.bdebt_his_ind,
      mmca.rel_ac1,
      mmca.rel_ac2,
      mmca.rel_ac3,
      mmca.rel_ac4,
      mmca.occupation,
      mmca.margin_int,
      mmca.lst_led_no,
      mmca.cur_led_no,
      mmca.remarks2,
      mmca.acc_type,
      mmca.mas_accno,
      mmca.legal,
      mmca.sell_limit,
      mmca.brk_rate,
      mmca.brokerage_type,
      mmca.cds_acc_no1,
      mmca.remarks1,
      mmca.payment_bank_code,
      mmca.noms,
      mmca.dms_date,
      mmca.violation_date,
      mmca.mcd_branch,
      mmca.home_branch,
      mmca.eaf_code,
      mmca.call_warrant,
      mmca.user_id,
      mmca.credit_int_rate,
      mmca.min_eligible_amt,
      mmca.intraday_flag,
      mmca.intraday_rate,
      mmca.cta_weight,
      mmca.sta_weight,
      mmca.bo_cds_acc_no,
      mmca.ecos_form,
      mmca.custodian_no,
      mmca.prin_acc,
      mmca.armada_type,
      mmca.old_authorisee,
      mmca.etrade_rate,
      mmca.etf,
      mmca.cstamp_client_exempt,
      mmca.main_branch,
      mmca.prev_client_no,
      mmca.web_eds,
      mmca.place,
      mmca.excl_tdr_deduct,
      mmca.excl_auto_susp,
      mmca.trust_flag,
      mmca.mgn_new_int_rate,
      mmca.counter_concentration,
      mmca.auto_trust,
      mmca.margin_pct2,
      mmca.df_flag,
      mmca.mgn_curr_int_rate,
      mmca.product_type,
      mmca.web_ecos,
      mmca.xeye_clt_grp,
      mmca.bursa_violation_date,
      mmca.brokerage_type_etrade,
      mmca.brokerage_type_odd_lot,
      mmca.omnibus,
      mmca.cg_tdr_code,
      mmca.limit_foreign,
      mmca.limit_bursa,
      mmca.brokerage_type_intraday,
      mmca.brokerage_type_intraday_etrade,
      mmca.bursa_violation_date1,
      mmca.cif_no,
      mmca.brokerage_type_foreign,
      mmca.soft_copy,
      mmca.exclude_rollover,
      mmca.account_status,
      mmca.w8ben,
      mmca.ic_no_rel1,
      mmca.ic_no_rel2,
      mmca.ic_no_rel3,
      mmca.ic_no_rel4,
      mmca.ic_no_rel5,
      mmca.rel1,
      mmca.rel2,
      mmca.rel3,
      mmca.rel4,
      mmca.rel5,
      mmca.brokerage_type_etrade_b,
      mmca.brokerage_type_odd_lot_b,
      mmca.brokerage_type_b,
      mmca.brokerage_type_foreign_b,
      mmca.brokerage_type_intraday_b,
      mmca.brokerage_type_intraday_etrade_b,
      mmca.brokerage_type_etrade_s,
      mmca.brokerage_type_odd_lot_s,
      mmca.brokerage_type_s,
      mmca.brokerage_type_foreign_s,
      mmca.brokerage_type_intraday_s,
      mmca.brokerage_type_intraday_etrade_s,
      mmca.no_free_trade,
      mmca.sms,
      mmca.mobile_prefix,
      mmca.foreign_curr_set,
      mmca.num_free_trade,
      mmca.etrader_type,
      mmca.check_limit,
      mmca.auto_margin,
      mmca.margin_client_no,
      mmca.dup_despatch_mode,
      mmca.risk,
      mmca.exclude_trader_limit,
      mmca.sett_mode_date_change,
      mmca.pick_up_fee_pct,
      mmca.e_payment,
      mmca.mgn_new_int_rate2,
      mmca.fund_cost_type,
      mmca.check_share,
      mmca.citibank_changes,
      mmca.citibank_charges,
      mmca.cq_market,
      mmca.exclude_margin_pro_rate,
      mmca.brokerage_type_cash_b,
      mmca.brokerage_type_etrade_cash_b,
      mmca.clt_consent,
      mmca.consent_start_date,
      mmca.portfolio,
      mmca.expiry_date,
      mmca.intraday_auto_contra_option,
      mmca.dcf_limit,
      mmca.brokerage_type_etb,
      mmca.mgn_force_sell_pct,
      mmca.mgn_tenure,
      mmca.mgn_expiry_date,
      mmca.loss_gl_acc_no,
      mmca.portfolio_date,
      mmca.day_prior_temp_susp,
      mmca.day_prior_perm_susp,
      mmca.gst_code,
      mmca.match_price_decimal_local,
      mmca.match_price_decimal_foreign,
      mmca.primary_id_expiry_date,
      mmca.secondary_id_expiry_date,
      mmca.mgn_int_tdr_spread_pct,
      mmca.mgn_base_int_rate,
      mmca.mgn_int_tdr_share,
      mmca.islamic_flag,
      mmca.mcd_resident_flag,
      mmca.chq_charges_flag,
      mmca.chq_charges_tdr_pct,
      mmca.brokerage_type_foreign_etrade,
      mmca.brokerage_type_foreign_etrade_b,
      mmca.brokerage_type_foreign_etrade_s,
      mmca.grp_exch_code,
      mmca.bdebt_ras,
      mmca.twse_declaration,
      mmca.joint_acc_amt,
      mmca.high_risk_market,
      mmca.brokerage_type_leap_normal,
      mmca.brokerage_type_leap_etrade,
      CURRENT_TIMESTAMP() AS etl_timestamp,
      step2.perm_country, /* added new field 20250715 */
      mmca.type_of_account, /* added new field 20250806 */
      step2.einvoice_email /* 20250903 einvoice_email */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step2 AS step2
      ON mmca.client_no = step2.client_no
""")
spark.sql(f"""
/* 2.12 Create a temporary table temp_t_mhbos_m_client_telephone_clean to store the cleaned telephone number information. */
    CREATE TABLE IF NOT EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_telephone_clean (
      `client_no` STRING,
      `source_mobile_no` STRING,
      `source_fax_no` STRING,
      `source_tel_no_home` STRING,
      `source_tel_no_office` STRING,
      `mobile_no` STRING,
      `fax_no` STRING,
      `tel_no_home` STRING,
      `tel_no_office` STRING
    )
""")
spark.sql(f"""
TRUNCATE TABLE   {params["com_schema"]}.temp_t_mhbos_m_client_telephone_clean /* 2.1.1 ddl-insert-sundexin */
""")
spark.sql(f"""
INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_telephone_clean /* modify 20250502 */
    SELECT
      t.client_no,
      t.source_mobile_no,
      t.source_fax_no,
      t.source_tel_no_home,
      t.source_tel_no_office,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(t.mobile_no, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
                '[^0-9/()+]',
                ''
              ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
              '\\((?![0]\\))',
              ''
            ) /* Remove '(' only if it's not part of "(0)" pattern */,
            '(?<!\\(0)\\)',
            ''
          ) /* Remove ')' only if it's not part of "(0)" pattern */,
          '/(?!([0-9]|\\(0\\)))',
          ''
        ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
        '(?<=.)(\\+)',
        ''
      ) AS mobile_no, /* Keep '+' sign if it's the first character, remove otherwise */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(t.fax_no, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
                '[^0-9/()+]',
                ''
              ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
              '\\((?![0]\\))',
              ''
            ) /* Remove '(' only if it's not part of "(0)" pattern */,
            '(?<!\\(0)\\)',
            ''
          ) /* Remove ')' only if it's not part of "(0)" pattern */,
          '/(?!([0-9]|\\(0\\)))',
          ''
        ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
        '(?<=.)(\\+)',
        ''
      ) AS fax_no, /* Keep '+' sign if it's the first character, remove otherwise */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(t.tel_no_home, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
                '[^0-9/()+]',
                ''
              ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
              '\\((?![0]\\))',
              ''
            ) /* Remove '(' only if it's not part of "(0)" pattern */,
            '(?<!\\(0)\\)',
            ''
          ) /* Remove ')' only if it's not part of "(0)" pattern */,
          '/(?!([0-9]|\\(0\\)))',
          ''
        ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
        '(?<=.)(\\+)',
        ''
      ) AS tel_no_home, /* Keep '+' sign if it's the first character, remove otherwise */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(t.tel_no_office, 'EXT', '/') /* Replace EXT to / */, '[*Xx]', '/') /* Replace *, x, X to / */,
                '[^0-9/()+]',
                ''
              ) /* Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later) */,
              '\\((?![0]\\))',
              ''
            ) /* Remove '(' only if it's not part of "(0)" pattern */,
            '(?<!\\(0)\\)',
            ''
          ) /* Remove ')' only if it's not part of "(0)" pattern */,
          '/(?!([0-9]|\\(0\\)))',
          ''
        ) /* Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0) */,
        '(?<=.)(\\+)',
        ''
      ) AS tel_no_office /* Keep '+' sign if it's the first character, remove otherwise */
    FROM (
      SELECT
        mmca.client_no,
        (
          mmca.mobile_prefix || mmca.mobile_no
        ) AS source_mobile_no,
        mmca.fax_no AS source_fax_no,
        mmca.tel_no_home AS source_tel_no_home,
        mmca.tel_no_office AS source_tel_no_office,
        (
          mmca.mobile_prefix || mmca.mobile_no
        ) AS mobile_no,
        mmca.fax_no AS fax_no,
        mmca.tel_no_home AS tel_no_home,
        mmca.tel_no_office AS tel_no_office
      FROM {params["com_schema"]}.temp_t_mhbos_m_client_all AS mmca
    ) AS t
    WHERE
      1 = 1
""")
spark.sql(f"""
/* 3.1 Clear the day's data 
     alter table {params["com_schema"]}.t_mhbos_m_client drop if exists partition ( etl_dt = '{batch_date}' ); 
     3.2 Inserts the cleaned data into the specified partition of the target table 
     insert into table {params["com_schema"]}.t_mhbos_m_client partition ( etl_dt = '{batch_date}' ) 
     select mmca.client_no 
            ,(id.primary_identification_type_flag || id.primary_identification_no_flag || 
               id.secondary_identification_type_flag ||id.secondary_identification_no_flag || 
               cn.client_name_flag || cn.client_name1_flag || cn.client_name2_flag || cn.client_name3_flag || 
               tel.mobile_no_flag || tel.fax_no_flag || tel.tel_no_home_flag || tel.tel_no_office_flag || 
               dob.date_of_birth_flag || tmmcr.race_flag || tmmcei.email_flag || tmmcgi.sex_flag) as clean_rule_flag 
            ,id.primary_identification_type 
            ,mn.primary_identification_no 
            ,replace(id.secondary_identification_type, '@[]', '') as secondary_identification_type 
            ,replace(id.secondary_identification_no, '@[]', '') as secondary_identification_no 
     	   ,mn.customer_name 
            ,cn.customer_name_concatenate 
            ,cn.client_name 
            ,cn.client_name1 
            ,cn.client_name2 
            ,cn.client_name3 
            ,tel.mobile_no 
            ,tel.fax_no 
            ,tel.tel_no_home 
            ,tel.tel_no_office 
            ,dob.date_of_birth 
            ,tmmcr.race 
            ,tmmcei.email_1 
     	   ,tmmcei.email_2 
     	   ,tmmcei.email_3 
     	   ,tmmcei.email_4 
     	   ,tmmcei.email_5 
     	   ,tmmcei.email_6 
     	   ,tmmcei.email_7 
     	   ,tmmcei.email_8 
     	   ,tmmcei.email_9 
     	   ,tmmcei.email_10 
            ,tmmcgi.sex 
            ,ad.addr1 
            ,ad.addr2 
            ,ad.addr3 
            ,ad.addr4 
            ,ad.postcode 
            ,ad.city 
            ,ad.state 
            ,ad.perm_addr1 
            ,ad.perm_addr2 
            ,ad.perm_addr3 
            ,ad.perm_addr4 
            ,ad.perm_postcode 
            ,ad.perm_city 
            ,ad.perm_state 
     	   ,nvl(nom.noms_ind, 'N') as noms_ind 
     	   ,nom.cleaned_nominees_name 
     	   ,nom.principal_name 
     	   ,nom.intermediary_name 
     	   ,nom.beneficiary_name 
     	   ,nominees_type 
     	   ,pledged_securities_flag 
            ,mmca.id_type 
            ,mmca.ic_no_new 
            ,mmca.ic_no_old 
            ,mmca.secondary_id_type 
            ,mmca.secondary_id_no 
            ,mmca.client_name as source_client_name 
            ,mmca.client_name1 as source_client_name1 
            ,mmca.client_name2 as source_client_name2 
            ,mmca.client_name3 as source_client_name3 
            ,mmca.mobile_no as source_mobile_no 
            ,mmca.fax_no as source_fax_no 
            ,mmca.tel_no_home as source_tel_no_home 
            ,mmca.tel_no_office as source_tel_no_office 
            ,mmca.date_of_birth as source_date_of_birth 
            ,mmca.race as source_race 
            ,mmca.email as source_email 
            ,mmca.sex as source_sex 
            ,mmca.client_group 
            ,mmca.cds_acc_no 
            ,mmca.tdr_code 
            ,mmca.client_type 
            ,mmca.margin 
            ,mmca.last_margin_date 
            ,mmca.int_rate 
            ,mmca.auto_ded 
            ,mmca.despatch_mode 
            ,mmca.copies 
            ,mmca.prohibit_trade 
            ,mmca.custody_status 
            ,mmca.country 
            ,mmca.margin_limit 
            ,mmca.margin_pct 
            ,mmca.rollover_rate 
            ,mmca.form_completed 
            ,mmca.last_tran_date 
            ,mmca.ytd_bvalue 
            ,mmca.ytd_svalue 
            ,mmca.ytd_brokerage 
            ,mmca.os_led_bal 
            ,mmca.title 
            ,mmca.date_created 
            ,mmca.stop_payt 
            ,mmca.acc_payee 
            ,mmca.category 
            ,mmca.auto_contra 
            ,mmca.pnl_acc_no 
            ,mmca.cr_limit 
            ,mmca.trust_bal 
            ,mmca.avg_ind 
            ,mmca.remarks 
            ,mmca.contact_person 
            ,mmca.date_closed 
            ,mmca.grace_period 
            ,mmca.acct_type 
            ,mmca.assoc_ind 
            ,mmca.short_sell_ind 
            ,mmca.short_name 
            ,mmca.mesdaq_pctlmt 
            ,mmca.date_change 
            ,mmca.resi_code 
            ,mmca.charge_int 
            ,mmca.bdebt 
            ,mmca.assets 
            ,mmca.liabilities 
            ,mmca.income 
            ,mmca.expenses 
            ,mmca.bdebt_his_ind 
            ,mmca.rel_ac1 
            ,mmca.rel_ac2 
            ,mmca.rel_ac3 
            ,mmca.rel_ac4 
            ,mmca.occupation 
            ,mmca.margin_int 
            ,mmca.lst_led_no 
            ,mmca.cur_led_no 
            ,mmca.remarks2 
            ,mmca.acc_type 
            ,mmca.mas_accno 
            ,mmca.legal 
            ,mmca.sell_limit 
            ,mmca.brk_rate 
            ,mmca.brokerage_type 
            ,mmca.cds_acc_no1 
            ,mmca.remarks1 
            ,mmca.payment_bank_code 
            ,mmca.noms 
            ,mmca.dms_date 
            ,mmca.violation_date 
            ,mmca.mcd_branch 
            ,mmca.home_branch 
            ,mmca.eaf_code 
            ,mmca.call_warrant 
            ,mmca.user_id 
            ,mmca.credit_int_rate 
            ,mmca.min_eligible_amt 
            ,mmca.intraday_flag 
            ,mmca.intraday_rate 
            ,mmca.cta_weight 
            ,mmca.sta_weight 
            ,mmca.bo_cds_acc_no 
            ,mmca.ecos_form 
            ,mmca.custodian_no 
            ,mmca.prin_acc 
            ,mmca.armada_type 
            ,mmca.old_authorisee 
            ,mmca.etrade_rate 
            ,mmca.etf 
            ,mmca.cstamp_client_exempt 
            ,mmca.main_branch 
            ,mmca.prev_client_no 
            ,mmca.web_eds 
            ,mmca.place 
            ,mmca.excl_tdr_deduct 
            ,mmca.excl_auto_susp 
            ,mmca.trust_flag 
            ,mmca.mgn_new_int_rate 
            ,mmca.counter_concentration 
            ,mmca.auto_trust 
            ,mmca.margin_pct2 
            ,mmca.df_flag 
            ,mmca.mgn_curr_int_rate 
            ,mmca.product_type 
            ,mmca.web_ecos 
            ,mmca.xeye_clt_grp 
            ,mmca.bursa_violation_date 
            ,mmca.brokerage_type_etrade 
            ,mmca.brokerage_type_odd_lot 
            ,mmca.omnibus 
            ,mmca.cg_tdr_code 
            ,mmca.limit_foreign 
            ,mmca.limit_bursa 
            ,mmca.brokerage_type_intraday 
            ,mmca.brokerage_type_intraday_etrade 
            ,mmca.bursa_violation_date1 
            ,mmca.cif_no 
            ,mmca.brokerage_type_foreign 
            ,mmca.soft_copy 
            ,mmca.exclude_rollover 
            ,mmca.account_status 
            ,mmca.w8ben 
            ,mmca.ic_no_rel1 
            ,mmca.ic_no_rel2 
            ,mmca.ic_no_rel3 
            ,mmca.ic_no_rel4 
            ,mmca.ic_no_rel5 
            ,mmca.rel1 
            ,mmca.rel2 
            ,mmca.rel3 
            ,mmca.rel4 
            ,mmca.rel5 
            ,mmca.brokerage_type_etrade_b 
            ,mmca.brokerage_type_odd_lot_b 
            ,mmca.brokerage_type_b 
            ,mmca.brokerage_type_foreign_b 
            ,mmca.brokerage_type_intraday_b 
            ,mmca.brokerage_type_intraday_etrade_b 
            ,mmca.brokerage_type_etrade_s 
            ,mmca.brokerage_type_odd_lot_s 
            ,mmca.brokerage_type_s 
            ,mmca.brokerage_type_foreign_s 
            ,mmca.brokerage_type_intraday_s 
            ,mmca.brokerage_type_intraday_etrade_s 
            ,mmca.no_free_trade 
            ,mmca.sms 
            ,mmca.mobile_prefix 
            ,mmca.foreign_curr_set 
            ,mmca.num_free_trade 
            ,mmca.etrader_type 
            ,mmca.check_limit 
            ,mmca.auto_margin 
            ,mmca.margin_client_no 
            ,mmca.dup_despatch_mode 
            ,mmca.risk 
            ,mmca.exclude_trader_limit 
            ,mmca.sett_mode_date_change 
            ,mmca.pick_up_fee_pct 
            ,mmca.e_payment 
            ,mmca.mgn_new_int_rate2 
            ,mmca.fund_cost_type 
            ,mmca.check_share 
            ,mmca.citibank_changes 
            ,mmca.citibank_charges 
            ,mmca.cq_market 
            ,mmca.exclude_margin_pro_rate 
            ,mmca.brokerage_type_cash_b 
            ,mmca.brokerage_type_etrade_cash_b 
            ,mmca.clt_consent 
            ,mmca.consent_start_date 
            ,mmca.portfolio 
            ,mmca.expiry_date 
            ,mmca.intraday_auto_contra_option 
            ,mmca.dcf_limit 
            ,mmca.brokerage_type_etb 
            ,mmca.mgn_force_sell_pct 
            ,mmca.mgn_tenure 
            ,mmca.mgn_expiry_date 
            ,mmca.loss_gl_acc_no 
            ,mmca.portfolio_date 
            ,mmca.day_prior_temp_susp 
            ,mmca.day_prior_perm_susp 
            ,mmca.gst_code 
            ,mmca.match_price_decimal_local 
            ,mmca.match_price_decimal_foreign 
            ,mmca.primary_id_expiry_date 
            ,mmca.secondary_id_expiry_date 
            ,mmca.mgn_int_tdr_spread_pct 
            ,mmca.mgn_base_int_rate 
            ,mmca.mgn_int_tdr_share 
            ,mmca.islamic_flag 
            ,mmca.mcd_resident_flag 
            ,mmca.chq_charges_flag 
            ,mmca.chq_charges_tdr_pct 
            ,mmca.brokerage_type_foreign_etrade 
            ,mmca.brokerage_type_foreign_etrade_b 
            ,mmca.brokerage_type_foreign_etrade_s 
            ,mmca.grp_exch_code 
            ,mmca.bdebt_ras 
            ,mmca.twse_declaration 
            ,mmca.joint_acc_amt 
            ,mmca.high_risk_market 
            ,mmca.brokerage_type_leap_normal 
            ,mmca.brokerage_type_leap_etrade 
            ,'current_timestamp()' as etl_timestamp 
       from {params["com_schema"]}.temp_t_mhbos_m_client_all mmca 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_identification_info id 
         on mmca.client_no = id.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 cn 
         on mmca.client_no = cn.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info tel 
         on mmca.client_no = tel.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_dob_info dob 
         on mmca.client_no = dob.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_race_info tmmcr 
         on mmca.client_no = tmmcr.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_email_info tmmcei 
         on mmca.client_no = tmmcei.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_gender_info tmmcgi 
         on mmca.client_no = tmmcgi.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_address_info ad 
         on mmca.client_no = ad.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info nom 
         on mmca.client_no = nom.client_no 
       left join {params["com_schema"]}.temp_t_mhbos_m_client_name_info  mn						--add_20240321 
         on mmca.client_no = mn.client_no 
     ; 
     3.2.1 step1 */
    DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step1
""")
spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step1 AS
    SELECT
      id.client_no,
      (
        id.primary_identification_type_flag || id.primary_identification_no_flag || id.secondary_identification_type_flag || id.secondary_identification_no_flag || cn.client_name_flag || cn.client_name1_flag || cn.client_name2_flag || cn.client_name3_flag || tel.mobile_no_flag || tel.fax_no_flag || tel.tel_no_home_flag || tel.tel_no_office_flag || dob.date_of_birth_flag || tmmcr.race_flag || tmmcei.email_flag || tmmcgi.sex_flag
      ) AS clean_rule_flag, /* ,id.primary_identification_type -- 20250313 
     ,mn.primary_identification_no 
     ,replace(id.secondary_identification_type, '@[]', '') as secondary_identification_type -- 20250620 
     ,replace(id.secondary_identification_no, '@[]', '') as secondary_identification_no -- 20250620 
     ,mn.customer_name */
      cn.customer_name_concatenate,
      cn.client_name,
      cn.client_name1,
      cn.client_name2,
      cn.client_name3,
      tel.mobile_no,
      tel.fax_no,
      tel.tel_no_home,
      tel.tel_no_office,
      dob.date_of_birth,
      tmmcr.race,
      tmmcei.einvoice_email, /* 20250903 einvoice_email */
      tmmcei.email_1,
      tmmcei.email_2,
      tmmcei.email_3,
      tmmcei.email_4,
      tmmcei.email_5,
      tmmcei.email_6,
      tmmcei.email_7,
      tmmcei.email_8,
      tmmcei.email_9,
      tmmcei.email_10,
      tmmcgi.sex
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_identification_info AS id
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_customer_name_2 AS cn
      ON id.client_no = cn.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_telephone_number_info AS tel
      ON id.client_no = tel.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_dob_info AS dob
      ON id.client_no = dob.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_race_info AS tmmcr
      ON id.client_no = tmmcr.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_email_info AS tmmcei
      ON id.client_no = tmmcei.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_gender_info AS tmmcgi
      ON id.client_no = tmmcgi.client_no
""")
spark.sql(f"""
/* 3.2.2 step2 */
    DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step2
""")
spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step2 AS
    SELECT
      step1.client_no, /* 2025/09/08: Add customer_name_flag */
      step1.clean_rule_flag || mn.customer_name_flag AS clean_rule_flag,
      mn.primary_identification_type, /* modify 20250313 */
      mn.primary_identification_no,
      mn.secondary_identification_type, /* modify from step1. to mn. 20250620 */
      mn.secondary_identification_no, /* modify from step1. to mn. 20250620 */
      mn.customer_name,
      step1.customer_name_concatenate,
      step1.client_name,
      step1.client_name1,
      step1.client_name2,
      step1.client_name3,
      step1.mobile_no,
      step1.fax_no,
      step1.tel_no_home,
      step1.tel_no_office,
      step1.date_of_birth,
      step1.race,
      step1.email_1,
      step1.email_2,
      step1.email_3,
      step1.email_4,
      step1.email_5,
      step1.email_6,
      step1.email_7,
      step1.email_8,
      step1.email_9,
      step1.email_10,
      step1.sex,
      ad.addr1,
      ad.addr2,
      ad.addr3,
      ad.addr4,
      ad.postcode,
      ad.city,
      ad.state,
      ad.perm_addr1,
      ad.perm_addr2,
      ad.perm_addr3,
      ad.perm_addr4,
      ad.perm_postcode,
      ad.perm_city,
      ad.perm_state,
      ad.perm_country, /* added new field 20250715 */
      COALESCE(nom.noms_ind, 'N') AS noms_ind,
      nom.cleaned_nominees_name,
      mn.principal_name, /* modify 20240518 */
      nom.intermediary_name,
      mn.beneficiary_name, /* modify 20240518 */
      nom.nominees_type,
      nom.pledged_securities_flag,
      step1.einvoice_email /* 20250903 einvoice_email */
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_identification_info_step1 AS step1
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_address_info AS ad
      ON step1.client_no = ad.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_nominees_info AS nom
      ON step1.client_no = nom.client_no
    LEFT JOIN {params["com_schema"]}.temp_t_mhbos_m_client_name_info AS mn
      ON step1.client_no = mn.client_no
""")

# ─── CONSOLIDATED TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_consolidated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_consolidated (
        client_no VARCHAR(9)
        , clean_rule_flag VARCHAR(60)
        , primary_identification_type VARCHAR(10)
        , primary_identification_no VARCHAR(60)
        , secondary_identification_type VARCHAR(10)
        , secondary_identification_no VARCHAR(60)
        , customer_name VARCHAR(250)
        , customer_name_concatenate VARCHAR(250)
        , client_name VARCHAR(60)
        , client_name1 VARCHAR(60)
        , client_name2 VARCHAR(60)
        , client_name3 VARCHAR(60)
        , mobile_no VARCHAR(20)
        , fax_no VARCHAR(20)
        , tel_no_home VARCHAR(20)
        , tel_no_office VARCHAR(20)
        , date_of_birth TIMESTAMP
        , race VARCHAR(30)
        , email_1 VARCHAR(300)
        , email_2 VARCHAR(300)
        , email_3 VARCHAR(300)
        , email_4 VARCHAR(300)
        , email_5 VARCHAR(300)
        , email_6 VARCHAR(300)
        , email_7 VARCHAR(300)
        , email_8 VARCHAR(300)
        , email_9 VARCHAR(300)
        , email_10 VARCHAR(300)
        , sex VARCHAR(10)
        , addr1 VARCHAR(45)
        , addr2 VARCHAR(45)
        , addr3 VARCHAR(45)
        , addr4 VARCHAR(45)
        , postcode VARCHAR(6)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , perm_addr1 VARCHAR(45)
        , perm_addr2 VARCHAR(45)
        , perm_addr3 VARCHAR(45)
        , perm_addr4 VARCHAR(45)
        , perm_postcode VARCHAR(6)
        , perm_city VARCHAR(255)
        , perm_state VARCHAR(50)
        , noms_ind VARCHAR(1)
        , cleaned_nominees_name VARCHAR(300)
        , principal_name VARCHAR(300)
        , intermediary_name VARCHAR(300)
        , beneficiary_name VARCHAR(300)
        , nominees_type VARCHAR(10)
        , pledged_securities_flag VARCHAR(1)
        , id_type VARCHAR(10)
        , ic_no_new VARCHAR(15)
        , ic_no_old VARCHAR(14)
        , secondary_id_type VARCHAR(2)
        , secondary_id_no VARCHAR(15)
        , source_client_name VARCHAR(50)
        , source_client_name1 VARCHAR(50)
        , source_client_name2 VARCHAR(50)
        , source_client_name3 VARCHAR(50)
        , source_mobile_no VARCHAR(15)
        , source_fax_no VARCHAR(15)
        , source_tel_no_home VARCHAR(15)
        , source_tel_no_office VARCHAR(15)
        , source_date_of_birth TIMESTAMP
        , source_race VARCHAR(20)
        , source_email VARCHAR(300)
        , source_sex VARCHAR(10)
        , client_group VARCHAR(14)
        , cds_acc_no VARCHAR(9)
        , tdr_code VARCHAR(5)
        , client_type VARCHAR(3)
        , margin VARCHAR(1)
        , last_margin_date TIMESTAMP
        , int_rate DECIMAL(5, 2)
        , auto_ded VARCHAR(1)
        , despatch_mode VARCHAR(2)
        , copies DECIMAL(2, 0)
        , prohibit_trade VARCHAR(1)
        , custody_status VARCHAR(1)
        , country VARCHAR(3)
        , margin_limit DECIMAL(9, 0)
        , margin_pct DECIMAL(5, 2)
        , rollover_rate DECIMAL(6, 3)
        , form_completed VARCHAR(1)
        , last_tran_date TIMESTAMP
        , ytd_bvalue DECIMAL(12, 2)
        , ytd_svalue DECIMAL(12, 2)
        , ytd_brokerage DECIMAL(11, 2)
        , os_led_bal DECIMAL(12, 2)
        , title VARCHAR(20)
        , date_created TIMESTAMP
        , stop_payt VARCHAR(1)
        , acc_payee VARCHAR(100)
        , category VARCHAR(1)
        , auto_contra VARCHAR(1)
        , pnl_acc_no VARCHAR(18)
        , cr_limit DECIMAL(9, 0)
        , trust_bal DECIMAL(12, 2)
        , avg_ind VARCHAR(8)
        , remarks VARCHAR(60)
        , contact_person VARCHAR(40)
        , date_closed TIMESTAMP
        , grace_period DECIMAL(3, 0)
        , acct_type DECIMAL(2, 0)
        , assoc_ind VARCHAR(1)
        , short_sell_ind VARCHAR(1)
        , short_name VARCHAR(10)
        , mesdaq_pctlmt DECIMAL(5, 2)
        , date_change TIMESTAMP
        , resi_code VARCHAR(5)
        , charge_int VARCHAR(1)
        , bdebt VARCHAR(1)
        , assets DECIMAL(12, 2)
        , liabilities DECIMAL(12, 2)
        , income DECIMAL(12, 2)
        , expenses DECIMAL(12, 2)
        , bdebt_his_ind VARCHAR(3)
        , rel_ac1 VARCHAR(9)
        , rel_ac2 VARCHAR(9)
        , rel_ac3 VARCHAR(9)
        , rel_ac4 VARCHAR(9)
        , occupation VARCHAR(60)
        , margin_int DECIMAL(12, 2)
        , lst_led_no DECIMAL(9, 0)
        , cur_led_no DECIMAL(9, 0)
        , remarks2 VARCHAR(60)
        , acc_type VARCHAR(1)
        , mas_accno VARCHAR(9)
        , legal VARCHAR(1)
        , sell_limit DECIMAL(9, 0)
        , brk_rate DECIMAL(9, 4)
        , brokerage_type VARCHAR(3)
        , cds_acc_no1 VARCHAR(9)
        , remarks1 VARCHAR(60)
        , payment_bank_code VARCHAR(5)
        , noms VARCHAR(1)
        , dms_date TIMESTAMP
        , violation_date TIMESTAMP
        , mcd_branch VARCHAR(3)
        , home_branch VARCHAR(3)
        , eaf_code VARCHAR(1)
        , call_warrant VARCHAR(1)
        , user_id VARCHAR(20)
        , credit_int_rate DECIMAL(5, 2)
        , min_eligible_amt DECIMAL(18, 4)
        , intraday_flag VARCHAR(1)
        , intraday_rate DECIMAL(5, 4)
        , cta_weight DECIMAL(3, 0)
        , sta_weight DECIMAL(3, 0)
        , bo_cds_acc_no VARCHAR(20)
        , ecos_form VARCHAR(1)
        , custodian_no VARCHAR(7)
        , prin_acc VARCHAR(2)
        , armada_type VARCHAR(8)
        , old_authorisee VARCHAR(5)
        , etrade_rate DECIMAL(9, 4)
        , etf VARCHAR(1)
        , cstamp_client_exempt VARCHAR(1)
        , main_branch VARCHAR(3)
        , prev_client_no VARCHAR(9)
        , web_eds VARCHAR(1)
        , place VARCHAR(5)
        , excl_tdr_deduct VARCHAR(1)
        , excl_auto_susp VARCHAR(1)
        , trust_flag VARCHAR(1)
        , mgn_new_int_rate DECIMAL(5, 2)
        , counter_concentration DECIMAL(5, 2)
        , auto_trust VARCHAR(1)
        , margin_pct2 DECIMAL(5, 2)
        , df_flag VARCHAR(1)
        , mgn_curr_int_rate DECIMAL(5, 2)
        , product_type VARCHAR(1)
        , web_ecos VARCHAR(1)
        , xeye_clt_grp VARCHAR(14)
        , bursa_violation_date TIMESTAMP
        , brokerage_type_etrade VARCHAR(3)
        , brokerage_type_odd_lot VARCHAR(3)
        , omnibus VARCHAR(1)
        , cg_tdr_code VARCHAR(7)
        , limit_foreign DECIMAL(9, 4)
        , limit_bursa DECIMAL(9, 4)
        , brokerage_type_intraday VARCHAR(3)
        , brokerage_type_intraday_etrade VARCHAR(3)
        , bursa_violation_date1 TIMESTAMP
        , cif_no VARCHAR(20)
        , brokerage_type_foreign VARCHAR(3)
        , soft_copy VARCHAR(1)
        , exclude_rollover VARCHAR(1)
        , account_status VARCHAR(1)
        , w8ben VARCHAR(1)
        , ic_no_rel1 VARCHAR(15)
        , ic_no_rel2 VARCHAR(15)
        , ic_no_rel3 VARCHAR(15)
        , ic_no_rel4 VARCHAR(15)
        , ic_no_rel5 VARCHAR(15)
        , rel1 VARCHAR(15)
        , rel2 VARCHAR(15)
        , rel3 VARCHAR(15)
        , rel4 VARCHAR(15)
        , rel5 VARCHAR(15)
        , brokerage_type_etrade_b VARCHAR(3)
        , brokerage_type_odd_lot_b VARCHAR(3)
        , brokerage_type_b VARCHAR(3)
        , brokerage_type_foreign_b VARCHAR(3)
        , brokerage_type_intraday_b VARCHAR(3)
        , brokerage_type_intraday_etrade_b VARCHAR(3)
        , brokerage_type_etrade_s VARCHAR(3)
        , brokerage_type_odd_lot_s VARCHAR(3)
        , brokerage_type_s VARCHAR(3)
        , brokerage_type_foreign_s VARCHAR(3)
        , brokerage_type_intraday_s VARCHAR(3)
        , brokerage_type_intraday_etrade_s VARCHAR(3)
        , no_free_trade DECIMAL(2, 0)
        , sms VARCHAR(1)
        , mobile_prefix VARCHAR(5)
        , foreign_curr_set VARCHAR(1)
        , num_free_trade DECIMAL(2, 0)
        , etrader_type VARCHAR(2)
        , check_limit VARCHAR(1)
        , auto_margin VARCHAR(1)
        , margin_client_no VARCHAR(9)
        , dup_despatch_mode VARCHAR(1)
        , risk VARCHAR(1)
        , exclude_trader_limit VARCHAR(1)
        , sett_mode_date_change TIMESTAMP
        , pick_up_fee_pct DECIMAL(5, 2)
        , e_payment VARCHAR(1)
        , mgn_new_int_rate2 DECIMAL(5, 2)
        , fund_cost_type VARCHAR(1)
        , check_share VARCHAR(1)
        , citibank_changes VARCHAR(1)
        , citibank_charges VARCHAR(1)
        , cq_market VARCHAR(1)
        , exclude_margin_pro_rate VARCHAR(1)
        , brokerage_type_cash_b VARCHAR(3)
        , brokerage_type_etrade_cash_b VARCHAR(3)
        , clt_consent VARCHAR(1)
        , consent_start_date TIMESTAMP
        , portfolio VARCHAR(1)
        , expiry_date TIMESTAMP
        , intraday_auto_contra_option VARCHAR(1)
        , dcf_limit DECIMAL(9, 0)
        , brokerage_type_etb VARCHAR(3)
        , mgn_force_sell_pct DECIMAL(5, 2)
        , mgn_tenure DECIMAL(3, 0)
        , mgn_expiry_date TIMESTAMP
        , loss_gl_acc_no VARCHAR(18)
        , portfolio_date TIMESTAMP
        , day_prior_temp_susp DECIMAL(4, 0)
        , day_prior_perm_susp DECIMAL(4, 0)
        , gst_code VARCHAR(3)
        , match_price_decimal_local DECIMAL(1, 0)
        , match_price_decimal_foreign DECIMAL(1, 0)
        , primary_id_expiry_date TIMESTAMP
        , secondary_id_expiry_date TIMESTAMP
        , mgn_int_tdr_spread_pct DECIMAL(5, 2)
        , mgn_base_int_rate DECIMAL(5, 2)
        , mgn_int_tdr_share DECIMAL(5, 2)
        , islamic_flag VARCHAR(1)
        , mcd_resident_flag VARCHAR(1)
        , chq_charges_flag VARCHAR(1)
        , chq_charges_tdr_pct DECIMAL(9, 2)
        , brokerage_type_foreign_etrade VARCHAR(3)
        , brokerage_type_foreign_etrade_b VARCHAR(3)
        , brokerage_type_foreign_etrade_s VARCHAR(3)
        , grp_exch_code VARCHAR(5)
        , bdebt_ras VARCHAR(1)
        , twse_declaration VARCHAR(1)
        , joint_acc_amt DECIMAL(12, 2)
        , high_risk_market VARCHAR(2)
        , brokerage_type_leap_normal VARCHAR(3)
        , brokerage_type_leap_etrade VARCHAR(3)
        , perm_country VARCHAR(3)
        , type_of_account STRING
        , einvoice_email STRING
        , dl_record_status VARCHAR(10)
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── INSERT INTO CONSOLIDATED TABLE ────────────────────────────────────────────────────────

spark.sql(f"""
/* 3.2 insert data to target table */
    WITH today_accounts AS (
      SELECT DISTINCT
        client_no
      FROM {params["com_schema"]}.temp_t_mhbos_m_client
    )
    INSERT INTO {params["com_schema"]}.temp_t_mhbos_m_client_consolidated
    SELECT
      client_no,
      clean_rule_flag,
      primary_identification_type,
      primary_identification_no,
      secondary_identification_type,
      secondary_identification_no,
      customer_name,
      customer_name_concatenate,
      client_name,
      client_name1,
      client_name2,
      client_name3,
      mobile_no,
      fax_no,
      tel_no_home,
      tel_no_office,
      date_of_birth,
      race,
      email_1,
      email_2,
      email_3,
      email_4,
      email_5,
      email_6,
      email_7,
      email_8,
      email_9,
      email_10,
      sex,
      addr1,
      addr2,
      addr3,
      addr4,
      postcode,
      city,
      state,
      perm_addr1,
      perm_addr2,
      perm_addr3,
      perm_addr4,
      perm_postcode,
      perm_city,
      perm_state,
      noms_ind,
      cleaned_nominees_name,
      principal_name,
      intermediary_name,
      beneficiary_name,
      nominees_type,
      pledged_securities_flag,
      id_type,
      ic_no_new,
      ic_no_old,
      secondary_id_type,
      secondary_id_no,
      source_client_name,
      source_client_name1,
      source_client_name2,
      source_client_name3,
      source_mobile_no,
      source_fax_no,
      source_tel_no_home,
      source_tel_no_office,
      source_date_of_birth,
      source_race,
      source_email,
      source_sex,
      client_group,
      cds_acc_no,
      tdr_code,
      client_type,
      margin,
      last_margin_date,
      int_rate,
      auto_ded,
      despatch_mode,
      copies,
      prohibit_trade,
      custody_status,
      country,
      margin_limit,
      margin_pct,
      rollover_rate,
      form_completed,
      last_tran_date,
      ytd_bvalue,
      ytd_svalue,
      ytd_brokerage,
      os_led_bal,
      title,
      date_created,
      stop_payt,
      acc_payee,
      category,
      auto_contra,
      pnl_acc_no,
      cr_limit,
      trust_bal,
      avg_ind,
      remarks,
      contact_person,
      date_closed,
      grace_period,
      acct_type,
      assoc_ind,
      short_sell_ind,
      short_name,
      mesdaq_pctlmt,
      date_change,
      resi_code,
      charge_int,
      bdebt,
      assets,
      liabilities,
      income,
      expenses,
      bdebt_his_ind,
      rel_ac1,
      rel_ac2,
      rel_ac3,
      rel_ac4,
      occupation,
      margin_int,
      lst_led_no,
      cur_led_no,
      remarks2,
      acc_type,
      mas_accno,
      legal,
      sell_limit,
      brk_rate,
      brokerage_type,
      cds_acc_no1,
      remarks1,
      payment_bank_code,
      noms,
      dms_date,
      violation_date,
      mcd_branch,
      home_branch,
      eaf_code,
      call_warrant,
      user_id,
      credit_int_rate,
      min_eligible_amt,
      intraday_flag,
      intraday_rate,
      cta_weight,
      sta_weight,
      bo_cds_acc_no,
      ecos_form,
      custodian_no,
      prin_acc,
      armada_type,
      old_authorisee,
      etrade_rate,
      etf,
      cstamp_client_exempt,
      main_branch,
      prev_client_no,
      web_eds,
      place,
      excl_tdr_deduct,
      excl_auto_susp,
      trust_flag,
      mgn_new_int_rate,
      counter_concentration,
      auto_trust,
      margin_pct2,
      df_flag,
      mgn_curr_int_rate,
      product_type,
      web_ecos,
      xeye_clt_grp,
      bursa_violation_date,
      brokerage_type_etrade,
      brokerage_type_odd_lot,
      omnibus,
      cg_tdr_code,
      limit_foreign,
      limit_bursa,
      brokerage_type_intraday,
      brokerage_type_intraday_etrade,
      bursa_violation_date1,
      cif_no,
      brokerage_type_foreign,
      soft_copy,
      exclude_rollover,
      account_status,
      w8ben,
      ic_no_rel1,
      ic_no_rel2,
      ic_no_rel3,
      ic_no_rel4,
      ic_no_rel5,
      rel1,
      rel2,
      rel3,
      rel4,
      rel5,
      brokerage_type_etrade_b,
      brokerage_type_odd_lot_b,
      brokerage_type_b,
      brokerage_type_foreign_b,
      brokerage_type_intraday_b,
      brokerage_type_intraday_etrade_b,
      brokerage_type_etrade_s,
      brokerage_type_odd_lot_s,
      brokerage_type_s,
      brokerage_type_foreign_s,
      brokerage_type_intraday_s,
      brokerage_type_intraday_etrade_s,
      no_free_trade,
      sms,
      mobile_prefix,
      foreign_curr_set,
      num_free_trade,
      etrader_type,
      check_limit,
      auto_margin,
      margin_client_no,
      dup_despatch_mode,
      risk,
      exclude_trader_limit,
      sett_mode_date_change,
      pick_up_fee_pct,
      e_payment,
      mgn_new_int_rate2,
      fund_cost_type,
      check_share,
      citibank_changes,
      citibank_charges,
      cq_market,
      exclude_margin_pro_rate,
      brokerage_type_cash_b,
      brokerage_type_etrade_cash_b,
      clt_consent,
      consent_start_date,
      portfolio,
      expiry_date,
      intraday_auto_contra_option,
      dcf_limit,
      brokerage_type_etb,
      mgn_force_sell_pct,
      mgn_tenure,
      mgn_expiry_date,
      loss_gl_acc_no,
      portfolio_date,
      day_prior_temp_susp,
      day_prior_perm_susp,
      gst_code,
      match_price_decimal_local,
      match_price_decimal_foreign,
      primary_id_expiry_date,
      secondary_id_expiry_date,
      mgn_int_tdr_spread_pct,
      mgn_base_int_rate,
      mgn_int_tdr_share,
      islamic_flag,
      mcd_resident_flag,
      chq_charges_flag,
      chq_charges_tdr_pct,
      brokerage_type_foreign_etrade,
      brokerage_type_foreign_etrade_b,
      brokerage_type_foreign_etrade_s,
      grp_exch_code,
      bdebt_ras,
      twse_declaration,
      joint_acc_amt,
      high_risk_market,
      brokerage_type_leap_normal,
      brokerage_type_leap_etrade,
      CURRENT_TIMESTAMP() AS etl_timestamp,
      perm_country,
      type_of_account,
      einvoice_email
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_consolidated AS t
    WHERE
      etl_dt = '{last_date}'
      AND NOT EXISTS(
        SELECT
          1
        FROM today_accounts AS ta
        WHERE
          ta.client_no = t.client_no
      )
    UNION ALL
    SELECT
      client_no,
      clean_rule_flag,
      primary_identification_type,
      primary_identification_no,
      secondary_identification_type,
      secondary_identification_no,
      customer_name,
      customer_name_concatenate,
      client_name,
      client_name1,
      client_name2,
      client_name3,
      mobile_no,
      fax_no,
      tel_no_home,
      tel_no_office,
      date_of_birth,
      race,
      email_1,
      email_2,
      email_3,
      email_4,
      email_5,
      email_6,
      email_7,
      email_8,
      email_9,
      email_10,
      sex,
      addr1,
      addr2,
      addr3,
      addr4,
      postcode,
      city,
      state,
      perm_addr1,
      perm_addr2,
      perm_addr3,
      perm_addr4,
      perm_postcode,
      perm_city,
      perm_state,
      noms_ind,
      cleaned_nominees_name,
      principal_name,
      intermediary_name,
      beneficiary_name,
      nominees_type,
      pledged_securities_flag,
      id_type,
      ic_no_new,
      ic_no_old,
      secondary_id_type,
      secondary_id_no,
      source_client_name,
      source_client_name1,
      source_client_name2,
      source_client_name3,
      source_mobile_no,
      source_fax_no,
      source_tel_no_home,
      source_tel_no_office,
      source_date_of_birth,
      source_race,
      source_email,
      source_sex,
      client_group,
      cds_acc_no,
      tdr_code,
      client_type,
      margin,
      last_margin_date,
      int_rate,
      auto_ded,
      despatch_mode,
      copies,
      prohibit_trade,
      custody_status,
      country,
      margin_limit,
      margin_pct,
      rollover_rate,
      form_completed,
      last_tran_date,
      ytd_bvalue,
      ytd_svalue,
      ytd_brokerage,
      os_led_bal,
      title,
      date_created,
      stop_payt,
      acc_payee,
      category,
      auto_contra,
      pnl_acc_no,
      cr_limit,
      trust_bal,
      avg_ind,
      remarks,
      contact_person,
      date_closed,
      grace_period,
      acct_type,
      assoc_ind,
      short_sell_ind,
      short_name,
      mesdaq_pctlmt,
      date_change,
      resi_code,
      charge_int,
      bdebt,
      assets,
      liabilities,
      income,
      expenses,
      bdebt_his_ind,
      rel_ac1,
      rel_ac2,
      rel_ac3,
      rel_ac4,
      occupation,
      margin_int,
      lst_led_no,
      cur_led_no,
      remarks2,
      acc_type,
      mas_accno,
      legal,
      sell_limit,
      brk_rate,
      brokerage_type,
      cds_acc_no1,
      remarks1,
      payment_bank_code,
      noms,
      dms_date,
      violation_date,
      mcd_branch,
      home_branch,
      eaf_code,
      call_warrant,
      user_id,
      credit_int_rate,
      min_eligible_amt,
      intraday_flag,
      intraday_rate,
      cta_weight,
      sta_weight,
      bo_cds_acc_no,
      ecos_form,
      custodian_no,
      prin_acc,
      armada_type,
      old_authorisee,
      etrade_rate,
      etf,
      cstamp_client_exempt,
      main_branch,
      prev_client_no,
      web_eds,
      place,
      excl_tdr_deduct,
      excl_auto_susp,
      trust_flag,
      mgn_new_int_rate,
      counter_concentration,
      auto_trust,
      margin_pct2,
      df_flag,
      mgn_curr_int_rate,
      product_type,
      web_ecos,
      xeye_clt_grp,
      bursa_violation_date,
      brokerage_type_etrade,
      brokerage_type_odd_lot,
      omnibus,
      cg_tdr_code,
      limit_foreign,
      limit_bursa,
      brokerage_type_intraday,
      brokerage_type_intraday_etrade,
      bursa_violation_date1,
      cif_no,
      brokerage_type_foreign,
      soft_copy,
      exclude_rollover,
      account_status,
      w8ben,
      ic_no_rel1,
      ic_no_rel2,
      ic_no_rel3,
      ic_no_rel4,
      ic_no_rel5,
      rel1,
      rel2,
      rel3,
      rel4,
      rel5,
      brokerage_type_etrade_b,
      brokerage_type_odd_lot_b,
      brokerage_type_b,
      brokerage_type_foreign_b,
      brokerage_type_intraday_b,
      brokerage_type_intraday_etrade_b,
      brokerage_type_etrade_s,
      brokerage_type_odd_lot_s,
      brokerage_type_s,
      brokerage_type_foreign_s,
      brokerage_type_intraday_s,
      brokerage_type_intraday_etrade_s,
      no_free_trade,
      sms,
      mobile_prefix,
      foreign_curr_set,
      num_free_trade,
      etrader_type,
      check_limit,
      auto_margin,
      margin_client_no,
      dup_despatch_mode,
      risk,
      exclude_trader_limit,
      sett_mode_date_change,
      pick_up_fee_pct,
      e_payment,
      mgn_new_int_rate2,
      fund_cost_type,
      check_share,
      citibank_changes,
      citibank_charges,
      cq_market,
      exclude_margin_pro_rate,
      brokerage_type_cash_b,
      brokerage_type_etrade_cash_b,
      clt_consent,
      consent_start_date,
      portfolio,
      expiry_date,
      intraday_auto_contra_option,
      dcf_limit,
      brokerage_type_etb,
      mgn_force_sell_pct,
      mgn_tenure,
      mgn_expiry_date,
      loss_gl_acc_no,
      portfolio_date,
      day_prior_temp_susp,
      day_prior_perm_susp,
      gst_code,
      match_price_decimal_local,
      match_price_decimal_foreign,
      primary_id_expiry_date,
      secondary_id_expiry_date,
      mgn_int_tdr_spread_pct,
      mgn_base_int_rate,
      mgn_int_tdr_share,
      islamic_flag,
      mcd_resident_flag,
      chq_charges_flag,
      chq_charges_tdr_pct,
      brokerage_type_foreign_etrade,
      brokerage_type_foreign_etrade_b,
      brokerage_type_foreign_etrade_s,
      grp_exch_code,
      bdebt_ras,
      twse_declaration,
      joint_acc_amt,
      high_risk_market,
      brokerage_type_leap_normal,
      brokerage_type_leap_etrade,
      CURRENT_TIMESTAMP() AS etl_timestamp,
      perm_country,
      type_of_account,
      einvoice_email
    FROM temp_t_mhbos_m_client
""")


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_updated""")
spark.sql(f"""
    CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated (
        client_no VARCHAR(9)
        , clean_rule_flag VARCHAR(60)
        , primary_identification_type VARCHAR(10)
        , primary_identification_no VARCHAR(60)
        , secondary_identification_type VARCHAR(10)
        , secondary_identification_no VARCHAR(60)
        , customer_name VARCHAR(250)
        , customer_name_concatenate VARCHAR(250)
        , client_name VARCHAR(60)
        , client_name1 VARCHAR(60)
        , client_name2 VARCHAR(60)
        , client_name3 VARCHAR(60)
        , mobile_no VARCHAR(20)
        , fax_no VARCHAR(20)
        , tel_no_home VARCHAR(20)
        , tel_no_office VARCHAR(20)
        , date_of_birth TIMESTAMP
        , race VARCHAR(30)
        , email_1 VARCHAR(300)
        , email_2 VARCHAR(300)
        , email_3 VARCHAR(300)
        , email_4 VARCHAR(300)
        , email_5 VARCHAR(300)
        , email_6 VARCHAR(300)
        , email_7 VARCHAR(300)
        , email_8 VARCHAR(300)
        , email_9 VARCHAR(300)
        , email_10 VARCHAR(300)
        , sex VARCHAR(10)
        , addr1 VARCHAR(45)
        , addr2 VARCHAR(45)
        , addr3 VARCHAR(45)
        , addr4 VARCHAR(45)
        , postcode VARCHAR(6)
        , city VARCHAR(255)
        , state VARCHAR(50)
        , perm_addr1 VARCHAR(45)
        , perm_addr2 VARCHAR(45)
        , perm_addr3 VARCHAR(45)
        , perm_addr4 VARCHAR(45)
        , perm_postcode VARCHAR(6)
        , perm_city VARCHAR(255)
        , perm_state VARCHAR(50)
        , noms_ind VARCHAR(1)
        , cleaned_nominees_name VARCHAR(300)
        , principal_name VARCHAR(300)
        , intermediary_name VARCHAR(300)
        , beneficiary_name VARCHAR(300)
        , nominees_type VARCHAR(10)
        , pledged_securities_flag VARCHAR(1)
        , id_type VARCHAR(10)
        , ic_no_new VARCHAR(15)
        , ic_no_old VARCHAR(14)
        , secondary_id_type VARCHAR(2)
        , secondary_id_no VARCHAR(15)
        , source_client_name VARCHAR(50)
        , source_client_name1 VARCHAR(50)
        , source_client_name2 VARCHAR(50)
        , source_client_name3 VARCHAR(50)
        , source_mobile_no VARCHAR(15)
        , source_fax_no VARCHAR(15)
        , source_tel_no_home VARCHAR(15)
        , source_tel_no_office VARCHAR(15)
        , source_date_of_birth TIMESTAMP
        , source_race VARCHAR(20)
        , source_email VARCHAR(300)
        , source_sex VARCHAR(10)
        , client_group VARCHAR(14)
        , cds_acc_no VARCHAR(9)
        , tdr_code VARCHAR(5)
        , client_type VARCHAR(3)
        , margin VARCHAR(1)
        , last_margin_date TIMESTAMP
        , int_rate DECIMAL(5, 2)
        , auto_ded VARCHAR(1)
        , despatch_mode VARCHAR(2)
        , copies DECIMAL(2, 0)
        , prohibit_trade VARCHAR(1)
        , custody_status VARCHAR(1)
        , country VARCHAR(3)
        , margin_limit DECIMAL(9, 0)
        , margin_pct DECIMAL(5, 2)
        , rollover_rate DECIMAL(6, 3)
        , form_completed VARCHAR(1)
        , last_tran_date TIMESTAMP
        , ytd_bvalue DECIMAL(12, 2)
        , ytd_svalue DECIMAL(12, 2)
        , ytd_brokerage DECIMAL(11, 2)
        , os_led_bal DECIMAL(12, 2)
        , title VARCHAR(20)
        , date_created TIMESTAMP
        , stop_payt VARCHAR(1)
        , acc_payee VARCHAR(100)
        , category VARCHAR(1)
        , auto_contra VARCHAR(1)
        , pnl_acc_no VARCHAR(18)
        , cr_limit DECIMAL(9, 0)
        , trust_bal DECIMAL(12, 2)
        , avg_ind VARCHAR(8)
        , remarks VARCHAR(60)
        , contact_person VARCHAR(40)
        , date_closed TIMESTAMP
        , grace_period DECIMAL(3, 0)
        , acct_type DECIMAL(2, 0)
        , assoc_ind VARCHAR(1)
        , short_sell_ind VARCHAR(1)
        , short_name VARCHAR(10)
        , mesdaq_pctlmt DECIMAL(5, 2)
        , date_change TIMESTAMP
        , resi_code VARCHAR(5)
        , charge_int VARCHAR(1)
        , bdebt VARCHAR(1)
        , assets DECIMAL(12, 2)
        , liabilities DECIMAL(12, 2)
        , income DECIMAL(12, 2)
        , expenses DECIMAL(12, 2)
        , bdebt_his_ind VARCHAR(3)
        , rel_ac1 VARCHAR(9)
        , rel_ac2 VARCHAR(9)
        , rel_ac3 VARCHAR(9)
        , rel_ac4 VARCHAR(9)
        , occupation VARCHAR(60)
        , margin_int DECIMAL(12, 2)
        , lst_led_no DECIMAL(9, 0)
        , cur_led_no DECIMAL(9, 0)
        , remarks2 VARCHAR(60)
        , acc_type VARCHAR(1)
        , mas_accno VARCHAR(9)
        , legal VARCHAR(1)
        , sell_limit DECIMAL(9, 0)
        , brk_rate DECIMAL(9, 4)
        , brokerage_type VARCHAR(3)
        , cds_acc_no1 VARCHAR(9)
        , remarks1 VARCHAR(60)
        , payment_bank_code VARCHAR(5)
        , noms VARCHAR(1)
        , dms_date TIMESTAMP
        , violation_date TIMESTAMP
        , mcd_branch VARCHAR(3)
        , home_branch VARCHAR(3)
        , eaf_code VARCHAR(1)
        , call_warrant VARCHAR(1)
        , user_id VARCHAR(20)
        , credit_int_rate DECIMAL(5, 2)
        , min_eligible_amt DECIMAL(18, 4)
        , intraday_flag VARCHAR(1)
        , intraday_rate DECIMAL(5, 4)
        , cta_weight DECIMAL(3, 0)
        , sta_weight DECIMAL(3, 0)
        , bo_cds_acc_no VARCHAR(20)
        , ecos_form VARCHAR(1)
        , custodian_no VARCHAR(7)
        , prin_acc VARCHAR(2)
        , armada_type VARCHAR(8)
        , old_authorisee VARCHAR(5)
        , etrade_rate DECIMAL(9, 4)
        , etf VARCHAR(1)
        , cstamp_client_exempt VARCHAR(1)
        , main_branch VARCHAR(3)
        , prev_client_no VARCHAR(9)
        , web_eds VARCHAR(1)
        , place VARCHAR(5)
        , excl_tdr_deduct VARCHAR(1)
        , excl_auto_susp VARCHAR(1)
        , trust_flag VARCHAR(1)
        , mgn_new_int_rate DECIMAL(5, 2)
        , counter_concentration DECIMAL(5, 2)
        , auto_trust VARCHAR(1)
        , margin_pct2 DECIMAL(5, 2)
        , df_flag VARCHAR(1)
        , mgn_curr_int_rate DECIMAL(5, 2)
        , product_type VARCHAR(1)
        , web_ecos VARCHAR(1)
        , xeye_clt_grp VARCHAR(14)
        , bursa_violation_date TIMESTAMP
        , brokerage_type_etrade VARCHAR(3)
        , brokerage_type_odd_lot VARCHAR(3)
        , omnibus VARCHAR(1)
        , cg_tdr_code VARCHAR(7)
        , limit_foreign DECIMAL(9, 4)
        , limit_bursa DECIMAL(9, 4)
        , brokerage_type_intraday VARCHAR(3)
        , brokerage_type_intraday_etrade VARCHAR(3)
        , bursa_violation_date1 TIMESTAMP
        , cif_no VARCHAR(20)
        , brokerage_type_foreign VARCHAR(3)
        , soft_copy VARCHAR(1)
        , exclude_rollover VARCHAR(1)
        , account_status VARCHAR(1)
        , w8ben VARCHAR(1)
        , ic_no_rel1 VARCHAR(15)
        , ic_no_rel2 VARCHAR(15)
        , ic_no_rel3 VARCHAR(15)
        , ic_no_rel4 VARCHAR(15)
        , ic_no_rel5 VARCHAR(15)
        , rel1 VARCHAR(15)
        , rel2 VARCHAR(15)
        , rel3 VARCHAR(15)
        , rel4 VARCHAR(15)
        , rel5 VARCHAR(15)
        , brokerage_type_etrade_b VARCHAR(3)
        , brokerage_type_odd_lot_b VARCHAR(3)
        , brokerage_type_b VARCHAR(3)
        , brokerage_type_foreign_b VARCHAR(3)
        , brokerage_type_intraday_b VARCHAR(3)
        , brokerage_type_intraday_etrade_b VARCHAR(3)
        , brokerage_type_etrade_s VARCHAR(3)
        , brokerage_type_odd_lot_s VARCHAR(3)
        , brokerage_type_s VARCHAR(3)
        , brokerage_type_foreign_s VARCHAR(3)
        , brokerage_type_intraday_s VARCHAR(3)
        , brokerage_type_intraday_etrade_s VARCHAR(3)
        , no_free_trade DECIMAL(2, 0)
        , sms VARCHAR(1)
        , mobile_prefix VARCHAR(5)
        , foreign_curr_set VARCHAR(1)
        , num_free_trade DECIMAL(2, 0)
        , etrader_type VARCHAR(2)
        , check_limit VARCHAR(1)
        , auto_margin VARCHAR(1)
        , margin_client_no VARCHAR(9)
        , dup_despatch_mode VARCHAR(1)
        , risk VARCHAR(1)
        , exclude_trader_limit VARCHAR(1)
        , sett_mode_date_change TIMESTAMP
        , pick_up_fee_pct DECIMAL(5, 2)
        , e_payment VARCHAR(1)
        , mgn_new_int_rate2 DECIMAL(5, 2)
        , fund_cost_type VARCHAR(1)
        , check_share VARCHAR(1)
        , citibank_changes VARCHAR(1)
        , citibank_charges VARCHAR(1)
        , cq_market VARCHAR(1)
        , exclude_margin_pro_rate VARCHAR(1)
        , brokerage_type_cash_b VARCHAR(3)
        , brokerage_type_etrade_cash_b VARCHAR(3)
        , clt_consent VARCHAR(1)
        , consent_start_date TIMESTAMP
        , portfolio VARCHAR(1)
        , expiry_date TIMESTAMP
        , intraday_auto_contra_option VARCHAR(1)
        , dcf_limit DECIMAL(9, 0)
        , brokerage_type_etb VARCHAR(3)
        , mgn_force_sell_pct DECIMAL(5, 2)
        , mgn_tenure DECIMAL(3, 0)
        , mgn_expiry_date TIMESTAMP
        , loss_gl_acc_no VARCHAR(18)
        , portfolio_date TIMESTAMP
        , day_prior_temp_susp DECIMAL(4, 0)
        , day_prior_perm_susp DECIMAL(4, 0)
        , gst_code VARCHAR(3)
        , match_price_decimal_local DECIMAL(1, 0)
        , match_price_decimal_foreign DECIMAL(1, 0)
        , primary_id_expiry_date TIMESTAMP
        , secondary_id_expiry_date TIMESTAMP
        , mgn_int_tdr_spread_pct DECIMAL(5, 2)
        , mgn_base_int_rate DECIMAL(5, 2)
        , mgn_int_tdr_share DECIMAL(5, 2)
        , islamic_flag VARCHAR(1)
        , mcd_resident_flag VARCHAR(1)
        , chq_charges_flag VARCHAR(1)
        , chq_charges_tdr_pct DECIMAL(9, 2)
        , brokerage_type_foreign_etrade VARCHAR(3)
        , brokerage_type_foreign_etrade_b VARCHAR(3)
        , brokerage_type_foreign_etrade_s VARCHAR(3)
        , grp_exch_code VARCHAR(5)
        , bdebt_ras VARCHAR(1)
        , twse_declaration VARCHAR(1)
        , joint_acc_amt DECIMAL(12, 2)
        , high_risk_market VARCHAR(2)
        , brokerage_type_leap_normal VARCHAR(3)
        , brokerage_type_leap_etrade VARCHAR(3)
        , perm_country VARCHAR(3)
        , type_of_account STRING
        , einvoice_email STRING
        , dl_record_status       VARCHAR(10)
        , dl_record_created_date TIMESTAMP
        , dl_record_updated_date TIMESTAMP
        , <<partition_column>> STRING
    )
    stored as parquet
    tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records from IMPACTED PARTITIONS ONLY ───────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated
    SELECT
        com_t.client_no,
        com_t.clean_rule_flag,
        com_t.primary_identification_type,
        com_t.primary_identification_no,
        com_t.secondary_identification_type,
        com_t.secondary_identification_no,
        com_t.customer_name,
        com_t.customer_name_concatenate,
        com_t.client_name,
        com_t.client_name1,
        com_t.client_name2,
        com_t.client_name3,
        com_t.mobile_no,
        com_t.fax_no,
        com_t.tel_no_home,
        com_t.tel_no_office,
        com_t.date_of_birth,
        com_t.race,
        com_t.email_1,
        com_t.email_2,
        com_t.email_3,
        com_t.email_4,
        com_t.email_5,
        com_t.email_6,
        com_t.email_7,
        com_t.email_8,
        com_t.email_9,
        com_t.email_10,
        com_t.sex,
        com_t.addr1,
        com_t.addr2,
        com_t.addr3,
        com_t.addr4,
        com_t.postcode,
        com_t.city,
        com_t.state,
        com_t.perm_addr1,
        com_t.perm_addr2,
        com_t.perm_addr3,
        com_t.perm_addr4,
        com_t.perm_postcode,
        com_t.perm_city,
        com_t.perm_state,
        com_t.noms_ind,
        com_t.cleaned_nominees_name,
        com_t.principal_name,
        com_t.intermediary_name,
        com_t.beneficiary_name,
        com_t.nominees_type,
        com_t.pledged_securities_flag,
        com_t.id_type,
        com_t.ic_no_new,
        com_t.ic_no_old,
        com_t.secondary_id_type,
        com_t.secondary_id_no,
        com_t.source_client_name,
        com_t.source_client_name1,
        com_t.source_client_name2,
        com_t.source_client_name3,
        com_t.source_mobile_no,
        com_t.source_fax_no,
        com_t.source_tel_no_home,
        com_t.source_tel_no_office,
        com_t.source_date_of_birth,
        com_t.source_race,
        com_t.source_email,
        com_t.source_sex,
        com_t.client_group,
        com_t.cds_acc_no,
        com_t.tdr_code,
        com_t.client_type,
        com_t.margin,
        com_t.last_margin_date,
        com_t.int_rate,
        com_t.auto_ded,
        com_t.despatch_mode,
        com_t.copies,
        com_t.prohibit_trade,
        com_t.custody_status,
        com_t.country,
        com_t.margin_limit,
        com_t.margin_pct,
        com_t.rollover_rate,
        com_t.form_completed,
        com_t.last_tran_date,
        com_t.ytd_bvalue,
        com_t.ytd_svalue,
        com_t.ytd_brokerage,
        com_t.os_led_bal,
        com_t.title,
        com_t.date_created,
        com_t.stop_payt,
        com_t.acc_payee,
        com_t.category,
        com_t.auto_contra,
        com_t.pnl_acc_no,
        com_t.cr_limit,
        com_t.trust_bal,
        com_t.avg_ind,
        com_t.remarks,
        com_t.contact_person,
        com_t.date_closed,
        com_t.grace_period,
        com_t.acct_type,
        com_t.assoc_ind,
        com_t.short_sell_ind,
        com_t.short_name,
        com_t.mesdaq_pctlmt,
        com_t.date_change,
        com_t.resi_code,
        com_t.charge_int,
        com_t.bdebt,
        com_t.assets,
        com_t.liabilities,
        com_t.income,
        com_t.expenses,
        com_t.bdebt_his_ind,
        com_t.rel_ac1,
        com_t.rel_ac2,
        com_t.rel_ac3,
        com_t.rel_ac4,
        com_t.occupation,
        com_t.margin_int,
        com_t.lst_led_no,
        com_t.cur_led_no,
        com_t.remarks2,
        com_t.acc_type,
        com_t.mas_accno,
        com_t.legal,
        com_t.sell_limit,
        com_t.brk_rate,
        com_t.brokerage_type,
        com_t.cds_acc_no1,
        com_t.remarks1,
        com_t.payment_bank_code,
        com_t.noms,
        com_t.dms_date,
        com_t.violation_date,
        com_t.mcd_branch,
        com_t.home_branch,
        com_t.eaf_code,
        com_t.call_warrant,
        com_t.user_id,
        com_t.credit_int_rate,
        com_t.min_eligible_amt,
        com_t.intraday_flag,
        com_t.intraday_rate,
        com_t.cta_weight,
        com_t.sta_weight,
        com_t.bo_cds_acc_no,
        com_t.ecos_form,
        com_t.custodian_no,
        com_t.prin_acc,
        com_t.armada_type,
        com_t.old_authorisee,
        com_t.etrade_rate,
        com_t.etf,
        com_t.cstamp_client_exempt,
        com_t.main_branch,
        com_t.prev_client_no,
        com_t.web_eds,
        com_t.place,
        com_t.excl_tdr_deduct,
        com_t.excl_auto_susp,
        com_t.trust_flag,
        com_t.mgn_new_int_rate,
        com_t.counter_concentration,
        com_t.auto_trust,
        com_t.margin_pct2,
        com_t.df_flag,
        com_t.mgn_curr_int_rate,
        com_t.product_type,
        com_t.web_ecos,
        com_t.xeye_clt_grp,
        com_t.bursa_violation_date,
        com_t.brokerage_type_etrade,
        com_t.brokerage_type_odd_lot,
        com_t.omnibus,
        com_t.cg_tdr_code,
        com_t.limit_foreign,
        com_t.limit_bursa,
        com_t.brokerage_type_intraday,
        com_t.brokerage_type_intraday_etrade,
        com_t.bursa_violation_date1,
        com_t.cif_no,
        com_t.brokerage_type_foreign,
        com_t.soft_copy,
        com_t.exclude_rollover,
        com_t.account_status,
        com_t.w8ben,
        com_t.ic_no_rel1,
        com_t.ic_no_rel2,
        com_t.ic_no_rel3,
        com_t.ic_no_rel4,
        com_t.ic_no_rel5,
        com_t.rel1,
        com_t.rel2,
        com_t.rel3,
        com_t.rel4,
        com_t.rel5,
        com_t.brokerage_type_etrade_b,
        com_t.brokerage_type_odd_lot_b,
        com_t.brokerage_type_b,
        com_t.brokerage_type_foreign_b,
        com_t.brokerage_type_intraday_b,
        com_t.brokerage_type_intraday_etrade_b,
        com_t.brokerage_type_etrade_s,
        com_t.brokerage_type_odd_lot_s,
        com_t.brokerage_type_s,
        com_t.brokerage_type_foreign_s,
        com_t.brokerage_type_intraday_s,
        com_t.brokerage_type_intraday_etrade_s,
        com_t.no_free_trade,
        com_t.sms,
        com_t.mobile_prefix,
        com_t.foreign_curr_set,
        com_t.num_free_trade,
        com_t.etrader_type,
        com_t.check_limit,
        com_t.auto_margin,
        com_t.margin_client_no,
        com_t.dup_despatch_mode,
        com_t.risk,
        com_t.exclude_trader_limit,
        com_t.sett_mode_date_change,
        com_t.pick_up_fee_pct,
        com_t.e_payment,
        com_t.mgn_new_int_rate2,
        com_t.fund_cost_type,
        com_t.check_share,
        com_t.citibank_changes,
        com_t.citibank_charges,
        com_t.cq_market,
        com_t.exclude_margin_pro_rate,
        com_t.brokerage_type_cash_b,
        com_t.brokerage_type_etrade_cash_b,
        com_t.clt_consent,
        com_t.consent_start_date,
        com_t.portfolio,
        com_t.expiry_date,
        com_t.intraday_auto_contra_option,
        com_t.dcf_limit,
        com_t.brokerage_type_etb,
        com_t.mgn_force_sell_pct,
        com_t.mgn_tenure,
        com_t.mgn_expiry_date,
        com_t.loss_gl_acc_no,
        com_t.portfolio_date,
        com_t.day_prior_temp_susp,
        com_t.day_prior_perm_susp,
        com_t.gst_code,
        com_t.match_price_decimal_local,
        com_t.match_price_decimal_foreign,
        com_t.primary_id_expiry_date,
        com_t.secondary_id_expiry_date,
        com_t.mgn_int_tdr_spread_pct,
        com_t.mgn_base_int_rate,
        com_t.mgn_int_tdr_share,
        com_t.islamic_flag,
        com_t.mcd_resident_flag,
        com_t.chq_charges_flag,
        com_t.chq_charges_tdr_pct,
        com_t.brokerage_type_foreign_etrade,
        com_t.brokerage_type_foreign_etrade_b,
        com_t.brokerage_type_foreign_etrade_s,
        com_t.grp_exch_code,
        com_t.bdebt_ras,
        com_t.twse_declaration,
        com_t.joint_acc_amt,
        com_t.high_risk_market,
        com_t.brokerage_type_leap_normal,
        com_t.brokerage_type_leap_etrade,
        com_t.perm_country,
        com_t.type_of_account,
        com_t.einvoice_email,
        com_t.dl_record_status,
        com_t.dl_record_created_date,
        com_t.dl_record_updated_date
        , com_t.<<partition_column>>
    FROM {params["com_schema"]}.t_mhbos_m_client com_t
    INNER JOIN (
        SELECT DISTINCT <<partition_column>>        FROM {params["com_schema"]}.temp_t_mhbos_m_client_consolidated
    ) impacted_partitions
    ON com_t.<<partition_column>> = impacted_partitions.<<partition_column>>    WHERE
        com_t.dl_record_status = 'A'
        AND NOT EXISTS (
            SELECT 1 FROM {params["com_schema"]}.temp_t_mhbos_m_client_consolidated r
            WHERE r.client_no = com_t.client_no        )
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
    INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated
    SELECT
        r.client_no,
        r.clean_rule_flag,
        r.primary_identification_type,
        r.primary_identification_no,
        r.secondary_identification_type,
        r.secondary_identification_no,
        r.customer_name,
        r.customer_name_concatenate,
        r.client_name,
        r.client_name1,
        r.client_name2,
        r.client_name3,
        r.mobile_no,
        r.fax_no,
        r.tel_no_home,
        r.tel_no_office,
        r.date_of_birth,
        r.race,
        r.email_1,
        r.email_2,
        r.email_3,
        r.email_4,
        r.email_5,
        r.email_6,
        r.email_7,
        r.email_8,
        r.email_9,
        r.email_10,
        r.sex,
        r.addr1,
        r.addr2,
        r.addr3,
        r.addr4,
        r.postcode,
        r.city,
        r.state,
        r.perm_addr1,
        r.perm_addr2,
        r.perm_addr3,
        r.perm_addr4,
        r.perm_postcode,
        r.perm_city,
        r.perm_state,
        r.noms_ind,
        r.cleaned_nominees_name,
        r.principal_name,
        r.intermediary_name,
        r.beneficiary_name,
        r.nominees_type,
        r.pledged_securities_flag,
        r.id_type,
        r.ic_no_new,
        r.ic_no_old,
        r.secondary_id_type,
        r.secondary_id_no,
        r.source_client_name,
        r.source_client_name1,
        r.source_client_name2,
        r.source_client_name3,
        r.source_mobile_no,
        r.source_fax_no,
        r.source_tel_no_home,
        r.source_tel_no_office,
        r.source_date_of_birth,
        r.source_race,
        r.source_email,
        r.source_sex,
        r.client_group,
        r.cds_acc_no,
        r.tdr_code,
        r.client_type,
        r.margin,
        r.last_margin_date,
        r.int_rate,
        r.auto_ded,
        r.despatch_mode,
        r.copies,
        r.prohibit_trade,
        r.custody_status,
        r.country,
        r.margin_limit,
        r.margin_pct,
        r.rollover_rate,
        r.form_completed,
        r.last_tran_date,
        r.ytd_bvalue,
        r.ytd_svalue,
        r.ytd_brokerage,
        r.os_led_bal,
        r.title,
        r.date_created,
        r.stop_payt,
        r.acc_payee,
        r.category,
        r.auto_contra,
        r.pnl_acc_no,
        r.cr_limit,
        r.trust_bal,
        r.avg_ind,
        r.remarks,
        r.contact_person,
        r.date_closed,
        r.grace_period,
        r.acct_type,
        r.assoc_ind,
        r.short_sell_ind,
        r.short_name,
        r.mesdaq_pctlmt,
        r.date_change,
        r.resi_code,
        r.charge_int,
        r.bdebt,
        r.assets,
        r.liabilities,
        r.income,
        r.expenses,
        r.bdebt_his_ind,
        r.rel_ac1,
        r.rel_ac2,
        r.rel_ac3,
        r.rel_ac4,
        r.occupation,
        r.margin_int,
        r.lst_led_no,
        r.cur_led_no,
        r.remarks2,
        r.acc_type,
        r.mas_accno,
        r.legal,
        r.sell_limit,
        r.brk_rate,
        r.brokerage_type,
        r.cds_acc_no1,
        r.remarks1,
        r.payment_bank_code,
        r.noms,
        r.dms_date,
        r.violation_date,
        r.mcd_branch,
        r.home_branch,
        r.eaf_code,
        r.call_warrant,
        r.user_id,
        r.credit_int_rate,
        r.min_eligible_amt,
        r.intraday_flag,
        r.intraday_rate,
        r.cta_weight,
        r.sta_weight,
        r.bo_cds_acc_no,
        r.ecos_form,
        r.custodian_no,
        r.prin_acc,
        r.armada_type,
        r.old_authorisee,
        r.etrade_rate,
        r.etf,
        r.cstamp_client_exempt,
        r.main_branch,
        r.prev_client_no,
        r.web_eds,
        r.place,
        r.excl_tdr_deduct,
        r.excl_auto_susp,
        r.trust_flag,
        r.mgn_new_int_rate,
        r.counter_concentration,
        r.auto_trust,
        r.margin_pct2,
        r.df_flag,
        r.mgn_curr_int_rate,
        r.product_type,
        r.web_ecos,
        r.xeye_clt_grp,
        r.bursa_violation_date,
        r.brokerage_type_etrade,
        r.brokerage_type_odd_lot,
        r.omnibus,
        r.cg_tdr_code,
        r.limit_foreign,
        r.limit_bursa,
        r.brokerage_type_intraday,
        r.brokerage_type_intraday_etrade,
        r.bursa_violation_date1,
        r.cif_no,
        r.brokerage_type_foreign,
        r.soft_copy,
        r.exclude_rollover,
        r.account_status,
        r.w8ben,
        r.ic_no_rel1,
        r.ic_no_rel2,
        r.ic_no_rel3,
        r.ic_no_rel4,
        r.ic_no_rel5,
        r.rel1,
        r.rel2,
        r.rel3,
        r.rel4,
        r.rel5,
        r.brokerage_type_etrade_b,
        r.brokerage_type_odd_lot_b,
        r.brokerage_type_b,
        r.brokerage_type_foreign_b,
        r.brokerage_type_intraday_b,
        r.brokerage_type_intraday_etrade_b,
        r.brokerage_type_etrade_s,
        r.brokerage_type_odd_lot_s,
        r.brokerage_type_s,
        r.brokerage_type_foreign_s,
        r.brokerage_type_intraday_s,
        r.brokerage_type_intraday_etrade_s,
        r.no_free_trade,
        r.sms,
        r.mobile_prefix,
        r.foreign_curr_set,
        r.num_free_trade,
        r.etrader_type,
        r.check_limit,
        r.auto_margin,
        r.margin_client_no,
        r.dup_despatch_mode,
        r.risk,
        r.exclude_trader_limit,
        r.sett_mode_date_change,
        r.pick_up_fee_pct,
        r.e_payment,
        r.mgn_new_int_rate2,
        r.fund_cost_type,
        r.check_share,
        r.citibank_changes,
        r.citibank_charges,
        r.cq_market,
        r.exclude_margin_pro_rate,
        r.brokerage_type_cash_b,
        r.brokerage_type_etrade_cash_b,
        r.clt_consent,
        r.consent_start_date,
        r.portfolio,
        r.expiry_date,
        r.intraday_auto_contra_option,
        r.dcf_limit,
        r.brokerage_type_etb,
        r.mgn_force_sell_pct,
        r.mgn_tenure,
        r.mgn_expiry_date,
        r.loss_gl_acc_no,
        r.portfolio_date,
        r.day_prior_temp_susp,
        r.day_prior_perm_susp,
        r.gst_code,
        r.match_price_decimal_local,
        r.match_price_decimal_foreign,
        r.primary_id_expiry_date,
        r.secondary_id_expiry_date,
        r.mgn_int_tdr_spread_pct,
        r.mgn_base_int_rate,
        r.mgn_int_tdr_share,
        r.islamic_flag,
        r.mcd_resident_flag,
        r.chq_charges_flag,
        r.chq_charges_tdr_pct,
        r.brokerage_type_foreign_etrade,
        r.brokerage_type_foreign_etrade_b,
        r.brokerage_type_foreign_etrade_s,
        r.grp_exch_code,
        r.bdebt_ras,
        r.twse_declaration,
        r.joint_acc_amt,
        r.high_risk_market,
        r.brokerage_type_leap_normal,
        r.brokerage_type_leap_etrade,
        r.perm_country,
        r.type_of_account,
        r.einvoice_email,
        'A' AS dl_record_status,
        CASE
            WHEN com_t.client_no IS NOT NULL THEN com_t.dl_record_created_date
            ELSE current_timestamp() END AS dl_record_created_date,
        current_timestamp() AS dl_record_updated_date
        , r.<<partition_column>>
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_consolidated r
    LEFT JOIN {params["com_schema"]}.t_mhbos_m_client com_t
        ON com_t.dl_record_status = 'A'
AND r.client_no = com_t.client_no""")

# ─── STEP 3: Overwrite target table partitions dynamically ──────────────────
spark.sql(f"""
    INSERT OVERWRITE TABLE {params["com_schema"]}.t_mhbos_m_client PARTITION (<<partition_column>>)
    SELECT
        client_no,
        clean_rule_flag,
        primary_identification_type,
        primary_identification_no,
        secondary_identification_type,
        secondary_identification_no,
        customer_name,
        customer_name_concatenate,
        client_name,
        client_name1,
        client_name2,
        client_name3,
        mobile_no,
        fax_no,
        tel_no_home,
        tel_no_office,
        date_of_birth,
        race,
        email_1,
        email_2,
        email_3,
        email_4,
        email_5,
        email_6,
        email_7,
        email_8,
        email_9,
        email_10,
        sex,
        addr1,
        addr2,
        addr3,
        addr4,
        postcode,
        city,
        state,
        perm_addr1,
        perm_addr2,
        perm_addr3,
        perm_addr4,
        perm_postcode,
        perm_city,
        perm_state,
        noms_ind,
        cleaned_nominees_name,
        principal_name,
        intermediary_name,
        beneficiary_name,
        nominees_type,
        pledged_securities_flag,
        id_type,
        ic_no_new,
        ic_no_old,
        secondary_id_type,
        secondary_id_no,
        source_client_name,
        source_client_name1,
        source_client_name2,
        source_client_name3,
        source_mobile_no,
        source_fax_no,
        source_tel_no_home,
        source_tel_no_office,
        source_date_of_birth,
        source_race,
        source_email,
        source_sex,
        client_group,
        cds_acc_no,
        tdr_code,
        client_type,
        margin,
        last_margin_date,
        int_rate,
        auto_ded,
        despatch_mode,
        copies,
        prohibit_trade,
        custody_status,
        country,
        margin_limit,
        margin_pct,
        rollover_rate,
        form_completed,
        last_tran_date,
        ytd_bvalue,
        ytd_svalue,
        ytd_brokerage,
        os_led_bal,
        title,
        date_created,
        stop_payt,
        acc_payee,
        category,
        auto_contra,
        pnl_acc_no,
        cr_limit,
        trust_bal,
        avg_ind,
        remarks,
        contact_person,
        date_closed,
        grace_period,
        acct_type,
        assoc_ind,
        short_sell_ind,
        short_name,
        mesdaq_pctlmt,
        date_change,
        resi_code,
        charge_int,
        bdebt,
        assets,
        liabilities,
        income,
        expenses,
        bdebt_his_ind,
        rel_ac1,
        rel_ac2,
        rel_ac3,
        rel_ac4,
        occupation,
        margin_int,
        lst_led_no,
        cur_led_no,
        remarks2,
        acc_type,
        mas_accno,
        legal,
        sell_limit,
        brk_rate,
        brokerage_type,
        cds_acc_no1,
        remarks1,
        payment_bank_code,
        noms,
        dms_date,
        violation_date,
        mcd_branch,
        home_branch,
        eaf_code,
        call_warrant,
        user_id,
        credit_int_rate,
        min_eligible_amt,
        intraday_flag,
        intraday_rate,
        cta_weight,
        sta_weight,
        bo_cds_acc_no,
        ecos_form,
        custodian_no,
        prin_acc,
        armada_type,
        old_authorisee,
        etrade_rate,
        etf,
        cstamp_client_exempt,
        main_branch,
        prev_client_no,
        web_eds,
        place,
        excl_tdr_deduct,
        excl_auto_susp,
        trust_flag,
        mgn_new_int_rate,
        counter_concentration,
        auto_trust,
        margin_pct2,
        df_flag,
        mgn_curr_int_rate,
        product_type,
        web_ecos,
        xeye_clt_grp,
        bursa_violation_date,
        brokerage_type_etrade,
        brokerage_type_odd_lot,
        omnibus,
        cg_tdr_code,
        limit_foreign,
        limit_bursa,
        brokerage_type_intraday,
        brokerage_type_intraday_etrade,
        bursa_violation_date1,
        cif_no,
        brokerage_type_foreign,
        soft_copy,
        exclude_rollover,
        account_status,
        w8ben,
        ic_no_rel1,
        ic_no_rel2,
        ic_no_rel3,
        ic_no_rel4,
        ic_no_rel5,
        rel1,
        rel2,
        rel3,
        rel4,
        rel5,
        brokerage_type_etrade_b,
        brokerage_type_odd_lot_b,
        brokerage_type_b,
        brokerage_type_foreign_b,
        brokerage_type_intraday_b,
        brokerage_type_intraday_etrade_b,
        brokerage_type_etrade_s,
        brokerage_type_odd_lot_s,
        brokerage_type_s,
        brokerage_type_foreign_s,
        brokerage_type_intraday_s,
        brokerage_type_intraday_etrade_s,
        no_free_trade,
        sms,
        mobile_prefix,
        foreign_curr_set,
        num_free_trade,
        etrader_type,
        check_limit,
        auto_margin,
        margin_client_no,
        dup_despatch_mode,
        risk,
        exclude_trader_limit,
        sett_mode_date_change,
        pick_up_fee_pct,
        e_payment,
        mgn_new_int_rate2,
        fund_cost_type,
        check_share,
        citibank_changes,
        citibank_charges,
        cq_market,
        exclude_margin_pro_rate,
        brokerage_type_cash_b,
        brokerage_type_etrade_cash_b,
        clt_consent,
        consent_start_date,
        portfolio,
        expiry_date,
        intraday_auto_contra_option,
        dcf_limit,
        brokerage_type_etb,
        mgn_force_sell_pct,
        mgn_tenure,
        mgn_expiry_date,
        loss_gl_acc_no,
        portfolio_date,
        day_prior_temp_susp,
        day_prior_perm_susp,
        gst_code,
        match_price_decimal_local,
        match_price_decimal_foreign,
        primary_id_expiry_date,
        secondary_id_expiry_date,
        mgn_int_tdr_spread_pct,
        mgn_base_int_rate,
        mgn_int_tdr_share,
        islamic_flag,
        mcd_resident_flag,
        chq_charges_flag,
        chq_charges_tdr_pct,
        brokerage_type_foreign_etrade,
        brokerage_type_foreign_etrade_b,
        brokerage_type_foreign_etrade_s,
        grp_exch_code,
        bdebt_ras,
        twse_declaration,
        joint_acc_amt,
        high_risk_market,
        brokerage_type_leap_normal,
        brokerage_type_leap_etrade,
        perm_country,
        type_of_account,
        einvoice_email,
        dl_record_status,
        dl_record_created_date,
        dl_record_updated_date,
        '{batch_date}' AS etl_dt,
        current_timestamp() AS etl_timestamp
        , <<partition_column>>
    FROM {params["com_schema"]}.temp_t_mhbos_m_client_updated
""")

spark.sql(f"""
    ANALYZE TABLE {params["com_schema"]}.t_mhbos_m_client COMPUTE STATISTICS
""")

spark.stop()