--Purpose:    Customer information cleaning rules implementation program
--Author:     Sunline
--Usage:      python $ETL_HOME/script/main.py 20230809 com_t_mhbos_m_client
--CreateDate: 20230810
--Logs:       zhairp 20230810 create script.
--Logs:       leext  20240321 modify and add rule.
--Logs:       leext  20240518 modify and add rule
--Logs:       marco  20241111 modify noms_ind and nominees_type logic
--Logs:       marco  20241111 modify customer_name logic to store principal name regardless of nominee criteria
--Logs:       marco  20241118 modify remove bracketed text at the end of the customer_name logic (from '\\(.*\\)$' to '\\s*\\([^)]*\\)$')
--Logs:       marco  20241120 modify customer_name logic to ignore splitting of "FOR" for client_type in ('#', '0', '1', '6', '8', 'V')
--Logs:       marco  20250313 added primary_identification_type conversion logic and new lookup file primaryidno_newcustname
--Logs:       marco  20250502 update contact number cleansing logic (temp_t_mhbos_m_client_telephone_clean, temp_t_mhbos_m_client_telephone_number_info)
--Logs:       marco  20250502 update BRN number length checking (temp_t_mhbos_m_client_primary_identification_no, temp_t_mhbos_m_client_secondary_identification_no)
--Logs:       marco  20250620 update customer_name, secondary_identification_no, secondary_identification_no conversion based on lookup_custname_primaryidno
--Logs:       marco  20250715 added new fields from m_client_ext: perm_city, perm_state_code, perm_country and derive address logic
--Logs:       syhmi  20250903 added new fields: einvoice_email
--Logs:       marco  20250925 optimize whole sript, only do data cleansing for incremental data from com_r_mhbos_m_client and com_r_mhbos_m_client_ext table
--Logs:       marco  20251013 add m_client_ext.state_code to determine mailing state, remove the full address scanning logic to define mailing state and registered state

-- 1.0 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/com_t_mhbos_m_client_para.sql;

-- 1.2 Drop all temporary tables at the start of the program
drop table if exists ${com_schema}.temp_t_mhbos_m_client_all;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_type;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_no;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_dob_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_gender_info; 
drop table if exists ${com_schema}.temp_t_mhbos_m_client_customer_name_1;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_customer_name_2;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_race_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_telephone_number_clean;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_telephone_number_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_email_clean;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_email_info_1;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_email_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_address_city;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_address_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_1;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_3;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_5;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client_name_info;
drop table if exists ${com_schema}.temp_t_mhbos_m_client;

-- 2.1 Create a temporary table temp_com_mhbos_m_client_all to store the current vaild data
create table if not exists ${com_schema}.temp_t_mhbos_m_client_all(
  `client_no` string, 
  `client_group` string, 
  `cds_acc_no` string, 
  `tdr_code` string, 
  `client_name` string, 
  `client_type` string, 
  `margin` string, 
  `last_margin_date` timestamp, 
  `int_rate` decimal(5,2), 
  `auto_ded` string, 
  `despatch_mode` string, 
  `copies` decimal(2,0), 
  `prohibit_trade` string, 
  `custody_status` string, 
  `race` string, 
  `country` string, 
  `ic_no_new` string, 
  `ic_no_old` string, 
  `margin_limit` decimal(9,0), 
  `margin_pct` decimal(5,2), 
  `rollover_rate` decimal(6,3), 
  `form_completed` string, 
  `last_tran_date` timestamp, 
  `ytd_bvalue` decimal(12,2), 
  `ytd_svalue` decimal(12,2), 
  `ytd_brokerage` decimal(11,2), 
  `os_led_bal` decimal(12,2), 
  `title` string, 
  `addr1` string, 
  `addr2` string, 
  `addr3` string, 
  `tel_no_home` string, 
  `tel_no_office` string, 
  `date_created` timestamp, 
  `stop_payt` string, 
  `acc_payee` string, 
  `category` string, 
  `fax_no` string, 
  `auto_contra` string, 
  `pnl_acc_no` string, 
  `cr_limit` decimal(9,0), 
  `trust_bal` decimal(12,2), 
  `avg_ind` string, 
  `remarks` string, 
  `contact_person` string, 
  `date_closed` timestamp, 
  `grace_period` decimal(3,0), 
  `acct_type` decimal(2,0), 
  `assoc_ind` string, 
  `short_sell_ind` string, 
  `short_name` string, 
  `mesdaq_pctlmt` decimal(5,2), 
  `date_change` timestamp, 
  `resi_code` string, 
  `sex` string, 
  `charge_int` string, 
  `bdebt` string, 
  `assets` decimal(12,2), 
  `liabilities` decimal(12,2), 
  `income` decimal(12,2), 
  `expenses` decimal(12,2), 
  `bdebt_his_ind` string, 
  `rel_ac1` string, 
  `rel_ac2` string, 
  `rel_ac3` string, 
  `rel_ac4` string, 
  `occupation` string, 
  `margin_int` decimal(12,2), 
  `lst_led_no` decimal(9,0), 
  `cur_led_no` decimal(9,0), 
  `remarks2` string, 
  `acc_type` string, 
  `mas_accno` string, 
  `legal` string, 
  `sell_limit` decimal(9,0), 
  `brk_rate` decimal(9,4), 
  `client_name1` string, 
  `postcode` string, 
  `brokerage_type` string, 
  `cds_acc_no1` string, 
  `remarks1` string, 
  `payment_bank_code` string, 
  `noms` string, 
  `dms_date` timestamp, 
  `violation_date` timestamp, 
  `mcd_branch` string, 
  `home_branch` string, 
  `eaf_code` string, 
  `call_warrant` string, 
  `user_id` string, 
  `credit_int_rate` decimal(5,2), 
  `min_eligible_amt` decimal(18,4), 
  `intraday_flag` string, 
  `intraday_rate` decimal(5,4), 
  `cta_weight` decimal(3,0), 
  `sta_weight` decimal(3,0), 
  `bo_cds_acc_no` string, 
  `ecos_form` string, 
  `custodian_no` string, 
  `prin_acc` string, 
  `armada_type` string, 
  `old_authorisee` string, 
  `etrade_rate` decimal(9,4), 
  `etf` string, 
  `cstamp_client_exempt` string, 
  `main_branch` string, 
  `prev_client_no` string, 
  `web_eds` string, 
  `place` string, 
  `addr4` string, 
  `excl_tdr_deduct` string, 
  `excl_auto_susp` string, 
  `trust_flag` string, 
  `mgn_new_int_rate` decimal(5,2), 
  `counter_concentration` decimal(5,2), 
  `auto_trust` string, 
  `margin_pct2` decimal(5,2), 
  `df_flag` string, 
  `mgn_curr_int_rate` decimal(5,2), 
  `product_type` string, 
  `web_ecos` string, 
  `xeye_clt_grp` string, 
  `bursa_violation_date` timestamp, 
  `client_name2` string, 
  `brokerage_type_etrade` string, 
  `brokerage_type_odd_lot` string, 
  `omnibus` string, 
  `cg_tdr_code` string, 
  `limit_foreign` decimal(9,4), 
  `limit_bursa` decimal(9,4), 
  `brokerage_type_intraday` string, 
  `brokerage_type_intraday_etrade` string, 
  `bursa_violation_date1` timestamp, 
  `cif_no` string, 
  `brokerage_type_foreign` string, 
  `soft_copy` string, 
  `exclude_rollover` string, 
  `account_status` string, 
  `w8ben` string, 
  `ic_no_rel1` string, 
  `ic_no_rel2` string, 
  `ic_no_rel3` string, 
  `ic_no_rel4` string, 
  `ic_no_rel5` string, 
  `rel1` string, 
  `rel2` string, 
  `rel3` string, 
  `rel4` string, 
  `rel5` string, 
  `brokerage_type_etrade_b` string, 
  `brokerage_type_odd_lot_b` string, 
  `brokerage_type_b` string, 
  `brokerage_type_foreign_b` string, 
  `brokerage_type_intraday_b` string, 
  `brokerage_type_intraday_etrade_b` string, 
  `brokerage_type_etrade_s` string, 
  `brokerage_type_odd_lot_s` string, 
  `brokerage_type_s` string, 
  `brokerage_type_foreign_s` string, 
  `brokerage_type_intraday_s` string, 
  `brokerage_type_intraday_etrade_s` string, 
  `no_free_trade` decimal(2,0), 
  `sms` string, 
  `mobile_prefix` string, 
  `mobile_no` string, 
  `foreign_curr_set` string, 
  `num_free_trade` decimal(2,0), 
  `etrader_type` string, 
  `check_limit` string, 
  `auto_margin` string, 
  `margin_client_no` string, 
  `dup_despatch_mode` string, 
  `risk` string, 
  `exclude_trader_limit` string, 
  `sett_mode_date_change` timestamp, 
  `pick_up_fee_pct` decimal(5,2), 
  `e_payment` string, 
  `mgn_new_int_rate2` decimal(5,2), 
  `fund_cost_type` string, 
  `check_share` string, 
  `citibank_changes` string, 
  `citibank_charges` string, 
  `cq_market` string, 
  `exclude_margin_pro_rate` string, 
  `brokerage_type_cash_b` string, 
  `brokerage_type_etrade_cash_b` string, 
  `clt_consent` string, 
  `consent_start_date` timestamp, 
  `portfolio` string, 
  `expiry_date` timestamp, 
  `intraday_auto_contra_option` string, 
  `dcf_limit` decimal(9,0), 
  `brokerage_type_etb` string, 
  `mgn_force_sell_pct` decimal(5,2), 
  `mgn_tenure` decimal(3,0), 
  `mgn_expiry_date` timestamp, 
  `loss_gl_acc_no` string, 
  `portfolio_date` timestamp, 
  `day_prior_temp_susp` decimal(4,0), 
  `day_prior_perm_susp` decimal(4,0), 
  `gst_code` string, 
  `match_price_decimal_local` decimal(1,0), 
  `match_price_decimal_foreign` decimal(1,0), 
  `id_type` string, 
  `primary_id_expiry_date` timestamp, 
  `secondary_id_no` string, 
  `secondary_id_expiry_date` timestamp, 
  `mgn_int_tdr_spread_pct` decimal(5,2), 
  `mgn_base_int_rate` decimal(5,2), 
  `mgn_int_tdr_share` decimal(5,2), 
  `islamic_flag` string, 
  `mcd_resident_flag` string, 
  `chq_charges_flag` string, 
  `chq_charges_tdr_pct` decimal(9,2), 
  `brokerage_type_foreign_etrade` string, 
  `brokerage_type_foreign_etrade_b` string, 
  `brokerage_type_foreign_etrade_s` string, 
  `grp_exch_code` string, 
  `secondary_id_type` string, 
  `bdebt_ras` string, 
  `twse_declaration` string, 
  `client_name3` string, 
  `joint_acc_amt` decimal(12,2), 
  `high_risk_market` string, 
  `brokerage_type_leap_normal` string, 
  `brokerage_type_leap_etrade` string, 
  `etl_timestamp` string, 
  `etl_dt` string, 
  `part_id` string, 
  `date_of_birth` timestamp, 
  `email` string, 
  `perm_addr1` string, 
  `perm_addr2` string, 
  `perm_addr3` string, 
  `perm_addr4` string, 
  `perm_postcode` string,
  `perm_city` string, -- 20250715
  `perm_state_code` string, -- 20250715
  `perm_country` string, -- 20250715
  `type_of_account` string, --20250806
  `einvoice_email` string, --20250903
  `state_code` string --20251013
);

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_all;


WITH filtered_clients AS (
    SELECT DISTINCT client_no
    FROM ${com_schema}.r_mhbos_m_client
    WHERE part_id = '${batch_date}'
    UNION
    SELECT DISTINCT client_no
    FROM ${com_schema}.r_mhbos_m_client_ext
    WHERE part_id = '${batch_date}'
),
latest_clients AS (
    SELECT mmc.*
        ,ROW_NUMBER() OVER (PARTITION BY mmc.client_no ORDER BY mmc.part_id DESC) AS rn
    FROM ${com_schema}.r_mhbos_m_client mmc
    INNER JOIN filtered_clients fc
        ON mmc.client_no = fc.client_no
    WHERE mmc.part_id <= '${batch_date}'
)
insert into table ${com_schema}.temp_t_mhbos_m_client_all
SELECT mmc.client_no
       ,mmc.client_group
       ,mmc.cds_acc_no
       ,mmc.tdr_code
       ,mmc.client_name
       ,mmc.client_type
       ,mmc.margin
       ,mmc.last_margin_date
       ,mmc.int_rate
       ,mmc.auto_ded
       ,mmc.despatch_mode
       ,mmc.copies
       ,mmc.prohibit_trade
       ,mmc.custody_status
       ,mmc.race
       ,mmc.country
       ,REGEXP_REPLACE(mmc.ic_no_new , '-', '') AS ic_no_new
       ,REGEXP_REPLACE(mmc.ic_no_old , '-', '') AS ic_no_old
       ,mmc.margin_limit
       ,mmc.margin_pct
       ,mmc.rollover_rate
       ,mmc.form_completed
       ,mmc.last_tran_date
       ,mmc.ytd_bvalue
       ,mmc.ytd_svalue
       ,mmc.ytd_brokerage
       ,mmc.os_led_bal
       ,mmc.title
       ,mmc.addr1
       ,mmc.addr2
       ,mmc.addr3
       ,mmc.tel_no_home
       ,mmc.tel_no_office
       ,mmc.date_created
       ,mmc.stop_payt
       ,mmc.acc_payee
       ,mmc.category
       ,mmc.fax_no
       ,mmc.auto_contra
       ,mmc.pnl_acc_no
       ,mmc.cr_limit
       ,mmc.trust_bal
       ,mmc.avg_ind
       ,mmc.remarks
       ,mmc.contact_person
       ,mmc.date_closed
       ,mmc.grace_period
       ,mmc.acct_type
       ,mmc.assoc_ind
       ,mmc.short_sell_ind
       ,mmc.short_name
       ,mmc.mesdaq_pctlmt
       ,mmc.date_change
       ,mmc.resi_code
       ,mmc.sex
       ,mmc.charge_int
       ,mmc.bdebt
       ,mmc.assets
       ,mmc.liabilities
       ,mmc.income
       ,mmc.expenses
       ,mmc.bdebt_his_ind
       ,mmc.rel_ac1
       ,mmc.rel_ac2
       ,mmc.rel_ac3
       ,mmc.rel_ac4
       ,mmc.occupation
       ,mmc.margin_int
       ,mmc.lst_led_no
       ,mmc.cur_led_no
       ,mmc.remarks2
       ,mmc.acc_type
       ,mmc.mas_accno
       ,mmc.legal
       ,mmc.sell_limit
       ,mmc.brk_rate
       ,mmc.client_name1
       ,mmc.postcode
       ,mmc.brokerage_type
       ,mmc.cds_acc_no1
       ,mmc.remarks1
       ,mmc.payment_bank_code
       ,mmc.noms
       ,mmc.dms_date
       ,mmc.violation_date
       ,mmc.mcd_branch
       ,mmc.home_branch
       ,mmc.eaf_code
       ,mmc.call_warrant
       ,mmc.`user_id`
       ,mmc.credit_int_rate
       ,mmc.min_eligible_amt
       ,mmc.intraday_flag
       ,mmc.intraday_rate
       ,mmc.cta_weight
       ,mmc.sta_weight
       ,mmc.bo_cds_acc_no
       ,mmc.ecos_form
       ,mmc.custodian_no
       ,mmc.prin_acc
       ,mmc.armada_type
       ,mmc.old_authorisee
       ,mmc.etrade_rate
       ,mmc.etf
       ,mmc.cstamp_client_exempt
       ,mmc.main_branch
       ,mmc.prev_client_no
       ,mmc.web_eds
       ,mmc.place
       ,mmc.addr4
       ,mmc.excl_tdr_deduct
       ,mmc.excl_auto_susp
       ,mmc.trust_flag
       ,mmc.mgn_new_int_rate
       ,mmc.counter_concentration
       ,mmc.auto_trust
       ,mmc.margin_pct2
       ,mmc.df_flag
       ,mmc.mgn_curr_int_rate
       ,mmc.product_type
       ,mmc.web_ecos
       ,mmc.xeye_clt_grp
       ,mmc.bursa_violation_date
       ,mmc.client_name2
       ,mmc.brokerage_type_etrade
       ,mmc.brokerage_type_odd_lot
       ,mmc.omnibus
       ,mmc.cg_tdr_code
       ,mmc.limit_foreign
       ,mmc.limit_bursa
       ,mmc.brokerage_type_intraday
       ,mmc.brokerage_type_intraday_etrade
       ,mmc.bursa_violation_date1
       ,mmc.cif_no
       ,mmc.brokerage_type_foreign
       ,mmc.soft_copy
       ,mmc.exclude_rollover
       ,mmc.account_status
       ,mmc.w8ben
       ,mmc.ic_no_rel1
       ,mmc.ic_no_rel2
       ,mmc.ic_no_rel3
       ,mmc.ic_no_rel4
       ,mmc.ic_no_rel5
       ,mmc.rel1
       ,mmc.rel2
       ,mmc.rel3
       ,mmc.rel4
       ,mmc.rel5
       ,mmc.brokerage_type_etrade_b
       ,mmc.brokerage_type_odd_lot_b
       ,mmc.brokerage_type_b
       ,mmc.brokerage_type_foreign_b
       ,mmc.brokerage_type_intraday_b
       ,mmc.brokerage_type_intraday_etrade_b
       ,mmc.brokerage_type_etrade_s
       ,mmc.brokerage_type_odd_lot_s
       ,mmc.brokerage_type_s
       ,mmc.brokerage_type_foreign_s
       ,mmc.brokerage_type_intraday_s
       ,mmc.brokerage_type_intraday_etrade_s
       ,mmc.no_free_trade
       ,mmc.sms
       ,mmc.mobile_prefix
       ,mmc.mobile_no
       ,mmc.foreign_curr_set
       ,mmc.num_free_trade
       ,mmc.etrader_type
       ,mmc.check_limit
       ,mmc.auto_margin
       ,mmc.margin_client_no
       ,mmc.dup_despatch_mode
       ,mmc.risk
       ,mmc.exclude_trader_limit
       ,mmc.sett_mode_date_change
       ,mmc.pick_up_fee_pct
       ,mmc.e_payment
       ,mmc.mgn_new_int_rate2
       ,mmc.fund_cost_type
       ,mmc.check_share
       ,mmc.citibank_changes
       ,mmc.citibank_charges
       ,mmc.cq_market
       ,mmc.exclude_margin_pro_rate
       ,mmc.brokerage_type_cash_b
       ,mmc.brokerage_type_etrade_cash_b
       ,mmc.clt_consent
       ,mmc.consent_start_date
       ,mmc.portfolio
       ,mmc.expiry_date
       ,mmc.intraday_auto_contra_option
       ,mmc.dcf_limit
       ,mmc.brokerage_type_etb
       ,mmc.mgn_force_sell_pct
       ,mmc.mgn_tenure
       ,mmc.mgn_expiry_date
       ,mmc.loss_gl_acc_no
       ,mmc.portfolio_date
       ,mmc.day_prior_temp_susp
       ,mmc.day_prior_perm_susp
       ,mmc.gst_code
       ,mmc.match_price_decimal_local
       ,mmc.match_price_decimal_foreign
       ,mmc.id_type
       ,mmc.primary_id_expiry_date
       ,REGEXP_REPLACE(mmc.secondary_id_no , '-', '') AS secondary_id_no
       ,mmc.secondary_id_expiry_date
       ,mmc.mgn_int_tdr_spread_pct
       ,mmc.mgn_base_int_rate
       ,mmc.mgn_int_tdr_share
       ,mmc.islamic_flag
       ,mmc.mcd_resident_flag
       ,mmc.chq_charges_flag
       ,mmc.chq_charges_tdr_pct
       ,mmc.brokerage_type_foreign_etrade
       ,mmc.brokerage_type_foreign_etrade_b
       ,mmc.brokerage_type_foreign_etrade_s
       ,mmc.grp_exch_code
       ,mmc.secondary_id_type
       ,mmc.bdebt_ras
       ,mmc.twse_declaration
       ,mmc.client_name3
       ,mmc.joint_acc_amt
       ,mmc.high_risk_market
       ,mmc.brokerage_type_leap_normal
       ,mmc.brokerage_type_leap_etrade
       ,mmc.etl_timestamp
       ,mmc.etl_dt
	     ,mmc.part_id
       ,mmce.date_of_birth
	     ,mmce.email
	     ,mmce.perm_addr1
	     ,mmce.perm_addr2
	     ,mmce.perm_addr3
	     ,mmce.perm_addr4
	     ,mmce.perm_postcode
       ,mmce.perm_city -- 20250715
       ,mmce.perm_state_code -- 20250715
       ,mmce.perm_country -- 20250715
       ,mmc.type_of_account --20250806
       ,mmce.einvoice_email --20250903
       ,mmce.state_code --20251013
FROM latest_clients mmc
LEFT JOIN ${com_schema}.t_mhbos_m_client_ext mmce
  ON mmc.client_no = mmce.client_no
  AND mmce.part_id = '${batch_date}'
WHERE mmc.rn = 1
;

-- 2.2 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification type.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_type(
  `client_no` string, 
  `id_type` string, 
  `secondary_id_type` string, 
  `primary_identification_type` string, 
  `primary_identification_type_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_primary_identification_type;

insert into table ${com_schema}.temp_t_mhbos_m_client_primary_identification_type
select mmc.client_no,
       mmc.id_type,
       mmc.secondary_id_type,
       (case when mmc.id_type = '1' then '1'
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' then '1'
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '4'
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '3'
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '2'
             when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '6'
			 when mmc.id_type = '2' then '3'
             when mmc.id_type = '3' then '4'
             when mmc.id_type = '4' then '5'
             when mmc.id_type = '5' then '2'
             when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '4'
             when mmc.id_type = '6' then '6'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '1' then '1'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '2' then '3'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '3' then '4'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '4' then '5'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '5' then '2'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '6' then '6'
             else '@[' || mmc.id_type || ']'
         end) as primary_identification_type,
       (case when mmc.id_type = '1' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '0'
			 when mmc.id_type = '2' then '0'
             when mmc.id_type = '3' then '0'
             when mmc.id_type = '4' then '0'
             when mmc.id_type = '5' then '0'
             when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type = '6' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '1' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '2' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '3' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '4' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '5' then '0'
             when nvl(mmc.id_type, '') = '' and mmc.secondary_id_type = '6' then '0'
             else '1'
         end) as primary_identification_type_flag
  from ${com_schema}.temp_t_mhbos_m_client_all mmc
;

-- 2.3 Create a temporary table temp_mhbos_m_client_primary_identification_type to store the cleaned primary identification number.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_no(
  `client_no` string, 
  `id_type` string, 
  `ic_no_new` string, 
  `ic_no_old` string, 
  `secondary_id_type` string, 
  `secondary_id_no` string, 
  `primary_identification_no` string, 
  `primary_identification_no_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_primary_identification_no;

insert into table ${com_schema}.temp_t_mhbos_m_client_primary_identification_no
select mmc.client_no,
       mmc.id_type,
       mmc.ic_no_new,
       mmc.ic_no_old,
       mmc.secondary_id_type,
       mmc.secondary_id_no,
       (case when mmc.id_type = '1' and nvl(mmc.ic_no_new, '') <> '' then mmc.ic_no_new
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' then mmc.secondary_id_no
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then mmc.secondary_id_no
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then mmc.ic_no_new
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then mmc.secondary_id_no
             when mmc.id_type = '2' and mmc.secondary_id_type = '6' then mmc.secondary_id_no
			 when mmc.id_type = '2' and nvl(mmc.ic_no_new, '') <> '' then mmc.ic_no_new
             when mmc.id_type = '3' and nvl(mmc.ic_no_new, '') = nvl(mmc.secondary_id_no, '') then mmc.ic_no_new 
             when mmc.id_type = '3' and length(mmc.ic_no_new) < 12 and length(mmc.secondary_id_no) >= 12 and length(mmc.secondary_id_no) <= 15 then mmc.secondary_id_no -- updated 20250502
             when mmc.id_type = '3' and nvl(mmc.ic_no_new, '') <> '' and length(mmc.secondary_id_no) < 12 then mmc.ic_no_new -- updated 20250502
             when mmc.id_type = '3' and length(mmc.ic_no_new) >= 12 and length(mmc.ic_no_new) <= 15 then mmc.ic_no_new -- updated 20250502
             when mmc.id_type = '4' then mmc.ic_no_new
             when mmc.id_type = '5' then mmc.ic_no_new
             when mmc.id_type = '6' and mmc.secondary_id_type = '3' then mmc.secondary_id_no
             when mmc.id_type = '6' then mmc.ic_no_new
             when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then mmc.secondary_id_no
             else '@[' || mmc.ic_no_new || ']'
         end) as primary_identification_no,
       (case when mmc.id_type = '1' and nvl(mmc.ic_no_new, '') <> '' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '0'
             when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '0'
			 when mmc.id_type = '2' and nvl(mmc.ic_no_new, '') <> '' then '0'
             when mmc.id_type = '3' and nvl(mmc.ic_no_new, '') = nvl(mmc.secondary_id_no, '') then '0'
             when mmc.id_type = '3' and length(mmc.ic_no_new) < 12 and length(mmc.secondary_id_no) >= 12 and length(mmc.secondary_id_no) <= 15 then '0' -- updated 20250502
             when mmc.id_type = '3' and nvl(mmc.ic_no_new, '') <> '' and length(mmc.secondary_id_no) < 12 then '0' -- updated 20250502
             when mmc.id_type = '3' and length(mmc.ic_no_new) >= 12 and length(mmc.ic_no_new) <= 15 then '0' -- updated 20250502
             when mmc.id_type = '4' then '0'
             when mmc.id_type = '5' then '0'
             when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '0'
             when mmc.id_type = '6' then '0'
             when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then '0'
             else '1'
         end) as primary_identification_no_flag 
  from ${com_schema}.temp_t_mhbos_m_client_all mmc
;

-- 2.4 Create a temporary table temp_mhbos_m_client_secondary_identification_type to store the cleaned secondary identification type.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type(
  `client_no` string, 
  `id_type` string, 
  `ic_no_new` string, 
  `ic_no_old` string, 
  `secondary_id_type` string, 
  `secondary_id_no` string, 
  `secondary_identification_type` string, 
  `secondary_identification_type_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type;

insert into table ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type
select mmc.client_no,
       mmc.id_type,
       mmc.ic_no_new,
       mmc.ic_no_old,
       mmc.secondary_id_type,
       mmc.secondary_id_no,
       (case when mmc.id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '2'
             when mmc.id_type = '1' and mmc.secondary_id_type = '1' then ''
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '2' then '3'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '3' then '4'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '4' then '5'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '5' then '2'
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '6' then '6'
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then '2'
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then '3'
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then '5'
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then '3'
			 when mmc.id_type = '2' and mmc.secondary_id_type = '2' then '3'
			 when mmc.id_type = '2' and mmc.secondary_id_type = '6' then '3'
             when mmc.id_type = '3' and mmc.secondary_id_type = '3' and nvl(mmc.ic_no_new, '') = nvl(mmc.secondary_id_no, '') then ''
			 when mmc.id_type = '3' and mmc.secondary_id_type = '3' and nvl(mmc.ic_no_new, '') <> nvl(mmc.secondary_id_no, '') then '4'
			 when mmc.id_type = '3' and mmc.secondary_id_type = '1' then '1'
			 when mmc.id_type = '3' and mmc.secondary_id_type = '2' then '3'
			 when mmc.id_type = '3' and mmc.secondary_id_type = '4' then '5'
			 when mmc.id_type = '3' and mmc.secondary_id_type = '5' then '2'
			 when mmc.id_type = '3' and mmc.secondary_id_type = '6' then '6'
			 when mmc.id_type = '6' and mmc.secondary_id_type = '3' then '6'
			 when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then ''
			 when nvl(trim(mmc.secondary_id_type), '') = '' then ''
             else mmc.secondary_id_type
         end) as secondary_identification_type,
       /*
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
        */
        '0' as secondary_identification_type_flag -- 20250620
  from ${com_schema}.temp_t_mhbos_m_client_all mmc
;

-- 2.5 Create a temporary table temp_mhbos_m_client_secondary_identification_no to store the cleaned secondary identification number.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no(
  `client_no` string, 
  `id_type` string, 
  `ic_no_new` string, 
  `ic_no_old` string, 
  `secondary_id_type` string, 
  `secondary_id_no` string, 
  `secondary_identification_no` string, 
  `secondary_identification_no_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no;

insert into table ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no
select mmc.client_no,
       mmc.id_type,
       mmc.ic_no_new,
       mmc.ic_no_old,
       mmc.secondary_id_type,
       mmc.secondary_id_no ,
       (case when mmc.id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then mmc.ic_no_old 
             when mmc.id_type = '1' and mmc.secondary_id_type = '1' then ''
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '2' then mmc.secondary_id_no
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '3' then mmc.secondary_id_no
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '4' then mmc.secondary_id_no
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '5' then mmc.secondary_id_no
             when mmc.id_type in ('1', '4', '5') and mmc.secondary_id_type = '6' then mmc.secondary_id_no
             when mmc.id_type = '2' and mmc.secondary_id_type = '1' and nvl(mmc.ic_no_old, '') <> '' then mmc.ic_no_old 
             when mmc.id_type = '2' and mmc.secondary_id_type = '3' then mmc.ic_no_new
             when mmc.id_type = '2' and mmc.secondary_id_type = '4' then mmc.secondary_id_no
             when mmc.id_type = '2' and mmc.secondary_id_type = '5' then mmc.ic_no_new
			 when mmc.id_type = '2' and mmc.secondary_id_type = '2' then mmc.secondary_id_no
			 when mmc.id_type = '2' and mmc.secondary_id_type = '6' then mmc.ic_no_new
			 when mmc.id_type = '3' and mmc.secondary_id_type = '3' and nvl(mmc.ic_no_new, '') = nvl(mmc.secondary_id_no, '') then '' 
             when mmc.id_type = '3' and mmc.secondary_id_type = '3' and length(mmc.ic_no_new) < 12 and length(mmc.secondary_id_no) >= 12 and length(mmc.secondary_id_no) <= 15 then mmc.ic_no_new -- updated 20250502
             when mmc.id_type = '3' and mmc.secondary_id_type = '3' and nvl(mmc.ic_no_new, '') <> nvl(mmc.secondary_id_no, '') then mmc.secondary_id_no
			 when mmc.id_type = '3' and mmc.secondary_id_type <> '3' then mmc.secondary_id_no
			 when mmc.id_type = '6' and mmc.secondary_id_type = '3' then mmc.ic_no_new
			 when nvl(mmc.id_type, '') = '' and nvl(mmc.secondary_id_type, '') <> '' and nvl(mmc.secondary_id_no, '') <> '' then ''
			 when nvl(trim(mmc.secondary_id_no), '') = '' then ''
             else mmc.secondary_id_no
         end ) as secondary_identification_no,
      /*
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
      */
      '0' as secondary_identification_no_flag -- 20250620
  from ${com_schema}.temp_t_mhbos_m_client_all mmc
;

-- 2.6 Create a temporary table temp_mhbos_m_client_identification_info to store the cleaned identification information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_identification_info(
  `client_no` string, 
  `id_type` string, 
  `ic_no_new` string, 
  `ic_no_old` string, 
  `secondary_id_type` string, 
  `secondary_id_no` string, 
  `primary_identification_type` string, 
  `primary_identification_type_flag` string, 
  `secondary_identification_type` string, 
  `secondary_identification_type_flag` string, 
  `primary_identification_no` string, 
  `primary_identification_no_flag` string, 
  `secondary_identification_no` string, 
  `secondary_identification_no_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_identification_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_identification_info
select p_no.client_no,
       p_no.id_type,
       p_no.ic_no_new,
       p_no.ic_no_old,
       p_no.secondary_id_type,
       p_no.secondary_id_no,
       p_type.primary_identification_type,
       p_type.primary_identification_type_flag,
	     s_type.secondary_identification_type,
       s_type.secondary_identification_type_flag,
       (case when p_type.primary_identification_type = '1' then -- identification type is '1'
               (case when p_no.primary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                          length(p_no.primary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                          -- 1st till 6 digit represent date of birth in YYMMDD format
                          (int(substr(p_no.primary_identification_no, 3, 2)) >= 1 and int(substr(p_no.primary_identification_no, 3, 2)) <= 12 ) and 
						  (int(substr(p_no.primary_identification_no, 5, 2)) >= 1 and int(substr(p_no.primary_identification_no, 5, 2)) <= 31 ) and 
                          -- At 7th and 8th digit referring to the Place of Birth
                          substr(p_no.primary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                 '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                 '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                            p_no.primary_identification_no
                     else '@[' || p_no.primary_identification_no || ']'
                end)
             else p_no.primary_identification_no
         end ) as primary_identification_no,
       (case when p_type.primary_identification_type = '1' then -- identification type is '1'
               (case when p_no.primary_identification_no rlike '^\\d+$' and -- Determine whether the ID number is all numbers
                          length(p_no.primary_identification_no) = 12 and -- Determine whether the ID number is 12 digits long
                          -- 1st till 6 digit represent date of birth in YYMMDD format
                          (int(substr(p_no.primary_identification_no, 3, 2)) >= 1 and int(substr(p_no.primary_identification_no, 3, 2)) <= 12 ) and 
						  (int(substr(p_no.primary_identification_no, 5, 2)) >= 1 and int(substr(p_no.primary_identification_no, 5, 2)) <= 31 ) and  
                          -- At 7th and 8th digit referring to the Place of Birth
                          substr(p_no.primary_identification_no, 7, 2) in ('01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                                 '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55','56','57','15','58','16','60','61','62','63',
                                 '64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84','85','86','87','88','89','90','91','92','93','98','99') then
                            '0'
                     else '1'
                end)
             else p_no.primary_identification_no_flag 
         end ) as primary_identification_no_flag,
       /*
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
        */
        s_no.secondary_identification_no as secondary_identification_no, -- 20250620
        /*
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
        */
        s_no.secondary_identification_no_flag as  secondary_identification_no_flag -- 20250620
  from ${com_schema}.temp_t_mhbos_m_client_primary_identification_no p_no
  left join ${com_schema}.temp_t_mhbos_m_client_primary_identification_type p_type
    on p_no.client_no = p_type.client_no
  left join ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no s_no 
    on p_no.client_no = s_no.client_no
  left join ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type s_type
    on p_no.client_no = s_type.client_no
;

-- 2.7 Create a temporary table temp_mhbos_m_client_dob_info to store the cleaned date of birth.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_dob_info(
  `client_no` string, 
  `primary_identification_type` string, 
  `primary_identification_no` string, 
  `primary_identification_type_flag` string, 
  `primary_identification_no_flag` string, 
  `source_date_of_birth` timestamp, 
  `date_of_birth` string, 
  `date_of_birth_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_dob_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_dob_info
select id.client_no,
       id.primary_identification_type,
       id.primary_identification_no,
       id.primary_identification_type_flag,
       id.primary_identification_no_flag,
       mmce.date_of_birth as source_date_of_birth,
       (case when id.primary_identification_type = '1' and id.primary_identification_no_flag = '0' then 
               date_format(from_unixtime(unix_timestamp(substr(id.primary_identification_no, 1, 6), 'yymmdd'), 'yyyy-mm-dd'), 'yyyy-MM-dd')
             when mmce.date_of_birth is not null then mmce.date_of_birth
             else mmce.date_of_birth
        end) as date_of_birth,
       (case when id.primary_identification_type = '1' and id.primary_identification_no_flag = '0' then '0'
             when mmce.date_of_birth is not null then'0'
             else '0'
        end) as date_of_birth_flag
  from ${com_schema}.temp_t_mhbos_m_client_identification_info id,
       ${com_schema}.temp_t_mhbos_m_client_all mmce
 where id.client_no = mmce.client_no
;

-- 2.8 Create a temporary table temp_mhbos_m_client_gender_info to store the cleaned gender.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_gender_info(
  `client_no` string, 
  `primary_identification_type` string, 
  `primary_identification_no` string, 
  `primary_identification_type_flag` string, 
  `primary_identification_no_flag` string, 
  `source_sex` string, 
  `sex` string, 
  `sex_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_gender_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_gender_info
select id.client_no,
       id.primary_identification_type,
       id.primary_identification_no,
       id.primary_identification_type_flag,
       id.primary_identification_no_flag,
       mmca.sex as source_sex,
       (case when nvl(trim(mmca.sex), '') <> '' then
               (case when mmca.sex = 'F' then 'FEMALE'
                     when mmca.sex = 'M' then 'MALE'
                     else '@[' || mmca.sex || ']'
                 end )
		     when id.primary_identification_type = '1' and id.primary_identification_no_flag = '0' then 
               (case when substr(id.primary_identification_no, 12, 1) in ('1', '3', '5', '7', '9') then 'MALE'
                     else 'FEMALE' end)
             else ''
       end) as sex,
       (case when id.primary_identification_type = '1' and id.primary_identification_no_flag = '0' then 
               '0'
             when nvl(trim(mmca.sex), '') <> '' then
               (case when mmca.sex = 'F' then '0'
                     when mmca.sex = 'M' then '0'
                     else '1'
                 end )
             else '0'
       end) as sex_flag
  from ${com_schema}.temp_t_mhbos_m_client_identification_info id
  left join ${com_schema}.temp_t_mhbos_m_client_all mmca
    on id.client_no = mmca.client_no
;

-- 2.9 Create a temporary table temp_mhbos_m_client_customer_name_1 to store the cleaned customer name.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_customer_name_1(
  `client_no` string, 
  `client_type` string,  
  `source_client_name` string, 
  `source_client_name1` string, 
  `source_client_name2` string, 
  `source_client_name3` string, 
  `primary_identification_type` string, 
  `client_name` string, 
  `client_name1` string, 
  `client_name2` string, 
  `client_name3` string, 
  `client_name_flag` string, 
  `client_name1_flag` string, 
  `client_name2_flag` string, 
  `client_name3_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_customer_name_1;

insert into table ${com_schema}.temp_t_mhbos_m_client_customer_name_1
select mmca.client_no,
       mmca.client_type,
       mmca.client_name as source_client_name,
       mmca.client_name1 as source_client_name1,
       mmca.client_name2 as source_client_name2,
       mmca.client_name3 as source_client_name3,
       tmmcpit.primary_identification_type,
       (case 
        /*     when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name like '%CONDO%' or 
                   mmca.client_name like '%NO 1%' or
                   mmca.client_name like '%NO 2%' or
                   mmca.client_name like '%OFFICE%' or
                   mmca.client_name like '%FLR%') then 
               '@[' || upper(mmca.client_name) || ']'
        */
             when (tmmcpit.primary_identification_type = '1' and 
                  nvl(mmca.client_name, '') <> '' and
                  nvl(mmca.client_name1, '') = '' and 
                  nvl(mmca.client_name2, '') = '' and
                  nvl(mmca.client_name3, '') = '') then 
               trim(regexp_replace(regexp_replace(regexp_replace(
                 regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                 regexp_replace(upper(trim(mmca.client_name)), '\\s*\\([^)]*\\)$',  ''), -- 20240723 changed from \\(.+\\) to \\(.*\\)$  --20241118 changes
                   ' MR$',  ''), ' MDM$',  ''), ' MS$',  ''), ' PUAN$',  ''),
                   '^MR ',  ''), '^DR ',  ''), '^DR. ',  ''))
             else upper(trim(mmca.client_name)) end) as client_name,
       /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name1 like '%CONDO%' or 
                   mmca.client_name1 like '%NO 1%' or
                   mmca.client_name1 like '%NO 2%' or
                   mmca.client_name1 like '%OFFICE%' or
                   mmca.client_name1 like '%FLR%') then 
               '@[' || upper(mmca.client_name1) || ']'
             else upper(trim(mmca.client_name1)) end) as client_name1,
        */
            upper(trim(mmca.client_name1)) as client_name1, -- 20250620
       /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name2 like '%CONDO%' or 
                   mmca.client_name2 like '%NO 1%' or
                   mmca.client_name2 like '%NO 2%' or
                   mmca.client_name2 like '%OFFICE%' or
                   mmca.client_name2 like '%FLR%') then 
               '@[' || upper(mmca.client_name2) || ']'
             else upper(trim(mmca.client_name2)) end)  as client_name2,
       */
           upper(trim(mmca.client_name2)) as client_name2, -- 20250620
       /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name3 like '%CONDO%' or 
                   mmca.client_name3 like '%NO 1%' or
                   mmca.client_name3 like '%NO 2%' or
                   mmca.client_name3 like '%OFFICE%' or
                   mmca.client_name3 like '%FLR%') then 
               '@[' || upper(mmca.client_name3) || ']'
             else upper(trim(mmca.client_name3)) end) as client_name3,
       */
            upper(trim(mmca.client_name3)) as client_name3, -- 20250620
       /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name like '%CONDO%' or 
                   mmca.client_name like '%NO 1%' or
                   mmca.client_name like '%NO 2%' or
                   mmca.client_name like '%OFFICE%' or
                   mmca.client_name like '%FLR%') then 
               '1'
             else '0' end) as client_name_flag,
        */
            '0' as client_name_flag, -- 20250620
        /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name1 like '%CONDO%' or 
                   mmca.client_name1 like '%NO 1%' or
                   mmca.client_name1 like '%NO 2%' or
                   mmca.client_name1 like '%OFFICE%' or
                   mmca.client_name1 like '%FLR%') then '1'
             else '0' end) as client_name1_flag,
        */
            '0' as client_name1_flag, -- 20250620
       /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name2 like '%CONDO%' or 
                   mmca.client_name2 like '%NO 1%' or
                   mmca.client_name2 like '%NO 2%' or
                   mmca.client_name2 like '%OFFICE%' or
                   mmca.client_name2 like '%FLR%') then '1'
              else '0' end)  as client_name2_flag,
       */
            '0' as client_name2_flag, -- 20250620
      /*
       (case when mmca.client_no in ('NG0086080' ,'MO0167753') and 
	              (mmca.client_name3 like '%CONDO%' or 
                   mmca.client_name3 like '%NO 1%' or
                   mmca.client_name3 like '%NO 2%' or
                   mmca.client_name3 like '%OFFICE%' or
                   mmca.client_name3 like '%FLR%') then '1'
              else '0' end) as client_name3_flag
      */
            '0' as client_name3_flag -- 20250620
  from ${com_schema}.temp_t_mhbos_m_client_all mmca
  left join ${com_schema}.temp_t_mhbos_m_client_primary_identification_type tmmcpit 
    on mmca.client_no = tmmcpit.client_no
;

-- 2.10 Create a temporary table temp_mhbos_m_client_customer_name_2 to store the second time cleaned customer name.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_customer_name_2(
  `client_no` string,
  `client_type` string,
  `customer_name_concatenate` string,
  `source_customer_name_concatenate` string,
  `customer_name` string, 
  `source_client_name` string, 
  `source_client_name1` string, 
  `source_client_name2` string, 
  `source_client_name3` string, 
  `primary_identification_type` string, 
  `client_name` string, 
  `client_name1` string, 
  `client_name2` string, 
  `client_name3` string, 
  `client_name_flag` string, 
  `client_name1_flag` string, 
  `client_name2_flag` string, 
  `client_name3_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_customer_name_2;

insert into table ${com_schema}.temp_t_mhbos_m_client_customer_name_2
select t.client_no,
       t.client_type,
       t.customer_name_concatenate,
	   t.source_customer_name_concatenate,
       (case when t.client_type in ('#', '0', '1', '6', '8', 'V') then t.customer_name_concatenate
        when t.customer_name_concatenate rlike '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$' then
			   t.customer_name_concatenate
	         when t.customer_name_concatenate rlike '.*(^| )FOR( +)(.+)$' then
               regexp_extract(t.customer_name_concatenate, '.*(^| )FOR( +)(.+)$', 3)
			 when t.customer_name_concatenate rlike '.*(^| )FOR$' then
			   t.client_name
             when (t.customer_name_concatenate rlike '.*(^| )NOMINEES( |$)' or 
                   t.customer_name_concatenate rlike '.*(^| )PLEDGED( |$)' or 
                   t.customer_name_concatenate rlike '.*(^| )TEMPATAN( |$)' or 
                   t.customer_name_concatenate rlike '.*(^| )ASING( |$)') and 
				   nvl(trim(t.client_name1), '') <> '' then 
               nvl(trim(t.client_name1), t.client_name)
             else t.customer_name_concatenate end) as customer_name,
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
  from (select cn.*,
               trim(cn.client_name) || 
                 (case when nvl(cn.client_name1, '') <> '' then  (' ' || cn.client_name1) else '' end) || 
                 (case when nvl(cn.client_name2, '') <> '' then  (' ' || cn.client_name2) else '' end) ||
                 (case when nvl(cn.client_name3, '') <> '' then  (' ' || cn.client_name3) else '' end) as customer_name_concatenate,
			   trim(cn.source_client_name) || 
                 (case when nvl(cn.source_client_name1, '') <> '' then  (' ' || cn.source_client_name1) else '' end) || 
                 (case when nvl(cn.source_client_name2, '') <> '' then  (' ' || cn.source_client_name2) else '' end) ||
                 (case when nvl(cn.source_client_name3, '') <> '' then  (' ' || cn.source_client_name3) else '' end) as source_customer_name_concatenate
          from ${com_schema}.temp_t_mhbos_m_client_customer_name_1 cn) t
;

-- 2.11 Create a temporary table temp_mhbos_m_client_race_info to store the cleaned race information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_race_info(
  `client_no` string, 
  `source_race` string, 
  `race` string, 
  `race_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_race_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_race_info
select mmca.client_no,
       mmca.race as source_race,
       (case when mmca.race = 'C' then 'CHINESE'
	         when mmca.race = 'F' then 'FOREIGNER'
			 when mmca.race = 'I' then 'INDIAN'
			 when mmca.race = 'M' then 'MALAY'
			 when mmca.race = 'O' then 'OTHERS'
			 when mmca.race = 'B' then 'BUMIPUTRA'
             when cn2.customer_name rlike '(^| )A/P( |$)' then 'INDIAN'
             when cn2.customer_name rlike '(^| )A/L( |$)' then 'INDIAN'
             when cn2.customer_name rlike '(^| )S/O( |$)' then 'INDIAN'
             when cn2.customer_name rlike '(^| )D/O( |$)' then 'INDIAN'
             else null end) as race,
       '0' as race_flag
  from ${com_schema}.temp_t_mhbos_m_client_all mmca
  left join ${com_schema}.temp_t_mhbos_m_client_customer_name_2 cn2
    on mmca.client_no = cn2.client_no
;

-- 2.12 Create a temporary table temp_t_mhbos_m_client_telephone_clean to store the cleaned telephone number information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_telephone_clean(
  `client_no` string, 
  `source_mobile_no` string, 
  `source_fax_no` string, 
  `source_tel_no_home` string, 
  `source_tel_no_office` string, 
  `mobile_no` string, 
  `fax_no` string, 
  `tel_no_home` string, 
  `tel_no_office` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_telephone_clean;

insert into table ${com_schema}.temp_t_mhbos_m_client_telephone_clean			--modify 20250502
select t.client_no,
       t.source_mobile_no,
       t.source_fax_no,
       t.source_tel_no_home,
       t.source_tel_no_office,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(t.mobile_no, 'EXT', '/') -- Replace EXT to /
                                , '[*Xx]', '/') -- Replace *, x, X to /
                            , '[^0-9/()+]', '') -- Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later)
                        , '\\((?![0]\\))', '') -- Remove '(' only if it's not part of "(0)" pattern
                    , '(?<!\\(0)\\)', '') -- Remove ')' only if it's not part of "(0)" pattern
                , '/(?!([0-9]|\\(0\\)))', '') -- Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0)
            , '(?<=.)(\\+)', '') -- Keep '+' sign if it's the first character, remove otherwise
        AS mobile_no,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(t.fax_no, 'EXT', '/') -- Replace EXT to /
                                , '[*Xx]', '/') -- Replace *, x, X to /
                            , '[^0-9/()+]', '') -- Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later)
                        , '\\((?![0]\\))', '') -- Remove '(' only if it's not part of "(0)" pattern
                    , '(?<!\\(0)\\)', '') -- Remove ')' only if it's not part of "(0)" pattern
                , '/(?!([0-9]|\\(0\\)))', '') -- Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0)
            , '(?<=.)(\\+)', '') -- Keep '+' sign if it's the first character, remove otherwise
        AS fax_no,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(t.tel_no_home, 'EXT', '/') -- Replace EXT to /
                                , '[*Xx]', '/') -- Replace *, x, X to /
                            , '[^0-9/()+]', '') -- Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later)
                        , '\\((?![0]\\))', '') -- Remove '(' only if it's not part of "(0)" pattern
                    , '(?<!\\(0)\\)', '') -- Remove ')' only if it's not part of "(0)" pattern
                , '/(?!([0-9]|\\(0\\)))', '') -- Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0)
            , '(?<=.)(\\+)', '') -- Keep '+' sign if it's the first character, remove otherwise
        AS tel_no_home,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(t.tel_no_office, 'EXT', '/') -- Replace EXT to /
                                , '[*Xx]', '/') -- Replace *, x, X to /
                            , '[^0-9/()+]', '') -- Remove non digit character (including alphabet, dashes and spaces), keep (), + (We will deal with it later)
                        , '\\((?![0]\\))', '') -- Remove '(' only if it's not part of "(0)" pattern
                    , '(?<!\\(0)\\)', '') -- Remove ')' only if it's not part of "(0)" pattern
                , '/(?!([0-9]|\\(0\\)))', '') -- Trim '/' that is EITHER NOT followed by digit OR NOT followed by (0)
            , '(?<=.)(\\+)', '') -- Keep '+' sign if it's the first character, remove otherwise
        AS tel_no_office
  from (select mmca.client_no,
               (mmca.mobile_prefix || mmca.mobile_no) as source_mobile_no,
               mmca.fax_no as source_fax_no,
               mmca.tel_no_home as source_tel_no_home,
               mmca.tel_no_office as source_tel_no_office,
               (mmca.mobile_prefix || mmca.mobile_no) as mobile_no,
               mmca.fax_no as fax_no,
               mmca.tel_no_home as tel_no_home,
               mmca.tel_no_office as tel_no_office
          from ${com_schema}.temp_t_mhbos_m_client_all mmca) t
where 1 = 1
;

-- 2.12 Create a temporary table temp_mhbos_m_client_telephone_number_info to store the cleaned telephone number information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_telephone_number_info(
  `client_no` string,
  `source_mobile_no` string,
  `source_fax_no` string,
  `source_tel_no_home` string,
  `source_tel_no_office` string,
  `mobile_no` string,
  `fax_no` string,
  `tel_no_home` string,
  `tel_no_office` string,
  `mobile_no_flag` string,
  `fax_no_flag` string,
  `tel_no_home_flag` string,
  `tel_no_office_flag` string)
;

--2.12.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_telephone_number_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_telephone_number_info  --modify 20250502 removed flag logic
select t.client_no,
       t.source_mobile_no,
       t.source_fax_no,
       t.source_tel_no_home,
       t.source_tel_no_office,
      --  (case when t.mobile_no rlike '^[0-9+]+$' then t.mobile_no
      --        when t.mobile_no like '%(0)%' then t.mobile_no
      --        when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then t.mobile_no
	    --      when nvl(trim(t.mobile_no), '') = '' then ''
      --        else '@[' || t.mobile_no || ']' end) as mobile_no,
      t.mobile_no,
      --  (case when t.fax_no rlike '^[0-9+]+$' then t.fax_no
      --        when t.fax_no like '%(0)%' then t.fax_no
      --        when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then t.fax_no
	    --      when nvl(trim(t.fax_no), '') = '' then ''
      --        else '@[' || t.fax_no || ']' end) as fax_no,
      t.fax_no,
      --  (case when t.tel_no_home rlike '^[0-9+]+$' then t.tel_no_home
      --        when t.tel_no_home like '%(0)%' then t.tel_no_home
      --        when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_home
	    --      when nvl(trim(t.tel_no_home), '') = '' then ''
      --        else '@[' || t.tel_no_home || ']' end) as tel_no_home,
      t.tel_no_home,
      --  (case when t.tel_no_office rlike '^[0-9+]+$' then t.tel_no_office
      --        when t.tel_no_office like '%(0)%' then t.tel_no_office
      --        when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then t.tel_no_office
	    --      when nvl(trim(t.tel_no_office), '') = '' then ''
      --        else '@[' || t.tel_no_office || ']' end) as tel_no_office,
      t.tel_no_office,
      --  (case when t.mobile_no rlike '^[0-9+]+$' then '0'
      --        when t.mobile_no like '%(0)%' then '0'
      --        when t.mobile_no rlike '^[0-9+]+\/[0-9]+$' then '0'
	    --      when nvl(trim(t.mobile_no), '') = '' then '0'
      --        else '1' end) as mobile_no_flag,
      '0' as mobile_no_flag,
      --  (case when t.fax_no rlike '^[0-9+]+$' then '0'
      --        when t.fax_no like '%(0)%' then '0'
      --        when t.fax_no rlike '^[0-9+]+\/[0-9]+$' then '0'
	    --      when nvl(trim(t.fax_no), '') = '' then '0'
      --        else '1' end) as fax_no_flag,
      '0' as fax_no_flag,
      --  (case when t.tel_no_home rlike '^[0-9+]+$' then '0'
      --        when t.tel_no_home like '%(0)%' then '0'
      --        when t.tel_no_home rlike '^[0-9+]+\/[0-9]+$' then '0'
	    --      when nvl(trim(t.tel_no_home), '') = '' then '0'
      --        else '1' end) as tel_no_home_flag,
      '0' as tel_no_home_flag,
      --  (case when t.tel_no_office rlike '^[0-9+]+$' then '0'
      --        when t.tel_no_office like '%(0)%' then '0'
      --        when t.tel_no_office rlike '^[0-9+]+\/[0-9]+$' then '0'
	    --      when nvl(trim(t.tel_no_office), '') = '' then '0'
      --        else '1' end) as tel_no_office_flag
      '0' as tel_no_office_flag
  from ${com_schema}.temp_t_mhbos_m_client_telephone_clean t
;

-- 2.13 Create a temporary table temp_t_mhbos_m_client_email_clean to store the cleaned email information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_email_clean(
  `client_no` string,
  `source_email` string,
  `einvoice_email` string, --20250903 einvoice_email
  `email_1` string,
  `email_2` string,
  `email_3` string,
  `email_4` string,
  `email_5` string,
  `email_6` string,
  `email_7` string,
  `email_8` string,
  `email_9` string,
  `email_10` string)
;

--2.13.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_email_clean;

insert into table ${com_schema}.temp_t_mhbos_m_client_email_clean
select t.client_no,
       t.source_email,
       t.einvoice_email, --20250903 einvoice_email
       trim(split(t.email, ';')[0]) as email_1,
	   trim(split(t.email, ';')[1]) as email_2,
	   trim(split(t.email, ';')[2]) as email_3,
	   trim(split(t.email, ';')[3]) as email_4,
	   trim(split(t.email, ';')[4]) as email_5,
	   trim(split(t.email, ';')[5]) as email_6,
	   trim(split(t.email, ';')[6]) as email_7,
	   trim(split(t.email, ';')[7]) as email_8,
	   trim(split(t.email, ';')[8]) as email_9,
	   trim(split(t.email, ';')[9]) as email_10
  from (select mmce.client_no,
               replace(lower(mmce.einvoice_email), ' ', '') as einvoice_email, --20250903 einvoice_email
               mmce.email as source_email,
               replace(lower(mmce.email), ' ', '') as email
          from ${com_schema}.temp_t_mhbos_m_client_all mmce
        ) t
;

-- 2.13 Create a temporary table temp_t_mhbos_m_client_email_info_1 to store the cleaned email information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_email_info_1(
  `client_no` string,
  `source_email` string,
  `einvoice_email` string, --20250903 einvoice_email
  `email_1` string,
  `email_2` string,
  `email_3` string,
  `email_4` string,
  `email_5` string,
  `email_6` string,
  `email_7` string,
  `email_8` string,
  `email_9` string,
  `email_10` string,
  `email_flag_1` string,
  `email_flag_2` string,
  `email_flag_3` string,
  `email_flag_4` string,
  `email_flag_5` string,
  `email_flag_6` string,
  `email_flag_7` string,
  `email_flag_8` string,
  `email_flag_9` string,
  `email_flag_10` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_email_info_1;

insert into table ${com_schema}.temp_t_mhbos_m_client_email_info_1
select t.client_no,
       t.source_email,
       (case when t.einvoice_email rlike '^.{1,64}\\@.+\\..+' and length(t.einvoice_email) <= 254 then t.einvoice_email
	         when nvl(trim(t.einvoice_email), '') = '' then ''
               else '' end) as einvoice_email,  --20250903 einvoice_email
       (case when t.email_1 rlike '^.{1,64}\\@.+\\..+' and length(t.email_1) <= 254 then t.email_1
	         when nvl(trim(t.email_1), '') = '' then ''
--             else '@[' || t.email_1 || ']' end) as email_1,
               else '' end) as email_1,
       (case when t.email_2 rlike '^.{1,64}\\@.+\\..+' and length(t.email_2) <= 254 then t.email_2
	         when nvl(trim(t.email_2), '') = '' then ''
--             else '@[' || t.email_2 || ']' end) as email_2,
               else '' end) as email_2,
       (case when t.email_3 rlike '^.{1,64}\\@.+\\..+' and length(t.email_3) <= 254 then t.email_3
	         when nvl(trim(t.email_3), '') = '' then ''
--             else '@[' || t.email_3 || ']' end) as email_3,
               else '' end) as email_3,
       (case when t.email_4 rlike '^.{1,64}\\@.+\\..+' and length(t.email_4) <= 254 then t.email_4
	         when nvl(trim(t.email_4), '') = '' then ''
--             else '@[' || t.email_4 || ']' end) as email_4,
               else '' end) as email_4,
       (case when t.email_5 rlike '^.{1,64}\\@.+\\..+' and length(t.email_5) <= 254 then t.email_5
	         when nvl(trim(t.email_5), '') = '' then ''
--             else '@[' || t.email_5 || ']' end) as email_5,
               else '' end) as email_5,
       (case when t.email_6 rlike '^.{1,64}\\@.+\\..+' and length(t.email_6) <= 254 then t.email_6
	         when nvl(trim(t.email_6), '') = '' then ''
--             else '@[' || t.email_6 || ']' end) as email_6,
               else '' end) as email_6,
       (case when t.email_7 rlike '^.{1,64}\\@.+\\..+' and length(t.email_7) <= 254 then t.email_7
	         when nvl(trim(t.email_7), '') = '' then ''
--             else '@[' || t.email_7 || ']' end) as email_7,
               else '' end) as email_7,
       (case when t.email_8 rlike '^.{1,64}\\@.+\\..+' and length(t.email_8) <= 254 then t.email_8
	         when nvl(trim(t.email_8), '') = '' then ''
--             else '@[' || t.email_8 || ']' end) as email_8,
               else '' end) as email_8,
       (case when t.email_9 rlike '^.{1,64}\\@.+\\..+' and length(t.email_9) <= 254 then t.email_9
	         when nvl(trim(t.email_9), '') = '' then ''
--             else '@[' || t.email_9 || ']' end) as email_9,
               else '' end) as email_9,
       (case when t.email_10 rlike '^.{1,64}\\@.+\\..+' and length(t.email_10) <= 254 then t.email_10
	         when nvl(trim(t.email_10), '') = '' then ''
--             else '@[' || t.email_10 || ']' end) as email_10,
               else '' end) as email_10,
/*       (case when t.email_1 rlike '^.{1,64}\\@.+\\..+' and length(t.email_1) <= 254 then '0'
	         when nvl(trim(t.email_1), '') = '' then '0'
             else '1' end) as email_flag_1,
*/
               '0' as email_flag_1,
/*       (case when t.email_2 rlike '^.{1,64}\\@.+\\..+' and length(t.email_2) <= 254 then '0'
	         when nvl(trim(t.email_2), '') = '' then '0'
             else '1' end) as email_flag_2,
*/
               '0' as email_flag_2,
/*       (case when t.email_3 rlike '^.{1,64}\\@.+\\..+' and length(t.email_3) <= 254 then '0'
	         when nvl(trim(t.email_3), '') = '' then '0'
             else '1' end) as email_flag_3,
*/
               '0' as email_flag_3,
/*       (case when t.email_4 rlike '^.{1,64}\\@.+\\..+' and length(t.email_4) <= 254 then '0'
	         when nvl(trim(t.email_4), '') = '' then '0'
             else '1' end) as email_flag_4,
*/
               '0' as email_flag_4,
/*       (case when t.email_5 rlike '^.{1,64}\\@.+\\..+' and length(t.email_5) <= 254 then '0'
	         when nvl(trim(t.email_5), '') = '' then '0'
             else '1' end) as email_flag_5,
*/             
               '0' as email_flag_5,
/*       (case when t.email_6 rlike '^.{1,64}\\@.+\\..+' and length(t.email_6) <= 254 then '0'
	         when nvl(trim(t.email_6), '') = '' then '0'
             else '1' end) as email_flag_6,
*/
               '0' as email_flag_6,
/*       (case when t.email_7 rlike '^.{1,64}\\@.+\\..+' and length(t.email_7) <= 254 then '0'
	         when nvl(trim(t.email_7), '') = '' then '0'
             else '1' end) as email_flag_7,
*/
               '0' as email_flag_7,
/*       (case when t.email_8 rlike '^.{1,64}\\@.+\\..+' and length(t.email_8) <= 254 then '0'
	         when nvl(trim(t.email_8), '') = '' then '0'
             else '1' end) as email_flag_8,
*/
               '0' as email_flag_8,
/*       (case when t.email_9 rlike '^.{1,64}\\@.+\\..+' and length(t.email_9) <= 254 then '0'
	         when nvl(trim(t.email_9), '') = '' then '0'
             else '1' end) as email_flag_9,
*/
               '0' as email_flag_9,
/*       (case when t.email_10 rlike '^.{1,64}\\@.+\\..+' and length(t.email_10) <= 254 then '0'
	         when nvl(trim(t.email_10), '') = '' then '0'
             else '1' end) as email_flag_10
*/
               '0' as email_flag_10
  from (select e.client_no,
               e.source_email,
               (case when e.einvoice_email not like '%@%' then ''
                     when e.einvoice_email like '%@hotmail' then e.einvoice_email || '.com'
                     when e.einvoice_email like '%@gmail' then e.einvoice_email || '.com'
                     when e.einvoice_email like '%@yahoo' then e.einvoice_email || '.com'
                     when e.einvoice_email rlike '\@hotmail[^\\.]+' then regexp_replace(e.einvoice_email,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.einvoice_email rlike '\@gmail[^\\.]+' then regexp_replace(e.einvoice_email,'\@gmail[^\\.]+', '@gmail.com')
                     when e.einvoice_email rlike '\@yahoo[^\\.]+' then regexp_replace(e.einvoice_email,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.einvoice_email
                 end ) as einvoice_email, --20250903 einvoice_email
               (case when e.email_1 not like '%@%' then ''
                     when e.email_1 like '%@hotmail' then e.email_1 || '.com'
                     when e.email_1 like '%@gmail' then e.email_1 || '.com'
                     when e.email_1 like '%@yahoo' then e.email_1 || '.com'
                     when e.email_1 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_1,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_1 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_1,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_1 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_1,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_1
                 end ) as email_1,
               (case when e.email_2 not like '%@%' then ''
                     when e.email_2 like '%@hotmail' then e.email_2 || '.com'
                     when e.email_2 like '%@gmail' then e.email_2 || '.com'
                     when e.email_2 like '%@yahoo' then e.email_2 || '.com'
                     when e.email_2 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_2,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_2 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_2,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_2 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_2,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_2
                 end ) as email_2,
               (case when e.email_3 not like '%@%' then ''
                     when e.email_3 like '%@hotmail' then e.email_3 || '.com'
                     when e.email_3 like '%@gmail' then e.email_3 || '.com'
                     when e.email_3 like '%@yahoo' then e.email_3 || '.com'
                     when e.email_3 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_3,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_3 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_3,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_3 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_3,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_3
                 end ) as email_3,
               (case when e.email_4 not like '%@%' then ''
                     when e.email_4 like '%@hotmail' then e.email_4 || '.com'
                     when e.email_4 like '%@gmail' then e.email_4 || '.com'
                     when e.email_4 like '%@yahoo' then e.email_4 || '.com'
                     when e.email_4 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_4,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_4 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_4,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_4 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_4,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_4
                 end ) as email_4,
               (case when e.email_5 not like '%@%' then ''
                     when e.email_5 like '%@hotmail' then e.email_5 || '.com'
                     when e.email_5 like '%@gmail' then e.email_5 || '.com'
                     when e.email_5 like '%@yahoo' then e.email_5 || '.com'
                     when e.email_5 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_5,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_5 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_5,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_5 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_5,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_5
                 end ) as email_5,
               (case when e.email_6 not like '%@%' then ''
                     when e.email_6 like '%@hotmail' then e.email_6 || '.com'
                     when e.email_6 like '%@gmail' then e.email_6 || '.com'
                     when e.email_6 like '%@yahoo' then e.email_6 || '.com'
                     when e.email_6 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_6,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_6 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_6,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_6 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_6,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_6
                 end ) as email_6,
               (case when e.email_7 not like '%@%' then ''
                     when e.email_7 like '%@hotmail' then e.email_7 || '.com'
                     when e.email_7 like '%@gmail' then e.email_7 || '.com'
                     when e.email_7 like '%@yahoo' then e.email_7 || '.com'
                     when e.email_7 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_7,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_7 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_7,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_7 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_7,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_7
                 end ) as email_7,
               (case when e.email_8 not like '%@%' then ''
                     when e.email_8 like '%@hotmail' then e.email_8 || '.com'
                     when e.email_8 like '%@gmail' then e.email_8 || '.com'
                     when e.email_8 like '%@yahoo' then e.email_8 || '.com'
                     when e.email_8 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_8,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_8 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_8,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_8 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_8,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_8
                 end ) as email_8,
               (case when e.email_9 not like '%@%' then ''
                     when e.email_9 like '%@hotmail' then e.email_9 || '.com'
                     when e.email_9 like '%@gmail' then e.email_9 || '.com'
                     when e.email_9 like '%@yahoo' then e.email_9 || '.com'
                     when e.email_9 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_9,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_9 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_9,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_9 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_9,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_9
                 end ) as email_9,
               (case when e.email_10 not like '%@%' then ''
                     when e.email_10 like '%@hotmail' then e.email_10 || '.com'
                     when e.email_10 like '%@gmail' then e.email_10 || '.com'
                     when e.email_10 like '%@yahoo' then e.email_10 || '.com'
                     when e.email_10 rlike '\@hotmail[^\\.]+' then regexp_replace(e.email_10,'\@hotmail[^\\.]+', '@hotmail.com')
                     when e.email_10 rlike '\@gmail[^\\.]+' then regexp_replace(e.email_10,'\@gmail[^\\.]+', '@gmail.com')
                     when e.email_10 rlike '\@yahoo[^\\.]+' then regexp_replace(e.email_10,'\@yahoo[^\\.]+', '@yahoo.com')
                     else e.email_10
                 end ) as email_10
          from ${com_schema}.temp_t_mhbos_m_client_email_clean e
        ) t
;

-- 2.13 Create a temporary table temp_t_mhbos_m_client_email_info to store the cleaned email information.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_email_info(
  `client_no` string,
  `source_email` string,
  `einvoice_email` string, --20250903 einvoice_email
  `email_1` string,
  `email_2` string,
  `email_3` string,
  `email_4` string,
  `email_5` string,
  `email_6` string,
  `email_7` string,
  `email_8` string,
  `email_9` string,
  `email_10` string,
  `email_flag` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_email_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_email_info
select t.client_no,
       t.source_email,
       t.einvoice_email, --20250903 einvoice_email
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
	   (case when (t.email_flag_1 || t.email_flag_2 || t.email_flag_3 || t.email_flag_4 || t.email_flag_5 || t.email_flag_6 ||
	               t.email_flag_7 || t.email_flag_8 || t.email_flag_9 || t.email_flag_10) like '%1%' then '1'
		     else '0'
		 end) as email_flag
  from ${com_schema}.temp_t_mhbos_m_client_email_info_1 t
 where 1 = 1
;

-- 2.14 Create a temporary table temp_t_mhbos_m_client_address_city to store the cleaned address cityinformation.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_address_city(
  `client_no` string,
  `addr1` string,
  `addr2` string,
  `addr3` string,
  `addr4` string,
  `postcode` string,
  `mailing_addr` string,
  `city` string,
  `state` string,
  `perm_addr1` string,
  `perm_addr2` string,
  `perm_addr3` string,
  `perm_addr4` string,
  `perm_postcode` string,
  `registered_addr` string,
  `perm_city` string,
  `perm_state` string,
  `perm_country` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_address_city;

insert into table ${com_schema}.temp_t_mhbos_m_client_address_city
select addr.client_no,
       coalesce(addr.addr1, addr.addr2, addr.addr3, addr.addr4, '') as addr1,
	   (case when addr.addr1 is not null then coalesce(addr.addr2, addr.addr3, addr.addr4, '')
	         when addr.addr1 is null and addr.addr2 is not null then coalesce(addr.addr3, addr.addr4, '')
			 when addr.addr1 is null and addr.addr2 is null and addr.addr3 is not null then coalesce(addr.addr4, '')
			 else '' end) as addr2,
	   (case when addr.addr1 is not null and addr.addr2 is not null then coalesce(addr.addr3, addr.addr4, '')
	         when addr.addr1 is null and addr.addr2 is not null and addr.addr3 is not null then coalesce(addr.addr4, '')
			 else '' end) as addr3,
	   (case when addr.addr1 is not null and addr.addr2 is not null and addr.addr3 is not null then coalesce(addr.addr4, '')
			 else ''
	     end ) as addr4,
       addr.postcode,
	   addr.mailing_addr,
       regexp_extract(upper(addr.mailing_addr), '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b', 1) as city,
--	   regexp_extract(upper(addr.mailing_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) as state,
      addr.state_code as state, -- 20251013
	   coalesce(addr.perm_addr1, addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '') as perm_addr1,
	   (case when addr.perm_addr1 is not null then coalesce(addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '')
	         when addr.perm_addr1 is null and addr.perm_addr2 is not null then coalesce(addr.perm_addr3, addr.perm_addr4, '')
			 when addr.perm_addr1 is null and addr.perm_addr2 is null and addr.perm_addr3 is not null then coalesce(addr.perm_addr4, '')
			 else '' end) as perm_addr2,
	   (case when addr.perm_addr1 is not null and addr.perm_addr2 is not null then coalesce(addr.perm_addr3, addr.perm_addr4, '')
	         when addr.perm_addr1 is null and addr.perm_addr2 is not null and addr.perm_addr3 is not null then coalesce(addr.perm_addr4, '')
			 else '' end) as perm_addr3,
	   (case when addr.perm_addr1 is not null and addr.perm_addr2 is not null and addr.perm_addr3 is not null then coalesce(addr.perm_addr4, '')
			 else ''
	     end ) as perm_addr4,
       addr.perm_postcode,
	   addr.registered_addr,
       case when nvl(trim(addr.perm_city),'') <> '' then addr.perm_city else regexp_extract(upper(addr.registered_addr), '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b', 1) end as perm_city, -- 20250715
--	     case when nvl(trim(addr.perm_state_code),'') <> '' then addr.perm_state_code else regexp_extract(upper(addr.registered_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) end as perm_state, -- 20250715
       addr.perm_state_code as perm_state, -- 20251013
       addr.perm_country -- 20250715
  from (select ca.client_no,
         (case when nvl(trim(ca.addr1), '') = '' then null else ca.addr1 end) as addr1,
			   (case when nvl(trim(ca.addr2), '') = '' then null else ca.addr2 end) as addr2,
			   (case when nvl(trim(ca.addr3), '') = '' then null else ca.addr3 end) as addr3,
			   (case when nvl(trim(ca.addr4), '') = '' then null else ca.addr4 end) as addr4,
			   ca.postcode,
         (nvl(ca.addr1, '') ||
            (case when nvl(ca.addr2, '') = '' then '' else ' ' || nvl(ca.addr2, '')  end) ||
            (case when nvl(ca.addr3, '') = '' then '' else ' ' || nvl(ca.addr3, '')  end) ||
            (case when nvl(ca.addr4, '') = '' then '' else ' ' || nvl(ca.addr4, '')  end)
         ) as mailing_addr,
         ca.state_code,
			   (case when nvl(trim(ca.perm_addr1), '') = '' then null else ca.perm_addr1 end) as perm_addr1,
			   (case when nvl(trim(ca.perm_addr2), '') = '' then null else ca.perm_addr2 end) as perm_addr2,
			   (case when nvl(trim(ca.perm_addr3), '') = '' then null else ca.perm_addr3 end) as perm_addr3,
			   (case when nvl(trim(ca.perm_addr4), '') = '' then null else ca.perm_addr4 end) as perm_addr4,
			   ca.perm_postcode,
         (nvl(ca.perm_addr1, '') ||
            (case when nvl(ca.perm_addr2, '') = '' then '' else ' ' || nvl(ca.perm_addr2, '')  end) ||
            (case when nvl(ca.perm_addr3, '') = '' then '' else ' ' || nvl(ca.perm_addr3, '')  end) ||
            (case when nvl(ca.perm_addr4, '') = '' then '' else ' ' || nvl(ca.perm_addr4, '')  end)
         ) as registered_addr,
         ca.perm_city,
         ca.perm_state_code,
         ca.perm_country
          from ${com_schema}.temp_t_mhbos_m_client_all ca) addr
;

-- 2.15 Create a temporary table temp_t_mhbos_m_client_address_info to store the cleaned cityinformation.
create table if not exists ${com_schema}.temp_t_mhbos_m_client_address_info(
  `client_no` string,
  `addr1` string,
  `addr2` string,
  `addr3` string,
  `addr4` string,
  `postcode` string,
  `mailing_addr` string,
  `city` string,
  `state` string,
  `perm_addr1` string,
  `perm_addr2` string,
  `perm_addr3` string,
  `perm_addr4` string,
  `perm_postcode` string,
  `registered_addr` string,
  `perm_city` string,
  `perm_state` string,
  `perm_country` string)
;

--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.temp_t_mhbos_m_client_address_info;

insert into table ${com_schema}.temp_t_mhbos_m_client_address_info
select addr.client_no,
       addr.addr1,
       addr.addr2,
       addr.addr3,
       addr.addr4,
       addr.postcode,
       addr.mailing_addr,
       addr.city,
/*       (case when nvl(trim(addr.state), '') = '' then mp1.target_cd_val
	         when trim(addr.state) = 'MALACCA' then 'Melaka'
			 when trim(addr.state) = 'WP' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
			 else addr.state end) as state,
*/
       addr.state, -- 20251013
       addr.perm_addr1,
       addr.perm_addr2,
       addr.perm_addr3,
       addr.perm_addr4,
       addr.perm_postcode,
       addr.registered_addr,
       addr.perm_city,
/*       (case when nvl(trim(addr.perm_state), '') = '' then mp2.target_cd_val
	         when trim(addr.perm_state) = 'MALACCA' then 'Melaka'
			 when trim(addr.perm_state) = 'WP' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'WP.' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'W.P' then 'WILAYAH PERSEKUTUAN'
			 when trim(addr.perm_state) = 'W.P.' then 'WILAYAH PERSEKUTUAN'
			 else addr.perm_state end) as perm_state,
*/
       addr.perm_state, -- 20251013
       addr.perm_country
  from ${com_schema}.temp_t_mhbos_m_client_address_city addr
  left join ${com_schema}.t_ref_pub_cd_map mp1
    on addr.city = mp1.src_code_val
   and mp1.subj = 'com'
   and mp1.src_sys_cd = 'mhbos'
   and mp1.src_tab_en_name = 'r_mhbos_m_client'
   and mp1.src_field_en_name = 'city'
   and mp1.valid_flag = 'Y'
  left join ${com_schema}.t_ref_pub_cd_map mp2
    on addr.perm_city = mp2.src_code_val
   and mp2.subj = 'com'
   and mp2.src_sys_cd = 'mhbos'
   and mp2.src_tab_en_name = 'r_mhbos_m_client'
   and mp2.src_field_en_name = 'city'
   and mp2.valid_flag = 'Y'
;

-- 2.16 Detect the Kenanga nominees name from account_full_name and remove them
-- create temporary table temp_t_mhbos_m_client_nominees_info_1
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_1(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `kenanga_noms_list_1` string,
  `kenanga_noms_list_2` string,
  `kenanga_noms_list_3` string,
  `kenanga_noms_list_4` string,
  `kenanga_noms_list_5` string,
  `kenanga_noms_list_6` string,
  `kenanga_noms_list_7` string,
  `kenanga_noms_list_8` string,
  `kenanga_noms_list_9` string,
  `kenanga_noms_list_10` string,
  `kenanga_noms_list_11` string,
  `kenanga_noms_list_12` string,
  `kenanga_noms_list_13` string,
  `kenanga_noms_list_14` string,
  `kenanga_noms_list_15` string,
  `kenanga_noms_list_16` string,
  `kenanga_noms_list_17` string,
  `kenanga_noms_list_18` string,
  `kenanga_noms_list_19` string,
  `kenanga_noms_list_20` string,
  `kenanga_noms_list_21` string,
  `kenanga_noms_list_22` string,
  `kenanga_noms_list_23` string,
  `kenanga_noms_list_24` string,
  `kenanga_noms_list_25` string,
  `kenanga_noms_list_26` string,
  `kenanga_noms_list_27` string,
  `kenanga_noms_list_28` string,
  `kenanga_noms_list_29` string,
  `kenanga_noms_list_30` string,
  `kenanga_noms_list_31` string,
  `kenanga_noms_list_32` string,
  `kenanga_noms_list_33` string,
  `kenanga_noms_list_34` string,
  `kenanga_noms_list_35` string,
  `kenanga_noms_list_36` string,
  `kenanga_noms_list_37` string,
  `kenanga_noms_list_38` string,
  `kenanga_noms_list_39` string,
  `kenanga_noms_list_40` string,
  `kenanga_noms_list_41` string,
  `kenanga_noms_list_42` string,
  `kenanga_noms_list_43` string,
  `kenanga_noms_list_44` string,
  `kenanga_noms_list_45` string,
  `kenanga_noms_list_46` string,
  `kenanga_noms_list_47` string,
  `kenanga_noms_list_48` string,
  `kenanga_noms_list_49` string,
  `kenanga_noms_list_50` string,
  `kenanga_noms_list_51` string,
  `kenanga_noms_list_52` string,
  `kenanga_noms_list_53` string,
  `kenanga_noms_list_54` string,
  `kenanga_noms_list_55` string,
  `kenanga_noms_list_56` string,
  `kenanga_noms_list_57` string,
  `kenanga_noms_list_58` string,
  `kenanga_noms_list_59` string,
  `kenanga_noms_list_60` string,
  `kenanga_noms_list_61` string,
  `kenanga_noms_list_62` string,
  `kenanga_noms_list_63` string,
  `kenanga_noms_list_64` string,
  `kenanga_noms_list_65` string,
  `kenanga_noms_list_66` string,
  `kenanga_noms_list_67` string,
  `kenanga_noms_list_68` string,
  `kenanga_noms_list_69` string,
  `kenanga_noms_list_70` string,
  `kenanga_noms_list_71` string,
  `kenanga_noms_list_72` string,
  `kenanga_noms_list_73` string,
  `kenanga_noms_list_74` string,
  `kenanga_noms_list_75` string,
  `kenanga_noms_list_76` string,
  `kenanga_noms_list_77` string,
  `kenanga_noms_list_78` string,
  `kenanga_noms_list_79` string,
  `kenanga_noms_list_80` string,
  `kenanga_noms_list_81` string,
  `kenanga_noms_list_82` string,
  `kenanga_noms_list_83` string,
  `kenanga_noms_list_84` string,
  `kenanga_noms_list_85` string,
  `kenanga_noms_list_86` string,
  `kenanga_noms_list_87` string,
  `kenanga_noms_list_88` string,
  `kenanga_noms_list_89` string,
  `kenanga_noms_list_90` string,
  `kenanga_noms_list_91` string,
  `kenanga_noms_list_92` string,
  `kenanga_noms_list_93` string,
  `kenanga_noms_list_94` string,
  `kenanga_noms_list_95` string,
  `kenanga_noms_list_96` string,
  `kenanga_noms_list_97` string,
  `kenanga_noms_list_98` string,
  `kenanga_noms_list_99` string,
  `kenanga_noms_list_100` string,
  `kenanga_noms_list_101` string,
  `kenanga_noms_list_102` string,
  `kenanga_noms_list_103` string,
  `kenanga_noms_list_104` string,
  `kenanga_noms_list_105` string,
  `kenanga_noms_list_106` string,
  `kenanga_noms_list_107` string,
  `kenanga_noms_list_108` string,
  `kenanga_noms_list_109` string,
  `kenanga_noms_list_110` string,
  `kenanga_noms_list_111` string,
  `noms_ind` string,
  `noms` string)
;

-- truncate temporary table
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_1;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_1
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_1
select t1.client_no
       ,t1.client_type
       ,t1.client_name
       ,t1.client_name1
       ,t1.client_name2
       ,t1.client_name3
       ,t1.customer_name_concatenate
       ,replace(t1.customer_name_concatenate, 'KNK NOMS(A)SDN BHD FOR', '') as kenanga_noms_list_1
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (T) S/B', '') as kenanga_noms_list_2
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (ASING) SDN BHD', '') as kenanga_noms_list_3
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (A) SDN BHD', '') as kenanga_noms_list_4
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (A) S/B', '') as kenanga_noms_list_5
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (A)-', '') as kenanga_noms_list_6
       ,replace(t1.customer_name_concatenate, 'KNK NOMS (A)', '') as kenanga_noms_list_7
       ,replace(t1.customer_name_concatenate, 'KENNAGA NOMINEES (TEMPATAN) SDN BHD', '') as kenanga_noms_list_8
       ,replace(t1.customer_name_concatenate, 'KENNAGA NOMINEES (ASING) SDN BHD', '') as kenanga_noms_list_9
       ,replace(t1.customer_name_concatenate, 'KENANGE NOMINEES (T) SDN BHD', '') as kenanga_noms_list_10
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS(T) SB FOR', '') as kenanga_noms_list_11
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS(A) S/B FOR', '') as kenanga_noms_list_12
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS(A) S/B', '') as kenanga_noms_list_13
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS(A) FOR', '') as kenanga_noms_list_14
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T) SDN BHD A/C ', '') as kenanga_noms_list_15
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T) SDN BHD', '') as kenanga_noms_list_16
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T) SB FOR', '') as kenanga_noms_list_17
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T) S/B FOR', '') as kenanga_noms_list_18
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T) S/B', '') as kenanga_noms_list_19
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T)  S/B FOR', '') as kenanga_noms_list_20
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (T)', '') as kenanga_noms_list_21
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (TEMPATAN) SDN BHD', '') as kenanga_noms_list_22
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (ASING) SDN BHD', '') as kenanga_noms_list_23
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (ASING) S/B', '') as kenanga_noms_list_24
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (A) SDN BHD FOR', '') as kenanga_noms_list_25
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (A) SDN BHD', '') as kenanga_noms_list_26
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (A) SB', '') as kenanga_noms_list_27
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (A) S/B FOR', '') as kenanga_noms_list_28
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS (A) S/B', '') as kenanga_noms_list_29
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMS  (T) S/B FOR', '') as kenanga_noms_list_30
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMNIEES (ASING) SDN BHD', '') as kenanga_noms_list_31
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINNES (ASING) SDN BHD', '') as kenanga_noms_list_32
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINNEES (ASING) SDN BHD', '') as kenanga_noms_list_33
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINIEES (ASING) SDN BHD', '') as kenanga_noms_list_34
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINESS (TEMPATAN) SDN BHD FOR', '') as kenanga_noms_list_35
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINESS (TEMPATAN) SDN BHD', '') as kenanga_noms_list_36
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINESS (ASING) SDN BHD', '') as kenanga_noms_list_37
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINESS ( ASING) SDN BHD FOR', '') as kenanga_noms_list_38
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINES (ASING) SDN BHD FOR', '') as kenanga_noms_list_39
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINES (ASING) SDN BHD', '') as kenanga_noms_list_40
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN)SDN BHD', '') as kenanga_noms_list_41
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN) SDN BHD FOR', '') as kenanga_noms_list_42
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(TEMPATAN) SDN BHD', '') as kenanga_noms_list_43
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING)SDN BHD FOR', '') as kenanga_noms_list_44
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING)SDN BHD', '') as kenanga_noms_list_45
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING) SDN BHD FOR', '') as kenanga_noms_list_46
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(ASING) SDN BHD', '') as kenanga_noms_list_47
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) SDN BHD FOR', '') as kenanga_noms_list_48
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) SDN BHD', '') as kenanga_noms_list_49
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES(A) S/B FOR', '') as kenanga_noms_list_50
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES TEMPATAN SDN BHD FOR', '') as kenanga_noms_list_51
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES TEMPATAN SDN BHD', '') as kenanga_noms_list_52
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD FOR', '') as kenanga_noms_list_53
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD F0R', '') as kenanga_noms_list_54
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD BHD FOR', '') as kenanga_noms_list_55
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD A/C FOR', '') as kenanga_noms_list_56
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD A/C', '') as kenanga_noms_list_57
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD  A/C', '') as kenanga_noms_list_58
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) SDN BHD', '') as kenanga_noms_list_59
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B FOR', '') as kenanga_noms_list_60
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B A/C', '') as kenanga_noms_list_61
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B  A/C', '') as kenanga_noms_list_62
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) S/B', '') as kenanga_noms_list_63
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T) A/B', '') as kenanga_noms_list_64
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (T)  SDN BHD FOR', '') as kenanga_noms_list_65
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN)SDN BHD', '') as kenanga_noms_list_66
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN.BHD.', '') as kenanga_noms_list_67
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN. BHD.', '') as kenanga_noms_list_68
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD FOR', '') as kenanga_noms_list_69
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD A/C FOR', '') as kenanga_noms_list_70
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD', '') as kenanga_noms_list_71
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN SDN BHD FOR', '') as kenanga_noms_list_72
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN ) SDN BHD', '') as kenanga_noms_list_73
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)SDN BHD FOR', '') as kenanga_noms_list_74
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)SDN BHD', '') as kenanga_noms_list_75
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SND BHD', '') as kenanga_noms_list_76
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SN BHD', '') as kenanga_noms_list_77
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN. BHD.', '') as kenanga_noms_list_78
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD FOR', '') as kenanga_noms_list_79
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD A/C FOR', '') as kenanga_noms_list_80
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD A/C', '') as kenanga_noms_list_81
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD  FOR', '') as kenanga_noms_list_82
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD  A/C FOR', '') as kenanga_noms_list_83
       ,regexp_replace(t1.customer_name_concatenate, 'KENANGA NOMINEES \\(ASING\\) SDN BHD\'', '') as kenanga_noms_list_84
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN BHD', '') as kenanga_noms_list_85
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) S/B FOR', '') as kenanga_noms_list_86
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) S/B', '') as kenanga_noms_list_87
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) FOR', '') as kenanga_noms_list_88
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING)', '') as kenanga_noms_list_89
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING ) SDN BHD', '') as kenanga_noms_list_90
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASIGN) SDN BHD', '') as kenanga_noms_list_91
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASIAN) SDN BHD', '') as kenanga_noms_list_92
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD FOR', '') as kenanga_noms_list_93
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD  FOR', '') as kenanga_noms_list_94
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD', '') as kenanga_noms_list_95
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) S/B FOR', '') as kenanga_noms_list_96
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) S/B', '') as kenanga_noms_list_97
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( T) SDN BHD', '') as kenanga_noms_list_98
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( TEMPATAN) SDN BHD', '') as kenanga_noms_list_99
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( TEMPATAN ) SDN BHD', '') as kenanga_noms_list_100
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( ASING) SDN BHD', '') as kenanga_noms_list_101
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( ASING ) SDN BHD FOR', '') as kenanga_noms_list_102
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES ( A ) SDN BHD', '') as kenanga_noms_list_103
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEE (TEMPATAN) SDN BHD FOR', '') as kenanga_noms_list_104
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMIMEES (T) SDN BHD', '') as kenanga_noms_list_105
       ,replace(t1.customer_name_concatenate, 'KENANGA NOMIMEES (TEMPATAN) SDN BHD', '') as kenanga_noms_list_106
       ,replace(t1.customer_name_concatenate, 'KENANGA NOM (T) SDN BHD FOR', '') as kenanga_noms_list_107
       ,replace(t1.customer_name_concatenate, 'KENANAGA NOMINEES (T) SDN BHD', '') as kenanga_noms_list_108
	   ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (ASING) SDN  BHD', '') as kenanga_noms_list_109
	   ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (TEMPATAN) SDN BHD -', '') as kenanga_noms_list_110
	   ,replace(t1.customer_name_concatenate, 'KENANGA NOMINEES (A) SDN BHD A/C', '') as kenanga_noms_list_111
	   ,case when t1.noms = 'T' then 'Y'
      when t1.noms = 'A' then 'Y'
      else 'N'
      end as noms_ind
	   ,t1.noms
  from (select cn.*,
			   upper(trim(cn.client_name)) ||
                 (case when nvl(upper(trim(cn.client_name1)), '') <> '' then  (' ' || upper(trim(cn.client_name1))) else '' end) ||
                 (case when nvl(upper(trim(cn.client_name2)), '') <> '' then  (' ' || upper(trim(cn.client_name2))) else '' end) ||
                 (case when nvl(upper(trim(cn.client_name3)), '') <> '' then  (' ' || upper(trim(cn.client_name3))) else '' end) as  customer_name_concatenate, mmca.noms_type, mmca.bene_type
          from ${com_schema}.temp_t_mhbos_m_client_all cn
		  left join ${com_schema}.t_mhbos_m_mcd_client_addr mmca
            on cn.client_no = mmca.client_no
           and mmca.part_id = '${batch_date}'
--         and (nvl(trim(mmca.bene_id), '') <> '' or nvl(trim(mmca.noms_type), '') <> '' or nvl(trim(mmca.bene_type), '') <> '' )
		 ) t1
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_2_1
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `replace_field_length` int
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_2_1
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_2_1
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,case when t.noms = 'T' then 'TEMPATAN'
      when t.noms = 'A' then 'ASING'
      else null
      end as nominees_type
	   ,least(length(kenanga_noms_list_1),
              length(kenanga_noms_list_2),
              length(kenanga_noms_list_3),
              length(kenanga_noms_list_4),
              length(kenanga_noms_list_5),
              length(kenanga_noms_list_6),
              length(kenanga_noms_list_7),
              length(kenanga_noms_list_8),
              length(kenanga_noms_list_9),
              length(kenanga_noms_list_10),
              length(kenanga_noms_list_11),
              length(kenanga_noms_list_12),
              length(kenanga_noms_list_13),
              length(kenanga_noms_list_14),
              length(kenanga_noms_list_15),
              length(kenanga_noms_list_16),
              length(kenanga_noms_list_17),
              length(kenanga_noms_list_18),
              length(kenanga_noms_list_19),
              length(kenanga_noms_list_20),
              length(kenanga_noms_list_21),
              length(kenanga_noms_list_22),
              length(kenanga_noms_list_23),
              length(kenanga_noms_list_24),
              length(kenanga_noms_list_25),
              length(kenanga_noms_list_26),
              length(kenanga_noms_list_27),
              length(kenanga_noms_list_28),
              length(kenanga_noms_list_29),
              length(kenanga_noms_list_30),
              length(kenanga_noms_list_31),
              length(kenanga_noms_list_32),
              length(kenanga_noms_list_33),
              length(kenanga_noms_list_34),
              length(kenanga_noms_list_35),
              length(kenanga_noms_list_36),
              length(kenanga_noms_list_37),
              length(kenanga_noms_list_38),
              length(kenanga_noms_list_39),
              length(kenanga_noms_list_40),
              length(kenanga_noms_list_41),
              length(kenanga_noms_list_42),
              length(kenanga_noms_list_43),
              length(kenanga_noms_list_44),
              length(kenanga_noms_list_45),
              length(kenanga_noms_list_46),
              length(kenanga_noms_list_47),
              length(kenanga_noms_list_48),
              length(kenanga_noms_list_49),
              length(kenanga_noms_list_50),
              length(kenanga_noms_list_51),
              length(kenanga_noms_list_52),
              length(kenanga_noms_list_53),
              length(kenanga_noms_list_54),
              length(kenanga_noms_list_55),
              length(kenanga_noms_list_56),
              length(kenanga_noms_list_57),
              length(kenanga_noms_list_58),
              length(kenanga_noms_list_59),
              length(kenanga_noms_list_60),
              length(kenanga_noms_list_61),
              length(kenanga_noms_list_62),
              length(kenanga_noms_list_63),
              length(kenanga_noms_list_64),
              length(kenanga_noms_list_65),
              length(kenanga_noms_list_66),
              length(kenanga_noms_list_67),
              length(kenanga_noms_list_68),
              length(kenanga_noms_list_69),
              length(kenanga_noms_list_70),
              length(kenanga_noms_list_71),
              length(kenanga_noms_list_72),
              length(kenanga_noms_list_73),
              length(kenanga_noms_list_74),
              length(kenanga_noms_list_75),
              length(kenanga_noms_list_76),
              length(kenanga_noms_list_77),
              length(kenanga_noms_list_78),
              length(kenanga_noms_list_79),
              length(kenanga_noms_list_80),
              length(kenanga_noms_list_81),
              length(kenanga_noms_list_82),
              length(kenanga_noms_list_83),
              length(kenanga_noms_list_84),
              length(kenanga_noms_list_85),
              length(kenanga_noms_list_86),
              length(kenanga_noms_list_87),
              length(kenanga_noms_list_88),
              length(kenanga_noms_list_89),
              length(kenanga_noms_list_90),
              length(kenanga_noms_list_91),
              length(kenanga_noms_list_92),
              length(kenanga_noms_list_93),
              length(kenanga_noms_list_94),
              length(kenanga_noms_list_95),
              length(kenanga_noms_list_96),
              length(kenanga_noms_list_97),
              length(kenanga_noms_list_98),
              length(kenanga_noms_list_99),
              length(kenanga_noms_list_100),
              length(kenanga_noms_list_101),
              length(kenanga_noms_list_102),
              length(kenanga_noms_list_103),
              length(kenanga_noms_list_104),
              length(kenanga_noms_list_105),
              length(kenanga_noms_list_106),
              length(kenanga_noms_list_107),
              length(kenanga_noms_list_108),
              length(kenanga_noms_list_109),
			  length(kenanga_noms_list_110),
			  length(kenanga_noms_list_111)
	   ) as replace_field_length
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_1 t
 -- where noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_2
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2(
  `client_no` string,
  `client_type` string, 
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `remove_kenanga_nominees_name` string
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_2
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_2;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_2
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_2
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,t1.nominees_type
       ,(case when length(t.kenanga_noms_list_1) = t1.replace_field_length then trim(t.kenanga_noms_list_1)
              when length(t.kenanga_noms_list_2) = t1.replace_field_length then trim(t.kenanga_noms_list_2)
              when length(t.kenanga_noms_list_3) = t1.replace_field_length then trim(t.kenanga_noms_list_3)
              when length(t.kenanga_noms_list_4) = t1.replace_field_length then trim(t.kenanga_noms_list_4)
              when length(t.kenanga_noms_list_5) = t1.replace_field_length then trim(t.kenanga_noms_list_5)
              when length(t.kenanga_noms_list_6) = t1.replace_field_length then trim(t.kenanga_noms_list_6)
              when length(t.kenanga_noms_list_7) = t1.replace_field_length then trim(t.kenanga_noms_list_7)
              when length(t.kenanga_noms_list_8) = t1.replace_field_length then trim(t.kenanga_noms_list_8)
              when length(t.kenanga_noms_list_9) = t1.replace_field_length then trim(t.kenanga_noms_list_9)
              when length(t.kenanga_noms_list_10) = t1.replace_field_length then trim(t.kenanga_noms_list_10)
              when length(t.kenanga_noms_list_11) = t1.replace_field_length then trim(t.kenanga_noms_list_11)
              when length(t.kenanga_noms_list_12) = t1.replace_field_length then trim(t.kenanga_noms_list_12)
              when length(t.kenanga_noms_list_13) = t1.replace_field_length then trim(t.kenanga_noms_list_13)
              when length(t.kenanga_noms_list_14) = t1.replace_field_length then trim(t.kenanga_noms_list_14)
              when length(t.kenanga_noms_list_15) = t1.replace_field_length then trim(t.kenanga_noms_list_15)
              when length(t.kenanga_noms_list_16) = t1.replace_field_length then trim(t.kenanga_noms_list_16)
              when length(t.kenanga_noms_list_17) = t1.replace_field_length then trim(t.kenanga_noms_list_17)
              when length(t.kenanga_noms_list_18) = t1.replace_field_length then trim(t.kenanga_noms_list_18)
              when length(t.kenanga_noms_list_19) = t1.replace_field_length then trim(t.kenanga_noms_list_19)
              when length(t.kenanga_noms_list_20) = t1.replace_field_length then trim(t.kenanga_noms_list_20)
              when length(t.kenanga_noms_list_21) = t1.replace_field_length then trim(t.kenanga_noms_list_21)
              when length(t.kenanga_noms_list_22) = t1.replace_field_length then trim(t.kenanga_noms_list_22)
              when length(t.kenanga_noms_list_23) = t1.replace_field_length then trim(t.kenanga_noms_list_23)
              when length(t.kenanga_noms_list_24) = t1.replace_field_length then trim(t.kenanga_noms_list_24)
              when length(t.kenanga_noms_list_25) = t1.replace_field_length then trim(t.kenanga_noms_list_25)
              when length(t.kenanga_noms_list_26) = t1.replace_field_length then trim(t.kenanga_noms_list_26)
              when length(t.kenanga_noms_list_27) = t1.replace_field_length then trim(t.kenanga_noms_list_27)
              when length(t.kenanga_noms_list_28) = t1.replace_field_length then trim(t.kenanga_noms_list_28)
              when length(t.kenanga_noms_list_29) = t1.replace_field_length then trim(t.kenanga_noms_list_29)
              when length(t.kenanga_noms_list_30) = t1.replace_field_length then trim(t.kenanga_noms_list_30)
              when length(t.kenanga_noms_list_31) = t1.replace_field_length then trim(t.kenanga_noms_list_31)
              when length(t.kenanga_noms_list_32) = t1.replace_field_length then trim(t.kenanga_noms_list_32)
              when length(t.kenanga_noms_list_33) = t1.replace_field_length then trim(t.kenanga_noms_list_33)
              when length(t.kenanga_noms_list_34) = t1.replace_field_length then trim(t.kenanga_noms_list_34)
              when length(t.kenanga_noms_list_35) = t1.replace_field_length then trim(t.kenanga_noms_list_35)
              when length(t.kenanga_noms_list_36) = t1.replace_field_length then trim(t.kenanga_noms_list_36)
              when length(t.kenanga_noms_list_37) = t1.replace_field_length then trim(t.kenanga_noms_list_37)
              when length(t.kenanga_noms_list_38) = t1.replace_field_length then trim(t.kenanga_noms_list_38)
              when length(t.kenanga_noms_list_39) = t1.replace_field_length then trim(t.kenanga_noms_list_39)
              when length(t.kenanga_noms_list_40) = t1.replace_field_length then trim(t.kenanga_noms_list_40)
              when length(t.kenanga_noms_list_41) = t1.replace_field_length then trim(t.kenanga_noms_list_41)
              when length(t.kenanga_noms_list_42) = t1.replace_field_length then trim(t.kenanga_noms_list_42)
              when length(t.kenanga_noms_list_43) = t1.replace_field_length then trim(t.kenanga_noms_list_43)
              when length(t.kenanga_noms_list_44) = t1.replace_field_length then trim(t.kenanga_noms_list_44)
              when length(t.kenanga_noms_list_45) = t1.replace_field_length then trim(t.kenanga_noms_list_45)
              when length(t.kenanga_noms_list_46) = t1.replace_field_length then trim(t.kenanga_noms_list_46)
              when length(t.kenanga_noms_list_47) = t1.replace_field_length then trim(t.kenanga_noms_list_47)
              when length(t.kenanga_noms_list_48) = t1.replace_field_length then trim(t.kenanga_noms_list_48)
              when length(t.kenanga_noms_list_49) = t1.replace_field_length then trim(t.kenanga_noms_list_49)
              when length(t.kenanga_noms_list_50) = t1.replace_field_length then trim(t.kenanga_noms_list_50)
              when length(t.kenanga_noms_list_51) = t1.replace_field_length then trim(t.kenanga_noms_list_51)
              when length(t.kenanga_noms_list_52) = t1.replace_field_length then trim(t.kenanga_noms_list_52)
              when length(t.kenanga_noms_list_53) = t1.replace_field_length then trim(t.kenanga_noms_list_53)
              when length(t.kenanga_noms_list_54) = t1.replace_field_length then trim(t.kenanga_noms_list_54)
              when length(t.kenanga_noms_list_55) = t1.replace_field_length then trim(t.kenanga_noms_list_55)
              when length(t.kenanga_noms_list_56) = t1.replace_field_length then trim(t.kenanga_noms_list_56)
              when length(t.kenanga_noms_list_57) = t1.replace_field_length then trim(t.kenanga_noms_list_57)
              when length(t.kenanga_noms_list_58) = t1.replace_field_length then trim(t.kenanga_noms_list_58)
              when length(t.kenanga_noms_list_59) = t1.replace_field_length then trim(t.kenanga_noms_list_59)
              when length(t.kenanga_noms_list_60) = t1.replace_field_length then trim(t.kenanga_noms_list_60)
              when length(t.kenanga_noms_list_61) = t1.replace_field_length then trim(t.kenanga_noms_list_61)
              when length(t.kenanga_noms_list_62) = t1.replace_field_length then trim(t.kenanga_noms_list_62)
              when length(t.kenanga_noms_list_63) = t1.replace_field_length then trim(t.kenanga_noms_list_63)
              when length(t.kenanga_noms_list_64) = t1.replace_field_length then trim(t.kenanga_noms_list_64)
              when length(t.kenanga_noms_list_65) = t1.replace_field_length then trim(t.kenanga_noms_list_65)
              when length(t.kenanga_noms_list_66) = t1.replace_field_length then trim(t.kenanga_noms_list_66)
              when length(t.kenanga_noms_list_67) = t1.replace_field_length then trim(t.kenanga_noms_list_67)
              when length(t.kenanga_noms_list_68) = t1.replace_field_length then trim(t.kenanga_noms_list_68)
              when length(t.kenanga_noms_list_69) = t1.replace_field_length then trim(t.kenanga_noms_list_69)
              when length(t.kenanga_noms_list_70) = t1.replace_field_length then trim(t.kenanga_noms_list_70)
              when length(t.kenanga_noms_list_71) = t1.replace_field_length then trim(t.kenanga_noms_list_71)
              when length(t.kenanga_noms_list_72) = t1.replace_field_length then trim(t.kenanga_noms_list_72)
              when length(t.kenanga_noms_list_73) = t1.replace_field_length then trim(t.kenanga_noms_list_73)
              when length(t.kenanga_noms_list_74) = t1.replace_field_length then trim(t.kenanga_noms_list_74)
              when length(t.kenanga_noms_list_75) = t1.replace_field_length then trim(t.kenanga_noms_list_75)
              when length(t.kenanga_noms_list_76) = t1.replace_field_length then trim(t.kenanga_noms_list_76)
              when length(t.kenanga_noms_list_77) = t1.replace_field_length then trim(t.kenanga_noms_list_77)
              when length(t.kenanga_noms_list_78) = t1.replace_field_length then trim(t.kenanga_noms_list_78)
              when length(t.kenanga_noms_list_79) = t1.replace_field_length then trim(t.kenanga_noms_list_79)
              when length(t.kenanga_noms_list_80) = t1.replace_field_length then trim(t.kenanga_noms_list_80)
              when length(t.kenanga_noms_list_81) = t1.replace_field_length then trim(t.kenanga_noms_list_81)
              when length(t.kenanga_noms_list_82) = t1.replace_field_length then trim(t.kenanga_noms_list_82)
              when length(t.kenanga_noms_list_83) = t1.replace_field_length then trim(t.kenanga_noms_list_83)
              when length(t.kenanga_noms_list_84) = t1.replace_field_length then trim(t.kenanga_noms_list_84)
              when length(t.kenanga_noms_list_85) = t1.replace_field_length then trim(t.kenanga_noms_list_85)
              when length(t.kenanga_noms_list_86) = t1.replace_field_length then trim(t.kenanga_noms_list_86)
              when length(t.kenanga_noms_list_87) = t1.replace_field_length then trim(t.kenanga_noms_list_87)
              when length(t.kenanga_noms_list_88) = t1.replace_field_length then trim(t.kenanga_noms_list_88)
              when length(t.kenanga_noms_list_89) = t1.replace_field_length then trim(t.kenanga_noms_list_89)
              when length(t.kenanga_noms_list_90) = t1.replace_field_length then trim(t.kenanga_noms_list_90)
              when length(t.kenanga_noms_list_91) = t1.replace_field_length then trim(t.kenanga_noms_list_91)
              when length(t.kenanga_noms_list_92) = t1.replace_field_length then trim(t.kenanga_noms_list_92)
              when length(t.kenanga_noms_list_93) = t1.replace_field_length then trim(t.kenanga_noms_list_93)
              when length(t.kenanga_noms_list_94) = t1.replace_field_length then trim(t.kenanga_noms_list_94)
              when length(t.kenanga_noms_list_95) = t1.replace_field_length then trim(t.kenanga_noms_list_95)
              when length(t.kenanga_noms_list_96) = t1.replace_field_length then trim(t.kenanga_noms_list_96)
              when length(t.kenanga_noms_list_97) = t1.replace_field_length then trim(t.kenanga_noms_list_97)
              when length(t.kenanga_noms_list_98) = t1.replace_field_length then trim(t.kenanga_noms_list_98)
              when length(t.kenanga_noms_list_99) = t1.replace_field_length then trim(t.kenanga_noms_list_99)
              when length(t.kenanga_noms_list_100) = t1.replace_field_length then trim(t.kenanga_noms_list_100)
              when length(t.kenanga_noms_list_101) = t1.replace_field_length then trim(t.kenanga_noms_list_101)
              when length(t.kenanga_noms_list_102) = t1.replace_field_length then trim(t.kenanga_noms_list_102)
              when length(t.kenanga_noms_list_103) = t1.replace_field_length then trim(t.kenanga_noms_list_103)
              when length(t.kenanga_noms_list_104) = t1.replace_field_length then trim(t.kenanga_noms_list_104)
              when length(t.kenanga_noms_list_105) = t1.replace_field_length then trim(t.kenanga_noms_list_105)
              when length(t.kenanga_noms_list_106) = t1.replace_field_length then trim(t.kenanga_noms_list_106)
              when length(t.kenanga_noms_list_107) = t1.replace_field_length then trim(t.kenanga_noms_list_107)
              when length(t.kenanga_noms_list_108) = t1.replace_field_length then trim(t.kenanga_noms_list_108)
			  when length(t.kenanga_noms_list_109) = t1.replace_field_length then trim(t.kenanga_noms_list_109)
			  when length(t.kenanga_noms_list_110) = t1.replace_field_length then trim(t.kenanga_noms_list_110)
			  when length(t.kenanga_noms_list_111) = t1.replace_field_length then trim(t.kenanga_noms_list_111)
              else trim(t.customer_name_concatenate)
          end ) as remove_kenanga_nominees_name
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_1 t
  left join ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1 t1
    on t.client_no = t1.client_no
 -- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_3
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_3(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `remove_kenanga_nominees_name` string,
  `pledged_name_list_1` string,
  `pledged_name_list_2` string,
  `pledged_name_list_39` string, --add_20240611
  `pledged_name_list_3` string,
  `pledged_name_list_4` string,
  `pledged_name_list_5` string,
  `pledged_name_list_6` string,
  `pledged_name_list_7` string,
  `pledged_name_list_8` string,
  `pledged_name_list_9` string,
  `pledged_name_list_42` string, --add_20240620
  `pledged_name_list_10` string,
  `pledged_name_list_11` string,
  `pledged_name_list_12` string,
  `pledged_name_list_13` string,
  `pledged_name_list_14` string,
  `pledged_name_list_15` string,
  `pledged_name_list_16` string,
  `pledged_name_list_17` string,
  `pledged_name_list_18` string,
  `pledged_name_list_41` string, --add_20240828
  `pledged_name_list_19` string,
  `pledged_name_list_20` string,
  `pledged_name_list_21` string,
  `pledged_name_list_22` string,
  `pledged_name_list_23` string,
  `pledged_name_list_24` string,
  `pledged_name_list_25` string,
  `pledged_name_list_26` string,
  `pledged_name_list_27` string,
  `pledged_name_list_28` string,
  `pledged_name_list_29` string,
  `pledged_name_list_30` string,
  `pledged_name_list_31` string,
  `pledged_name_list_32` string,
  `pledged_name_list_33` string,
  `pledged_name_list_34` string,
  `pledged_name_list_35` string,
  `pledged_name_list_36` string, --add_20240321
  `pledged_name_list_37` string, --add_20240321
  `pledged_name_list_38` string, --add_20240518
  `pledged_name_list_40` string --add_20240828
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_3
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_3;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_3
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_3
select t.client_no,
       t.client_type,
       t.client_name,
       t.client_name1,
       t.client_name2,
       t.client_name3,
       t.customer_name_concatenate,
	   t.noms_ind,
	   t.nominees_type,
       t.remove_kenanga_nominees_name,
       replace(t.remove_kenanga_nominees_name, '(PLEDGED)', '') as pledged_name_list_1,
       replace(t.remove_kenanga_nominees_name, '(PLEDGE)', '') as pledged_name_list_2,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES A/C FOR PLEDGED SECURITIES ACCOUNT', '') as pledged_name_list_39, --add_20240611
       replace(t.remove_kenanga_nominees_name, 'PLEGDED SECURITIES ACCOUNT', '') as pledged_name_list_3,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITISED ACCOUNT', '') as pledged_name_list_4,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SDN BHD', '') as pledged_name_list_5,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES SA/C', '') as pledged_name_list_6,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') as pledged_name_list_7,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') as pledged_name_list_8,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCT', '') as pledged_name_list_9,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT - ', '') as pledged_name_list_42, --add_20250620
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') as pledged_name_list_10,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT', '') as pledged_name_list_11,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCOUNT ', '') as pledged_name_list_12,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACCCOUNT', '') as pledged_name_list_13,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES ACC', '') as pledged_name_list_14,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES A/C', '') as pledged_name_list_15,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES  ACCOUNT', '') as pledged_name_list_16,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITIES', '') as pledged_name_list_17,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURITES ACCOUNT', '') as pledged_name_list_18,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECURUTIES ACCOUNT', '') as pledged_name_list_41, --add_20240828
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECS ACCOUNT', '') as pledged_name_list_19,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SECS A/C', '') as pledged_name_list_20,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SEC. A/C', '') as pledged_name_list_21,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SEC', '') as pledged_name_list_22,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SEC A/C ', '') as pledged_name_list_23,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED SEC  A/C', '') as pledged_name_list_24,
       replace(t.remove_kenanga_nominees_name, 'PLEDGED', '') as pledged_name_list_25,
       replace(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES ACCOUNT', '') as pledged_name_list_26,
       replace(t.remove_kenanga_nominees_name, 'PLEDGE SECURITIES A/C', '') as pledged_name_list_27,
       replace(t.remove_kenanga_nominees_name, 'PLEDGE SEC A/C', '') as pledged_name_list_28,
       replace(t.remove_kenanga_nominees_name, 'PLEDGE', '') as pledged_name_list_29,
       replace(t.remove_kenanga_nominees_name, 'PLED GED SECURITIES ACCOUNT', '') as pledged_name_list_30,
       replace(t.remove_kenanga_nominees_name, 'PLDG SEC', '') as pledged_name_list_31,
       replace(t.remove_kenanga_nominees_name, 'PLDG SEC AC ', '') as pledged_name_list_32,
       replace(t.remove_kenanga_nominees_name, 'PLDG SEC A/C ', '') as pledged_name_list_33,
       replace(t.remove_kenanga_nominees_name, 'PLDG A/C', '') as pledged_name_list_34,
       replace(t.remove_kenanga_nominees_name, 'PL SEC A/C - ', '') as pledged_name_list_35,
	     replace(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES ACCOUNT', '') as pledged_name_list_36,		--add_20240321
	     replace(t.remove_kenanga_nominees_name, 'PLEGED SECURITIES A/C', '') as pledged_name_list_37,				--add_20240321
       replace(t.remove_kenanga_nominees_name, 'PLEDEGD SECURITIES ACCOUNT', '') as pledged_name_list_38, --add_20240518
       replace(t.remove_kenanga_nominees_name, 'PLGD SEC ACC', '') as pledged_name_list_40 --add_20240828
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_2 t
-- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_4_1
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `remove_pledged_name_length` int
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_4_1
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4_1
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,t.nominees_type
	   ,least(length(pledged_name_list_1),
              length(pledged_name_list_2),
              length(pledged_name_list_3),
              length(pledged_name_list_4),
              length(pledged_name_list_5),
              length(pledged_name_list_6),
              length(pledged_name_list_7),
              length(pledged_name_list_8),
              length(pledged_name_list_9),
              length(pledged_name_list_10),
              length(pledged_name_list_11),
              length(pledged_name_list_12),
              length(pledged_name_list_13),
              length(pledged_name_list_14),
              length(pledged_name_list_15),
              length(pledged_name_list_16),
              length(pledged_name_list_17),
              length(pledged_name_list_18),
              length(pledged_name_list_19),
              length(pledged_name_list_20),
              length(pledged_name_list_21),
              length(pledged_name_list_22),
              length(pledged_name_list_23),
              length(pledged_name_list_24),
              length(pledged_name_list_25),
              length(pledged_name_list_26),
              length(pledged_name_list_27),
              length(pledged_name_list_28),
              length(pledged_name_list_29),
              length(pledged_name_list_30),
              length(pledged_name_list_31),
              length(pledged_name_list_32),
              length(pledged_name_list_33),
              length(pledged_name_list_34),
              length(pledged_name_list_35),
              length(pledged_name_list_36),    -- add_20240321
              length(pledged_name_list_37), 	 -- add_20240321
              length(pledged_name_list_38),  	 -- add_20240518    
              length(pledged_name_list_39),  	 -- add_20240611 
              length(pledged_name_list_40),  	 -- add_20240828      
              length(pledged_name_list_41),  	 -- add_20240828         
              length(pledged_name_list_42)  	 -- add_20250620                     
       ) as remove_pledged_name_length
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_3 t
-- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_4
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `pledged_securities_flag` string,
  `remove_kenanga_nominees_name` string,
  `remove_pledged_name` string
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_4
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_4;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_4
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_4
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,t.nominees_type
	   ,(case when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_1) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_2) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_3) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_4) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_5) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_6) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_7) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_8) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_9) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_10) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_11) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_12) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_13) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_14) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_15) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_16) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_17) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_18) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_19) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_20) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_21) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_22) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_23) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_24) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_25) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_26) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_27) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_28) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_29) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_30) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_31) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_32) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_33) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_34) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_35) then 'Y'
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_36) then 'Y'	--add_20240321
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_37) then 'Y'	--add_20240321
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_38) then 'Y'	--add_20240518  
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_39) then 'Y'	--add_20240611
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_40) then 'Y'	--add_20240828   
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_41) then 'Y'	--add_20240828          
              when length(t.remove_kenanga_nominees_name) <> length(t.pledged_name_list_42) then 'Y'	--add_20250620   
              else 'N'
          end ) as pledged_securities_flag
       ,t.remove_kenanga_nominees_name
       ,(case when length(t.pledged_name_list_1) = t1.remove_pledged_name_length then trim(t.pledged_name_list_1)
              when length(t.pledged_name_list_2) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_2)
              when length(t.pledged_name_list_3) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_3)
              when length(t.pledged_name_list_4) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_4)
              when length(t.pledged_name_list_5) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_5)
              when length(t.pledged_name_list_6) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_6)
              when length(t.pledged_name_list_7) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_7)
              when length(t.pledged_name_list_8) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_8)
              when length(t.pledged_name_list_9) = t1.remove_pledged_name_length  then trim(t.pledged_name_list_9)
              when length(t.pledged_name_list_10) = t1.remove_pledged_name_length then trim(t.pledged_name_list_10)
              when length(t.pledged_name_list_11) = t1.remove_pledged_name_length then trim(t.pledged_name_list_11)
              when length(t.pledged_name_list_12) = t1.remove_pledged_name_length then trim(t.pledged_name_list_12)
              when length(t.pledged_name_list_13) = t1.remove_pledged_name_length then trim(t.pledged_name_list_13)
              when length(t.pledged_name_list_14) = t1.remove_pledged_name_length then trim(t.pledged_name_list_14)
              when length(t.pledged_name_list_15) = t1.remove_pledged_name_length then trim(t.pledged_name_list_15)
              when length(t.pledged_name_list_16) = t1.remove_pledged_name_length then trim(t.pledged_name_list_16)
              when length(t.pledged_name_list_17) = t1.remove_pledged_name_length then trim(t.pledged_name_list_17)
              when length(t.pledged_name_list_18) = t1.remove_pledged_name_length then trim(t.pledged_name_list_18)
              when length(t.pledged_name_list_19) = t1.remove_pledged_name_length then trim(t.pledged_name_list_19)
              when length(t.pledged_name_list_20) = t1.remove_pledged_name_length then trim(t.pledged_name_list_20)
              when length(t.pledged_name_list_21) = t1.remove_pledged_name_length then trim(t.pledged_name_list_21)
              when length(t.pledged_name_list_22) = t1.remove_pledged_name_length then trim(t.pledged_name_list_22)
              when length(t.pledged_name_list_23) = t1.remove_pledged_name_length then trim(t.pledged_name_list_23)
              when length(t.pledged_name_list_24) = t1.remove_pledged_name_length then trim(t.pledged_name_list_24)
              when length(t.pledged_name_list_25) = t1.remove_pledged_name_length then trim(t.pledged_name_list_25)
              when length(t.pledged_name_list_26) = t1.remove_pledged_name_length then trim(t.pledged_name_list_26)
              when length(t.pledged_name_list_27) = t1.remove_pledged_name_length then trim(t.pledged_name_list_27)
              when length(t.pledged_name_list_28) = t1.remove_pledged_name_length then trim(t.pledged_name_list_28)
              when length(t.pledged_name_list_29) = t1.remove_pledged_name_length then trim(t.pledged_name_list_29)
              when length(t.pledged_name_list_30) = t1.remove_pledged_name_length then trim(t.pledged_name_list_30)
              when length(t.pledged_name_list_31) = t1.remove_pledged_name_length then trim(t.pledged_name_list_31)
              when length(t.pledged_name_list_32) = t1.remove_pledged_name_length then trim(t.pledged_name_list_32)
              when length(t.pledged_name_list_33) = t1.remove_pledged_name_length then trim(t.pledged_name_list_33)
              when length(t.pledged_name_list_34) = t1.remove_pledged_name_length then trim(t.pledged_name_list_34)
              when length(t.pledged_name_list_35) = t1.remove_pledged_name_length then trim(t.pledged_name_list_35)
              when length(t.pledged_name_list_36) = t1.remove_pledged_name_length then trim(t.pledged_name_list_36)			--add_20240321
              when length(t.pledged_name_list_37) = t1.remove_pledged_name_length then trim(t.pledged_name_list_37)			--add_20240321
              when length(t.pledged_name_list_38) = t1.remove_pledged_name_length then trim(t.pledged_name_list_38)			--add_20240518
              when length(t.pledged_name_list_39) = t1.remove_pledged_name_length then trim(t.pledged_name_list_39)			--add_20240611
              when length(t.pledged_name_list_40) = t1.remove_pledged_name_length then trim(t.pledged_name_list_40)			--add_20240828
              when length(t.pledged_name_list_41) = t1.remove_pledged_name_length then trim(t.pledged_name_list_41)			--add_20240828
              when length(t.pledged_name_list_42) = t1.remove_pledged_name_length then trim(t.pledged_name_list_42)			--add_20250620
              else trim(t.remove_kenanga_nominees_name)
          end ) as remove_pledged_name
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_3 t
  left join ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1 t1
    on t.client_no = t1.client_no
-- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info_5
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_5(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `pledged_securities_flag` string,
  `remove_kenanga_nominees_name` string,
  `remove_pledged_name` string,
  `replace_dsal_handling` string,
  `cleaned_nominees_name` string
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_5
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info_5;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info_5
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info_5
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,t.nominees_type
	   ,t.pledged_securities_flag
       ,t.remove_kenanga_nominees_name
       ,t.remove_pledged_name
       ,regexp_replace(t.remove_pledged_name, '(^| )DSAL( |$)', ' ') as replace_dsal_handling
	   ,trim(replace(replace(replace(replace(regexp_replace(trim(t.remove_pledged_name), '^FOR ', ''),
	            'FOR EXEMPT AN FOR', 'FOR'),
				'EXEMPT AN FOR', 'FOR'),
				'RSS/SBL EXEMPT AN FOR', 'FOR'),
				'RSS/SBL FOR', 'FOR')) as cleaned_nominees_name
  from ${com_schema}.temp_t_mhbos_m_client_nominees_info_4 t
-- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_nominees_info
create table if not exists ${com_schema}.temp_t_mhbos_m_client_nominees_info(
  `client_no` string,
  `client_type` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `noms_ind` string,
  `nominees_type` string,
  `pledged_securities_flag` string,
  `remove_kenanga_nominees_name` string,
  `remove_pledged_name` string,
  `replace_dsal_handling` string,
  `cleaned_nominees_name` string,
  `principal_name` string,
  `intermediary_name` string,
  `beneficiary_name` string
);

-- trunate temporary table temp_t_mhbos_m_client_nominees_info_5
truncate table ${com_schema}.temp_t_mhbos_m_client_nominees_info;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info
insert into table ${com_schema}.temp_t_mhbos_m_client_nominees_info
select t.client_no
       ,t.client_type
       ,t.client_name
       ,t.client_name1
       ,t.client_name2
       ,t.client_name3
       ,t.customer_name_concatenate
	   ,t.noms_ind
	   ,t.nominees_type
	   ,t.pledged_securities_flag
       ,t.remove_kenanga_nominees_name
       ,t.remove_pledged_name
       ,t.replace_dsal_handling
	   ,t.cleaned_nominees_name
	   ,(case when t.client_type in ('#', '0', '1', '6', '8', 'V') then t.cleaned_nominees_name
          when t.cleaned_nominees_name rlike '^(.+) FOR (.+) FOR (.+)$' then
                trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 1))
              when t.cleaned_nominees_name rlike '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$' then
                t.cleaned_nominees_name
              when t.cleaned_nominees_name rlike '^(.+) FOR (.+)$' then
                trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR (.+)$', 1))
              else t.cleaned_nominees_name
          end) as principal_name
	   ,(case when t.client_type in ('#', '0', '1', '6', '8', 'V') then null
            when t.cleaned_nominees_name rlike '^(.+) FOR[ ]+([^ ]+)[ ]+FOR[ ]+([^ ]+)$' then null
	          when t.cleaned_nominees_name rlike '^(.+) FOR (.+) FOR (.+)$' then trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 2))
			  else null
		  end) as intermediary_name
	   ,(case when t.client_type in ('#', '0', '1', '6', '8', 'V') then null
              when t.cleaned_nominees_name rlike '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$' then
                trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR[ ]+([^ ]+[ ]+FOR[ ]+[^ ]+)$', 2))
              when t.cleaned_nominees_name rlike '^(.+) FOR (.+) FOR (.+)$' then
                trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR (.+) FOR (.+)$', 3))
              when t.cleaned_nominees_name rlike '^([^ ]+)[ ]+FOR[ ]+([^ ]+)$' then null
              when t.cleaned_nominees_name rlike '^(.+) FOR (.+)$' then
                trim(regexp_extract(t.cleaned_nominees_name, '^(.+) FOR (.+)$', 2))
              else null
          end) as beneficiary_name
  from (select client_no
               ,client_type
               ,client_name
               ,client_name1
               ,client_name2
               ,client_name3
               ,customer_name_concatenate
	           ,noms_ind
	           ,nominees_type
	           ,pledged_securities_flag
               ,remove_kenanga_nominees_name
               ,remove_pledged_name
               ,replace_dsal_handling
	           ,(case when cleaned_nominees_name like 'FOR %' then substr(cleaned_nominees_name, 5)
			          else cleaned_nominees_name
			      end) as cleaned_nominees_name
          from ${com_schema}.temp_t_mhbos_m_client_nominees_info_5)	t
-- where t.noms_ind = 'Y'
;

-- create temporary table temp_t_mhbos_m_client_name_info 				Add 20240321
drop table if exists ${com_schema}.temp_t_mhbos_m_client_name_info;
create table if not exists ${com_schema}.temp_t_mhbos_m_client_name_info(
  `client_no` string,
  `client_name` string,
  `client_name1` string,
  `client_name2` string,
  `client_name3` string,
  `customer_name_concatenate` string,
  `org_customer_name`  string,
  `customer_name` string,
  `customer_name_flag` string,
  `org_primary_identification_type` string,
  `primary_identification_type` string,
  `org_primary_identification_no` string,
  `primary_identification_no`  string,
  `org_secondary_identification_type` string,
  `secondary_identification_type` string,
  `org_secondary_identification_no` string,
  `secondary_identification_no`  string,
  `org_principal_name` string,
  `principal_name`  string,
  `org_beneficiary_name` string,
  `beneficiary_name`  string
);

-- trunate temporary table temp_t_mhbos_m_client_name_info
truncate table ${com_schema}.temp_t_mhbos_m_client_name_info;

-- Insert the processed data into the temporary table temp_t_mhbos_m_client_nominees_info		add 20240321
insert into table ${com_schema}.temp_t_mhbos_m_client_name_info
select 	
      t2.client_no
	    ,t2.client_name
      ,t2.client_name1
      ,t2.client_name2
      ,t2.client_name3
	    ,t2.customer_name_concatenate
	    ,t2.org_customer_name
        -- 2025/09/08: Add customer_name_flag
        , case when trim(coalesce(t2.Customer_Name, '')) <> ''
            then trim(coalesce(t2.Customer_Name, ''))
            else '@[' ||trim(coalesce(t2.Customer_Name, '')) || ']'
        end as customer_name
        -- 2025/09/08: Add customer_name_flag
    , case when trim(coalesce(t2.Customer_Name, '')) <> ''
        then '0'
        else '1'
        end as customer_name_flag
      ,t2.primary_identification_type as org_primary_identification_type -- added 20250313
      ,case 
        when l1.primary_id_type is not null then l1.primary_id_type -- added 20250313
        else t2.primary_identification_type 
      end as primary_identification_type
	    ,t2.org_primary_identification_no
	    ,case 
        when l1.primary_id_no is not null then l1.primary_id_no
		    else t2.primary_identification_no 
      end as primary_identification_no
      ,t2.secondary_identification_type as org_secondary_identification_type -- added 20250620
      ,case 
        when l1.secondary_id_type is not null then l1.secondary_id_type -- added 20250620
        else t2.secondary_identification_type 
      end as secondary_identification_type
      ,t2.secondary_identification_no as org_secondary_identification_no -- added 20250620
      ,case 
        when l1.secondary_id_no is not null then l1.secondary_id_no -- added 20250620
		    else t2.secondary_identification_no 
      end as secondary_identification_no
      ,t2.org_principal_name
      ,t2.principal_name
      ,t2.org_beneficiary_name
      ,t2.beneficiary_name
from (
        select 	t1.client_no
               ,t1.client_name
               ,t1.client_name1
               ,t1.client_name2
               ,t1.client_name3
               ,t1.customer_name_concatenate
               ,t1.customer_name as org_customer_name
               ,trim(case 
                      when l2.cust_name is not null then l2.cust_name -- add new mapping 20240828
                      when l3.new_cust_name is not null then l3.new_cust_name -- add new mapping 20240828
                      when l4.new_cust_name is not null then l4.new_cust_name -- add new mapping 20250313
                      when l5.cust_name is not null then l5.cust_name -- add new mapping 20250620
                      when t1.customer_name_concatenate like '%RSS/SBL FOR KIBB%' then regexp_extract(t1.customer_name, '\\(([^)]+)\\)', 1) -- added logic 20240815
                      when t1.customer_name_concatenate like '%RSS/ SBL FOR KIBB%' then regexp_extract(t1.customer_name, '\\(([^)]+)\\)', 1) -- added logic 20240828
                      when t1.customer_name_concatenate like '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%' then regexp_extract(t1.customer_name, '\\(([^)]+)\\)', 1) -- added logic 20240828
                      when t1.customer_name rlike '.+\\(.*\\)$' then  regexp_replace(regexp_replace(t1.customer_name,'\\s*\\([^)]*\\)$',''),'^(SMT\\s+|INTRADAY A/C\\s+)','') --add 20240518 --20241118 changes
                      when t1.customer_name_concatenate like '%SHARE BUY%' then regexp_replace(t1.customer_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') -- added logic 20240828
                else regexp_replace(t1.customer_name,'^(SMT\\s+|INTRADAY A/C\\s+)','') 
                end) as customer_name
               ,t1.primary_identification_type -- added 20250313
               ,t1.primary_identification_no as org_primary_identification_no
               ,t1.primary_identification_no
               ,t1.secondary_identification_type -- added 20250620
               ,t1.secondary_identification_no -- added 20250620
               ,t1.principal_name as org_principal_name
               ,trim(case when t1.customer_name_concatenate like '%RSS/SBL FOR KIBB%' then regexp_extract(t1.principal_name, '\\(([^)]+)\\)', 1) --added logic 20240815
                when t1.customer_name_concatenate like '%RSS/ SBL FOR KIBB%' then regexp_extract(t1.principal_name, '\\(([^)]+)\\)', 1) -- added logic 20240828
                when t1.customer_name_concatenate like '%RSS/SBL FOR KENANGA INVESTMENT BANK BERHAD%' then regexp_extract(t1.principal_name, '\\(([^)]+)\\)', 1) -- added logic 20240828
                when t1.principal_name rlike '.+\\(.*\\)$' then regexp_replace(t1.principal_name,'\\s*\\([^)]*\\)$','') --add 20240518 --20241118 changes
                when t1.customer_name_concatenate like '%SHARE BUY%' then regexp_replace(t1.principal_name, '(SHARE.*|-SHARE.*|- SHARE.*|"SHARE.*)$', '') -- added logic 20240828
                else t1.principal_name end) as principal_name    
               ,t1.beneficiary_name as org_beneficiary_name
               ,case when t1.beneficiary_name rlike '.+\\(.*\\)$' then regexp_replace(t1.beneficiary_name,'\\s*\\([^)]*\\)$','') else t1.beneficiary_name end as  beneficiary_name  --add 20240518 --20241118 changes
        from (
        select
                mmca.client_no
               ,cn.client_name
               ,cn.client_name1
               ,cn.client_name2
               ,cn.client_name3
               ,cn.customer_name_concatenate
               ,cn.customer_name as org_customer_name
  --             ,case when nvl(nom.noms_ind, 'Y') = 'Y' and nvl(trim(nom.principal_name), '') <> '' then nom.principal_name
                ,case when nvl(trim(nom.principal_name), '') <> '' then nom.principal_name
                      when nvl(trim(cn.customer_name), '') = '' and nvl(trim(cn.client_name), '') <> '' then cn.client_name
                      when nvl(trim(cn.customer_name), '') = '' and nvl(trim(cn.client_name), '') = '' then '@[]'
                      else cn.customer_name end as customer_name
               ,id.primary_identification_type -- added 20250313
               ,id.primary_identification_no
               ,id.secondary_identification_type -- added 20250620
               ,id.secondary_identification_no -- added 20250620
               ,nom.principal_name
               ,nom.beneficiary_name
          from ${com_schema}.temp_t_mhbos_m_client_all mmca
          left join ${com_schema}.temp_t_mhbos_m_client_customer_name_2 cn
            on mmca.client_no = cn.client_no
          left join ${com_schema}.temp_t_mhbos_m_client_nominees_info nom
            on mmca.client_no = nom.client_no
          left join ${com_schema}.temp_t_mhbos_m_client_identification_info id
            on mmca.client_no = id.client_no
        ) t1
        left join ${com_schema}.r_lookup_clientno_custname l2
          on t1.client_no = l2.client_no
          and l2.source_system = 'MHBOS'
        left join ${com_schema}.r_lookup_custname_custname l3
          on t1.customer_name = l3.cust_name
          and l3.source_system = 'MHBOS'
        left join ${com_schema}.r_lookup_primaryidno_newcustname l4 -- added 20250313
          on t1.primary_identification_no = l4.primary_id_no
          and l4.source_system = 'MHBOS'
        left join ${com_schema}.r_lookup_custname_primaryidno l5 -- added 20250620
          on t1.primary_identification_no = l5.primary_id_no
          and l5.source_system = 'MHBOS'
    ) t2
    left join ${com_schema}.r_lookup_custname_primaryidno l1
      on t2.customer_name = l1.cust_name
      and l1.source_system = 'MHBOS'
;

-- 3.1 Clear the day's data
--alter table ${com_schema}.t_mhbos_m_client drop if exists partition ( etl_dt = '${batch_date}' );

-- 3.2 Inserts the cleaned data into the specified partition of the target table
-- insert into table ${com_schema}.t_mhbos_m_client partition ( etl_dt = '${batch_date}' )
-- select mmca.client_no
--        ,(id.primary_identification_type_flag || id.primary_identification_no_flag ||
--           id.secondary_identification_type_flag ||id.secondary_identification_no_flag ||
--           cn.client_name_flag || cn.client_name1_flag || cn.client_name2_flag || cn.client_name3_flag ||
--           tel.mobile_no_flag || tel.fax_no_flag || tel.tel_no_home_flag || tel.tel_no_office_flag ||
--           dob.date_of_birth_flag || tmmcr.race_flag || tmmcei.email_flag || tmmcgi.sex_flag) as clean_rule_flag
--        ,id.primary_identification_type
--        ,mn.primary_identification_no
--        ,replace(id.secondary_identification_type, '@[]', '') as secondary_identification_type
--        ,replace(id.secondary_identification_no, '@[]', '') as secondary_identification_no
-- 	   ,mn.customer_name
--        ,cn.customer_name_concatenate
--        ,cn.client_name
--        ,cn.client_name1
--        ,cn.client_name2
--        ,cn.client_name3
--        ,tel.mobile_no
--        ,tel.fax_no
--        ,tel.tel_no_home
--        ,tel.tel_no_office
--        ,dob.date_of_birth
--        ,tmmcr.race
--        ,tmmcei.email_1
-- 	   ,tmmcei.email_2
-- 	   ,tmmcei.email_3
-- 	   ,tmmcei.email_4
-- 	   ,tmmcei.email_5
-- 	   ,tmmcei.email_6
-- 	   ,tmmcei.email_7
-- 	   ,tmmcei.email_8
-- 	   ,tmmcei.email_9
-- 	   ,tmmcei.email_10
--        ,tmmcgi.sex
--        ,ad.addr1
--        ,ad.addr2
--        ,ad.addr3
--        ,ad.addr4
--        ,ad.postcode
--        ,ad.city
--        ,ad.state
--        ,ad.perm_addr1
--        ,ad.perm_addr2
--        ,ad.perm_addr3
--        ,ad.perm_addr4
--        ,ad.perm_postcode
--        ,ad.perm_city
--        ,ad.perm_state
-- 	   ,nvl(nom.noms_ind, 'N') as noms_ind
-- 	   ,nom.cleaned_nominees_name
-- 	   ,nom.principal_name
-- 	   ,nom.intermediary_name
-- 	   ,nom.beneficiary_name
-- 	   ,nominees_type
-- 	   ,pledged_securities_flag
--        ,mmca.id_type
--        ,mmca.ic_no_new
--        ,mmca.ic_no_old
--        ,mmca.secondary_id_type
--        ,mmca.secondary_id_no
--        ,mmca.client_name as source_client_name
--        ,mmca.client_name1 as source_client_name1
--        ,mmca.client_name2 as source_client_name2
--        ,mmca.client_name3 as source_client_name3
--        ,mmca.mobile_no as source_mobile_no
--        ,mmca.fax_no as source_fax_no
--        ,mmca.tel_no_home as source_tel_no_home
--        ,mmca.tel_no_office as source_tel_no_office
--        ,mmca.date_of_birth as source_date_of_birth
--        ,mmca.race as source_race
--        ,mmca.email as source_email
--        ,mmca.sex as source_sex
--        ,mmca.client_group
--        ,mmca.cds_acc_no
--        ,mmca.tdr_code
--        ,mmca.client_type
--        ,mmca.margin
--        ,mmca.last_margin_date
--        ,mmca.int_rate
--        ,mmca.auto_ded
--        ,mmca.despatch_mode
--        ,mmca.copies
--        ,mmca.prohibit_trade
--        ,mmca.custody_status
--        ,mmca.country
--        ,mmca.margin_limit
--        ,mmca.margin_pct
--        ,mmca.rollover_rate
--        ,mmca.form_completed
--        ,mmca.last_tran_date
--        ,mmca.ytd_bvalue
--        ,mmca.ytd_svalue
--        ,mmca.ytd_brokerage
--        ,mmca.os_led_bal
--        ,mmca.title
--        ,mmca.date_created
--        ,mmca.stop_payt
--        ,mmca.acc_payee
--        ,mmca.category
--        ,mmca.auto_contra
--        ,mmca.pnl_acc_no
--        ,mmca.cr_limit
--        ,mmca.trust_bal
--        ,mmca.avg_ind
--        ,mmca.remarks
--        ,mmca.contact_person
--        ,mmca.date_closed
--        ,mmca.grace_period
--        ,mmca.acct_type
--        ,mmca.assoc_ind
--        ,mmca.short_sell_ind
--        ,mmca.short_name
--        ,mmca.mesdaq_pctlmt
--        ,mmca.date_change
--        ,mmca.resi_code
--        ,mmca.charge_int
--        ,mmca.bdebt
--        ,mmca.assets
--        ,mmca.liabilities
--        ,mmca.income
--        ,mmca.expenses
--        ,mmca.bdebt_his_ind
--        ,mmca.rel_ac1
--        ,mmca.rel_ac2
--        ,mmca.rel_ac3
--        ,mmca.rel_ac4
--        ,mmca.occupation
--        ,mmca.margin_int
--        ,mmca.lst_led_no
--        ,mmca.cur_led_no
--        ,mmca.remarks2
--        ,mmca.acc_type
--        ,mmca.mas_accno
--        ,mmca.legal
--        ,mmca.sell_limit
--        ,mmca.brk_rate
--        ,mmca.brokerage_type
--        ,mmca.cds_acc_no1
--        ,mmca.remarks1
--        ,mmca.payment_bank_code
--        ,mmca.noms
--        ,mmca.dms_date
--        ,mmca.violation_date
--        ,mmca.mcd_branch
--        ,mmca.home_branch
--        ,mmca.eaf_code
--        ,mmca.call_warrant
--        ,mmca.user_id
--        ,mmca.credit_int_rate
--        ,mmca.min_eligible_amt
--        ,mmca.intraday_flag
--        ,mmca.intraday_rate
--        ,mmca.cta_weight
--        ,mmca.sta_weight
--        ,mmca.bo_cds_acc_no
--        ,mmca.ecos_form
--        ,mmca.custodian_no
--        ,mmca.prin_acc
--        ,mmca.armada_type
--        ,mmca.old_authorisee
--        ,mmca.etrade_rate
--        ,mmca.etf
--        ,mmca.cstamp_client_exempt
--        ,mmca.main_branch
--        ,mmca.prev_client_no
--        ,mmca.web_eds
--        ,mmca.place
--        ,mmca.excl_tdr_deduct
--        ,mmca.excl_auto_susp
--        ,mmca.trust_flag
--        ,mmca.mgn_new_int_rate
--        ,mmca.counter_concentration
--        ,mmca.auto_trust
--        ,mmca.margin_pct2
--        ,mmca.df_flag
--        ,mmca.mgn_curr_int_rate
--        ,mmca.product_type
--        ,mmca.web_ecos
--        ,mmca.xeye_clt_grp
--        ,mmca.bursa_violation_date
--        ,mmca.brokerage_type_etrade
--        ,mmca.brokerage_type_odd_lot
--        ,mmca.omnibus
--        ,mmca.cg_tdr_code
--        ,mmca.limit_foreign
--        ,mmca.limit_bursa
--        ,mmca.brokerage_type_intraday
--        ,mmca.brokerage_type_intraday_etrade
--        ,mmca.bursa_violation_date1
--        ,mmca.cif_no
--        ,mmca.brokerage_type_foreign
--        ,mmca.soft_copy
--        ,mmca.exclude_rollover
--        ,mmca.account_status
--        ,mmca.w8ben
--        ,mmca.ic_no_rel1
--        ,mmca.ic_no_rel2
--        ,mmca.ic_no_rel3
--        ,mmca.ic_no_rel4
--        ,mmca.ic_no_rel5
--        ,mmca.rel1
--        ,mmca.rel2
--        ,mmca.rel3
--        ,mmca.rel4
--        ,mmca.rel5
--        ,mmca.brokerage_type_etrade_b
--        ,mmca.brokerage_type_odd_lot_b
--        ,mmca.brokerage_type_b
--        ,mmca.brokerage_type_foreign_b
--        ,mmca.brokerage_type_intraday_b
--        ,mmca.brokerage_type_intraday_etrade_b
--        ,mmca.brokerage_type_etrade_s
--        ,mmca.brokerage_type_odd_lot_s
--        ,mmca.brokerage_type_s
--        ,mmca.brokerage_type_foreign_s
--        ,mmca.brokerage_type_intraday_s
--        ,mmca.brokerage_type_intraday_etrade_s
--        ,mmca.no_free_trade
--        ,mmca.sms
--        ,mmca.mobile_prefix
--        ,mmca.foreign_curr_set
--        ,mmca.num_free_trade
--        ,mmca.etrader_type
--        ,mmca.check_limit
--        ,mmca.auto_margin
--        ,mmca.margin_client_no
--        ,mmca.dup_despatch_mode
--        ,mmca.risk
--        ,mmca.exclude_trader_limit
--        ,mmca.sett_mode_date_change
--        ,mmca.pick_up_fee_pct
--        ,mmca.e_payment
--        ,mmca.mgn_new_int_rate2
--        ,mmca.fund_cost_type
--        ,mmca.check_share
--        ,mmca.citibank_changes
--        ,mmca.citibank_charges
--        ,mmca.cq_market
--        ,mmca.exclude_margin_pro_rate
--        ,mmca.brokerage_type_cash_b
--        ,mmca.brokerage_type_etrade_cash_b
--        ,mmca.clt_consent
--        ,mmca.consent_start_date
--        ,mmca.portfolio
--        ,mmca.expiry_date
--        ,mmca.intraday_auto_contra_option
--        ,mmca.dcf_limit
--        ,mmca.brokerage_type_etb
--        ,mmca.mgn_force_sell_pct
--        ,mmca.mgn_tenure
--        ,mmca.mgn_expiry_date
--        ,mmca.loss_gl_acc_no
--        ,mmca.portfolio_date
--        ,mmca.day_prior_temp_susp
--        ,mmca.day_prior_perm_susp
--        ,mmca.gst_code
--        ,mmca.match_price_decimal_local
--        ,mmca.match_price_decimal_foreign
--        ,mmca.primary_id_expiry_date
--        ,mmca.secondary_id_expiry_date
--        ,mmca.mgn_int_tdr_spread_pct
--        ,mmca.mgn_base_int_rate
--        ,mmca.mgn_int_tdr_share
--        ,mmca.islamic_flag
--        ,mmca.mcd_resident_flag
--        ,mmca.chq_charges_flag
--        ,mmca.chq_charges_tdr_pct
--        ,mmca.brokerage_type_foreign_etrade
--        ,mmca.brokerage_type_foreign_etrade_b
--        ,mmca.brokerage_type_foreign_etrade_s
--        ,mmca.grp_exch_code
--        ,mmca.bdebt_ras
--        ,mmca.twse_declaration
--        ,mmca.joint_acc_amt
--        ,mmca.high_risk_market
--        ,mmca.brokerage_type_leap_normal
--        ,mmca.brokerage_type_leap_etrade
--        ,'${batch_timestamp}' as etl_timestamp
--   from ${com_schema}.temp_t_mhbos_m_client_all mmca
--   left join ${com_schema}.temp_t_mhbos_m_client_identification_info id
--     on mmca.client_no = id.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_customer_name_2 cn
--     on mmca.client_no = cn.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_telephone_number_info tel
--     on mmca.client_no = tel.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_dob_info dob
--     on mmca.client_no = dob.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_race_info tmmcr
--     on mmca.client_no = tmmcr.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_email_info tmmcei
--     on mmca.client_no = tmmcei.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_gender_info tmmcgi
--     on mmca.client_no = tmmcgi.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_address_info ad
--     on mmca.client_no = ad.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_nominees_info nom
--     on mmca.client_no = nom.client_no
--   left join ${com_schema}.temp_t_mhbos_m_client_name_info  mn						--add_20240321
--     on mmca.client_no = mn.client_no
-- ;

-- 3.2.1 step1
drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info_step1;
create table ${com_schema}.temp_t_mhbos_m_client_identification_info_step1
as
select id.client_no
    ,(id.primary_identification_type_flag || id.primary_identification_no_flag ||
    id.secondary_identification_type_flag ||id.secondary_identification_no_flag ||
    cn.client_name_flag || cn.client_name1_flag || cn.client_name2_flag || cn.client_name3_flag ||
    tel.mobile_no_flag || tel.fax_no_flag || tel.tel_no_home_flag || tel.tel_no_office_flag ||
    dob.date_of_birth_flag || tmmcr.race_flag || tmmcei.email_flag || tmmcgi.sex_flag) as clean_rule_flag
    -- ,id.primary_identification_type -- 20250313
    -- ,mn.primary_identification_no
    -- ,replace(id.secondary_identification_type, '@[]', '') as secondary_identification_type -- 20250620
    -- ,replace(id.secondary_identification_no, '@[]', '') as secondary_identification_no -- 20250620
    -- ,mn.customer_name
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
    ,tmmcei.einvoice_email --20250903 einvoice_email
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
from ${com_schema}.temp_t_mhbos_m_client_identification_info id
left join ${com_schema}.temp_t_mhbos_m_client_customer_name_2 cn
on id.client_no = cn.client_no
left join ${com_schema}.temp_t_mhbos_m_client_telephone_number_info tel
on id.client_no = tel.client_no
left join ${com_schema}.temp_t_mhbos_m_client_dob_info dob
on id.client_no = dob.client_no
left join ${com_schema}.temp_t_mhbos_m_client_race_info tmmcr
on id.client_no = tmmcr.client_no
left join ${com_schema}.temp_t_mhbos_m_client_email_info tmmcei
on id.client_no = tmmcei.client_no
left join ${com_schema}.temp_t_mhbos_m_client_gender_info tmmcgi
on id.client_no = tmmcgi.client_no
;

-- 3.2.2 step2
drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info_step2;
create table ${com_schema}.temp_t_mhbos_m_client_identification_info_step2
as
select step1.client_no
    -- 2025/09/08: Add customer_name_flag
    , step1.clean_rule_flag || mn.customer_name_flag as clean_rule_flag
    , mn.primary_identification_type -- modify 20250313
    , mn.primary_identification_no
    , mn.secondary_identification_type -- modify from step1. to mn. 20250620
    , mn.secondary_identification_no -- modify from step1. to mn. 20250620
    , mn.customer_name
    , step1.customer_name_concatenate
    , step1.client_name
    , step1.client_name1
    , step1.client_name2
    , step1.client_name3
    , step1.mobile_no
    , step1.fax_no
    , step1.tel_no_home
    , step1.tel_no_office
    , step1.date_of_birth
    , step1.race
    , step1.email_1
    , step1.email_2
    , step1.email_3
    , step1.email_4
    , step1.email_5
    , step1.email_6
    , step1.email_7
    , step1.email_8
    , step1.email_9
    , step1.email_10
    , step1.sex
    , ad.addr1
    , ad.addr2
    , ad.addr3
    , ad.addr4
    , ad.postcode
    , ad.city
    , ad.state
    , ad.perm_addr1
    , ad.perm_addr2
    , ad.perm_addr3
    , ad.perm_addr4
    , ad.perm_postcode
    , ad.perm_city
    , ad.perm_state
    , ad.perm_country -- added new field 20250715
    , nvl(nom.noms_ind, 'N') as noms_ind
    , nom.cleaned_nominees_name
    , mn.principal_name --modify 20240518
    , nom.intermediary_name
    , mn.beneficiary_name    --modify 20240518
    , nom.nominees_type
    , nom.pledged_securities_flag
    , step1.einvoice_email --20250903 einvoice_email
from ${com_schema}.temp_t_mhbos_m_client_identification_info_step1 step1
left join ${com_schema}.temp_t_mhbos_m_client_address_info ad
on step1.client_no = ad.client_no
left join ${com_schema}.temp_t_mhbos_m_client_nominees_info nom
on step1.client_no = nom.client_no
left join ${com_schema}.temp_t_mhbos_m_client_name_info  mn
on step1.client_no = mn.client_no
;

-- 3.0.1 create temp_t_mhbos_m_client table
create table if not exists ${com_schema}.temp_t_mhbos_m_client(
    client_no varchar(9) comment ''
    ,clean_rule_flag varchar(60) comment ''
    ,primary_identification_type varchar(10) comment ''
    ,primary_identification_no varchar(60) comment ''
    ,secondary_identification_type varchar(10) comment ''
    ,secondary_identification_no varchar(60) comment ''
    ,customer_name varchar(250) comment ''
    ,customer_name_concatenate varchar(250) comment ''
    ,client_name varchar(60) comment ''
    ,client_name1 varchar(60) comment ''
    ,client_name2 varchar(60) comment ''
    ,client_name3 varchar(60) comment ''
    ,mobile_no varchar(20) comment ''
    ,fax_no varchar(20) comment ''
    ,tel_no_home varchar(20) comment ''
    ,tel_no_office varchar(20) comment ''
    ,date_of_birth timestamp comment ''
    ,race varchar(30) comment ''
    ,email_1 varchar(300) comment ''
    ,email_2 varchar(300) comment ''
    ,email_3 varchar(300) comment ''
    ,email_4 varchar(300) comment '' 
    ,email_5 varchar(300) comment ''
    ,email_6 varchar(300) comment ''
    ,email_7 varchar(300) comment ''
    ,email_8 varchar(300) comment ''
    ,email_9 varchar(300) comment '' 
    ,email_10 varchar(300) comment ''
    ,sex varchar(10) comment ''
    ,addr1 varchar(45) comment ''
    ,addr2 varchar(45) comment ''
    ,addr3 varchar(45) comment ''
    ,addr4 varchar(45) comment ''
    ,postcode varchar(6) comment ''
    ,city varchar(255) comment ''
    ,state varchar(50) comment ''
    ,perm_addr1 varchar(45) comment ''
    ,perm_addr2 varchar(45) comment ''
    ,perm_addr3 varchar(45) comment ''
    ,perm_addr4 varchar(45) comment ''
    ,perm_postcode varchar(6) comment ''
    ,perm_city varchar(255) comment ''
    ,perm_state varchar(50) comment ''
    ,noms_ind varchar(1) comment ''
    ,cleaned_nominees_name varchar(300) comment ''
    ,principal_name varchar(300) comment ''
    ,intermediary_name varchar(300) comment ''
    ,beneficiary_name varchar(300) comment ''
    ,nominees_type	varchar(10) comment ''
    ,pledged_securities_flag	varchar(1) comment ''  
    ,id_type	varchar(10) comment ''
    ,ic_no_new varchar(15) comment ''
    ,ic_no_old varchar(14) comment ''
    ,secondary_id_type varchar(2) comment ''
    ,secondary_id_no varchar(15) comment ''
    ,source_client_name varchar(50) comment ''
    ,source_client_name1 varchar(50) comment ''
    ,source_client_name2 varchar(50) comment ''
    ,source_client_name3 varchar(50) comment ''
    ,source_mobile_no varchar(15) comment ''
    ,source_fax_no varchar(15) comment ''
    ,source_tel_no_home varchar(15) comment ''
    ,source_tel_no_office varchar(15) comment ''
    ,source_date_of_birth timestamp comment ''
    ,source_race varchar(20) comment ''
    ,source_email varchar(300) comment ''
    ,source_sex varchar(10) comment ''
    ,client_group varchar(14) comment ''
    ,cds_acc_no varchar(9) comment ''
    ,tdr_code varchar(5) comment ''
    ,client_type varchar(3) comment ''
    ,margin varchar(1) comment ''
    ,last_margin_date timestamp comment ''
    ,int_rate numeric(5,2) comment ''
    ,auto_ded varchar(1) comment ''
    ,despatch_mode varchar(2) comment ''
    ,copies numeric(2,0) comment ''
    ,prohibit_trade varchar(1) comment ''
    ,custody_status varchar(1) comment ''
    ,country varchar(3) comment ''
    ,margin_limit numeric(9,0) comment ''
    ,margin_pct numeric(5,2) comment ''
    ,rollover_rate numeric(6,3) comment ''
    ,form_completed varchar(1) comment ''
    ,last_tran_date timestamp comment ''
    ,ytd_bvalue numeric(12,2) comment ''
    ,ytd_svalue numeric(12,2) comment ''
    ,ytd_brokerage numeric(11,2) comment ''
    ,os_led_bal numeric(12,2) comment ''
    ,title varchar(20) comment ''
    ,date_created timestamp comment ''
    ,stop_payt varchar(1) comment ''
    ,acc_payee varchar(100) comment ''
    ,category varchar(1) comment ''
    ,auto_contra varchar(1) comment ''
    ,pnl_acc_no varchar(18) comment ''
    ,cr_limit numeric(9,0) comment ''
    ,trust_bal numeric(12,2) comment ''
    ,avg_ind varchar(8) comment ''
    ,remarks varchar(60) comment ''
    ,contact_person varchar(40) comment ''
    ,date_closed timestamp comment ''
    ,grace_period numeric(3,0) comment ''
    ,acct_type numeric(2,0) comment ''
    ,assoc_ind varchar(1) comment ''
    ,short_sell_ind varchar(1) comment ''
    ,short_name varchar(10) comment ''
    ,mesdaq_pctlmt numeric(5,2) comment ''
    ,date_change timestamp comment ''
    ,resi_code varchar(5) comment ''
    ,charge_int varchar(1) comment ''
    ,bdebt varchar(1) comment ''
    ,assets numeric(12,2) comment ''
    ,liabilities numeric(12,2) comment ''
    ,income numeric(12,2) comment ''
    ,expenses numeric(12,2) comment ''
    ,bdebt_his_ind varchar(3) comment ''
    ,rel_ac1 varchar(9) comment ''
    ,rel_ac2 varchar(9) comment ''
    ,rel_ac3 varchar(9) comment ''
    ,rel_ac4 varchar(9) comment ''
    ,occupation varchar(60) comment ''
    ,margin_int numeric(12,2) comment ''
    ,lst_led_no numeric(9,0) comment ''
    ,cur_led_no numeric(9,0) comment ''
    ,remarks2 varchar(60) comment ''
    ,acc_type varchar(1) comment ''
    ,mas_accno varchar(9) comment ''
    ,legal varchar(1) comment ''
    ,sell_limit numeric(9,0) comment ''
    ,brk_rate numeric(9,4) comment ''
    ,brokerage_type varchar(3) comment ''
    ,cds_acc_no1 varchar(9) comment ''
    ,remarks1 varchar(60) comment ''
    ,payment_bank_code varchar(5) comment ''
    ,noms varchar(1) comment ''
    ,dms_date timestamp comment ''
    ,violation_date timestamp comment ''
    ,mcd_branch varchar(3) comment ''
    ,home_branch varchar(3) comment ''
    ,eaf_code varchar(1) comment ''
    ,call_warrant varchar(1) comment ''
    ,user_id varchar(20) comment ''
    ,credit_int_rate numeric(5,2) comment ''
    ,min_eligible_amt numeric(18,4) comment ''
    ,intraday_flag varchar(1) comment ''
    ,intraday_rate numeric(5,4) comment ''
    ,cta_weight numeric(3,0) comment ''
    ,sta_weight numeric(3,0) comment ''
    ,bo_cds_acc_no varchar(20) comment ''
    ,ecos_form varchar(1) comment ''
    ,custodian_no varchar(7) comment ''
    ,prin_acc varchar(2) comment ''
    ,armada_type varchar(8) comment ''
    ,old_authorisee varchar(5) comment ''
    ,etrade_rate numeric(9,4) comment ''
    ,etf varchar(1) comment ''
    ,cstamp_client_exempt varchar(1) comment ''
    ,main_branch varchar(3) comment ''
    ,prev_client_no varchar(9) comment ''
    ,web_eds varchar(1) comment ''
    ,place varchar(5) comment ''
    ,excl_tdr_deduct varchar(1) comment ''
    ,excl_auto_susp varchar(1) comment ''
    ,trust_flag varchar(1) comment ''
    ,mgn_new_int_rate numeric(5,2) comment ''
    ,counter_concentration numeric(5,2) comment ''
    ,auto_trust varchar(1) comment ''
    ,margin_pct2 numeric(5,2) comment ''
    ,df_flag varchar(1) comment ''
    ,mgn_curr_int_rate numeric(5,2) comment ''
    ,product_type varchar(1) comment ''
    ,web_ecos varchar(1) comment ''
    ,xeye_clt_grp varchar(14) comment ''
    ,bursa_violation_date timestamp comment ''
    ,brokerage_type_etrade varchar(3) comment ''
    ,brokerage_type_odd_lot varchar(3) comment ''
    ,omnibus varchar(1) comment ''
    ,cg_tdr_code varchar(7) comment ''
    ,limit_foreign numeric(9,4) comment ''
    ,limit_bursa numeric(9,4) comment ''
    ,brokerage_type_intraday varchar(3) comment ''
    ,brokerage_type_intraday_etrade varchar(3) comment ''
    ,bursa_violation_date1 timestamp comment ''
    ,cif_no varchar(20) comment ''
    ,brokerage_type_foreign varchar(3) comment ''
    ,soft_copy varchar(1) comment ''
    ,exclude_rollover varchar(1) comment ''
    ,account_status varchar(1) comment ''
    ,w8ben varchar(1) comment ''
    ,ic_no_rel1 varchar(15) comment ''
    ,ic_no_rel2 varchar(15) comment ''
    ,ic_no_rel3 varchar(15) comment ''
    ,ic_no_rel4 varchar(15) comment ''
    ,ic_no_rel5 varchar(15) comment ''
    ,rel1 varchar(15) comment ''
    ,rel2 varchar(15) comment ''
    ,rel3 varchar(15) comment ''
    ,rel4 varchar(15) comment ''
    ,rel5 varchar(15) comment ''
    ,brokerage_type_etrade_b varchar(3) comment ''
    ,brokerage_type_odd_lot_b varchar(3) comment ''
    ,brokerage_type_b varchar(3) comment ''
    ,brokerage_type_foreign_b varchar(3) comment ''
    ,brokerage_type_intraday_b varchar(3) comment ''
    ,brokerage_type_intraday_etrade_b varchar(3) comment ''
    ,brokerage_type_etrade_s varchar(3) comment ''
    ,brokerage_type_odd_lot_s varchar(3) comment ''
    ,brokerage_type_s varchar(3) comment ''
    ,brokerage_type_foreign_s varchar(3) comment ''
    ,brokerage_type_intraday_s varchar(3) comment ''
    ,brokerage_type_intraday_etrade_s varchar(3) comment ''
    ,no_free_trade numeric(2,0) comment ''
    ,sms varchar(1) comment ''
    ,mobile_prefix varchar(5) comment ''
    ,foreign_curr_set varchar(1) comment ''
    ,num_free_trade numeric(2,0) comment ''
    ,etrader_type varchar(2) comment ''
    ,check_limit varchar(1) comment ''
    ,auto_margin varchar(1) comment ''
    ,margin_client_no varchar(9) comment ''
    ,dup_despatch_mode varchar(1) comment ''
    ,risk varchar(1) comment ''
    ,exclude_trader_limit varchar(1) comment ''
    ,sett_mode_date_change timestamp comment ''
    ,pick_up_fee_pct numeric(5,2) comment ''
    ,e_payment varchar(1) comment ''
    ,mgn_new_int_rate2 numeric(5,2) comment ''
    ,fund_cost_type varchar(1) comment ''
    ,check_share varchar(1) comment ''
    ,citibank_changes varchar(1) comment ''
    ,citibank_charges varchar(1) comment ''
    ,cq_market varchar(1) comment ''
    ,exclude_margin_pro_rate varchar(1) comment ''
    ,brokerage_type_cash_b varchar(3) comment ''
    ,brokerage_type_etrade_cash_b varchar(3) comment ''
    ,clt_consent varchar(1) comment ''
    ,consent_start_date timestamp comment ''
    ,portfolio varchar(1) comment ''
    ,expiry_date timestamp comment ''
    ,intraday_auto_contra_option varchar(1) comment ''
    ,dcf_limit numeric(9,0) comment ''
    ,brokerage_type_etb varchar(3) comment ''
    ,mgn_force_sell_pct numeric(5,2) comment ''
    ,mgn_tenure numeric(3,0) comment ''
    ,mgn_expiry_date timestamp comment ''
    ,loss_gl_acc_no varchar(18) comment ''
    ,portfolio_date timestamp comment ''
    ,day_prior_temp_susp numeric(4,0) comment ''
    ,day_prior_perm_susp numeric(4,0) comment ''
    ,gst_code varchar(3) comment ''
    ,match_price_decimal_local numeric(1,0) comment ''
    ,match_price_decimal_foreign numeric(1,0) comment ''
    ,primary_id_expiry_date timestamp comment ''
    ,secondary_id_expiry_date timestamp comment ''
    ,mgn_int_tdr_spread_pct numeric(5,2) comment ''
    ,mgn_base_int_rate numeric(5,2) comment ''
    ,mgn_int_tdr_share numeric(5,2) comment ''
    ,islamic_flag varchar(1) comment ''
    ,mcd_resident_flag varchar(1) comment ''
    ,chq_charges_flag varchar(1) comment ''
    ,chq_charges_tdr_pct numeric(9,2) comment ''
    ,brokerage_type_foreign_etrade varchar(3) comment ''
    ,brokerage_type_foreign_etrade_b varchar(3) comment ''
    ,brokerage_type_foreign_etrade_s varchar(3) comment ''
    ,grp_exch_code varchar(5) comment ''
    ,bdebt_ras varchar(1) comment ''
    ,twse_declaration varchar(1) comment ''
    ,joint_acc_amt numeric(12,2) comment ''
    ,high_risk_market varchar(2) comment ''
    ,brokerage_type_leap_normal varchar(3) comment ''
    ,brokerage_type_leap_etrade varchar(3) comment ''
    ,etl_timestamp  string comment 'ETL_processing_time'
    ,perm_country varchar(3) comment '' 
    ,type_of_account string comment ''
    ,einvoice_email string comment ''
);

-- 3.0.2 insert into temp_t_mhbos_m_client
insert into table ${com_schema}.temp_t_mhbos_m_client
select step2.client_no
    , step2.clean_rule_flag
    , step2.primary_identification_type
    , step2.primary_identification_no
    , step2.secondary_identification_type
    , step2.secondary_identification_no
    , step2.customer_name
    , step2.customer_name_concatenate
    , step2.client_name
    , step2.client_name1
    , step2.client_name2
    , step2.client_name3
    , step2.mobile_no
    , step2.fax_no
    , step2.tel_no_home
    , step2.tel_no_office
    , step2.date_of_birth
    , step2.race
    , step2.email_1
    , step2.email_2
    , step2.email_3
    , step2.email_4
    , step2.email_5
    , step2.email_6
    , step2.email_7
    , step2.email_8
    , step2.email_9
    , step2.email_10
    , step2.sex
    , step2.addr1
    , step2.addr2
    , step2.addr3
    , step2.addr4
    , step2.postcode
    , step2.city
    , step2.state
    , step2.perm_addr1
    , step2.perm_addr2
    , step2.perm_addr3
    , step2.perm_addr4
    , step2.perm_postcode
    , step2.perm_city
    , step2.perm_state
    , step2.noms_ind
    , step2.cleaned_nominees_name
    , step2.principal_name
    , step2.intermediary_name
    , step2.beneficiary_name
    , step2.nominees_type
    , step2.pledged_securities_flag
    , mmca.id_type
    , mmca.ic_no_new
    , mmca.ic_no_old
    , mmca.secondary_id_type
    , mmca.secondary_id_no
    , mmca.client_name as source_client_name
    , mmca.client_name1 as source_client_name1
    , mmca.client_name2 as source_client_name2
    , mmca.client_name3 as source_client_name3
    , mmca.mobile_no as source_mobile_no
    , mmca.fax_no as source_fax_no
    , mmca.tel_no_home as source_tel_no_home
    , mmca.tel_no_office as source_tel_no_office
    , mmca.date_of_birth as source_date_of_birth
    , mmca.race as source_race
    , mmca.email as source_email
    , mmca.sex as source_sex
    , mmca.client_group
    , mmca.cds_acc_no
    , mmca.tdr_code
    , mmca.client_type
    , mmca.margin
    , mmca.last_margin_date
    , mmca.int_rate
    , mmca.auto_ded
    , mmca.despatch_mode
    , mmca.copies
    , mmca.prohibit_trade
    , mmca.custody_status
    , mmca.country
    , mmca.margin_limit
    , mmca.margin_pct
    , mmca.rollover_rate
    , mmca.form_completed
    , mmca.last_tran_date
    , mmca.ytd_bvalue
    , mmca.ytd_svalue
    , mmca.ytd_brokerage
    , mmca.os_led_bal
    , mmca.title
    , mmca.date_created
    , mmca.stop_payt
    , mmca.acc_payee
    , mmca.category
    , mmca.auto_contra
    , mmca.pnl_acc_no
    , mmca.cr_limit
    , mmca.trust_bal
    , mmca.avg_ind
    , mmca.remarks
    , mmca.contact_person
    , mmca.date_closed
    , mmca.grace_period
    , mmca.acct_type
    , mmca.assoc_ind
    , mmca.short_sell_ind
    , mmca.short_name
    , mmca.mesdaq_pctlmt
    , mmca.date_change
    , mmca.resi_code
    , mmca.charge_int
    , mmca.bdebt
    , mmca.assets
    , mmca.liabilities
    , mmca.income
    , mmca.expenses
    , mmca.bdebt_his_ind
    , mmca.rel_ac1
    , mmca.rel_ac2
    , mmca.rel_ac3
    , mmca.rel_ac4
    , mmca.occupation
    , mmca.margin_int
    , mmca.lst_led_no
    , mmca.cur_led_no
    , mmca.remarks2
    , mmca.acc_type
    , mmca.mas_accno
    , mmca.legal
    , mmca.sell_limit
    , mmca.brk_rate
    , mmca.brokerage_type
    , mmca.cds_acc_no1
    , mmca.remarks1
    , mmca.payment_bank_code
    , mmca.noms
    , mmca.dms_date
    , mmca.violation_date
    , mmca.mcd_branch
    , mmca.home_branch
    , mmca.eaf_code
    , mmca.call_warrant
    , mmca.user_id
    , mmca.credit_int_rate
    , mmca.min_eligible_amt
    , mmca.intraday_flag
    , mmca.intraday_rate
    , mmca.cta_weight
    , mmca.sta_weight
    , mmca.bo_cds_acc_no
    , mmca.ecos_form
    , mmca.custodian_no
    , mmca.prin_acc
    , mmca.armada_type
    , mmca.old_authorisee
    , mmca.etrade_rate
    , mmca.etf
    , mmca.cstamp_client_exempt
    , mmca.main_branch
    , mmca.prev_client_no
    , mmca.web_eds
    , mmca.place
    , mmca.excl_tdr_deduct
    , mmca.excl_auto_susp
    , mmca.trust_flag
    , mmca.mgn_new_int_rate
    , mmca.counter_concentration
    , mmca.auto_trust
    , mmca.margin_pct2
    , mmca.df_flag
    , mmca.mgn_curr_int_rate
    , mmca.product_type
    , mmca.web_ecos
    , mmca.xeye_clt_grp
    , mmca.bursa_violation_date
    , mmca.brokerage_type_etrade
    , mmca.brokerage_type_odd_lot
    , mmca.omnibus
    , mmca.cg_tdr_code
    , mmca.limit_foreign
    , mmca.limit_bursa
    , mmca.brokerage_type_intraday
    , mmca.brokerage_type_intraday_etrade
    , mmca.bursa_violation_date1
    , mmca.cif_no
    , mmca.brokerage_type_foreign
    , mmca.soft_copy
    , mmca.exclude_rollover
    , mmca.account_status
    , mmca.w8ben
    , mmca.ic_no_rel1
    , mmca.ic_no_rel2
    , mmca.ic_no_rel3
    , mmca.ic_no_rel4
    , mmca.ic_no_rel5
    , mmca.rel1
    , mmca.rel2
    , mmca.rel3
    , mmca.rel4
    , mmca.rel5
    , mmca.brokerage_type_etrade_b
    , mmca.brokerage_type_odd_lot_b
    , mmca.brokerage_type_b
    , mmca.brokerage_type_foreign_b
    , mmca.brokerage_type_intraday_b
    , mmca.brokerage_type_intraday_etrade_b
    , mmca.brokerage_type_etrade_s
    , mmca.brokerage_type_odd_lot_s
    , mmca.brokerage_type_s
    , mmca.brokerage_type_foreign_s
    , mmca.brokerage_type_intraday_s
    , mmca.brokerage_type_intraday_etrade_s
    , mmca.no_free_trade
    , mmca.sms
    , mmca.mobile_prefix
    , mmca.foreign_curr_set
    , mmca.num_free_trade
    , mmca.etrader_type
    , mmca.check_limit
    , mmca.auto_margin
    , mmca.margin_client_no
    , mmca.dup_despatch_mode
    , mmca.risk
    , mmca.exclude_trader_limit
    , mmca.sett_mode_date_change
    , mmca.pick_up_fee_pct
    , mmca.e_payment
    , mmca.mgn_new_int_rate2
    , mmca.fund_cost_type
    , mmca.check_share
    , mmca.citibank_changes
    , mmca.citibank_charges
    , mmca.cq_market
    , mmca.exclude_margin_pro_rate
    , mmca.brokerage_type_cash_b
    , mmca.brokerage_type_etrade_cash_b
    , mmca.clt_consent
    , mmca.consent_start_date
    , mmca.portfolio
    , mmca.expiry_date
    , mmca.intraday_auto_contra_option
    , mmca.dcf_limit
    , mmca.brokerage_type_etb
    , mmca.mgn_force_sell_pct
    , mmca.mgn_tenure
    , mmca.mgn_expiry_date
    , mmca.loss_gl_acc_no
    , mmca.portfolio_date
    , mmca.day_prior_temp_susp
    , mmca.day_prior_perm_susp
    , mmca.gst_code
    , mmca.match_price_decimal_local
    , mmca.match_price_decimal_foreign
    , mmca.primary_id_expiry_date
    , mmca.secondary_id_expiry_date
    , mmca.mgn_int_tdr_spread_pct
    , mmca.mgn_base_int_rate
    , mmca.mgn_int_tdr_share
    , mmca.islamic_flag
    , mmca.mcd_resident_flag
    , mmca.chq_charges_flag
    , mmca.chq_charges_tdr_pct
    , mmca.brokerage_type_foreign_etrade
    , mmca.brokerage_type_foreign_etrade_b
    , mmca.brokerage_type_foreign_etrade_s
    , mmca.grp_exch_code
    , mmca.bdebt_ras
    , mmca.twse_declaration
    , mmca.joint_acc_amt
    , mmca.high_risk_market
    , mmca.brokerage_type_leap_normal
    , mmca.brokerage_type_leap_etrade
    , '${batch_timestamp}' as etl_timestamp
    , step2.perm_country -- added new field 20250715
    , mmca.type_of_account -- added new field 20250806
    , step2.einvoice_email --20250903 einvoice_email
from ${com_schema}.temp_t_mhbos_m_client_all mmca
left join ${com_schema}.temp_t_mhbos_m_client_identification_info_step2 step2
on mmca.client_no = step2.client_no
;

-- 3.1 truncate batch date partition
alter table ${com_schema}.t_mhbos_m_client drop if exists partition ( etl_dt = '${batch_date}' );

-- 3.2 insert data to target table
WITH today_accounts AS (
  SELECT DISTINCT client_no
  FROM ${com_schema}.temp_t_mhbos_m_client
)
INSERT INTO TABLE ${com_schema}.t_mhbos_m_client PARTITION (etl_dt = '${batch_date}')
SELECT client_no
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
    , '${batch_timestamp}' as etl_timestamp
    , perm_country 
    , type_of_account
    , einvoice_email 
FROM ${com_schema}.t_mhbos_m_client t
WHERE etl_dt = '${last_date}'
AND NOT EXISTS (
  SELECT 1 FROM today_accounts ta 
  WHERE ta.client_no = t.client_no
)
UNION ALL 
SELECT client_no
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
    , '${batch_timestamp}' as etl_timestamp
    , perm_country 
    , type_of_account
    , einvoice_email 
FROM temp_t_mhbos_m_client;

-- 3.3 Collect statistics about partitions in the target table on the current day
analyze table ${com_schema}.t_mhbos_m_client partition (etl_dt = '${batch_date}') compute statistics;

-- 4.1 Drop all temporary tables
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_all;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_type;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_primary_identification_no;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_type;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_secondary_identification_no;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_dob_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_gender_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_customer_name_1;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_customer_name_2;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_race_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_telephone_number_clean;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_telephone_number_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_email_clean;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_email_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_address_city;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_address_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_1;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_2_1;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_3;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_4_1;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info_5;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_nominees_info;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info_step1;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client_identification_info_step2;
-- drop table if exists ${com_schema}.temp_t_mhbos_m_client;

