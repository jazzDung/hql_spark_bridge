##  File Name   : com_t_mhbos_m_client
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-06 07:36:52
##  Source      : com_t_mhbos_m_client (migrated from Datalake Old)

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


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated (
    client_no STRING
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
    , perm_country STRING
    , type_of_account STRING
    , einvoice_email STRING
    ,record_status       VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated
SELECT
    client_no
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
    , perm_country
    , type_of_account
    , einvoice_email
    ,'A' AS record_status
    ,record_created_date
    ,record_updated_date
FROM {params["com_schema"]}.t_mhbos_m_client com_t
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.mhbos_m_client r
    WHERE r.etl_dt = '{batch_date}'
      AND r.client_no = com_t.client_no
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_updated
SELECT
    r.client_no
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
    , r.perm_country
    , r.type_of_account
    , r.einvoice_email
    ,'A' AS record_status
    ,CASE WHEN com_t.client_no IS NOT NULL THEN com_t.record_created_date
          ELSE current_timestamp() END AS record_created_date
    ,current_timestamp() AS record_updated_date
FROM {params["raw_schema"]}.mhbos_m_client r
LEFT JOIN {params["com_schema"]}.t_mhbos_m_client com_t
    ON r.client_no = com_t.client_no
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.t_mhbos_m_client
SELECT
    client_no
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
    , perm_country
    , type_of_account
    , einvoice_email
    ,record_status
    ,record_created_date
    ,record_updated_date
    ,'{batch_date}'       AS etl_dt
    ,current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_t_mhbos_m_client_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.t_mhbos_m_client COMPUTE STATISTICS""")

spark.stop()