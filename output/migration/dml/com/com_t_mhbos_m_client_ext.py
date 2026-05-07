##  File Name   : com_t_mhbos_m_client_ext
##  File Type   : DML
##  Model       : 3a
##  Generated   : 2026-05-06 07:57:20
##  Source      : com_t_mhbos_m_client_ext (migrated from Datalake Old)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "mhbos"
table_name  = "m_client_ext"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
# batch_date = today_date
batch_date = '20260429'
params = set_parameter(spark)


# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_t_mhbos_m_client_ext_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_t_mhbos_m_client_ext_updated (
    client_no STRING
    , bank_accno STRING
    , bank_name STRING
    , bank_branch STRING
    , monthly_income STRING
    , home_branch STRING
    , eligible STRING
    , perm_addr1 STRING
    , perm_addr2 STRING
    , perm_addr3 STRING
    , perm_addr4 STRING
    , perm_postcode STRING
    , passport_no STRING
    , comm_mgn_facility_date TIMESTAMP
    , exp_mgn_facility_date TIMESTAMP
    , int_code_con STRING
    , int_code_ctr STRING
    , int_code_bill STRING
    , setoff_gain STRING
    , setoff_trust_losses STRING
    , setoff_trust_pur STRING
    , pnl_acc_no_company STRING
    , clr_acc_profit_pct STRING
    , clr_acc_loss_pct STRING
    , sbl_cds_acc_no STRING
    , rss_flag STRING
    , bo_ic_no_new STRING
    , bo_ic_no_old STRING
    , sbl_client_no STRING
    , pledge_client_no STRING
    , campaign_code STRING
    , email STRING
    , estimated_networth STRING
    , smf STRING
    , employer_name STRING
    , date_of_birth TIMESTAMP
    , marital_status STRING
    , bumi_status STRING
    , basic_code STRING
    , state_code STRING
    , rcc_code STRING
    , country_code STRING
    , last_maint_date TIMESTAMP
    , last_maint_date_addr TIMESTAMP
    , last_maint_date_empl TIMESTAMP
    , approval_date TIMESTAMP
    , brk_sharing_ind STRING
    , brk_sharing_effective_date TIMESTAMP
    , brk_sharing_intro_branch_tdr_code STRING
    , brk_sharing_intro_branch_level1_pct STRING
    , brk_sharing_intro_branch_tdr_level2_pct STRING
    , brk_sharing_exec_branch_tdr_level2_pct STRING
    , omnibus_no STRING
    , excl_stmt_printing STRING
    , multi_currency STRING
    , market_maker STRING
    , introduced_br STRING
    , foreign_declare STRING
    , foreign_declare_date TIMESTAMP
    , promotion_code STRING
    , ecm_money_usr STRING
    , amla_or STRING
    , bursa TIMESTAMP
    , sgx TIMESTAMP
    , hkex TIMESTAMP
    , nyse TIMESTAMP
    , amex TIMESTAMP
    , nasdaq TIMESTAMP
    , pep_ind STRING
    , amlatf STRING
    , equities_form STRING
    , ecmmoney_form STRING
    , futures_form STRING
    , treasury_form STRING
    , ut_form STRING
    , loan_form STRING
    , others_form STRING
    , equities_form_date TIMESTAMP
    , ecmmoney_form_date TIMESTAMP
    , futures_form_date TIMESTAMP
    , treasury_form_date TIMESTAMP
    , ut_form_date TIMESTAMP
    , loan_form_date TIMESTAMP
    , others_form_date TIMESTAMP
    , type_of_business STRING
    , guarantor_name1 STRING
    , guarantor_ic1 STRING
    , guarantor_country1 STRING
    , type_of_guarantee1 STRING
    , guarantor_name2 STRING
    , guarantor_ic2 STRING
    , guarantor_country2 STRING
    , type_of_guarantee2 STRING
    , guarantor_name3 STRING
    , guarantor_ic3 STRING
    , guarantor_country3 STRING
    , type_of_guarantee3 STRING
    , corp_status STRING
    , industrial_sector STRING
    , priority_sector STRING
    , nationality STRING
    , offshore_sett STRING
    , pdpa_form STRING
    , fatca_form STRING
    , us_taxpayer_no STRING
    , disclosure_of_info STRING
    , cp_ind STRING
    , invest_type STRING
    , mcd_acct_type STRING
    , mcd_bo_cd STRING
    , ediv_bank_acct_no STRING
    , ediv_bank_code STRING
    , consol_ediv STRING
    , ediv_joint STRING
    , mcd_race STRING
    , mcd_nationality STRING
    , foreign_curr_con STRING
    , setoff_sales_debit STRING
    , mta_ind STRING
    , rollover_option_flag STRING
    , rollover_day STRING
    , last_rollover_date TIMESTAMP
    , tdr_code_ir STRING
    , tdr_code_tr STRING
    , percentage_ir STRING
    , percentage_tr STRING
    , iss_settlement_buy STRING
    , iss_settlement_sell STRING
    , npl_date TIMESTAMP
    , setoff_sales_purchase STRING
    , prohibit_date TIMESTAMP
    , prohibit_user STRING
    , default_stmt_pword STRING
    , stmt_pword STRING
    , trading_platform STRING
    , lor_exempt STRING
    , gtd_bursa STRING
    , exempt_date TIMESTAMP
    , exempt_by STRING
    , crd_ownership STRING
    , bank_accno2 STRING
    , bank_branch2 STRING
    , bank_name2 STRING
    , fatca_acct_type STRING
    , fatca_acct_class STRING
    , fatca_entity_class STRING
    , fatca_doc_type STRING
    , fatca_doc_receiving_date TIMESTAMP
    , fatca_doc_expiry_date TIMESTAMP
    , eq_tracker STRING
    , bank_join_acc_name STRING
    , bank_join_acc_flag STRING
    , bjcard_member_no STRING
    , bjcard_member_flag STRING
    , bank_join_acc_flag2 STRING
    , bank_join_acc_name2 STRING
    , bank_join_acc_flag_foreign STRING
    , bank_join_acc_name_foreign STRING
    , bank_accno_foreign STRING
    , bank_branch_foreign STRING
    , bank_swift_code_foreign STRING
    , bank_name_foreign STRING
    , bjcard_reason STRING
    , jompay_ref1_code STRING
    , financing_concept_code STRING
    , staff_ind STRING
    , annual_income STRING
    , ccris_acc_status STRING
    , ccris_acc_status_date TIMESTAMP
    , installment_amt STRING
    , crs_tax STRING
    , leap_market STRING
    , vbip_flag STRING
    , vbip_group STRING
    , vbip_effective_date TIMESTAMP
    , vbip_threshold_amt STRING
    , vbip_rebate_percentage STRING
    , occupation_dup STRING
    , idss_flag STRING
    , idss_limit_percentage STRING
    , limit_multiplier STRING
    , ccris_occ_main STRING
    , ccris_occ_sub STRING
    , emp_sector STRING
    , residency_status STRING
    , approved_date TIMESTAMP
    , emp_type STRING
    , legal_status STRING
    , legal_status_date TIMESTAMP
    , ex_ccris STRING
    , ex_ccris_reason STRING
    , loan_utilised_date TIMESTAMP
    , app_ref_no STRING
    , fin_rate STRING
    , def_ori STRING
    , type_of_est STRING
    , probability_def STRING
    , loss_def STRING
    , guarantor_entity1 STRING
    , guarantor_entity2 STRING
    , guarantor_entity3 STRING
    , dob_dor1 TIMESTAMP
    , dob_dor2 TIMESTAMP
    , dob_dor3 TIMESTAMP
    , ind_sec_main STRING
    , ind_sec_sub STRING
    , ex_ccris_date TIMESTAMP
    , bnm_id STRING
    , exclude_sell_limit_formula STRING
    , cds_status STRING
    , exempt_auto_df STRING
    , einvoice STRING
    , sst_no STRING
    , df_fee_rate STRING
    , einvoice_date TIMESTAMP
    , perm_city STRING
    , perm_state_code STRING
    , perm_country STRING
    , vulnerable STRING
    , einvoice_email STRING
    ,dl_record_status       VARCHAR(10)
    ,dl_record_created_date TIMESTAMP
    ,dl_record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_ext_updated
SELECT
    client_no
    , bank_accno
    , bank_name
    , bank_branch
    , monthly_income
    , home_branch
    , eligible
    , perm_addr1
    , perm_addr2
    , perm_addr3
    , perm_addr4
    , perm_postcode
    , passport_no
    , comm_mgn_facility_date
    , exp_mgn_facility_date
    , int_code_con
    , int_code_ctr
    , int_code_bill
    , setoff_gain
    , setoff_trust_losses
    , setoff_trust_pur
    , pnl_acc_no_company
    , clr_acc_profit_pct
    , clr_acc_loss_pct
    , sbl_cds_acc_no
    , rss_flag
    , bo_ic_no_new
    , bo_ic_no_old
    , sbl_client_no
    , pledge_client_no
    , campaign_code
    , email
    , estimated_networth
    , smf
    , employer_name
    , date_of_birth
    , marital_status
    , bumi_status
    , basic_code
    , state_code
    , rcc_code
    , country_code
    , last_maint_date
    , last_maint_date_addr
    , last_maint_date_empl
    , approval_date
    , brk_sharing_ind
    , brk_sharing_effective_date
    , brk_sharing_intro_branch_tdr_code
    , brk_sharing_intro_branch_level1_pct
    , brk_sharing_intro_branch_tdr_level2_pct
    , brk_sharing_exec_branch_tdr_level2_pct
    , omnibus_no
    , excl_stmt_printing
    , multi_currency
    , market_maker
    , introduced_br
    , foreign_declare
    , foreign_declare_date
    , promotion_code
    , ecm_money_usr
    , amla_or
    , bursa
    , sgx
    , hkex
    , nyse
    , amex
    , nasdaq
    , pep_ind
    , amlatf
    , equities_form
    , ecmmoney_form
    , futures_form
    , treasury_form
    , ut_form
    , loan_form
    , others_form
    , equities_form_date
    , ecmmoney_form_date
    , futures_form_date
    , treasury_form_date
    , ut_form_date
    , loan_form_date
    , others_form_date
    , type_of_business
    , guarantor_name1
    , guarantor_ic1
    , guarantor_country1
    , type_of_guarantee1
    , guarantor_name2
    , guarantor_ic2
    , guarantor_country2
    , type_of_guarantee2
    , guarantor_name3
    , guarantor_ic3
    , guarantor_country3
    , type_of_guarantee3
    , corp_status
    , industrial_sector
    , priority_sector
    , nationality
    , offshore_sett
    , pdpa_form
    , fatca_form
    , us_taxpayer_no
    , disclosure_of_info
    , cp_ind
    , invest_type
    , mcd_acct_type
    , mcd_bo_cd
    , ediv_bank_acct_no
    , ediv_bank_code
    , consol_ediv
    , ediv_joint
    , mcd_race
    , mcd_nationality
    , foreign_curr_con
    , setoff_sales_debit
    , mta_ind
    , rollover_option_flag
    , rollover_day
    , last_rollover_date
    , tdr_code_ir
    , tdr_code_tr
    , percentage_ir
    , percentage_tr
    , iss_settlement_buy
    , iss_settlement_sell
    , npl_date
    , setoff_sales_purchase
    , prohibit_date
    , prohibit_user
    , default_stmt_pword
    , stmt_pword
    , trading_platform
    , lor_exempt
    , gtd_bursa
    , exempt_date
    , exempt_by
    , crd_ownership
    , bank_accno2
    , bank_branch2
    , bank_name2
    , fatca_acct_type
    , fatca_acct_class
    , fatca_entity_class
    , fatca_doc_type
    , fatca_doc_receiving_date
    , fatca_doc_expiry_date
    , eq_tracker
    , bank_join_acc_name
    , bank_join_acc_flag
    , bjcard_member_no
    , bjcard_member_flag
    , bank_join_acc_flag2
    , bank_join_acc_name2
    , bank_join_acc_flag_foreign
    , bank_join_acc_name_foreign
    , bank_accno_foreign
    , bank_branch_foreign
    , bank_swift_code_foreign
    , bank_name_foreign
    , bjcard_reason
    , jompay_ref1_code
    , financing_concept_code
    , staff_ind
    , annual_income
    , ccris_acc_status
    , ccris_acc_status_date
    , installment_amt
    , crs_tax
    , leap_market
    , vbip_flag
    , vbip_group
    , vbip_effective_date
    , vbip_threshold_amt
    , vbip_rebate_percentage
    , occupation_dup
    , idss_flag
    , idss_limit_percentage
    , limit_multiplier
    , ccris_occ_main
    , ccris_occ_sub
    , emp_sector
    , residency_status
    , approved_date
    , emp_type
    , legal_status
    , legal_status_date
    , ex_ccris
    , ex_ccris_reason
    , loan_utilised_date
    , app_ref_no
    , fin_rate
    , def_ori
    , type_of_est
    , probability_def
    , loss_def
    , guarantor_entity1
    , guarantor_entity2
    , guarantor_entity3
    , dob_dor1
    , dob_dor2
    , dob_dor3
    , ind_sec_main
    , ind_sec_sub
    , ex_ccris_date
    , bnm_id
    , exclude_sell_limit_formula
    , cds_status
    , exempt_auto_df
    , einvoice
    , sst_no
    , df_fee_rate
    , einvoice_date
    , perm_city
    , perm_state_code
    , perm_country
    , vulnerable
    , einvoice_email
    ,'A' AS dl_record_status
    ,dl_record_created_date
    ,dl_record_updated_date
FROM {params["com_schema"]}.t_mhbos_m_client_ext com_t
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.mhbos_m_client_ext r
    WHERE r.etl_dt = '{batch_date}'
      AND r.client_no = com_t.client_no
)
""")

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_t_mhbos_m_client_ext_updated
SELECT
    r.client_no
    , r.bank_accno
    , r.bank_name
    , r.bank_branch
    , r.monthly_income
    , r.home_branch
    , r.eligible
    , r.perm_addr1
    , r.perm_addr2
    , r.perm_addr3
    , r.perm_addr4
    , r.perm_postcode
    , r.passport_no
    , r.comm_mgn_facility_date
    , r.exp_mgn_facility_date
    , r.int_code_con
    , r.int_code_ctr
    , r.int_code_bill
    , r.setoff_gain
    , r.setoff_trust_losses
    , r.setoff_trust_pur
    , r.pnl_acc_no_company
    , r.clr_acc_profit_pct
    , r.clr_acc_loss_pct
    , r.sbl_cds_acc_no
    , r.rss_flag
    , r.bo_ic_no_new
    , r.bo_ic_no_old
    , r.sbl_client_no
    , r.pledge_client_no
    , r.campaign_code
    , r.email
    , r.estimated_networth
    , r.smf
    , r.employer_name
    , r.date_of_birth
    , r.marital_status
    , r.bumi_status
    , r.basic_code
    , r.state_code
    , r.rcc_code
    , r.country_code
    , r.last_maint_date
    , r.last_maint_date_addr
    , r.last_maint_date_empl
    , r.approval_date
    , r.brk_sharing_ind
    , r.brk_sharing_effective_date
    , r.brk_sharing_intro_branch_tdr_code
    , r.brk_sharing_intro_branch_level1_pct
    , r.brk_sharing_intro_branch_tdr_level2_pct
    , r.brk_sharing_exec_branch_tdr_level2_pct
    , r.omnibus_no
    , r.excl_stmt_printing
    , r.multi_currency
    , r.market_maker
    , r.introduced_br
    , r.foreign_declare
    , r.foreign_declare_date
    , r.promotion_code
    , r.ecm_money_usr
    , r.amla_or
    , r.bursa
    , r.sgx
    , r.hkex
    , r.nyse
    , r.amex
    , r.nasdaq
    , r.pep_ind
    , r.amlatf
    , r.equities_form
    , r.ecmmoney_form
    , r.futures_form
    , r.treasury_form
    , r.ut_form
    , r.loan_form
    , r.others_form
    , r.equities_form_date
    , r.ecmmoney_form_date
    , r.futures_form_date
    , r.treasury_form_date
    , r.ut_form_date
    , r.loan_form_date
    , r.others_form_date
    , r.type_of_business
    , r.guarantor_name1
    , r.guarantor_ic1
    , r.guarantor_country1
    , r.type_of_guarantee1
    , r.guarantor_name2
    , r.guarantor_ic2
    , r.guarantor_country2
    , r.type_of_guarantee2
    , r.guarantor_name3
    , r.guarantor_ic3
    , r.guarantor_country3
    , r.type_of_guarantee3
    , r.corp_status
    , r.industrial_sector
    , r.priority_sector
    , r.nationality
    , r.offshore_sett
    , r.pdpa_form
    , r.fatca_form
    , r.us_taxpayer_no
    , r.disclosure_of_info
    , r.cp_ind
    , r.invest_type
    , r.mcd_acct_type
    , r.mcd_bo_cd
    , r.ediv_bank_acct_no
    , r.ediv_bank_code
    , r.consol_ediv
    , r.ediv_joint
    , r.mcd_race
    , r.mcd_nationality
    , r.foreign_curr_con
    , r.setoff_sales_debit
    , r.mta_ind
    , r.rollover_option_flag
    , r.rollover_day
    , r.last_rollover_date
    , r.tdr_code_ir
    , r.tdr_code_tr
    , r.percentage_ir
    , r.percentage_tr
    , r.iss_settlement_buy
    , r.iss_settlement_sell
    , r.npl_date
    , r.setoff_sales_purchase
    , r.prohibit_date
    , r.prohibit_user
    , r.default_stmt_pword
    , r.stmt_pword
    , r.trading_platform
    , r.lor_exempt
    , r.gtd_bursa
    , r.exempt_date
    , r.exempt_by
    , r.crd_ownership
    , r.bank_accno2
    , r.bank_branch2
    , r.bank_name2
    , r.fatca_acct_type
    , r.fatca_acct_class
    , r.fatca_entity_class
    , r.fatca_doc_type
    , r.fatca_doc_receiving_date
    , r.fatca_doc_expiry_date
    , r.eq_tracker
    , r.bank_join_acc_name
    , r.bank_join_acc_flag
    , r.bjcard_member_no
    , r.bjcard_member_flag
    , r.bank_join_acc_flag2
    , r.bank_join_acc_name2
    , r.bank_join_acc_flag_foreign
    , r.bank_join_acc_name_foreign
    , r.bank_accno_foreign
    , r.bank_branch_foreign
    , r.bank_swift_code_foreign
    , r.bank_name_foreign
    , r.bjcard_reason
    , r.jompay_ref1_code
    , r.financing_concept_code
    , r.staff_ind
    , r.annual_income
    , r.ccris_acc_status
    , r.ccris_acc_status_date
    , r.installment_amt
    , r.crs_tax
    , r.leap_market
    , r.vbip_flag
    , r.vbip_group
    , r.vbip_effective_date
    , r.vbip_threshold_amt
    , r.vbip_rebate_percentage
    , r.occupation_dup
    , r.idss_flag
    , r.idss_limit_percentage
    , r.limit_multiplier
    , r.ccris_occ_main
    , r.ccris_occ_sub
    , r.emp_sector
    , r.residency_status
    , r.approved_date
    , r.emp_type
    , r.legal_status
    , r.legal_status_date
    , r.ex_ccris
    , r.ex_ccris_reason
    , r.loan_utilised_date
    , r.app_ref_no
    , r.fin_rate
    , r.def_ori
    , r.type_of_est
    , r.probability_def
    , r.loss_def
    , r.guarantor_entity1
    , r.guarantor_entity2
    , r.guarantor_entity3
    , r.dob_dor1
    , r.dob_dor2
    , r.dob_dor3
    , r.ind_sec_main
    , r.ind_sec_sub
    , r.ex_ccris_date
    , r.bnm_id
    , r.exclude_sell_limit_formula
    , r.cds_status
    , r.exempt_auto_df
    , r.einvoice
    , r.sst_no
    , r.df_fee_rate
    , r.einvoice_date
    , r.perm_city
    , r.perm_state_code
    , r.perm_country
    , r.vulnerable
    , r.einvoice_email
    ,'A' AS dl_record_status
    ,CASE WHEN com_t.client_no IS NOT NULL THEN com_t.dl_record_created_date
          ELSE current_timestamp() END AS dl_record_created_date
    ,current_timestamp() AS dl_record_updated_date
FROM {params["raw_schema"]}.mhbos_m_client_ext r
LEFT JOIN {params["com_schema"]}.t_mhbos_m_client_ext com_t
    ON r.client_no = com_t.client_no
WHERE r.etl_dt = '{batch_date}'
""")

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.t_mhbos_m_client_ext
SELECT
    client_no
    , bank_accno
    , bank_name
    , bank_branch
    , monthly_income
    , home_branch
    , eligible
    , perm_addr1
    , perm_addr2
    , perm_addr3
    , perm_addr4
    , perm_postcode
    , passport_no
    , comm_mgn_facility_date
    , exp_mgn_facility_date
    , int_code_con
    , int_code_ctr
    , int_code_bill
    , setoff_gain
    , setoff_trust_losses
    , setoff_trust_pur
    , pnl_acc_no_company
    , clr_acc_profit_pct
    , clr_acc_loss_pct
    , sbl_cds_acc_no
    , rss_flag
    , bo_ic_no_new
    , bo_ic_no_old
    , sbl_client_no
    , pledge_client_no
    , campaign_code
    , email
    , estimated_networth
    , smf
    , employer_name
    , date_of_birth
    , marital_status
    , bumi_status
    , basic_code
    , state_code
    , rcc_code
    , country_code
    , last_maint_date
    , last_maint_date_addr
    , last_maint_date_empl
    , approval_date
    , brk_sharing_ind
    , brk_sharing_effective_date
    , brk_sharing_intro_branch_tdr_code
    , brk_sharing_intro_branch_level1_pct
    , brk_sharing_intro_branch_tdr_level2_pct
    , brk_sharing_exec_branch_tdr_level2_pct
    , omnibus_no
    , excl_stmt_printing
    , multi_currency
    , market_maker
    , introduced_br
    , foreign_declare
    , foreign_declare_date
    , promotion_code
    , ecm_money_usr
    , amla_or
    , bursa
    , sgx
    , hkex
    , nyse
    , amex
    , nasdaq
    , pep_ind
    , amlatf
    , equities_form
    , ecmmoney_form
    , futures_form
    , treasury_form
    , ut_form
    , loan_form
    , others_form
    , equities_form_date
    , ecmmoney_form_date
    , futures_form_date
    , treasury_form_date
    , ut_form_date
    , loan_form_date
    , others_form_date
    , type_of_business
    , guarantor_name1
    , guarantor_ic1
    , guarantor_country1
    , type_of_guarantee1
    , guarantor_name2
    , guarantor_ic2
    , guarantor_country2
    , type_of_guarantee2
    , guarantor_name3
    , guarantor_ic3
    , guarantor_country3
    , type_of_guarantee3
    , corp_status
    , industrial_sector
    , priority_sector
    , nationality
    , offshore_sett
    , pdpa_form
    , fatca_form
    , us_taxpayer_no
    , disclosure_of_info
    , cp_ind
    , invest_type
    , mcd_acct_type
    , mcd_bo_cd
    , ediv_bank_acct_no
    , ediv_bank_code
    , consol_ediv
    , ediv_joint
    , mcd_race
    , mcd_nationality
    , foreign_curr_con
    , setoff_sales_debit
    , mta_ind
    , rollover_option_flag
    , rollover_day
    , last_rollover_date
    , tdr_code_ir
    , tdr_code_tr
    , percentage_ir
    , percentage_tr
    , iss_settlement_buy
    , iss_settlement_sell
    , npl_date
    , setoff_sales_purchase
    , prohibit_date
    , prohibit_user
    , default_stmt_pword
    , stmt_pword
    , trading_platform
    , lor_exempt
    , gtd_bursa
    , exempt_date
    , exempt_by
    , crd_ownership
    , bank_accno2
    , bank_branch2
    , bank_name2
    , fatca_acct_type
    , fatca_acct_class
    , fatca_entity_class
    , fatca_doc_type
    , fatca_doc_receiving_date
    , fatca_doc_expiry_date
    , eq_tracker
    , bank_join_acc_name
    , bank_join_acc_flag
    , bjcard_member_no
    , bjcard_member_flag
    , bank_join_acc_flag2
    , bank_join_acc_name2
    , bank_join_acc_flag_foreign
    , bank_join_acc_name_foreign
    , bank_accno_foreign
    , bank_branch_foreign
    , bank_swift_code_foreign
    , bank_name_foreign
    , bjcard_reason
    , jompay_ref1_code
    , financing_concept_code
    , staff_ind
    , annual_income
    , ccris_acc_status
    , ccris_acc_status_date
    , installment_amt
    , crs_tax
    , leap_market
    , vbip_flag
    , vbip_group
    , vbip_effective_date
    , vbip_threshold_amt
    , vbip_rebate_percentage
    , occupation_dup
    , idss_flag
    , idss_limit_percentage
    , limit_multiplier
    , ccris_occ_main
    , ccris_occ_sub
    , emp_sector
    , residency_status
    , approved_date
    , emp_type
    , legal_status
    , legal_status_date
    , ex_ccris
    , ex_ccris_reason
    , loan_utilised_date
    , app_ref_no
    , fin_rate
    , def_ori
    , type_of_est
    , probability_def
    , loss_def
    , guarantor_entity1
    , guarantor_entity2
    , guarantor_entity3
    , dob_dor1
    , dob_dor2
    , dob_dor3
    , ind_sec_main
    , ind_sec_sub
    , ex_ccris_date
    , bnm_id
    , exclude_sell_limit_formula
    , cds_status
    , exempt_auto_df
    , einvoice
    , sst_no
    , df_fee_rate
    , einvoice_date
    , perm_city
    , perm_state_code
    , perm_country
    , vulnerable
    , einvoice_email
    ,dl_record_status
    ,dl_record_created_date
    ,dl_record_updated_date
    ,'{batch_date}'       AS etl_dt
    ,current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_t_mhbos_m_client_ext_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.t_mhbos_m_client_ext COMPUTE STATISTICS""")

spark.stop()