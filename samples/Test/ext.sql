SELECT
a.client_no
, ,a.bank_accno
, ,a.bank_name
, ,a.bank_branch
, ,a.monthly_income
, ,a.home_branch
, ,a.eligible
, ,a.perm_addr1
, ,a.perm_addr2
, ,a.perm_addr3
, ,a.perm_addr4
, ,a.perm_postcode
, ,a.passport_no
, ,a.comm_mgn_facility_date
, ,a.exp_mgn_facility_date
, ,a.int_code_con
, ,a.int_code_ctr
, ,a.int_code_bill
, ,a.setoff_gain
, ,a.setoff_trust_losses
, ,a.setoff_trust_pur
, ,a.pnl_acc_no_company
, ,a.clr_acc_profit_pct
, ,a.clr_acc_loss_pct
, ,a.sbl_cds_acc_no
, ,a.rss_flag
, ,a.bo_ic_no_new
, ,a.bo_ic_no_old
, ,a.sbl_client_no
, ,a.pledge_client_no
, ,a.campaign_code
, ,a.email
, ,a.estimated_networth
, ,a.smf
, ,a.employer_name
, ,a.date_of_birth
, ,a.marital_status
, ,a.bumi_status
, ,a.basic_code
, ,a.state_code
, ,a.rcc_code
, ,a.country_code
, ,a.last_maint_date
, ,a.last_maint_date_addr
, ,a.last_maint_date_empl
, ,a.approval_date
, ,a.brk_sharing_ind
, ,a.brk_sharing_effective_date
, ,a.brk_sharing_intro_branch_tdr_code
, ,a.brk_sharing_intro_branch_level1_pct
, ,a.brk_sharing_intro_branch_tdr_level2_pct
, ,a.brk_sharing_exec_branch_tdr_level2_pct
, ,a.omnibus_no
, ,a.excl_stmt_printing
, ,a.multi_currency
, ,a.market_maker
, ,a.introduced_br
, ,a.foreign_declare
, ,a.foreign_declare_date
, ,a.promotion_code
, ,a.ecm_money_usr
, ,a.amla_or
, ,a.bursa
, ,a.sgx
, ,a.hkex
, ,a.nyse
, ,a.amex
, ,a.nasdaq
, ,a.pep_ind
, ,a.amlatf
, ,a.equities_form
, ,a.ecmmoney_form
, ,a.futures_form
, ,a.treasury_form
, ,a.ut_form
, ,a.loan_form
, ,a.others_form
, ,a.equities_form_date
, ,a.ecmmoney_form_date
, ,a.futures_form_date
, ,a.treasury_form_date
, ,a.ut_form_date
, ,a.loan_form_date
, ,a.others_form_date
, ,a.type_of_business
, ,a.guarantor_name1
, ,a.guarantor_ic1
, ,a.guarantor_country1
, ,a.type_of_guarantee1
, ,a.guarantor_name2
, ,a.guarantor_ic2
, ,a.guarantor_country2
, ,a.type_of_guarantee2
, ,a.guarantor_name3
, ,a.guarantor_ic3
, ,a.guarantor_country3
, ,a.type_of_guarantee3
, ,a.corp_status
, ,a.industrial_sector
, ,a.priority_sector
, ,a.nationality
, ,a.offshore_sett
, ,a.pdpa_form
, ,a.fatca_form
, ,a.us_taxpayer_no
, ,a.disclosure_of_info
, ,a.cp_ind
, ,a.invest_type
, ,a.mcd_acct_type
, ,a.mcd_bo_cd
, ,a.ediv_bank_acct_no
, ,a.ediv_bank_code
, ,a.consol_ediv
, ,a.ediv_joint
, ,a.mcd_race
, ,a.mcd_nationality
, ,a.foreign_curr_con
, ,a.setoff_sales_debit
, ,a.mta_ind
, ,a.rollover_option_flag
, ,a.rollover_day
, ,a.last_rollover_date
, ,a.tdr_code_ir
, ,a.tdr_code_tr
, ,a.percentage_ir
, ,a.percentage_tr
, ,a.iss_settlement_buy
, ,a.iss_settlement_sell
, ,a.npl_date
, ,a.setoff_sales_purchase
, ,a.prohibit_date
, ,a.prohibit_user
, ,a.default_stmt_pword
, ,a.stmt_pword
, ,a.trading_platform
, ,a.lor_exempt
, ,a.gtd_bursa
, ,a.exempt_date
, ,a.exempt_by
, ,a.crd_ownership
, ,a.bank_accno2
, ,a.bank_branch2
, ,a.bank_name2
, ,a.fatca_acct_type
, ,a.fatca_acct_class
, ,a.fatca_entity_class
, ,a.fatca_doc_type
, ,a.fatca_doc_receiving_date
, ,a.fatca_doc_expiry_date
, ,a.eq_tracker
, ,a.bank_join_acc_name
, ,a.bank_join_acc_flag
, ,a.bjcard_member_no
, ,a.bjcard_member_flag
, ,a.bank_join_acc_flag2
, ,a.bank_join_acc_name2
, ,a.bank_join_acc_flag_foreign
, ,a.bank_join_acc_name_foreign
, ,a.bank_accno_foreign
, ,a.bank_branch_foreign
, ,a.bank_swift_code_foreign
, ,a.bank_name_foreign
, ,a.bjcard_reason
, ,a.jompay_ref1_code
, ,a.financing_concept_code
, ,a.staff_ind
, ,a.annual_income
, ,a.ccris_acc_status
, ,a.ccris_acc_status_date
, ,a.installment_amt
, ,a.crs_tax
, ,a.leap_market
, ,a.vbip_flag
, ,a.vbip_group
, ,a.vbip_effective_date
, ,a.vbip_threshold_amt
, ,a.vbip_rebate_percentage
, ,a.occupation_dup
, ,a.idss_flag
, ,a.idss_limit_percentage
, ,a.limit_multiplier
, ,a.ccris_occ_main
, ,a.ccris_occ_sub
, ,a.emp_sector
, ,a.residency_status
, ,a.approved_date
, ,a.emp_type
, ,a.legal_status
, ,a.legal_status_date
, ,a.ex_ccris
, ,a.ex_ccris_reason
, ,a.loan_utilised_date
, ,a.app_ref_no
, ,a.fin_rate
, ,a.def_ori
, ,a.type_of_est
, ,a.probability_def
, ,a.loss_def
, ,a.guarantor_entity1
, ,a.guarantor_entity2
, ,a.guarantor_entity3
, ,a.dob_dor1
, ,a.dob_dor2
, ,a.dob_dor3
, ,a.ind_sec_main
, ,a.ind_sec_sub
, ,a.ex_ccris_date
, ,a.bnm_id
, ,a.exclude_sell_limit_formula
, ,a.cds_status
, ,a.exempt_auto_df
, ,&#x27;&#x24;&#x7b;end_timestamp&#x7d;&#x27; as raw_sys_time
, ,a.einvoice
, ,a.sst_no
, ,a.df_fee_rate
, ,a.einvoice_date
, ,a.perm_city
, ,a.perm_state_code
, ,a.perm_country
, ,a.vulnerable
, ,a.einvoice_email
, FROM &#x24;&#x7b;db_schema&#x7d;.m_client_ext &#x28;nolock&#x29; a
, join
, &#x28;
,   select distinct client_no
,   from &#x24;&#x7b;db_schema&#x7d;.a_m_client_ext &#x28;nolock&#x29;
,   where audit_type in &#x28;&#x27;A&#x27;,&#x27;E1&#x27;&#x29;
,   and audit_date &#x3e;&#x3d; convert&#x28;datetime,&#x27;&#x24;&#x7b;start_timestamp&#x7d;&#x27;,126&#x29;
,   and audit_date &#x3c; convert&#x28;datetime,&#x27;&#x24;&#x7b;end_timestamp&#x7d;&#x27;,126&#x29;
,   union
,   select distinct client_no
,   from &#x24;&#x7b;db_schema&#x7d;.m_client_ext &#x28;nolock&#x29;
,   where client_no in 
,   &#x28;
, 	select distinct client_no
,   	from &#x24;&#x7b;db_schema&#x7d;.m_client &#x28;nolock&#x29;
,   	where convert&#x28;date,date_change&#x29; &#x3e;&#x3d; convert&#x28;date,&#x27;&#x24;&#x7b;start_timestamp&#x7d;&#x27;&#x29;
,   	and convert&#x28;date,date_change&#x29; &#x3c;&#x3d; convert&#x28;date,&#x27;&#x24;&#x7b;end_timestamp&#x7d;&#x27;&#x29;
,   &#x29;
,   union 
,   select distinct client_no
,   from &#x24;&#x7b;db_schema&#x7d;.m_client_ext &#x28;nolock&#x29;
,   where client_no in
,   &#x28;
, 	select distinct client_no
, 	from &#x24;&#x7b;db_schema&#x7d;.m_client &#x28;nolock&#x29;
,   	where convert&#x28;date,last_tran_date&#x29; &#x3e;&#x3d; convert&#x28;date,&#x27;&#x24;&#x7b;start_timestamp&#x7d;&#x27;&#x29;
,  	and convert&#x28;date,last_tran_date&#x29; &#x3c;&#x3d; convert&#x28;date,&#x27;&#x24;&#x7b;end_timestamp&#x7d;&#x27;&#x29;
,   &#x29;
, &#x29; b
, on a.client_no &#x3d; b.client_no</sql>
