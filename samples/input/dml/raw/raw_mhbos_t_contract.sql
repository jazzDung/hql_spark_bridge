-- Purpose:    RAW-DML,Load the data file into the target table's same-day partition
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_mhbos_t_contract
-- CreateDate: 20230809
-- FileType:   DML
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/raw_mhbos_t_contract_para.sql;

-- 1.1 drop external table partition
drop table if exists ${raw_schema}.mhbos_t_contract_et; 
create external table if not exists ${raw_schema}.mhbos_t_contract_et(
    con_no string -- 
    ,client_no string -- 
    ,tdr_code string -- 
    ,stock_no string -- 
    ,con_date timestamp -- 
    ,trs_no string -- 
    ,con_qty decimal(15,0) --
    ,net_qty decimal(15,0) --
    ,netted_qty decimal(15,0) --
    ,contra_qty decimal(15,0) --
    ,adv_qty decimal(15,0) --
    ,switch_qty decimal(15,0) --
    ,issue_del_qty decimal(15,0) --
    ,pledge_qty decimal(15,0) --
    ,reg_qty decimal(15,0) --
    ,contra_ind string -- 
    ,basis string -- 
    ,term_code string -- 
    ,amend_flag string -- 
    ,price decimal(13,6) -- 
    ,gross_brk decimal(9,2) -- 
    ,brokerage decimal(9,2) -- 
    ,commission decimal(9,2) -- 
    ,cstamp decimal(8,2) -- 
    ,cfee decimal(8,2) -- 
    ,con_value decimal(12,2) -- 
    ,buyin_comm decimal(9,2) -- 
    ,sduty decimal(8,2) -- 
    ,misc_amt decimal(8,2) -- 
    ,deduction decimal(8,2) -- 
    ,terminal_no string -- 
    ,order_no string -- 
    ,brk_rate decimal(7,4) -- 
    ,adv_value decimal(12,2) -- 
    ,adv_brk decimal(9,2) -- 
    ,adv_cstamp decimal(8,2) -- 
    ,adv_cfee decimal(8,2) -- 
    ,due_date timestamp -- 
    ,last_alloc_no decimal(4,0) -- 
    ,last_switch_no decimal(3,0) -- 
    ,last_issue_no decimal(3,0) -- 
    ,buy_in string -- 
    ,married string -- 
    ,currency string -- 
    ,f_price decimal(15,6) -- 
    ,client_type string -- 
    ,rollover_qty decimal(15,0) --
    ,date_created timestamp -- 
    ,paid_charges decimal(9,2) -- 
    ,org_accno string -- 
    ,alloc_ind string -- 
    ,del_due_date timestamp -- 
    ,b_s string -- 
    ,scrip string -- 
    ,approval string -- 
    ,notice_no string -- 
    ,tdr_earned decimal(9,2) -- 
    ,order_type string -- 
    ,exch_rate decimal(9,6) -- 
    ,tran_type string -- 
    ,amend_date timestamp -- 
    ,user_id string -- 
    ,tdr_sharing decimal(9,4) -- 
    ,min_brk string -- 
    ,foreign_rate decimal(7,4) -- 
    ,foreign_currency string -- 
    ,intraday_flag string -- 
    ,sub_tdr_code string -- 
    ,custodian_no string -- 
    ,print_date timestamp -- 
    ,new_amt decimal(12,2) -- 
    ,settle_mode string -- 
    ,confirmed string -- 
    ,net_amount decimal(12,2) -- 
    ,remarks string -- 
    ,part_qty_iss decimal(15,0) -- 
    ,part_qty_fdss decimal(15,0) -- 
    ,part_new_amt_iss decimal(12,2) -- 
    ,part_new_amt_fdss decimal(12,2) -- 
    ,fdss_sttl string -- 
    ,iss_sttl string -- 
    ,transfered string -- 
    ,auto_tran string -- 
    ,reverse_by string -- 
    ,reverse_date timestamp -- 
    ,trx_branch string -- 
    ,others_amt decimal(8,2) -- 
    ,dept_code string -- 
    ,traded_sec string -- 
    ,foreign_sec_no string -- 
    ,etrade_flag string -- 
    ,multicurrency string -- 
    ,f_exchange_code string -- 
    ,f_con_value decimal(12,2) -- 
    ,trade_type string -- 
    ,sett_type string -- 
    ,sub_con_no string -- 
    ,gl_accno string -- 
    ,brkg_gl_accno string -- 
    ,tdr_type string -- 
    ,product_type string -- 
    ,cq_type string -- 
    ,prog_id string -- 
    ,intraday_qty decimal(15,0) --
    ,contra_qty_for_intraday decimal(12,0) -- 
    ,intraday_etrade_qty decimal(15,0) --
    ,intraday_etrade_brk_amt decimal(12,2) -- 
    ,intraday_etrade_brk_rate decimal(7,4) -- 
    ,intraday_etrade_brk_min_amt decimal(12,2) -- 
    ,intraday_normal_qty decimal(15,0) --
    ,intraday_normal_brk_amt decimal(12,2) -- 
    ,intraday_normal_brk_rate decimal(7,4) -- 
    ,intraday_normal_brk_min_amt decimal(12,2) -- 
    ,etrade_qty decimal(15,0) --
    ,etrade_brk_amt decimal(12,2) -- 
    ,etrade_brk_rate decimal(7,4) -- 
    ,etrade_brk_min_amt decimal(12,2) -- 
    ,normal_qty decimal(15,0) --
    ,normal_brk_amt decimal(12,2) -- 
    ,normal_brk_rate decimal(7,4) -- 
    ,normal_brk_min_amt decimal(12,2) -- 
    ,cash_upfront string -- 
    ,jnl_br_brkg string -- 
    ,gst_amt decimal(12,2) -- 
    ,jnl_date_post timestamp -- 
    ,jnl_date_del timestamp -- 
    ,f_custodian_code string -- 
    ,gst_others_amt decimal(12,2) -- 
    ,gst_code_others_amt string -- 
    ,gst_rate_others_amt decimal(12,2) -- 
    ,gst_gross_brk decimal(12,2) -- 
    ,gst_code_gross_brk string -- 
    ,gst_rate_gross_brk decimal(12,2) -- 
    ,gst_cfee decimal(12,2) -- 
    ,gst_code_cfee string -- 
    ,gst_rate_cfee decimal(12,2) -- 
    ,gross_con_value decimal(12,2) -- 
    ,gst_cstamp decimal(12,2) -- 
    ,gst_code_cstamp string -- 
    ,gst_rate_cstamp decimal(12,2) -- 
    ,tax_inv_no string -- 
    ,credit_note_no string -- 
    ,credit_note_date timestamp -- 
    ,foreign_exchange_code string -- 
    ,tax_inv_date timestamp -- 
    ,bo_cds_acc_no string -- 
    ,islamic_ind string -- 
    ,f_gross_brk decimal(12,2) -- 
    ,f_cstamp decimal(12,2) -- 
    ,f_cfee decimal(12,2) -- 
    ,f_other_amt decimal(12,2) -- 
    ,f_gst_amt decimal(12,2) -- 
    ,f_gst_gross_brk decimal(12,2) -- 
    ,f_gst_cstamp decimal(12,2) -- 
    ,f_gst_cfee decimal(12,2) -- 
    ,f_gst_others_amt decimal(12,2) -- 
    ,raw_sys_time string -- 
)
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location '${itl_data_path}/mhbos_t_contract_i.${batch_date}.dat'
;


-- 2.0 drop history partition
alter table ${raw_schema}.mhbos_t_contract drop if exists partition ( etl_dt = '${retain_day}' );

-- 2.1 drop partition
alter table ${raw_schema}.mhbos_t_contract drop if exists partition ( etl_dt = '${batch_date}' );

-- 2.2 insert data to target table
insert into table ${raw_schema}.mhbos_t_contract partition ( etl_dt = '${batch_date}' )
select
    con_no -- 
    ,client_no -- 
    ,tdr_code -- 
    ,stock_no -- 
    ,con_date -- 
    ,trs_no -- 
    ,con_qty -- 
    ,net_qty -- 
    ,netted_qty -- 
    ,contra_qty -- 
    ,adv_qty -- 
    ,switch_qty -- 
    ,issue_del_qty -- 
    ,pledge_qty -- 
    ,reg_qty -- 
    ,contra_ind -- 
    ,basis -- 
    ,term_code -- 
    ,amend_flag -- 
    ,price -- 
    ,gross_brk -- 
    ,brokerage -- 
    ,commission -- 
    ,cstamp -- 
    ,cfee -- 
    ,con_value -- 
    ,buyin_comm -- 
    ,sduty -- 
    ,misc_amt -- 
    ,deduction -- 
    ,terminal_no -- 
    ,order_no -- 
    ,brk_rate -- 
    ,adv_value -- 
    ,adv_brk -- 
    ,adv_cstamp -- 
    ,adv_cfee -- 
    ,due_date -- 
    ,last_alloc_no -- 
    ,last_switch_no -- 
    ,last_issue_no -- 
    ,buy_in -- 
    ,married -- 
    ,currency -- 
    ,f_price -- 
    ,client_type -- 
    ,rollover_qty -- 
    ,date_created -- 
    ,paid_charges -- 
    ,org_accno -- 
    ,alloc_ind -- 
    ,del_due_date -- 
    ,b_s -- 
    ,scrip -- 
    ,approval -- 
    ,notice_no -- 
    ,tdr_earned -- 
    ,order_type -- 
    ,exch_rate -- 
    ,tran_type -- 
    ,amend_date -- 
    ,user_id -- 
    ,tdr_sharing -- 
    ,min_brk -- 
    ,foreign_rate -- 
    ,foreign_currency -- 
    ,intraday_flag -- 
    ,sub_tdr_code -- 
    ,custodian_no -- 
    ,print_date -- 
    ,new_amt -- 
    ,settle_mode -- 
    ,confirmed -- 
    ,net_amount -- 
    ,remarks -- 
    ,part_qty_iss -- 
    ,part_qty_fdss -- 
    ,part_new_amt_iss -- 
    ,part_new_amt_fdss -- 
    ,fdss_sttl -- 
    ,iss_sttl -- 
    ,transfered -- 
    ,auto_tran -- 
    ,reverse_by -- 
    ,reverse_date -- 
    ,trx_branch -- 
    ,others_amt -- 
    ,dept_code -- 
    ,traded_sec -- 
    ,foreign_sec_no -- 
    ,etrade_flag -- 
    ,multicurrency -- 
    ,f_exchange_code -- 
    ,f_con_value -- 
    ,trade_type -- 
    ,sett_type -- 
    ,sub_con_no -- 
    ,gl_accno -- 
    ,brkg_gl_accno -- 
    ,tdr_type -- 
    ,product_type -- 
    ,cq_type -- 
    ,prog_id -- 
    ,intraday_qty -- 
    ,contra_qty_for_intraday -- 
    ,intraday_etrade_qty -- 
    ,intraday_etrade_brk_amt -- 
    ,intraday_etrade_brk_rate -- 
    ,intraday_etrade_brk_min_amt -- 
    ,intraday_normal_qty -- 
    ,intraday_normal_brk_amt -- 
    ,intraday_normal_brk_rate -- 
    ,intraday_normal_brk_min_amt -- 
    ,etrade_qty -- 
    ,etrade_brk_amt -- 
    ,etrade_brk_rate -- 
    ,etrade_brk_min_amt -- 
    ,normal_qty -- 
    ,normal_brk_amt -- 
    ,normal_brk_rate -- 
    ,normal_brk_min_amt -- 
    ,cash_upfront -- 
    ,jnl_br_brkg -- 
    ,gst_amt -- 
    ,jnl_date_post -- 
    ,jnl_date_del -- 
    ,f_custodian_code -- 
    ,gst_others_amt -- 
    ,gst_code_others_amt -- 
    ,gst_rate_others_amt -- 
    ,gst_gross_brk -- 
    ,gst_code_gross_brk -- 
    ,gst_rate_gross_brk -- 
    ,gst_cfee -- 
    ,gst_code_cfee -- 
    ,gst_rate_cfee -- 
    ,gross_con_value -- 
    ,gst_cstamp -- 
    ,gst_code_cstamp -- 
    ,gst_rate_cstamp -- 
    ,tax_inv_no -- 
    ,credit_note_no -- 
    ,credit_note_date -- 
    ,foreign_exchange_code -- 
    ,tax_inv_date -- 
    ,bo_cds_acc_no -- 
    ,islamic_ind -- 
    ,f_gross_brk -- 
    ,f_cstamp -- 
    ,f_cfee -- 
    ,f_other_amt -- 
    ,f_gst_amt -- 
    ,f_gst_gross_brk -- 
    ,f_gst_cstamp -- 
    ,f_gst_cfee -- 
    ,f_gst_others_amt -- 
    ,raw_sys_time -- 
    ,'${batch_timestamp}' as etl_timestamp --ETL_processing time
from ${raw_schema}.mhbos_t_contract_et
;



