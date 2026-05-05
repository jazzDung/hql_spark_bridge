"""
Purpose:    RAW-DML,Load the data file into the target table's same-day partition
Author:     zjj
Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_mhbos_m_client
CreateDate: 20230809
FileType:   DML
Logs:
1.for hive 3.x on cdp 7.1.5
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

source_name = "mhbos"
table_name = "m_client"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# ext_start_time = datetime.strptime(ext_start_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# ext_end_time = datetime.strptime(ext_end_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)


# -- JDBC Read Optimization cho bảng dbo.m_client --
jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

query = f"""
SELECT a.client_no

,a.client_group

,a.cds_acc_no

,a.tdr_code

,a.client_name

,a.client_type

,a.margin

,a.last_margin_date

,a.int_rate

,a.auto_ded

,a.despatch_mode

,a.copies

,a.prohibit_trade

,a.custody_status

,a.race

,a.country

,a.ic_no_new

,a.ic_no_old

,a.margin_limit

,a.margin_pct

,a.rollover_rate

,a.form_completed

,a.last_tran_date

,a.ytd_bvalue

,a.ytd_svalue

,a.ytd_brokerage

,a.os_led_bal

,a.title

,a.addr1

,a.addr2

,a.addr3

,a.tel_no_home

,a.tel_no_office

,a.date_created

,a.stop_payt

,a.acc_payee

,a.category

,a.fax_no

,a.auto_contra

,a.pnl_acc_no

,a.cr_limit

,a.trust_bal

,a.avg_ind

,a.remarks

,a.contact_person

,a.date_closed

,a.grace_period

,a.acct_type

,a.assoc_ind

,a.short_sell_ind

,a.short_name

,a.mesdaq_pctlmt

,a.date_change

,a.resi_code

,a.sex

,a.charge_int

,a.bdebt

,a.assets

,a.liabilities

,a.income

,a.expenses

,a.bdebt_his_ind

,a.rel_ac1

,a.rel_ac2

,a.rel_ac3

,a.rel_ac4

,a.occupation

,a.margin_int

,a.lst_led_no

,a.cur_led_no

,a.remarks2

,a.acc_type

,a.mas_accno

,a.legal

,a.sell_limit

,a.brk_rate

,a.client_name1

,a.postcode

,a.brokerage_type

,a.cds_acc_no1

,a.remarks1

,a.payment_bank_code

,a.noms

,a.dms_date

,a.violation_date

,a.mcd_branch

,a.home_branch

,a.eaf_code

,a.call_warrant

,a.user_id

,a.credit_int_rate

,a.min_eligible_amt

,a.intraday_flag

,a.intraday_rate

,a.cta_weight

,a.sta_weight

,a.bo_cds_acc_no

,a.ecos_form

,a.custodian_no

,a.prin_acc

,a.armada_type

,a.old_authorisee

,a.etrade_rate

,a.etf

,a.cstamp_client_exempt

,a.main_branch

,a.prev_client_no

,a.web_eds

,a.place

,a.addr4

,a.excl_tdr_deduct

,a.excl_auto_susp

,a.trust_flag

,a.mgn_new_int_rate

,a.counter_concentration

,a.auto_trust

,a.margin_pct2

,a.df_flag

,a.mgn_curr_int_rate

,a.product_type

,a.web_ecos

,a.xeye_clt_grp

,a.bursa_violation_date

,a.client_name2

,a.brokerage_type_etrade

,a.brokerage_type_odd_lot

,a.omnibus

,a.cg_tdr_code

,a.limit_foreign

,a.limit_bursa

,a.brokerage_type_intraday

,a.brokerage_type_intraday_etrade

,a.bursa_violation_date1

,a.cif_no

,a.brokerage_type_foreign

,a.soft_copy

,a.exclude_rollover

,a.account_status

,a.w8ben

,a.ic_no_rel1

,a.ic_no_rel2

,a.ic_no_rel3

,a.ic_no_rel4

,a.ic_no_rel5

,a.rel1

,a.rel2

,a.rel3

,a.rel4

,a.rel5

,a.brokerage_type_etrade_b

,a.brokerage_type_odd_lot_b

,a.brokerage_type_b

,a.brokerage_type_foreign_b

,a.brokerage_type_intraday_b

,a.brokerage_type_intraday_etrade_b

,a.brokerage_type_etrade_s

,a.brokerage_type_odd_lot_s

,a.brokerage_type_s

,a.brokerage_type_foreign_s

,a.brokerage_type_intraday_s

,a.brokerage_type_intraday_etrade_s

,a.no_free_trade

,a.sms

,a.mobile_prefix

,a.mobile_no

,a.foreign_curr_set

,a.num_free_trade

,a.etrader_type

,a.check_limit

,a.auto_margin

,a.margin_client_no

,a.dup_despatch_mode

,a.risk

,a.exclude_trader_limit

,a.sett_mode_date_change

,a.pick_up_fee_pct

,a.e_payment

,a.mgn_new_int_rate2

,a.fund_cost_type

,a.check_share

,a.citibank_changes

,a.citibank_charges

,a.cq_market

,a.exclude_margin_pro_rate

,a.brokerage_type_cash_b

,a.brokerage_type_etrade_cash_b

,a.clt_consent

,a.consent_start_date

,a.portfolio

,a.expiry_date

,a.intraday_auto_contra_option

,a.dcf_limit

,a.brokerage_type_etb

,a.mgn_force_sell_pct

,a.mgn_tenure

,a.mgn_expiry_date

,a.loss_gl_acc_no

,a.portfolio_date

,a.day_prior_temp_susp

,a.day_prior_perm_susp

,a.gst_code

,a.match_price_decimal_local

,a.match_price_decimal_foreign

,a.id_type

,a.primary_id_expiry_date

,a.secondary_id_no

,a.secondary_id_expiry_date

,a.mgn_int_tdr_spread_pct

,a.mgn_base_int_rate

,a.mgn_int_tdr_share

,a.islamic_flag

,a.mcd_resident_flag

,a.chq_charges_flag

,a.chq_charges_tdr_pct

,a.brokerage_type_foreign_etrade

,a.brokerage_type_foreign_etrade_b

,a.brokerage_type_foreign_etrade_s

,a.grp_exch_code

,a.secondary_id_type

,a.bdebt_ras

,a.twse_declaration

,a.client_name3

,a.joint_acc_amt

,a.high_risk_market

,a.brokerage_type_leap_normal

,a.brokerage_type_leap_etrade

,'{ext_end_time}' as raw_sys_time

,a.type_of_account

FROM dbo.m_client (nolock) a

join

(

  select distinct client_no

  from dbo.a_m_client (nolock)

  where audit_type in ('A','E1')

  and audit_date >= convert(datetime,'{ext_start_time}',126)

  and audit_date < convert(datetime,'{ext_end_time}',126)

  union 

  select distinct client_no

  from dbo.m_client (nolock)

  where convert(date,date_change) >= convert(date,'{ext_start_time}')

  and convert(date,date_change) <= convert(date,'{ext_end_time}')

  union 

  select distinct client_no

  from dbo.m_client (nolock)

  where convert(date,last_tran_date) >= convert(date,'{ext_start_time}')

  and convert(date,last_tran_date) <= convert(date,'{ext_end_time}')

) b

on a.client_no = b.client_no
"""

# Read from MSSQL
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", query)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)

df = df.withColumn("etl_timestamp", current_timestamp().cast("string"))
df = df.withColumn("etl_dt", lit(batch_date))



# Write directly to Hive
target_table = f"{params['raw_schema']}.{hive_table_name}"
target_cols = spark.table(target_table).columns
df = df.select(*target_cols)

(
    df.write
    .mode("overwrite")
    .insertInto(f"{params['raw_schema']}.{hive_table_name}")
)

# Stop Spark when done
spark.stop()