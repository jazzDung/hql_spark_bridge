-- Purpose:    RAW-DML,Load the data file into the target table's same-day partition
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_k2_bank
-- CreateDate: 20230809
-- FileType:   DML
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.1 drop external table partition
drop table if exists ${raw_schema}.k2_bank_et; 
create external table if not exists ${raw_schema}.k2_bank_et(
    bankid string -- 
    ,localbankcode string -- 
    ,bankname string -- 
    ,swiftcode string -- 
    ,oribankid string -- 
    ,approveuser bigint -- 
    ,approvets timestamp -- 
    ,createuser bigint -- 
    ,createts timestamp -- 
    ,updateuser bigint -- 
    ,updatets timestamp -- 
    ,recstatus string -- 
)
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location '${itl_data_path}/k2_bank_f.${batch_date}.dat'
;


-- 2.0 drop history partition
alter table ${raw_schema}.k2_bank drop if exists partition ( etl_dt = '${retain_day}' );

-- 2.1 drop partition
alter table ${raw_schema}.k2_bank drop if exists partition ( etl_dt = '${batch_date}' );

-- 2.2 insert data to target table
insert into table ${raw_schema}.k2_bank partition ( etl_dt = '${batch_date}' )
select
    bankid -- 
    ,nvl(trim(localbankcode), '') -- 
    ,bankname -- 
    ,nvl(trim(swiftcode), '') -- 
    ,oribankid -- 
    ,approveuser -- 
    ,approvets -- 
    ,createuser -- 
    ,createts -- 
    ,updateuser -- 
    ,updatets -- 
    ,nvl(trim(recstatus), '') -- 
    ,'${batch_timestamp}' as etl_timestamp --ETL_processing time
from ${raw_schema}.k2_bank_et
;



