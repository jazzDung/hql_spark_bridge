-- Purpose:    RAW-DDL-CREATE TABLE
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/init.py raw k2_bank
-- CreateDate: 20230907
-- FileType:   DDL
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 1.0 drop table if exists table
drop table if exists ${raw_schema}.k2_bank;

-- 1.1 create table
create table ${raw_schema}.k2_bank(
    bankid string comment ''
    ,localbankcode string comment ''
    ,bankname string comment ''
    ,swiftcode string comment ''
    ,oribankid string comment ''
    ,approveuser bigint comment ''
    ,approvets timestamp comment ''
    ,createuser bigint comment ''
    ,createts timestamp comment ''
    ,updateuser bigint comment ''
    ,updatets timestamp comment ''
    ,recstatus string comment ''
    ,etl_timestamp string comment 'ETL_processing_time'
)
comment ''
partitioned by (
    etl_dt string comment 'partition by day'
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
;
