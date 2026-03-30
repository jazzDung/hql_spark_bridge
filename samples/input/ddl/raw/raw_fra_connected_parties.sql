-- Purpose:    RAW-DDL-CREATE TABLE
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/init.py raw fra_connected_parties
-- CreateDate: 20230907
-- FileType:   DDL
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 1.0 drop table if exists table
drop table if exists ${raw_schema}.fra_connected_parties;

-- 1.1 create table
create table ${raw_schema}.fra_connected_parties(
    staffname string comment ''
    ,staff_nric string comment ''
    ,staff_oldic string comment ''
    ,cp_name string comment ''
    ,cp_nric string comment ''
    ,cp_oldic string comment ''
    ,cp_relationship string comment ''
    ,actual_status string comment ''
    ,define_status string comment ''
    ,processdate timestamp comment ''
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
