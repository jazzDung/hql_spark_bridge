-- Purpose:    RAW-DDL-CREATE TABLE
-- Author:     marco
-- Usage:      python $ETL_HOME/script/init.py raw general_reference_lookup
-- CreateDate: 20240529
-- FileType:   DDL
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 1.0 drop table if exists table
drop table if exists ${raw_schema}.general_reference_lookup;

-- 1.1 create table
create table ${raw_schema}.general_reference_lookup(
    reference_type string comment ''
    ,reference_code string comment ''
    ,reference_value string comment ''
    ,etl_timestamp string comment 'ETL_processing_time'
    ,source_name string comment 'Source identifier'
    ,reference_value_2 string comment ''
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
