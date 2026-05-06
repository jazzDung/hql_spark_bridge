-- Purpose:    DDL/COM STATUS
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py com r_k2_cif_alias
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2021-05-19 create table
--     Version: 1.2

-- 1.0 drop table if exists table
drop table if exists ${com_schema}.r_k2_cif_alias;

-- 1.1 create table
create table ${com_schema}.r_k2_cif_alias(
    cifaliasid string comment ''
    ,cifid string comment ''
    ,aliastype string comment ''
    ,aliasvalue string comment ''
    ,effectivefrom timestamp comment ''
    ,effectiveto timestamp comment ''
    ,updateuser bigint comment ''
    ,updatets timestamp comment ''
    ,etl_timestamp  string comment 'ETL_processing_time'
    ,start_dt string comment 'start_dt'
    ,end_dt string comment 'end_dt'
    ,id_mark string comment 'id_mark'
)
comment ''
partitioned by (
    part_id string comment 'partition_by_month'
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
;