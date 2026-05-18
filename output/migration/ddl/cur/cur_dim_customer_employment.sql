-- Purpose:    ddl/cur
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py icl dim_customer_employment
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2023-09-11 create table
--     Version: 1.0

drop table if exists ${cur_schema}.dim_customer_employment;

create table ${cur_schema}.dim_customer_employment(
    customer_id varchar(20) comment ''
    ,customer_employer_name varchar(100) comment ''
    ,customer_employer_industry varchar(100) comment ''
    ,customer_employer_type varchar(100) comment ''
    ,customer_amla_occupation varchar(100) comment ''
    ,customer_ccris_occupation varchar(10) comment ''
    ,source_record_id varchar(20) comment ''
    ,source_update_date timestamp comment ''
    ,customer_employer_type_of_business varchar(200) comment ''
    -- Standard fields (Model 3a)
    , dl_record_status VARCHAR(10) comment 'A=Active'
    , dl_record_created_date TIMESTAMP comment 'First insert time'
    , dl_record_updated_date TIMESTAMP comment 'Last update time'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
)
comment ''
partitioned by (
    source_name varchar(10) comment ''
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);