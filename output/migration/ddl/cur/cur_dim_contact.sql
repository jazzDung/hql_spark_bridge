-- Purpose:    ddl/cur
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py cur dim_contact
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2023-09-11 create table
--     Version: 1.0

drop table if exists ${cur_schema}.dim_contact;

create table ${cur_schema}.dim_contact(
    owner_id varchar(50) comment ''
    ,contact_owner_type varchar(20) comment ''
    ,contact_type varchar(10) comment ''
    ,contact_value varchar(150) comment ''
    ,contact_name varchar(100) comment ''
    ,contact_create_date date comment ''
    ,contact_update_date date comment ''
    ,line_of_business varchar(20) comment ''
    ,source_record_id varchar(50) comment ''
    ,sequence_no	int comment ''
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
)
;