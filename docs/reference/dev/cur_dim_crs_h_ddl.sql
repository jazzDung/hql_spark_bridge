-- Purpose:    ddl/cur
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py cur dim_crs
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2023-09-11 create table
--     Version: 1.0

drop table if exists ${cur_schema}.dim_crs_h;

create table ${cur_schema}.dim_crs_h (
    customer_id varchar(20) comment ''
    ,crs_entity_type varchar(2) comment ''
    ,name_of_controlling_person_1 varchar(100) comment ''
    ,name_of_controlling_person_2 varchar(100) comment ''
    ,crs_country varchar(2) comment ''
    ,crs_tax_residence varchar(2) comment ''
    ,taxpayer_identification_no_1 varchar(20) comment ''
    ,taxpayer_identification_no_2 varchar(20) comment ''
    ,tin_unavailable_reason varchar(2) comment ''
    ,tin_remarks varchar(200) comment ''
    ,source_record_id varchar(20) comment ''
    ,priority_level INT
    ,source_update_date TIMESTAMP
    ,source_name varchar(10) comment ''
    ,source_key varchar(50)
    ,dl_record_status string
    ,hash_value             STRING
    ,etl_dt string
    ,start_timestamp timestamp
    ,end_timestamp timestamp
)
comment ''
partitioned by (
    log_period string COMMENT 'end_timestamp in YYYYMM'
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
;