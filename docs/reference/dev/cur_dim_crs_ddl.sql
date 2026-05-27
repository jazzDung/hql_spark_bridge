-- Purpose:    ddl/cur
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py cur dim_crs
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2023-09-11 create table
--     Version: 1.0

drop table if exists ${cur_schema}.dim_crs;

create table ${cur_schema}.dim_crs(
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
    , dl_record_status       VARCHAR(10)
    , hash_value             STRING
    , dl_record_created_date TIMESTAMP
    , dl_record_updated_date TIMESTAMP
    , etl_dt                 STRING
    , source_name            Varchar(20)
    , etl_timestamp          TIMESTAMP
)
comment ''
partitioned by (
    source_key            Varchar(50)
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
;