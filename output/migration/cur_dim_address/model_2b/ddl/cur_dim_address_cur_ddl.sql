-- Purpose:    DDL/CUR STATUS
-- Generated:  2026-05-25 13:22:46
-- Source:     cur_dim_address

DROP TABLE IF EXISTS cur.DIM_ADDRESS;

CREATE TABLE cur.DIM_ADDRESS(
    owner_id VARCHAR(50) comment ''
    , address_owner_type VARCHAR(20) comment ''
    , address_type VARCHAR(10) comment ''
    , address_line_1 VARCHAR(100) comment ''
    , address_line_2 VARCHAR(100) comment ''
    , address_line_3 VARCHAR(100) comment ''
    , address_line_4 VARCHAR(100) comment ''
    , city VARCHAR(255) comment ''
    , state VARCHAR(50) comment ''
    , postcode VARCHAR(5) comment ''
    , country VARCHAR(3) comment ''
    , address_create_date DATE comment ''
    , address_update_date DATE comment ''
    , line_of_business VARCHAR(20) comment ''
    , source_record_id VARCHAR(50) comment ''
    -- Standard fields (Model 2b)
    , dl_record_status VARCHAR(10) comment 'A=Active'
    , dl_record_created_date TIMESTAMP comment 'First insert time'
    , dl_record_updated_date TIMESTAMP comment 'Last update time'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
    , source_name varchar(10) comment 'Source Name'
)
PARTITIONED BY (
    source_key varchar(50)
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);