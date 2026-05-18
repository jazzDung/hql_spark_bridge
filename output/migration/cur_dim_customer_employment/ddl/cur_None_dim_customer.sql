-- Purpose:    DDL/COM STATUS
-- Generated:  2026-05-18 08:07:26
-- Source:     cur_dim_customer_employment

DROP TABLE IF EXISTS ${com_schema}.None_dim_customer;

CREATE TABLE ${com_schema}.None_dim_customer(
    customer_id VARCHAR(20) comment ''
    , customer_employer_name VARCHAR(100) comment ''
    , customer_employer_industry VARCHAR(100) comment ''
    , customer_employer_type VARCHAR(100) comment ''
    , customer_amla_occupation VARCHAR(50) comment ''
    , customer_ccris_occupation VARCHAR(10) comment ''
    , source_name VARCHAR(10) comment ''
    , source_record_id VARCHAR(20) comment ''
    , source_update_date TIMESTAMP comment ''
    , customer_employer_type_of_business VARCHAR(200) comment ''
    -- Standard fields (Model 3a)
    , dl_record_status VARCHAR(10) comment 'A=Active'
    , dl_record_created_date TIMESTAMP comment 'First insert time'
    , dl_record_updated_date TIMESTAMP comment 'Last update time'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);