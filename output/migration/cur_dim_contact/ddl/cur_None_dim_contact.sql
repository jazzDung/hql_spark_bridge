-- Purpose:    DDL/COM STATUS
-- Generated:  2026-05-14 09:44:44
-- Source:     cur_dim_contact

DROP TABLE IF EXISTS ${com_schema}.None_dim_contact;

CREATE TABLE ${com_schema}.None_dim_contact(
    owner_id VARCHAR(50) comment ''
    , contact_owner_type VARCHAR(20) comment ''
    , contact_type VARCHAR(10) comment ''
    , contact_value VARCHAR(150) comment ''
    , contact_name VARCHAR(100) comment ''
    , contact_create_date DATE comment ''
    , contact_update_date DATE comment ''
    , line_of_business VARCHAR(20) comment ''
    , source_name VARCHAR(10) comment ''
    , source_record_id VARCHAR(50) comment ''
    , sequence_no INT comment ''
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