-- Purpose:    DDL/CUR STATUS
-- Generated:  2026-05-26 07:31:01
-- Source:     cur_dim_crs

DROP TABLE IF EXISTS cur.DIM_CRS;

CREATE TABLE cur.DIM_CRS(
    customer_id VARCHAR(20) comment ''
    , crs_entity_type VARCHAR(2) comment ''
    , name_of_controlling_person_1 VARCHAR(100) comment ''
    , name_of_controlling_person_2 VARCHAR(100) comment ''
    , crs_country VARCHAR(2) comment ''
    , crs_tax_residence VARCHAR(2) comment ''
    , taxpayer_identification_no_1 VARCHAR(20) comment ''
    , taxpayer_identification_no_2 VARCHAR(20) comment ''
    , tin_unavailable_reason VARCHAR(2) comment ''
    , tin_remarks VARCHAR(200) comment ''
    , source_record_id VARCHAR(20) comment ''
    , priority_level INT comment ''
    , source_update_date TIMESTAMP comment ''
    -- Standard fields (Model 5a)
    , dl_record_status VARCHAR(10) comment 'A=Active'
    , dl_record_created_date TIMESTAMP comment 'First insert time'
    , dl_record_updated_date TIMESTAMP comment 'Last update time'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
    , source_name varchar(10) comment 'Source Name'
)
PARTITIONED BY (
    source_key varchar(50)
    , <<partition_column>> <<data_type>>
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);