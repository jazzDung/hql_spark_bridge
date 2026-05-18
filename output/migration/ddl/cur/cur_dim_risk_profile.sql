-- Purpose:    DDL/COM STATUS
-- Generated:  2026-05-15 08:21:55
-- Source:     cur_dim_risk_profile

DROP TABLE IF EXISTS ${cur_schema}.dim_risk_profile;

CREATE TABLE ${cur_schema}.dim_risk_profile(
    customer_id VARCHAR(20) comment ''
    , amla_risk_profile_date DATE comment ''
    , amlatf_risk VARCHAR(1) comment ''
    , estimated_networth VARCHAR(100) comment ''
    , annual_income INT comment ''
    , source_record_id VARCHAR(20) comment ''
    , source_update_date TIMESTAMP comment ''
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