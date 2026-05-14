-- Purpose:    DDL/COM STATUS
-- Generated:  2026-05-11 08:33:18
-- Source:     com_r_mhbos_m_client_crs

DROP TABLE IF EXISTS ${com_schema}.t_mhbos_m_client_crs;

CREATE TABLE ${com_schema}.t_mhbos_m_client_crs(
    client_no STRING comment ''
    , controlling_name STRING comment ''
    , country_tax_residence STRING comment ''
    , tax_identification_no STRING comment ''
    , reason STRING comment ''
    , reason_remarks STRING comment ''
    , entity_type STRING comment ''
    , crs_tax_type STRING comment ''
    -- Standard fields (Model 3a)
    , dl_record_status VARCHAR(10) comment 'A=Active'
    , dl_record_created_date DATE comment 'First insert time'
    , dl_record_updated_date DATE comment 'Last update time'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);