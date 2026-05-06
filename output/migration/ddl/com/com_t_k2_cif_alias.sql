-- Purpose:    DDL/COM STATUS
-- Generated:  2026-05-06 00:24:50
-- Source:     com_r_k2_cif_alias

DROP TABLE IF EXISTS ${com_schema}.t_k2_cif_alias;

CREATE TABLE ${com_schema}.t_k2_cif_alias(
    cifaliasid STRING comment ''
    , cifid STRING comment ''
    , aliastype STRING comment ''
    , aliasvalue STRING comment ''
    , effectivefrom TIMESTAMP comment ''
    , effectiveto TIMESTAMP comment ''
    , updateuser BIGINT comment ''
    , updatets TIMESTAMP comment ''
    , id_mark STRING comment ''
    -- Standard fields (Model 3)
    , record_status VARCHAR(10) comment 'A=Active'
    , record_created_date TIMESTAMP comment 'First insert time'
    , record_updated_date TIMESTAMP comment 'Last update time'
    , hash_value STRING comment 'MD5 of business columns'
    , etl_dt STRING comment 'Batch run date'
    , etl_timestamp STRING comment 'ETL processing timestamp'
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);