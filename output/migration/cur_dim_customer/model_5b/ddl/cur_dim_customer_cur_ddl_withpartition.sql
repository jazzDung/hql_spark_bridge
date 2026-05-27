-- Purpose:    DDL/CUR STATUS
-- Generated:  2026-05-22 13:55:03
-- Source:     cur_dim_customer

DROP TABLE IF EXISTS cur.DIM_CUSTOMER;

CREATE TABLE cur.DIM_CUSTOMER(
    customer_id VARCHAR(20) comment ''
    , customer_type VARCHAR(10) comment ''
    , customer_title VARCHAR(20) comment ''
    , customer_name VARCHAR(250) comment ''
    , customer_primary_identification_no_type VARCHAR(20) comment ''
    , customer_primary_identification_no VARCHAR(20) comment ''
    , customer_primary_identification_no_expiry_date DATE comment ''
    , customer_secondary_identification_no_type VARCHAR(20) comment ''
    , customer_secondary_identification_no VARCHAR(20) comment ''
    , customer_secondary_identification_no_expiry_date DATE comment ''
    , customer_nationality VARCHAR(2) comment ''
    , customer_country_of_residence VARCHAR(2) comment ''
    , customer_country_of_birth VARCHAR(2) comment ''
    , customer_date_of_birth DATE comment ''
    , customer_bumiputra_status VARCHAR(1) comment ''
    , customer_race VARCHAR(20) comment ''
    , customer_gender VARCHAR(10) comment ''
    , customer_marital_status VARCHAR(20) comment ''
    , customer_company_contact_person VARCHAR(100) comment ''
    , customer_company_ownership VARCHAR(5) comment ''
    , customer_company_country_of_registration VARCHAR(2) comment ''
    , customer_company_type_of_business VARCHAR(200) comment ''
    , customer_company_date_of_incorporation DATE comment ''
    , customer_company_website VARCHAR(100) comment ''
    , customer_company_type_of_organization VARCHAR(100) comment ''
    , pdpa_flag VARCHAR(1) comment ''
    , connected_party_flag VARCHAR(1) comment ''
    , politically_exposed_person_flag VARCHAR(3) comment ''
    , cross_selling_consent_flag VARCHAR(4) comment ''
    , dcf_flag VARCHAR(5) comment ''
    , multi_trading_account_flag VARCHAR(6) comment ''
    , fatca_flag VARCHAR(7) comment ''
    , crs_flag VARCHAR(8) comment ''
    , customer_company_personnel_designation VARCHAR(150) comment ''
    , customer_residency_status VARCHAR(1) comment ''
    , auto_einvoice_indicator VARCHAR(1) comment ''
    , sst_registration_no VARCHAR(20) comment ''
    , customer_company_country_of_business VARCHAR(2) comment ''
    , vulnerable_flag VARCHAR(1) comment ''
    -- Standard fields (Model 5b)
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