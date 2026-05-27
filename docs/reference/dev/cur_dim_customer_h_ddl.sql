-- Purpose:    ddl/cur
-- Author:     Sunline
-- Usage:      python $ETL_HOME/script/init.py cur dim_customer
-- CreateDate: 20180515
-- FileType:   DDL
-- Logs:
--     sunlinedata 2023-09-11 create table
--     Version: 1.0
--      syhmi       20251023        change data length customer_company_type_of_business from 100 to 200
drop table if exists ${cur_schema}.dim_customer_h;

create table ${cur_schema}.dim_customer_h(
    customer_id varchar(20) comment ''
    ,customer_type varchar(10) comment ''
    ,customer_title varchar(20) comment ''
    ,customer_name varchar(250) comment ''
    ,customer_primary_identification_no_type varchar(20) comment ''
    ,customer_primary_identification_no varchar(20) comment ''
    ,customer_primary_identification_no_expiry_date date comment ''
    ,customer_secondary_identification_no_type varchar(20) comment ''
    ,customer_secondary_identification_no varchar(20) comment ''
    ,customer_secondary_identification_no_expiry_date date comment ''
    ,customer_nationality varchar(2) comment ''
    ,customer_country_of_residence varchar(2) comment ''
    ,customer_country_of_birth varchar(2) comment ''
    ,customer_date_of_birth date comment ''
    ,customer_bumiputra_status varchar(1) comment ''
    ,customer_race varchar(20) comment ''
    ,customer_gender varchar(10) comment ''
    ,customer_marital_status varchar(20) comment ''
    ,customer_company_contact_person varchar(100) comment ''
    ,customer_company_ownership varchar(5) comment ''
    ,customer_company_country_of_registration varchar(2) comment ''
    ,customer_company_type_of_business varchar(200) comment ''
    ,customer_company_date_of_incorporation date comment ''
    ,customer_company_website varchar(100) comment ''
    ,customer_company_type_of_organization varchar(100) comment ''
    ,pdpa_flag varchar(1) comment ''
    ,connected_party_flag varchar(1) comment ''
    ,politically_exposed_person_flag varchar(3) comment ''
    ,cross_selling_consent_flag varchar(4) comment ''
    ,dcf_flag varchar(5) comment ''
    ,multi_trading_account_flag varchar(6) comment ''
    ,fatca_flag varchar(7) comment ''
    ,crs_flag varchar(8) comment ''
    ,customer_company_personnel_designation varchar(150) comment ''
    ,customer_residency_status varchar(1) comment ''
    ,update_date timestamp
    ,auto_einvoice_indicator varchar(1) -- none
    ,sst_registration_no varchar(20) -- none
    ,customer_company_country_of_business varchar(2) -- none
    ,vulnerable_flag varchar(1) -- added new field on 20250715
    ,dl_record_status string
    ,dl_last_updated_source_name varchar(50)
    ,etl_dt string
    ,start_timestamp timestamp
    ,end_timestamp timestamp
)
comment ''
PARTITIONED BY ( 
  log_period string COMMENT 'end_timestamp in YYYYMM'
)
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
;
