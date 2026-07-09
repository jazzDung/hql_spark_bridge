import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append(os.environ["sys_path_py_script"])

from etl_common_function import run_etl, set_parameter, drop_partition_day


source_name = "lmskibb2"
table_name = "tbl_counterparty"
hive_table_name = source_name + "_" + table_name

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
batch_yyyymm = batch_date[:6]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

#Model 3
#Drop all temporary tables
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_updated""")

#Counter party transformation
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_tomerge""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_type""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_no""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_secondary_identification""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_identification_info""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_gender_info""")
spark.sql(f"""drop table if exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_date_of_birth_info""")

spark.sql(f"""
create table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all(
    record_id int comment ''
    ,function_id int comment ''
    ,counterparty_id int comment ''
    ,entry_date timestamp comment ''
    ,entity_id int comment ''
    ,entity_code string comment ''
    ,counterparty_class_id int comment ''
    ,counterparty_class_code string comment ''
    ,counterparty_type_list string comment ''
    ,counterparty_code string comment ''
    ,counterparty_name string comment ''
    ,counterparty_base_ccy_security_id int comment ''
    ,ccris_entity_type_id int comment ''
    ,ccris_customer_last_maintenance_date timestamp comment ''
    ,gender_code string comment ''
    ,title_id int comment ''
    ,title string comment ''
    ,identity_type_id int comment ''
    ,identity_type string comment ''
    ,identity_number string comment ''
    ,passport_expiry_date timestamp comment ''
    ,date_of_birth timestamp comment ''
    ,country_of_birth_country_id int comment ''
    ,country_of_birth_country_code string comment ''
    ,nationality_country_id int comment ''
    ,nationality_country_code string comment ''
    ,counterparty_country_id int comment ''
    ,counterparty_country_code string comment ''
    ,race_id int comment ''
    ,race_code string comment ''
    ,bumiputera_status_id int comment ''
    ,bumiputera_status string comment ''
    ,marital_status_id int comment ''
    ,marital_status_code string comment ''
    ,old_identity_number string comment ''
    ,website string comment ''
    ,counterparty_address_1 string comment ''
    ,counterparty_address_2 string comment ''
    ,counterparty_address_3 string comment ''
    ,counterparty_address_4 string comment ''
    ,counterparty_address_postal_code string comment ''
    ,counterparty_address_city string comment ''
    ,counterparty_address_state string comment ''
    ,counterparty_address_state_code string comment ''
    ,counterparty_address_country_code string comment ''
    ,counterparty_address_last_maintenance_date timestamp comment ''
    ,tax_status string comment ''
    ,bank_id int comment ''
    ,bank_code string comment ''
    ,bank_name string comment ''
    ,bank_account_number string comment ''
    ,bank_account_name string comment ''
    ,business_code_id int comment ''
    ,business_desc string comment ''
    ,residency_status_id int comment ''
    ,residency_status_code string comment ''
    ,bnm_residency_status string comment ''
    ,company_type_id int comment ''
    ,company_type string comment ''
    ,date_of_incorporation timestamp comment ''
    ,country_of_incorporation_country_id int comment ''
    ,country_of_incorporation_country_code string comment ''
    ,paid_up_capital_ccy_security_id int comment ''
    ,paid_up_capital_amount decimal(38,18) comment ''
    ,country_of_operation_country_id int comment ''
    ,country_of_operation_country_code string comment ''
    ,constitution string comment ''
    ,is_advisory_regulated int comment ''
    ,advisory_regulated_remarks string comment ''
    ,turnover_ccy_security_id int comment ''
    ,turnover_amount decimal(38,18) comment ''
    ,turnover_projected_amount decimal(38,18) comment ''
    ,turnover_total_borrowing_amount decimal(38,18) comment ''
    ,is_ownership_structure_complex int comment ''
    ,ownership_structure_remarks string comment ''
    ,ccris_corporate_status_id int comment ''
    ,ccris_industrial_sector_id int comment ''
    ,next_of_kin_1_name string comment ''
    ,next_of_kin_1_identity_type_id int comment ''
    ,next_of_kin_1_identity_number string comment ''
    ,next_of_kin_1_relationship_id int comment ''
    ,next_of_kin_1_phone_no string comment ''
    ,next_of_kin_1_occupation_id int comment ''
    ,next_of_kin_1_employer_name string comment ''
    ,next_of_kin_2_name string comment ''
    ,next_of_kin_2_identity_type_id int comment ''
    ,next_of_kin_2_identity_number string comment ''
    ,next_of_kin_2_relationship_id int comment ''
    ,next_of_kin_2_phone_no string comment ''
    ,next_of_kin_2_occupation_id int comment ''
    ,next_of_kin_2_employer_name string comment ''
    ,source_of_income string comment ''
    ,other_source_of_income string comment ''
    ,pdpa_connected_party string comment ''
    ,pdpa string comment ''
    ,pdpa_disclosure_reason string comment ''
    ,is_domestic_myr_borrow int comment ''
    ,remaining_balance_from_investment_ccy_security_id int comment ''
    ,remaining_balance_from_investment decimal(38,18) comment ''
    ,currently_invested_ccy_security_id int comment ''
    ,currently_invested decimal(38,18) comment ''
    ,is_fatca int comment ''
    ,fatca_account_type_id int comment ''
    ,fatca_account_class_id int comment ''
    ,is_different_mailing_address int comment ''
    ,correspondence_address_1 string comment ''
    ,correspondence_address_2 string comment ''
    ,correspondence_address_3 string comment ''
    ,correspondence_address_4 string comment ''
    ,correspondence_address_postal_code string comment ''
    ,correspondence_address_city string comment ''
    ,correspondence_address_state string comment ''
    ,correspondence_address_state_code string comment ''
    ,correspondence_address_country_code string comment ''
    ,phone_no string comment ''
    ,home_phone_no string comment ''
    ,office_phone_no string comment ''
    ,contact_person_phone_no string comment ''
    ,contact_person_name string comment ''
    ,email_address string comment ''
    ,employment_status_id int comment ''
    ,employment_status string comment ''
    ,name_of_company string comment ''
    ,employer_address_1 string comment ''
    ,employer_address_2 string comment ''
    ,employer_address_3 string comment ''
    ,employer_address_4 string comment ''
    ,employer_address_postal_code string comment ''
    ,employer_address_city string comment ''
    ,employer_address_state string comment ''
    ,employer_address_country_code string comment ''
    ,aml_occupation_id int comment ''
    ,aml_occupation string comment ''
    ,ccris_occupation_id int comment ''
    ,ccris_occupation string comment ''
    ,ccris_occupation_code string comment ''
    ,employer_business_code_id int comment ''
    ,employer_business_desc string comment ''
    ,ccris_employment_sector_id int comment ''
    ,ccris_employment_sector_code string comment ''
    ,ccris_employment_type_id int comment ''
    ,ccris_employment_type_code string comment ''
    ,gross_annual_income_date timestamp comment ''
    ,gross_annual_income_ccy_security_id int comment ''
    ,gross_annual_income_amount decimal(38,18) comment ''
    ,country_of_resident_declaration_id int comment ''
    ,mlrpc_risk_id int comment ''
    ,cif_document string comment ''
    ,record_status_id int comment ''
    ,remarks string comment ''
    ,review_date timestamp comment ''
    ,effective_from timestamp comment ''
    ,effective_to timestamp comment ''
    ,user_effective_to timestamp comment ''
    ,update_record_id int comment ''
    ,action_type_id int comment ''
    ,action_type_snapshot_id int comment ''
    ,last_action_by string comment ''
    ,last_action_datetime timestamp comment ''
    ,system_remarks string comment ''
    ,system_updated_datetime timestamp comment ''
    ,related_function_id int comment ''
    ,related_record_id int comment ''
    ,cif_annual_income_id int comment ''
    ,cif_net_worth_id int comment ''
    ,cif_net_worth string comment ''
    ,is_politically_exposed_person string comment ''
    ,is_pdpa_disclosure string comment ''
    ,collateral_reference_number string comment ''
    ,e_invoice_indicator_id int comment ''
    ,e_invoice_indicator string comment ''
    ,customer_sst_registration_number string comment ''
    ,business_code string comment ''
    ,employer_business_code string comment ''
    ,etl_timestamp string
    ,etl_dt string
  )
  stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
""")

#WHERE
#        ETL_DT = '{batch_date}'
#        AND RECORD_STATUS_ID = 3
#        AND TO_DATE('{batch_date}', 'yyyyMMdd') BETWEEN EFFECTIVE_FROM AND EFFECTIVE_TO
#        -- Only get customer data
#        AND COUNTERPARTY_TYPE_LIST LIKE '%CUST%'

#-- 2.2 insert data to target table
spark.sql(f"""
WITH COUNTERPARTY_RANKED AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY COUNTERPARTY_ID
            ORDER BY SYSTEM_UPDATED_DATETIME DESC, LAST_ACTION_DATETIME DESC
        ) RN
    FROM {params["raw_schema"]}.LMSKIBB2_TBL_COUNTERPARTY
    WHERE
        ETL_DT = '{batch_date}'
        AND RECORD_STATUS_ID = 3
        AND TO_DATE('{batch_date}', 'yyyyMMdd') BETWEEN EFFECTIVE_FROM AND EFFECTIVE_TO
        -- Only get customer data
        AND COUNTERPARTY_TYPE_LIST LIKE '%CUST%'
)
INSERT INTO TABLE {params["com_schema"]}.TEMP_T_LMSKIBB2_TBL_COUNTERPARTY_ALL
SELECT
    T0.RECORD_ID
    , T0.FUNCTION_ID
    , T0.COUNTERPARTY_ID
    , T0.ENTRY_DATE
    , T0.ENTITY_ID
    , T17.ENTITY_CODE AS ENTITY_CODE
    , T0.COUNTERPARTY_CLASS_ID
    , T16.COUNTERPARTY_CLASS_CODE AS COUNTERPARTY_CLASS_CODE
    , T0.COUNTERPARTY_TYPE_LIST
    , T0.COUNTERPARTY_CODE
    , T0.COUNTERPARTY_NAME
    , T0.COUNTERPARTY_BASE_CCY_SECURITY_ID
    , T0.CCRIS_ENTITY_TYPE_ID
    , T0.CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE
    , T0.GENDER_CODE
    , T0.TITLE_ID
    , T18.TITLE AS TITLE
    , T0.IDENTITY_TYPE_ID
    , T1.E_INVOICE_IDENTITY_TYPE AS IDENTITY_TYPE
    , T0.IDENTITY_NUMBER
    , T0.PASSPORT_EXPIRY_DATE
    , T0.DATE_OF_BIRTH
    , T0.COUNTRY_OF_BIRTH_COUNTRY_ID
    , T15.COUNTRY_CODE AS COUNTRY_OF_BIRTH_COUNTRY_CODE
    , T0.NATIONALITY_COUNTRY_ID
    , T2.COUNTRY_CODE AS NATIONALITY_COUNTRY_CODE
    , T0.COUNTERPARTY_COUNTRY_ID
    , T2.COUNTRY_CODE AS COUNTERPARTY_COUNTRY_CODE
    , T0.RACE_ID
    , T5.RACE_CODE AS RACE_CODE
    , T0.BUMIPUTERA_STATUS_ID
    , T4.BUMIPUTERA_STATUS AS BUMIPUTERA_STATUS
    , T0.MARITAL_STATUS_ID
    , T6.MARITAL_STATUS_CODE AS MARITAL_STATUS_CODE
    , T0.OLD_IDENTITY_NUMBER
    , T0.WEBSITE
    , T0.COUNTERPARTY_ADDRESS_1
    , T0.COUNTERPARTY_ADDRESS_2
    , T0.COUNTERPARTY_ADDRESS_3
    , T0.COUNTERPARTY_ADDRESS_4
    , T0.COUNTERPARTY_ADDRESS_POSTAL_CODE
    , T0.COUNTERPARTY_ADDRESS_CITY
    , T0.COUNTERPARTY_ADDRESS_STATE
    , T25.STATE_CODE AS COUNTERPARTY_ADDRESS_STATE_CODE
    , T0.COUNTERPARTY_ADDRESS_COUNTRY_CODE
    , T0.COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE
    , T0.TAX_STATUS
    , T0.BANK_ID
    , T19.BANK_CODE AS BANK_CODE
    , T19.BANK_NAME AS BANK_NAME
    , T0.BANK_ACCOUNT_NUMBER
    , T0.BANK_ACCOUNT_NAME
    , T0.BUSINESS_CODE_ID
    , T8.BUSINESS_DESC AS BUSINESS_DESC
    , T0.RESIDENCY_STATUS_ID
    , T20.RESIDENCY_STATUS_CODE AS RESIDENCY_STATUS_CODE
    , T0.BNM_RESIDENCY_STATUS
    , T0.COMPANY_TYPE_ID
    , T21.COMPANY_TYPE AS COMPANY_TYPE
    , T0.DATE_OF_INCORPORATION
    , T0.COUNTRY_OF_INCORPORATION_COUNTRY_ID
    , T7.COUNTRY_CODE AS COUNTRY_OF_INCORPORATION_COUNTRY_CODE
    , T0.PAID_UP_CAPITAL_CCY_SECURITY_ID
    , T0.PAID_UP_CAPITAL_AMOUNT
    , T0.COUNTRY_OF_OPERATION_COUNTRY_ID
    , T9.COUNTRY_CODE AS COUNTRY_OF_OPERATION_COUNTRY_CODE
    , T0.CONSTITUTION
    , T0.IS_ADVISORY_REGULATED
    , T0.ADVISORY_REGULATED_REMARKS
    , T0.TURNOVER_CCY_SECURITY_ID
    , T0.TURNOVER_AMOUNT
    , T0.TURNOVER_PROJECTED_AMOUNT
    , T0.TURNOVER_TOTAL_BORROWING_AMOUNT
    , T0.IS_OWNERSHIP_STRUCTURE_COMPLEX
    , T0.OWNERSHIP_STRUCTURE_REMARKS
    , T0.CCRIS_CORPORATE_STATUS_ID
    , T0.CCRIS_INDUSTRIAL_SECTOR_ID
    , T0.NEXT_OF_KIN_1_NAME
    , T0.NEXT_OF_KIN_1_IDENTITY_TYPE_ID
    , T0.NEXT_OF_KIN_1_IDENTITY_NUMBER
    , T0.NEXT_OF_KIN_1_RELATIONSHIP_ID
    , T0.NEXT_OF_KIN_1_PHONE_NO
    , T0.NEXT_OF_KIN_1_OCCUPATION_ID
    , T0.NEXT_OF_KIN_1_EMPLOYER_NAME
    , T0.NEXT_OF_KIN_2_NAME
    , T0.NEXT_OF_KIN_2_IDENTITY_TYPE_ID
    , T0.NEXT_OF_KIN_2_IDENTITY_NUMBER
    , T0.NEXT_OF_KIN_2_RELATIONSHIP_ID
    , T0.NEXT_OF_KIN_2_PHONE_NO
    , T0.NEXT_OF_KIN_2_OCCUPATION_ID
    , T0.NEXT_OF_KIN_2_EMPLOYER_NAME
    , T0.SOURCE_OF_INCOME
    , T0.OTHER_SOURCE_OF_INCOME
    , T0.PDPA_CONNECTED_PARTY
    , T0.PDPA
    , T0.PDPA_DISCLOSURE_REASON
    , T0.IS_DOMESTIC_MYR_BORROW
    , T0.REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID
    , T0.REMAINING_BALANCE_FROM_INVESTMENT
    , T0.CURRENTLY_INVESTED_CCY_SECURITY_ID
    , T0.CURRENTLY_INVESTED
    , T0.IS_FATCA
    , T0.FATCA_ACCOUNT_TYPE_ID
    , T0.FATCA_ACCOUNT_CLASS_ID
    , T0.IS_DIFFERENT_MAILING_ADDRESS
    , T0.CORRESPONDENCE_ADDRESS_1
    , T0.CORRESPONDENCE_ADDRESS_2
    , T0.CORRESPONDENCE_ADDRESS_3
    , T0.CORRESPONDENCE_ADDRESS_4
    , T0.CORRESPONDENCE_ADDRESS_POSTAL_CODE
    , T0.CORRESPONDENCE_ADDRESS_CITY
    , T0.CORRESPONDENCE_ADDRESS_STATE
    , T26.STATE_CODE AS CORRESPONDENCE_ADDRESS_STATE_CODE
    , T0.CORRESPONDENCE_ADDRESS_COUNTRY_CODE
    , T0.PHONE_NO
    , T0.HOME_PHONE_NO
    , T0.OFFICE_PHONE_NO
    , T0.CONTACT_PERSON_PHONE_NO
    , T0.CONTACT_PERSON_NAME
    , T0.EMAIL_ADDRESS
    , T0.EMPLOYMENT_STATUS_ID
    , T23.EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS
    , T0.NAME_OF_COMPANY
    , T0.EMPLOYER_ADDRESS_1
    , T0.EMPLOYER_ADDRESS_2
    , T0.EMPLOYER_ADDRESS_3
    , T0.EMPLOYER_ADDRESS_4
    , T0.EMPLOYER_ADDRESS_POSTAL_CODE
    , T0.EMPLOYER_ADDRESS_CITY
    , T0.EMPLOYER_ADDRESS_STATE
    , T0.EMPLOYER_ADDRESS_COUNTRY_CODE
    , T0.AML_OCCUPATION_ID
    , T10.OCCUPATION AS AML_OCCUPATION
    , T0.CCRIS_OCCUPATION_ID
    , T11.OCCUPATION AS CCRIS_OCCUPATION
    , T27.OCCUPATION_CODE AS CCRIS_OCCUPATION_CODE
    , T0.EMPLOYER_BUSINESS_CODE_ID
    , T24.BUSINESS_DESC AS EMPLOYER_BUSINESS_DESC
    , T0.CCRIS_EMPLOYMENT_SECTOR_ID
    , T12.EMPLOYMENT_SECTOR_CODE AS CCRIS_EMPLOYMENT_SECTOR_CODE
    , T0.CCRIS_EMPLOYMENT_TYPE_ID
    , T14.CCRIS_EMPLOYMENT_TYPE_CODE AS CCRIS_EMPLOYMENT_TYPE_CODE
    , T0.GROSS_ANNUAL_INCOME_DATE
    , T0.GROSS_ANNUAL_INCOME_CCY_SECURITY_ID
    , T0.GROSS_ANNUAL_INCOME_AMOUNT
    , T0.COUNTRY_OF_RESIDENT_DECLARATION_ID
    , T0.MLRPC_RISK_ID
    , T0.CIF_DOCUMENT
    , T0.RECORD_STATUS_ID
    , T0.REMARKS
    , T0.REVIEW_DATE
    , T0.EFFECTIVE_FROM
    , T0.EFFECTIVE_TO
    , T0.USER_EFFECTIVE_TO
    , T0.UPDATE_RECORD_ID
    , T0.ACTION_TYPE_ID
    , T0.ACTION_TYPE_SNAPSHOT_ID
    , T0.LAST_ACTION_BY
    , T0.LAST_ACTION_DATETIME
    , T0.SYSTEM_REMARKS
    , T0.SYSTEM_UPDATED_DATETIME
    , T0.RELATED_FUNCTION_ID
    , T0.RELATED_RECORD_ID
    , T0.CIF_ANNUAL_INCOME_ID
    , T0.CIF_NET_WORTH_ID
    , T13.CIF_NET_WORTH AS CIF_NET_WORTH
    , T0.IS_POLITICALLY_EXPOSED_PERSON
    , T0.IS_PDPA_DISCLOSURE
    , T0.COLLATERAL_REFERENCE_NUMBER
    , T0.E_INVOICE_INDICATOR_ID
    , T22.E_INVOICE_INDICATOR
    , T0.CUSTOMER_SST_REGISTRATION_NUMBER
    , T8.BUSINESS_CODE AS BUSINESS_CODE
    , T24.BUSINESS_CODE AS EMPLOYER_BUSINESS_CODE
    , T0.ETL_TIMESTAMP AS ETL_TIMESTAMP -- etl processing time
    , T0.ETL_DT AS ETL_DT
FROM COUNTERPARTY_RANKED T0
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_IDENTITYTYPE T1
    ON T0.IDENTITY_TYPE_ID = T1.IDENTITY_TYPE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTRY T2
    ON T0.NATIONALITY_COUNTRY_ID = T2.COUNTRY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTRY T3
    ON T0.COUNTERPARTY_COUNTRY_ID = T3.COUNTRY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_BUMIPUTERASTATUS T4
    ON T0.BUMIPUTERA_STATUS_ID = T4.BUMIPUTERA_STATUS_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISRACE T5
    ON T0.RACE_ID = T5.RACE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_MARITALSTATUS T6
    ON T0.MARITAL_STATUS_ID = T6.MARITAL_STATUS_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTRY T7
    ON T0.COUNTRY_OF_INCORPORATION_COUNTRY_ID = T7.COUNTRY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_BUSINESSCODE T8
    ON T0.BUSINESS_CODE_ID = T8.BUSINESS_CODE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTRY T9
    ON T0.COUNTRY_OF_OPERATION_COUNTRY_ID = T9.COUNTRY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_OCCUPATION T10
    ON T0.AML_OCCUPATION_ID = T10.OCCUPATION_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_OCCUPATION T11
    ON T0.CCRIS_OCCUPATION_ID = T11.OCCUPATION_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISEMPLOYMENTSECTOR T12
    ON T0.CCRIS_EMPLOYMENT_SECTOR_ID = T12.CCRIS_EMPLOYMENT_SECTOR_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CIFNETWORTH T13
    ON T0.CIF_NET_WORTH_ID = T13.CIF_NET_WORTH_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISEMPLOYMENTTYPE T14
    ON T0.CCRIS_EMPLOYMENT_TYPE_ID = T14.CCRIS_EMPLOYMENT_TYPE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTRY T15
    ON T0.COUNTRY_OF_BIRTH_COUNTRY_ID = T15.COUNTRY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COUNTERPARTYCLASS T16
    ON T0.COUNTERPARTY_CLASS_ID = T16.COUNTERPARTY_CLASS_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_ENTITY T17
    ON T0.ENTITY_ID = T17.ENTITY_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_TITLE T18
    ON T0.TITLE_ID = T18.TITLE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_BANK T19
    ON T0.BANK_ID = T19.BANK_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISRESIDENCYSTATUS T20
    ON T0.RESIDENCY_STATUS_ID = T20.RESIDENCY_STATUS_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_COMPANYTYPE T21
    ON T0.COMPANY_TYPE_ID = T21.COMPANY_TYPE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_EINVOICEINDICATOR T22
    ON T0.E_INVOICE_INDICATOR_ID = T22.E_INVOICE_INDICATOR_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_EMPLOYMENTSTATUS T23
    ON T0.EMPLOYMENT_STATUS_ID = T23.EMPLOYMENT_STATUS_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_BUSINESSCODE T24
    ON T0.EMPLOYER_BUSINESS_CODE_ID = T24.BUSINESS_CODE_ID
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISSTATE T25
    ON T0.COUNTERPARTY_ADDRESS_STATE = T25.STATE_NAME
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISSTATE T26
    ON T0.CORRESPONDENCE_ADDRESS_STATE = T26.STATE_NAME
LEFT JOIN {params["com_schema"]}.T_LMSKIBB2_TBL_CCRISOCCUPATION T27
    ON T0.CCRIS_OCCUPATION_ID = T27.CCRIS_OCCUPATION_ID
WHERE
    T0.RN = 1
""")


#-- 2.2 Create a temporary table temp_lmskibb2_tbl_counterparty_primary_identification_type to store the cleaned primary identification type.
spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_type
(
  record_id string,
  IDENTITY_TYPE string,
  primary_identification_type string,
  primary_identification_type_flag string
)
""")

spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_type
select
    record_id
    , IDENTITY_TYPE AS IDENTITY_TYPE
    , case
        when IDENTITY_TYPE in ('1', '2', '3', '4', '5', '6') then IDENTITY_TYPE
        else '@[' || IDENTITY_TYPE || ']'
        end as primary_identification_type
    , case
        when IDENTITY_TYPE in ('1', '2', '3', '4', '5', '6') then '0'
        else '1'
        end as primary_identification_type_flag
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all
""")


#-- 2.3 Create a temporary table temp_lmskibb2_tbl_counterparty_primary_identification_type to store the cleaned primary identification number.
spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_no
(
    record_id string,
    IDENTITY_TYPE string,
    IDENTITY_NUMBER string,
    primary_identification_no string,
    primary_identification_no_flag string
)
""")

spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_no
select
    record_id,
    IDENTITY_TYPE,
    IDENTITY_NUMBER,
    case
        when IDENTITY_TYPE in ('1', '2', '3', '4', '5', '6') and COALESCE(identity_number, '') <> '' then
            TRIM (
                CASE
                    WHEN IDENTITY_TYPE = '1' THEN REPLACE(IDENTITY_NUMBER, '-', '')
                    WHEN IDENTITY_TYPE = '4' AND LOCATE('-', IDENTITY_NUMBER) > 0
                        THEN SUBSTR(IDENTITY_NUMBER, 0, LOCATE('-', IDENTITY_NUMBER) - 1)
                    ELSE IDENTITY_NUMBER
                    END
            )
        else '@[' || identity_number || ']'
        end as primary_identification_no,
    case
        when IDENTITY_TYPE in ('1', '2', '3', '4', '5', '6') and COALESCE(identity_number, '') <> '' then '0'
        else '1'
        end as primary_identification_no_flag
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all
""")


#-- 2.2 Create a temporary table temp_lmskibb2_tbl_counterparty_primary_identification_type to store the cleaned primary identification type.
spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_secondary_identification
(
    record_id string,
    OLD_IDENTITY_NUMBER string,
    secondary_identification_no string,
    secondary_identification_no_flag string,
    secondary_identification_type string,
    secondary_identification_type_flag string
)
""")

spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_secondary_identification
select
    record_id
    , IDENTITY_TYPE AS IDENTITY_TYPE
    , OLD_IDENTITY_NUMBER AS secondary_identification_no
    , '0' AS secondary_identification_no_flag
    , IF (
        COALESCE(OLD_IDENTITY_NUMBER, '') != ''
        , CASE
            WHEN IDENTITY_TYPE = '1' THEN '2'
            WHEN IDENTITY_TYPE = '4' THEN '6'
            END
        , NULL
    ) AS secondary_identification_type
    , '0' as secondary_identification_type_flag
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all
""")


#-- 2.6 Create a temporary table temp_lmskibb2_tbl_counterparty_identification_info to store the cleaned identification information.
spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_identification_info(
    record_id string,
    IDENTITY_TYPE string,
    identity_number string,
    primary_identification_type string,
    primary_identification_type_flag string,
    primary_identification_no string,
    primary_identification_no_flag string,
    secondary_identification_type string,
    secondary_identification_type_flag string,
    secondary_identification_no string,
    secondary_identification_no_flag string
)
""")

spark.sql(f"""
WITH T0_BASE AS (
    SELECT
        T0.*,
        CASE
            WHEN T0.primary_identification_no RLIKE '^\\d+$'
                 AND LENGTH(T0.primary_identification_no) = 12
                 AND INT(SUBSTR(T0.primary_identification_no, 3, 2)) BETWEEN 1 AND 12
                 AND INT(SUBSTR(T0.primary_identification_no, 5, 2)) BETWEEN 1 AND 31
                 AND SUBSTR(T0.primary_identification_no, 7, 2) IN (
                     '01','21','22','23','24','02','25','26','27','03','28','29','04','30','05','31','59','06','32','33','07','34','35','08','36',
                     '37','38','39','09','40','10','41','42','43','44','11','45','46','12','47','48','49','13','50','51','52','53','14','54','55',
                     '56','57','15','58','16','60','61','62','63','64','65','66','67','68','71','72','74','75','76','77','78','79','82','83','84',
                     '85','86','87','88','89','90','91','92','93','98','99'
                 )
            THEN 1
            ELSE 0
        END AS IS_VALID_ID
    FROM {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_no T0
)

insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_identification_info
SELECT
    T0.RECORD_ID
    ,T0.IDENTITY_TYPE
    ,T0.IDENTITY_NUMBER
    ,T1.PRIMARY_IDENTIFICATION_TYPE
    ,T1.PRIMARY_IDENTIFICATION_TYPE_FLAG
    ,CASE
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE = '1' THEN
            CASE
                WHEN T0.IS_VALID_ID = 1 THEN T0.PRIMARY_IDENTIFICATION_NO
                ELSE CONCAT('@[', T0.PRIMARY_IDENTIFICATION_NO, ']')
            END
        ELSE T0.PRIMARY_IDENTIFICATION_NO
     END AS PRIMARY_IDENTIFICATION_NO
    ,CASE
        WHEN T1.PRIMARY_IDENTIFICATION_TYPE = '1' THEN
            CASE
                WHEN T0.IS_VALID_ID = 1 THEN '0'
                ELSE '1'
            END
        ELSE T0.PRIMARY_IDENTIFICATION_NO_FLAG
     END AS PRIMARY_IDENTIFICATION_NO_FLAG
    ,T2.SECONDARY_IDENTIFICATION_TYPE
    ,T2.SECONDARY_IDENTIFICATION_TYPE_FLAG
    ,T2.SECONDARY_IDENTIFICATION_NO
    ,T2.SECONDARY_IDENTIFICATION_NO_FLAG
FROM T0_BASE T0
LEFT JOIN {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_primary_identification_type T1
    ON T0.RECORD_ID = T1.RECORD_ID
LEFT JOIN {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_secondary_identification T2
    ON T0.RECORD_ID = T2.RECORD_ID
""")



spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_gender_info
(
    record_id string,
    primary_identification_type string,
    primary_identification_no string,
    primary_identification_type_flag string,
    primary_identification_no_flag string,
    SOURCE_GENDER_CODE string,
    GENDER_CODE string,
    GENDER_CODE_FLAG string
)
""")

spark.sql(f"""
WITH T_BASE AS (
    SELECT
        T0.*,
        T1.GENDER_CODE AS SOURCE_GENDER_CODE,
        COALESCE(TRIM(T1.GENDER_CODE), '') AS GENDER_SRC_CLEAN
    FROM {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_identification_info T0
    LEFT JOIN {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all T1
        ON T0.record_id = T1.record_id
)

insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_gender_info
SELECT
    T0.record_id
    ,T0.primary_identification_type
    ,T0.primary_identification_no
    ,T0.primary_identification_type_flag
    ,T0.primary_identification_no_flag

    ,T0.SOURCE_GENDER_CODE

    ,CASE
        WHEN T0.GENDER_SRC_CLEAN <> '' THEN
            CASE
                WHEN T0.GENDER_SRC_CLEAN = 'F' THEN 'FEMALE'
                WHEN T0.GENDER_SRC_CLEAN = 'M' THEN 'MALE'
                ELSE CONCAT('@[', T0.GENDER_SRC_CLEAN, ']')
            END
        WHEN T0.primary_identification_type = '1'
             AND T0.primary_identification_no_flag = '0' THEN
            CASE
                WHEN SUBSTR(T0.primary_identification_no, 12, 1) IN ('1','3','5','7','9')
                THEN 'MALE'
                ELSE 'FEMALE'
            END

        ELSE ''
     END AS GENDER_CODE
    ,CASE
        WHEN T0.primary_identification_type = '1'
             AND T0.primary_identification_no_flag = '0'
        THEN '0'
        WHEN T0.GENDER_SRC_CLEAN <> '' THEN
            CASE
                WHEN T0.GENDER_SRC_CLEAN IN ('F','M') THEN '0'
                ELSE '1'
            END
        ELSE '0'
     END AS GENDER_CODE_FLAG

FROM T_BASE T0
""")


#-- 2.7 Create a temporary table temp_mhbos_m_client_dob_info to store the cleaned date of birth.
spark.sql(f"""
create table if not exists {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_date_of_birth_info(
  record_id string,
  primary_identification_type string,
  primary_identification_no string,
  primary_identification_type_flag string,
  primary_identification_no_flag string,
  source_date_of_birth timestamp,
  date_of_birth timestamp,
  date_of_birth_flag string
)
""")

spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_date_of_birth_info
select T0.record_id,
     T0.primary_identification_type,
     T0.primary_identification_no,
     T0.primary_identification_type_flag,
     T0.primary_identification_no_flag,
     T1.date_of_birth as source_date_of_birth,
     (case when T0.primary_identification_type = '1' and T0.primary_identification_no_flag = '0'
             then TO_TIMESTAMP(SUBSTR(T0.primary_identification_no, 1, 6),'yyMMdd')
           when T1.date_of_birth is not null
             then T1.date_of_birth
           else T1.date_of_birth
      end) as date_of_birth,
     (case when T0.primary_identification_type = '1' and T0.primary_identification_no_flag = '0' then '0'
           when T1.date_of_birth is not null then'0'
           else '0'
      end) as date_of_birth_flag
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_identification_info T0
left join {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all T1
  on T0.record_id = T1.record_id
""")

spark.sql(f"""
create table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_tomerge(
  RECORD_ID INT COMMENT ''
  , CLEAN_RULE_FLAG VARCHAR(60) COMMENT '-primary_identification_type_flag
                                        2-primary_identification_no_flag
                                        3-secondary_identification_type_flag
                                        4-secondary_identification_no_flag
                                        5-gender_code_flag
                                        6-date_of_birth_flag'
  , FUNCTION_ID INT COMMENT ''
  , COUNTERPARTY_ID INT COMMENT ''
  , ENTRY_DATE TIMESTAMP COMMENT ''
  , ENTITY_ID INT COMMENT ''
  , ENTITY_CODE STRING COMMENT ''
  , COUNTERPARTY_CLASS_ID INT COMMENT ''
  , COUNTERPARTY_CLASS_CODE STRING COMMENT ''
  , COUNTERPARTY_TYPE_LIST STRING COMMENT ''
  , COUNTERPARTY_CODE STRING COMMENT ''
  , COUNTERPARTY_NAME STRING COMMENT ''
  , COUNTERPARTY_BASE_CCY_SECURITY_ID INT COMMENT ''
  , CCRIS_ENTITY_TYPE_ID INT COMMENT ''
  , CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE TIMESTAMP COMMENT ''
  , GENDER_CODE STRING COMMENT ''
  , TITLE_ID INT COMMENT ''
  , TITLE STRING COMMENT ''
  , PRIMARY_IDENTIFICATION_TYPE VARCHAR(10) COMMENT ''
  , PRIMARY_IDENTIFICATION_NO VARCHAR(60) COMMENT ''
  , SECONDARY_IDENTIFICATION_TYPE VARCHAR(10) COMMENT ''
  , SECONDARY_IDENTIFICATION_NO VARCHAR(60) COMMENT ''
  , PASSPORT_EXPIRY_DATE TIMESTAMP COMMENT ''
  , DATE_OF_BIRTH TIMESTAMP COMMENT ''
  , COUNTRY_OF_BIRTH_COUNTRY_ID INT COMMENT ''
  , COUNTRY_OF_BIRTH_COUNTRY_CODE STRING COMMENT ''
  , NATIONALITY_COUNTRY_ID INT COMMENT ''
  , NATIONALITY_COUNTRY_CODE STRING COMMENT ''
  , COUNTERPARTY_COUNTRY_ID INT COMMENT ''
  , COUNTERPARTY_COUNTRY_CODE STRING COMMENT ''
  , RACE_ID INT COMMENT ''
  , RACE_CODE STRING COMMENT ''
  , BUMIPUTERA_STATUS_ID INT COMMENT ''
  , BUMIPUTERA_STATUS STRING COMMENT ''
  , MARITAL_STATUS_ID INT COMMENT ''
  , MARITAL_STATUS_CODE STRING COMMENT ''
  , WEBSITE STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_1 STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_2 STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_3 STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_4 STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_POSTAL_CODE STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_CITY STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_STATE STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_STATE_CODE STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_COUNTRY_CODE STRING COMMENT ''
  , COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE TIMESTAMP COMMENT ''
  , TAX_STATUS STRING COMMENT ''
  , BANK_ID INT COMMENT ''
  , BANK_CODE STRING COMMENT ''
  , BANK_NAME STRING COMMENT ''
  , BANK_ACCOUNT_NUMBER STRING COMMENT ''
  , BANK_ACCOUNT_NAME STRING COMMENT ''
  , BUSINESS_CODE_ID INT COMMENT ''
  , BUSINESS_DESC STRING COMMENT ''
  , RESIDENCY_STATUS_ID INT COMMENT ''
  , RESIDENCY_STATUS_CODE STRING COMMENT ''
  , BNM_RESIDENCY_STATUS STRING COMMENT ''
  , COMPANY_TYPE_ID INT COMMENT ''
  , COMPANY_TYPE STRING COMMENT ''
  , DATE_OF_INCORPORATION TIMESTAMP COMMENT ''
  , COUNTRY_OF_INCORPORATION_COUNTRY_ID INT COMMENT ''
  , COUNTRY_OF_INCORPORATION_COUNTRY_CODE STRING COMMENT ''
  , PAID_UP_CAPITAL_CCY_SECURITY_ID INT COMMENT ''
  , PAID_UP_CAPITAL_AMOUNT DECIMAL(38,18) COMMENT ''
  , COUNTRY_OF_OPERATION_COUNTRY_ID INT COMMENT ''
  , COUNTRY_OF_OPERATION_COUNTRY_CODE STRING COMMENT ''
  , CONSTITUTION STRING COMMENT ''
  , IS_ADVISORY_REGULATED INT COMMENT ''
  , ADVISORY_REGULATED_REMARKS STRING COMMENT ''
  , TURNOVER_CCY_SECURITY_ID INT COMMENT ''
  , TURNOVER_AMOUNT DECIMAL(38,18) COMMENT ''
  , TURNOVER_PROJECTED_AMOUNT DECIMAL(38,18) COMMENT ''
  , TURNOVER_TOTAL_BORROWING_AMOUNT DECIMAL(38,18) COMMENT ''
  , IS_OWNERSHIP_STRUCTURE_COMPLEX INT COMMENT ''
  , OWNERSHIP_STRUCTURE_REMARKS STRING COMMENT ''
  , CCRIS_CORPORATE_STATUS_ID INT COMMENT ''
  , CCRIS_INDUSTRIAL_SECTOR_ID INT COMMENT ''
  , NEXT_OF_KIN_1_NAME STRING COMMENT ''
  , NEXT_OF_KIN_1_IDENTITY_TYPE_ID INT COMMENT ''
  , NEXT_OF_KIN_1_IDENTITY_NUMBER STRING COMMENT ''
  , NEXT_OF_KIN_1_RELATIONSHIP_ID INT COMMENT ''
  , NEXT_OF_KIN_1_PHONE_NO STRING COMMENT ''
  , NEXT_OF_KIN_1_OCCUPATION_ID INT COMMENT ''
  , NEXT_OF_KIN_1_EMPLOYER_NAME STRING COMMENT ''
  , NEXT_OF_KIN_2_NAME STRING COMMENT ''
  , NEXT_OF_KIN_2_IDENTITY_TYPE_ID INT COMMENT ''
  , NEXT_OF_KIN_2_IDENTITY_NUMBER STRING COMMENT ''
  , NEXT_OF_KIN_2_RELATIONSHIP_ID INT COMMENT ''
  , NEXT_OF_KIN_2_PHONE_NO STRING COMMENT ''
  , NEXT_OF_KIN_2_OCCUPATION_ID INT COMMENT ''
  , NEXT_OF_KIN_2_EMPLOYER_NAME STRING COMMENT ''
  , SOURCE_OF_INCOME STRING COMMENT ''
  , OTHER_SOURCE_OF_INCOME STRING COMMENT ''
  , PDPA_CONNECTED_PARTY STRING COMMENT ''
  , PDPA STRING COMMENT ''
  , PDPA_DISCLOSURE_REASON STRING COMMENT ''
  , IS_DOMESTIC_MYR_BORROW STRING COMMENT ''
  , REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID INT COMMENT ''
  , REMAINING_BALANCE_FROM_INVESTMENT DECIMAL(38,18) COMMENT ''
  , CURRENTLY_INVESTED_CCY_SECURITY_ID INT COMMENT ''
  , CURRENTLY_INVESTED DECIMAL(38,18) COMMENT ''
  , IS_FATCA STRING COMMENT ''
  , FATCA_ACCOUNT_TYPE_ID INT COMMENT ''
  , FATCA_ACCOUNT_CLASS_ID INT COMMENT ''
  , IS_DIFFERENT_MAILING_ADDRESS INT COMMENT ''
  , CORRESPONDENCE_ADDRESS_1 STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_2 STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_3 STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_4 STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_POSTAL_CODE STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_CITY STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_STATE STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_STATE_CODE STRING COMMENT ''
  , CORRESPONDENCE_ADDRESS_COUNTRY_CODE STRING COMMENT ''
  , PHONE_NO STRING COMMENT ''
  , HOME_PHONE_NO STRING COMMENT ''
  , OFFICE_PHONE_NO STRING COMMENT ''
  , CONTACT_PERSON_PHONE_NO STRING COMMENT ''
  , CONTACT_PERSON_NAME STRING COMMENT ''
  , EMAIL_ADDRESS STRING COMMENT ''
  , EMPLOYMENT_STATUS_ID INT COMMENT ''
  , EMPLOYMENT_STATUS STRING COMMENT ''
  , NAME_OF_COMPANY STRING COMMENT ''
  , EMPLOYER_ADDRESS_1 STRING COMMENT ''
  , EMPLOYER_ADDRESS_2 STRING COMMENT ''
  , EMPLOYER_ADDRESS_3 STRING COMMENT ''
  , EMPLOYER_ADDRESS_4 STRING COMMENT ''
  , EMPLOYER_ADDRESS_POSTAL_CODE STRING COMMENT ''
  , EMPLOYER_ADDRESS_CITY STRING COMMENT ''
  , EMPLOYER_ADDRESS_STATE STRING COMMENT ''
  , EMPLOYER_ADDRESS_COUNTRY_CODE STRING COMMENT ''
  , AML_OCCUPATION_ID INT COMMENT ''
  , AML_OCCUPATION STRING COMMENT ''
  , CCRIS_OCCUPATION_ID INT COMMENT ''
  , CCRIS_OCCUPATION STRING COMMENT ''
  , CCRIS_OCCUPATION_CODE STRING COMMENT ''
  , EMPLOYER_BUSINESS_CODE_ID INT COMMENT ''
  , EMPLOYER_BUSINESS_DESC STRING COMMENT ''
  , CCRIS_EMPLOYMENT_SECTOR_ID INT COMMENT ''
  , CCRIS_EMPLOYMENT_SECTOR_CODE STRING COMMENT ''
  , CCRIS_EMPLOYMENT_TYPE_ID INT COMMENT ''
  , CCRIS_EMPLOYMENT_TYPE_CODE STRING COMMENT ''
  , GROSS_ANNUAL_INCOME_DATE TIMESTAMP COMMENT ''
  , GROSS_ANNUAL_INCOME_CCY_SECURITY_ID INT COMMENT ''
  , GROSS_ANNUAL_INCOME_AMOUNT DECIMAL(38,18) COMMENT ''
  , COUNTRY_OF_RESIDENT_DECLARATION_ID INT COMMENT ''
  , MLRPC_RISK_ID INT COMMENT ''
  , CIF_DOCUMENT STRING COMMENT ''
  , RECORD_STATUS_ID INT COMMENT ''
  , REMARKS STRING COMMENT ''
  , REVIEW_DATE TIMESTAMP COMMENT ''
  , EFFECTIVE_FROM TIMESTAMP COMMENT ''
  , EFFECTIVE_TO TIMESTAMP COMMENT ''
  , USER_EFFECTIVE_TO TIMESTAMP COMMENT ''
  , UPDATE_RECORD_ID INT COMMENT ''
  , ACTION_TYPE_ID INT COMMENT ''
  , ACTION_TYPE_SNAPSHOT_ID INT COMMENT ''
  , LAST_ACTION_BY STRING COMMENT ''
  , LAST_ACTION_DATETIME TIMESTAMP COMMENT ''
  , SYSTEM_REMARKS STRING COMMENT ''
  , SYSTEM_UPDATED_DATETIME TIMESTAMP COMMENT ''
  , RELATED_FUNCTION_ID INT COMMENT ''
  , RELATED_RECORD_ID INT COMMENT ''
  , CIF_ANNUAL_INCOME_ID INT COMMENT ''
  , CIF_NET_WORTH_ID INT COMMENT ''
  , CIF_NET_WORTH STRING COMMENT ''
  , IS_POLITICALLY_EXPOSED_PERSON STRING COMMENT ''
  , IS_PDPA_DISCLOSURE STRING COMMENT ''
  , COLLATERAL_REFERENCE_NUMBER STRING COMMENT ''
  , E_INVOICE_INDICATOR_ID INT COMMENT ''
  , E_INVOICE_INDICATOR STRING COMMENT ''
  , CUSTOMER_SST_REGISTRATION_NUMBER STRING COMMENT ''
  , BUSINESS_CODE STRING COMMENT ''
  , EMPLOYER_BUSINESS_CODE STRING COMMENT ''
  , etl_timestamp string comment 'ETL_processing_time'
  , etl_dt string comment 'Data_date'
  , dl_record_status string comment 'status indicator for record'
	, dl_record_created_date timestamp
	, dl_record_updated_date timestamp
)
stored as parquet
""")

spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_tomerge
SELECT
    T0.RECORD_ID
    , (
        T1.PRIMARY_IDENTIFICATION_TYPE_FLAG || T1.PRIMARY_IDENTIFICATION_NO_FLAG ||
        T1.SECONDARY_IDENTIFICATION_TYPE_FLAG ||T1.SECONDARY_IDENTIFICATION_NO_FLAG ||
        T2.GENDER_CODE_FLAG || T3.DATE_OF_BIRTH_FLAG
    ) AS CLEAN_RULE_FLAG
    , T0.FUNCTION_ID
    , T0.COUNTERPARTY_ID
    , T0.ENTRY_DATE
    , T0.ENTITY_ID
    , T0.ENTITY_CODE
    , T0.COUNTERPARTY_CLASS_ID
    , T0.COUNTERPARTY_CLASS_CODE
    , T0.COUNTERPARTY_TYPE_LIST
    , T0.COUNTERPARTY_CODE
    , T0.COUNTERPARTY_NAME
    , T0.COUNTERPARTY_BASE_CCY_SECURITY_ID
    , T0.CCRIS_ENTITY_TYPE_ID
    , T0.CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE
    , T2.GENDER_CODE
    , T0.TITLE_ID
    , T0.TITLE
    , T1.PRIMARY_IDENTIFICATION_TYPE
    , T1.PRIMARY_IDENTIFICATION_NO
    , T1.SECONDARY_IDENTIFICATION_TYPE
    , T1.SECONDARY_IDENTIFICATION_NO
    , T0.PASSPORT_EXPIRY_DATE
    , T3.DATE_OF_BIRTH
    , T0.COUNTRY_OF_BIRTH_COUNTRY_ID
    , T0.COUNTRY_OF_BIRTH_COUNTRY_CODE
    , T0.NATIONALITY_COUNTRY_ID
    , T0.NATIONALITY_COUNTRY_CODE
    , T0.COUNTERPARTY_COUNTRY_ID
    , T0.COUNTERPARTY_COUNTRY_CODE
    , T0.RACE_ID
    , T0.RACE_CODE
    , T0.BUMIPUTERA_STATUS_ID
    , T0.BUMIPUTERA_STATUS
    , T0.MARITAL_STATUS_ID
    , T0.MARITAL_STATUS_CODE
    , T0.WEBSITE
    , T0.COUNTERPARTY_ADDRESS_1
    , T0.COUNTERPARTY_ADDRESS_2
    , T0.COUNTERPARTY_ADDRESS_3
    , T0.COUNTERPARTY_ADDRESS_4
    , T0.COUNTERPARTY_ADDRESS_POSTAL_CODE
    , T0.COUNTERPARTY_ADDRESS_CITY
    , T0.COUNTERPARTY_ADDRESS_STATE
    , T0.COUNTERPARTY_ADDRESS_STATE_CODE
    , T0.COUNTERPARTY_ADDRESS_COUNTRY_CODE
    , T0.COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE
    , T0.TAX_STATUS
    , T0.BANK_ID
    , T0.BANK_CODE
    , T0.BANK_NAME
    , T0.BANK_ACCOUNT_NUMBER
    , T0.BANK_ACCOUNT_NAME
    , T0.BUSINESS_CODE_ID
    , T0.BUSINESS_DESC
    , T0.RESIDENCY_STATUS_ID
    , T0.RESIDENCY_STATUS_CODE
    , T0.BNM_RESIDENCY_STATUS
    , T0.COMPANY_TYPE_ID
    , T0.COMPANY_TYPE
    , T0.DATE_OF_INCORPORATION
    , T0.COUNTRY_OF_INCORPORATION_COUNTRY_ID
    , T0.COUNTRY_OF_INCORPORATION_COUNTRY_CODE
    , T0.PAID_UP_CAPITAL_CCY_SECURITY_ID
    , T0.PAID_UP_CAPITAL_AMOUNT
    , T0.COUNTRY_OF_OPERATION_COUNTRY_ID
    , T0.COUNTRY_OF_OPERATION_COUNTRY_CODE
    , T0.CONSTITUTION
    , T0.IS_ADVISORY_REGULATED
    , T0.ADVISORY_REGULATED_REMARKS
    , T0.TURNOVER_CCY_SECURITY_ID
    , T0.TURNOVER_AMOUNT
    , T0.TURNOVER_PROJECTED_AMOUNT
    , T0.TURNOVER_TOTAL_BORROWING_AMOUNT
    , T0.IS_OWNERSHIP_STRUCTURE_COMPLEX
    , T0.OWNERSHIP_STRUCTURE_REMARKS
    , T0.CCRIS_CORPORATE_STATUS_ID
    , T0.CCRIS_INDUSTRIAL_SECTOR_ID
    , T0.NEXT_OF_KIN_1_NAME
    , T0.NEXT_OF_KIN_1_IDENTITY_TYPE_ID
    , T0.NEXT_OF_KIN_1_IDENTITY_NUMBER
    , T0.NEXT_OF_KIN_1_RELATIONSHIP_ID
    , T0.NEXT_OF_KIN_1_PHONE_NO
    , T0.NEXT_OF_KIN_1_OCCUPATION_ID
    , T0.NEXT_OF_KIN_1_EMPLOYER_NAME
    , T0.NEXT_OF_KIN_2_NAME
    , T0.NEXT_OF_KIN_2_IDENTITY_TYPE_ID
    , T0.NEXT_OF_KIN_2_IDENTITY_NUMBER
    , T0.NEXT_OF_KIN_2_RELATIONSHIP_ID
    , T0.NEXT_OF_KIN_2_PHONE_NO
    , T0.NEXT_OF_KIN_2_OCCUPATION_ID
    , T0.NEXT_OF_KIN_2_EMPLOYER_NAME
    , T0.SOURCE_OF_INCOME
    , T0.OTHER_SOURCE_OF_INCOME
    , T0.PDPA_CONNECTED_PARTY
    , T0.PDPA
    , T0.PDPA_DISCLOSURE_REASON
    , CASE
        WHEN T0.IS_DOMESTIC_MYR_BORROW = 1 THEN 'Y'
        WHEN T0.IS_DOMESTIC_MYR_BORROW = 0 THEN 'N'
        END AS IS_DOMESTIC_MYR_BORROW
    , T0.REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID
    , T0.REMAINING_BALANCE_FROM_INVESTMENT
    , T0.CURRENTLY_INVESTED_CCY_SECURITY_ID
    , T0.CURRENTLY_INVESTED
    , CASE
        WHEN T0.IS_FATCA = 1 THEN 'Y'
        WHEN T0.IS_FATCA = 0 THEN 'N'
        END AS IS_FATCA
    , T0.FATCA_ACCOUNT_TYPE_ID
    , T0.FATCA_ACCOUNT_CLASS_ID
    , T0.IS_DIFFERENT_MAILING_ADDRESS
    , T0.CORRESPONDENCE_ADDRESS_1
    , T0.CORRESPONDENCE_ADDRESS_2
    , T0.CORRESPONDENCE_ADDRESS_3
    , T0.CORRESPONDENCE_ADDRESS_4
    , T0.CORRESPONDENCE_ADDRESS_POSTAL_CODE
    , T0.CORRESPONDENCE_ADDRESS_CITY
    , T0.CORRESPONDENCE_ADDRESS_STATE
    , T0.CORRESPONDENCE_ADDRESS_STATE_CODE
    , T0.CORRESPONDENCE_ADDRESS_COUNTRY_CODE
    , T0.PHONE_NO
    , T0.HOME_PHONE_NO
    , T0.OFFICE_PHONE_NO
    , T0.CONTACT_PERSON_PHONE_NO
    , T0.CONTACT_PERSON_NAME
    , T0.EMAIL_ADDRESS
    , T0.EMPLOYMENT_STATUS_ID
    , T0.EMPLOYMENT_STATUS
    , T0.NAME_OF_COMPANY
    , T0.EMPLOYER_ADDRESS_1
    , T0.EMPLOYER_ADDRESS_2
    , T0.EMPLOYER_ADDRESS_3
    , T0.EMPLOYER_ADDRESS_4
    , T0.EMPLOYER_ADDRESS_POSTAL_CODE
    , T0.EMPLOYER_ADDRESS_CITY
    , T0.EMPLOYER_ADDRESS_STATE
    , T0.EMPLOYER_ADDRESS_COUNTRY_CODE
    , T0.AML_OCCUPATION_ID
    , T0.AML_OCCUPATION
    , T0.CCRIS_OCCUPATION_ID
    , T0.CCRIS_OCCUPATION
    , T0.CCRIS_OCCUPATION_CODE
    , T0.EMPLOYER_BUSINESS_CODE_ID
    , T0.EMPLOYER_BUSINESS_DESC
    , T0.CCRIS_EMPLOYMENT_SECTOR_ID
    , T0.CCRIS_EMPLOYMENT_SECTOR_CODE
    , T0.CCRIS_EMPLOYMENT_TYPE_ID
    , T0.CCRIS_EMPLOYMENT_TYPE_CODE
    , T0.GROSS_ANNUAL_INCOME_DATE
    , T0.GROSS_ANNUAL_INCOME_CCY_SECURITY_ID
    , T0.GROSS_ANNUAL_INCOME_AMOUNT
    , T0.COUNTRY_OF_RESIDENT_DECLARATION_ID
    , T0.MLRPC_RISK_ID
    , T0.CIF_DOCUMENT
    , T0.RECORD_STATUS_ID
    , T0.REMARKS
    , T0.REVIEW_DATE
    , T0.EFFECTIVE_FROM
    , T0.EFFECTIVE_TO
    , T0.USER_EFFECTIVE_TO
    , T0.UPDATE_RECORD_ID
    , T0.ACTION_TYPE_ID
    , T0.ACTION_TYPE_SNAPSHOT_ID
    , T0.LAST_ACTION_BY
    , T0.LAST_ACTION_DATETIME
    , T0.SYSTEM_REMARKS
    , T0.SYSTEM_UPDATED_DATETIME
    , T0.RELATED_FUNCTION_ID
    , T0.RELATED_RECORD_ID
    , T0.CIF_ANNUAL_INCOME_ID
    , T0.CIF_NET_WORTH_ID
    , T0.CIF_NET_WORTH
    , T0.IS_POLITICALLY_EXPOSED_PERSON
    , T0.IS_PDPA_DISCLOSURE
    , T0.COLLATERAL_REFERENCE_NUMBER
    , T0.E_INVOICE_INDICATOR_ID
    , T0.E_INVOICE_INDICATOR
    , T0.CUSTOMER_SST_REGISTRATION_NUMBER
    , T0.BUSINESS_CODE
    , T0.EMPLOYER_BUSINESS_CODE
    , T0.etl_timestamp as etl_timestamp
    , T0.etl_dt
    , 'A' as dl_record_status
    , to_timestamp(T0.etl_timestamp) as dl_record_created_date
    , to_timestamp(T0.etl_timestamp) as dl_record_updated_date
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_all T0
left join {params["com_schema"]}.TEMP_T_LMSKIBB2_TBL_COUNTERPARTY_IDENTIFICATION_INFO T1
    on T0.record_id = T1.record_id
left join {params["com_schema"]}.TEMP_T_LMSKIBB2_TBL_COUNTERPARTY_GENDER_INFO T2
    on T0.record_id = T2.record_id
left join {params["com_schema"]}.TEMP_T_LMSKIBB2_TBL_COUNTERPARTY_DATE_OF_BIRTH_INFO T3
    on T0.record_id = T3.record_id
""")

#-- 2.0 Populate latest complete dataset to temp table
#-- 2.1 Insert existing unchanged records from COM_T to temp table
spark.sql(f"""
create table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_updated(
      RECORD_ID INT COMMENT ''
    , CLEAN_RULE_FLAG VARCHAR(60) COMMENT '-primary_identification_type_flag
                                          2-primary_identification_no_flag
                                          3-secondary_identification_type_flag
                                          4-secondary_identification_no_flag
                                          5-gender_code_flag
                                          6-date_of_birth_flag'
    , FUNCTION_ID INT COMMENT ''
    , COUNTERPARTY_ID INT COMMENT ''
    , ENTRY_DATE TIMESTAMP COMMENT ''
    , ENTITY_ID INT COMMENT ''
    , ENTITY_CODE STRING COMMENT ''
    , COUNTERPARTY_CLASS_ID INT COMMENT ''
    , COUNTERPARTY_CLASS_CODE STRING COMMENT ''
    , COUNTERPARTY_TYPE_LIST STRING COMMENT ''
    , COUNTERPARTY_CODE STRING COMMENT ''
    , COUNTERPARTY_NAME STRING COMMENT ''
    , COUNTERPARTY_BASE_CCY_SECURITY_ID INT COMMENT ''
    , CCRIS_ENTITY_TYPE_ID INT COMMENT ''
    , CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE TIMESTAMP COMMENT ''
    , GENDER_CODE STRING COMMENT ''
    , TITLE_ID INT COMMENT ''
    , TITLE STRING COMMENT ''
    , PRIMARY_IDENTIFICATION_TYPE VARCHAR(10) COMMENT ''
    , PRIMARY_IDENTIFICATION_NO VARCHAR(60) COMMENT ''
    , SECONDARY_IDENTIFICATION_TYPE VARCHAR(10) COMMENT ''
    , SECONDARY_IDENTIFICATION_NO VARCHAR(60) COMMENT ''
    , PASSPORT_EXPIRY_DATE TIMESTAMP COMMENT ''
    , DATE_OF_BIRTH TIMESTAMP COMMENT ''
    , COUNTRY_OF_BIRTH_COUNTRY_ID INT COMMENT ''
    , COUNTRY_OF_BIRTH_COUNTRY_CODE STRING COMMENT ''
    , NATIONALITY_COUNTRY_ID INT COMMENT ''
    , NATIONALITY_COUNTRY_CODE STRING COMMENT ''
    , COUNTERPARTY_COUNTRY_ID INT COMMENT ''
    , COUNTERPARTY_COUNTRY_CODE STRING COMMENT ''
    , RACE_ID INT COMMENT ''
    , RACE_CODE STRING COMMENT ''
    , BUMIPUTERA_STATUS_ID INT COMMENT ''
    , BUMIPUTERA_STATUS STRING COMMENT ''
    , MARITAL_STATUS_ID INT COMMENT ''
    , MARITAL_STATUS_CODE STRING COMMENT ''
    , WEBSITE STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_1 STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_2 STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_3 STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_4 STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_POSTAL_CODE STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_CITY STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_STATE STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_STATE_CODE STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_COUNTRY_CODE STRING COMMENT ''
    , COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE TIMESTAMP COMMENT ''
    , TAX_STATUS STRING COMMENT ''
    , BANK_ID INT COMMENT ''
    , BANK_CODE STRING COMMENT ''
    , BANK_NAME STRING COMMENT ''
    , BANK_ACCOUNT_NUMBER STRING COMMENT ''
    , BANK_ACCOUNT_NAME STRING COMMENT ''
    , BUSINESS_CODE_ID INT COMMENT ''
    , BUSINESS_DESC STRING COMMENT ''
    , RESIDENCY_STATUS_ID INT COMMENT ''
    , RESIDENCY_STATUS_CODE STRING COMMENT ''
    , BNM_RESIDENCY_STATUS STRING COMMENT ''
    , COMPANY_TYPE_ID INT COMMENT ''
    , COMPANY_TYPE STRING COMMENT ''
    , DATE_OF_INCORPORATION TIMESTAMP COMMENT ''
    , COUNTRY_OF_INCORPORATION_COUNTRY_ID INT COMMENT ''
    , COUNTRY_OF_INCORPORATION_COUNTRY_CODE STRING COMMENT ''
    , PAID_UP_CAPITAL_CCY_SECURITY_ID INT COMMENT ''
    , PAID_UP_CAPITAL_AMOUNT DECIMAL(38,18) COMMENT ''
    , COUNTRY_OF_OPERATION_COUNTRY_ID INT COMMENT ''
    , COUNTRY_OF_OPERATION_COUNTRY_CODE STRING COMMENT ''
    , CONSTITUTION STRING COMMENT ''
    , IS_ADVISORY_REGULATED INT COMMENT ''
    , ADVISORY_REGULATED_REMARKS STRING COMMENT ''
    , TURNOVER_CCY_SECURITY_ID INT COMMENT ''
    , TURNOVER_AMOUNT DECIMAL(38,18) COMMENT ''
    , TURNOVER_PROJECTED_AMOUNT DECIMAL(38,18) COMMENT ''
    , TURNOVER_TOTAL_BORROWING_AMOUNT DECIMAL(38,18) COMMENT ''
    , IS_OWNERSHIP_STRUCTURE_COMPLEX INT COMMENT ''
    , OWNERSHIP_STRUCTURE_REMARKS STRING COMMENT ''
    , CCRIS_CORPORATE_STATUS_ID INT COMMENT ''
    , CCRIS_INDUSTRIAL_SECTOR_ID INT COMMENT ''
    , NEXT_OF_KIN_1_NAME STRING COMMENT ''
    , NEXT_OF_KIN_1_IDENTITY_TYPE_ID INT COMMENT ''
    , NEXT_OF_KIN_1_IDENTITY_NUMBER STRING COMMENT ''
    , NEXT_OF_KIN_1_RELATIONSHIP_ID INT COMMENT ''
    , NEXT_OF_KIN_1_PHONE_NO STRING COMMENT ''
    , NEXT_OF_KIN_1_OCCUPATION_ID INT COMMENT ''
    , NEXT_OF_KIN_1_EMPLOYER_NAME STRING COMMENT ''
    , NEXT_OF_KIN_2_NAME STRING COMMENT ''
    , NEXT_OF_KIN_2_IDENTITY_TYPE_ID INT COMMENT ''
    , NEXT_OF_KIN_2_IDENTITY_NUMBER STRING COMMENT ''
    , NEXT_OF_KIN_2_RELATIONSHIP_ID INT COMMENT ''
    , NEXT_OF_KIN_2_PHONE_NO STRING COMMENT ''
    , NEXT_OF_KIN_2_OCCUPATION_ID INT COMMENT ''
    , NEXT_OF_KIN_2_EMPLOYER_NAME STRING COMMENT ''
    , SOURCE_OF_INCOME STRING COMMENT ''
    , OTHER_SOURCE_OF_INCOME STRING COMMENT ''
    , PDPA_CONNECTED_PARTY STRING COMMENT ''
    , PDPA STRING COMMENT ''
    , PDPA_DISCLOSURE_REASON STRING COMMENT ''
    , IS_DOMESTIC_MYR_BORROW STRING COMMENT ''
    , REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID INT COMMENT ''
    , REMAINING_BALANCE_FROM_INVESTMENT DECIMAL(38,18) COMMENT ''
    , CURRENTLY_INVESTED_CCY_SECURITY_ID INT COMMENT ''
    , CURRENTLY_INVESTED DECIMAL(38,18) COMMENT ''
    , IS_FATCA STRING COMMENT ''
    , FATCA_ACCOUNT_TYPE_ID INT COMMENT ''
    , FATCA_ACCOUNT_CLASS_ID INT COMMENT ''
    , IS_DIFFERENT_MAILING_ADDRESS INT COMMENT ''
    , CORRESPONDENCE_ADDRESS_1 STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_2 STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_3 STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_4 STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_POSTAL_CODE STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_CITY STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_STATE STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_STATE_CODE STRING COMMENT ''
    , CORRESPONDENCE_ADDRESS_COUNTRY_CODE STRING COMMENT ''
    , PHONE_NO STRING COMMENT ''
    , HOME_PHONE_NO STRING COMMENT ''
    , OFFICE_PHONE_NO STRING COMMENT ''
    , CONTACT_PERSON_PHONE_NO STRING COMMENT ''
    , CONTACT_PERSON_NAME STRING COMMENT ''
    , EMAIL_ADDRESS STRING COMMENT ''
    , EMPLOYMENT_STATUS_ID INT COMMENT ''
    , EMPLOYMENT_STATUS STRING COMMENT ''
    , NAME_OF_COMPANY STRING COMMENT ''
    , EMPLOYER_ADDRESS_1 STRING COMMENT ''
    , EMPLOYER_ADDRESS_2 STRING COMMENT ''
    , EMPLOYER_ADDRESS_3 STRING COMMENT ''
    , EMPLOYER_ADDRESS_4 STRING COMMENT ''
    , EMPLOYER_ADDRESS_POSTAL_CODE STRING COMMENT ''
    , EMPLOYER_ADDRESS_CITY STRING COMMENT ''
    , EMPLOYER_ADDRESS_STATE STRING COMMENT ''
    , EMPLOYER_ADDRESS_COUNTRY_CODE STRING COMMENT ''
    , AML_OCCUPATION_ID INT COMMENT ''
    , AML_OCCUPATION STRING COMMENT ''
    , CCRIS_OCCUPATION_ID INT COMMENT ''
    , CCRIS_OCCUPATION STRING COMMENT ''
    , CCRIS_OCCUPATION_CODE STRING COMMENT ''
    , EMPLOYER_BUSINESS_CODE_ID INT COMMENT ''
    , EMPLOYER_BUSINESS_DESC STRING COMMENT ''
    , CCRIS_EMPLOYMENT_SECTOR_ID INT COMMENT ''
    , CCRIS_EMPLOYMENT_SECTOR_CODE STRING COMMENT ''
    , CCRIS_EMPLOYMENT_TYPE_ID INT COMMENT ''
    , CCRIS_EMPLOYMENT_TYPE_CODE STRING COMMENT ''
    , GROSS_ANNUAL_INCOME_DATE TIMESTAMP COMMENT ''
    , GROSS_ANNUAL_INCOME_CCY_SECURITY_ID INT COMMENT ''
    , GROSS_ANNUAL_INCOME_AMOUNT DECIMAL(38,18) COMMENT ''
    , COUNTRY_OF_RESIDENT_DECLARATION_ID INT COMMENT ''
    , MLRPC_RISK_ID INT COMMENT ''
    , CIF_DOCUMENT STRING COMMENT ''
    , RECORD_STATUS_ID INT COMMENT ''
    , REMARKS STRING COMMENT ''
    , REVIEW_DATE TIMESTAMP COMMENT ''
    , EFFECTIVE_FROM TIMESTAMP COMMENT ''
    , EFFECTIVE_TO TIMESTAMP COMMENT ''
    , USER_EFFECTIVE_TO TIMESTAMP COMMENT ''
    , UPDATE_RECORD_ID INT COMMENT ''
    , ACTION_TYPE_ID INT COMMENT ''
    , ACTION_TYPE_SNAPSHOT_ID INT COMMENT ''
    , LAST_ACTION_BY STRING COMMENT ''
    , LAST_ACTION_DATETIME TIMESTAMP COMMENT ''
    , SYSTEM_REMARKS STRING COMMENT ''
    , SYSTEM_UPDATED_DATETIME TIMESTAMP COMMENT ''
    , RELATED_FUNCTION_ID INT COMMENT ''
    , RELATED_RECORD_ID INT COMMENT ''
    , CIF_ANNUAL_INCOME_ID INT COMMENT ''
    , CIF_NET_WORTH_ID INT COMMENT ''
    , CIF_NET_WORTH STRING COMMENT ''
    , IS_POLITICALLY_EXPOSED_PERSON STRING COMMENT ''
    , IS_PDPA_DISCLOSURE STRING COMMENT ''
    , COLLATERAL_REFERENCE_NUMBER STRING COMMENT ''
    , E_INVOICE_INDICATOR_ID INT COMMENT ''
    , E_INVOICE_INDICATOR STRING COMMENT ''
    , CUSTOMER_SST_REGISTRATION_NUMBER STRING COMMENT ''
    , BUSINESS_CODE STRING COMMENT ''
    , EMPLOYER_BUSINESS_CODE STRING COMMENT ''
    , etl_timestamp string comment 'ETL_processing_time'
    , etl_dt string comment 'Data_date'
    , dl_record_status string comment 'status indicator for record'
  	, dl_record_created_date timestamp
  	, dl_record_updated_date timestamp
  )
  stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
)
""")

#-- 2.0 Populate latest complete dataset to temp table
#-- 2.1 Insert existing unchanged records from COM_T to temp table
spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_updated
select
    com_t.RECORD_ID
  , com_t.CLEAN_RULE_FLAG
  , com_t.FUNCTION_ID
  , com_t.COUNTERPARTY_ID
  , com_t.ENTRY_DATE
  , com_t.ENTITY_ID
  , com_t.ENTITY_CODE
  , com_t.COUNTERPARTY_CLASS_ID
  , com_t.COUNTERPARTY_CLASS_CODE
  , com_t.COUNTERPARTY_TYPE_LIST
  , com_t.COUNTERPARTY_CODE
  , com_t.COUNTERPARTY_NAME
  , com_t.COUNTERPARTY_BASE_CCY_SECURITY_ID
  , com_t.CCRIS_ENTITY_TYPE_ID
  , com_t.CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE
  , com_t.GENDER_CODE
  , com_t.TITLE_ID
  , com_t.TITLE
  , com_t.PRIMARY_IDENTIFICATION_TYPE
  , com_t.PRIMARY_IDENTIFICATION_NO
  , com_t.SECONDARY_IDENTIFICATION_TYPE
  , com_t.SECONDARY_IDENTIFICATION_NO
  , com_t.PASSPORT_EXPIRY_DATE
  , com_t.DATE_OF_BIRTH
  , com_t.COUNTRY_OF_BIRTH_COUNTRY_ID
  , com_t.COUNTRY_OF_BIRTH_COUNTRY_CODE
  , com_t.NATIONALITY_COUNTRY_ID
  , com_t.NATIONALITY_COUNTRY_CODE
  , com_t.COUNTERPARTY_COUNTRY_ID
  , com_t.COUNTERPARTY_COUNTRY_CODE
  , com_t.RACE_ID
  , com_t.RACE_CODE
  , com_t.BUMIPUTERA_STATUS_ID
  , com_t.BUMIPUTERA_STATUS
  , com_t.MARITAL_STATUS_ID
  , com_t.MARITAL_STATUS_CODE
  , com_t.WEBSITE
  , com_t.COUNTERPARTY_ADDRESS_1
  , com_t.COUNTERPARTY_ADDRESS_2
  , com_t.COUNTERPARTY_ADDRESS_3
  , com_t.COUNTERPARTY_ADDRESS_4
  , com_t.COUNTERPARTY_ADDRESS_POSTAL_CODE
  , com_t.COUNTERPARTY_ADDRESS_CITY
  , com_t.COUNTERPARTY_ADDRESS_STATE
  , com_t.COUNTERPARTY_ADDRESS_STATE_CODE
  , com_t.COUNTERPARTY_ADDRESS_COUNTRY_CODE
  , com_t.COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE
  , com_t.TAX_STATUS
  , com_t.BANK_ID
  , com_t.BANK_CODE
  , com_t.BANK_NAME
  , com_t.BANK_ACCOUNT_NUMBER
  , com_t.BANK_ACCOUNT_NAME
  , com_t.BUSINESS_CODE_ID
  , com_t.BUSINESS_DESC
  , com_t.RESIDENCY_STATUS_ID
  , com_t.RESIDENCY_STATUS_CODE
  , com_t.BNM_RESIDENCY_STATUS
  , com_t.COMPANY_TYPE_ID
  , com_t.COMPANY_TYPE
  , com_t.DATE_OF_INCORPORATION
  , com_t.COUNTRY_OF_INCORPORATION_COUNTRY_ID
  , com_t.COUNTRY_OF_INCORPORATION_COUNTRY_CODE
  , com_t.PAID_UP_CAPITAL_CCY_SECURITY_ID
  , com_t.PAID_UP_CAPITAL_AMOUNT
  , com_t.COUNTRY_OF_OPERATION_COUNTRY_ID
  , com_t.COUNTRY_OF_OPERATION_COUNTRY_CODE
  , com_t.CONSTITUTION
  , com_t.IS_ADVISORY_REGULATED
  , com_t.ADVISORY_REGULATED_REMARKS
  , com_t.TURNOVER_CCY_SECURITY_ID
  , com_t.TURNOVER_AMOUNT
  , com_t.TURNOVER_PROJECTED_AMOUNT
  , com_t.TURNOVER_TOTAL_BORROWING_AMOUNT
  , com_t.IS_OWNERSHIP_STRUCTURE_COMPLEX
  , com_t.OWNERSHIP_STRUCTURE_REMARKS
  , com_t.CCRIS_CORPORATE_STATUS_ID
  , com_t.CCRIS_INDUSTRIAL_SECTOR_ID
  , com_t.NEXT_OF_KIN_1_NAME
  , com_t.NEXT_OF_KIN_1_IDENTITY_TYPE_ID
  , com_t.NEXT_OF_KIN_1_IDENTITY_NUMBER
  , com_t.NEXT_OF_KIN_1_RELATIONSHIP_ID
  , com_t.NEXT_OF_KIN_1_PHONE_NO
  , com_t.NEXT_OF_KIN_1_OCCUPATION_ID
  , com_t.NEXT_OF_KIN_1_EMPLOYER_NAME
  , com_t.NEXT_OF_KIN_2_NAME
  , com_t.NEXT_OF_KIN_2_IDENTITY_TYPE_ID
  , com_t.NEXT_OF_KIN_2_IDENTITY_NUMBER
  , com_t.NEXT_OF_KIN_2_RELATIONSHIP_ID
  , com_t.NEXT_OF_KIN_2_PHONE_NO
  , com_t.NEXT_OF_KIN_2_OCCUPATION_ID
  , com_t.NEXT_OF_KIN_2_EMPLOYER_NAME
  , com_t.SOURCE_OF_INCOME
  , com_t.OTHER_SOURCE_OF_INCOME
  , com_t.PDPA_CONNECTED_PARTY
  , com_t.PDPA
  , com_t.PDPA_DISCLOSURE_REASON
  , com_t.IS_DOMESTIC_MYR_BORROW
  , com_t.REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID
  , com_t.REMAINING_BALANCE_FROM_INVESTMENT
  , com_t.CURRENTLY_INVESTED_CCY_SECURITY_ID
  , com_t.CURRENTLY_INVESTED
  , com_t.IS_FATCA
  , com_t.FATCA_ACCOUNT_TYPE_ID
  , com_t.FATCA_ACCOUNT_CLASS_ID
  , com_t.IS_DIFFERENT_MAILING_ADDRESS
  , com_t.CORRESPONDENCE_ADDRESS_1
  , com_t.CORRESPONDENCE_ADDRESS_2
  , com_t.CORRESPONDENCE_ADDRESS_3
  , com_t.CORRESPONDENCE_ADDRESS_4
  , com_t.CORRESPONDENCE_ADDRESS_POSTAL_CODE
  , com_t.CORRESPONDENCE_ADDRESS_CITY
  , com_t.CORRESPONDENCE_ADDRESS_STATE
  , com_t.CORRESPONDENCE_ADDRESS_STATE_CODE
  , com_t.CORRESPONDENCE_ADDRESS_COUNTRY_CODE
  , com_t.PHONE_NO
  , com_t.HOME_PHONE_NO
  , com_t.OFFICE_PHONE_NO
  , com_t.CONTACT_PERSON_PHONE_NO
  , com_t.CONTACT_PERSON_NAME
  , com_t.EMAIL_ADDRESS
  , com_t.EMPLOYMENT_STATUS_ID
  , com_t.EMPLOYMENT_STATUS
  , com_t.NAME_OF_COMPANY
  , com_t.EMPLOYER_ADDRESS_1
  , com_t.EMPLOYER_ADDRESS_2
  , com_t.EMPLOYER_ADDRESS_3
  , com_t.EMPLOYER_ADDRESS_4
  , com_t.EMPLOYER_ADDRESS_POSTAL_CODE
  , com_t.EMPLOYER_ADDRESS_CITY
  , com_t.EMPLOYER_ADDRESS_STATE
  , com_t.EMPLOYER_ADDRESS_COUNTRY_CODE
  , com_t.AML_OCCUPATION_ID
  , com_t.AML_OCCUPATION
  , com_t.CCRIS_OCCUPATION_ID
  , com_t.CCRIS_OCCUPATION
  , com_t.CCRIS_OCCUPATION_CODE
  , com_t.EMPLOYER_BUSINESS_CODE_ID
  , com_t.EMPLOYER_BUSINESS_DESC
  , com_t.CCRIS_EMPLOYMENT_SECTOR_ID
  , com_t.CCRIS_EMPLOYMENT_SECTOR_CODE
  , com_t.CCRIS_EMPLOYMENT_TYPE_ID
  , com_t.CCRIS_EMPLOYMENT_TYPE_CODE
  , com_t.GROSS_ANNUAL_INCOME_DATE
  , com_t.GROSS_ANNUAL_INCOME_CCY_SECURITY_ID
  , com_t.GROSS_ANNUAL_INCOME_AMOUNT
  , com_t.COUNTRY_OF_RESIDENT_DECLARATION_ID
  , com_t.MLRPC_RISK_ID
  , com_t.CIF_DOCUMENT
  , com_t.RECORD_STATUS_ID
  , com_t.REMARKS
  , com_t.REVIEW_DATE
  , com_t.EFFECTIVE_FROM
  , com_t.EFFECTIVE_TO
  , com_t.USER_EFFECTIVE_TO
  , com_t.UPDATE_RECORD_ID
  , com_t.ACTION_TYPE_ID
  , com_t.ACTION_TYPE_SNAPSHOT_ID
  , com_t.LAST_ACTION_BY
  , com_t.LAST_ACTION_DATETIME
  , com_t.SYSTEM_REMARKS
  , com_t.SYSTEM_UPDATED_DATETIME
  , com_t.RELATED_FUNCTION_ID
  , com_t.RELATED_RECORD_ID
  , com_t.CIF_ANNUAL_INCOME_ID
  , com_t.CIF_NET_WORTH_ID
  , com_t.CIF_NET_WORTH
  , com_t.IS_POLITICALLY_EXPOSED_PERSON
  , com_t.IS_PDPA_DISCLOSURE
  , com_t.COLLATERAL_REFERENCE_NUMBER
  , com_t.E_INVOICE_INDICATOR_ID
  , com_t.E_INVOICE_INDICATOR
  , com_t.CUSTOMER_SST_REGISTRATION_NUMBER
  , com_t.BUSINESS_CODE
  , com_t.EMPLOYER_BUSINESS_CODE
  , com_t.ETL_TIMESTAMP
  , com_t.ETL_DT
  , com_t.DL_RECORD_STATUS
  , com_t.DL_RECORD_CREATED_DATE
  , com_t.DL_RECORD_UPDATED_DATE
FROM {params["com_schema"]}.t_lmskibb2_tbl_counterparty com_t
WHERE NOT EXISTS
    (SELECT 1 FROM {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_tomerge r
    WHERE r.etl_dt = '{batch_date}'
    AND com_t.COUNTERPARTY_ID = com_t.COUNTERPARTY_ID)
""")

#-- 2.2 Insert latest records from RAW to temp table
spark.sql(f"""
insert into table {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_updated
select
    r.RECORD_ID
  , r.CLEAN_RULE_FLAG
  , r.FUNCTION_ID
  , r.COUNTERPARTY_ID
  , r.ENTRY_DATE
  , r.ENTITY_ID
  , r.ENTITY_CODE
  , r.COUNTERPARTY_CLASS_ID
  , r.COUNTERPARTY_CLASS_CODE
  , r.COUNTERPARTY_TYPE_LIST
  , r.COUNTERPARTY_CODE
  , r.COUNTERPARTY_NAME
  , r.COUNTERPARTY_BASE_CCY_SECURITY_ID
  , r.CCRIS_ENTITY_TYPE_ID
  , r.CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE
  , r.GENDER_CODE
  , r.TITLE_ID
  , r.TITLE
  , r.PRIMARY_IDENTIFICATION_TYPE
  , r.PRIMARY_IDENTIFICATION_NO
  , r.SECONDARY_IDENTIFICATION_TYPE
  , r.SECONDARY_IDENTIFICATION_NO
  , r.PASSPORT_EXPIRY_DATE
  , r.DATE_OF_BIRTH
  , r.COUNTRY_OF_BIRTH_COUNTRY_ID
  , r.COUNTRY_OF_BIRTH_COUNTRY_CODE
  , r.NATIONALITY_COUNTRY_ID
  , r.NATIONALITY_COUNTRY_CODE
  , r.COUNTERPARTY_COUNTRY_ID
  , r.COUNTERPARTY_COUNTRY_CODE
  , r.RACE_ID
  , r.RACE_CODE
  , r.BUMIPUTERA_STATUS_ID
  , r.BUMIPUTERA_STATUS
  , r.MARITAL_STATUS_ID
  , r.MARITAL_STATUS_CODE
  , r.WEBSITE
  , r.COUNTERPARTY_ADDRESS_1
  , r.COUNTERPARTY_ADDRESS_2
  , r.COUNTERPARTY_ADDRESS_3
  , r.COUNTERPARTY_ADDRESS_4
  , r.COUNTERPARTY_ADDRESS_POSTAL_CODE
  , r.COUNTERPARTY_ADDRESS_CITY
  , r.COUNTERPARTY_ADDRESS_STATE
  , r.COUNTERPARTY_ADDRESS_STATE_CODE
  , r.COUNTERPARTY_ADDRESS_COUNTRY_CODE
  , r.COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE
  , r.TAX_STATUS
  , r.BANK_ID
  , r.BANK_CODE
  , r.BANK_NAME
  , r.BANK_ACCOUNT_NUMBER
  , r.BANK_ACCOUNT_NAME
  , r.BUSINESS_CODE_ID
  , r.BUSINESS_DESC
  , r.RESIDENCY_STATUS_ID
  , r.RESIDENCY_STATUS_CODE
  , r.BNM_RESIDENCY_STATUS
  , r.COMPANY_TYPE_ID
  , r.COMPANY_TYPE
  , r.DATE_OF_INCORPORATION
  , r.COUNTRY_OF_INCORPORATION_COUNTRY_ID
  , r.COUNTRY_OF_INCORPORATION_COUNTRY_CODE
  , r.PAID_UP_CAPITAL_CCY_SECURITY_ID
  , r.PAID_UP_CAPITAL_AMOUNT
  , r.COUNTRY_OF_OPERATION_COUNTRY_ID
  , r.COUNTRY_OF_OPERATION_COUNTRY_CODE
  , r.CONSTITUTION
  , r.IS_ADVISORY_REGULATED
  , r.ADVISORY_REGULATED_REMARKS
  , r.TURNOVER_CCY_SECURITY_ID
  , r.TURNOVER_AMOUNT
  , r.TURNOVER_PROJECTED_AMOUNT
  , r.TURNOVER_TOTAL_BORROWING_AMOUNT
  , r.IS_OWNERSHIP_STRUCTURE_COMPLEX
  , r.OWNERSHIP_STRUCTURE_REMARKS
  , r.CCRIS_CORPORATE_STATUS_ID
  , r.CCRIS_INDUSTRIAL_SECTOR_ID
  , r.NEXT_OF_KIN_1_NAME
  , r.NEXT_OF_KIN_1_IDENTITY_TYPE_ID
  , r.NEXT_OF_KIN_1_IDENTITY_NUMBER
  , r.NEXT_OF_KIN_1_RELATIONSHIP_ID
  , r.NEXT_OF_KIN_1_PHONE_NO
  , r.NEXT_OF_KIN_1_OCCUPATION_ID
  , r.NEXT_OF_KIN_1_EMPLOYER_NAME
  , r.NEXT_OF_KIN_2_NAME
  , r.NEXT_OF_KIN_2_IDENTITY_TYPE_ID
  , r.NEXT_OF_KIN_2_IDENTITY_NUMBER
  , r.NEXT_OF_KIN_2_RELATIONSHIP_ID
  , r.NEXT_OF_KIN_2_PHONE_NO
  , r.NEXT_OF_KIN_2_OCCUPATION_ID
  , r.NEXT_OF_KIN_2_EMPLOYER_NAME
  , r.SOURCE_OF_INCOME
  , r.OTHER_SOURCE_OF_INCOME
  , r.PDPA_CONNECTED_PARTY
  , r.PDPA
  , r.PDPA_DISCLOSURE_REASON
  , r.IS_DOMESTIC_MYR_BORROW
  , r.REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID
  , r.REMAINING_BALANCE_FROM_INVESTMENT
  , r.CURRENTLY_INVESTED_CCY_SECURITY_ID
  , r.CURRENTLY_INVESTED
  , r.IS_FATCA
  , r.FATCA_ACCOUNT_TYPE_ID
  , r.FATCA_ACCOUNT_CLASS_ID
  , r.IS_DIFFERENT_MAILING_ADDRESS
  , r.CORRESPONDENCE_ADDRESS_1
  , r.CORRESPONDENCE_ADDRESS_2
  , r.CORRESPONDENCE_ADDRESS_3
  , r.CORRESPONDENCE_ADDRESS_4
  , r.CORRESPONDENCE_ADDRESS_POSTAL_CODE
  , r.CORRESPONDENCE_ADDRESS_CITY
  , r.CORRESPONDENCE_ADDRESS_STATE
  , r.CORRESPONDENCE_ADDRESS_STATE_CODE
  , r.CORRESPONDENCE_ADDRESS_COUNTRY_CODE
  , r.PHONE_NO
  , r.HOME_PHONE_NO
  , r.OFFICE_PHONE_NO
  , r.CONTACT_PERSON_PHONE_NO
  , r.CONTACT_PERSON_NAME
  , r.EMAIL_ADDRESS
  , r.EMPLOYMENT_STATUS_ID
  , r.EMPLOYMENT_STATUS
  , r.NAME_OF_COMPANY
  , r.EMPLOYER_ADDRESS_1
  , r.EMPLOYER_ADDRESS_2
  , r.EMPLOYER_ADDRESS_3
  , r.EMPLOYER_ADDRESS_4
  , r.EMPLOYER_ADDRESS_POSTAL_CODE
  , r.EMPLOYER_ADDRESS_CITY
  , r.EMPLOYER_ADDRESS_STATE
  , r.EMPLOYER_ADDRESS_COUNTRY_CODE
  , r.AML_OCCUPATION_ID
  , r.AML_OCCUPATION
  , r.CCRIS_OCCUPATION_ID
  , r.CCRIS_OCCUPATION
  , r.CCRIS_OCCUPATION_CODE
  , r.EMPLOYER_BUSINESS_CODE_ID
  , r.EMPLOYER_BUSINESS_DESC
  , r.CCRIS_EMPLOYMENT_SECTOR_ID
  , r.CCRIS_EMPLOYMENT_SECTOR_CODE
  , r.CCRIS_EMPLOYMENT_TYPE_ID
  , r.CCRIS_EMPLOYMENT_TYPE_CODE
  , r.GROSS_ANNUAL_INCOME_DATE
  , r.GROSS_ANNUAL_INCOME_CCY_SECURITY_ID
  , r.GROSS_ANNUAL_INCOME_AMOUNT
  , r.COUNTRY_OF_RESIDENT_DECLARATION_ID
  , r.MLRPC_RISK_ID
  , r.CIF_DOCUMENT
  , r.RECORD_STATUS_ID
  , r.REMARKS
  , r.REVIEW_DATE
  , r.EFFECTIVE_FROM
  , r.EFFECTIVE_TO
  , r.USER_EFFECTIVE_TO
  , r.UPDATE_RECORD_ID
  , r.ACTION_TYPE_ID
  , r.ACTION_TYPE_SNAPSHOT_ID
  , r.LAST_ACTION_BY
  , r.LAST_ACTION_DATETIME
  , r.SYSTEM_REMARKS
  , r.SYSTEM_UPDATED_DATETIME
  , r.RELATED_FUNCTION_ID
  , r.RELATED_RECORD_ID
  , r.CIF_ANNUAL_INCOME_ID
  , r.CIF_NET_WORTH_ID
  , r.CIF_NET_WORTH
  , r.IS_POLITICALLY_EXPOSED_PERSON
  , r.IS_PDPA_DISCLOSURE
  , r.COLLATERAL_REFERENCE_NUMBER
  , r.E_INVOICE_INDICATOR_ID
  , r.E_INVOICE_INDICATOR
  , r.CUSTOMER_SST_REGISTRATION_NUMBER
  , r.BUSINESS_CODE
  , r.EMPLOYER_BUSINESS_CODE
  , r.ETL_TIMESTAMP
  , r.ETL_DT
  , 'A' as dl_record_status
  , case when com_t.COUNTERPARTY_ID is not null then com_t.dl_record_created_date else current_timestamp() end as dl_record_created_date
  , current_timestamp() as dl_record_updated_date
FROM {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_tomerge r
LEFT JOIN {params["com_schema"]}.t_lmskibb2_tbl_counterparty com_t
  ON r.COUNTERPARTY_ID = com_t.COUNTERPARTY_ID
WHERE r.etl_dt = '{batch_date}'
""")

#-- 3.0 Insert overwrite COM_T impacted partition(s) with updated data set from temp table
spark.sql(f"""
insert overwrite table {params["com_schema"]}.t_lmskibb2_tbl_counterparty
select
    RECORD_ID
  , CLEAN_RULE_FLAG
  , FUNCTION_ID
  , COUNTERPARTY_ID
  , ENTRY_DATE
  , ENTITY_ID
  , ENTITY_CODE
  , COUNTERPARTY_CLASS_ID
  , COUNTERPARTY_CLASS_CODE
  , COUNTERPARTY_TYPE_LIST
  , COUNTERPARTY_CODE
  , COUNTERPARTY_NAME
  , COUNTERPARTY_BASE_CCY_SECURITY_ID
  , CCRIS_ENTITY_TYPE_ID
  , CCRIS_CUSTOMER_LAST_MAINTENANCE_DATE
  , GENDER_CODE
  , TITLE_ID
  , TITLE
  , PRIMARY_IDENTIFICATION_TYPE
  , PRIMARY_IDENTIFICATION_NO
  , SECONDARY_IDENTIFICATION_TYPE
  , SECONDARY_IDENTIFICATION_NO
  , PASSPORT_EXPIRY_DATE
  , DATE_OF_BIRTH
  , COUNTRY_OF_BIRTH_COUNTRY_ID
  , COUNTRY_OF_BIRTH_COUNTRY_CODE
  , NATIONALITY_COUNTRY_ID
  , NATIONALITY_COUNTRY_CODE
  , COUNTERPARTY_COUNTRY_ID
  , COUNTERPARTY_COUNTRY_CODE
  , RACE_ID
  , RACE_CODE
  , BUMIPUTERA_STATUS_ID
  , BUMIPUTERA_STATUS
  , MARITAL_STATUS_ID
  , MARITAL_STATUS_CODE
  , WEBSITE
  , COUNTERPARTY_ADDRESS_1
  , COUNTERPARTY_ADDRESS_2
  , COUNTERPARTY_ADDRESS_3
  , COUNTERPARTY_ADDRESS_4
  , COUNTERPARTY_ADDRESS_POSTAL_CODE
  , COUNTERPARTY_ADDRESS_CITY
  , COUNTERPARTY_ADDRESS_STATE
  , COUNTERPARTY_ADDRESS_STATE_CODE
  , COUNTERPARTY_ADDRESS_COUNTRY_CODE
  , COUNTERPARTY_ADDRESS_LAST_MAINTENANCE_DATE
  , TAX_STATUS
  , BANK_ID
  , BANK_CODE
  , BANK_NAME
  , BANK_ACCOUNT_NUMBER
  , BANK_ACCOUNT_NAME
  , BUSINESS_CODE_ID
  , BUSINESS_DESC
  , RESIDENCY_STATUS_ID
  , RESIDENCY_STATUS_CODE
  , BNM_RESIDENCY_STATUS
  , COMPANY_TYPE_ID
  , COMPANY_TYPE
  , DATE_OF_INCORPORATION
  , COUNTRY_OF_INCORPORATION_COUNTRY_ID
  , COUNTRY_OF_INCORPORATION_COUNTRY_CODE
  , PAID_UP_CAPITAL_CCY_SECURITY_ID
  , PAID_UP_CAPITAL_AMOUNT
  , COUNTRY_OF_OPERATION_COUNTRY_ID
  , COUNTRY_OF_OPERATION_COUNTRY_CODE
  , CONSTITUTION
  , IS_ADVISORY_REGULATED
  , ADVISORY_REGULATED_REMARKS
  , TURNOVER_CCY_SECURITY_ID
  , TURNOVER_AMOUNT
  , TURNOVER_PROJECTED_AMOUNT
  , TURNOVER_TOTAL_BORROWING_AMOUNT
  , IS_OWNERSHIP_STRUCTURE_COMPLEX
  , OWNERSHIP_STRUCTURE_REMARKS
  , CCRIS_CORPORATE_STATUS_ID
  , CCRIS_INDUSTRIAL_SECTOR_ID
  , NEXT_OF_KIN_1_NAME
  , NEXT_OF_KIN_1_IDENTITY_TYPE_ID
  , NEXT_OF_KIN_1_IDENTITY_NUMBER
  , NEXT_OF_KIN_1_RELATIONSHIP_ID
  , NEXT_OF_KIN_1_PHONE_NO
  , NEXT_OF_KIN_1_OCCUPATION_ID
  , NEXT_OF_KIN_1_EMPLOYER_NAME
  , NEXT_OF_KIN_2_NAME
  , NEXT_OF_KIN_2_IDENTITY_TYPE_ID
  , NEXT_OF_KIN_2_IDENTITY_NUMBER
  , NEXT_OF_KIN_2_RELATIONSHIP_ID
  , NEXT_OF_KIN_2_PHONE_NO
  , NEXT_OF_KIN_2_OCCUPATION_ID
  , NEXT_OF_KIN_2_EMPLOYER_NAME
  , SOURCE_OF_INCOME
  , OTHER_SOURCE_OF_INCOME
  , PDPA_CONNECTED_PARTY
  , PDPA
  , PDPA_DISCLOSURE_REASON
  , IS_DOMESTIC_MYR_BORROW
  , REMAINING_BALANCE_FROM_INVESTMENT_CCY_SECURITY_ID
  , REMAINING_BALANCE_FROM_INVESTMENT
  , CURRENTLY_INVESTED_CCY_SECURITY_ID
  , CURRENTLY_INVESTED
  , IS_FATCA
  , FATCA_ACCOUNT_TYPE_ID
  , FATCA_ACCOUNT_CLASS_ID
  , IS_DIFFERENT_MAILING_ADDRESS
  , CORRESPONDENCE_ADDRESS_1
  , CORRESPONDENCE_ADDRESS_2
  , CORRESPONDENCE_ADDRESS_3
  , CORRESPONDENCE_ADDRESS_4
  , CORRESPONDENCE_ADDRESS_POSTAL_CODE
  , CORRESPONDENCE_ADDRESS_CITY
  , CORRESPONDENCE_ADDRESS_STATE
  , CORRESPONDENCE_ADDRESS_STATE_CODE
  , CORRESPONDENCE_ADDRESS_COUNTRY_CODE
  , PHONE_NO
  , HOME_PHONE_NO
  , OFFICE_PHONE_NO
  , CONTACT_PERSON_PHONE_NO
  , CONTACT_PERSON_NAME
  , EMAIL_ADDRESS
  , EMPLOYMENT_STATUS_ID
  , EMPLOYMENT_STATUS
  , NAME_OF_COMPANY
  , EMPLOYER_ADDRESS_1
  , EMPLOYER_ADDRESS_2
  , EMPLOYER_ADDRESS_3
  , EMPLOYER_ADDRESS_4
  , EMPLOYER_ADDRESS_POSTAL_CODE
  , EMPLOYER_ADDRESS_CITY
  , EMPLOYER_ADDRESS_STATE
  , EMPLOYER_ADDRESS_COUNTRY_CODE
  , AML_OCCUPATION_ID
  , AML_OCCUPATION
  , CCRIS_OCCUPATION_ID
  , CCRIS_OCCUPATION
  , CCRIS_OCCUPATION_CODE
  , EMPLOYER_BUSINESS_CODE_ID
  , EMPLOYER_BUSINESS_DESC
  , CCRIS_EMPLOYMENT_SECTOR_ID
  , CCRIS_EMPLOYMENT_SECTOR_CODE
  , CCRIS_EMPLOYMENT_TYPE_ID
  , CCRIS_EMPLOYMENT_TYPE_CODE
  , GROSS_ANNUAL_INCOME_DATE
  , GROSS_ANNUAL_INCOME_CCY_SECURITY_ID
  , GROSS_ANNUAL_INCOME_AMOUNT
  , COUNTRY_OF_RESIDENT_DECLARATION_ID
  , MLRPC_RISK_ID
  , CIF_DOCUMENT
  , RECORD_STATUS_ID
  , REMARKS
  , REVIEW_DATE
  , EFFECTIVE_FROM
  , EFFECTIVE_TO
  , USER_EFFECTIVE_TO
  , UPDATE_RECORD_ID
  , ACTION_TYPE_ID
  , ACTION_TYPE_SNAPSHOT_ID
  , LAST_ACTION_BY
  , LAST_ACTION_DATETIME
  , SYSTEM_REMARKS
  , SYSTEM_UPDATED_DATETIME
  , RELATED_FUNCTION_ID
  , RELATED_RECORD_ID
  , CIF_ANNUAL_INCOME_ID
  , CIF_NET_WORTH_ID
  , CIF_NET_WORTH
  , IS_POLITICALLY_EXPOSED_PERSON
  , IS_PDPA_DISCLOSURE
  , COLLATERAL_REFERENCE_NUMBER
  , E_INVOICE_INDICATOR_ID
  , E_INVOICE_INDICATOR
  , CUSTOMER_SST_REGISTRATION_NUMBER
  , BUSINESS_CODE
  , EMPLOYER_BUSINESS_CODE
  , etl_timestamp
  , etl_dt
  , DL_RECORD_STATUS
  , DL_RECORD_CREATED_DATE
  , DL_RECORD_UPDATED_DATE
from {params["com_schema"]}.temp_t_lmskibb2_tbl_counterparty_updated
""")

spark.sql(f"""analyze table {params["com_schema"]}.t_lmskibb2_tbl_counterparty compute statistics""")

spark.stop()