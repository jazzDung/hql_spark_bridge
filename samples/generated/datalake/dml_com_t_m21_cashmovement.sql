--    File Name     : com_t_m21_cashmovement
--    File Type		: DML
--    Purpose       : [Model 5B] Insert overwrite impacted partition(s) by delete and re-insert updated records based on date range. 
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp table(s) 
-- 1.1 Create temp table to store updated complete data set for impacted partitions
DROP TABLE IF EXISTS ${com_schema}.temp_t_m21_cashmovement_updated;
CREATE TABLE ${com_schema}.temp_t_m21_cashmovement_updated (
    Firm INT
    , TransactionNo INT
    , TransactionDate TIMESTAMP
    , ValueDate TIMESTAMP
    , DateCleared TIMESTAMP
    , DateCancelled TIMESTAMP
    , CustOrBroker TINYINT
    , CustBroker INT
    , BankAccount INT
    , BankAccountGL INT
    , Type TINYINT
    , Currency INT
    , Amount STRING
    , ModeOfPayment TINYINT
    , ChequeNo STRING
    , Remarks STRING
    , Reference INT
    , CreateUser INT
    , CreateDate TIMESTAMP
    , ModifyUser INT
    , ModifyDate TIMESTAMP
    , SourceDocPrefix STRING
    , UserRef STRING
    , ChqBankAcro STRING
    , SourceOfFunds STRING
    , record_status VARCHAR(10)
    , record_created_date TIMESTAMP
    , record_updated_date TIMESTAMP
    , etl_timestamp STRING COMMENT 'etl_processing_time'
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 1.2 Create temp table to store impacted partitions
DROP TABLE IF EXISTS ${com_schema}.temp_t_m21_cashmovement_partitions;
CREATE TABLE ${com_schema}.temp_t_m21_cashmovement_partitions (
    year_month STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.0 Populate latest complete dataset to temp table - only for impacted partitions
-- 2.1 Identify impacted partition(s) from RAW and load partition value(s) to temp table 
INSERT INTO TABLE ${com_schema}.temp_t_m21_cashmovement_partitions
SELECT DISTINCT
    year_month
FROM ${raw_schema}.t_m21_cashmovement
WHERE etl_dt = '${batch_date}';


-- 2.2 Insert existing unchanged records from COM_T to temp table
INSERT INTO TABLE ${com_schema}.temp_t_m21_cashmovement_updated
SELECT 
    Firm
    , TransactionNo
    , TransactionDate
    , ValueDate
    , DateCleared
    , DateCancelled
    , CustOrBroker
    , CustBroker
    , BankAccount
    , BankAccountGL
    , Type
    , Currency
    , TRIM(Amount) AS Amount
    , ModeOfPayment
    , TRIM(ChequeNo) AS ChequeNo
    , TRIM(Remarks) AS Remarks
    , Reference
    , CreateUser
    , CreateDate
    , ModifyUser
    , ModifyDate
    , TRIM(SourceDocPrefix) AS SourceDocPrefix
    , TRIM(UserRef) AS UserRef
    , TRIM(ChqBankAcro) AS ChqBankAcro
    , TRIM(SourceOfFunds) AS SourceOfFunds
    , year_month
FROM ${com_schema}.t_m21_cashmovement com_t
INNER JOIN ${com_schema}.temp_t_m21_cashmovement_partitions p -- to filter COM_T only for impacted partition(s)    ON com_t.year_month = p.year_month
WHERE NOT EXISTS ( -- get unchanged records based on date column (date range) and partition column(s)
    SELECT 1 FROM ${raw_schema}.t_m21_cashmovement r
    WHERE
        r.etl_dt = '${batch_date}'
        AND r.year_month = com_t.year_month
        AND r.TransactionDate = com_t.TransactionDate
);


-- 2.3 Insert latest records from RAW to temp table 

/************** this is section to write all transformation logic for data from RAW ***************/
/************** once all transformation completed, load the cleaned records temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to temp table
INSERT INTO TABLE ${com_schema}.temp_t_m21_cashmovement_updated
SELECT 
    Firm
    , TransactionNo
    , TransactionDate
    , ValueDate
    , DateCleared
    , DateCancelled
    , CustOrBroker
    , CustBroker
    , BankAccount
    , BankAccountGL
    , Type
    , Currency
    , Amount
    , ModeOfPayment
    , ChequeNo
    , Remarks
    , Reference
    , CreateUser
    , CreateDate
    , ModifyUser
    , ModifyDate
    , SourceDocPrefix
    , UserRef
    , ChqBankAcro
    , SourceOfFunds
    , 'A' as record_status 
    , CURRENT_TIMESTAMP() record_created_date
    , CURRENT_TIMESTAMP() record_updated_date
    , year_month
FROM ${raw_schema}.t_m21_cashmovement
WHERE etl_dt = '${batch_date}' 
;


-- 3.0 Insert overwrite COM_T impacted partition(s) with updated data set from temp table
INSERT OVERWRITE TABLE ${com_schema}.t_m21_cashmovement PARTITION (
    year_month
)
SELECT 
    Firm
    , TransactionNo
    , TransactionDate
    , ValueDate
    , DateCleared
    , DateCancelled
    , CustOrBroker
    , CustBroker
    , BankAccount
    , BankAccountGL
    , Type
    , Currency
    , Amount
    , ModeOfPayment
    , ChequeNo
    , Remarks
    , Reference
    , CreateUser
    , CreateDate
    , ModifyUser
    , ModifyDate
    , SourceDocPrefix
    , UserRef
    , ChqBankAcro
    , SourceOfFunds
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
    , year_month
FROM ${com_schema}.temp_t_m21_cashmovement_updated;


-- Collect table statistics 
ANALYZE TABLE ${com_schema}.t_m21_cashmovement COMPUTE STATISTICS;

