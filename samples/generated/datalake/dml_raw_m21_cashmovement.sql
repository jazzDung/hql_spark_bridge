-- Purpose:    RAW-DML - Load data from HDFS (Model 5B)
-- Author:     dungp
-- CreateDate: 2026-04-09 08:07:54
-- FileType:   DML

-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;

-- 1.0 Create external table to read the source data from HDFS
DROP TABLE IF EXISTS ${raw_schema}.m21_cashmovement_et;

CREATE EXTERNAL TABLE IF NOT EXISTS ${raw_schema}.m21_cashmovement_et(
    FIRM INT
    , TRANSACTIONNO INT
    , TRANSACTIONDATE TIMESTAMP
    , VALUEDATE TIMESTAMP
    , DATECLEARED TIMESTAMP
    , DATECANCELLED TIMESTAMP
    , CUSTORBROKER TINYINT
    , CUSTBROKER INT
    , BANKACCOUNT INT
    , BANKACCOUNTGL INT
    , TYPE TINYINT
    , CURRENCY INT
    , AMOUNT STRING
    , MODEOFPAYMENT TINYINT
    , CHEQUENO STRING
    , REMARKS STRING
    , REFERENCE INT
    , CREATEUSER INT
    , CREATEDATE TIMESTAMP
    , MODIFYUSER INT
    , MODIFYDATE TIMESTAMP
    , SOURCEDOCPREFIX STRING
    , USERREF STRING
    , CHQBANKACRO STRING
    , SOURCEOFFUNDS STRING
    , RAW_SYS_TIME STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${itl_data_path}/m21_cashmovement_i.${batch_date}.dat'

;

-- 2.0 Insert overwrite partition with data
INSERT OVERWRITE TABLE ${raw_schema}.m21_cashmovement
PARTITION (
    etl_dt = '${batch_date}'
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
    , RAW_SYS_TIME AS RAW_SYS_TIME
    , '${batch_timestamp}' AS ETL_TIMESTAMP
    , NULL AS HASH_VALUE
FROM ${raw_schema}.m21_cashmovement_et
;

-- 3.0 Drop history partitions - Data retention (Optional)
-- ALTER TABLE ${raw_schema}.m21_cashmovement DROP IF EXISTS PARTITION ( etl_dt < '${retain_day}' );

-- 4.0 Cleanup External Table
DROP TABLE IF EXISTS ${raw_schema}.m21_cashmovement