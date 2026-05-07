"""
Purpose:    RAW-DML,Load the data file into the target table's same-day partition
Author:     Gia Dung
Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_lmskibb2_tbl_account
CreateDate: 20250515
FileType:   DML
Logs:
1.for hive 3.x on cdp 7.1.5
0.1 set parameter
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

source_name = "lmskibb2"
table_name = "tbl_account"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
last_date = yesterday_date
batch_yyyymm = batch_date[:-2]

# ext_start_time = datetime.strptime(ext_start_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
# ext_end_time = datetime.strptime(ext_end_time, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)



spark.sql(rf"""
/* 1.1 drop external table */
DROP TABLE IF EXISTS {params["raw_schema"]}.LMSKIBB2_TBL_ACCOUNT_ET
""")

spark.sql(rf"""
CREATE EXTERNAL TABLE IF NOT EXISTS {params["raw_schema"]}.LMSKIBB2_TBL_ACCOUNT_ET (
  RECORD_ID INT,
  FUNCTION_ID INT,
  ACCOUNT_ID INT,
  COUNTERPARTY_CLASS_ID INT,
  COUNTERPARTY_TYPE_ID INT,
  COUNTERPARTY_ID_1 INT,
  COUNTERPARTY_ID_2 INT,
  COUNTERPARTY_ID_3 INT,
  COUNTERPARTY_ID_4 INT,
  ACCOUNT_NUMBER STRING,
  ACCOUNT_NAME STRING,
  SCHEME_ID INT,
  ACCOUNT_BASE_CCY_SECURITY_ID INT,
  EMAIL_ADDRESS STRING,
  ACCOUNT_CONTACT_PERSON STRING,
  ACCOUNT_CONTACT_PERSON_PHONE_NO STRING,
  ACCOUNT_STATUS_ID INT,
  OPENING_DATE TIMESTAMP,
  CLOSURE_DATE TIMESTAMP,
  RELATED_STATIC_ID INT,
  RELATED_RECORD_ID INT,
  RELATED_FUNCTION_ID INT,
  REVIEW_DATE TIMESTAMP,
  REMARKS STRING,
  EFFECTIVE_FROM TIMESTAMP,
  EFFECTIVE_TO TIMESTAMP,
  USER_EFFECTIVE_TO TIMESTAMP,
  RECORD_STATUS_ID INT,
  UPDATE_RECORD_ID INT,
  ACTION_TYPE_ID INT,
  ACTION_TYPE_SNAPSHOT_ID INT,
  LAST_ACTION_BY STRING,
  LAST_ACTION_DATETIME TIMESTAMP,
  SYSTEM_REMARKS STRING,
  SYSTEM_UPDATED_DATETIME TIMESTAMP,
  ENTITY_ID INT
)
""")

# -- JDBC Read Optimization cho bảng dbo.tbl_account --
jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

query = """
SELECT 
Record_Id
, Function_Id
, Account_Id
, Counterparty_Class_Id
, Counterparty_Type_Id
, Counterparty_Id_1
, Counterparty_Id_2
, Counterparty_Id_3
, Counterparty_Id_4
, Account_Number
, Account_Name
, Scheme_Id
, Account_Base_Ccy_Security_Id
, Email_Address
, Account_Contact_Person
, Account_Contact_Person_Phone_No
, Account_Status_Id
, Opening_Date
, Closure_Date
, Related_Static_Id
, Related_Record_Id
, Related_Function_Id
, Review_Date
, Remarks
, Effective_From
, Effective_To
, User_Effective_To
, Record_Status_Id
, Update_Record_Id
, Action_Type_Id
, Action_Type_Snapshot_Id
, Last_Action_By
, Last_Action_Datetime
, System_Remarks
, System_Updated_Datetime
, Entity_Id
FROM ${db_schema}.tbl_Account (nolock)
where Last_Action_Datetime >= CONVERT(datetime, '${start_timestamp}', 126)
or System_Updated_Datetime >= CONVERT(datetime, '${start_timestamp}', 126)
;
"""

# Read from MSSQL
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", query)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)

df = df.withColumn("etl_timestamp", current_timestamp().cast("string"))
df = df.withColumn("etl_dt", lit(batch_date))


# Write directly to Hive
target_table = f"{params['raw_schema']}.{hive_table_name}_et"
target_cols = spark.table(target_table).columns
df = df.select(*target_cols)

(
    df.write
    .mode("overwrite")
    .insertInto(f"{params['raw_schema']}.{hive_table_name}")
)


# add queries here

spark.sql(rf"""
/* 2.2 insert data to target table */
INSERT INTO {params["raw_schema"]}.LMSKIBB2_TBL_ACCOUNT PARTITION(ETL_DT = '{batch_date}')
SELECT
  RECORD_ID,
  FUNCTION_ID,
  ACCOUNT_ID,
  COUNTERPARTY_CLASS_ID,
  COUNTERPARTY_TYPE_ID,
  COUNTERPARTY_ID_1,
  COUNTERPARTY_ID_2,
  COUNTERPARTY_ID_3,
  COUNTERPARTY_ID_4,
  TRIM(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER,
  TRIM(ACCOUNT_NAME) AS ACCOUNT_NAME,
  SCHEME_ID,
  ACCOUNT_BASE_CCY_SECURITY_ID,
  TRIM(EMAIL_ADDRESS) AS EMAIL_ADDRESS,
  TRIM(ACCOUNT_CONTACT_PERSON) AS ACCOUNT_CONTACT_PERSON,
  TRIM(ACCOUNT_CONTACT_PERSON_PHONE_NO) AS ACCOUNT_CONTACT_PERSON_PHONE_NO,
  ACCOUNT_STATUS_ID,
  OPENING_DATE,
  CLOSURE_DATE,
  RELATED_STATIC_ID,
  RELATED_RECORD_ID,
  RELATED_FUNCTION_ID,
  REVIEW_DATE,
  TRIM(REMARKS) AS REMARKS,
  EFFECTIVE_FROM,
  EFFECTIVE_TO,
  USER_EFFECTIVE_TO,
  RECORD_STATUS_ID,
  UPDATE_RECORD_ID,
  ACTION_TYPE_ID,
  ACTION_TYPE_SNAPSHOT_ID,
  TRIM(LAST_ACTION_BY) AS LAST_ACTION_BY,
  LAST_ACTION_DATETIME,
  TRIM(SYSTEM_REMARKS) AS SYSTEM_REMARKS,
  SYSTEM_UPDATED_DATETIME,
  ENTITY_ID,
  CURRENT_TIMESTAMP() AS ETL_TIMESTAMP /* etl processing time */
FROM {params["raw_schema"]}.LMSKIBB2_TBL_ACCOUNT_ET
""")


# Stop Spark when done
spark.stop()