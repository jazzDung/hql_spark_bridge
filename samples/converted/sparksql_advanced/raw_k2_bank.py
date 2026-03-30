"""
Purpose:    RAW-DDL-CREATE TABLE
Author:     zjj
Usage:      python $ETL_HOME/script/init.py raw k2_bank
CreateDate: 20230907
FileType:   DDL
Logs:
1.for hive 3.x on cdp 7.1.5
1.0 drop table if exists table
"""

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, batch_start, batch_end
from pyspark.sql.functions import current_timestamp, lit

source_name = "k2"
table_name = "bank"

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter()

# spark session
spark, batch_date = run_etl(source_name, table_name)

# update control table start, first task in the flow only
batch_start(spark, source_name, table_name)


# --- OPTIMIZED BLOCKS ---
spark.sql(f"""
DROP TABLE IF EXISTS {params["raw_schema"]}.k2_bank
""")
# -- JDBC Read Optimization cho bảng dbo.bank --
jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

query = """
SELECT
    bankid,
    localbankcode,
    bankname,
    swiftcode,
    oribankid,
    approveuser,
    approvets,
    createuser,
    createts,
    updateuser,
    updatets,
    recstatus,
    etl_timestamp
FROM dbo.bank (NOLOCK)
WHERE 1 = 1
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

# update control table end, last task in the flow only
batch_end(spark, source_name, table_name)

# Stop Spark when done
spark.stop()