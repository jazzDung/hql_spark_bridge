import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day


jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

source_name = "k2"
table_name = "bank"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

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
    recstatus
FROM dbo.bank (NOLOCK)
WHERE 1 = 1
"""

# housekeep partition if required
drop_partition_day(spark, batch_date, params["raw_schema"], hive_table_name, partition_col, params["retention_raw_delta"])

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
target_table = f"{params['raw_schema']}.{hive_table_name}"
target_cols = spark.table(target_table).columns
df = df.select(*target_cols)
  
(
    df.write
    .mode("overwrite")
    .insertInto(f"{params['raw_schema']}.{hive_table_name}")
)

spark.stop()
