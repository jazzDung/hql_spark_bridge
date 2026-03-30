import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day


jdbc_url = (
    f"jdbc:hive2://172.21.5.63:10000/raw"
)
user = "root"
password = "root"

source_name = "fra"
table_name = "connected_parties"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

classpath = spark._jvm.java.lang.System.getProperty("java.class.path")

# Split classpath into individual paths (colon-separated on Linux/macOS, semicolon on Windows)
if os.name == "nt":
    paths = classpath.split(";")
else:
    paths = classpath.split(":")

# Filter for hive-jdbc jars
hive_jdbc_jars = [p for p in paths if re.search(r"hive-jdbc.*\.jar", p, re.IGNORECASE)]

print("Hive JDBC JARs loaded by Spark:")
for jar in hive_jdbc_jars:
    print(jar)

query = """
(select record_type, supplier, source_system, account_no from raw.guava_trx where etl_dt = '20230717' limit 2) t
"""

# Read from MSSQL
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", user)
    .option("password", password)
    .option("driver", "org.apache.hive.jdbc.HiveDriver")
    .load()
)

df.show()

spark.stop()