import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter

source_name = "fra"
table_name = "connected_parties"
hive_table_name = source_name + "_" + table_name

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

# add queries here
spark.sql(f"""
SELECT *
FROM test.all_para_delta3
""")

# housekeep partition if required

# add new spark.sql block if required
#spark.sql(f"""
#
#""")


# Stop Spark when done
spark.stop()