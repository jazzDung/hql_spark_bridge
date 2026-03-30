import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl_delta, set_parameter, drop_partition_day

vflow_name = "flow_erc_fra_connected_parties"

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter()

# spark session
spark, batch_date = run_etl_delta(flow_name=vflow_name)

# add queries here
drop_partition_day(spark, batch_date, params["raw_schema"], "fra_connected_parties", "etl_dt", params["retention_raw_delta"])

# Stop Spark when done
spark.stop()