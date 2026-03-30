import sys

sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter

source_name = "ref"
table_name = "general_lookup"
hive_table_name = source_name + "_" + table_name

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

# add queries here
spark.sql(f"""

drop table if exists {params["raw_schema"]}.general_reference_lookup_et;
create external table if not exists {params["raw_schema"]}.general_reference_lookup_et(
    source_name string, -- Source identifier
    reference_type string, --
    reference_code string, --
    reference_value string, --
    reference_value_2 string --
)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '{params["itl_data_path"]}/general_reference_lookup_f.{params["batch_date"]}.dat'
tblproperties (
    "skip.header.line.count"="1"
);

""")


spark.sql(f"""
alter table {params["raw_schema"]}.general_reference_lookup drop if exists partition (etl_dt = '{batch_date}');
""")


spark.sql(f"""
insert into table {params["raw_schema"]}.general_reference_lookup partition (etl_dt = '{params["batch_date"]}')
select
    reference_type, --
    reference_code, --
    reference_value, --
    '{params["batch_timestamp"]}' as etl_timestamp, -- ETL processing time
    source_name, -- Source identifier
    reference_value_2 --
from {params["raw_schema"]}.general_reference_lookup_et;
""")


# housekeep partition if required

# add new spark.sql block if required
#spark.sql(f"""
#
#""")


# Stop Spark when done
spark.stop()