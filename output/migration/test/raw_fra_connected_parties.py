import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")

from etl_common_function import run_etl, set_parameter, drop_partition_day, batch_start, batch_end

source_name = "fra"
table_name = "connected_parties"

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter()

# spark session
spark, batch_date = run_etl(source_name, table_name)

# update control table start, first task in the flow only
batch_start(spark, source_name, table_name)

# add queries here
spark.sql(f"""
drop table if exists {params["raw_schema"]}.fra_connected_parties_et
""")

print(f"""{params["itl_data_path"]}/{batch_date}/ConnectedParties_Data_{batch_date}.TXT""")

spark.sql(f"""
create external table if not exists {params["raw_schema"]}.fra_connected_parties_et(
    staffname string -- 
    ,staff_nric string -- 
    ,staff_oldic string -- 
    ,cp_name string -- 
    ,cp_nric string -- 
    ,cp_oldic string -- 
    ,cp_relationship string -- 
    ,actual_status string -- 
    ,define_status string -- 
    ,processdate timestamp -- 
)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '{params["itl_data_path"]}/{batch_date}/ConnectedParties_Data_{batch_date}.TXT'
tblproperties (
    "skip.header.line.count"="1"
)
""")

# housekeep partition if required
drop_partition_day(spark, batch_date, params["raw_schema"], "fra_connected_parties", "etl_dt", params["retention_raw_delta"])

spark.sql(f"""
alter table {params["raw_schema"]}.fra_connected_parties drop if exists partition (etl_dt='{batch_date}')
""")

spark.sql(f"""
insert into table {params["raw_schema"]}.fra_connected_parties partition ( etl_dt = '{batch_date}' )
select
    staffname -- 
    ,staff_nric -- 
    ,staff_oldic -- 
    ,cp_name -- 
    ,cp_nric -- 
    ,cp_oldic -- 
    ,cp_relationship -- 
    ,actual_status -- 
    ,define_status -- 
    ,processdate -- 
    ,current_timestamp() as etl_timestamp --ETL_processing time
from {params["raw_schema"]}.fra_connected_parties_et
""")

# update control table end, last task in the flow only
batch_end(spark, source_name, table_name)

# Stop Spark when done
spark.stop()