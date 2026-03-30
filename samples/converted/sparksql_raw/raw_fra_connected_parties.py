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
-- Purpose:    RAW-DML,Load the data file into the target table's same-day partition
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_fra_connected_parties
-- CreateDate: 20230809
-- FileType:   DML
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 0.1 set parameter
-- Bỏ qua lệnh không chuẩn: source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.1 drop external table partition
drop table if exists {params["raw_schema"]}.fra_connected_parties_et; 
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
location '${itl_data_path}/fra_connected_parties_i.{batch_date}.dat'
tblproperties (
    "skip.header.line.count"="1"
)
;


-- 2.0 drop history partition
alter table {params["raw_schema"]}.fra_connected_parties drop if exists partition ( etl_dt = '{params["retention_raw_delta"]}' );

-- 2.1 drop partition
alter table {params["raw_schema"]}.fra_connected_parties drop if exists partition ( etl_dt = '{batch_date}' );

-- 2.2 insert data to target table
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
    ,'{current_timestamp()}' as etl_timestamp --ETL_processing time
from {params["raw_schema"]}.fra_connected_parties_et
;




""")

# update control table end, last task in the flow only
batch_end(spark, source_name, table_name)

# Stop Spark when done
spark.stop()