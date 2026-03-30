-- Purpose:    RAW-DML, Load the data file into the target table's same-day partition with support for multiple sources
-- Author:     marco
-- ModifiedBy: [Your Name]
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd raw_general_reference_lookup
-- CreateDate: 20240529
-- FileType:   DML
-- Logs:
--     1. For Hive 3.x on CDP 7.1.5
--     2. Modified to handle multiple sources and store the source_name column

-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;

-- 1.1 drop external table partition
drop table if exists ${raw_schema}.general_reference_lookup_et;
create external table if not exists ${raw_schema}.general_reference_lookup_et(
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
location '${itl_data_path}/general_reference_lookup_f.${batch_date}.dat'
tblproperties (
    "skip.header.line.count"="1"
);

-- 2.1 drop partition
alter table ${raw_schema}.general_reference_lookup drop if exists partition (etl_dt = '${batch_date}');

-- 2.2 insert data to target table
insert into table ${raw_schema}.general_reference_lookup partition (etl_dt = '${batch_date}')
select
    reference_type, --
    reference_code, --
    reference_value, --
    '${batch_timestamp}' as etl_timestamp, -- ETL processing time
    source_name, -- Source identifier
    reference_value_2 --
from ${raw_schema}.general_reference_lookup_et;
