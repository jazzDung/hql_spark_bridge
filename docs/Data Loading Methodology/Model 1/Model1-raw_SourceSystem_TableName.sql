
--    File Name		: raw_<sourcesystem_tablename>
--    File Type		: DML
--    Purpose		: [Model 1] To read raw data from HDFS 
--    Logs			: 20251219		Developer1			<UserStory/Task number> Create script
--					: 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create external table to read the source data from HDFS
DROP TABLE IS EXISTS ${raw_schema}.<sourcesystem_tablename>_et;
CREATE EXTERNAL TABLE IF NOT EXISTS ${raw_schema}.<sourcesystem_tablename>_et (
    keycolumn1 <datatype>
    ,column2 <datatype>
    ,column3 <datatype>
    ,addr1 <datatype>
    ,addr2 <datatype>
)
row format delimited
fields terminated by '<column delimiter [database table: \001, file: |]>'
lines terminated by '\n'
stored as textfile
location '${itl_data_path}/<filename>.${etl_dt}.dat'
;

-- 2.0 Insert overwrite partition with data
INSERT OVERWRITE TABLE ${raw_schema}.<sourcesystem_tablename> PARTITION (ETL_DT = '${etl_dt}')
SELECT
    keycolumn1
    ,column2
    ,column3
    ,addr1
    ,addr2
FROM ${raw_schema}.<sourcesystem_tablename>_et
;

-- Drop history partitions - Data retention 
ALTER TABLE ${raw_schema}.<sourcesystem_tablename> DROP IF EXISTS PARTITION ( etl_dt < '${retain_day}' )
;

