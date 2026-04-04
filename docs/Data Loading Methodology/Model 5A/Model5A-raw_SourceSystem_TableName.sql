
--    File Name     : raw_<sourcesystem_tablename>
--    File Type     : DML
--    Purpose       : [Model 5A] To read raw data from HDFS
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create external table to read the source data from HDFS
DROP TABLE IS EXISTS ${raw_schema}.<sourcesystem_tablename>_et; 
CREATE EXTERNAL TABLE IF NOT EXISTS ${raw_schema}.<sourcesystem_tablename>_et(
    <column1> <datatype> 
    ,<datecolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>   
)
row format delimited
fields terminated by '<column delimiter [database table: \001, file: |]>'
lines terminated by '\n'
stored as textfile
location '${itl_data_path}/<filename>.${batch_date}.dat'
;


-- 2.0 Insert overwrite partition with data
INSERT OVERWRITE TABLE ${raw_schema}.<sourcesystem_tablename> PARTITION ( etl_dt = '${batch_date}' )
SELECT
    <column1> <datatype> 
    ,<datecolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype> 
    ,'${batch_timestamp}' as etl_timestamp --ETL_processing time
FROM ${raw_schema}.<sourcesystem_tablename>_et
;


-- Drop history partitions - Data retention 
ALTER TABLE ${raw_schema}.<sourcesystem_tablename> DROP IF EXISTS PARTITION ( etl_dt < '${retain_day}' );




