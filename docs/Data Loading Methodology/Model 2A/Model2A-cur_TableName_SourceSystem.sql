
--    File Name		: cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose		: [Model 2A] Truncate and reload CUR for full data refresh. 
--    Logs			: 20251219		Developer1			<UserStory/Task number> Create script
--					: 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Overwrite table with latest data 
INSERT OVERWRITE TABLE ${cur_schema}.<tablename_sourcesystem> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>')  
SELECT 
	<keycolumn1>
	, <column2>
	, <column3>	
	, <column4>	
	, current_timestamp() as record_created_date 
	, current_timestamp() as record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
FROM ${com_schema}.t_<sourcesystem_tablename>
;


-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename_sourcesystem> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>') COMPUTE STATISTICS;



