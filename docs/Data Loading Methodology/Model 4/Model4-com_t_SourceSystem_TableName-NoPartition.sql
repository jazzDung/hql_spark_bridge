
--    File Name     : com_t_<sourcesystem_tablename>
--    File Type		: DML
--    Purpose       : [Model 4] Insert overwrite table by delete and re-insert updated records based on key column(s). 
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp table to store updated complete data set
DROP TABLE IF EXISTS ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated;
CREATE TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated (
    <keycolumn1> <datatype> 
    ,<keycolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>  
    ,<transformed_col1> <datatype>   
    ,<transformed_col2> <datatype> 
    ,record_status VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.0 Populate latest complete dataset to temp table
-- 2.1 Insert existing unchanged records from COM_T to temp table
INSERT INTO TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated
SELECT 
	<keycolumn1>
	, <keycolumn2>
	, <column3>	
	, <column4>	
    , <transformed_col1>
    , <transformed_col2>
    , record_status 
    , record_created_date 
    , record_updated_date 
FROM ${com_schema}.t_<sourcesystem_tablename> com_t
WHERE NOT EXISTS 
    (SELECT 1 FROM ${raw_schema}.<sourcesystem_tablename> r 
    WHERE r.etl_dt = '${batch_date}' 
    AND r.<keycolumn1> = com_t.<keycolumn1> AND r.<keycolumn2> = com_t.<keycolumn2> ) 


-- 2.2 Insert latest records from RAW to temp table

/************** this is section to write all transformation logic for data from RAW ***************/
/************** once all transformation completed, load the cleaned records temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to temp table
INSERT INTO TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated
SELECT 
	r.<keycolumn1>
	, r.<keycolumn2>
	, r.<column3>	
	, r.<column4>	
    , <transformation logic 1> as <transformed_col1>
    , <transformation logic 2> as <transformed_col2>
    , 'A' as record_status 
    , CASE WHEN com_t.<keycolumn1> IS NOT NULL THEN com_t.record_created_date ELSE current_timestamp() END as record_created_date 
    , current_timestamp() record_updated_date 
FROM ${raw_schema}.<sourcesystem_tablename> r 
LEFT JOIN ${com_schema}.t_<sourcesystem_tablename> com_t 
ON r.<keycolumn1> = com_t.<keycolumn1> AND r.<keycolumn2> = com_t.<keycolumn2>
WHERE r.etl_dt = '${batch_date}' 
;


-- 3.0 Insert overwrite COM_T impacted partition(s) with updated data set from temp table
INSERT OVERWRITE TABLE ${com_schema}.t_<sourcesystem_tablename>
SELECT 
    <keycolumn1>
	, <keycolumn2>
	, <column3>	
	, <column4>	
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
FROM ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated;


-- Collect table statistics 
ANALYZE TABLE ${com_schema}.t_<sourcesystem_tablename> COMPUTE STATISTICS;


