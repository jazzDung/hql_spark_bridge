
--    File Name     : com_t_<sourcesystem_tablename>
--    File Type		: DML
--    Purpose       : [Model 5B] Insert overwrite impacted partition(s) by delete and re-insert updated records based on date range. 
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp table(s) 
-- 1.1 Create temp table to store updated complete data set for impacted partitions
DROP TABLE IF EXISTS ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated;
CREATE TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated (
    <column1> <datatype> 
    ,<datecolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>  
    ,<transformed_col1> <datatype>   
    ,<transformed_col2> <datatype> 
    ,record_status VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
    ,<data_partition_column1> STRING
    ,<data_partition_column2> STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 1.2 Create temp table to store impacted partitions
DROP TABLE IF EXISTS ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions;
CREATE TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions (
    <data_partition_column1> STRING
    ,<data_partition_column2> STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.0 Populate latest complete dataset to temp table - only for impacted partitions
-- 2.1 Identify impacted partition(s) from RAW and load partition value(s) to temp table 
INSERT INTO TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions
SELECT DISTINCT 
    <data_partition_column1>
    ,<data_partition_column2>
FROM ${raw_schema}.<sourcesystem_tablename>  
WHERE etl_dt = '${batch_date}' ; 


-- 2.2 Insert existing unchanged records from COM_T to temp table
INSERT INTO TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated
SELECT 
	com_t.<column1>
	, com_t.<datecolumn2>
	, com_t.<column3>	
	, com_t.<column4>	
    , com_t.<transformed_col1>
    , com_t.<transformed_col2>
    , com_t.record_status 
    , com_t.record_created_date 
    , com_t.record_updated_date 
    , com_t.<data_partition_column1>
    , com_t.<data_partition_column2>
FROM ${com_schema}.t_<sourcesystem_tablename> com_t
INNER JOIN ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions p -- to filter COM_T only for impacted partition(s)
    ON com_t.<data_partition_column1> = p.<data_partition_column1> 
    AND com_t.<data_partition_column2> = p.<data_partition_column2> 
WHERE NOT EXISTS -- get unchanged records based on date column (date range) and partition column(s)
    (SELECT 1 FROM ${raw_schema}.<sourcesystem_tablename> r 
    WHERE r.etl_dt = '${batch_date}' 
    AND r.<datecolumn2> = com_t.<datecolumn2>
    AND r.<data_partition_column1> = com_t.<data_partition_column1> AND r.<data_partition_column2> = com_t.<data_partition_column2>)


-- 2.3 Insert latest records from RAW to temp table 

/************** this is section to write all transformation logic for data from RAW ***************/
/************** once all transformation completed, load the cleaned records temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to temp table
INSERT INTO TABLE ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated
SELECT 
	<column1>
	, <datecolumn2>
	, <column3>	
	, <column4>	
    , <transformation logic 1> as <transformed_col1>
    , <transformation logic 2> as <transformed_col2>
    , 'A' as record_status 
    , current_timestamp() record_created_date 
    , current_timestamp() record_updated_date 
    , <data_partition_column1>
    , <data_partition_column2>
FROM ${raw_schema}.<sourcesystem_tablename>
WHERE etl_dt = '${batch_date}' 
;


-- 3.0 Insert overwrite COM_T impacted partition(s) with updated data set from temp table
INSERT OVERWRITE TABLE ${com_schema}.t_<sourcesystem_tablename> PARTITION (<data_partition_column1>, <data_partition_column2>)
SELECT 
    <column1>
	, <datecolumn2>
	, <column3>	
	, <column4>	
    , <transformed_col1>
    , <transformed_col2>
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
    , <data_partition_column1>
    , <data_partition_column2>
FROM ${tmp_schema}.temp_t_<sourcesystem_tablename>_updated;


-- Collect table statistics 
ANALYZE TABLE ${com_schema}.t_<sourcesystem_tablename> COMPUTE STATISTICS;


