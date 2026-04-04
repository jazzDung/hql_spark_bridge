
--    File Name     : cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose       : [Model 5B] Insert overwrite table by delete and re-insert updated records based on date range. 
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp tables
-- 1.1 Create temp table to store updated transformed COM_T delta
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_delta;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta (
    <column1> <datatype> 
    ,<datecolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>  
    ,<transformed_col1> <datatype>   
    ,<transformed_col2> <datatype> 
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 1.2 Create temp table to store updated complete data set
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_updated;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated (
    <column1> <datatype> 
    ,<datecolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>  
    ,<cur_transformed_col1> <datatype>   
    ,<cur_transformed_col2> <datatype> 
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.0 Populate latest complete dataset to temp table
-- 2.1 Insert latest records with transformation logic from COM_T to temp table

--get impacted as_of_date value for updated COM_T records, regardless COM_T record_status (to include records being patched manually for CUR refresh)
WITH delta_date AS (
    SELECT DISTINCT <datecolumn2>
    FROM ${com_schema}.t_<sourcesystem_tablename> 
    WHERE DATE_FORMAT(record_updated_date, 'yyyyMMdd') = '${batch_date}' 
)
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta
-- get complete active COM_T records for impacted as_of_date
SELECT 
	com_t.<column1>
	, com_t.<datecolumn2>
	, com_t.<column3>	
	, com_t.<column4>	
    , com_t.<transformed_col1>
    , com_t.<transformed_col2>
    , com_t.record_created_date 
    , com_t.record_updated_date 
FROM ${com_schema}.t_<sourcesystem_tablename> com_t
INNER JOIN delta_date delta ON com_t.<datecolumn2> = delta.<datecolumn2> 
WHERE com_t.record_status = 'A'  --should only get active transactions to CUR
;


-- 2.2 Insert existing unchanged records from CUR to temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	<column1>
	, <datecolumn2>
	, <column3>	
	, <column4>	
    , <transformed_col1>
    , <transformed_col2>
    , record_created_date 
    , record_updated_date 
FROM ${cur_schema}.<tablename> cur
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    NOT EXISTS --identify records not exists in COM_T delta
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta
        WHERE delta.<datecolumn2> = cur.<datecolumn2>)


-- 2.2 Insert latest records from COM_T to temp table, identify COM_T delta based on record_updated_date 

/************** this is section to write all transformation logic for delta from COM_T ***************/
/************** once all transformation completed, load the cleaned delta records temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	<column1>
	, <datecolumn2>
	, <column3>	
	, <column4>	
    , <transformation logic 1> as <cur_transformed_col1>
    , <transformation logic 2> as <cur_transformed_col2>
    , current_timestamp() record_created_date 
    , current_timestamp() record_updated_date 
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta
;


-- 3.0 Insert overwrite CUR with updated data set from temp table
INSERT OVERWRITE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>')  
SELECT 
    <column1>
	, <datecolumn2>
	, <column3>	
	, <column4>	
    , <cur_transformed_col1>
    , <cur_transformed_col2>
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_updated;



-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename> 
PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>') 
COMPUTE STATISTICS ; 



