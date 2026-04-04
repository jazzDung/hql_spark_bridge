
--    File Name     : cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose       : [Model 4] Insert overwrite table by delete and re-insert updated records based on COM_T delta.
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp tables
-- 1.1 Create temp table to store updated transformed COM_T delta
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_delta;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta (
    <keycolumn1> <datatype> 
    ,<keycolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<cur_transformed_col1> <datatype>   
    ,<cur_transformed_col2> <datatype> 
    ,record_status VARCHAR(10)
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
    <keycolumn1> <datatype> 
    ,<keycolumn2> <datatype>   
    ,<column3> <datatype>   
    ,<cur_transformed_col1> <datatype>   
    ,<cur_transformed_col2> <datatype> 
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
-- 2.1 Insert latest records from COM_T to temp table, identify COM_T delta based on record_updated_date 

/************** this is section to write all transformation logic for delta from COM_T ***************/
/************** once all transformation completed, load the cleaned delta records temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to delta temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta
SELECT 
	<keycolumn1>
	, <keycolumn2>
	, <column3>	
    , <transformation logic 1> as <cur_transformed_col1>
    , <transformation logic 2> as <cur_transformed_col2>
    , record_status 
    , current_timestamp() record_created_date 
    , current_timestamp() record_updated_date 
FROM ${com_schema}.t_<sourcesystem_tablename>
WHERE DATE_FORMAT(record_updated_date, 'yyyyMMdd') = '${batch_date}' -- criteria to identify delta from COM_T
;


-- 2.2 Insert existing unchanged records from CUR to temp table - CUR records not in COM_T delta
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	<keycolumn1>
	, <keycolumn2>
	, <column3>	
    , <cur_transformed_col1>
    , <cur_transformed_col2>
    , record_status 
    , record_created_date 
    , record_updated_date 
FROM ${cur_schema}.<tablename> cur
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    NOT EXISTS --identify records not exists in COM_T delta
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta
        WHERE delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>)


-- 2.3 Insert transformed delta into temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	delta.<keycolumn1>
	, delta.<keycolumn2>
	, delta.<column3>	
    , delta.<cur_transformed_col1>
    , delta.<cur_transformed_col2>
    , delta.record_status 
    , IF(cur.<keycolumn1> IS NOT NULL, cur.record_created_date, delta.record_created_date) as record_created_date
    , delta.record_updated_date
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta  
LEFT JOIN ${cur_schema}.<tablename> cur ON cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>
;


-- 3.0 Insert CUR records exists in delta into history table
INSERT INTO TABLE ${cur_schema}.<tablename>_h PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>', hist_year)  
SELECT 
	<keycolumn1>
	, <keycolumn2>
	, <column3>	
    , <cur_transformed_col1>
    , <cur_transformed_col2>
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
    , DATE_FORMAT(current_timestamp(), 'yyyy') as hist_year
FROM ${cur_schema}.<tablename> cur
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    EXISTS --identify records exists in COM_T delta
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta
        WHERE delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>)


-- 4.0 Insert overwrite CUR with updated data set from temp table
INSERT OVERWRITE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>')  
SELECT 
    <keycolumn1>
	, <keycolumn2>
	, <column3>	
	, <column4>	
    , <cur_transformed_col1>
    , <cur_transformed_col2>
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_updated;



-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>') COMPUTE STATISTICS ; 


