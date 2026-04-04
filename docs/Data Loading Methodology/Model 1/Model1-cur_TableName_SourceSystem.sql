
--    File Name		: cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose		: [Model 1] Insert overwrite table by delete and re-insert updated records.
--    Logs			: 20251219		Developer1			<UserStory/Task number> Create script
--					: 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp tables
-- 1.1 Create temp table to store updated transformed COM_T
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_delta;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta (
    <keycolumn1> <datatype> 
    ,<column2> <datatype>   
    ,<column3> <datatype>   
    ,<full_address> <datatype>   
    ,<curated_column1> <datatype> 
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
    ,<column2> <datatype>   
    ,<column3> <datatype>   
    ,<full_address> <datatype>   
    ,<curated_column1> <datatype> 
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
-- 2.1 Insert latest records from COM_T to temp table 

/************** this is section to write all transformation logic for data from COM_T ***************/
/************** once all transformation completed, load the cleaned records to temp table ***************/

--Below shows sample of simple transformation logic that can be writen in single query, and directly load to delta temp table

INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_delta
SELECT 
    <keycolumn1>
    , <column2>
    , <column3>	
    , <full_address>
    , <curated transformation logic 1> as <curated_column1>
    , record_status 
    , current_timestamp() record_created_date 
    , current_timestamp() record_updated_date 
FROM ${com_schema}.t_<sourcesystem_tablename>
;


-- 2.2 Insert new & updated records from COM_T to temp table 
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	delta.<keycolumn1>
	, delta.<column2>
	, delta.<column3>	
    , delta.<full_address>
    , delta.<curated_column1>
    , delta.record_status 
    , IF(cur.<keycolumn1> IS NOT NULL, cur.record_created_date, delta.record_created_date) AS record_created_date 
    , delta.record_updated_date 
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta 
LEFT JOIN ${cur_schema}.<tablename> cur  
    ON cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND delta.<keycolumn1> = cur.<keycolumn1>  
;


-- 2.3 Insert removed CUR records into temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
	cur.<keycolumn1>
	, cur.<column2>
	, cur.<column3>	
    , cur.<full_address>
    , cur.<curated_column1>
    , 'X' as record_status 
    , cur.record_created_date 
    , current_timestamp() as record_updated_date -- when hash_value is different, means record has changes
FROM ${cur_schema}.<tablename> cur   
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    NOT EXISTS 
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta 
        WHERE cur.<keycolumn1> AND delta.<keycolumn1>
        )
;


-- 3.0 Insert overwrite CUR with updated data set from temp table
INSERT OVERWRITE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>')  
SELECT 
    <keycolumn1>
	, <column2>
	, <column3>	
	, <full_address>
    , <curated_column1>	
    , record_status 
    , record_created_date 
    , record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_updated;



-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>') COMPUTE STATISTICS ; 



