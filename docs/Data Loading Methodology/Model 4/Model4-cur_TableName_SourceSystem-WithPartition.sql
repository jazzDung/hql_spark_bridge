
--    File Name     : cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose       : [Model 4] Insert overwrite table's impacted partition(s) by delete and re-insert updated records based on key column(s). 
--    Logs          : 20251219		Developer1			<UserStory/Task number> Create script
--                  : 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp table(s)
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
    ,<data_partition_column1> STRING
    ,<data_partition_column2> STRING
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
    ,<data_partition_column1> STRING
    ,<data_partition_column2> STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 1.3 Create temp table to store impacted partitions
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_partitions;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_partitions (
    <data_partition_column1> STRING
    ,<data_partition_column2> STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.0 Populate latest complete dataset to temp table
-- 2.1 Insert latest records with transformation logic from COM_T to temp table, identify COM_T delta based on record_updated_date 

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
    , <data_partition_column1>
    , <data_partition_column2>
FROM ${com_schema}.t_<sourcesystem_tablename>
WHERE DATE_FORMAT(record_updated_date, 'yyyyMMdd') = '${batch_date}' -- criteria to identify delta from COM_T
;


-- 2.2 Identify impacted partition(s) from delta and load partition value(s) to temp table 
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_partitions
SELECT DISTINCT 
    <data_partition_column1>
    ,<data_partition_column2>
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta;


-- 2.2 Insert existing unchanged records from CUR to temp table
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
    , <data_partition_column1>
    , <data_partition_column2>
FROM ${cur_schema}.<tablename> cur
INNER JOIN ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions p -- to filter CUR only for impacted partition(s)
    ON cur.<data_partition_column1> = p.<data_partition_column1> 
    AND cur.<data_partition_column2> = p.<data_partition_column2> 
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    NOT EXISTS -- get unchanged records based on key column(s) and partition column(s)
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta
        WHERE delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>
        AND delta.<data_partition_column1> = cur.<data_partition_column1> AND delta.<data_partition_column2> = cur.<data_partition_column2>)


-- 2.3 Insert transformed delta COM_T to temp table
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
    , delta.<data_partition_column1>
    , delta.<data_partition_column2>
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta
LEFT JOIN ${cur_schema}.<tablename> cur ON cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>
;


 -- 3.0 Insert CUR old records exists in delta into history table
INSERT INTO TABLE ${cur_schema}.<tablename>_h PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>', <data_partition_column1>, <data_partition_column2>, hist_year)
SELECT 
	cur.<keycolumn1>
	, cur.<keycolumn2>
	, cur.<column3>	
    , cur.<cur_transformed_col1>
    , cur.<cur_transformed_col2>
    , cur.record_status 
    , cur.record_created_date 
    , cur.record_updated_date 
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
    , cur.<data_partition_column1>
    , cur.<data_partition_column2>
    , DATE_FORMAT(current_timestamp(), 'yyyy') as hist_year
FROM ${cur_schema}.<tablename> cur
INNER JOIN ${tmp_schema}.temp_t_<sourcesystem_tablename>_partitions p -- to filter CUR only for impacted partition(s)
    ON cur.<data_partition_column1> = p.<data_partition_column1> 
    AND cur.<data_partition_column2> = p.<data_partition_column2> 
WHERE cur.SOURCE_KEY = '<if partition applicable, else remove clause>' AND 
    EXISTS -- get old records based on key column(s) and partition column(s)
        (SELECT 1 FROM ${tmp_schema}.temp_<tablename_sourcesystem>_delta delta
        WHERE delta.<keycolumn1> = cur.<keycolumn1> AND delta.<keycolumn2> = cur.<keycolumn2>
        AND delta.<data_partition_column1> = cur.<data_partition_column1> AND delta.<data_partition_column2> = cur.<data_partition_column2>)


-- 4.0 Insert overwrite CUR impacted partition(s) dynamically with updated data set from temp table
INSERT OVERWRITE TABLE ${cur_schema}.<tablename> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>', <data_partition_column1>, <data_partition_column2>)
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
    , <data_partition_column1>
    , <data_partition_column2>
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_updated;



-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename_sourcesystem> PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>') COMPUTE STATISTICS;



