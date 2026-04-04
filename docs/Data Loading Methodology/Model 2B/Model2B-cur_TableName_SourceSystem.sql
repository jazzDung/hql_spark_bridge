
--    File Name		: cur_<tablename_sourcesystem>
--    File Type		: DML
--    Purpose		: [Model 2B] Load data to curated by insert overwrite snapshot date partition. 
--    Logs			: 20251219		Developer1			<UserStory/Task number> Create script
--					: 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
SOURCE /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


-- 1.0 Create temp table to store updated complete data set
DROP TABLE IF EXISTS ${tmp_schema}.temp_<tablename_sourcesystem>_updated;
CREATE TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated (
    <snapshot_date> DATE
    ,<keycolumn1> <datatype> 
    ,<column2> <datatype>   
    ,<column3> <datatype>   
    ,<column4> <datatype>  
    ,<transformed_col1> <datatype>   
    ,<transformed_col2> <datatype> 
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
    ,year_month STRING
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);


-- 2.1 Insert unchanged records from CUR to temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
    <snapshot_date>
    ,<keycolumn1>
    ,<column2>
    ,<column3>
    ,<column4>
    ,<transformed_col1>
    ,<transformed_col2>
    ,record_created_date
    ,record_updated_date
    ,year_month
FROM ${cur_schema}.<tablename_sourcesystem>  
WHERE PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>')  
    --in normal scenario, batch date = today, refer to data model logic used for snapshot_date. 
    --if snapshot_date = {yesterday}, then should apply the condition accordingly 
    --this is because COM_T only keep single snapshot of data, hence only need to exclude that snapshot from CUR
    AND year_month = SUBSTR('${batch_date}', 1, 6) --replace {batch_date} according to data model logic. e.g.: SUBSTR('${yesterday}', 1, 6) 
    AND <snapshot_date> != from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd') --replace {batch_date} according to data model logic
;


-- 2.2 Insert snapshot data from COM_T to temp table
INSERT INTO TABLE ${tmp_schema}.temp_<tablename_sourcesystem>_updated
SELECT 
    from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd') as <snapshot_date> --replace {batch_date} according to data model logic
    ,<keycolumn1>
    ,<column2>
    ,<column3>
    ,<column4>
    ,<transformation logic 1> as <transformed_col1>
    ,<transformation logic 1> as <transformed_col2>
    ,current_timestamp() as record_created_date
    ,current_timestamp() as record_updated_date
    ,SUBSTR('${batch_date}', 1, 6) as year_month --replace {batch_date} according to data model logic
FROM ${com_schema}.t_<sourcesystem_tablename>
;


-- 3.0 Overwrite table's partition with latest data 
INSERT OVERWRITE TABLE ${cur_schema}.<tablename_sourcesystem> 
PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>', year_month )
SELECT 
	<snapshot_date>
    , <keycolumn1>
	, <column2>
	, <column3>	
	, <column4>	
    , record_created_date
    , record_updated_date
    , '${batch_date}' as etl_dt
    , '${batch_timestamp}' as etl_timestamp
    ,year_month
FROM ${tmp_schema}.temp_<tablename_sourcesystem>_updated
;


-- Collect table's partition statistics 
ANALYZE TABLE ${cur_schema}.<tablename_sourcesystem> 
PARTITION (SOURCE_KEY = '<if partition applicable, else remove clause>', year_month) 
COMPUTE STATISTICS;



