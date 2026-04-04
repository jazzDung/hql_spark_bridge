
--    File Name		: com_t_<sourcesystem_tablename>
--    File Type		: DML
--    Purpose		: [Model 1] Insert overwrite table by delete and re-insert updated records.
--    Logs			: 20251219		Developer1			<UserStory/Task number> Create script
--					: 20251220 		Developer2			<UserStory/Task number> <changes description>


-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;


DROP TABLE IS EXISTS ${com_schema}.temp_t_<sourcesystem_tablename>; 
CREATE EXTERNAL TABLE IF NOT EXISTS ${com_schema}.temp_t_<sourcesystem_tablename> (
    keycolumn1 <datatype>
    ,column2 <datatype>
    ,column3 <datatype>
    ,addr1 <datatype>
    ,addr2 <datatype>
    ,full_address <datatype>
    ,record_status <datatype>
    ,record_created_date <datatype>
    ,record_updated_date <datatype>
)
stored as parquet
tblproperties(
   'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);

-- 1.0 Insert new & updated records from RAW into Temp table
INSERT INTO TABLE ${com_schema}.temp_t_<sourcesystem_tablename>
SELECT 
    cr.keycolumn1
    ,cr.column2
    ,cr.column3
    ,UPPER(cr.addr1)
    ,UPPER(cr.addr2)
    ,concat(coalesce(UPPER(cr.addr1),''),' ',coalesce(UPPER(cr.addr2),'')) as full_address
    ,'A' as record_status
    ,CASE WHEN ct.keycolumn1 IS NOT NULL THEN ct.record_created_date ELSE current_timestamp() END as record_created_date
    ,current_timestamp() as record_updated_date
FROM ${raw_schema}.<sourcesystem_tablename> r
LEFT JOIN ${com_schema}.t_<sourcesystem_tablename> ct ON (r.keycolumn1 = ct.keycolumn1)
;

-- 1.1 Update removed/deleted COM_T records into Temp table
INSERT INTO TABLE ${com_schema}.temp_t_<sourcesystem_tablename>
SELECT 
    ct.keycolumn1
    ,ct.column2
    ,ct.column3
    ,ct.full_address
    ,'X' as record_status
    ,ct.record_created_date
    ,current_timestamp() as record_updated_date
FROM ${com_schema}.t_<sourcesystem_tablename> ct
WHERE NOT EXISTS 
  (SELECT 1 FROM ${raw_schema}.<sourcesystem_tablename> r 
  WHERE r.etl_dt = '${batch_date}' AND ct.keycolumn1 = r.keycolumn1)
;


-- 1.2 Overwrite table with latest data
INSERT OVERWRITE TABLE ${com_schema}.t_<sourcesystem_tablename>
SELECT
    keycolumn1
    ,column2
    ,column3
    ,addr1
    ,addr2
    ,full_address
    ,record_status
    ,hash_value
    ,record_created_date
    ,record_updated_date
    ,'${etl_dt}' as etl_dt
    ,'${etl_timestamp}' as etl_timestamp
FROM ${com_schema}.temp_t_<sourcesystem_tablename>
;


-- Collect table statistics 
ANALYZE TABLE ${com_schema}.t_<sourcesystem_tablename> COMPUTE STATISTICS;


