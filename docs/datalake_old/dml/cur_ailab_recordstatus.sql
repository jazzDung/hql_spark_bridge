-- Purpose:    RAW-DML,Load the data file into the target table's same-day partition
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd LMSKIBB2_
-- CreateDate: 20230907
-- FileType:   DML
-- Logs:
--     1.for hive 3.x on cdp 7.1.5


-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_para.sql;


-- 2.1 drop partition
ALTER TABLE ${cur_schema}.AILAB_RECORDSTATUS DROP IF EXISTS PARTITION ( etl_dt = '${batch_date}' );

-- 2.2 insert data to target table
INSERT INTO TABLE ${cur_schema}.AILAB_RECORDSTATUS PARTITION ( etl_dt = '${batch_date}' )
SELECT
    T0.RECORD_STATUS_ID AS RECORD_STATUS_ID
    , T0.RECORD_STATUS AS RECORD_STATUS
    , T0.CREATED_BY AS CREATED_BY
    , T0.CREATED_DATETIME AS CREATED_DATETIME
    , T0.LAST_UPDATED_BY AS LAST_UPDATED_BY
    , T0.LAST_UPDATED_DATETIME AS LAST_UPDATED_DATETIME
    , T0.DELETED_BY AS DELETED_BY
    , T0.DELETED_DATETIME AS DELETED_DATETIME
    , T0.AUTHORIZED_BY AS AUTHORIZED_BY
    , T0.AUTHORIZED_DATETIME AS AUTHORIZED_DATETIME
    ,'${batch_timestamp}' AS ETL_TIMESTAMP -- etl processing time
FROM ${raw_schema}.LMSKIBB2_TBL_RECORDSTATUS T0
WHERE T0.ETL_DT = '${batch_date}'
;

