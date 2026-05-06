-- Purpose:    DML/COM I-STATUS
-- Author:     zjj
-- Usage:      python $ETL_HOME/script/main.py yyyymmdd com_r_k2_cif_alias
-- CreateDate: 20230809
-- FileType:   DML
-- Logs:
--     1.for hive 3.x on cdp 7.1.5

-- 0.1 set parameter
source /jcmAgent/etlscript/execution_engine/autocode/dml/para_config/all_tez_para.sql;



-- 1.1 get last date data
-- if backup table is exists, mean script if failed on last time
create table if not exists ${com_schema}.r_k2_cif_alias_bk
-- stored as parquet
-- tblproperties(
--     'parquet.compression'='SNAPPY'
--     ,'external.table.purge'='true'
-- )
as
select
    cifaliasid
    ,cifid
    ,aliastype
    ,aliasvalue
    ,effectivefrom
    ,effectiveto
    ,updateuser
    ,updatets
    ,etl_timestamp
    ,case when '${batch_date}' = '${month_start}' then '${month_start}' else start_dt end as start_dt
    ,case when '${batch_date}' = '${month_start}' then '${next_month_start}' else end_dt end as end_dt
    ,id_mark
    ,part_id
from ${com_schema}.r_k2_cif_alias
where start_dt < '${batch_date}'
and end_dt >= '${batch_date}'
and part_id >= substr(replace(cast(date_add(from_unixtime(unix_timestamp('${batch_date}' , 'yyyyMMdd')), -1) as string),'-',''),1,6)
and part_id <= '${batch_yyyymm}'
;


-- 1.2 back history data
create table if not exists ${com_schema}.r_k2_cif_alias_bf
-- stored as parquet
-- tblproperties(
--     'parquet.compression'='SNAPPY'
--     ,'external.table.purge'='true'
-- )
as
select
    cifaliasid
    ,cifid
    ,aliastype
    ,aliasvalue
    ,effectivefrom
    ,effectiveto
    ,updateuser
    ,updatets
    ,etl_timestamp
    ,start_dt
    ,end_dt
    ,id_mark
    ,part_id
from ${com_schema}.r_k2_cif_alias
where end_dt < '${batch_date}'
  and part_id = '${batch_yyyymm}'
;


-- 2.1 create temp table:get new, alter, delete data and put into temp table
drop table if exists ${com_schema}.r_k2_cif_alias_nw;
create table if not exists ${com_schema}.r_k2_cif_alias_nw
-- stored as parquet
-- tblproperties(
--     'parquet.compression'='SNAPPY'
--     ,'external.table.purge'='true'
-- )
as
select
     nvl(n.cifaliasid, o.cifaliasid) as cifaliasid
     ,nvl(n.cifid, o.cifid) as cifid
     ,nvl(n.aliastype, o.aliastype) as aliastype
     ,nvl(n.aliasvalue, o.aliasvalue) as aliasvalue
     ,nvl(n.effectivefrom, o.effectivefrom) as effectivefrom
     ,nvl(n.effectiveto, o.effectiveto) as effectiveto
     ,nvl(n.updateuser, o.updateuser) as updateuser
     ,nvl(n.updatets, o.updatets) as updatets
     ,'${batch_timestamp}' as etl_timestamp
     ,case when n.etl_dt is null then o.start_dt
      else '${batch_date}'
      end as start_dt
     ,case when n.etl_dt is null then '${batch_date}'
      else '${next_month_start}'
      end as end_dt
     ,'I' as id_mark
     ,'${batch_yyyymm}' as part_id
from (
        select
        cifaliasid
        ,cifid
        ,aliastype
        ,aliasvalue
        ,effectivefrom
        ,effectiveto
        ,updateuser
        ,updatets
        ,etl_dt
        from ${raw_schema}.k2_cif_alias
        where etl_dt='${batch_date}'
    ) n
    left join ${com_schema}.r_k2_cif_alias_bk o
    on
        o.cifaliasid = n.cifaliasid
where (
    o.cifaliasid is null
    ) or (
    nvl( cast(o.cifid as string) , '' ) <> nvl( cast(n.cifid as string) , '' )
    or nvl( cast(o.aliastype as string) , '' ) <> nvl( cast(n.aliastype as string) , '' )
    or nvl( cast(o.aliasvalue as string) , '' ) <> nvl( cast(n.aliasvalue as string) , '' )
    or nvl( cast(o.effectivefrom as string) , '' ) <> nvl( cast(n.effectivefrom as string) , '' )
    or nvl( cast(o.effectiveto as string) , '' ) <> nvl( cast(n.effectiveto as string) , '' )
    or nvl( cast(o.updateuser as string) , '' ) <> nvl( cast(n.updateuser as string) , '' )
    or nvl( cast(o.updatets as string) , '' ) <> nvl( cast(n.updatets as string) , '' )
)
;


--2.1.1 ddl-insert-sundexin
truncate table ${com_schema}.r_k2_cif_alias_nw;

insert into table ${com_schema}.r_k2_cif_alias_nw
select
     nvl(n.cifaliasid, o.cifaliasid) as cifaliasid
     ,nvl(n.cifid, o.cifid) as cifid
     ,nvl(n.aliastype, o.aliastype) as aliastype
     ,nvl(n.aliasvalue, o.aliasvalue) as aliasvalue
     ,nvl(n.effectivefrom, o.effectivefrom) as effectivefrom
     ,nvl(n.effectiveto, o.effectiveto) as effectiveto
     ,nvl(n.updateuser, o.updateuser) as updateuser
     ,nvl(n.updatets, o.updatets) as updatets
     ,'${batch_timestamp}' as etl_timestamp
     ,case when n.etl_dt is null then o.start_dt
      else '${batch_date}'
      end as start_dt
     ,case when n.etl_dt is null then '${batch_date}'
      else '${next_month_start}'
      end as end_dt
     ,'I' as id_mark
     ,'${batch_yyyymm}' as part_id
from (
        select
        cifaliasid
        ,cifid
        ,aliastype
        ,aliasvalue
        ,effectivefrom
        ,effectiveto
        ,updateuser
        ,updatets
        ,etl_dt
--        from ${raw_schema}.k2_cif_alias
--        where etl_dt='${batch_date}'
from
(select * from (select *, row_number() over (partition by cifaliasid order by raw_sys_time desc) rn from ${raw_schema}.k2_cif_alias where etl_dt = '${batch_date}') t1 where t1.rn = 1 order by 1 ) t2 

    ) n
    left join ${com_schema}.r_k2_cif_alias_bk o
    on
        o.cifaliasid = n.cifaliasid
where (
    o.cifaliasid is null
    ) or (
    nvl( cast(o.cifid as string) , '' ) <> nvl( cast(n.cifid as string) , '' )
    or nvl( cast(o.aliastype as string) , '' ) <> nvl( cast(n.aliastype as string) , '' )
    or nvl( cast(o.aliasvalue as string) , '' ) <> nvl( cast(n.aliasvalue as string) , '' )
    or nvl( cast(o.effectivefrom as string) , '' ) <> nvl( cast(n.effectivefrom as string) , '' )
    or nvl( cast(o.effectiveto as string) , '' ) <> nvl( cast(n.effectiveto as string) , '' )
    or nvl( cast(o.updateuser as string) , '' ) <> nvl( cast(n.updateuser as string) , '' )
    or nvl( cast(o.updatets as string) , '' ) <> nvl( cast(n.updatets as string) , '' )
)
;


-- 2.2 create temp table:get unchange, alter(close) data and put into temp table
drop table if exists ${com_schema}.r_k2_cif_alias_od;
create table if not exists ${com_schema}.r_k2_cif_alias_od
-- stored as parquet
-- tblproperties(
--     'parquet.compression'='SNAPPY'
--     ,'external.table.purge'='true'
-- )
as
select
        o.cifaliasid
        ,o.cifid
        ,o.aliastype
        ,o.aliasvalue
        ,o.effectivefrom
        ,o.effectiveto
        ,o.updateuser
        ,o.updatets
        ,o.etl_timestamp
        ,o.start_dt
        ,case when n.start_dt is not null then '${batch_date}'
         when o.end_dt >= '${batch_date}'  then '${next_month_start}'
         else o.end_dt
         end as end_dt
        ,'I'  as id_mark
        ,'${batch_yyyymm}' as part_id
from ${com_schema}.r_k2_cif_alias_bk o
    left join ${com_schema}.r_k2_cif_alias_nw n
    on
        o.cifaliasid = n.cifaliasid
where nvl(n.end_dt, '${next_month_start}') <> '${batch_date}'
;



-- 3.1 truncate target table
alter table ${com_schema}.r_k2_cif_alias drop if exists partition (part_id = '${batch_yyyymm}');

-- 3.2 insert data to target table
insert into table ${com_schema}.r_k2_cif_alias partition (part_id)
select * from ${com_schema}.r_k2_cif_alias_nw
where end_dt <> '${month_start}'
union all 
select * from ${com_schema}.r_k2_cif_alias_od
where end_dt <> '${month_start}'
union all 
select * from ${com_schema}.r_k2_cif_alias_bf
where end_dt <> '${month_start}'
;

-- 5.2 drop temp table
drop table ${com_schema}.r_k2_cif_alias_bk;
drop table ${com_schema}.r_k2_cif_alias_bf;
drop table ${com_schema}.r_k2_cif_alias_nw;
drop table ${com_schema}.r_k2_cif_alias_od;

