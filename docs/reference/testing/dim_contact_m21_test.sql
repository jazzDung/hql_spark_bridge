
/*==============[Group.15 + 16 + 17: M21, M21_A, M21_O Account Contact]==============*/
with temp_dim_account_contact_m21_row_num as (
  select
    OWNER_ID
    ,CONTACT_OWNER_TYPE
    ,CONTACT_TYPE
    ,CONTACT_VALUE
    ,CONTACT_NAME
    ,date_format(CONTACT_CREATE_TIME, 'yyyy-MM-dd') as CONTACT_CREATE_DATE-- None
    ,date_format(CONTACT_UPDATE_TIME, 'yyyy-MM-dd') as CONTACT_UPDATE_DATE-- None
    ,LINE_OF_BUSINESS
    ,SOURCE_NAME
    ,SOURCE_RECORD_ID
    ,SEQUENCE_NO
    ,row_number() over(partition by OWNER_ID, CONTACT_OWNER_TYPE, CONTACT_TYPE order by CONTACT_UPDATE_TIME desc) as rn
  from ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21
)
, temp_dim_account_contact_m21_row_num as (
  select
    OWNER_ID
    ,CONTACT_OWNER_TYPE
    ,CONTACT_TYPE
    ,CONTACT_VALUE
    ,CONTACT_NAME
    ,date_format(CONTACT_CREATE_TIME, 'yyyy-MM-dd') as CONTACT_CREATE_DATE-- None
    ,date_format(CONTACT_UPDATE_TIME, 'yyyy-MM-dd') as CONTACT_UPDATE_DATE-- None
    ,LINE_OF_BUSINESS
    ,SOURCE_NAME
    ,SOURCE_RECORD_ID
    ,SEQUENCE_NO
    ,row_number() over(partition by OWNER_ID, CONTACT_OWNER_TYPE, CONTACT_TYPE order by CONTACT_UPDATE_TIME desc) as rn
  from ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT_M21
)
insert into ${cur_schema}.TEMP_DIM_ACCOUNT_CONTACT(
  OWNER_ID -- None
  ,CONTACT_OWNER_TYPE -- None
  ,CONTACT_TYPE -- None
  ,CONTACT_VALUE -- None
  ,CONTACT_NAME -- None
  ,CONTACT_CREATE_DATE -- None
  ,CONTACT_UPDATE_DATE -- None
  ,LINE_OF_BUSINESS -- None
  ,SOURCE_NAME -- None
  ,SOURCE_RECORD_ID -- None
  ,SEQUENCE_NO -- None
)
select
  OWNER_ID -- None
  ,CONTACT_OWNER_TYPE -- None
  ,CONTACT_TYPE -- None
  ,CONTACT_VALUE -- None
  ,CONTACT_NAME -- None
  ,CONTACT_CREATE_DATE -- None
  ,CONTACT_UPDATE_DATE -- None
  ,LINE_OF_BUSINESS -- None
  ,SOURCE_NAME -- None
  ,SOURCE_RECORD_ID -- None
  ,SEQUENCE_NO -- None
from temp_dim_account_contact_m21_row_num
where rn = 1
;

