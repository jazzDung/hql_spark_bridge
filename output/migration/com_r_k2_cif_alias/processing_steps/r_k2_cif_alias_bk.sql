/* 1.1 get last date data */ /* if backup table is exists, mean script if failed on last time */
CREATE TABLE IF NOT EXISTS ${com_schema}.r_k2_cif_alias_bk AS
SELECT
  cifaliasid,
  cifid,
  aliastype,
  aliasvalue,
  effectivefrom,
  effectiveto,
  updateuser,
  updatets,
  etl_timestamp,
  CASE WHEN '${batch_date}' = '${month_start}' THEN '${month_start}' ELSE start_dt END AS start_dt,
  CASE
    WHEN '${batch_date}' = '${month_start}'
    THEN '${next_month_start}'
    ELSE end_dt
  END AS end_dt,
  id_mark,
  part_id
FROM ${com_schema}.r_k2_cif_alias
WHERE
  start_dt < '${batch_date}'
  AND end_dt >= '${batch_date}'
  AND part_id >= SUBSTRING(
    REPLACE(
      CAST(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${batch_date}', 'yyyyMMdd')), -1) AS STRING),
      '-',
      ''
    ),
    1,
    6
  )
  AND part_id <= '${batch_yyyymm}'

/* 5.2 drop temp table */
DROP TABLE ${com_schema}.r_k2_cif_alias_bk