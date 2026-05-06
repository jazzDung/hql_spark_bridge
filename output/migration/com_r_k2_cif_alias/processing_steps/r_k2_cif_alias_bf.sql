/* 1.2 back history data */
CREATE TABLE IF NOT EXISTS ${com_schema}.r_k2_cif_alias_bf AS
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
  start_dt,
  end_dt,
  id_mark,
  part_id
FROM ${com_schema}.r_k2_cif_alias
WHERE
  end_dt < '${batch_date}' AND part_id = '${batch_yyyymm}'

DROP TABLE ${com_schema}.r_k2_cif_alias_bf