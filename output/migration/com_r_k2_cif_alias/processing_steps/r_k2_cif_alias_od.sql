/* 2.2 create temp table:get unchange, alter(close) data and put into temp table */
DROP TABLE IF EXISTS ${com_schema}.r_k2_cif_alias_od

CREATE TABLE IF NOT EXISTS ${com_schema}.r_k2_cif_alias_od AS
SELECT
  o.cifaliasid,
  o.cifid,
  o.aliastype,
  o.aliasvalue,
  o.effectivefrom,
  o.effectiveto,
  o.updateuser,
  o.updatets,
  o.etl_timestamp,
  o.start_dt,
  CASE
    WHEN NOT n.start_dt IS NULL
    THEN '${batch_date}'
    WHEN o.end_dt >= '${batch_date}'
    THEN '${next_month_start}'
    ELSE o.end_dt
  END AS end_dt,
  'I' AS id_mark,
  '${batch_yyyymm}' AS part_id
FROM ${com_schema}.r_k2_cif_alias_bk AS o
LEFT JOIN ${com_schema}.r_k2_cif_alias_nw AS n
  ON o.cifaliasid = n.cifaliasid
WHERE
  COALESCE(n.end_dt, '${next_month_start}') <> '${batch_date}'

DROP TABLE ${com_schema}.r_k2_cif_alias_od