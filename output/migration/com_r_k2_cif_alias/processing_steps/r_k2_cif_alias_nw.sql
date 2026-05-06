/* 2.1 create temp table:get new, alter, delete data and put into temp table */
DROP TABLE IF EXISTS ${com_schema}.r_k2_cif_alias_nw

CREATE TABLE IF NOT EXISTS ${com_schema}.r_k2_cif_alias_nw AS
SELECT
  COALESCE(n.cifaliasid, o.cifaliasid) AS cifaliasid,
  COALESCE(n.cifid, o.cifid) AS cifid,
  COALESCE(n.aliastype, o.aliastype) AS aliastype,
  COALESCE(n.aliasvalue, o.aliasvalue) AS aliasvalue,
  COALESCE(n.effectivefrom, o.effectivefrom) AS effectivefrom,
  COALESCE(n.effectiveto, o.effectiveto) AS effectiveto,
  COALESCE(n.updateuser, o.updateuser) AS updateuser,
  COALESCE(n.updatets, o.updatets) AS updatets,
  '${batch_timestamp}' AS etl_timestamp,
  CASE WHEN n.etl_dt IS NULL THEN o.start_dt ELSE '${batch_date}' END AS start_dt,
  CASE WHEN n.etl_dt IS NULL THEN '${batch_date}' ELSE '${next_month_start}' END AS end_dt,
  'I' AS id_mark,
  '${batch_yyyymm}' AS part_id
FROM (
  SELECT
    cifaliasid,
    cifid,
    aliastype,
    aliasvalue,
    effectivefrom,
    effectiveto,
    updateuser,
    updatets,
    etl_dt
  FROM ${raw_schema}.k2_cif_alias
  WHERE
    etl_dt = '${batch_date}'
) AS n
LEFT JOIN ${com_schema}.r_k2_cif_alias_bk AS o
  ON o.cifaliasid = n.cifaliasid
WHERE
  (
    o.cifaliasid IS NULL
  )
  OR (
    COALESCE(CAST(o.cifid AS STRING), '') <> COALESCE(CAST(n.cifid AS STRING), '')
    OR COALESCE(CAST(o.aliastype AS STRING), '') <> COALESCE(CAST(n.aliastype AS STRING), '')
    OR COALESCE(CAST(o.aliasvalue AS STRING), '') <> COALESCE(CAST(n.aliasvalue AS STRING), '')
    OR COALESCE(CAST(o.effectivefrom AS STRING), '') <> COALESCE(CAST(n.effectivefrom AS STRING), '')
    OR COALESCE(CAST(o.effectiveto AS STRING), '') <> COALESCE(CAST(n.effectiveto AS STRING), '')
    OR COALESCE(CAST(o.updateuser AS STRING), '') <> COALESCE(CAST(n.updateuser AS STRING), '')
    OR COALESCE(CAST(o.updatets AS STRING), '') <> COALESCE(CAST(n.updatets AS STRING), '')
  )

TRUNCATE TABLE   ${com_schema}.r_k2_cif_alias_nw /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.r_k2_cif_alias_nw
SELECT
  COALESCE(n.cifaliasid, o.cifaliasid) AS cifaliasid,
  COALESCE(n.cifid, o.cifid) AS cifid,
  COALESCE(n.aliastype, o.aliastype) AS aliastype,
  COALESCE(n.aliasvalue, o.aliasvalue) AS aliasvalue,
  COALESCE(n.effectivefrom, o.effectivefrom) AS effectivefrom,
  COALESCE(n.effectiveto, o.effectiveto) AS effectiveto,
  COALESCE(n.updateuser, o.updateuser) AS updateuser,
  COALESCE(n.updatets, o.updatets) AS updatets,
  '${batch_timestamp}' AS etl_timestamp,
  CASE WHEN n.etl_dt IS NULL THEN o.start_dt ELSE '${batch_date}' END AS start_dt,
  CASE WHEN n.etl_dt IS NULL THEN '${batch_date}' ELSE '${next_month_start}' END AS end_dt,
  'I' AS id_mark,
  '${batch_yyyymm}' AS part_id
FROM (
  SELECT
    cifaliasid,
    cifid,
    aliastype,
    aliasvalue,
    effectivefrom,
    effectiveto,
    updateuser,
    updatets,
    etl_dt
  /*        from ${raw_schema}.k2_cif_alias */ /*        where etl_dt='${batch_date}' */
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cifaliasid ORDER BY raw_sys_time DESC) AS rn
      FROM ${raw_schema}.k2_cif_alias
      WHERE
        etl_dt = '${batch_date}'
    ) AS t1
    WHERE
      t1.rn = 1
    ORDER BY
      1
  ) AS t2
) AS n
LEFT JOIN ${com_schema}.r_k2_cif_alias_bk AS o
  ON o.cifaliasid = n.cifaliasid
WHERE
  (
    o.cifaliasid IS NULL
  )
  OR (
    COALESCE(CAST(o.cifid AS STRING), '') <> COALESCE(CAST(n.cifid AS STRING), '')
    OR COALESCE(CAST(o.aliastype AS STRING), '') <> COALESCE(CAST(n.aliastype AS STRING), '')
    OR COALESCE(CAST(o.aliasvalue AS STRING), '') <> COALESCE(CAST(n.aliasvalue AS STRING), '')
    OR COALESCE(CAST(o.effectivefrom AS STRING), '') <> COALESCE(CAST(n.effectivefrom AS STRING), '')
    OR COALESCE(CAST(o.effectiveto AS STRING), '') <> COALESCE(CAST(n.effectiveto AS STRING), '')
    OR COALESCE(CAST(o.updateuser AS STRING), '') <> COALESCE(CAST(n.updateuser AS STRING), '')
    OR COALESCE(CAST(o.updatets AS STRING), '') <> COALESCE(CAST(n.updatets AS STRING), '')
  )

DROP TABLE ${com_schema}.r_k2_cif_alias_nw