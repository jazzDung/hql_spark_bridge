/* 3.2 insert data to target table */
INSERT INTO ${com_schema}.r_k2_cif_alias PARTITION(part_id)
SELECT
  *
FROM ${com_schema}.r_k2_cif_alias_nw
WHERE
  end_dt <> '${month_start}'
UNION ALL
SELECT
  *
FROM ${com_schema}.r_k2_cif_alias_od
WHERE
  end_dt <> '${month_start}'
UNION ALL
SELECT
  *
FROM ${com_schema}.r_k2_cif_alias_bf
WHERE
  end_dt <> '${month_start}'