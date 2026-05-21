

WITH filtered_clients AS (
    SELECT DISTINCT client_no
    FROM ${com_schema}.r_mhbos_m_client
    WHERE part_id = '${batch_date}'
    UNION
    SELECT DISTINCT client_no
    FROM ${com_schema}.r_mhbos_m_client_ext
    WHERE part_id = '${batch_date}'
),
latest_clients AS (
    SELECT mmc.*
        ,ROW_NUMBER() OVER (PARTITION BY mmc.client_no ORDER BY mmc.part_id DESC) AS rn
    FROM ${com_schema}.r_mhbos_m_client mmc
    INNER JOIN filtered_clients fc
        ON mmc.client_no = fc.client_no
    WHERE mmc.part_id <= '${batch_date}'
)
insert into table ${com_schema}.temp_t_mhbos_m_client_all
select *
FROM ${com_schema}.r_mhbos_m_client mmc
LEFT JOIN latest_clients lc
  ON mmc.client_no = lc.client_no
  AND lc.rn = 1
LEFT JOIN ${com_schema}.t_mhbos_m_client_ext mmce
  ON mmc.client_no = mmce.client_no
  AND mmce.part_id = '${batch_date}'

WHERE mmc.part_id = '${batch_date}'

;
