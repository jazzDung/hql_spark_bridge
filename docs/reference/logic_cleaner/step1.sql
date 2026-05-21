

WITH filtered_clients AS (
    SELECT DISTINCT client_no
    FROM ${raw_schema}.mhbos_m_client
    WHERE etl_dt = '${batch_date}'
    UNION
    SELECT DISTINCT client_no
    FROM ${raw_schema}.mhbos_m_client_ext
    WHERE etl_dt = '${batch_date}'
),
latest_clients AS (
    SELECT mmc.*
        ,ROW_NUMBER() OVER (PARTITION BY mmc.client_no ORDER BY mmc.etl_dt DESC) AS rn
    FROM ${raw_schema}.mhbos_m_client mmc
    INNER JOIN filtered_clients fc
        ON mmc.client_no = fc.client_no
    WHERE mmc.etl_dt <= '${batch_date}'
)
insert into table ${com_schema}.temp_t_mhbos_m_client_all
select *
FROM ${raw_schema}.mhbos_m_client mmc
LEFT JOIN latest_clients lc
  ON mmc.client_no = lc.client_no
  AND lc.rn = 1
LEFT JOIN ${com_schema}.t_mhbos_m_client_ext mmce
  ON mmc.client_no = mmce.client_no
  AND mmce.etl_dt = '${batch_date}'

WHERE mmc.etl_dt = '${batch_date}'

;
