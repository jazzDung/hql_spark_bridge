SELECT ext_start_time,
       ext_end_time,
       to_char(current_date, 'yyyyMMdd')                    AS today_date,
       to_char(current_date - interval '1 day', 'yyyyMMdd') AS yesterday_date,
       CASE
           WHEN (length(ext_end_time) = 8 OR length(ext_end_time) = 8)
               AND coalesce(ext_end_time, to_char(current_date, 'yyyyMMdd')) <= to_char(current_date, 'yyyyMMdd')
               AND ext_start_time <= to_char(current_date, 'yyyyMMdd')
               AND coalesce(ext_end_time, to_char(current_date, 'yyyyMMdd')) >= ext_start_time
               THEN 1
           WHEN (length(ext_end_time) > 8 OR length(ext_end_time) > 8)
               AND coalesce(ext_end_time::timestamp, now()) <= now()
               AND ext_start_time::timestamp <= now()
                  AND coalesce(ext_end_time::timestamp, now()) >= ext_start_time::timestamp
                  THEN 1
                ELSE 0
END
AS run_flag
 FROM public.c_etl_run
 WHERE source_name = 'mhbos'
   and table_name = 'm_client'
     LIMIT 1
;


UPDATE ctr.c_etl_run
SET
    last_etl_start_time = '2025-01-01T00:00:00',
    last_etl_end_time = '2025-01-01T00:05:00',
    last_ext_start_time = '2025-01-01T00:00:00',
    last_ext_end_time = '2025-01-02T00:00:00',
    ext_start_time = NULL,
    ext_end_time = NULL
WHERE source_name = 'mhbos'
 AND table_name = 'm_client';



UPDATE public.c_etl_run
SET
    last_etl_start_time = '2025-01-01T00:00:00',
    last_etl_end_time = '2025-01-01T00:05:00',
    last_ext_start_time = '2025-01-01T00:00:00',
    last_ext_end_time = '2025-01-02T00:00:00',
    ext_start_time = NULL,
    ext_end_time = NULL
WHERE source_name = 'cur'
 AND table_name = 'dim_contact_mhbos';

INSERT INTO public.c_etl_run (source_name, table_name, last_etl_start_time, last_etl_end_time, last_ext_start_time,
                              last_ext_end_time, ext_start_time)
VALUES ('cur', 'dim_contact_mhbos', '2026-03-25 10:26:46.213', '2026-03-25 10:29:31.985', '20260326', '20260326', '20260327');
