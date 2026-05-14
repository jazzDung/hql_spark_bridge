UPDATE public.c_etl_run
SET
    last_etl_start_time = '2026-01-01T00:00:00',
    last_etl_end_time = '2026-01-01T00:05:00',
    last_ext_start_time = '2026-01-01T00:00:00',
    last_ext_end_time = '2026-01-02T00:00:00',
    ext_start_time = '2026-01-02T00:00:00',
    ext_end_time = '2026-04-28T00:00:00'
WHERE source_name = 'cur'
 AND table_name = 'dim_contact_mhbos';




INSERT INTO public.c_etl_run (
    source_name,
    table_name,
    last_etl_start_time,
    last_etl_end_time,
    last_ext_start_time,
    last_ext_end_time,
    ext_start_time,
    ext_end_time
)
VALUES (
    'mhbos',
    'm_client_crs',
    '2026-03-18 15:57:19.062',                -- set when flow starts
    '2026-04-29 15:57:19.062',                -- set when flow ends
    NULL,  -- e.g., yyyyMMdd or timestamp 
    NULL,    -- optional per logic
    '2026-04-29',  -- used at run start
    NULL     -- used at run start
);



INSERT INTO ctr.c_etl_run (
    source_name,
    table_name,
    last_etl_start_time,
    last_etl_end_time,
    last_ext_start_time,
    last_ext_end_time,
    ext_start_time,
    ext_end_time
)
VALUES (
    'mhbos',
    'm_client_crs',
    '2026-03-18 15:57:19.062',                -- set when flow starts
    '2026-04-29 15:57:19.062',                -- set when flow ends
    NULL,  -- e.g., yyyyMMdd or timestamp
    NULL,    -- optional per logic
    '2026-04-29',  -- used at run start
    NULL     -- used at run start
);



UPDATE ctr.c_etl_run
SET
    last_etl_start_time = '2026-01-01T00:00:00',
    last_etl_end_time = '2026-01-01T00:05:00',
    last_ext_start_time = '2026-01-01T00:00:00',
    last_ext_end_time = '2026-01-02T00:00:00',
    ext_start_time = '2026-01-02T00:00:00',
    ext_end_time = '2026-04-28T00:00:00'
WHERE source_name = 'mhbos'
 AND table_name = 'm_client_crs';

