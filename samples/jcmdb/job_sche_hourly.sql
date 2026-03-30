create table job_sche_hourly
(
    src_name       varchar(10),
    table_name     varchar(100),
    batch_dt       varchar(20),
    ext_start_time varchar(20),
    prog_end_time  varchar(20),
    ext_status     varchar(10)
);

alter table job_sche_hourly
    owner to postgres;

INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_companytype', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:23:37', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_trust_acc', '20251202', '2025-12-03T10:00:00', '2025-12-03 10:58:11', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('k2', 'cif_account', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:06:12', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'tmp_bfe614', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:10:51', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_bdt_dt', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:10:52', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_rec_hd', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:11:19', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_rec_dtb', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:11:46', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'm_client_thirdparty', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:12:19', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'e_trust_withdrawal', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:12:26', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'tmp_bfe613', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:12:37', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('smf', 'tbl_einvoicing_transactiondata_2', '20240817', '2024-08-17T08:00:00', '2024-08-17 08:00:55', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_glled_hd', '', '2024-08-15T08:00:00', '', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_glled_dt', '', '2024-08-15T08:00:00', '', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'm_mcd_client_addr', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:00:34', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'm_client_ext', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:02:53', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('k2', 'account', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:06:09', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('k2', 'cif_ext', '20241017', '2024-10-17T15:00:00', '2024-10-17 15:06:09', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('k2', 'rule_value', '20251023', '2025-10-23T10:00:00', '2025-10-23 10:40:29', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('smf', 'tbl_einvoicing_clientdata', '20251209', '2025-12-09T10:00:00', '2025-12-09 10:49:33', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_ledger_dt', '20251222', '2025-12-22T10:00:00', '2025-12-22 10:12:37', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_bdt_hd', '20250201', '2025-03-19T12:00:00', '2025-03-19 12:02:56', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_title', '20251201', '2025-12-26T15:00:00', '2025-12-26 15:34:32', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'm_client', '20251201', '2026-01-02T15:00:00', '2026-01-02 15:24:19', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('smfkibb', 'tbl_V3_Cache_AccountMarginPosition', '20260107', '2026-01-07T00:00:00', null, '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('k2', 'cif_alias', '20260210', '2026-02-10T15:00:00', '2026-02-10 15:45:42', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'e_epayment_log', '20250409', '2025-04-10T17:00:00', '2025-04-10 17:47:41', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('lms', 'tbl_einvoicing_clientdata', '20250416', '2025-04-16T11:00:00', '2025-04-16 11:48:54', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('smf', 'tbl_einvoicing_transactiondata', '20240301', '2025-03-11T13:00:00', '', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_ledger_hd', '20250418', '2025-04-18T11:00:00', '2025-04-18 11:34:03', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_rec_dta', '20250227', '2025-02-27T00:00:00', null, '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('lms', 'tbl_einvoicing_transactiondata', '20250505', '2025-05-05T17:00:00', '2025-05-05 17:00:31', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sbl', 'tbl_einvoicing_transactiondata', '20250505', '2025-05-05T17:00:00', '2025-05-05 17:01:05', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 'a_m_client_thirdparty', '20250917', '2025-09-17T09:00:00', '2025-09-17 09:29:10', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sbl', 'tbl_einvoicing_clientdata', '20250930', '2025-09-30T14:00:00', '2025-09-30 14:08:44', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('mhbos', 't_contract', '20250930', '2025-10-06T15:00:00', '2025-10-06 15:50:12', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_counterparty', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:21:33', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_account', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:21:47', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_accountstatus', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:22:01', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_counterpartyclass', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:22:14', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_country', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:22:38', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_bumistatus', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:22:51', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_race', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:23:03', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_maritalstatus', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:23:15', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_typeofbusiness', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:23:26', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_residencystatus', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:23:49', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_employmentsector', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:24:01', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_employmentstatustype', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:24:16', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_occupation', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:24:29', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_investmentprofilingestimatednetworth', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:24:40', '1');
INSERT INTO public.job_sche_hourly (src_name, table_name, batch_dt, ext_start_time, prog_end_time, ext_status) VALUES ('sblkibb', 'tbl_countrystate', '20251017', '2025-10-26T16:00:00', '2025-10-26 16:24:51', '1');
