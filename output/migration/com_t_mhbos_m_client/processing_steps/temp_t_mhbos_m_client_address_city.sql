DROP TABLE IF EXISTS ${com_schema}.temp_t_mhbos_m_client_address_city

/* 2.14 Create a temporary table temp_t_mhbos_m_client_address_city to store the cleaned address cityinformation. */
CREATE TABLE IF NOT EXISTS ${com_schema}.temp_t_mhbos_m_client_address_city (
  `client_no` STRING,
  `addr1` STRING,
  `addr2` STRING,
  `addr3` STRING,
  `addr4` STRING,
  `postcode` STRING,
  `mailing_addr` STRING,
  `city` STRING,
  `state` STRING,
  `perm_addr1` STRING,
  `perm_addr2` STRING,
  `perm_addr3` STRING,
  `perm_addr4` STRING,
  `perm_postcode` STRING,
  `registered_addr` STRING,
  `perm_city` STRING,
  `perm_state` STRING,
  `perm_country` STRING
)

TRUNCATE TABLE   ${com_schema}.temp_t_mhbos_m_client_address_city /* 2.1.1 ddl-insert-sundexin */

INSERT INTO ${com_schema}.temp_t_mhbos_m_client_address_city
SELECT
  addr.client_no,
  COALESCE(addr.addr1, addr.addr2, addr.addr3, addr.addr4, '') AS addr1,
  (
    CASE
      WHEN NOT addr.addr1 IS NULL
      THEN COALESCE(addr.addr2, addr.addr3, addr.addr4, '')
      WHEN addr.addr1 IS NULL AND NOT addr.addr2 IS NULL
      THEN COALESCE(addr.addr3, addr.addr4, '')
      WHEN addr.addr1 IS NULL AND addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
      THEN COALESCE(addr.addr4, '')
      ELSE ''
    END
  ) AS addr2,
  (
    CASE
      WHEN NOT addr.addr1 IS NULL AND NOT addr.addr2 IS NULL
      THEN COALESCE(addr.addr3, addr.addr4, '')
      WHEN addr.addr1 IS NULL AND NOT addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
      THEN COALESCE(addr.addr4, '')
      ELSE ''
    END
  ) AS addr3,
  (
    CASE
      WHEN NOT addr.addr1 IS NULL AND NOT addr.addr2 IS NULL AND NOT addr.addr3 IS NULL
      THEN COALESCE(addr.addr4, '')
      ELSE ''
    END
  ) AS addr4,
  addr.postcode,
  addr.mailing_addr,
  REGEXP_EXTRACT(
    UPPER(addr.mailing_addr),
    '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b'
  ) AS city,
  addr.state_code AS state, /*	   regexp_extract(upper(addr.mailing_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) as state, */ /* 20251013 */
  COALESCE(addr.perm_addr1, addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '') AS perm_addr1,
  (
    CASE
      WHEN NOT addr.perm_addr1 IS NULL
      THEN COALESCE(addr.perm_addr2, addr.perm_addr3, addr.perm_addr4, '')
      WHEN addr.perm_addr1 IS NULL AND NOT addr.perm_addr2 IS NULL
      THEN COALESCE(addr.perm_addr3, addr.perm_addr4, '')
      WHEN addr.perm_addr1 IS NULL
      AND addr.perm_addr2 IS NULL
      AND NOT addr.perm_addr3 IS NULL
      THEN COALESCE(addr.perm_addr4, '')
      ELSE ''
    END
  ) AS perm_addr2,
  (
    CASE
      WHEN NOT addr.perm_addr1 IS NULL AND NOT addr.perm_addr2 IS NULL
      THEN COALESCE(addr.perm_addr3, addr.perm_addr4, '')
      WHEN addr.perm_addr1 IS NULL
      AND NOT addr.perm_addr2 IS NULL
      AND NOT addr.perm_addr3 IS NULL
      THEN COALESCE(addr.perm_addr4, '')
      ELSE ''
    END
  ) AS perm_addr3,
  (
    CASE
      WHEN NOT addr.perm_addr1 IS NULL
      AND NOT addr.perm_addr2 IS NULL
      AND NOT addr.perm_addr3 IS NULL
      THEN COALESCE(addr.perm_addr4, '')
      ELSE ''
    END
  ) AS perm_addr4,
  addr.perm_postcode,
  addr.registered_addr,
  CASE
    WHEN COALESCE(TRIM(addr.perm_city), '') <> ''
    THEN addr.perm_city
    ELSE REGEXP_EXTRACT(
      UPPER(addr.registered_addr),
      '\\b(?:.*\\b)((BATU PAHAT)|(JOHOR BAHRU)|(KLUANG)|(KOTA TINGGI)|(MERSING)|(MUAR)|(PONTIAN)|(SEGAMAT)|(LEDANG)|(KULAI)|(TANGKAK)|(BALING)|(SERDANG)|(ALOR SETAR)|(SUNGAI PETANI)|(JITRA)|(KULIM)|(KUAH)|(KUALA NERANG)|(PENDANG)|(POKOK SENA)|(SIK)|(YAM)|(BACHOK)|(GUA MUSANG)|(JELI)|(KOTA BHARU)|(KUALA KRAI)|(MACHANG)|(PASIR MAS)|(PASIR PUTEH)|(TANAH MERAH)|(TUMPAT)|(ALOR GAJAH)|(JASIN)|(AYER KEROH)|(KUALA KLAWANG)|(BANDAR SERI JEMPOL)|(KUALA PILAH)|(PORT DICKSON)|(REMBAU)|(SEREMBAN)|(TAMPIN)|(BENTONG)|(BANDAR BERA)|(TANAH RATA)|(JERANTUT)|(KUANTAN)|(KUALA LIPIS)|(MARAN)|(PEKAN)|(RAUB)|(KUALA ROMPIN)|(TEMERLOH)|(BUKIT MERTAJAM)|(KEPALA BATAS)|(GEORGE TOWN)|(SUNGAI JAWI)|(BALIK PULAU)|(TAPAH)|(TELUK INTAN)|(GERIK)|(KAMPAR)|(PARIT BUNTAR)|(BATU GAJAH)|(KUALA KANGSAR)|(SERI MANJUNG)|(TAIPING)|(SERI ISKANDAR)|(BAGAN DATUK)|(BEAUFORT)|(BELURAN)|(KENINGAU)|(KOTA KINABATANGAN)|(KOTA BELUD)|(KOTA KINABALU)|(KOTA MARUDU)|(KUALA PENYU)|(KUDAT)|(KUNAK)|(LAHAD DATU)|(NABAWAN)|(PAPAR)|(DONGGONGON)|(PITAS)|(PUTATAN)|(RANAU)|(SANDAKAN)|(SEMPORNA)|(SIPITANG)|(TAMBUNAN)|(TAWAU)|(TELUPID)|(TENOM)|(TONGOD)|(TUARAN)|(ASAJAYA)|(BAU)|(BELAGA)|(BELUGU)|(BETONG)|(BINTULU)|(DALAT)|(MATU)|(JULAU)|(KABONG)|(KANOWIT)|(KAPIT)|(KUCHING)|(LAWAS)|(LIMBANG)|(LUBOK ANTU)|(LUNDU)|(MARUDI)|(MATU)|(BINTANGOR)|(MIRI)|(MUKAH)|(PAKAN)|(PUSA)|(KOTA SAMAHARAN)|(SARATOK)|(SARIKEI)|(SEBAUH)|(SELANGAU)|(SERIAN)|(SIBU)|(SIMUNJAN)|(SONG)|(SIMANGGANG)|(SUBIS)|(BELAWAI)|(TATAU)|(TEBEDU)|(LONG LAMA)|(BANDAR BARU SELAYANG)|(BANDAR BARU BANGI)|(KUALA KUBU BAHRU)|(KLANG)|(TELUK DATOK)|(KUALA SELANGOR)|(SUBANG)|(SABAK)|(SALAK TINGGI)|(KAMPUNG RAJA)|(KUALA DUNGUN)|(KUALA BERANG)|(CHUKAI)|(KUALA NERUS)|(KUALA TERENGGANU)|(MARANG)|(BANDAR PERMAISURI)|(KUALA LUMPUR)|(PUTRAJAYA)|(LABUAN))\\b'
    )
  END AS perm_city, /* 20250715 */
  addr.perm_state_code AS perm_state, /*	     case when nvl(trim(addr.perm_state_code),'') <> '' then addr.perm_state_code else regexp_extract(upper(addr.registered_addr), '\\b(?:.*\\b)((JOHOR)|(KEDAH)|(KELANTAN)|(MELAKA)|(MALACCA)|(NEGERI SEMBILAN)|(PAHANG)|(PENANG)|(PERAK)|(SABAH)|(SARAWAK)|(SELANGOR)|(TERENGGANU)|(WILAYAH PERSEKUTUAN)|( W\\.*P\\.* )|( W\\.*P\.*$)|(^W\\.*P\\.* ))\\b', 1) end as perm_state, -- 20250715 */ /* 20251013 */
  addr.perm_country /* 20250715 */
FROM (
  SELECT
    ca.client_no,
    (
      CASE WHEN COALESCE(TRIM(ca.addr1), '') = '' THEN NULL ELSE ca.addr1 END
    ) AS addr1,
    (
      CASE WHEN COALESCE(TRIM(ca.addr2), '') = '' THEN NULL ELSE ca.addr2 END
    ) AS addr2,
    (
      CASE WHEN COALESCE(TRIM(ca.addr3), '') = '' THEN NULL ELSE ca.addr3 END
    ) AS addr3,
    (
      CASE WHEN COALESCE(TRIM(ca.addr4), '') = '' THEN NULL ELSE ca.addr4 END
    ) AS addr4,
    ca.postcode,
    (
      COALESCE(ca.addr1, '') || (
        CASE WHEN COALESCE(ca.addr2, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr2, '') END
      ) || (
        CASE WHEN COALESCE(ca.addr3, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr3, '') END
      ) || (
        CASE WHEN COALESCE(ca.addr4, '') = '' THEN '' ELSE ' ' || COALESCE(ca.addr4, '') END
      )
    ) AS mailing_addr,
    ca.state_code,
    (
      CASE WHEN COALESCE(TRIM(ca.perm_addr1), '') = '' THEN NULL ELSE ca.perm_addr1 END
    ) AS perm_addr1,
    (
      CASE WHEN COALESCE(TRIM(ca.perm_addr2), '') = '' THEN NULL ELSE ca.perm_addr2 END
    ) AS perm_addr2,
    (
      CASE WHEN COALESCE(TRIM(ca.perm_addr3), '') = '' THEN NULL ELSE ca.perm_addr3 END
    ) AS perm_addr3,
    (
      CASE WHEN COALESCE(TRIM(ca.perm_addr4), '') = '' THEN NULL ELSE ca.perm_addr4 END
    ) AS perm_addr4,
    ca.perm_postcode,
    (
      COALESCE(ca.perm_addr1, '') || (
        CASE
          WHEN COALESCE(ca.perm_addr2, '') = ''
          THEN ''
          ELSE ' ' || COALESCE(ca.perm_addr2, '')
        END
      ) || (
        CASE
          WHEN COALESCE(ca.perm_addr3, '') = ''
          THEN ''
          ELSE ' ' || COALESCE(ca.perm_addr3, '')
        END
      ) || (
        CASE
          WHEN COALESCE(ca.perm_addr4, '') = ''
          THEN ''
          ELSE ' ' || COALESCE(ca.perm_addr4, '')
        END
      )
    ) AS registered_addr,
    ca.perm_city,
    ca.perm_state_code,
    ca.perm_country
  FROM ${com_schema}.temp_t_mhbos_m_client_all AS ca
) AS addr