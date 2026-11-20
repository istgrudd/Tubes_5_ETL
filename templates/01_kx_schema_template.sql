-- 01_kx_schema_template.sql : CONTOH schema untuk templates/kx_template.py (opsional, bukan bagian K1).
-- Copy ke sql/ jadi 01_k2_schema.sql / 01_k3_schema.sql / dst., ganti semua "kx." dengan schema KPI-mu,
-- lalu ganti tabel contoh dengan dimensi & fakta dari desain dimensional model KPI-mu.
-- PERINGATAN: --init-db menghapus schema ini lalu membuat ulang.
DROP SCHEMA IF EXISTS kx CASCADE;
CREATE SCHEMA kx;

-- Dimensi tanggal (boleh copy dari 01_k1_schema.sql). 0 = belum diketahui.
CREATE TABLE kx.dim_tanggal (
  tanggal_key  integer PRIMARY KEY,
  tanggal      date UNIQUE,
  tahun        smallint,
  nomor_bulan  smallint,
  tahun_bulan  varchar(7)
);
INSERT INTO kx.dim_tanggal VALUES (0, NULL, NULL, NULL, NULL);
INSERT INTO kx.dim_tanggal
SELECT to_char(d,'YYYYMMDD')::int, d::date, extract(year FROM d), extract(month FROM d), to_char(d,'YYYY-MM')
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') g(d);

-- Contoh dimensi. Key 0 = Unknown.
CREATE TABLE kx.dim_contoh (
  contoh_key   integer PRIMARY KEY,
  kode_contoh  varchar(80)  NOT NULL UNIQUE,     -- natural key
  nama_contoh  varchar(300) NOT NULL
);
INSERT INTO kx.dim_contoh VALUES (0, 'UNKNOWN', 'Tidak diketahui');

-- Contoh fakta. Grain: 1 baris sumber yang dimuat.
CREATE TABLE kx.fact_contoh (
  fakta_key      bigint   PRIMARY KEY,
  contoh_key     integer  NOT NULL REFERENCES kx.dim_contoh,
  tanggal_key    integer  NOT NULL REFERENCES kx.dim_tanggal,
  jumlah         smallint NOT NULL DEFAULT 1 CHECK (jumlah = 1),
  batch_id       varchar(64) NOT NULL,
  waktu_ekstrak  timestamp   NOT NULL,
  aturan_versi   varchar(40) NOT NULL
);

-- Audit: setiap baris sumber tercatat dengan keputusannya (pola sama dengan K1).
CREATE TABLE kx.audit_baris_sumber (
  audit_key      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nama_file      varchar(200) NOT NULL,
  nama_sheet     varchar(100) NOT NULL,
  nomor_baris    integer      NOT NULL,
  hash_baris     varchar(64)  NOT NULL,
  payload_asli   jsonb        NOT NULL,
  fakta_key      bigint REFERENCES kx.fact_contoh,
  keputusan      varchar(30)  NOT NULL CHECK (keputusan IN
                 ('DIMUAT','DIGABUNG','DUPLIKAT','PERLU_REVIEW','DI_LUAR_CAKUPAN')),
  alasan         text,
  batch_id       varchar(64)  NOT NULL,
  waktu_ekstrak  timestamp    NOT NULL,
  aturan_versi   varchar(40)  NOT NULL
);
