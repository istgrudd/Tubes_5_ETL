-- 00_staging.sql : landing zone (raw) untuk SEMUA KPI (K1–K5).
-- Setiap baris Excel disimpan apa adanya (jsonb) + jejak file/sheet/baris/hash.
-- Tabel ini dipakai bersama; tiap KPI hanya menghapus/mengisi baris miliknya (kolom kpi).
CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE IF NOT EXISTS stg.raw_baris (
  kpi            varchar(5)   NOT NULL,          -- 'k1'..'k5'
  nama_file      varchar(200) NOT NULL,
  nama_sheet     varchar(100) NOT NULL,
  nomor_baris    integer      NOT NULL,          -- nomor baris di Excel (header = 1)
  hash_baris     varchar(64)  NOT NULL,          -- sha256 nilai mentah
  payload_asli   jsonb        NOT NULL,
  batch_id       varchar(64)  NOT NULL,
  waktu_ekstrak  timestamp    NOT NULL,
  PRIMARY KEY (kpi, nama_file, nama_sheet, nomor_baris)
);
CREATE TABLE IF NOT EXISTS stg.etl_log (
  batch_id       varchar(64) PRIMARY KEY,
  kpi            varchar(5)  NOT NULL,
  mulai          timestamp   NOT NULL,
  selesai        timestamp,
  status         varchar(20) NOT NULL,           -- BERJALAN / SUKSES / GAGAL
  ringkasan      jsonb
);
