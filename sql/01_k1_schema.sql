-- 01_k1_schema.sql
-- DDL + isi dimensi referensi K1. Diambil APA ADANYA dari Tubes_4/Rancangan_K1_PostgreSQL_Revisi.sql
-- (revisi 2 Nov 2026). Dijalankan oleh: python -m etl.run --kpi k1 --init-db
-- PERINGATAN: --init-db menghapus schema k1 lalu membuat ulang.
DROP SCHEMA IF EXISTS k1 CASCADE;
-- =====================================================================
-- Rancangan fisik K1 (Penugasan Dosen melalui SK dan Surat Tugas)
-- Revisi 2 November 2026 berdasarkan masukan dosen pengampu:
--   1. bridge_penugasan_peran dihapus; peran_key menjadi FK langsung
--      di fact_penugasan_surat ke dim_peran.
--   2. Kolom lolos_kpi pada dim_status_administrasi di-drop. Kelayakan
--      dibaca langsung dari kode_status (hanya TERCATAT yang dihitung).
--      Catatan BATAL dan PERLU_REVIEW tetap disimpan di fact;
--      DUPLIKAT disimpan di audit_baris_sumber dan menunjuk fakta kanonik.
-- Target: PostgreSQL 16+ (prototype sebelum migrasi ke BigQuery).
-- Nama tabel mengikuti laporan Tubes 3 (fact_penugasan_surat, dst).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS k1;
SET search_path TO k1;

-- ---------- Dimensi ----------
CREATE TABLE dim_dosen (
  dosen_key          integer PRIMARY KEY,
  kode_dosen         varchar(20)  NOT NULL UNIQUE,
  nama_dosen         varchar(200) NOT NULL,
  prodi_asal         varchar(150),
  kelompok_keahlian  varchar(150),
  fakultas_asal      varchar(150),
  cakupan_populasi   boolean NOT NULL DEFAULT false
);

CREATE TABLE dim_jenis_surat (
  jenis_surat_key  smallint PRIMARY KEY,
  kode_jenis       varchar(10) NOT NULL UNIQUE,
  nama_jenis       varchar(60) NOT NULL
);

CREATE TABLE dim_jenis_kegiatan (
  jenis_kegiatan_key  smallint PRIMARY KEY,
  kode_kegiatan       varchar(40)  NOT NULL UNIQUE,
  nama_kategori       varchar(100) NOT NULL,
  deskripsi           text,
  aturan_versi        varchar(40)
);

-- Peran kini terhubung langsung ke fakta (tanpa bridge).
-- urutan_prioritas dipakai ETL hanya bila satu dosen tercatat dengan
-- lebih dari satu peran pada surat yang sama (angka kecil = utama).
CREATE TABLE dim_peran (
  peran_key         smallint PRIMARY KEY,
  kode_peran        varchar(40) NOT NULL UNIQUE,
  nama_peran        varchar(80) NOT NULL,
  urutan_prioritas  smallint    NOT NULL
);

-- lolos_kpi dihapus. Kelayakan hitung = kode_status 'TERCATAT'.
CREATE TABLE dim_status_administrasi (
  status_administrasi_key  smallint PRIMARY KEY,
  kode_status              varchar(30) NOT NULL UNIQUE,
  nama_status              varchar(60) NOT NULL,
  keterangan               text        NOT NULL
);

CREATE TABLE dim_unit (
  unit_key          integer PRIMARY KEY,
  kode_unit         varchar(40)  NOT NULL UNIQUE,
  nama_unit         varchar(150) NOT NULL,
  jenis_unit        varchar(30)  NOT NULL,
  nama_prodi        varchar(150),
  nama_fakultas     varchar(150),
  nama_universitas  varchar(150)
);

CREATE TABLE dim_mitra (
  mitra_key    integer PRIMARY KEY,
  mitra_id     varchar(64)  NOT NULL UNIQUE,
  nama_mitra   varchar(200) NOT NULL,
  jenis_mitra  varchar(60)  NOT NULL,
  negara       varchar(100)
);

CREATE TABLE dim_tanggal (
  tanggal_key   integer PRIMARY KEY,       -- YYYYMMDD; 0 = belum diketahui
  tanggal       date UNIQUE,               -- NULL hanya untuk key 0
  tahun         smallint,
  triwulan      smallint,
  nomor_bulan   smallint,
  nama_bulan    varchar(15),
  nama_hari     varchar(15),
  tahun_bulan   varchar(7)
);

-- dim_semester bukan FK K1; dipakai sebagai parameter interval laporan.
CREATE TABLE dim_semester (
  semester_key     integer PRIMARY KEY,
  tahun_akademik   varchar(9)  NOT NULL,
  semester         varchar(10) NOT NULL,
  label_semester   varchar(25) NOT NULL UNIQUE,
  tanggal_mulai    date NOT NULL,
  tanggal_akhir    date NOT NULL,
  sumber_kalender  varchar(200) NOT NULL,
  CHECK (tanggal_mulai <= tanggal_akhir)
);

-- ---------- Fakta ----------
-- Grain tetap: satu dosen dalam satu surat dengan satu interval tugas
-- yang direkonsiliasi. Satu baris = satu peran utama.
CREATE TABLE fact_penugasan_surat (
  penugasan_key            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  surat_id                 varchar(150) NOT NULL,          -- DD
  nomor_surat              varchar(150) NOT NULL,          -- DD
  judul_kegiatan           text,
  dosen_key                integer  NOT NULL REFERENCES dim_dosen,
  jenis_surat_key          smallint NOT NULL REFERENCES dim_jenis_surat,
  jenis_kegiatan_key       smallint NOT NULL REFERENCES dim_jenis_kegiatan,
  peran_key                smallint NOT NULL REFERENCES dim_peran,   -- BARU
  unit_pengelola_key       integer  NOT NULL REFERENCES dim_unit,
  mitra_key                integer  NOT NULL REFERENCES dim_mitra,
  status_administrasi_key  smallint NOT NULL REFERENCES dim_status_administrasi,
  tgl_terbit_key           integer  NOT NULL REFERENCES dim_tanggal,
  tgl_mulai_key            integer  NOT NULL REFERENCES dim_tanggal,
  tgl_selesai_key          integer  NOT NULL REFERENCES dim_tanggal,
  status_sumber            varchar(60),
  tanggal_valid            boolean  NOT NULL,
  jumlah_penugasan         smallint NOT NULL DEFAULT 1 CHECK (jumlah_penugasan = 1),
  durasi_hari              integer  CHECK (durasi_hari IS NULL OR durasi_hari >= 1),
  sumber_utama             varchar(100) NOT NULL,
  batch_id                 varchar(64)  NOT NULL,
  waktu_ekstrak            timestamp    NOT NULL,
  aturan_versi             varchar(40)  NOT NULL,
  CONSTRAINT uq_grain_surat_dosen UNIQUE (surat_id, dosen_key),
  CONSTRAINT ck_durasi_valid CHECK (tanggal_valid OR durasi_hari IS NULL)
);
CREATE INDEX ix_fact_dosen  ON fact_penugasan_surat (dosen_key);
CREATE INDEX ix_fact_peran  ON fact_penugasan_surat (peran_key);
CREATE INDEX ix_fact_status ON fact_penugasan_surat (status_administrasi_key);

-- ---------- Audit (pendukung, bukan bagian star) ----------
-- Semua baris sumber disimpan. Duplikat dan peran sekunder tidak dibuang,
-- melainkan ditautkan ke fakta kanonik.
CREATE TABLE audit_baris_sumber (
  audit_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nama_file        varchar(200) NOT NULL,
  nama_sheet       varchar(100) NOT NULL,
  nomor_baris      integer      NOT NULL,
  hash_baris       varchar(64)  NOT NULL,
  payload_asli     jsonb        NOT NULL,
  penugasan_key    bigint REFERENCES fact_penugasan_surat,
  keputusan        varchar(30)  NOT NULL CHECK (keputusan IN
                   ('DIMUAT','DIGABUNG','DUPLIKAT','PERLU_REVIEW','DI_LUAR_CAKUPAN')),
  alasan           text,
  bukti_keputusan  text,
  batch_id         varchar(64)  NOT NULL,
  waktu_ekstrak    timestamp    NOT NULL,
  aturan_versi     varchar(40)  NOT NULL,
  UNIQUE (nama_file, nama_sheet, nomor_baris, hash_baris)
);

-- ---------- Konfigurasi K1C (opsional, simulasi) ----------
CREATE TABLE cfg_bobot_peran (
  skema_id     varchar(30)  NOT NULL,
  status_skema varchar(20)  NOT NULL CHECK (status_skema IN ('SIMULASI','DISETUJUI')),
  peran_key    smallint     NOT NULL REFERENCES dim_peran,
  bobot        numeric(6,2) NOT NULL CHECK (bobot >= 0),
  PRIMARY KEY (skema_id, peran_key)
);

-- ---------- Isi dimensi referensi ----------
INSERT INTO dim_jenis_surat VALUES
 (0,'UNKNOWN','Belum diketahui'), (1,'SK','Surat Keputusan'), (2,'ST','Surat Tugas');

INSERT INTO dim_jenis_kegiatan (jenis_kegiatan_key,kode_kegiatan,nama_kategori,aturan_versi) VALUES
 (0,'BELUM_DIKLASIFIKASIKAN','Belum diklasifikasikan','v1'),
 (1,'PENDIDIKAN','Pendidikan','v1'), (2,'PENELITIAN','Penelitian','v1'),
 (3,'PENGABDIAN','Pengabdian masyarakat','v1'),
 (4,'AKREDITASI_MUTU','Akreditasi dan mutu','v1'),
 (5,'TATA_KELOLA','Tata kelola','v1'),
 (6,'PENGEMBANGAN_KOMPETENSI','Pengembangan kompetensi','v1'),
 (7,'LAINNYA','Lainnya','v1');

-- Urutan prioritas = usulan kelompok (tanggung jawab tertinggi lebih dulu).
INSERT INTO dim_peran VALUES
 (0,'BELUM_DIKETAHUI','Belum diketahui',99),
 (1,'KETUA','Ketua',1),            (2,'WAKIL_KETUA','Wakil Ketua',2),
 (3,'PENGARAH','Pengarah',3),      (4,'PEMBINA','Pembina',4),
 (5,'PIC','PIC',5),                (6,'SEKRETARIS','Sekretaris',6),
 (7,'NARASUMBER','Narasumber',7),  (8,'PEMBIMBING','Pembimbing',8),
 (9,'PENGUJI','Penguji',9),        (10,'ASESOR','Asesor',10),
 (11,'REVIEWER','Reviewer',11),    (12,'JURI','Juri',12),
 (13,'PEWAWANCARA','Pewawancara',13), (14,'PENYUSUN','Penyusun',14),
 (15,'PELAKSANA','Pelaksana',15),  (16,'ANGGOTA','Anggota',16),
 (17,'PESERTA','Peserta',17),      (18,'LAINNYA','Lainnya',98);

INSERT INTO dim_status_administrasi VALUES
 (1,'TERCATAT','Tercatat','Identitas surat, dosen, dan jenis telah direkonsiliasi. Dihitung dalam KPI.'),
 (2,'BATAL','Batal','Pembatalan didukung bukti. Tetap disimpan di fact sebagai konteks, tidak dihitung.'),
 (3,'PERLU_REVIEW','Perlu review','Atribut material masih berkonflik. Tetap disimpan di fact, tidak dihitung sampai diputuskan.'),
 (4,'DUPLIKAT','Duplikat','Catatan ganda dari surat yang sama. Disimpan di audit_baris_sumber dan menunjuk fakta kanonik.');

INSERT INTO dim_unit VALUES
 (0,'UNKNOWN','Belum diketahui','UNKNOWN',NULL,NULL,NULL),
 (1,'FIF','Fakultas Informatika','FAKULTAS',NULL,'Fakultas Informatika','Telkom University');
INSERT INTO dim_mitra VALUES (0,'UNKNOWN','Tidak diketahui','BELUM_DIKETAHUI',NULL);

INSERT INTO dim_tanggal VALUES (0,NULL,NULL,NULL,NULL,'Unknown','Unknown',NULL);
INSERT INTO dim_tanggal
SELECT to_char(d,'YYYYMMDD')::int, d::date,
       extract(year FROM d), extract(quarter FROM d), extract(month FROM d),
       to_char(d,'TMMonth'), to_char(d,'TMDay'), to_char(d,'YYYY-MM')
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') g(d);

-- Bobot contoh untuk simulasi K1C; bukan ketentuan fakultas.
INSERT INTO cfg_bobot_peran VALUES
 ('SIM_V1','SIMULASI',1,3.00), ('SIM_V1','SIMULASI',7,2.00),
 ('SIM_V1','SIMULASI',11,2.00), ('SIM_V1','SIMULASI',16,1.00),
 ('SIM_V1','SIMULASI',17,0.50);

