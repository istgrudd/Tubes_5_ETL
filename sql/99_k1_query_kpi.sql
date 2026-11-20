-- 99_k1_query_kpi.sql : contoh query K1A–K1D (dari Tubes_4). Parameter :semester_key, :tanggal_acuan, :kode_peran
-- Jalankan di psql dengan \set, mis. \set tanggal_acuan '''2026-11-16'''
SET search_path TO k1;
-- =====================================================================
-- Aturan ETL peran utama (dijalankan pada staging sebelum load fact)
-- Jika satu (surat_id, kode_dosen) punya >1 peran: ambil peran dengan
-- urutan_prioritas terkecil; tanggal diambil dari baris dengan interval
-- valid; baris lain dicatat di audit dengan keputusan 'DIGABUNG'.
-- Contoh pola (stg_rincian = hasil staging yang sudah dibakukan):
--   SELECT DISTINCT ON (surat_id, kode_dosen) ...
--   FROM stg_rincian s JOIN dim_peran p ON p.kode_peran = s.kode_peran
--   ORDER BY surat_id, kode_dosen, p.urutan_prioritas;
-- =====================================================================

-- =====================================================================
-- Contoh query KPI. Parameter: :semester_key, :tanggal_acuan, :kode_peran
-- Semua query memfilter kode_status = 'TERCATAT' (pengganti lolos_kpi).
-- =====================================================================

-- K1A  Keterlibatan dan surat unik per dosen pada semester
WITH periode AS (
  SELECT tanggal_mulai AS awal, tanggal_akhir AS akhir
  FROM dim_semester WHERE semester_key = :semester_key
), terpilih AS (
  SELECT f.* FROM fact_penugasan_surat f
  JOIN dim_status_administrasi a USING (status_administrasi_key)
  JOIN dim_tanggal tm ON tm.tanggal_key = f.tgl_mulai_key
  JOIN dim_tanggal ts ON ts.tanggal_key = f.tgl_selesai_key
  CROSS JOIN periode p
  WHERE a.kode_status = 'TERCATAT' AND f.tanggal_valid
    AND tm.tanggal <= p.akhir AND ts.tanggal >= p.awal
)
SELECT d.kode_dosen, d.nama_dosen,
  COUNT(f.penugasan_key)                                AS keterlibatan,
  COUNT(DISTINCT f.surat_id)                            AS surat_unik,
  COUNT(f.penugasan_key) FILTER (WHERE j.kode_jenis='SK') AS jumlah_sk,
  COUNT(f.penugasan_key) FILTER (WHERE j.kode_jenis='ST') AS jumlah_st
FROM dim_dosen d
LEFT JOIN terpilih f ON f.dosen_key = d.dosen_key
LEFT JOIN dim_jenis_surat j ON j.jenis_surat_key = f.jenis_surat_key
WHERE d.cakupan_populasi
GROUP BY d.dosen_key, d.kode_dosen, d.nama_dosen
ORDER BY keterlibatan DESC, d.kode_dosen;

-- K1B  Status masa tugas per dosen pada tanggal acuan
WITH s AS (
  SELECT f.dosen_key, f.penugasan_key,
    CASE WHEN NOT f.tanggal_valid THEN 'TIDAK_DAPAT_DITENTUKAN'
         WHEN CAST(:tanggal_acuan AS date) < tm.tanggal THEN 'BELUM_MULAI'
         WHEN CAST(:tanggal_acuan AS date) > ts.tanggal THEN 'BERAKHIR'
         ELSE 'AKTIF' END AS status_masa_tugas
  FROM fact_penugasan_surat f
  JOIN dim_status_administrasi a USING (status_administrasi_key)
  JOIN dim_tanggal tm ON tm.tanggal_key = f.tgl_mulai_key
  JOIN dim_tanggal ts ON ts.tanggal_key = f.tgl_selesai_key
  WHERE a.kode_status = 'TERCATAT'
)
SELECT d.kode_dosen, d.nama_dosen,
  COUNT(*) FILTER (WHERE s.status_masa_tugas='AKTIF')       AS aktif,
  COUNT(*) FILTER (WHERE s.status_masa_tugas='BERAKHIR')    AS berakhir,
  COUNT(*) FILTER (WHERE s.status_masa_tugas='BELUM_MULAI') AS belum_mulai,
  COUNT(*) FILTER (WHERE s.status_masa_tugas='TIDAK_DAPAT_DITENTUKAN') AS tak_diketahui
FROM dim_dosen d LEFT JOIN s ON s.dosen_key = d.dosen_key
WHERE d.cakupan_populasi
GROUP BY d.dosen_key, d.kode_dosen, d.nama_dosen
ORDER BY aktif DESC, d.kode_dosen;

-- K1C  Simulasi poin kontribusi (bobot langsung dari peran utama)
SELECT d.kode_dosen, d.nama_dosen, b.skema_id, b.status_skema,
  COUNT(f.penugasan_key)                          AS penugasan_berakhir,
  COUNT(f.penugasan_key) FILTER (WHERE b.bobot IS NULL) AS belum_berbobot,
  SUM(b.bobot)                                    AS poin_simulasi
FROM fact_penugasan_surat f
JOIN dim_status_administrasi a USING (status_administrasi_key)
JOIN dim_dosen d USING (dosen_key)
JOIN dim_tanggal ts ON ts.tanggal_key = f.tgl_selesai_key
LEFT JOIN cfg_bobot_peran b ON b.peran_key = f.peran_key AND b.skema_id = 'SIM_V1'
WHERE a.kode_status = 'TERCATAT' AND f.tanggal_valid
  AND ts.tanggal < CAST(:tanggal_acuan AS date)
GROUP BY d.kode_dosen, d.nama_dosen, b.skema_id, b.status_skema;

-- K1D  Sebaran penugasan per jenis kegiatan dan peran (usulan kelompok)
SELECT k.nama_kategori, r.nama_peran,
  COUNT(f.penugasan_key)     AS keterlibatan,
  COUNT(DISTINCT f.surat_id) AS surat_unik
FROM fact_penugasan_surat f
JOIN dim_status_administrasi a USING (status_administrasi_key)
JOIN dim_jenis_kegiatan k USING (jenis_kegiatan_key)
JOIN dim_peran r USING (peran_key)
WHERE a.kode_status = 'TERCATAT'
GROUP BY k.nama_kategori, r.nama_peran, r.urutan_prioritas
ORDER BY k.nama_kategori, r.urutan_prioritas;

-- Filter satu peran (dulu butuh EXISTS ke bridge, kini join biasa)
SELECT COUNT(*) AS keterlibatan
FROM fact_penugasan_surat f
JOIN dim_status_administrasi a USING (status_administrasi_key)
JOIN dim_peran r USING (peran_key)
WHERE a.kode_status = 'TERCATAT' AND r.kode_peran = :kode_peran;

-- Kontrol cakupan: semua status tetap terlihat (BATAL tidak dihapus)
SELECT a.kode_status, COUNT(*) AS jumlah_baris_fact
FROM fact_penugasan_surat f
JOIN dim_status_administrasi a USING (status_administrasi_key)
GROUP BY a.kode_status ORDER BY a.kode_status;
