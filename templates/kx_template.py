"""
kx_template.py — TEMPLATE OPSIONAL (acuan pola) untuk PIC K2–K5.
Repo ini khusus ETL K1. File ini TIDAK dijalankan oleh pipeline K1; isinya hanya contoh pola
yang sama dengan etl/k1_sk_st.py, disimpan di folder templates/ supaya terpisah dari kode K1.

Cara pakai:
    1. Copy file ini ke folder etl/, mis. jadi etl/k3_kerjasama.py
       (harus di dalam etl/ karena memakai `from . import common as c`).
    2. Copy templates/01_kx_schema_template.sql jadi sql/01_k3_schema.sql, lalu ubah tabelnya
       sesuai desain dimensional model KPI-mu (Tubes 3/4). Ganti semua "kx." menjadi "k3.".
    3. Isi bagian bertanda TODO di file ini.
    4. Daftarkan di etl/run.py bagian MODULES, mis. "k3": ("etl.k3_kerjasama", "01_k3_schema.sql").
    5. Jalankan:  python -m etl.run --kpi k3 --init-db    (pertama kali)
                  python -m etl.run --kpi k3              (run berikutnya)
    6. Cek output/k3/laporan_validasi_k3.md dan CSV-nya.

Isi contoh: membaca "Kunjungan ke FIF.xlsx" dan membuat 1 dimensi + 1 fakta sederhana.
Untuk mencobanya apa adanya: copy ke etl/kx_template.py, copy SQL-nya ke sql/, lalu daftarkan
di MODULES: "kx": ("etl.kx_template", "01_kx_schema_template.sql").

Kontrak yang harus dipenuhi modul KPI (dipanggil oleh run.py):
    extract(ds_dir)                 -> dict {nama: DataFrame}; boleh ada "_staging_lain": [DataFrame,...]
    transform(raw, batch)           -> (tables, info)
                                       tables = {nama_tabel: DataFrame}, kolom = kolom tabel di SQL
                                       tables WAJIB punya "audit_baris_sumber"
                                       info   = {"catatan": [...], "staging": DataFrame dengan kolom _row & catatan}
    load(conn, tables)              -> isi database (full refresh, jangan commit; run.py yang commit)
    validate(conn, tables, info, raw) -> list (nama_cek, lulus: bool, detail)
    ringkasan_sql(conn)             -> dict {judul: DataFrame} angka contoh metrik

Aturan wajib (sama dengan K1):
    - Jangan hapus baris bermasalah. Setiap baris sumber masuk audit dengan keputusan + alasan
      (DIMUAT / DUPLIKAT / PERLU_REVIEW / DI_LUAR_CAKUPAN).
    - Tanggal tidak ditebak: pakai c.parse_date(); yang tidak jelas -> key 0.
    - Kamus/alias taruh di config/kX/*.csv, jangan di-hardcode panjang di Python.
    - Load harus bisa diulang dengan hasil sama (TRUNCATE/DELETE lalu INSERT).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import common as c

# =============================================================================
# KONSTANTA — TODO: ganti sesuai KPI-mu
# =============================================================================
KPI = "kx"                              # TODO: "k2" / "k3" / "k4" / "k5" (juga jadi nama schema SQL)
ATURAN_VERSI = f"{KPI}-etl-v1.0"        # versi aturan transformasi, dicatat di fact & audit
CFG = c.CONFIG_DIR / KPI                # folder kamus KPI ini: config/kX/
FILE_UTAMA = "Kunjungan ke FIF.xlsx"    # TODO: workbook sumber fakta, mis. "Kerjasama FIF.xlsx"
SHEET = "Kunjungan"                     # TODO: nama sheet sumber


# =============================================================================
# 1. EXTRACT — baca Excel apa adanya
# =============================================================================
def extract(ds_dir: Path) -> dict[str, pd.DataFrame]:
    """Baca sumber data KPI ini.

    c.read_excel_raw() otomatis menambah kolom jejak _file, _sheet, _row, _hash, _payload.
    Nama key bebas ("utama" di sini). Kalau KPI-mu punya >1 file (mis. K3: Kerjasama FIF + rekap),
    tambahkan key lain, mis. "rekap": c.read_excel_raw(...). Semua DataFrame di dict ini otomatis
    disimpan ke stg.raw_baris oleh run.py.
    """
    return {
        "utama": c.read_excel_raw(ds_dir / FILE_UTAMA, SHEET),
        "_staging_lain": [],            # opsional: sheet lain yang hanya disimpan di staging
    }


# =============================================================================
# 2. TRANSFORM — semua aturan bisnis KPI di sini
# =============================================================================
def transform(raw: dict, batch: dict) -> tuple[dict[str, pd.DataFrame], dict]:
    """Ubah data mentah menjadi tabel dimensi, fakta, dan audit.

    Contoh di bawah membuat:
        dim_contoh  : daftar kegiatan unik (contoh dimensi)
        fact_contoh : 1 baris = 1 baris sumber yang tanggalnya terbaca (contoh fakta)
        audit_baris_sumber : keputusan untuk setiap baris sumber
    TODO: ganti dengan dimensi & fakta sesuai desain KPI-mu.
    """
    src = raw["utama"]

    # --- 2a. Bakukan kolom sumber ke tabel staging s (1 baris s = 1 baris Excel) ---
    s = pd.DataFrame({"_i": src.index, "_row": src["_row"]})
    s["nama"] = src["Kegiatan"].map(c.clean_text)                       # TODO: kolom teks utama
    s[["tanggal", "flag_tanggal"]] = src["Tanggal"].map(c.parse_date).tolist()   # TODO: kolom tanggal
    s["nama"] = s["nama"].astype(object).where(s["nama"].notna(), None)
    s["catatan"] = [[] for _ in range(len(s))]                          # catatan info per baris
    for i, flag in zip(s.index, s["flag_tanggal"]):
        if flag not in ("OK", "KOSONG"):
            s.at[i, "catatan"].append(f"tanggal {flag.lower()}")

    # --- 2b. Dimensi: surrogate key mulai 1 (key 0 = Unknown sudah di-seed di SQL) ---
    nama_unik = sorted(n for n in s["nama"].dropna().unique())
    dim_contoh = pd.DataFrame({
        "contoh_key": range(1, len(nama_unik) + 1),
        "kode_contoh": [c.slug(n) for n in nama_unik],
        "nama_contoh": nama_unik,
    }).drop_duplicates("kode_contoh")                                    # natural key wajib unik
    key_by_kode = dict(zip(dim_contoh.kode_contoh, dim_contoh.contoh_key))

    # --- 2c. Fakta + keputusan audit untuk setiap baris sumber ---
    fact_rows, audit_rows = [], []
    for i, r in s.iterrows():
        if r["nama"] is None:
            # TODO: aturan kapan baris TIDAK dimuat. Jangan dihapus: catat di audit.
            audit_rows.append({"_i": i, "fakta_key": None, "keputusan": "PERLU_REVIEW", "alasan": "nama kosong"})
            continue
        fk = len(fact_rows) + 1                                           # key fakta dibuat di sini (CSV == DB)
        fact_rows.append({
            "fakta_key": fk,
            "contoh_key": key_by_kode[c.slug(r["nama"])],
            "tanggal_key": c.date_key(r["tanggal"]),                      # 0 bila tanggal tidak diketahui
            "jumlah": 1,                                                  # measure: 1 kejadian
            "batch_id": batch["batch_id"], "waktu_ekstrak": batch["waktu_ekstrak"], "aturan_versi": ATURAN_VERSI,
        })
        audit_rows.append({"_i": i, "fakta_key": fk, "keputusan": "DIMUAT", "alasan": "baris kanonik"})
    fact = pd.DataFrame(fact_rows)

    # --- 2d. audit_baris_sumber: tepat satu keputusan untuk setiap baris sumber ---
    au = pd.DataFrame(audit_rows).set_index("_i").sort_index()
    audit = pd.DataFrame({
        "nama_file": src.loc[au.index, "_file"].values, "nama_sheet": src.loc[au.index, "_sheet"].values,
        "nomor_baris": src.loc[au.index, "_row"].values, "hash_baris": src.loc[au.index, "_hash"].values,
        "payload_asli": src.loc[au.index, "_payload"].values,
        "fakta_key": au["fakta_key"].astype("Int64").values,
        "keputusan": au["keputusan"].values, "alasan": au["alasan"].values,
        "batch_id": batch["batch_id"], "waktu_ekstrak": batch["waktu_ekstrak"], "aturan_versi": ATURAN_VERSI,
    })

    info = {"catatan": [], "staging": s}     # dipakai run.py untuk laporan validasi
    tables = {"dim_contoh": dim_contoh, "fact_contoh": fact, "audit_baris_sumber": audit}
    return tables, info


# =============================================================================
# 3. LOAD — full refresh (run.py yang melakukan commit/rollback)
# =============================================================================
def load(conn, tables: dict[str, pd.DataFrame]):
    """Kosongkan tabel milik KPI ini lalu isi ulang. Urutan: dimensi -> fakta -> audit (ikut arah FK).
    TODO: sesuaikan nama tabel. Tabel referensi yang isinya tetap (seed) jangan dihapus."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {KPI}.audit_baris_sumber, {KPI}.fact_contoh")
        cur.execute(f"DELETE FROM {KPI}.dim_contoh WHERE contoh_key <> 0")     # key 0 (Unknown) dipertahankan
    c.insert_df(conn, f"{KPI}.dim_contoh", tables["dim_contoh"])
    c.insert_df(conn, f"{KPI}.fact_contoh", tables["fact_contoh"])
    c.insert_df(conn, f"{KPI}.audit_baris_sumber", tables["audit_baris_sumber"])


# =============================================================================
# 4. VALIDATE — cek penerimaan
# =============================================================================
def validate(conn, tables, info, raw) -> list[tuple[str, bool, str]]:
    """Return list (nama_cek, lulus, detail). Kalau ada yang tidak lulus, run.py me-rollback semua.
    TODO: tambah cek grain (natural key unik), FK, dan kasus khusus KPI-mu."""
    n_sumber = len(raw["utama"])
    n_audit = len(tables["audit_baris_sumber"])
    hasil = [("Audit lengkap: setiap baris sumber punya keputusan", n_audit == n_sumber, f"{n_audit} dari {n_sumber}")]
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {KPI}.fact_contoh")
        n_db = cur.fetchone()[0]
    hasil.append(("Jumlah fakta di DB = hasil transform", n_db == len(tables["fact_contoh"]), f"{n_db}"))
    return hasil


def ringkasan_sql(conn) -> dict[str, pd.DataFrame]:
    """Angka contoh metrik untuk laporan (bukan angka final). TODO: ganti dengan query KxA–KxD."""
    with conn.cursor() as cur:
        cur.execute(f"""SELECT t.tahun_bulan, COUNT(*) AS jumlah FROM {KPI}.fact_contoh f
                        JOIN {KPI}.dim_tanggal t USING (tanggal_key) GROUP BY 1 ORDER BY 1""")
        df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    return {"contoh_per_bulan": df}
