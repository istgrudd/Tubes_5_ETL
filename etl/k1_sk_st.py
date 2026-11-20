"""
k1_sk_st.py — ETL KPI K1: Penugasan dosen melalui SK dan Surat Tugas (PIC: Rudi Firdaus).

APA YANG DILAKUKAN FILE INI
    Mengubah data mentah Excel SK/ST menjadi star schema K1 di PostgreSQL (schema `k1`),
    sesuai desain revisi Tubes_4 (tanpa bridge peran, tanpa kolom lolos_kpi).

SUMBER DATA
    Dataset Tugas Besar/ST dan SK FIF 2026.xlsx
        - sheet "Rincian SK dan ST" : 1 baris = 1 dosen di 1 surat (sumber fakta)
        - sheet "Data Dosen"        : master dosen (sumber dim_dosen)
    Tiga workbook lain (ST dan SK Dosen FIF, ST FIF 2023-2025, SK FIF 2023-2025) ikut dibaca
    dan disimpan di staging sebagai jejak, tapi BELUM diubah jadi fakta. Alasannya: identitas
    dosennya masih perlu direkonsiliasi (tahap 2).

TABEL HASIL (schema k1)
    dim_dosen, dim_mitra, dim_semester      -> diisi ETL setiap run
    dim_jenis_surat, dim_jenis_kegiatan,
    dim_peran, dim_status_administrasi,
    dim_unit, dim_tanggal, cfg_bobot_peran  -> isi tetap (seed) dari sql/01_k1_schema.sql
    fact_penugasan_surat                    -> tabel fakta, grain: 1 dosen x 1 surat
    audit_baris_sumber                      -> jejak keputusan untuk SETIAP baris sumber

ALUR
    extract()  -> baca Excel apa adanya
    transform()-> semua aturan bisnis (pembakuan, rekonsiliasi, pembentukan key)
    load()     -> tulis ke PostgreSQL (full refresh, 1 transaksi)
    validate() -> cek penerimaan (grain, audit lengkap, tanggal, kasus DAY 478)
    ringkasan_sql() -> beberapa angka contoh K1A/K1B/K1D untuk laporan

    Kamus (alias peran, alias mitra, kata kunci kegiatan, dll.) disimpan di config/k1/*.csv
    supaya aturan bisa diubah tanpa menyentuh kode Python.
"""
from __future__ import annotations

import re                      # regex: pencocokan kata kunci kegiatan & pola teks
from datetime import date      # tipe tanggal Python (untuk batas rentang tanggal)
from pathlib import Path       # path file lintas OS (Windows/Linux)

import pandas as pd            # pengolahan tabel

from . import common as c      # fungsi bersama semua KPI (lihat etl/common.py)

# =============================================================================
# KONSTANTA
# =============================================================================
KPI = "k1"                                   # kode KPI, dipakai untuk nama schema & folder output
ATURAN_VERSI = "k1-etl-v1.0"                 # versi aturan transformasi; dicatat di setiap baris fact/audit
CFG = c.CONFIG_DIR / "k1"                    # folder kamus K1: config/k1/

FILE_UTAMA = "ST dan SK FIF 2026.xlsx"       # workbook sumber fakta
SHEET_RINCIAN = "Rincian SK dan ST"          # sheet rincian penugasan
SHEET_MASTER = "Data Dosen"                  # sheet master dosen
# Workbook lain: hanya disimpan di staging (raw), belum ditransform ke fact.
FILE_STAGING_SAJA = ["ST dan SK Dosen FIF.xlsx", "ST FIF 2023-2025.xlsx", "SK FIF 2023-2025.xlsx"]

# Surrogate key dimensi referensi. Nilainya HARUS sama dengan isi seed di sql/01_k1_schema.sql.
JENIS_SURAT_KEY = {"SK": 1, "ST": 2, None: 0}                         # 0 = belum diketahui
STATUS_KEY = {"TERCATAT": 1, "BATAL": 2, "PERLU_REVIEW": 3, "DUPLIKAT": 4}
KEGIATAN_KEY = {"BELUM_DIKLASIFIKASIKAN": 0, "PENDIDIKAN": 1, "PENELITIAN": 2, "PENGABDIAN": 3,
                "AKREDITASI_MUTU": 4, "TATA_KELOLA": 5, "PENGEMBANGAN_KOMPETENSI": 6, "LAINNYA": 7}
# kode_peran -> (peran_key, urutan_prioritas). Prioritas kecil = tanggung jawab lebih tinggi.
# Dipakai untuk memilih peran utama bila 1 dosen punya >1 peran di surat yang sama.
PERAN = {
    "BELUM_DIKETAHUI": (0, 99), "KETUA": (1, 1), "WAKIL_KETUA": (2, 2), "PENGARAH": (3, 3),
    "PEMBINA": (4, 4), "PIC": (5, 5), "SEKRETARIS": (6, 6), "NARASUMBER": (7, 7),
    "PEMBIMBING": (8, 8), "PENGUJI": (9, 9), "ASESOR": (10, 10), "REVIEWER": (11, 11),
    "JURI": (12, 12), "PEWAWANCARA": (13, 13), "PENYUSUN": (14, 14), "PELAKSANA": (15, 15),
    "ANGGOTA": (16, 16), "PESERTA": (17, 17), "LAINNYA": (18, 98),
}
UNIT_FIF = 1                                                       # dim_unit: Fakultas Informatika
TANGGAL_MIN, TANGGAL_MAX = date(2020, 1, 1), date(2030, 12, 31)   # = rentang isi dim_tanggal

# Nama prodi di master ditulis berbeda dengan di rincian ("PRODI S1 RPL" vs
# "S1 Rekayasa Perangkat Lunak"). Kamus ini menyeragamkan ke penulisan rincian.
PRODI_ALIAS = {
    "prodi s1 informatika": "S1 Informatika", "prodi s1 teknologi informasi": "S1 Teknologi Informasi",
    "prodi s1 rpl": "S1 Rekayasa Perangkat Lunak", "prodi s1 sains data": "S1 Sains Data",
    "prodi s1 pjj informatika": "S1 PJJ Informatika", "prodi s2 informatika": "S2 Informatika",
    "prodi s3 informatika": "S3 Informatika",
    "prodi s2 ilmu forensik": "S2 Forensik Digital dan Keamanan Siber",
}


# =============================================================================
# 1. EXTRACT — baca data mentah tanpa mengubah isinya
# =============================================================================
def _sheet(path: Path, wanted: str) -> str:
    """Cari nama sheet yang sebenarnya di workbook.

    Beberapa sheet punya spasi tersembunyi di belakang namanya (mis. 'Data Dosen '),
    jadi pencocokan dilakukan setelah spasi dibuang dan huruf dikecilkan.
    """
    for s in pd.ExcelFile(path).sheet_names:            # daftar semua sheet di workbook
        if s.strip().lower() == wanted.strip().lower():  # cocokkan tanpa spasi & kapital
            return s                                     # kembalikan nama asli (dengan spasinya)
    raise ValueError(f"Sheet '{wanted}' tidak ditemukan di {path.name}")


def extract(ds_dir: Path) -> dict[str, pd.DataFrame]:
    """Baca semua sumber K1 dari folder dataset.

    Output dict:
        "rincian"       -> DataFrame sheet Rincian SK dan ST (sumber fakta)
        "master"        -> DataFrame sheet Data Dosen (sumber dim_dosen)
        "_staging_lain" -> list DataFrame workbook lain (hanya untuk staging)
    Setiap DataFrame sudah punya kolom jejak _file, _sheet, _row, _hash, _payload
    (ditambahkan oleh common.read_excel_raw).
    """
    utama = ds_dir / FILE_UTAMA
    raw = {
        "rincian": c.read_excel_raw(utama, _sheet(utama, SHEET_RINCIAN)),
        "master": c.read_excel_raw(utama, _sheet(utama, SHEET_MASTER)),
    }
    staging_saja = []
    for fname in FILE_STAGING_SAJA:
        p = ds_dir / fname
        if not p.exists():                               # file opsional: kalau tidak ada, lewati saja
            print(f"  [peringatan] {fname} tidak ditemukan, dilewati")
            continue
        for s in pd.ExcelFile(p).sheet_names:            # simpan semua sheet di workbook itu
            staging_saja.append(c.read_excel_raw(p, s))
    raw["_staging_lain"] = staging_saja
    return raw


# =============================================================================
# 2. TRANSFORM — semua aturan bisnis K1 ada di bagian ini
# =============================================================================
def _load_cfg() -> dict:
    """Baca semua kamus di config/k1/*.csv lalu ubah ke bentuk yang siap dipakai (dict/regex)."""
    peran = pd.read_csv(CFG / "peran_alias.csv", dtype=str)            # nilai Keterangan -> kode_peran
    mitra = pd.read_csv(CFG / "mitra_alias.csv", dtype=str)            # alias instansi -> mitra baku
    kw = pd.read_csv(CFG / "kegiatan_kata_kunci.csv", dtype=str)       # regex per kategori kegiatan
    ovr = pd.read_csv(CFG / "kegiatan_override.csv", dtype=str)        # kategori manual per nomor surat
    sem = pd.read_csv(CFG / "semester.csv", dtype=str)                 # kalender semester resmi
    batal = pd.read_csv(CFG / "kandidat_batal.csv", dtype=str)         # surat yang (mungkin) dibatalkan
    return {
        # kunci dikecilkan supaya pencocokan tidak peka huruf besar/kecil
        "peran": dict(zip(peran.nilai_sumber.str.strip().str.lower(), peran.kode_peran.str.strip())),
        "mitra": {r.nilai_sumber.strip().lower(): (r.mitra_id, r.nama_mitra, r.jenis_mitra)
                  for r in mitra.itertuples()},
        # regex dikompilasi sekali di sini, re.I = tidak peka huruf besar/kecil
        "kw": [(r.kode_kegiatan, re.compile(r.pola_regex, re.I)) for r in kw.itertuples()],
        "override": {c.clean_text(r.nomor_surat): r.kode_kegiatan for r in ovr.itertuples()},
        "semester": sem,
        # hanya surat yang SUDAH dikonfirmasi Sekpim (kolom = YA) yang benar-benar diberi status BATAL
        "batal": {c.clean_text(r.nomor_surat): r for r in batal.itertuples()
                  if str(r.dikonfirmasi_sekpim).strip().upper() == "YA"},
        "batal_kandidat": batal,   # daftar lengkap, ditampilkan di laporan validasi
    }


def klasifikasi_kegiatan(judul: str | None, nomor: str, cfg: dict) -> tuple[str, str]:
    """Tentukan SATU kategori utama kegiatan dari judul surat.

    Urutan aturan:
      1. Kalau nomor surat ada di kegiatan_override.csv -> pakai kategori manual itu.
      2. Hitung skor: berapa kali kata kunci tiap kategori muncul di judul.
      3. Kategori dengan skor tertinggi menang.
      4. Kalau skor seri atau tidak ada yang cocok -> BELUM_DIKLASIFIKASIKAN
         (sesuai Tubes 3: benturan kategori tidak diputuskan otomatis).
    Return: (kode_kegiatan, alasan) — alasan ditulis ke output/k1/_cek_klasifikasi_kegiatan.csv.
    """
    if nomor in cfg["override"]:
        return cfg["override"][nomor], "override manual"
    if not judul:
        return "BELUM_DIKLASIFIKASIKAN", "judul kosong"
    skor = {}
    for kode, pola in cfg["kw"]:
        n = len(pola.findall(judul))           # jumlah kata kunci kategori ini yang muncul
        if n:
            skor[kode] = skor.get(kode, 0) + n
    if not skor:
        return "BELUM_DIKLASIFIKASIKAN", "tidak ada kata kunci cocok"
    best = max(skor.values())
    top = [k for k, v in skor.items() if v == best]   # semua kategori dengan skor tertinggi
    if len(top) > 1:
        return "BELUM_DIKLASIFIKASIKAN", "kategori seri: " + ", ".join(sorted(top))
    return top[0], f"kata kunci (skor {skor})"


def _kk_code(v) -> str | None:
    """Ambil kode KK dari teks master, mis. 'Data Science and Intelligent Systems(DSIS)' -> 'DSIS'.
    Sekaligus menyelesaikan salah ketik nama KK (Algoritm/Algortm) karena yang dipakai kodenya."""
    s = c.clean_text(v)
    if not s:
        return None
    m = re.search(r"\(([A-Z]{3,6})\)", s)   # cari huruf kapital 3–6 di dalam kurung
    return m.group(1) if m else s


def build_dim_dosen(master: pd.DataFrame):
    """Bentuk dim_dosen dari sheet master 'Data Dosen' (SCD tipe 1 = keadaan terbaru saja).

    Return:
        dim   -> DataFrame calon dim_dosen (dosen_key belum diberikan)
        audit -> list (index_baris, keputusan, alasan) untuk baris master yang tidak bisa dipakai
    """
    rows, audit = [], []
    for r in master.itertuples():
        # r[...] dipakai karena nama kolom berisi spasi; +1 karena elemen pertama itertuples = index
        kode = c.clean_text(r[master.columns.get_loc("Kode Dosen") + 1])
        nama = c.clean_text(r[master.columns.get_loc("Nama") + 1])
        if not kode or not nama:
            # Tanpa kode, dosen tidak bisa dihubungkan ke rincian -> tidak masuk populasi
            audit.append((r.Index, "DI_LUAR_CAKUPAN", "Master dosen tanpa kode unik; tidak masuk populasi"))
            continue
        prodi_raw = c.clean_text(r[master.columns.get_loc("Program Studi") + 1])
        rows.append({
            "kode_dosen": kode.upper(),                                          # natural key
            "nama_dosen": nama,
            "prodi_asal": PRODI_ALIAS.get(c.norm_key(prodi_raw), prodi_raw),     # seragamkan nama prodi
            "kelompok_keahlian": _kk_code(r[master.columns.get_loc("Kelompok Keahlian") + 1]),
            "fakultas_asal": "Fakultas Informatika",
            "cakupan_populasi": True,     # TRUE = ikut dihitung di K1A/K1B (termasuk yang nol penugasan)
            "_src": r.Index,              # kolom bantu (dibuang sebelum load)
        })
    dim = pd.DataFrame(rows)
    # Kode dosen wajib unik. Kalau ganda, lebih baik berhenti daripada salah gabung dua orang.
    dup = dim[dim.kode_dosen.duplicated(keep=False)]
    if not dup.empty:
        raise ValueError(f"Kode dosen ganda di master: {sorted(dup.kode_dosen.unique())}")
    return dim, audit


def transform(raw: dict, batch: dict) -> tuple[dict[str, pd.DataFrame], dict]:
    """Ubah data mentah menjadi tabel-tabel star schema K1.

    Langkah:
      1. Bentuk dim_dosen dari master.
      2. Bakukan setiap baris rincian (teks, jenis surat, identitas dosen, peran, mitra, tanggal).
      3. Kelompokkan per (surat_id, kode_dosen) -> bentuk SATU fakta kanonik per kelompok,
         sambil menangani duplikat, multi-peran, dan konflik.
      4. Bentuk audit_baris_sumber: setiap baris sumber dapat keputusan + alasan.
      5. Bentuk dim_semester dari config (tidak ditebak).
    Return:
      tables -> dict {nama_tabel: DataFrame}; kolom DataFrame = kolom tabel di database
      info   -> data pendukung untuk laporan validasi (catatan, staging, kandidat batal, dll.)
    """
    cfg = _load_cfg()
    rin, master = raw["rincian"], raw["master"]
    info: dict = {"catatan": []}   # catatan umum yang perlu ditindaklanjuti manusia

    # ---------- 1. Master dosen -> dim_dosen ----------
    dim_dosen, audit_master = build_dim_dosen(master)
    by_kode = dim_dosen.set_index("kode_dosen")          # lookup cepat: kode -> data master
    nama_ke_kode = {}                                    # lookup: nama ternormalisasi -> [kode]
    for r in dim_dosen.itertuples():
        nama_ke_kode.setdefault(c.norm_name(r.nama_dosen), []).append(r.kode_dosen)

    # ---------- 2. Bakukan setiap baris rincian ----------
    # s = tabel staging hasil pembakuan; satu baris s = satu baris Excel rincian.
    s = pd.DataFrame({"_i": rin.index})                  # _i = index baris di DataFrame rincian
    s["_row"] = rin["_row"]                              # nomor baris asli di Excel (untuk ditelusuri)
    s["nomor_surat"] = rin["No ST/SK"].map(c.clean_text)
    s["jenis"] = rin["ST/SK"].map(lambda v: (c.clean_text(v) or "").upper() or None)        # 'SK'/'ST'/None
    s["kode_src"] = rin["Kode Dosen"].map(lambda v: (c.clean_text(v) or "").upper() or None)
    s["nama_src"] = rin["Nama Dosen"].map(c.clean_text)
    s["prodi_src"] = rin["Prodi"].map(c.clean_text)
    s["kk_src"] = rin["Kelompok Keahlian"].map(c.clean_text)
    s["judul"] = rin["Nama Kegiatan/Penugasan"].map(c.clean_text)
    s["peran_src"] = rin["Keterangan"].map(c.clean_text)                  # kolom Keterangan berisi peran
    s["mitra_src"] = rin["Instansi/Mitra Terkait"].map(c.clean_text)
    s["status_sumber"] = rin["Status"].map(c.clean_text)                  # Aktif/Selesai dari Excel (disimpan saja)
    # parse_date mengembalikan (tanggal, flag); flag menjelaskan kondisi nilai aslinya
    s[["mulai", "flag_mulai"]] = rin["Tanggal Mulai"].map(c.parse_date).tolist()
    s[["selesai", "flag_selesai"]] = rin["Tanggal Selesai"].map(c.parse_date).tolist()
    # pandas 3 mengubah None jadi NaN pada kolom teks; kembalikan ke None supaya `if nilai:` aman
    for col in ["nomor_surat", "jenis", "kode_src", "nama_src", "prodi_src", "kk_src", "judul",
                "peran_src", "mitra_src", "status_sumber"]:
        s[col] = s[col].astype(object).where(s[col].notna(), None)
    s["masalah"] = [[] for _ in range(len(s))]   # daftar masalah -> membuat status PERLU_REVIEW
    s["catatan"] = [[] for _ in range(len(s))]   # daftar catatan info -> tidak mengubah status

    # 2a. Jenis surat kosong: boleh diwarisi dari baris lain dengan NOMOR SURAT SAMA
    #     (bukti dari data itu sendiri). Tidak ditebak dari nama file atau format nomor.
    jenis_per_nomor = s.dropna(subset=["jenis"]).groupby("nomor_surat")["jenis"].agg(set)
    for i in s.index[s.jenis.isna()]:
        js = jenis_per_nomor.get(s.at[i, "nomor_surat"], set())
        if len(js) == 1:                                   # hanya kalau jenisnya tunggal & jelas
            s.at[i, "jenis"] = next(iter(js))
            s.at[i, "catatan"].append("jenis surat diwarisi dari baris lain bernomor sama")
        else:
            s.at[i, "masalah"].append("jenis SK/ST kosong dan tidak ada bukti dari nomor yang sama")
    s.loc[~s.jenis.isin(["SK", "ST"]) & s.jenis.notna(), "jenis"] = None   # nilai aneh -> tidak diketahui

    # 2b. surat_id = ID integrasi surat: "FIF|<jenis>|<nomor kapital>".
    #     Tahun dan angka nol di depan nomor dipertahankan (tidak "dibetulkan").
    s["surat_id"] = s.apply(lambda r: f"FIF|{r.jenis or 'UNKNOWN'}|{(r.nomor_surat or '').upper()}", axis=1)

    # 2c. Identitas dosen.
    master_nama = {k: list(v) for k, v in nama_ke_kode.items()}   # salinan: nama dari master SAJA
    # Tambahkan pasangan nama->kode yang terbukti dari baris rincian lain yang berkode.
    # Contoh: 'Hilya Tsania Ismet' di master ditulis 'Tsaniya', tapi di rincian ada baris berkode HYA.
    for kd, nm in s.dropna(subset=["kode_src", "nama_src"])[["kode_src", "nama_src"]].drop_duplicates().itertuples(index=False):
        lst = nama_ke_kode.setdefault(c.norm_name(nm), [])
        if kd not in lst:
            lst.append(kd)
    extra_dosen = {}                                   # kode yang ada di rincian tapi tidak ada di master
    s["kode_dosen"] = pd.Series([None] * len(s), dtype=object, index=s.index)   # kode final per baris
    for i, r in s.iterrows():
        if r.kode_src:
            # Kasus A: baris punya kode dosen -> kode dipakai apa adanya.
            s.at[i, "kode_dosen"] = r.kode_src
            if r.kode_src in by_kode.index:
                # Kode ada di master: cek apakah ejaan nama berbeda (info saja, kode tetap dipakai)
                if c.norm_name(r.nama_src) and c.norm_name(r.nama_src) != c.norm_name(by_kode.at[r.kode_src, "nama_dosen"]):
                    s.at[i, "catatan"].append(
                        f"nama sumber '{r.nama_src}' beda ejaan dengan master '{by_kode.at[r.kode_src, 'nama_dosen']}'; kode dipakai")
            else:
                # Kode tidak ada di master: tetap dimuat sebagai dosen di luar populasi.
                # Kalau namanya sama dengan dosen master berkode lain -> kemungkinan salah ketik kode.
                extra_dosen.setdefault(r.kode_src, r)
                mirip = [k for k in master_nama.get(c.norm_name(r.nama_src), []) if k != r.kode_src]
                s.at[i, "catatan"].append("kode dosen tidak ada di master; dosen di luar populasi (cakupan_populasi = false)"
                                          + (f"; NAMA SAMA dengan master kode {', '.join(mirip)} -> cek kemungkinan salah kode" if mirip else ""))
        else:
            # Kasus B: kode kosong -> boleh diisi dari nama HANYA bila nama persis cocok dengan
            # tepat SATU kode. Kalau tidak cocok / cocok ke >1 kode, dibiarkan kosong (-> audit PERLU_REVIEW).
            cocok = nama_ke_kode.get(c.norm_name(r.nama_src), []) if r.nama_src else []
            if len(cocok) == 1:
                s.at[i, "kode_dosen"] = cocok[0]
                s.at[i, "catatan"].append(f"kode kosong; diisi '{cocok[0]}' berdasarkan nama persis (master/baris berkode)")
    # Dosen berkode tapi tidak ada di master -> ditambahkan ke dim_dosen dengan cakupan_populasi = false
    if extra_dosen:
        add = pd.DataFrame([{
            "kode_dosen": k, "nama_dosen": r.nama_src or k, "prodi_asal": r.prodi_src,
            "kelompok_keahlian": r.kk_src, "fakultas_asal": None, "cakupan_populasi": False, "_src": None,
        } for k, r in sorted(extra_dosen.items())])
        dim_dosen = pd.concat([dim_dosen, add], ignore_index=True)
    dim_dosen.insert(0, "dosen_key", range(1, len(dim_dosen) + 1))       # surrogate key 1..n
    dosen_key = dict(zip(dim_dosen.kode_dosen, dim_dosen.dosen_key))     # lookup kode -> dosen_key

    # 2d. Peran: nilai Keterangan dipetakan ke kode_peran lewat config/k1/peran_alias.csv
    def map_peran(v):
        """Return (kode_peran, pesan_error). Kosong -> BELUM_DIKETAHUI tanpa error;
        nilai yang tidak dikenal -> BELUM_DIKETAHUI + error (baris jadi PERLU_REVIEW, tidak ditebak)."""
        if not v:
            return "BELUM_DIKETAHUI", None
        k = cfg["peran"].get(v.lower())
        return (k, None) if k else ("BELUM_DIKETAHUI", f"peran '{v}' tidak ada di kamus peran")
    for i, (k, err) in zip(s.index, s.peran_src.map(map_peran)):
        s.at[i, "kode_peran"] = k
        if err:
            s.at[i, "masalah"].append(err)

    # 2e. Mitra: alias disatukan lewat config/k1/mitra_alias.csv (mis. Telu/TelU/Telkom University).
    #     Nilai yang belum ada di kamus tetap dibuat mitra sendiri dengan jenis BELUM_DIKETAHUI.
    mitra_rows = {}                                      # mitra_id -> (nama_mitra, jenis_mitra)
    def map_mitra(v):
        """Return mitra_id baku untuk satu nilai Instansi/Mitra.
        Kosong -> 'UNKNOWN' (mitra_key 0). Ada di kamus -> mitra_id dari kamus.
        Tidak ada di kamus -> mitra_id dibentuk dengan slug() dan jenis BELUM_DIKETAHUI.
        Setiap mitra yang dipakai dicatat di mitra_rows untuk membentuk dim_mitra."""
        if not v:
            return "UNKNOWN"                             # -> mitra_key 0
        hit = cfg["mitra"].get(v.lower())
        mid, nama, jenis = hit if hit else (c.slug(v), v, "BELUM_DIKETAHUI")
        mitra_rows.setdefault(mid, (nama, jenis))
        return mid
    s["mitra_id"] = s.mitra_src.map(map_mitra)
    dim_mitra = pd.DataFrame([{"mitra_id": k, "nama_mitra": v[0], "jenis_mitra": v[1], "negara": None}
                              for k, v in sorted(mitra_rows.items())])
    dim_mitra.insert(0, "mitra_key", range(1, len(dim_mitra) + 1))       # 0 sudah dipakai UNKNOWN (seed)
    mitra_key = {"UNKNOWN": 0, **dict(zip(dim_mitra.mitra_id, dim_mitra.mitra_key))}

    # 2f. Tanggal.
    #     Tanggal di luar rentang dim_tanggal (2020–2030) hampir pasti salah ketik (mis. 2000-10-26)
    #     -> dianggap belum diketahui; nilai aslinya tetap tersimpan di audit (payload_asli).
    for col, fcol in (("mulai", "flag_mulai"), ("selesai", "flag_selesai")):
        luar = s[col].map(lambda d: d is not None and not (TANGGAL_MIN <= d <= TANGGAL_MAX))
        s.loc[luar, fcol] = "DI_LUAR_RENTANG_" + s.loc[luar, col].astype(str)
        s.loc[luar, col] = None
    # Catat kondisi tanggal yang tidak normal sebagai info
    for i, r in s.iterrows():
        for col, flag in (("mulai", r.flag_mulai), ("selesai", r.flag_selesai)):
            if flag == "TEKS":
                s.at[i, "catatan"].append(f"tanggal {col} diparse dari teks")
            elif flag != "OK" and flag != "KOSONG":
                s.at[i, "catatan"].append(f"tanggal {col} {flag.lower()}; dianggap belum diketahui")
    # tanggal_valid = kedua tanggal ada DAN mulai <= selesai. Hanya yang valid yang punya status masa tugas.
    s["tanggal_valid"] = s.apply(lambda r: r.mulai is not None and r.selesai is not None and r.mulai <= r.selesai, axis=1)
    terbalik = s.mulai.notna() & s.selesai.notna() & ~s.tanggal_valid
    for i in s.index[terbalik]:
        s.at[i, "catatan"].append("tanggal mulai > tanggal selesai; tanggal_valid = false")

    # ---------- 3. Bentuk SATU fakta kanonik per (surat_id, kode_dosen) ----------
    def judul_key(v):
        """Judul untuk perbandingan: huruf kecil, tanpa tanda baca/spasi (beda ')' di ujung dianggap sama)."""
        return re.sub(r"[^a-z0-9]", "", (v or "").lower())

    def sig(r):
        """'Sidik jari' isi material satu baris. Dua baris dengan sig sama = salinan identik.
        Kolom No (nomor urut Excel) sengaja tidak ikut."""
        return (judul_key(r.judul), r.kode_peran, r.mitra_id, r.mulai, r.selesai, r.jenis)

    fact_rows, audit_rows = [], []

    # 3a. Baris tanpa identitas dosen tidak bisa jadi fakta (dosen_key wajib) -> audit PERLU_REVIEW.
    #     Sesuai Tubes 3: tidak digabung ke satu dosen "Unknown".
    tanpa_dosen = s[s.kode_dosen.isna()]
    for i, r in tanpa_dosen.iterrows():
        alasan = ("baris tanpa identitas dosen (kode & nama kosong)" if not r.nama_src else
                  f"kode dosen kosong dan nama '{r.nama_src}' tidak ada di master (kemungkinan mahasiswa/tendik/luar FIF)")
        audit_rows.append({"_i": i, "fact": None, "keputusan": "PERLU_REVIEW", "alasan": alasan})

    # 3b. Setiap kelompok (surat_id, kode_dosen) = calon 1 fakta (grain K1)
    for (sid, kode), g in s[s.kode_dosen.notna()].groupby(["surat_id", "kode_dosen"], sort=True):
        g = g.sort_values("_row")                       # urut sesuai posisi di Excel
        sigs = {}                                       # sig -> daftar index baris dengan isi sama
        for i, r in g.iterrows():
            sigs.setdefault(sig(r), []).append(i)
        varian = [idx[0] for idx in sigs.values()]          # 1 wakil untuk tiap variasi isi
        dupl = [i for idx in sigs.values() for i in idx[1:]]  # sisanya = salinan identik -> DUPLIKAT
        masalah = [m for i in g.index for m in s.at[i, "masalah"]]   # kumpulkan masalah semua baris
        alasan_kanon = []

        if len(varian) == 1:
            # Kasus normal: hanya satu variasi isi -> baris pertama jadi fakta kanonik
            kanon = varian[0]
            peran, mulai, selesai = s.at[kanon, "kode_peran"], s.at[kanon, "mulai"], s.at[kanon, "selesai"]
            sekunder = []
        else:
            # Ada >1 variasi isi untuk pasangan surat-dosen yang sama -> cek jenis perbedaannya
            v = s.loc[varian]
            judul_sama = v.judul.map(judul_key).nunique() == 1
            mitra_sama = v.mitra_id.nunique() == 1
            interval_valid = {(a, b) for a, b, ok in zip(v.mulai, v.selesai, v.tanggal_valid) if ok}
            interval_lain = v[~v.tanggal_valid & (v.mulai.notna() | v.selesai.notna())]
            if judul_sama and mitra_sama and len(interval_valid) <= 1 and interval_lain.empty and v.kode_peran.nunique() > 1:
                # MULTI-PERAN (Tubes_4 subbab 3.3): isi sama, hanya peran yang beda.
                # -> peran dengan urutan_prioritas terkecil jadi peran utama,
                #    tanggal diambil dari baris yang tanggalnya valid,
                #    baris lain dicatat DIGABUNG di audit.
                v = v.assign(_prio=v.kode_peran.map(lambda k: PERAN[k][1])).sort_values(["_prio", "_row"])
                kanon = v.index[0]
                peran = s.at[kanon, "kode_peran"]
                (mulai, selesai) = next(iter(interval_valid)) if interval_valid else (None, None)
                sekunder = list(v.index[1:])
                alasan_kanon.append(f"multi-peran: peran utama {peran} (prioritas {PERAN[peran][1]})"
                                    + ("; interval dari baris lain yang valid" if not s.at[kanon, "tanggal_valid"] and interval_valid else ""))
            else:
                # KONFLIK: judul/tanggal/mitra berbeda -> tidak dipilih otomatis.
                # Fakta tetap dibuat dari baris pertama, tapi statusnya PERLU_REVIEW (tidak dihitung KPI).
                kanon = varian[0]
                peran, mulai, selesai = s.at[kanon, "kode_peran"], s.at[kanon, "mulai"], s.at[kanon, "selesai"]
                sekunder = []
                beda = [n for n, ok in (("judul", judul_sama), ("mitra", mitra_sama),
                                         ("tanggal", len(interval_valid) <= 1 and interval_lain.empty)) if not ok]
                if not beda and v.kode_peran.nunique() == 1:
                    beda = ["atribut lain"]
                masalah.append(f"konflik antar baris surat-dosen yang sama ({', '.join(beda)} berbeda)")

        nomor = s.at[kanon, "nomor_surat"]
        jenis = s.at[kanon, "jenis"]
        tanggal_valid = mulai is not None and selesai is not None and mulai <= selesai
        keg, keg_alasan = klasifikasi_kegiatan(s.at[kanon, "judul"], nomor, cfg)

        # Status administrasi fakta (pengganti lolos_kpi): hanya TERCATAT yang dihitung KPI.
        if masalah:
            status = "PERLU_REVIEW"
        elif nomor in cfg["batal"]:
            status = "BATAL"
            alasan_kanon.append(f"dibatalkan oleh {cfg['batal'][nomor].dibatalkan_oleh} (dikonfirmasi Sekpim)")
        else:
            status = "TERCATAT"

        fk = len(fact_rows) + 1          # penugasan_key dibuat di sini supaya CSV == isi database
        fact_rows.append({
            "penugasan_key": fk, "surat_id": sid, "nomor_surat": nomor, "judul_kegiatan": s.at[kanon, "judul"],
            "dosen_key": dosen_key[kode], "jenis_surat_key": JENIS_SURAT_KEY[jenis],
            "jenis_kegiatan_key": KEGIATAN_KEY[keg], "peran_key": PERAN[peran][0],
            "unit_pengelola_key": UNIT_FIF, "mitra_key": mitra_key[s.at[kanon, "mitra_id"]],
            "status_administrasi_key": STATUS_KEY[status],
            "tgl_terbit_key": 0,  # sumber 2026 tidak punya tanggal terbit; tidak direkonstruksi dari tanggal mulai
            "tgl_mulai_key": c.date_key(mulai), "tgl_selesai_key": c.date_key(selesai),   # YYYYMMDD atau 0
            "status_sumber": s.at[kanon, "status_sumber"], "tanggal_valid": bool(tanggal_valid),
            "jumlah_penugasan": 1,                                                    # measure: selalu 1
            "durasi_hari": (selesai - mulai).days + 1 if tanggal_valid else None,     # measure: inklusif
            "sumber_utama": f"{FILE_UTAMA}#{SHEET_RINCIAN}",
            "batch_id": batch["batch_id"], "waktu_ekstrak": batch["waktu_ekstrak"], "aturan_versi": ATURAN_VERSI,
            "_kegiatan_alasan": keg_alasan,   # kolom bantu (dibuang sebelum load)
        })
        # Catat keputusan audit untuk setiap baris di kelompok ini
        kep_kanon = "PERLU_REVIEW" if status == "PERLU_REVIEW" else "DIMUAT"
        audit_rows.append({"_i": kanon, "fact": fk, "keputusan": kep_kanon,
                           "alasan": "; ".join(alasan_kanon + masalah) or "baris kanonik"})
        for i in sekunder:     # baris peran sekunder (multi-peran)
            audit_rows.append({"_i": i, "fact": fk, "keputusan": "DIGABUNG",
                               "alasan": f"peran sekunder: {s.at[i, 'kode_peran']}"})
        for i in dupl:         # salinan identik
            audit_rows.append({"_i": i, "fact": fk, "keputusan": "DUPLIKAT",
                               "alasan": f"salinan identik baris {s.at[kanon, '_row']} (kolom No diabaikan)"})
        for i in g.index:      # sisa baris (varian konflik)
            if i not in [kanon, *sekunder, *dupl]:
                audit_rows.append({"_i": i, "fact": fk, "keputusan": "PERLU_REVIEW",
                                   "alasan": "; ".join(masalah)})

    fact = pd.DataFrame(fact_rows)

    # ---------- 4. audit_baris_sumber: SEMUA baris sumber harus tercatat ----------
    au = pd.DataFrame(audit_rows).set_index("_i").sort_index()
    # Pengaman: jumlah keputusan audit harus sama persis dengan jumlah baris sumber
    assert len(au) == len(s) and au.index.is_unique, "Ada baris sumber yang hilang/ganda di audit"
    bukti = s.set_index("_i")["catatan"].map(lambda x: "; ".join(x) or None)   # catatan info -> bukti_keputusan
    audit = pd.DataFrame({
        "nama_file": rin.loc[au.index, "_file"].values, "nama_sheet": rin.loc[au.index, "_sheet"].values,
        "nomor_baris": rin.loc[au.index, "_row"].values, "hash_baris": rin.loc[au.index, "_hash"].values,
        "payload_asli": rin.loc[au.index, "_payload"].values,                      # isi baris asli (jsonb)
        "penugasan_key": au["fact"].astype("Int64").values,                         # NULL bila tidak jadi fakta
        "keputusan": au["keputusan"].values,
        "alasan": au["alasan"].values, "bukti_keputusan": bukti.loc[au.index].values,
    })
    # Baris master yang tidak dipakai juga dicatat di audit (penugasan_key NULL).
    # Dibuat sebagai DataFrame dengan tipe kolom yang sama, lalu digabung (concat),
    # supaya pandas tidak memunculkan FutureWarning soal kolom yang isinya NA semua.
    if audit_master:
        idx = [i for i, _, _ in audit_master]
        audit_m = pd.DataFrame({
            "nama_file": master.loc[idx, "_file"].values, "nama_sheet": master.loc[idx, "_sheet"].values,
            "nomor_baris": master.loc[idx, "_row"].values, "hash_baris": master.loc[idx, "_hash"].values,
            "payload_asli": master.loc[idx, "_payload"].values,
            "penugasan_key": pd.array([pd.NA] * len(idx), dtype="Int64"),
            "keputusan": [k for _, k, _ in audit_master], "alasan": [a for _, _, a in audit_master],
            "bukti_keputusan": pd.Series([None] * len(idx), dtype=object),
        })
        audit = pd.concat([audit, audit_m], ignore_index=True)
    audit["batch_id"] = batch["batch_id"]
    audit["waktu_ekstrak"] = batch["waktu_ekstrak"]
    audit["aturan_versi"] = ATURAN_VERSI

    # ---------- 5. dim_semester dari config/k1/semester.csv (tidak ditebak) ----------
    dim_semester = cfg["semester"].copy()
    if dim_semester.empty:
        info["catatan"].append("config/k1/semester.csv masih kosong: isi dari kalender akademik resmi agar query K1A per semester bisa jalan.")

    # Data pendukung laporan validasi
    info["kandidat_batal"] = cfg["batal_kandidat"]
    info["staging"] = s
    tables = {
        "dim_dosen": dim_dosen.drop(columns=["_src"]),
        "dim_mitra": dim_mitra,
        "dim_semester": dim_semester,
        "fact_penugasan_surat": fact.drop(columns=["_kegiatan_alasan"]),
        "audit_baris_sumber": audit,
    }
    info["kegiatan_alasan"] = fact[["penugasan_key", "nomor_surat", "judul_kegiatan", "jenis_kegiatan_key", "_kegiatan_alasan"]]
    return tables, info


# =============================================================================
# 3. LOAD — full refresh dalam 1 transaksi (dijalankan 2x hasilnya tetap sama = idempotent)
# =============================================================================
def load(conn, tables: dict[str, pd.DataFrame]):
    """Kosongkan tabel yang diisi ETL, lalu isi ulang dari hasil transform.

    Dimensi referensi (jenis surat, peran, status, kegiatan, unit, tanggal) TIDAK disentuh
    karena isinya tetap (seed). Commit/rollback dilakukan oleh run.py.
    """
    with conn.cursor() as cur:
        # Fakta & audit dikosongkan dulu karena punya FK ke dimensi
        cur.execute("TRUNCATE k1.audit_baris_sumber, k1.fact_penugasan_surat RESTART IDENTITY")
        cur.execute("DELETE FROM k1.dim_dosen")
        cur.execute("DELETE FROM k1.dim_mitra WHERE mitra_key <> 0")     # key 0 (UNKNOWN) dipertahankan
        cur.execute("DELETE FROM k1.dim_semester")
    # Urutan isi: dimensi dulu, baru fakta, terakhir audit (mengikuti arah FK)
    c.insert_df(conn, "k1.dim_dosen", tables["dim_dosen"])
    c.insert_df(conn, "k1.dim_mitra", tables["dim_mitra"])
    if not tables["dim_semester"].empty:
        c.insert_df(conn, "k1.dim_semester", tables["dim_semester"])
    _insert_fact(conn, tables["fact_penugasan_surat"])
    c.insert_df(conn, "k1.audit_baris_sumber", tables["audit_baris_sumber"])


def _insert_fact(conn, fact: pd.DataFrame):
    """INSERT fakta dengan penugasan_key dari transform.

    Kolom penugasan_key bertipe GENERATED ALWAYS AS IDENTITY, jadi perlu OVERRIDING SYSTEM VALUE.
    Setelah itu sequence-nya disamakan dengan nilai maksimum agar INSERT manual berikutnya tidak bentrok.
    """
    from psycopg2.extras import execute_values
    cols = list(fact.columns)
    rows = [tuple(c._py(v) for v in r) for r in fact.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO k1.fact_penugasan_surat ({', '.join(cols)}) "
                            f"OVERRIDING SYSTEM VALUE VALUES %s", rows, page_size=500)
        cur.execute("SELECT setval(pg_get_serial_sequence('k1.fact_penugasan_surat','penugasan_key'), "
                    "COALESCE(MAX(penugasan_key),1)) FROM k1.fact_penugasan_surat")


# =============================================================================
# 4. VALIDATE — cek penerimaan (Lampiran C Tubes 3 + kasus revisi Tubes 4)
# =============================================================================
# Setiap query menghitung jumlah PELANGGARAN. 0 = lulus.
CEK_SQL = {
    "Grain: tidak ada surat_id+dosen_key ganda": """
        SELECT COUNT(*) FROM (SELECT surat_id, dosen_key FROM k1.fact_penugasan_surat
        GROUP BY 1,2 HAVING COUNT(*)>1) x""",
    "Audit lengkap: jumlah baris audit rincian = jumlah baris sumber": None,   # dicek di Python, lihat validate()
    "Setiap baris audit DIGABUNG/DUPLIKAT menunjuk fakta": """
        SELECT COUNT(*) FROM k1.audit_baris_sumber
        WHERE keputusan IN ('DIGABUNG','DUPLIKAT') AND penugasan_key IS NULL""",
    "durasi_hari hanya terisi bila tanggal valid": """
        SELECT COUNT(*) FROM k1.fact_penugasan_surat WHERE NOT tanggal_valid AND durasi_hari IS NOT NULL""",
    "Key tanggal 0 hanya bila tanggal tidak valid": """
        SELECT COUNT(*) FROM k1.fact_penugasan_surat
        WHERE tanggal_valid AND (tgl_mulai_key = 0 OR tgl_selesai_key = 0)""",
    "Kasus DAY surat 478 = 1 fakta, peran PIC, 4–6 Agu 2026": """
        SELECT CASE WHEN COUNT(*)=1 AND MIN(r.kode_peran)='PIC' AND MIN(f.tgl_mulai_key)=20260804
                     AND MIN(f.tgl_selesai_key)=20260806 THEN 0 ELSE 1 END
        FROM k1.fact_penugasan_surat f JOIN k1.dim_dosen d USING (dosen_key) JOIN k1.dim_peran r USING (peran_key)
        WHERE f.nomor_surat='478/SDM-33/IF-DEK/2026' AND d.kode_dosen='DAY'""",
}


def validate(conn, tables, info, raw) -> list[tuple[str, bool, str]]:
    """Jalankan semua cek. Return list (nama_cek, lulus?, detail). run.py me-rollback bila ada yang gagal.

    raw = hasil extract(); dipakai untuk menghitung jumlah baris sumber rincian.
    """
    n_sumber = len(raw["rincian"])
    hasil = []
    with conn.cursor() as cur:
        for nama, sql in CEK_SQL.items():
            if sql is None:
                # Audit lengkap: setiap baris rincian harus punya tepat satu baris audit
                n = (tables["audit_baris_sumber"].nama_sheet.str.strip() == SHEET_RINCIAN).sum()
                hasil.append((nama, n == n_sumber, f"{n} dari {n_sumber}"))
                continue
            cur.execute(sql)
            bad = cur.fetchone()[0]                    # jumlah pelanggaran
            hasil.append((nama, bad == 0, "OK" if bad == 0 else f"{bad} pelanggaran"))
    return hasil


def ringkasan_sql(conn) -> dict[str, pd.DataFrame]:
    """Angka contoh untuk laporan progres (bukan angka final KPI).

    Menunjukkan bahwa star schema hasil ETL bisa menjawab metrik K1A, K1B, dan K1D.
    Semua query memfilter kode_status = 'TERCATAT' (pengganti lolos_kpi).
    """
    q = {
        # Kontrol cakupan: berapa fakta per status administrasi
        "status_administrasi": """
            SELECT a.kode_status, COUNT(*) AS baris_fact FROM k1.fact_penugasan_surat f
            JOIN k1.dim_status_administrasi a USING (status_administrasi_key) GROUP BY 1 ORDER BY 1""",
        # Kontrol kualitas: berapa baris sumber per keputusan audit
        "keputusan_audit": """
            SELECT keputusan, COUNT(*) AS baris FROM k1.audit_baris_sumber GROUP BY 1 ORDER BY 1""",
        # K1A: 10 dosen dengan keterlibatan tertinggi pada tahun 2026 (interval beririsan dengan 2026)
        "k1a_top10_2026": """
            SELECT d.kode_dosen, d.nama_dosen, COUNT(*) AS keterlibatan, COUNT(DISTINCT f.surat_id) AS surat_unik,
                   COUNT(*) FILTER (WHERE j.kode_jenis='SK') AS sk, COUNT(*) FILTER (WHERE j.kode_jenis='ST') AS st
            FROM k1.fact_penugasan_surat f
            JOIN k1.dim_status_administrasi a USING (status_administrasi_key)
            JOIN k1.dim_dosen d USING (dosen_key) JOIN k1.dim_jenis_surat j USING (jenis_surat_key)
            JOIN k1.dim_tanggal tm ON tm.tanggal_key=f.tgl_mulai_key JOIN k1.dim_tanggal ts ON ts.tanggal_key=f.tgl_selesai_key
            WHERE a.kode_status='TERCATAT' AND f.tanggal_valid AND d.cakupan_populasi
              AND tm.tanggal <= DATE '2026-12-31' AND ts.tanggal >= DATE '2026-01-01'
            GROUP BY 1,2 ORDER BY keterlibatan DESC, 1 LIMIT 10""",
        # K1B: status masa tugas pada tanggal hari ini (CURRENT_DATE sebagai tanggal acuan)
        "k1b_status_masa_tugas": """
            SELECT CASE WHEN NOT f.tanggal_valid THEN 'TIDAK_DAPAT_DITENTUKAN'
                        WHEN CURRENT_DATE < tm.tanggal THEN 'BELUM_MULAI'
                        WHEN CURRENT_DATE > ts.tanggal THEN 'BERAKHIR' ELSE 'AKTIF' END AS status_masa_tugas,
                   COUNT(*) AS penugasan
            FROM k1.fact_penugasan_surat f JOIN k1.dim_status_administrasi a USING (status_administrasi_key)
            JOIN k1.dim_tanggal tm ON tm.tanggal_key=f.tgl_mulai_key JOIN k1.dim_tanggal ts ON ts.tanggal_key=f.tgl_selesai_key
            WHERE a.kode_status='TERCATAT' GROUP BY 1 ORDER BY 1""",
        # K1D: sebaran keterlibatan per jenis kegiatan x peran
        "k1d_kegiatan_x_peran": """
            SELECT k.nama_kategori, r.nama_peran, COUNT(*) AS keterlibatan
            FROM k1.fact_penugasan_surat f JOIN k1.dim_status_administrasi a USING (status_administrasi_key)
            JOIN k1.dim_jenis_kegiatan k USING (jenis_kegiatan_key) JOIN k1.dim_peran r USING (peran_key)
            WHERE a.kode_status='TERCATAT' GROUP BY 1,2,r.urutan_prioritas ORDER BY 1, r.urutan_prioritas""",
    }
    out = {}
    with conn.cursor() as cur:
        for k, sql in q.items():
            cur.execute(sql)
            # ubah hasil query (list of tuple) jadi DataFrame dengan nama kolom dari cursor
            out[k] = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    return out
