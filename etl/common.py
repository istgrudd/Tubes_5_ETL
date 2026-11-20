"""
common.py — fungsi bersama untuk ETL semua KPI (K1–K5).

Kenapa ada file ini?
    Supaya semua anggota kelompok memakai cara yang SAMA untuk membaca Excel, membakukan teks,
    membaca tanggal, dan menulis ke database. Teman-teman (K2–K5) cukup `from . import common as c`,
    jangan copy-paste fungsi dari sini.

Isi:
    1. Path proyek & pengaturan (config/settings.ini)
    2. Batch / provenance  : ID unik setiap kali ETL dijalankan
    3. EXTRACT helper      : baca Excel apa adanya + jejak sumber (file, sheet, baris, hash)
    4. TRANSFORM helper    : bersihkan teks, normalisasi nama, parsing tanggal, slug
    5. LOAD helper         : koneksi PostgreSQL, jalankan file SQL, INSERT DataFrame, staging, CSV
"""
from __future__ import annotations

import configparser            # membaca file .ini (settings.ini)
import hashlib                 # sha256 untuk hash baris sumber
import json                    # menyimpan isi baris asli sebagai JSON (kolom jsonb)
import math                    # cek NaN
import re                      # regex
import unicodedata             # normalisasi karakter unicode (mis. spasi aneh dari Excel)
import uuid                    # bagian acak pada batch_id
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# =============================================================================
# 1. PATH PROYEK & PENGATURAN
# =============================================================================
ROOT = Path(__file__).resolve().parents[1]          # folder Tubes_5_ETL (induk dari folder etl/)
CONFIG_DIR = ROOT / "config"                        # kamus & settings
SQL_DIR = ROOT / "sql"                              # DDL schema
OUTPUT_DIR = ROOT / "output"                        # hasil CSV + laporan validasi
# Default lokasi dataset: folder "Dataset Tugas Besar" di sebelah folder Tubes_5_ETL (di dalam Tugas/)
DEFAULT_DATASET_DIR = ROOT.parent / "Dataset Tugas Besar"


def load_settings() -> configparser.ConfigParser:
    """Baca config/settings.ini. Kalau belum dibuat, pakai settings.example.ini sebagai cadangan.

    settings.ini berisi password database, jadi file ini tidak ikut dibagikan.
    """
    cfg = configparser.ConfigParser()
    path = CONFIG_DIR / "settings.ini"
    if not path.exists():
        path = CONFIG_DIR / "settings.example.ini"
    cfg.read(path, encoding="utf-8")
    return cfg


def dataset_dir(cfg: configparser.ConfigParser) -> Path:
    """Folder dataset: dari settings.ini [path] dataset_dir, atau default bila kosong."""
    p = cfg.get("path", "dataset_dir", fallback="").strip()
    return Path(p) if p else DEFAULT_DATASET_DIR


# =============================================================================
# 2. BATCH / PROVENANCE
# =============================================================================
def new_batch(kpi: str) -> dict:
    """Buat identitas satu kali run ETL.

    batch_id dan waktu_ekstrak ditulis ke setiap baris staging, fact, dan audit,
    sehingga setiap data bisa ditelusuri berasal dari run yang mana (provenance).
    Contoh batch_id: k1_20261117_020501_a1b2c3
    """
    now = datetime.now().replace(microsecond=0)
    return {
        "kpi": kpi,
        "batch_id": f"{kpi}_{now:%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}",
        "waktu_ekstrak": now,
    }


# =============================================================================
# 3. EXTRACT HELPER
# =============================================================================
def _json_safe(v):
    """Ubah nilai satu sel Excel menjadi nilai yang bisa disimpan sebagai JSON.

    NaN -> None, tanggal -> teks 'YYYY-MM-DD HH:MM:SS', angka numpy -> angka Python biasa.
    """
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if hasattr(v, "item"):            # numpy scalar (mis. numpy.int64) -> int Python
        return v.item()
    return v


def read_excel_raw(path: Path, sheet: str, header_row: int = 0) -> pd.DataFrame:
    """Baca satu sheet Excel APA ADANYA, lalu tambahkan kolom jejak sumber.

    - dtype=object: pandas tidak boleh mengubah tipe data (mis. kode '07' jadi angka 7).
    - Baris yang semua selnya kosong dibuang (itu cuma format Excel, bukan data).
    Kolom jejak yang ditambahkan:
        _file    : nama file Excel
        _sheet   : nama sheet (tanpa spasi di ujung)
        _row     : nomor baris di Excel (baris header = 1, data pertama = 2)
        _payload : dict isi baris asli -> disimpan ke kolom jsonb (staging & audit)
        _hash    : sha256 dari isi baris asli -> mendeteksi baris identik / perubahan data
    """
    df = pd.read_excel(path, sheet_name=sheet, header=header_row, dtype=object)
    df.columns = [str(col).strip() for col in df.columns]     # rapikan spasi di nama kolom
    df = df.dropna(how="all")                                  # buang baris kosong total
    payloads = [
        {col: _json_safe(v) for col, v in zip(df.columns, row)}
        for row in df.itertuples(index=False, name=None)
    ]
    df["_file"] = path.name
    df["_sheet"] = sheet.strip()
    df["_row"] = df.index + header_row + 2      # index pandas mulai 0 -> +2 = nomor baris Excel
    df["_payload"] = payloads
    df["_hash"] = [
        hashlib.sha256(json.dumps(p, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()
        for p in payloads
    ]
    return df.reset_index(drop=True)


# =============================================================================
# 4. TRANSFORM HELPER
# =============================================================================
def is_blank(v) -> bool:
    """True bila nilai dianggap kosong: None, NaN, string kosong, '-', 'nan', 'None'."""
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return isinstance(v, str) and v.strip() in ("", "-", "nan", "None")


def clean_text(v) -> str | None:
    """Rapikan teks: normalisasi unicode, gabungkan spasi/enter berlebih jadi satu spasi.
    Nilai kosong dikembalikan sebagai None."""
    if is_blank(v):
        return None
    s = unicodedata.normalize("NFKC", str(v))   # mis. spasi non-breaking -> spasi biasa
    s = " ".join(s.split())                     # spasi ganda, tab, enter -> satu spasi
    return s or None


def norm_key(v) -> str | None:
    """Kunci pencocokan kamus/alias: teks bersih + huruf kecil. Mis. ' TelU ' -> 'telu'."""
    s = clean_text(v)
    return s.lower() if s else None


def norm_name(v) -> str | None:
    """Normalisasi nama orang untuk pencocokan.

    'Erwin Budi Setiawan, S.T., M.T.' -> 'erwin budi setiawan'
    Gelar setelah koma dibuang, tanda baca dibuang, huruf dikecilkan.
    """
    s = clean_text(v)
    if not s:
        return None
    s = s.split(",")[0]                         # ambil bagian sebelum koma pertama
    s = re.sub(r"[^a-zA-Z ]", " ", s).lower()   # sisakan huruf dan spasi
    return " ".join(s.split()) or None


def slug(v, maxlen: int = 60) -> str:
    """Bentuk ID teks yang stabil dari nama, mis. 'Univ. Matana' -> 'UNIV_MATANA'.
    Dipakai sebagai natural key untuk nilai yang belum ada di kamus."""
    s = unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()
    return s[:maxlen] or "UNKNOWN"


# Singkatan bulan Indonesia & Inggris untuk membaca tanggal berbentuk teks (mis. '31des26')
_BULAN = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mei": 5, "may": 5, "jun": 6, "jul": 7,
    "agu": 8, "agt": 8, "ags": 8, "aug": 8, "sep": 9, "okt": 10, "oct": 10,
    "nov": 11, "des": 12, "dec": 12,
}


def parse_date(v) -> tuple[date | None, str]:
    """Baca nilai tanggal dari Excel TANPA menebak (aturan Tubes 3).

    Return (tanggal, flag):
        datetime asli Excel                       -> (tanggal, 'OK')
        teks jelas: '31des26', '31 Des 2026',
                    '2026-12-31'                  -> (tanggal, 'TEKS')
        hanya tahun, mis. 2026                    -> (None, 'HANYA_TAHUN')   # tidak diubah jadi 1 Jan
        kosong                                    -> (None, 'KOSONG')
        lainnya                                   -> (None, 'TIDAK_TERBACA')
    """
    if is_blank(v):
        return None, "KOSONG"
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.date(), "OK"
    if isinstance(v, date):
        return v, "OK"
    if isinstance(v, (int, float)):
        # angka di kolom tanggal biasanya cuma tahun (2026)
        return None, "HANYA_TAHUN" if 1900 <= v <= 2100 else "TIDAK_TERBACA"
    s = str(v).strip().lower()
    if re.fullmatch(r"\d{4}", s):
        return None, "HANYA_TAHUN"
    # pola hari + nama bulan + tahun, pemisah boleh spasi/-/./ atau tanpa pemisah
    m = re.fullmatch(r"(\d{1,2})[\s\-/.]*([a-z]{3,9})[\s\-/.]*(\d{2,4})", s)
    if m and m.group(2)[:3] in _BULAN:
        d, mon, y = int(m.group(1)), _BULAN[m.group(2)[:3]], int(m.group(3))
        y = y + 2000 if y < 100 else y          # tahun 2 digit: 26 -> 2026
        try:
            return date(y, mon, d), "TEKS"
        except ValueError:                      # mis. 31 Februari
            return None, "TIDAK_TERBACA"
    try:
        return datetime.strptime(s, "%Y-%m-%d").date(), "TEKS"
    except ValueError:
        return None, "TIDAK_TERBACA"


def date_key(d: date | None) -> int:
    """Ubah tanggal jadi key dim_tanggal (YYYYMMDD). Tanggal tidak diketahui -> 0."""
    return int(d.strftime("%Y%m%d")) if d else 0


# =============================================================================
# 5. LOAD HELPER
# =============================================================================
def connect(cfg: configparser.ConfigParser):
    """Buka koneksi PostgreSQL memakai bagian [postgres] di settings.ini.

    psycopg2 di-import di dalam fungsi supaya mode --no-db tetap jalan walau psycopg2 belum terpasang.
    """
    import psycopg2
    s = cfg["postgres"]
    return psycopg2.connect(
        host=s.get("host", "localhost"), port=s.getint("port", 5432),
        dbname=s.get("dbname"), user=s.get("user"), password=s.get("password", ""),
    )


def run_sql_file(conn, path: Path):
    """Jalankan seluruh isi satu file .sql (dipakai untuk membuat schema saat --init-db)."""
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))


def _py(v):
    """Ubah nilai pandas/numpy menjadi tipe Python biasa yang dimengerti psycopg2.

    NaN/NA/NaT -> None (jadi NULL di database), dict/list -> teks JSON (untuk kolom jsonb),
    numpy scalar -> int/float/bool Python.
    """
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if v is pd.NaT or v is pd.NA:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, default=str)
    if hasattr(v, "item"):
        return v.item()
    return v


def insert_df(conn, table: str, df: pd.DataFrame, returning: str | None = None):
    """INSERT semua baris DataFrame ke tabel sekaligus (batch 500 baris, lebih cepat dari satu-satu).

    Syarat: nama kolom DataFrame = nama kolom tabel di database.
    returning: opsional, mis. "penugasan_key" untuk mengambil key yang dibuat database.
    """
    from psycopg2.extras import execute_values
    if df.empty:
        return []
    cols = list(df.columns)
    rows = [tuple(_py(v) for v in r) for r in df.itertuples(index=False, name=None)]
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
    if returning:
        sql += f" RETURNING {returning}"
    with conn.cursor() as cur:
        res = execute_values(cur, sql, rows, page_size=500, fetch=bool(returning))
    return res or []


def save_staging(conn, batch: dict, raw_frames: list[pd.DataFrame]):
    """Simpan SEMUA baris mentah ke stg.raw_baris (landing zone).

    Baris lama milik KPI ini dihapus dulu, jadi run ulang tidak menggandakan data (idempotent).
    KPI lain tidak tersentuh karena difilter dengan kolom kpi.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stg.raw_baris WHERE kpi = %s", (batch["kpi"],))
    for df in raw_frames:
        out = pd.DataFrame({
            "kpi": batch["kpi"], "nama_file": df["_file"], "nama_sheet": df["_sheet"],
            "nomor_baris": df["_row"], "hash_baris": df["_hash"], "payload_asli": df["_payload"],
            "batch_id": batch["batch_id"], "waktu_ekstrak": batch["waktu_ekstrak"],
        })
        insert_df(conn, "stg.raw_baris", out)


def write_csv(tables: dict[str, pd.DataFrame], folder: Path):
    """Tulis setiap tabel hasil transform ke CSV (UTF-8 dengan BOM supaya rapi dibuka di Excel).

    CSV ini isinya sama dengan yang dimuat ke database, jadi bisa dipakai untuk
    upload ke BigQuery atau sebagai sumber Looker Studio.
    """
    folder.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        out = df.copy()
        for col in out.columns:
            # kolom berisi dict/list (mis. payload_asli) diubah ke teks JSON
            if out[col].map(lambda v: isinstance(v, (dict, list))).any():
                out[col] = out[col].map(lambda v: json.dumps(v, ensure_ascii=False, default=str))
        out.to_csv(folder / f"{name}.csv", index=False, encoding="utf-8-sig")
