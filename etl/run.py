"""
run.py — pintu masuk (entry point) ETL. Semua KPI dijalankan lewat file ini.

Cara pakai (dari folder Tubes_5_ETL):
    python -m etl.run --kpi k1 --init-db     # pertama kali: buat schema stg + k1, lalu jalankan ETL
    python -m etl.run --kpi k1               # run berikutnya: full refresh (hasil sama walau diulang)
    python -m etl.run --kpi k1 --no-db       # tanpa database: hasil transform disimpan ke CSV saja

Yang dilakukan main():
    [1/4] EXTRACT   -> modul KPI membaca Excel
    [2/4] TRANSFORM -> modul KPI membentuk tabel dimensi, fakta, audit; hasil ditulis ke output/<kpi>/*.csv
    [3/4] LOAD      -> simpan staging + muat ke PostgreSQL dalam SATU transaksi
    [4/4] VALIDATE  -> cek penerimaan; kalau ada yang gagal, semua perubahan di-rollback
    Terakhir menulis output/<kpi>/laporan_validasi_<kpi>.md dan mencatat run di stg.etl_log.

Folder ini hanya untuk K1. run.py dibuat generik, jadi KPI lain bisa didaftarkan di MODULES bila diperlukan.
"""
from __future__ import annotations

import argparse        # membaca argumen command line (--kpi, --init-db, ...)
import importlib       # memuat modul KPI berdasarkan nama (mis. "etl.k1_sk_st")
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import common as c

# Daftar KPI yang bisa dijalankan: kode -> (nama modul Python, file DDL di folder sql/)
MODULES = {
    "k1": ("etl.k1_sk_st", "01_k1_schema.sql"),          # Rudi
    # K2–K5 dikerjakan PIC masing-masing (Aldave, Yayang, Zidan, Adrian), tidak termasuk folder ini.
}


def tulis_laporan(path: Path, batch, raw, tables, info, cek, ringkas):
    """Tulis laporan validasi (Markdown) berisi: jumlah baris per tabel, hasil cek,
    angka contoh metrik, daftar baris yang perlu ditinjau, dan catatan transformasi."""
    s = info.get("staging")
    L = [f"# Laporan validasi ETL {batch['kpi'].upper()}", "",
         f"- batch_id: `{batch['batch_id']}`", f"- waktu: {batch['waktu_ekstrak']}"]
    # Jumlah baris tiap sumber utama (semua DataFrame di raw, kecuali yang diawali '_')
    for k, v in raw.items():
        if isinstance(v, pd.DataFrame) and not k.startswith("_"):
            L.append(f"- baris sumber `{k}`: {len(v)} ({v['_file'].iat[0] if len(v) else '-'})")
    lain = sorted({d['_file'].iat[0] for d in raw.get('_staging_lain', []) if len(d)})
    if lain:
        L.append("- workbook lain di staging (belum ditransform): " + ", ".join(lain))
    L.append("")
    # Bagian 1: ukuran tiap tabel hasil
    L += ["## Jumlah baris per tabel", "", "| tabel | baris |", "|---|---|"]
    L += [f"| {k} | {len(v)} |" for k, v in tables.items()]
    # Bagian 2: hasil cek penerimaan (hanya ada bila load ke database)
    if cek:
        L += ["", "## Cek penerimaan", "", "| cek | hasil | detail |", "|---|---|---|"]
        L += [f"| {n} | {'LULUS' if ok else 'GAGAL'} | {d} |" for n, ok, d in cek]
    # Bagian 3: angka contoh metrik dari database
    for k, df in (ringkas or {}).items():
        L += ["", f"## {k}", "", df.to_markdown(index=False)]
    # Bagian 4: baris sumber yang tidak dimuat apa adanya
    au = tables["audit_baris_sumber"]
    rev = au[au.keputusan.isin(["PERLU_REVIEW", "DIGABUNG", "DUPLIKAT"])]
    # kolom FK ke fakta di tabel audit (K1: penugasan_key); dicari otomatis supaya berlaku untuk semua KPI
    key_col = next((col for col in au.columns if col.endswith("_key") and col != "audit_key"), None)
    L += ["", "## Baris yang perlu dilihat manusia (PERLU_REVIEW / DIGABUNG / DUPLIKAT)", "",
          f"| baris Excel | sheet | keputusan | {key_col or 'fakta'} | alasan |", "|---|---|---|---|---|"]
    for _, r in rev.sort_values("nomor_baris").iterrows():
        kv = r[key_col] if key_col else None
        kv = "" if kv is None or pd.isna(kv) else kv
        L.append(f"| {r['nomor_baris']} | {r['nama_sheet']} | {r['keputusan']} | {kv} | {r['alasan']} |")
    # Bagian 5: surat yang mungkin dibatalkan (menunggu konfirmasi) — khusus KPI yang menyediakannya
    kb = info.get("kandidat_batal")
    if kb is not None and len(kb):
        L += ["", "## Kandidat BATAL (menunggu konfirmasi Sekpim, saat ini tetap TERCATAT)", "",
              kb.to_markdown(index=False)]
    # Bagian 6: catatan transformasi per baris (info saja)
    if s is not None and "catatan" in s.columns:
        cat = s[s.catatan.map(len) > 0]
        L += ["", "## Catatan transformasi (info, tidak mengubah status)", "", "| baris Excel | catatan |", "|---|---|"]
        L += [f"| {row} | {'; '.join(ct)} |" for row, ct in zip(cat['_row'], cat['catatan'])]
    # Bagian 7: hal yang perlu ditindaklanjuti
    if info.get("catatan"):
        L += ["", "## Hal yang perlu ditindaklanjuti", ""] + [f"- {x}" for x in info["catatan"]]
    path.write_text("\n".join(L) + "\n", encoding="utf-8")


def kumpulkan_raw(raw: dict) -> list[pd.DataFrame]:
    """Ambil SEMUA DataFrame mentah dari hasil extract() untuk disimpan ke staging.

    Berlaku untuk semua KPI: nilai dict boleh berupa DataFrame (mis. raw["rincian"], raw["utama"])
    atau list DataFrame (mis. raw["_staging_lain"]). Nama key bebas, jadi run.py tidak perlu tahu
    struktur data tiap KPI.
    """
    frames = []
    for v in raw.values():
        if isinstance(v, pd.DataFrame):
            frames.append(v)
        elif isinstance(v, (list, tuple)):
            frames.extend(x for x in v if isinstance(x, pd.DataFrame))
    return frames


def _catat_log(conn, batch_id: str, status: str, ringkasan: dict):
    """Perbarui status run di stg.etl_log. Dibungkus try supaya error pencatatan log
    tidak menutupi error utama (mis. saat schema stg belum pernah dibuat)."""
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE stg.etl_log SET selesai=%s, status=%s, ringkasan=%s WHERE batch_id=%s",
                        (datetime.now(), status, json.dumps(ringkasan), batch_id))
        conn.commit()
    except Exception:
        conn.rollback()


def main(argv=None):
    """Jalankan ETL satu KPI dari awal sampai akhir.

    Urutan: baca argumen -> extract -> transform -> tulis CSV -> (bila tidak --no-db) buat schema
    (bila --init-db), simpan staging, load, validate dalam satu transaksi -> commit atau rollback
    -> catat di stg.etl_log -> tulis laporan validasi Markdown.
    argv: daftar argumen (None = ambil dari command line); berguna untuk pengujian.
    """
    # ---------- Argumen command line ----------
    ap = argparse.ArgumentParser(description="ETL Data Warehouse Sekpim FIF")
    ap.add_argument("--kpi", required=True, choices=sorted(MODULES), help="KPI yang dijalankan, mis. k1")
    ap.add_argument("--init-db", action="store_true", help="buat ulang schema KPI (data lama di schema itu hilang)")
    ap.add_argument("--no-db", action="store_true", help="tanpa database, hasil hanya CSV")
    ap.add_argument("--dataset-dir", help="override folder dataset")
    a = ap.parse_args(argv)

    # ---------- Persiapan ----------
    cfg = c.load_settings()                                         # baca settings.ini
    ds = Path(a.dataset_dir) if a.dataset_dir else c.dataset_dir(cfg)
    mod_name, schema_file = MODULES[a.kpi]
    mod = importlib.import_module(mod_name)                         # muat modul KPI, mis. etl.k1_sk_st
    batch = c.new_batch(a.kpi)                                      # ID run ini
    out_dir = c.OUTPUT_DIR / a.kpi                                  # output/k1/
    print(f"[{a.kpi}] batch {batch['batch_id']} | dataset: {ds}")

    # ---------- EXTRACT & TRANSFORM (tidak butuh database) ----------
    print("[1/4] EXTRACT ...")
    raw = mod.extract(ds)
    print("[2/4] TRANSFORM ...")
    tables, info = mod.transform(raw, batch)
    for k, v in tables.items():
        print(f"      {k:<24} {len(v):>5} baris")
    c.write_csv(tables, out_dir)                                    # hasil transform selalu disimpan ke CSV
    if "kegiatan_alasan" in info:
        info["kegiatan_alasan"].to_csv(out_dir / "_cek_klasifikasi_kegiatan.csv", index=False, encoding="utf-8-sig")

    # ---------- LOAD & VALIDATE (butuh database) ----------
    cek, ringkas = [], {}
    if a.no_db:
        print("[3/4] LOAD dilewati (--no-db). CSV ada di", out_dir)
    else:
        conn = c.connect(cfg)
        try:
            if a.init_db:
                # Buat schema staging (aman diulang) + schema KPI (DROP lalu CREATE ulang)
                print("      init schema stg +", schema_file)
                c.run_sql_file(conn, c.SQL_DIR / "00_staging.sql")
                c.run_sql_file(conn, c.SQL_DIR / schema_file)
                conn.commit()
            # Catat bahwa run ini dimulai
            with conn.cursor() as cur:
                cur.execute("INSERT INTO stg.etl_log VALUES (%s,%s,%s,NULL,'BERJALAN',NULL)",
                            (batch["batch_id"], a.kpi, batch["waktu_ekstrak"]))
            conn.commit()
            # Mulai SATU transaksi: staging + load + validate. Belum di-commit sampai semua cek lulus.
            print("[3/4] LOAD (1 transaksi) ...")
            c.save_staging(conn, batch, kumpulkan_raw(raw))         # semua sheet mentah -> stg.raw_baris
            mod.load(conn, tables)
            print("[4/4] VALIDATE ...")
            cek = mod.validate(conn, tables, info, raw)
            gagal = [n for n, ok, _ in cek if not ok]
            if gagal:
                raise RuntimeError("Cek validasi gagal: " + "; ".join(gagal))
            ringkas = mod.ringkasan_sql(conn)
            conn.commit()                                           # semua lulus -> simpan permanen
            _catat_log(conn, batch["batch_id"], "SUKSES", {k: len(v) for k, v in tables.items()})
            for n, ok, d in cek:
                print(f"      [{'LULUS' if ok else 'GAGAL'}] {n} ({d})")
        except Exception as e:
            conn.rollback()                                         # batalkan semua perubahan run ini
            _catat_log(conn, batch["batch_id"], "GAGAL", {"error": str(e)})
            print("GAGAL, semua perubahan di-rollback:", e)
            raise
        finally:
            conn.close()

    # ---------- Laporan ----------
    lap = out_dir / f"laporan_validasi_{a.kpi}.md"
    tulis_laporan(lap, batch, raw, tables, info, cek, ringkas)
    print("Selesai. Laporan:", lap)


if __name__ == "__main__":
    sys.exit(main())
