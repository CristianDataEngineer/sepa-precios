# Databricks notebook source
# MAGIC %md
# MAGIC # EDA SEPA — paso 0: extraer y perfilar la muestra
# MAGIC
# MAGIC Toma los ZIP por comercio depositados en el volume `raw` (tal cual bajan de
# MAGIC datos.produccion.gob.ar), los abre, detecta encoding/BOM/footer de cada CSV,
# MAGIC normaliza a UTF-8 y los deja comprimidos (gzip) en el volume `eda` para leerlos con `read_files`.
# MAGIC
# MAGIC Python puro: solo I/O y perfilado de archivos, sin transformación tabular.
# MAGIC Los mismos pasos (unzip, detectar encoding, descartar footer) son los que hará landing → bronze.
# MAGIC
# MAGIC Nota: para la muestra del 2026-10-09 este notebook se corrió localmente (mismo código,
# MAGIC `RAW`/`EDA` apuntando a carpetas locales) porque el MCP no pudo lanzar serverless;
# MAGIC los `.csv.gz` resultantes se subieron al volume `eda`.

# COMMAND ----------

import csv
import gzip
import os
import re
import zipfile

RAW = "/Volumes/sepa/landing/raw/sepa_minorista/fecha_datos=2026-10-09"
EDA = "/Volumes/sepa/landing/eda/fecha_datos=2026-10-09"
PERFIL = "/Volumes/sepa/landing/eda/perfil_archivos_2026-10-09.csv"

# COMMAND ----------


def detect_encoding(b: bytes):
    """utf-8-sig si trae BOM, utf-8 si decodifica, cp1252 como fallback."""
    if b.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig", True
    try:
        b.decode("utf-8")
        return "utf-8", False
    except UnicodeDecodeError:
        return "cp1252", False


def es_footer(linea: str) -> bool:
    return linea.strip() == "" or linea.strip().lower().startswith(("ultima", "última"))


# COMMAND ----------

os.makedirs(EDA, exist_ok=True)
rows = []

for zname in sorted(os.listdir(RAW)):
    zpath = os.path.join(RAW, zname)
    m = re.search(r"comercio-sepa-(\d+)_", zname)
    cid = m.group(1) if m else "?"
    size = os.path.getsize(zpath)
    try:
        zf = zipfile.ZipFile(zpath)
        names = zf.namelist()
    except zipfile.BadZipFile as e:
        # ZIP corrupto o vacío: se registra y sigue. En el pipeline va a cuarentena.
        rows.append(dict(zip=zname, comercio=cid, zip_bytes=size, archivo="", bytes=0, encoding="", bom="",
                         crlf="", lineas=0, n_cols_header=0, header="", footer_lines=0, error=f"BadZipFile: {e}"))
        continue

    outdir = os.path.join(EDA, f"comercio_{cid}")
    os.makedirs(outdir, exist_ok=True)
    for n in names:
        raw = zf.read(n)
        enc, bom = detect_encoding(raw)
        txt = raw.decode(enc, errors="replace")
        lines = txt.splitlines()
        header = lines[0] if lines else ""
        n_footer = sum(1 for l in lines[-5:] if es_footer(l))
        with gzip.open(os.path.join(outdir, os.path.basename(n) + ".gz"), "wt", encoding="utf-8", newline="\n") as f:
            f.write(txt)
        rows.append(dict(zip=zname, comercio=cid, zip_bytes=size, archivo=os.path.basename(n), bytes=len(raw),
                         encoding=enc, bom=bom, crlf="\r\n" in txt[:5000], lineas=len(lines),
                         n_cols_header=header.count("|") + 1, header=header, footer_lines=n_footer, error=""))

with open(PERFIL, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

# COMMAND ----------

print(f"{'com':>3} {'archivo':<15} {'bytes':>11} {'enc':<9} {'bom':<5} {'crlf':<5} {'lineas':>9} {'cols':>4} {'foot':>4} error")
for r in rows:
    print(f"{r['comercio']:>3} {r['archivo']:<15} {r['bytes']:>11} {r['encoding']:<9} {str(r['bom']):<5} "
          f"{str(r['crlf']):<5} {r['lineas']:>9} {r['n_cols_header']:>4} {r['footer_lines']:>4} {r['error']}")

print("\n--- Headers distintos por archivo ---")
for a in ["comercio.csv", "sucursales.csv", "productos.csv"]:
    seen = {}
    for r in rows:
        if r["archivo"] == a:
            seen.setdefault(r["header"], []).append(r["comercio"])
    for h, cs in seen.items():
        print(f"[{a}] comercios {cs}: {h[:120]}...")
