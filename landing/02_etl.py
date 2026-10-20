# Databricks notebook source
# MAGIC %md
# MAGIC # Landing — ETL: registrar lotes
# MAGIC
# MAGIC Contrato: `landing/SPEC.md`. DDL previo: `landing/01_ddl.sql` (tiene que existir `sepa.landing.lotes`).
# MAGIC
# MAGIC **Qué hace:** toma los ZIP sueltos en la raíz del volume `raw` y para cada uno:
# MAGIC
# MAGIC 1. Lee la **fecha del dato** de adentro del ZIP (carpeta `yyyy-mm-dd/`).
# MAGIC 2. Calcula la **huella SHA-256**.
# MAGIC 3. Lo **mueve** a `sepa_minorista/fecha_datos=<fecha>/`.
# MAGIC 4. Escribe el **manifiesto** JSON al lado.
# MAGIC 5. `MERGE` en `sepa.landing.lotes`.
# MAGIC
# MAGIC **Qué no hace:** no crea tablas, no abre los CSV, no borra ZIP originales ya registrados
# MAGIC (solo una copia redundante de la raíz si el hash ya existe).
# MAGIC
# MAGIC Idempotente: correrlo dos veces deja todo igual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros
# MAGIC
# MAGIC - `volume_root`: raíz del volume de landing.
# MAGIC - `origen`: hoy `manual` (UI); el job de descarga pondrá `job`.

# COMMAND ----------

dbutils.widgets.text("volume_root", "/Volumes/sepa/landing/raw")
dbutils.widgets.text("origen", "manual")

VOLUME_ROOT = dbutils.widgets.get("volume_root").rstrip("/")
ORIGEN = dbutils.widgets.get("origen")
DESTINO = f"{VOLUME_ROOT}/sepa_minorista"
TABLA_LOTES = "sepa.landing.lotes"

print(f"volume_root = {VOLUME_ROOT}")
print(f"origen      = {ORIGEN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones auxiliares
# MAGIC
# MAGIC - `sha256_de(path)`: hash en bloques de 8 MB (no carga 320 MB de una).
# MAGIC - `fecha_interna(path)`: abre el ZIP sin extraerlo y saca la carpeta `yyyy-mm-dd/`.
# MAGIC - `dia_semana_de(nombre)`: `sepa_sabado.zip` → `sabado`.

# COMMAND ----------

import hashlib
import json
import os
import re
import shutil
import zipfile
from datetime import date, datetime, timezone

RE_FECHA = re.compile(r"^(\d{4}-\d{2}-\d{2})/")
RE_DIA = re.compile(r"sepa_(lunes|martes|miercoles|jueves|viernes|sabado|domingo)", re.IGNORECASE)


def sha256_de(path: str, bloque: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(bloque)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fecha_interna(path: str):
    try:
        with zipfile.ZipFile(path) as zf:
            fechas = {m.group(1) for n in zf.namelist() for m in [RE_FECHA.match(n)] if m}
    except (zipfile.BadZipFile, OSError):
        return None
    if len(fechas) != 1:
        return None
    return date.fromisoformat(fechas.pop())


def dia_semana_de(nombre: str):
    m = RE_DIA.search(nombre)
    return m.group(1).lower() if m else None


def ts_archivo(path: str) -> datetime:
    return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Qué ZIP hay en la raíz
# MAGIC
# MAGIC Solo la raíz del volume. `sepa_minorista/` y `referencia/` no se tocan.

# COMMAND ----------

pendientes = sorted(
    f for f in os.listdir(VOLUME_ROOT)
    if f.lower().endswith(".zip") and os.path.isfile(os.path.join(VOLUME_ROOT, f))
)
print(f"{len(pendientes)} ZIP en la raíz:")
for f in pendientes:
    print(f"  {f}  {os.path.getsize(os.path.join(VOLUME_ROOT, f)) / 1e6:,.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Registrar cada ZIP
# MAGIC
# MAGIC | Caso | Qué pasa |
# MAGIC |---|---|
# MAGIC | No se puede leer la fecha | `ilegible`, queda en la raíz |
# MAGIC | Mismo hash ya registrado | se quita la copia de la raíz |
# MAGIC | Misma fecha, otro hash | `rectificativo`, sufijo `__<hash8>` |
# MAGIC | Normal | se mueve a `fecha_datos=<fecha>/`, `registrado` |

# COMMAND ----------

ya_registrados = {r["lote_id"] for r in spark.table(TABLA_LOTES).select("lote_id").collect()}
fechas_registradas = {
    r["fecha_datos"] for r in spark.table(TABLA_LOTES).where("fecha_datos IS NOT NULL").select("fecha_datos").collect()
}
ahora = datetime.now(timezone.utc)
filas = []

for nombre in pendientes:
    origen_path = os.path.join(VOLUME_ROOT, nombre)
    sha = sha256_de(origen_path)
    fecha = fecha_interna(origen_path)
    base = dict(
        lote_id=sha, dia_semana=dia_semana_de(nombre), bytes=os.path.getsize(origen_path), sha256=sha,
        origen=ORIGEN, url_descarga=None, fecha_pagina=None, ts_llegada=ts_archivo(origen_path), ts_registro=ahora,
    )

    if sha in ya_registrados:
        os.remove(origen_path)
        print(f"[duplicado]     {nombre}: mismo hash ya registrado, se quita la copia de la raíz")
        continue

    if fecha is None:
        filas.append(dict(base, fecha_datos=None, archivo=nombre, ruta=origen_path, estado="ilegible"))
        print(f"[ilegible]      {nombre}: no se pudo leer la fecha interna, queda en la raíz")
        continue

    carpeta = f"{DESTINO}/fecha_datos={fecha.isoformat()}"
    os.makedirs(carpeta, exist_ok=True)
    stem = nombre[:-4]
    if fecha in fechas_registradas:
        estado, archivo_final = "rectificativo", f"{stem}__{sha[:8]}.zip"
    else:
        estado, archivo_final = "registrado", nombre
    destino_path = f"{carpeta}/{archivo_final}"

    shutil.move(origen_path, destino_path)
    fila = dict(base, fecha_datos=fecha, archivo=archivo_final, ruta=destino_path, estado=estado)
    filas.append(fila)
    fechas_registradas.add(fecha)

    with open(f"{carpeta}/{archivo_final[:-4]}.manifest.json", "w", encoding="utf-8") as f:
        json.dump({k: (v.isoformat() if isinstance(v, (date, datetime)) else v) for k, v in fila.items()},
                  f, ensure_ascii=False, indent=2)
    print(f"[{estado:<13}] {nombre} → fecha_datos={fecha}  ({fila['bytes'] / 1e6:,.1f} MB)")

print(f"\n{len(filas)} lotes para registrar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `MERGE` en `sepa.landing.lotes`
# MAGIC
# MAGIC Por `lote_id`: si el hash ya estaba, actualiza; si no, inserta.

# COMMAND ----------

from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("lote_id", T.StringType()), T.StructField("fecha_datos", T.DateType()),
    T.StructField("dia_semana", T.StringType()), T.StructField("archivo", T.StringType()),
    T.StructField("ruta", T.StringType()), T.StructField("bytes", T.LongType()),
    T.StructField("sha256", T.StringType()), T.StructField("origen", T.StringType()),
    T.StructField("url_descarga", T.StringType()), T.StructField("fecha_pagina", T.DateType()),
    T.StructField("ts_llegada", T.TimestampType()), T.StructField("ts_registro", T.TimestampType()),
    T.StructField("estado", T.StringType()),
])

if filas:
    spark.createDataFrame(filas, schema).createOrReplaceTempView("lotes_nuevos")
    spark.sql(f"""
        MERGE INTO {TABLA_LOTES} AS t
        USING lotes_nuevos AS s
        ON t.lote_id = s.lote_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    print("Nada nuevo para registrar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultado

# COMMAND ----------

display(
    spark.table(TABLA_LOTES)
    .selectExpr("fecha_datos", "dia_semana", "archivo", "round(bytes / 1e6, 1) AS mb", "estado", "origen",
                "ts_llegada", "left(sha256, 12) AS sha256_corto", "ruta")
    .orderBy("fecha_datos")
)
