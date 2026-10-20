# Databricks notebook source
# MAGIC %md
# MAGIC # Landing — descargar SEPA
# MAGIC
# MAGIC Contrato: `landing/SPEC.md`. DDL previo: `landing/01_ddl.sql`.
# MAGIC
# MAGIC **Qué hace:** lee la ficha de los 7 ZIP (`sepa_lunes.zip` … `sepa_domingo.zip`).
# MAGIC Si el tamaño y la fecha de subida no cambiaron, no baja ese archivo.
# MAGIC Si cambiaron, lo baja, le calcula el hash y lo registra en `sepa.landing.lotes` con `origen = job`.
# MAGIC
# MAGIC 1. La ficha del portal (tamaño + `last_modified`) decide si se descarga. No es la fecha del dato.
# MAGIC 2. La fecha del dato sale de adentro del ZIP (carpeta `yyyy-mm-dd/`). La fecha de la web es solo informativa.
# MAGIC 3. El `sha256` es el `lote_id`. Si ese hash ya está en `lotes`, se **borra la copia recién bajada**.
# MAGIC 4. Si el hash es nuevo, el ZIP va a `sepa_minorista/fecha_datos=<fecha>/`. No pisa un archivo que ya esté ahí.
# MAGIC 5. Misma fecha y otro hash: `sepa_<dia>__<hash8>.zip` con `estado = rectificativo`.
# MAGIC
# MAGIC **Qué no hace:** no abre los CSV, no toca Bronze, Silver ni Gold, no borra un ZIP ya registrado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

dbutils.widgets.text("volume_root", "/Volumes/sepa/landing/raw")
dbutils.widgets.text("api_base", "https://datos.produccion.gob.ar")
dbutils.widgets.text("dataset_id", "sepa-precios")

VOLUME_ROOT = dbutils.widgets.get("volume_root").rstrip("/")
API_BASE = dbutils.widgets.get("api_base").rstrip("/")
DATASET_ID = dbutils.widgets.get("dataset_id").strip()
DESTINO = f"{VOLUME_ROOT}/sepa_minorista"
PORTAL = f"{DESTINO}/_portal.json"
TABLA_LOTES = "sepa.landing.lotes"
ORIGEN = "job"

print(f"volume_root = {VOLUME_ROOT}")
print(f"dataset     = {DATASET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones
# MAGIC
# MAGIC La bajada va a `/tmp` (disco de la corrida, no el volume). Al volume solo entra un ZIP con hash nuevo.

# COMMAND ----------

import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import urllib.request
import zipfile
from datetime import date, datetime, timezone

RE_FECHA = re.compile(r"^(\d{4}-\d{2}-\d{2})/")
RE_FECHA_TEXTO = re.compile(r"(\d{4}-\d{2}-\d{2})")
RE_ZIP = re.compile(r"sepa_(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\.zip$", re.IGNORECASE)
ORDEN = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]
BLOQUE = 8 * 1024 * 1024


def sha256_de(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(BLOQUE)
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


def fecha_de_texto(texto):
    if not texto:
        return None
    m = RE_FECHA_TEXTO.search(texto)
    return date.fromisoformat(m.group(1)) if m else None


def traer_json(url: str, intentos: int = 5):
    ultimo = None
    for i in range(intentos):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "sepa-diario/1.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            ultimo = e
            print(f"[reintento]    ficha del portal, intento {i + 1}: {e}", flush=True)
            time.sleep(10 * (i + 1))
    raise ultimo


def descargar(url: str, destino: str, intentos: int = 3) -> str:
    ultimo = None
    for i in range(intentos):
        if os.path.exists(destino):
            os.remove(destino)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "sepa-diario/1.0"})
            h = hashlib.sha256()
            total = 0
            with urllib.request.urlopen(req, timeout=300) as resp, open(destino, "wb") as out:
                esperado = resp.headers.get("Content-Length")
                while True:
                    chunk = resp.read(BLOQUE)
                    if not chunk:
                        break
                    h.update(chunk)
                    out.write(chunk)
                    total += len(chunk)
            if total == 0:
                raise IOError("archivo vacio")
            if esperado is not None and total != int(esperado):
                raise IOError(f"descarga corta: {total} de {esperado}")
            return h.hexdigest()
        except Exception as e:
            ultimo = e
            if os.path.exists(destino):
                os.remove(destino)
            print(f"[reintento]    {url.rsplit('/', 1)[-1]}, intento {i + 1}: {e}", flush=True)
            time.sleep(10 * (i + 1))
    raise ultimo


def hash_si_existe(path: str):
    if not os.path.isfile(path):
        return None
    return sha256_de(path)


def tamano(valor):
    if valor is None or valor == "":
        return None
    return int(valor)


def leer_portal():
    if not os.path.isfile(PORTAL):
        return {}
    with open(PORTAL, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def guardar_portal(estado):
    os.makedirs(os.path.dirname(PORTAL), exist_ok=True)
    tmp = PORTAL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PORTAL)


def ficha_sin_cambios(recurso, portal) -> bool:
    prev = portal.get(recurso["url"])
    if not prev or recurso.get("bytes") is None or not recurso.get("last_modified"):
        return False
    return prev.get("bytes") == recurso["bytes"] and prev.get("last_modified") == recurso["last_modified"]


def recordar_portal(portal, recurso, sha):
    portal[recurso["url"]] = {
        "last_modified": recurso.get("last_modified"),
        "bytes": recurso.get("bytes"),
        "sha256": sha,
    }
    guardar_portal(portal)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Los 7 recursos del portal
# MAGIC
# MAGIC Solo los ZIP `sepa_<dia>.zip`. El Excel de provincias y el PDF de metadata no se bajan.

# COMMAND ----------

api = f"{API_BASE}/api/3/action/package_show?id={DATASET_ID}"
paquete = traer_json(api)
if not paquete.get("success"):
    raise RuntimeError(f"CKAN no devolvio el dataset {DATASET_ID}")

recursos = []
for r in paquete["result"]["resources"]:
    nombre = r.get("url", "").rstrip("/").split("/")[-1]
    m = RE_ZIP.search(nombre)
    if not m:
        continue
    recursos.append({
        "dia": m.group(1).lower(),
        "archivo": nombre,
        "url": r["url"],
        "fecha_pagina": fecha_de_texto(r.get("description")),
        "bytes": tamano(r.get("size")),
        "last_modified": r.get("last_modified"),
    })
recursos.sort(key=lambda r: ORDEN.index(r["dia"]))

print(f"{len(recursos)} ZIP en el portal:")
for r in recursos:
    print(f"  {r['archivo']}  {r['bytes']} bytes  subido={r['last_modified']}  fecha_pagina={r['fecha_pagina']}")
if len(recursos) != 7:
    raise RuntimeError(f"se esperaban 7 ZIP diarios y llegaron {len(recursos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bajar solo lo que la ficha dice que cambió
# MAGIC
# MAGIC `_portal.json` recuerda tamaño y fecha de subida de cada URL. Si coinciden, ese ZIP no se baja.
# MAGIC La primera corrida no tiene ese recuerdo, así que baja los 7 y deja el archivo para la próxima.
# MAGIC Un ZIP ya registrado no se vuelve a copiar al volume. Si el contenido cambió, entra al lado como rectificativo.

# COMMAND ----------

ya_registrados = {r["lote_id"] for r in spark.table(TABLA_LOTES).select("lote_id").collect()}
fechas_registradas = {
    r["fecha_datos"] for r in spark.table(TABLA_LOTES).where("fecha_datos IS NOT NULL").select("fecha_datos").collect()
}
ahora = datetime.now(timezone.utc)
filas = []
errores = []
portal = leer_portal()
temp_dir = tempfile.mkdtemp(prefix="sepa_descarga_")

for recurso in recursos:
    dia = recurso["dia"]
    if ficha_sin_cambios(recurso, portal):
        print(f"[sin cambios]   {recurso['archivo']}: misma ficha en el portal, no se baja")
        continue

    temp = os.path.join(temp_dir, recurso["archivo"])
    print(f"\nbajando {recurso['archivo']} ...", flush=True)
    try:
        sha = descargar(recurso["url"], temp)
    except Exception as e:
        errores.append(f"{recurso['archivo']}: {e}")
        print(f"[error]         {recurso['archivo']}: {e}")
        continue

    if sha in ya_registrados:
        os.remove(temp)
        recordar_portal(portal, recurso, sha)
        print(f"[duplicado]     {recurso['archivo']}: mismo hash ya registrado, se borra la copia bajada")
        continue

    fecha = fecha_interna(temp)
    base = dict(
        lote_id=sha, dia_semana=dia, bytes=os.path.getsize(temp), sha256=sha,
        origen=ORIGEN, url_descarga=recurso["url"], fecha_pagina=recurso["fecha_pagina"],
        ts_registro=ahora,
    )

    if fecha is None:
        carpeta = f"{DESTINO}/_ilegibles"
        preferido = recurso["archivo"]
        estado = "ilegible"
    else:
        carpeta = f"{DESTINO}/fecha_datos={fecha.isoformat()}"
        preferido = f"sepa_{dia}.zip"
        estado = "rectificativo" if fecha in fechas_registradas else "registrado"

    os.makedirs(carpeta, exist_ok=True)
    destino_preferido = os.path.join(carpeta, preferido)
    sha_existente = hash_si_existe(destino_preferido)

    if sha_existente == sha:
        os.remove(temp)
        print(f"[duplicado]     {recurso['archivo']}: ya esta en {destino_preferido}, se borra la copia bajada")
        recordar_portal(portal, recurso, sha)
        if sha not in ya_registrados:
            fila = dict(base, fecha_datos=fecha, archivo=preferido, ruta=destino_preferido, estado=estado,
                        ts_llegada=datetime.fromtimestamp(os.path.getmtime(destino_preferido), tz=timezone.utc))
            filas.append(fila)
            ya_registrados.add(sha)
        continue

    if estado == "registrado" and sha_existente is None:
        archivo_final = preferido
    else:
        estado = "ilegible" if fecha is None else "rectificativo"
        archivo_final = f"sepa_{dia}__{sha[:8]}.zip"
        if hash_si_existe(os.path.join(carpeta, archivo_final)) not in (None, sha):
            archivo_final = f"sepa_{dia}__{sha}.zip"

    destino_path = os.path.join(carpeta, archivo_final)
    if os.path.isfile(destino_path):
        os.remove(temp)
        if hash_si_existe(destino_path) != sha:
            errores.append(f"{recurso['archivo']}: {destino_path} existe con otro contenido y no se pisa")
            print(f"[error]         {recurso['archivo']}: no se pisa {destino_path}")
            continue
        recordar_portal(portal, recurso, sha)
        if sha not in ya_registrados:
            fila = dict(base, fecha_datos=fecha, archivo=archivo_final, ruta=destino_path, estado=estado,
                        ts_llegada=datetime.fromtimestamp(os.path.getmtime(destino_path), tz=timezone.utc))
            filas.append(fila)
            ya_registrados.add(sha)
        print(f"[duplicado]     {recurso['archivo']}: {archivo_final} ya estaba, no se pisa")
        continue

    shutil.move(temp, destino_path)
    fila = dict(
        base, fecha_datos=fecha, archivo=archivo_final, ruta=destino_path, estado=estado,
        ts_llegada=datetime.fromtimestamp(os.path.getmtime(destino_path), tz=timezone.utc),
    )
    manifiesto = os.path.join(carpeta, archivo_final[:-4] + ".manifest.json")
    if not os.path.exists(manifiesto):
        with open(manifiesto, "w", encoding="utf-8") as f:
            json.dump(
                {k: (v.isoformat() if isinstance(v, (date, datetime)) else v) for k, v in fila.items()},
                f, ensure_ascii=False, indent=2,
            )
    filas.append(fila)
    ya_registrados.add(sha)
    if fecha is not None:
        fechas_registradas.add(fecha)
    recordar_portal(portal, recurso, sha)
    print(f"[{estado:<13}] {recurso['archivo']} → {destino_path}  ({fila['bytes'] / 1e6:,.1f} MB)")

shutil.rmtree(temp_dir, ignore_errors=True)
print(f"\n{len(filas)} lotes para registrar, {len(errores)} errores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `MERGE` en `sepa.landing.lotes`
# MAGIC
# MAGIC Solo inserta hashes nuevos. Una fila que ya existe no se actualiza.

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
    spark.createDataFrame(filas, schema).createOrReplaceTempView("lotes_descarga")
    spark.sql(f"""
        MERGE INTO {TABLA_LOTES} AS t
        USING lotes_descarga AS s
        ON t.lote_id = s.lote_id
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    print("Nada nuevo para registrar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultado
# MAGIC
# MAGIC Si algún ZIP no se pudo bajar, la tarea falla después de guardar los que sí llegaron.
# MAGIC La próxima corrida no los vuelve a copiar.

# COMMAND ----------

display(
    spark.table(TABLA_LOTES)
    .selectExpr("fecha_datos", "dia_semana", "archivo", "round(bytes / 1e6, 1) AS mb", "estado", "origen",
                "fecha_pagina", "left(sha256, 12) AS sha256_corto", "ruta")
    .orderBy("fecha_datos")
)

if errores:
    raise RuntimeError("descarga incompleta: " + " | ".join(errores))
