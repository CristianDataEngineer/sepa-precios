# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ETL: cargar lotes pendientes
# MAGIC
# MAGIC Contrato: `bronze/SPEC.md`. DDL previo: `bronze/01_ddl.sql`.
# MAGIC
# MAGIC **Qué hace:** mira `sepa.landing.lotes` y carga **todo lo que todavía no está** en
# MAGIC `sepa.bronze.lotes_procesados`. Extrae los CSV y los escribe en `comercio` /
# MAGIC `sucursales` / `productos` (STRING). Archivos ilegibles → cuarentena.
# MAGIC
# MAGIC Así funciona el Job: Landing registra un ZIP nuevo → este notebook carga solo ese día.
# MAGIC Hoy, con 6 días sin cargar, un Run all los procesa todos (el 09/10 ya está, lo salta).
# MAGIC
# MAGIC **Qué no hace:** no crea tablas, no corrige precios ni EAN, no descarga ZIP (eso es Landing).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros
# MAGIC
# MAGIC - `fecha_datos`: vacío = todos los pendientes (modo Job). Un día `2026-10-08` = solo ese.
# MAGIC - `umbral_parse`: % mínimo de filas con `id_comercio` numérico (default 80 %).
# MAGIC - `forzar`: `true` recarga aunque el lote ya esté procesado (solo tiene efecto si hay `fecha_datos`).

# COMMAND ----------

dbutils.widgets.text("fecha_datos", "")
dbutils.widgets.text("umbral_parse", "0.80")
dbutils.widgets.text("forzar", "false")

FECHA_FILTRO = dbutils.widgets.get("fecha_datos").strip()
UMBRAL = float(dbutils.widgets.get("umbral_parse"))
FORZAR = dbutils.widgets.get("forzar").strip().lower() in ("true", "1", "yes")
EXTRACT_ROOT = "/Volumes/sepa/bronze/extract"
TABLA_LOTES = "sepa.landing.lotes"

print(f"fecha_datos  = {FECHA_FILTRO or '(pendientes)'}")
print(f"umbral_parse = {UMBRAL}")
print(f"forzar       = {FORZAR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Qué lotes faltan
# MAGIC
# MAGIC Un lote por `fecha_datos` (si hay rectificativo, el de `ts_registro` más reciente).
# MAGIC Se excluyen los `lote_id` que ya están en `lotes_procesados`, salvo `forzar=true` con una fecha puntual.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

filtro_fecha = f"AND l.fecha_datos = DATE('{FECHA_FILTRO}')" if FECHA_FILTRO else ""
anti_join = "" if (FORZAR and FECHA_FILTRO) else """
  AND NOT EXISTS (
    SELECT 1 FROM sepa.bronze.lotes_procesados p WHERE p.lote_id = l.lote_id
  )
"""

pendientes_df = spark.sql(f"""
SELECT l.lote_id, l.fecha_datos, l.archivo, l.ruta, l.bytes, l.estado, l.ts_registro
FROM {TABLA_LOTES} l
WHERE l.estado IN ('registrado', 'rectificativo')
  AND l.fecha_datos IS NOT NULL
  {filtro_fecha}
  {anti_join}
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.fecha_datos ORDER BY l.ts_registro DESC) = 1
ORDER BY l.fecha_datos
""")
pendientes = pendientes_df.collect()
print(f"{len(pendientes)} lote(s) a cargar:")
for r in pendientes:
    print(f"  {r['fecha_datos']}  {r['archivo']}  {r['bytes'] / 1e6:,.1f} MB")

if not pendientes:
    dbutils.notebook.exit("Nada pendiente. Landing no tiene lotes nuevos (o ya estan todos en Bronze).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extraer + parsear + cargar (un día por vuelta)
# MAGIC
# MAGIC Misma lógica que antes, adentro de un `for`. Si un día falla, los que ya se cargaron quedan;
# MAGIC los siguientes no se procesan (el Job se pone rojo y se reintenta: los hechos no se duplican).

# COMMAND ----------

import gzip
import io
import os
import re
import shutil
import zipfile

RE_COMERCIO = re.compile(r"comercio-sepa-(\d+)", re.IGNORECASE)
CSV_ESPERADOS = ("comercio.csv", "sucursales.csv", "productos.csv")
COLS = {
    "comercio": [
        "id_comercio", "id_bandera", "comercio_cuit", "comercio_razon_social",
        "comercio_bandera_nombre", "comercio_bandera_url", "comercio_ultima_actualizacion",
        "comercio_version_sepa",
    ],
    "sucursales": [
        "id_comercio", "id_bandera", "id_sucursal", "sucursales_nombre", "sucursales_tipo",
        "sucursales_calle", "sucursales_numero", "sucursales_latitud", "sucursales_longitud",
        "sucursales_observaciones", "sucursales_barrio", "sucursales_codigo_postal",
        "sucursales_localidad", "sucursales_provincia", "sucursales_lunes_horario_atencion",
        "sucursales_martes_horario_atencion", "sucursales_miercoles_horario_atencion",
        "sucursales_jueves_horario_atencion", "sucursales_viernes_horario_atencion",
        "sucursales_sabado_horario_atencion", "sucursales_domingo_horario_atencion",
    ],
    "productos": [
        "id_comercio", "id_bandera", "id_sucursal", "id_producto", "productos_ean",
        "productos_descripcion", "productos_cantidad_presentacion",
        "productos_unidad_medida_presentacion", "productos_marca", "productos_precio_lista",
        "productos_precio_referencia", "productos_cantidad_referencia",
        "productos_unidad_medida_referencia", "productos_precio_unitario_promo1",
        "productos_leyenda_promo1", "productos_precio_unitario_promo2", "productos_leyenda_promo2",
    ],
}
schema_c = StructType([
    StructField("lote_id", StringType()), StructField("fecha_datos", StringType()),
    StructField("id_comercio_zip", StringType()), StructField("archivo", StringType()),
    StructField("motivo", StringType()), StructField("detalle", StringType()),
    StructField("ruta_origen", StringType()), StructField("ts_cuarentena", TimestampType()),
])


def extraer_zip(ruta_zip, fecha, lote_id):
    extract_dir = f"{EXTRACT_ROOT}/fecha_datos={fecha}"
    ahora = datetime.now(timezone.utc)
    cuarentena_py = []
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(ruta_zip) as outer:
        inners = [n for n in outer.namelist() if n.lower().endswith(".zip")]
        print(f"  {len(inners)} ZIP de comercio")
        for inner_name in sorted(inners):
            m = RE_COMERCIO.search(inner_name)
            cid = m.group(1) if m else "desconocido"
            dest = os.path.join(extract_dir, f"comercio_{cid}")
            os.makedirs(dest, exist_ok=True)
            try:
                raw_inner = outer.read(inner_name)
                if len(raw_inner) == 0:
                    raise zipfile.BadZipFile("ZIP vacio (0 bytes)")
                inner = zipfile.ZipFile(io.BytesIO(raw_inner))
                names = {os.path.basename(n).lower(): n for n in inner.namelist() if not n.endswith("/")}
            except zipfile.BadZipFile as e:
                cuarentena_py.append(dict(
                    lote_id=lote_id, fecha_datos=fecha, id_comercio_zip=cid,
                    archivo=os.path.basename(inner_name), motivo="zip_corrupto", detalle=str(e),
                    ruta_origen=ruta_zip, ts_cuarentena=ahora,
                ))
                print(f"    [zip_corrupto] comercio {cid}: {e}")
                continue
            faltan = [c for c in CSV_ESPERADOS if c not in names]
            if faltan:
                inner.close()
                cuarentena_py.append(dict(
                    lote_id=lote_id, fecha_datos=fecha, id_comercio_zip=cid, archivo=",".join(faltan),
                    motivo="csv_ausente", detalle=f"presentes={sorted(names)}", ruta_origen=ruta_zip,
                    ts_cuarentena=ahora,
                ))
                print(f"    [csv_ausente]  comercio {cid}: faltan {faltan}")
                continue
            for csv_name in CSV_ESPERADOS:
                with inner.open(names[csv_name]) as src, gzip.open(os.path.join(dest, csv_name + ".gz"), "wb") as dst:
                    shutil.copyfileobj(src, dst)
            inner.close()
            print(f"    [ok]           comercio {cid}")
    return extract_dir, cuarentena_py


def cargar_lote(lote):
    lote_id = lote["lote_id"]
    fecha = lote["fecha_datos"].isoformat()
    print(f"\n=== {fecha}  {lote['archivo']} ===")
    extract_dir, cuarentena_py = extraer_zip(lote["ruta"], fecha, lote_id)

    stats_rows = []
    for entidad in ("comercio", "sucursales", "productos"):
        glob = f"{extract_dir}/*/{entidad}.csv.gz"
        if not any(f"{entidad}.csv.gz" in files for _, _, files in os.walk(extract_dir)):
            print(f"  {entidad}: no hay CSV extraidos")
            continue
        spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW src_{entidad} AS
        SELECT *, _metadata.file_path AS source_path
        FROM read_files('{glob}', format => 'csv', sep => '|', header => true, inferSchema => false)
        """)
        st = spark.sql(f"""
        SELECT source_path,
               regexp_extract(source_path, 'comercio_(\\\\d+)/', 1) AS id_comercio_zip,
               '{entidad}.csv' AS archivo,
               count(*) AS filas,
               count_if(id_comercio RLIKE '^[0-9]+$') AS validas,
               count_if(id_comercio RLIKE '(?i)ltima actualizaci') AS footer,
               count_if(
                 NOT (id_comercio RLIKE '^[0-9]+$')
                 AND NOT (coalesce(id_comercio, '') RLIKE '(?i)ltima actualizaci')
               ) AS basura,
               max(CASE WHEN id_comercio RLIKE '(?i)ltima actualizaci' THEN id_comercio END) AS footer_ts,
               count_if(_rescued_data IS NOT NULL) AS rescued
        FROM src_{entidad}
        GROUP BY ALL
        """).collect()
        stats_rows.extend(st)

    stats = spark.createDataFrame(stats_rows) if stats_rows else spark.createDataFrame(
        [], "source_path string, id_comercio_zip string, archivo string, filas long, validas long, footer long, basura long, footer_ts string, rescued long"
    )
    stats.createOrReplaceTempView("parse_stats")

    c_py = spark.createDataFrame(cuarentena_py, schema_c) if cuarentena_py else spark.createDataFrame([], schema_c)
    c_parse = spark.sql(f"""
    SELECT '{lote_id}' AS lote_id, '{fecha}' AS fecha_datos, id_comercio_zip, archivo,
           CASE WHEN validas = 0 THEN 'sin_filas_validas'
                WHEN (validas + basura) > 0 AND validas / (validas + basura) < {UMBRAL} THEN 'parse_bajo'
           END AS motivo,
           concat('filas=', filas, ' validas=', validas, ' footer=', footer, ' basura=', basura,
                  ' rescued=', rescued, ' tasa=',
                  round(CASE WHEN (validas + basura) = 0 THEN 0 ELSE validas / (validas + basura) END, 3)) AS detalle,
           source_path AS ruta_origen, current_timestamp() AS ts_cuarentena
    FROM parse_stats
    WHERE validas = 0
       OR ((validas + basura) > 0 AND validas / (validas + basura) < {UMBRAL})
    """)
    cuarentena = c_py.unionByName(c_parse, allowMissingColumns=True)
    cuarentena.createOrReplaceTempView("cuarentena_lote")
    spark.table("parse_stats").select("source_path").subtract(
        spark.table("cuarentena_lote").select(F.col("ruta_origen").alias("source_path"))
    ).createOrReplaceTempView("archivos_ok")

    n_com = n_suc = n_prod = 0
    for entidad, tabla in (("comercio", "sepa.bronze.comercio"),
                           ("sucursales", "sepa.bronze.sucursales"),
                           ("productos", "sepa.bronze.productos")):
        if not any(f"{entidad}.csv.gz" in files for _, _, files in os.walk(extract_dir)):
            continue
        concat = ", ".join(COLS[entidad])
        spark.sql(f"DELETE FROM {tabla} WHERE _fecha_datos = DATE('{fecha}')")
        spark.sql(f"""
        INSERT INTO {tabla}
        SELECT {concat}, DATE('{fecha}') AS _fecha_datos, '{lote_id}' AS _lote_id,
               s.source_path AS _source_path, current_timestamp() AS _ingest_ts,
               sha2(concat_ws('||', {concat}), 256) AS _row_hash, p.footer_ts AS _footer_ts
        FROM src_{entidad} s
        JOIN parse_stats p ON p.source_path = s.source_path
        JOIN archivos_ok ok ON ok.source_path = s.source_path
        WHERE s.id_comercio RLIKE '^[0-9]+$'
        """)
        n = spark.sql(f"SELECT count(*) AS n FROM {tabla} WHERE _fecha_datos = DATE('{fecha}')").collect()[0]["n"]
        if entidad == "comercio":
            n_com = n
        elif entidad == "sucursales":
            n_suc = n
        else:
            n_prod = n
        print(f"  {tabla}: {n:,} filas")

    spark.sql(f"DELETE FROM sepa.bronze.cuarentena_archivos WHERE lote_id = '{lote_id}'")
    n_c = cuarentena.count()
    if n_c:
        cuarentena.createOrReplaceTempView("c_ins")
        spark.sql("""
        INSERT INTO sepa.bronze.cuarentena_archivos
        SELECT lote_id, DATE(fecha_datos), id_comercio_zip, archivo, motivo, detalle, ruta_origen, ts_cuarentena
        FROM c_ins
        """)
    estado_lote = "parcial" if n_c else "ok"
    spark.createDataFrame(
        [(lote_id, fecha, datetime.now(timezone.utc), int(n_com), int(n_suc), int(n_prod), int(n_c), estado_lote)],
        "lote_id string, fecha_datos string, ts_proceso timestamp, n_comercio long, n_sucursales long, n_productos long, n_cuarentena long, estado string",
    ).createOrReplaceTempView("lp")
    spark.sql("""
    MERGE INTO sepa.bronze.lotes_procesados t
    USING (SELECT lote_id, DATE(fecha_datos) AS fecha_datos, ts_proceso, n_comercio, n_sucursales,
                  n_productos, n_cuarentena, estado FROM lp) s
    ON t.lote_id = s.lote_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  estado={estado_lote}  cuarentena={n_c}")
    return estado_lote


resultados = [cargar_lote(r) for r in pendientes]
print(f"\nFin. {len(resultados)} lote(s): {resultados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultado
# MAGIC
# MAGIC Tiene que haber una fila por día en `lotes_procesados` (04/10 al 10/10). El 09/10 ya estaba.

# COMMAND ----------

display(spark.table("sepa.bronze.lotes_procesados").orderBy("fecha_datos"))
display(spark.table("sepa.bronze.cuarentena_archivos").orderBy("fecha_datos"))
display(spark.sql("""
SELECT _fecha_datos, count(*) AS productos
FROM sepa.bronze.productos
GROUP BY ALL
ORDER BY _fecha_datos
"""))
