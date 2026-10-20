# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from datetime import date, datetime, timezone

dbutils.widgets.text("fecha_datos", "")
dbutils.widgets.text("forzar", "false")

FECHA_FILTRO = dbutils.widgets.get("fecha_datos").strip()
FORZAR = dbutils.widgets.get("forzar").strip().lower() in ("true", "1", "yes")
print(f"fecha_datos = {FECHA_FILTRO or '(pendientes)'}")
print(f"forzar      = {FORZAR}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _unidad_seed AS
SELECT * FROM VALUES
  ('g','g',1.0),('gr','g',1.0),('grm','g',1.0),
  ('kg','g',1000.0),('kgm','g',1000.0),('kgr','g',1000.0),
  ('ml','ml',1.0),('cm3','ml',1.0),('cc','ml',1.0),('cmq','ml',1.0),('mi','ml',1.0),
  ('l','ml',1000.0),('lt','ml',1000.0),('ltr','ml',1000.0),('dm3','ml',1000.0),
  ('cl','ml',10.0),('litros','ml',1000.0),
  ('un','un',1.0),('uni','un',1.0),('cu','un',1.0),('pc','un',1.0),
  ('par','un',1.0),('pie','un',1.0),('hjs','un',1.0),
  ('ea','un',1.0),('ud','un',1.0),('unidad','un',1.0),('unida','un',1.0),
  ('cj','un',1.0),('panos','un',1.0),('paque','un',1.0),
  ('m','m',1.0),('mt','m',1.0),('mtr','m',1.0),
  ('cmt','m',0.01),('cm','m',0.01),
  ('m2','m2',1.0)
AS t(unidad_raw, unidad_canon, factor_a_base)
""")
spark.sql("""
MERGE INTO sepa.silver.unidad_medida t
USING _unidad_seed s
ON t.unidad_raw = s.unidad_raw
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

ISO_FALLBACK = [
    ("AR-A", "Salta"), ("AR-B", "Buenos Aires"),
    ("AR-C", "Ciudad Autónoma de Buenos Aires"), ("AR-D", "San Luis"),
    ("AR-E", "Entre Ríos"), ("AR-F", "La Rioja"),
    ("AR-G", "Santiago del Estero"), ("AR-H", "Chaco"),
    ("AR-J", "San Juan"), ("AR-K", "Catamarca"),
    ("AR-L", "La Pampa"), ("AR-M", "Mendoza"),
    ("AR-N", "Misiones"), ("AR-P", "Formosa"),
    ("AR-Q", "Neuquén"), ("AR-R", "Río Negro"),
    ("AR-S", "Santa Fe"), ("AR-T", "Tucumán"),
    ("AR-U", "Chubut"), ("AR-V", "Tierra del Fuego"),
    ("AR-W", "Corrientes"), ("AR-X", "Córdoba"),
    ("AR-Y", "Jujuy"), ("AR-Z", "Santa Cruz"),
]

def cargar_provincias():
    spark.createDataFrame(
        ISO_FALLBACK, "provincia_iso string, provincia_nombre string"
    ).createOrReplaceTempView("_prov_seed")
    spark.sql("""
    MERGE INTO sepa.silver.provincia t
    USING (SELECT DISTINCT provincia_iso, provincia_nombre FROM _prov_seed) s
    ON t.provincia_iso = s.provincia_iso
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"provincia: {len(ISO_FALLBACK)} filas ISO")

cargar_provincias()

spark.sql("""
CREATE OR REPLACE TEMP VIEW _prov_alias AS
SELECT * FROM VALUES
  ('buenos aires','AR-B'),('bs as','AR-B'),('provincia de buenos aires','AR-B'),
  ('caba','AR-C'),('capital federal','AR-C'),
  ('ciudad autonoma de buenos aires','AR-C'),
  ('ciudad autónoma de buenos aires','AR-C'),
  ('cordoba','AR-X'),('córdoba','AR-X'),
  ('santa fe','AR-S'),('entre rios','AR-E'),('entre ríos','AR-E'),
  ('tucuman','AR-T'),('tucumán','AR-T'),
  ('neuquen','AR-Q'),('neuquén','AR-Q'),
  ('rio negro','AR-R'),('río negro','AR-R'),
  ('tierra del fuego','AR-V')
AS t(nombre_norm, provincia_iso)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _tipo_comercio AS
SELECT * FROM VALUES
  ('3','estacion'),('4','estacion'),('23','estacion'),
  ('24','otro'),
  ('2','supermercado'),('5','supermercado'),('6','supermercado'),
  ('8','supermercado'),('9','supermercado'),('10','supermercado'),
  ('11','supermercado'),('12','supermercado'),('13','supermercado'),
  ('15','supermercado'),('20','supermercado'),('21','supermercado'),
  ('47','supermercado')
AS t(id_comercio, tipo_comercio)
""")

print("diccionarios listos")

# COMMAND ----------

if FORZAR:
    print("forzar=true: se reconstruye sucursal, producto, comercio, producto_comercio, precio_diario")
    spark.sql("DELETE FROM sepa.silver.precio_diario")
    spark.sql("DELETE FROM sepa.silver.producto")
    spark.sql("DELETE FROM sepa.silver.producto_comercio")
    spark.sql("DELETE FROM sepa.silver.sucursal")
    spark.sql("DELETE FROM sepa.silver.comercio")
    spark.sql("DELETE FROM sepa.silver.lotes_procesados")

# forzar = replay de todos los dias (el SCD2 no se puede parchear un dia en el medio).
filtro_fecha = "" if FORZAR else (
    f"AND b.fecha_datos = DATE('{FECHA_FILTRO}')" if FECHA_FILTRO else ""
)
anti = "" if FORZAR else """
  AND NOT EXISTS (
    SELECT 1 FROM sepa.silver.lotes_procesados s WHERE s.fecha_datos = b.fecha_datos
  )
"""

pendientes_df = spark.sql(f"""
SELECT b.fecha_datos, b.lote_id
FROM sepa.bronze.lotes_procesados b
WHERE 1=1
  {filtro_fecha}
  {anti}
ORDER BY b.fecha_datos
""")
pendientes = pendientes_df.collect()
print(f"{len(pendientes)} dia(s) a procesar:")
for r in pendientes:
    print(f"  {r['fecha_datos']}  lote={r['lote_id'][:12]}")

if not pendientes:
    dbutils.notebook.exit("Nada pendiente. Bronze no tiene dias nuevos (o ya estan todos en Silver).")

# COMMAND ----------

def txt(col):
    return f"nullif(trim(regexp_replace({col}, '\\\\s+', ' ')), '')"


def procesar_dia(fecha, lote_id):
    f = fecha if isinstance(fecha, str) else fecha.isoformat()
    print(f"\n=== {f} ===")

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_com_dia AS
    SELECT
      {txt('b.id_comercio')} AS id_comercio,
      {txt('b.id_bandera')}  AS id_bandera,
      {txt('b.comercio_cuit')} AS comercio_cuit,
      {txt('b.comercio_razon_social')} AS razon_social,
      {txt('b.comercio_bandera_nombre')} AS bandera_nombre,
      {txt('b.comercio_bandera_url')} AS bandera_url,
      coalesce(tc.tipo_comercio, 'otro') AS tipo_comercio,
      b._lote_id
    FROM sepa.bronze.comercio b
    LEFT JOIN _tipo_comercio tc ON tc.id_comercio = {txt('b.id_comercio')}
    WHERE b._fecha_datos = DATE('{f}')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {txt('b.id_comercio')}, {txt('b.id_bandera')}
      ORDER BY b._row_hash
    ) = 1
    """)

    spark.sql(f"""
    MERGE INTO sepa.silver.comercio t
    USING (
      SELECT *, DATE('{f}') AS fecha_datos FROM v_com_dia
    ) s
    ON t.id_comercio = s.id_comercio AND t.id_bandera = s.id_bandera
    WHEN MATCHED AND s.fecha_datos >= coalesce(t.fecha_ultima_foto, DATE('1900-01-01')) THEN UPDATE SET
      comercio_cuit = s.comercio_cuit,
      razon_social = s.razon_social,
      bandera_nombre = s.bandera_nombre,
      bandera_url = s.bandera_url,
      tipo_comercio = s.tipo_comercio,
      fecha_ultima_foto = s.fecha_datos,
      _lote_id = s._lote_id
    WHEN NOT MATCHED THEN INSERT (
      id_comercio, id_bandera, comercio_cuit, razon_social, bandera_nombre, bandera_url,
      tipo_comercio, fecha_ultima_foto, _lote_id
    ) VALUES (
      s.id_comercio, s.id_bandera, s.comercio_cuit, s.razon_social, s.bandera_nombre, s.bandera_url,
      s.tipo_comercio, s.fecha_datos, s._lote_id
    )
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_suc_dia AS
    SELECT
      {txt('b.id_comercio')} AS id_comercio,
      {txt('b.id_bandera')}  AS id_bandera,
      {txt('b.id_sucursal')} AS id_sucursal,
      {txt('sucursales_nombre')} AS sucursal_nombre,
      CASE lower({txt('sucursales_tipo')})
        WHEN 'hipermercado' THEN 'Hipermercado'
        WHEN 'supermercado' THEN 'Supermercado'
        WHEN 'autoservicio' THEN 'Autoservicio'
        WHEN 'web' THEN 'Web'
        ELSE 'otro'
      END AS sucursal_tipo,
      {txt('sucursales_calle')} AS calle,
      {txt('sucursales_numero')} AS numero,
      try_cast(sucursales_latitud AS DECIMAL(12,8)) AS latitud,
      try_cast(sucursales_longitud AS DECIMAL(12,8)) AS longitud,
      {txt('sucursales_observaciones')} AS observaciones,
      {txt('sucursales_barrio')} AS barrio,
      {txt('sucursales_codigo_postal')} AS codigo_postal,
      {txt('sucursales_localidad')} AS localidad,
      CASE
        WHEN {txt('sucursales_provincia')} RLIKE '^AR-[A-Z]$'
          THEN {txt('sucursales_provincia')}
        ELSE coalesce(a.provincia_iso, p2.provincia_iso)
      END AS provincia_iso,
      {txt('sucursales_lunes_horario_atencion')} AS horario_lunes,
      {txt('sucursales_martes_horario_atencion')} AS horario_martes,
      {txt('sucursales_miercoles_horario_atencion')} AS horario_miercoles,
      {txt('sucursales_jueves_horario_atencion')} AS horario_jueves,
      {txt('sucursales_viernes_horario_atencion')} AS horario_viernes,
      {txt('sucursales_sabado_horario_atencion')} AS horario_sabado,
      {txt('sucursales_domingo_horario_atencion')} AS horario_domingo,
      b._lote_id
    FROM sepa.bronze.sucursales b
    LEFT JOIN _prov_alias a
      ON a.nombre_norm = lower(translate(coalesce({txt('b.sucursales_provincia')}, ''),
           'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))
    LEFT JOIN sepa.silver.provincia p2
      ON lower(translate(p2.provincia_nombre, 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))
       = lower(translate(coalesce({txt('b.sucursales_provincia')}, ''),
           'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))
    WHERE b._fecha_datos = DATE('{f}')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {txt('b.id_comercio')}, {txt('b.id_bandera')}, {txt('b.id_sucursal')}
      ORDER BY b._row_hash
    ) = 1
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_suc_snap AS
    SELECT *,
      sha2(concat_ws('||',
        coalesce(sucursal_nombre,''), coalesce(sucursal_tipo,''),
        coalesce(calle,''), coalesce(numero,''),
        coalesce(cast(latitud AS STRING),''), coalesce(cast(longitud AS STRING),''),
        coalesce(observaciones,''), coalesce(barrio,''), coalesce(codigo_postal,''),
        coalesce(localidad,''), coalesce(provincia_iso,''),
        coalesce(horario_lunes,''), coalesce(horario_martes,''),
        coalesce(horario_miercoles,''), coalesce(horario_jueves,''),
        coalesce(horario_viernes,''), coalesce(horario_sabado,''),
        coalesce(horario_domingo,'')
      ), 256) AS attr_hash
    FROM v_suc_dia
    """)

    spark.sql(f"""
    MERGE INTO sepa.silver.sucursal t
    USING v_suc_snap s
    ON  t.id_comercio = s.id_comercio
    AND t.id_bandera  = s.id_bandera
    AND t.id_sucursal = s.id_sucursal
    AND t.es_vigente
    AND NOT t.es_placeholder
    WHEN MATCHED AND sha2(concat_ws('||',
        coalesce(t.sucursal_nombre,''), coalesce(t.sucursal_tipo,''),
        coalesce(t.calle,''), coalesce(t.numero,''),
        coalesce(cast(t.latitud AS STRING),''), coalesce(cast(t.longitud AS STRING),''),
        coalesce(t.observaciones,''), coalesce(t.barrio,''), coalesce(t.codigo_postal,''),
        coalesce(t.localidad,''), coalesce(t.provincia_iso,''),
        coalesce(t.horario_lunes,''), coalesce(t.horario_martes,''),
        coalesce(t.horario_miercoles,''), coalesce(t.horario_jueves,''),
        coalesce(t.horario_viernes,''), coalesce(t.horario_sabado,''),
        coalesce(t.horario_domingo,'')
      ), 256) <> s.attr_hash
    THEN UPDATE SET vigencia_hasta = date_sub(DATE('{f}'), 1), es_vigente = false
    """)

    spark.sql(f"""
    INSERT INTO sepa.silver.sucursal (
      id_comercio, id_bandera, id_sucursal, sucursal_nombre, sucursal_tipo,
      calle, numero, latitud, longitud, observaciones, barrio, codigo_postal, localidad,
      provincia_iso, horario_lunes, horario_martes, horario_miercoles, horario_jueves,
      horario_viernes, horario_sabado, horario_domingo,
      es_placeholder, vigencia_desde, vigencia_hasta, es_vigente, _lote_id
    )
    SELECT
      s.id_comercio, s.id_bandera, s.id_sucursal, s.sucursal_nombre, s.sucursal_tipo,
      s.calle, s.numero, s.latitud, s.longitud, s.observaciones, s.barrio, s.codigo_postal,
      s.localidad, s.provincia_iso, s.horario_lunes, s.horario_martes, s.horario_miercoles,
      s.horario_jueves, s.horario_viernes, s.horario_sabado, s.horario_domingo,
      false, DATE('{f}'), CAST(NULL AS DATE), true, s._lote_id
    FROM v_suc_snap s
    WHERE NOT EXISTS (
      SELECT 1 FROM sepa.silver.sucursal t
      WHERE t.id_comercio = s.id_comercio AND t.id_bandera = s.id_bandera
        AND t.id_sucursal = s.id_sucursal AND t.es_vigente AND NOT t.es_placeholder
    )
    """)

    spark.sql(f"""
    INSERT INTO sepa.silver.sucursal (
      id_comercio, id_bandera, id_sucursal, sucursal_nombre, sucursal_tipo,
      calle, numero, latitud, longitud, observaciones, barrio, codigo_postal, localidad,
      provincia_iso, horario_lunes, horario_martes, horario_miercoles, horario_jueves,
      horario_viernes, horario_sabado, horario_domingo,
      es_placeholder, vigencia_desde, vigencia_hasta, es_vigente, _lote_id
    )
    SELECT
      c.id_comercio, c.id_bandera, '_DESCONOCIDA', 'Sucursal desconocida', 'otro',
      CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS DECIMAL(12,8)),
      CAST(NULL AS DECIMAL(12,8)), CAST(NULL AS STRING), CAST(NULL AS STRING),
      CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
      CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
      CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
      CAST(NULL AS STRING),
      true, DATE('{f}'), CAST(NULL AS DATE), true, '{lote_id}'
    FROM (SELECT DISTINCT id_comercio, id_bandera FROM v_com_dia) c
    WHERE NOT EXISTS (
      SELECT 1 FROM sepa.silver.sucursal t
      WHERE t.id_comercio = c.id_comercio AND t.id_bandera = c.id_bandera
        AND t.id_sucursal = '_DESCONOCIDA' AND t.es_placeholder AND t.es_vigente
    )
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_prod_dedup AS
    SELECT
      {txt('id_comercio')} AS id_comercio,
      {txt('id_bandera')}  AS id_bandera,
      {txt('id_sucursal')} AS id_sucursal_fuente,
      {txt('id_producto')} AS id_producto,
      regexp_replace(coalesce({txt('id_producto')}, ''), '[^0-9]', '') AS id_prod_digitos,
      {txt('productos_descripcion')} AS descripcion,
      CASE WHEN upper({txt('productos_marca')}) IN ('SIN MARCA') THEN NULL
           ELSE {txt('productos_marca')} END AS marca,
      try_cast(productos_cantidad_presentacion AS DECIMAL(18,6)) AS cant_raw,
      regexp_replace(lower(trim(coalesce(productos_unidad_medida_presentacion,''))), '[.]+$', '') AS unidad_raw,
      try_cast(productos_precio_lista AS DECIMAL(18,4)) AS precio_lista,
      try_cast(productos_precio_unitario_promo1 AS DECIMAL(18,4)) AS precio_promo_raw,
      _row_hash,
      _lote_id,
      max(productos_precio_lista) OVER (
        PARTITION BY {txt('id_comercio')}, {txt('id_bandera')}, {txt('id_sucursal')}, {txt('id_producto')}
      ) <> min(productos_precio_lista) OVER (
        PARTITION BY {txt('id_comercio')}, {txt('id_bandera')}, {txt('id_sucursal')}, {txt('id_producto')}
      ) AS _tiene_conflicto_precio
    FROM sepa.bronze.productos
    WHERE _fecha_datos = DATE('{f}')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {txt('id_comercio')}, {txt('id_bandera')}, {txt('id_sucursal')}, {txt('id_producto')}
      ORDER BY _row_hash
    ) = 1
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_precio_dia AS
    SELECT
      d.id_comercio, d.id_bandera, d.id_sucursal_fuente, d.id_producto,
      CASE WHEN length(d.id_prod_digitos) IN (8, 12, 13, 14)
            AND NOT (length(d.id_prod_digitos) = 13 AND substr(d.id_prod_digitos, 1, 1) = '2')
            AND NOT d.id_prod_digitos RLIKE '^0{7}'
           THEN (
             (10 - (
               aggregate(
                 transform(sequence(1, length(d.id_prod_digitos) - 1), i ->
                   CAST(substr(d.id_prod_digitos, length(d.id_prod_digitos) - i, 1) AS INT)
                   * IF(i % 2 = 1, 3, 1)
                 ),
                 0,
                 (acc, x) -> acc + coalesce(x, 0)
               ) % 10
             )) % 10
           ) = CAST(substr(d.id_prod_digitos, length(d.id_prod_digitos), 1) AS INT)
           ELSE false END
      AS es_ean_global,
      d.descripcion, d.marca, d.precio_lista,
      CASE WHEN d.precio_promo_raw IS NOT NULL AND d.precio_promo_raw > 0
                AND d.precio_promo_raw < d.precio_lista
           THEN d.precio_promo_raw END AS precio_promo,
      CASE WHEN d.precio_promo_raw IS NOT NULL AND d.precio_promo_raw > 0
                AND d.precio_promo_raw < d.precio_lista
           THEN d.precio_promo_raw ELSE d.precio_lista END AS precio_efectivo,
      CASE
        WHEN d.cant_raw IS NULL OR d.cant_raw <= 0 OR u.unidad_canon IS NULL THEN CAST(NULL AS DECIMAL(18,6))
        WHEN d.id_comercio = '9' AND d.cant_raw > 0 AND d.cant_raw < 1
             AND d.unidad_raw IN ('g','gr','grm','ml','cm3','cc','cmq','mi')
          THEN d.cant_raw * 1000 * u.factor_a_base
        ELSE d.cant_raw * u.factor_a_base
      END AS cantidad_canon,
      u.unidad_canon,
      d._tiene_conflicto_precio, d._lote_id,
      coalesce(tc.tipo_comercio, 'otro') AS tipo_comercio
    FROM v_prod_dedup d
    LEFT JOIN sepa.silver.unidad_medida u ON u.unidad_raw = d.unidad_raw
    LEFT JOIN _tipo_comercio tc ON tc.id_comercio = d.id_comercio
    WHERE d.precio_lista IS NOT NULL AND d.precio_lista >= 0
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_precio_out AS
    SELECT
      p.*,
      (p.precio_lista <= 0 OR (p.precio_lista < 100 AND p.es_ean_global)) AS _is_outlier,
      CASE
        WHEN p.cantidad_canon IS NULL OR p.unidad_canon IS NULL OR p.cantidad_canon <= 0 THEN CAST(NULL AS DECIMAL(18,4))
        WHEN p.unidad_canon IN ('g','ml') THEN p.precio_lista / (p.cantidad_canon / 1000)
        ELSE p.precio_lista / p.cantidad_canon
      END AS precio_por_unidad_canon,
      CASE WHEN s.id_sucursal IS NOT NULL THEN p.id_sucursal_fuente ELSE '_DESCONOCIDA' END AS id_sucursal
    FROM v_precio_dia p
    LEFT JOIN v_suc_dia s
      ON s.id_comercio = p.id_comercio
     AND s.id_bandera  = p.id_bandera
     AND s.id_sucursal = p.id_sucursal_fuente
    """)

    spark.sql(f"DELETE FROM sepa.silver.precio_diario WHERE fecha_datos = DATE('{f}')")
    spark.sql(f"""
    INSERT INTO sepa.silver.precio_diario (
      fecha_datos, id_comercio, id_bandera, id_sucursal_fuente, id_sucursal, id_producto,
      es_ean_global, precio_lista, precio_promo, precio_efectivo, precio_por_unidad_canon,
      cantidad_canon, unidad_canon, _is_outlier, _tiene_conflicto_precio, _lote_id
    )
    SELECT
      DATE('{f}'), id_comercio, id_bandera, id_sucursal_fuente, id_sucursal, id_producto,
      es_ean_global, precio_lista, precio_promo, precio_efectivo, precio_por_unidad_canon,
      cantidad_canon, unidad_canon, _is_outlier, _tiene_conflicto_precio, _lote_id
    FROM v_precio_out
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_pc_dia AS
    SELECT id_comercio, id_bandera, id_producto, es_ean_global, descripcion, marca,
           cantidad_canon, unidad_canon
    FROM (
      SELECT id_comercio, id_bandera, id_producto, es_ean_global, descripcion, marca,
             cantidad_canon, unidad_canon,
             count(*) AS n
      FROM v_precio_out
      GROUP BY ALL
    )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY id_comercio, id_bandera, id_producto
      ORDER BY n DESC, coalesce(descripcion, '') ASC
    ) = 1
    """)

    spark.sql(f"""
    MERGE INTO sepa.silver.producto_comercio t
    USING (
      SELECT *, DATE('{f}') AS fecha_datos, '{lote_id}' AS lote_id FROM v_pc_dia
    ) s
    ON t.id_comercio = s.id_comercio AND t.id_bandera = s.id_bandera AND t.id_producto = s.id_producto
    WHEN MATCHED AND s.fecha_datos >= coalesce(t.fecha_ultima_foto, DATE('1900-01-01')) THEN UPDATE SET
      es_ean_global = s.es_ean_global,
      descripcion = s.descripcion,
      marca = s.marca,
      cantidad_canon = s.cantidad_canon,
      unidad_canon = s.unidad_canon,
      fecha_ultima_foto = s.fecha_datos,
      _lote_id = s.lote_id
    WHEN NOT MATCHED THEN INSERT (
      id_comercio, id_bandera, id_producto, es_ean_global, descripcion, marca,
      cantidad_canon, unidad_canon, fecha_ultima_foto, _lote_id
    ) VALUES (
      s.id_comercio, s.id_bandera, s.id_producto, s.es_ean_global, s.descripcion, s.marca,
      s.cantidad_canon, s.unidad_canon, s.fecha_datos, s.lote_id
    )
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_prod_desc AS
    SELECT id_producto, descripcion AS descripcion_canon
    FROM (
      SELECT id_producto, descripcion, count(*) AS n
      FROM v_precio_out
      WHERE es_ean_global AND NOT _is_outlier AND tipo_comercio = 'supermercado'
        AND descripcion IS NOT NULL
      GROUP BY ALL
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY n DESC, descripcion ASC) = 1
    """)
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_prod_marca AS
    SELECT id_producto, marca AS marca_canon
    FROM (
      SELECT id_producto, marca, count(*) AS n
      FROM v_precio_out
      WHERE es_ean_global AND NOT _is_outlier AND tipo_comercio = 'supermercado'
        AND marca IS NOT NULL
      GROUP BY ALL
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY n DESC, marca ASC) = 1
    """)
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_prod_pres AS
    SELECT id_producto, cantidad_canon, unidad_canon
    FROM (
      SELECT id_producto, cantidad_canon, unidad_canon, count(*) AS n
      FROM v_precio_out
      WHERE es_ean_global AND NOT _is_outlier AND tipo_comercio = 'supermercado'
        AND unidad_canon IS NOT NULL AND cantidad_canon IS NOT NULL
      GROUP BY ALL
    )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY id_producto
      ORDER BY n DESC, concat_ws('|', CAST(cantidad_canon AS STRING), unidad_canon) ASC
    ) = 1
    """)
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_prod_canon AS
    SELECT
      d.id_producto,
      d.descripcion_canon,
      m.marca_canon,
      p.cantidad_canon,
      p.unidad_canon
    FROM v_prod_desc d
    LEFT JOIN v_prod_marca m ON m.id_producto = d.id_producto
    LEFT JOIN v_prod_pres  p ON p.id_producto = d.id_producto
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_prod_snap AS
    SELECT *,
      sha2(concat_ws('||',
        coalesce(descripcion_canon,''), coalesce(marca_canon,''),
        coalesce(cast(cantidad_canon AS STRING),''), coalesce(unidad_canon,'')
      ), 256) AS attr_hash
    FROM v_prod_canon
    """)

    spark.sql(f"""
    MERGE INTO sepa.silver.producto t
    USING v_prod_snap s
    ON t.id_producto = s.id_producto AND t.es_vigente
    WHEN MATCHED AND sha2(concat_ws('||',
        coalesce(t.descripcion_canon,''), coalesce(t.marca_canon,''),
        coalesce(cast(t.cantidad_canon AS STRING),''), coalesce(t.unidad_canon,'')
      ), 256) <> s.attr_hash
    THEN UPDATE SET vigencia_hasta = date_sub(DATE('{f}'), 1), es_vigente = false
    """)

    spark.sql(f"""
    INSERT INTO sepa.silver.producto (
      id_producto, descripcion_canon, marca_canon, cantidad_canon, unidad_canon,
      vigencia_desde, vigencia_hasta, es_vigente, _lote_id
    )
    SELECT
      s.id_producto, s.descripcion_canon, s.marca_canon, s.cantidad_canon, s.unidad_canon,
      DATE('{f}'), CAST(NULL AS DATE), true, '{lote_id}'
    FROM v_prod_snap s
    WHERE NOT EXISTS (
      SELECT 1 FROM sepa.silver.producto t
      WHERE t.id_producto = s.id_producto AND t.es_vigente
    )
    """)

    stats = spark.sql(f"""
    SELECT
      (SELECT count(*) FROM sepa.silver.comercio) AS n_comercio,
      (SELECT count(*) FROM sepa.silver.sucursal WHERE es_vigente) AS n_sucursal_vigente,
      (SELECT count(*) FROM sepa.silver.producto WHERE es_vigente) AS n_producto_vigente,
      (SELECT count(*) FROM sepa.silver.precio_diario WHERE fecha_datos = DATE('{f}')) AS n_precio_diario,
      (SELECT count_if(_is_outlier) FROM sepa.silver.precio_diario WHERE fecha_datos = DATE('{f}')) AS n_outlier,
      (SELECT count_if(id_sucursal = '_DESCONOCIDA') FROM sepa.silver.precio_diario WHERE fecha_datos = DATE('{f}')) AS n_placeholder
    """).collect()[0]

    spark.sql(f"""
    MERGE INTO sepa.silver.lotes_procesados t
    USING (
      SELECT DATE('{f}') AS fecha_datos, '{lote_id}' AS lote_id,
             current_timestamp() AS ts_proceso,
             {int(stats['n_comercio'])} AS n_comercio,
             {int(stats['n_sucursal_vigente'])} AS n_sucursal_vigente,
             {int(stats['n_producto_vigente'])} AS n_producto_vigente,
             {int(stats['n_precio_diario'])} AS n_precio_diario,
             {int(stats['n_outlier'])} AS n_outlier,
             {int(stats['n_placeholder'])} AS n_placeholder,
             'ok' AS estado
    ) s
    ON t.fecha_datos = s.fecha_datos
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(
        f"  precios={stats['n_precio_diario']:,}  outlier={stats['n_outlier']:,}  "
        f"placeholder={stats['n_placeholder']:,}  suc_vig={stats['n_sucursal_vigente']:,}  "
        f"prod_vig={stats['n_producto_vigente']:,}"
    )


for r in pendientes:
    procesar_dia(r["fecha_datos"], r["lote_id"])

print(f"\nFin. {len(pendientes)} dia(s).")

# COMMAND ----------

display(spark.table("sepa.silver.lotes_procesados").orderBy("fecha_datos"))
display(spark.sql("""
SELECT fecha_datos, count(*) AS precios,
       count_if(es_ean_global) AS ean_global,
       count_if(_is_outlier) AS outlier,
       count_if(id_sucursal = '_DESCONOCIDA') AS placeholder
FROM sepa.silver.precio_diario
GROUP BY ALL
ORDER BY fecha_datos
"""))
display(spark.sql("""
SELECT tipo_comercio, count(*) AS banderas
FROM sepa.silver.comercio
GROUP BY ALL
ORDER BY tipo_comercio
"""))