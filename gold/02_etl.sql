-- Databricks notebook source
CREATE WIDGET TEXT forzar_canasta DEFAULT "false";

-- COMMAND ----------

-- MAGIC %python
FORZAR_CANASTA = dbutils.widgets.get("forzar_canasta").strip().lower() in ("true", "1", "yes")
print(f"forzar_canasta = {FORZAR_CANASTA}")
if FORZAR_CANASTA:
    spark.sql("DELETE FROM sepa.gold.canasta_ean")
    print("canasta vaciada: se vuelve a elegir")

-- COMMAND ----------

INSERT OVERWRITE TABLE sepa.gold.fact_precio
SELECT
  p.fecha_datos,
  p.id_comercio,
  p.id_bandera,
  p.id_sucursal,
  p.id_sucursal_fuente,
  p.id_producto,
  p.precio_lista,
  p.precio_promo,
  p.precio_efectivo,
  p.precio_por_unidad_canon,
  p.cantidad_canon,
  p.unidad_canon,
  p._lote_id
FROM sepa.silver.precio_diario p
INNER JOIN sepa.silver.comercio c
  ON  c.id_comercio = p.id_comercio
  AND c.id_bandera  = p.id_bandera
INNER JOIN sepa.silver.sucursal s
  ON  s.id_comercio = p.id_comercio
  AND s.id_bandera  = p.id_bandera
  AND s.id_sucursal = p.id_sucursal
  AND p.fecha_datos >= s.vigencia_desde
  AND (s.vigencia_hasta IS NULL OR p.fecha_datos <= s.vigencia_hasta)
WHERE c.tipo_comercio = 'supermercado'
  AND p.es_ean_global
  AND NOT p._is_outlier
  AND NOT s.es_placeholder;

-- COMMAND ----------

INSERT OVERWRITE TABLE sepa.gold.dim_fecha
WITH bounds AS (
  SELECT MIN(fecha_datos) AS fmin, MAX(fecha_datos) AS fmax
  FROM sepa.gold.fact_precio
),
cal AS (
  SELECT explode(sequence(fmin, fmax, interval 1 day)) AS fecha
  FROM bounds
  WHERE fmin IS NOT NULL
)
SELECT
  fecha,
  year(fecha)  AS anio,
  month(fecha) AS mes,
  day(fecha)   AS dia,
  weekday(fecha) + 1 AS dia_semana,
  CASE weekday(fecha) + 1
    WHEN 1 THEN 'lunes'
    WHEN 2 THEN 'martes'
    WHEN 3 THEN 'miercoles'
    WHEN 4 THEN 'jueves'
    WHEN 5 THEN 'viernes'
    WHEN 6 THEN 'sabado'
    WHEN 7 THEN 'domingo'
  END AS nombre_dia,
  (weekday(fecha) + 1) IN (6, 7) AS es_finde,
  fecha = (SELECT MIN(fecha) FROM cal) AS es_fecha_base
FROM cal;

-- COMMAND ----------

INSERT OVERWRITE TABLE sepa.gold.dim_comercio
SELECT
  id_comercio,
  id_bandera,
  bandera_nombre,
  razon_social,
  tipo_comercio
FROM sepa.silver.comercio;

-- COMMAND ----------

INSERT OVERWRITE TABLE sepa.gold.dim_sucursal
SELECT
  s.id_comercio,
  s.id_bandera,
  s.id_sucursal,
  s.sucursal_nombre,
  s.sucursal_tipo,
  s.calle,
  s.numero,
  s.latitud,
  s.longitud,
  s.localidad,
  s.provincia_iso,
  p.provincia_nombre,
  s.es_placeholder,
  s.vigencia_desde,
  s.vigencia_hasta,
  s.es_vigente
FROM sepa.silver.sucursal s
LEFT JOIN sepa.silver.provincia p
  ON p.provincia_iso = s.provincia_iso;

-- COMMAND ----------

INSERT OVERWRITE TABLE sepa.gold.dim_producto
SELECT
  id_producto,
  descripcion_canon,
  marca_canon,
  cantidad_canon,
  unidad_canon,
  vigencia_desde,
  vigencia_hasta,
  es_vigente
FROM sepa.silver.producto;

-- COMMAND ----------

INSERT INTO sepa.gold.canasta_ean (
  id_producto, categoria, descripcion, n_banderas_dia_base, fecha_base
)
WITH base AS (
  SELECT MIN(fecha_datos) AS fecha_base FROM sepa.gold.fact_precio
),
cand AS (
  SELECT
    f.id_producto,
    COUNT(DISTINCT concat_ws('|', f.id_comercio, f.id_bandera)) AS n_banderas,
    COUNT(DISTINCT concat_ws('|', f.id_comercio, f.id_bandera, f.id_sucursal)) AS n_sucursales
  FROM sepa.gold.fact_precio f
  INNER JOIN base b ON f.fecha_datos = b.fecha_base
  GROUP BY f.id_producto
  HAVING COUNT(DISTINCT concat_ws('|', f.id_comercio, f.id_bandera)) >= 5
),
txt AS (
  SELECT
    c.id_producto,
    c.n_banderas,
    c.n_sucursales,
    lower(trim(coalesce(pr.descripcion_canon, ''))) AS d,
    coalesce(pr.descripcion_canon, c.id_producto) AS descripcion,
    b.fecha_base
  FROM cand c
  CROSS JOIN base b
  LEFT JOIN sepa.gold.dim_producto pr
    ON  pr.id_producto = c.id_producto
    AND pr.es_vigente
),
cat AS (
  SELECT
    *,
    CASE
      WHEN d LIKE '%detergente%' OR d LIKE '%lavandina%' THEN 'detergente'
      WHEN d LIKE '%fideo%' THEN 'fideos'
      WHEN d LIKE '%mayonesa%' THEN 'mayonesa'
      WHEN d LIKE '%yerba%' THEN 'yerba'
      WHEN d LIKE '%gallet%' THEN 'galletitas'
      WHEN d LIKE '%azucar%' OR d LIKE '%azúcar%' THEN 'azucar'
      WHEN d LIKE '%harina%' THEN 'harina'
      WHEN d LIKE '%arroz%' THEN 'arroz'
      WHEN d LIKE '%aceite%' THEN 'aceite'
      WHEN d LIKE '%cafe%' OR d LIKE '%café%' THEN 'cafe'
      WHEN d LIKE '%sal fina%' OR d LIKE '%celusal%' OR d LIKE '%sal entrefina%' THEN 'sal'
      WHEN d LIKE '%leche%' AND d NOT LIKE '%dulce%' AND d NOT LIKE '%polvo%' THEN 'leche'
      WHEN d LIKE '%agua mineral%' OR d LIKE '%sin gas%' OR d LIKE '%s/gas%' THEN 'agua'
      WHEN d LIKE '%pan lactal%' OR d LIKE '%pan de mesa%' THEN 'pan'
      WHEN d LIKE '%vino tinto%' OR d LIKE '%vino blanco%' OR d LIKE '%malbec%' THEN 'vino'
      ELSE CAST(NULL AS STRING)
    END AS categoria
  FROM txt
),
ranked AS (
  SELECT
    id_producto,
    categoria,
    descripcion,
    n_banderas AS n_banderas_dia_base,
    fecha_base,
    row_number() OVER (
      PARTITION BY categoria
      ORDER BY n_banderas DESC, n_sucursales DESC, id_producto
    ) AS rn
  FROM cat
  WHERE categoria IS NOT NULL
)
SELECT id_producto, categoria, descripcion, n_banderas_dia_base, fecha_base
FROM ranked
WHERE rn = 1
  AND NOT EXISTS (SELECT 1 FROM sepa.gold.canasta_ean);

-- COMMAND ----------

SELECT 'fact_precio' t, COUNT(*) n FROM sepa.gold.fact_precio
UNION ALL SELECT 'dim_fecha', COUNT(*) FROM sepa.gold.dim_fecha
UNION ALL SELECT 'dim_comercio', COUNT(*) FROM sepa.gold.dim_comercio
UNION ALL SELECT 'dim_sucursal', COUNT(*) FROM sepa.gold.dim_sucursal
UNION ALL SELECT 'dim_producto', COUNT(*) FROM sepa.gold.dim_producto
UNION ALL SELECT 'canasta_ean', COUNT(*) FROM sepa.gold.canasta_ean;

-- COMMAND ----------

SELECT * FROM sepa.gold.canasta_ean ORDER BY categoria;
