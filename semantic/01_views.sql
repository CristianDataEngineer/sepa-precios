-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS sepa.semantic
COMMENT 'Contrato dashboard SEPA. semantic/SPEC.md';

-- COMMAND ----------

CREATE OR REPLACE VIEW sepa.semantic.v_precio_bandera_dia
COMMENT 'Mediana y brecha: GROUP BY de gold.fact_precio + dims'
AS
WITH med AS (
  SELECT
    f.fecha_datos AS fecha,
    f.id_comercio,
    f.id_bandera,
    c.bandera_nombre AS bandera,
    f.id_producto,
    MAX(pr.descripcion_canon) AS producto,
    MAX(pr.marca_canon) AS marca,
    MAX(CASE WHEN k.id_producto IS NOT NULL THEN TRUE ELSE FALSE END) AS en_canasta,
    CAST(percentile_approx(f.precio_efectivo, 0.5) AS DECIMAL(18, 4)) AS precio_mediano_bandera,
    COUNT(DISTINCT f.id_sucursal) AS n_sucursales,
    CAST(AVG(CASE WHEN f.precio_promo IS NOT NULL THEN 1.0 ELSE 0.0 END) AS DECIMAL(18, 6)) AS share_en_promo
  FROM sepa.gold.fact_precio f
  INNER JOIN sepa.gold.dim_comercio c
    ON  c.id_comercio = f.id_comercio
    AND c.id_bandera  = f.id_bandera
  LEFT JOIN sepa.gold.dim_producto pr
    ON  pr.id_producto = f.id_producto
    AND f.fecha_datos >= pr.vigencia_desde
    AND (pr.vigencia_hasta IS NULL OR f.fecha_datos <= pr.vigencia_hasta)
  LEFT JOIN sepa.gold.canasta_ean k
    ON k.id_producto = f.id_producto
  GROUP BY f.fecha_datos, f.id_comercio, f.id_bandera, c.bandera_nombre, f.id_producto
)
SELECT
  fecha,
  id_comercio,
  id_bandera,
  bandera,
  id_producto,
  producto,
  marca,
  en_canasta,
  precio_mediano_bandera,
  n_sucursales,
  share_en_promo,
  CAST(
    (precio_mediano_bandera - MIN(precio_mediano_bandera) OVER (PARTITION BY fecha, id_producto))
    / NULLIF(MIN(precio_mediano_bandera) OVER (PARTITION BY fecha, id_producto), 0)
    AS DECIMAL(18, 6)
  ) AS brecha_entre_banderas_pct
FROM med;

-- COMMAND ----------

CREATE OR REPLACE VIEW sepa.semantic.v_indice_canasta
COMMENT 'Indice 100 y variacion 7d. Ranking: entra_ranking = true'
AS
WITH ncan AS (
  SELECT COUNT(*) AS n_ean_canasta FROM sepa.gold.canasta_ean
),
fecha_b AS (
  SELECT COALESCE(
    (SELECT MIN(fecha_base) FROM sepa.gold.canasta_ean),
    (SELECT MIN(fecha) FROM sepa.gold.dim_fecha WHERE es_fecha_base)
  ) AS fecha_base
),
en_can AS (
  SELECT * FROM sepa.semantic.v_precio_bandera_dia WHERE en_canasta
),
presentes AS (
  SELECT
    e.fecha,
    e.id_comercio,
    e.id_bandera,
    e.bandera,
    COUNT(DISTINCT e.id_producto) AS n_ean_presentes
  FROM en_can e
  GROUP BY e.fecha, e.id_comercio, e.id_bandera, e.bandera
),
base_px AS (
  SELECT
    e.id_comercio,
    e.id_bandera,
    e.id_producto,
    e.precio_mediano_bandera AS precio_base
  FROM en_can e
  INNER JOIN fecha_b b ON e.fecha = b.fecha_base
),
ratios AS (
  SELECT
    e.fecha,
    e.id_comercio,
    e.id_bandera,
    AVG(e.precio_mediano_bandera / NULLIF(b.precio_base, 0)) * 100 AS indice_canasta
  FROM en_can e
  INNER JOIN base_px b
    ON  b.id_comercio = e.id_comercio
    AND b.id_bandera  = e.id_bandera
    AND b.id_producto = e.id_producto
  GROUP BY e.fecha, e.id_comercio, e.id_bandera
),
armado AS (
  SELECT
    p.fecha,
    p.id_comercio,
    p.id_bandera,
    p.bandera,
    CAST(p.n_ean_presentes / NULLIF(n.n_ean_canasta, 0) AS DECIMAL(18, 6)) AS cobertura_pct,
    CAST(p.n_ean_presentes / NULLIF(n.n_ean_canasta, 0) >= 0.70 AS BOOLEAN) AS entra_ranking,
    CAST(r.indice_canasta AS DECIMAL(18, 6)) AS indice_canasta
  FROM presentes p
  CROSS JOIN ncan n
  LEFT JOIN ratios r
    ON  r.fecha = p.fecha
    AND r.id_comercio = p.id_comercio
    AND r.id_bandera  = p.id_bandera
)
SELECT
  a.fecha,
  a.id_comercio,
  a.id_bandera,
  a.bandera,
  a.cobertura_pct,
  a.entra_ranking,
  a.indice_canasta,
  CAST(
    a.indice_canasta / NULLIF(t7.indice_canasta, 0) - 1
    AS DECIMAL(18, 6)
  ) AS variacion_7d,
  CAST(
    (a.indice_canasta - MIN(CASE WHEN a.entra_ranking THEN a.indice_canasta END)
      OVER (PARTITION BY a.fecha))
    / NULLIF(
        MIN(CASE WHEN a.entra_ranking THEN a.indice_canasta END)
          OVER (PARTITION BY a.fecha),
        0
      )
    AS DECIMAL(18, 6)
  ) AS brecha_canasta_pct
FROM armado a
LEFT JOIN armado t7
  ON  t7.id_comercio = a.id_comercio
  AND t7.id_bandera  = a.id_bandera
  AND t7.fecha = date_add(a.fecha, -7);

-- COMMAND ----------

CREATE OR REPLACE VIEW sepa.semantic.v_sucursal_dia
COMMENT 'Cobertura de un local un dia. GROUP BY de gold.fact_precio'
AS
WITH n AS (
  SELECT
    f.fecha_datos AS fecha,
    f.id_comercio,
    f.id_bandera,
    f.id_sucursal,
    COUNT(*) AS n_precios,
    COUNT(DISTINCT f.id_producto) AS n_ean,
    COUNT(DISTINCT CASE WHEN k.id_producto IS NOT NULL THEN f.id_producto END) AS n_ean_canasta
  FROM sepa.gold.fact_precio f
  LEFT JOIN sepa.gold.canasta_ean k
    ON k.id_producto = f.id_producto
  GROUP BY f.fecha_datos, f.id_comercio, f.id_bandera, f.id_sucursal
)
SELECT
  n.fecha,
  n.id_comercio,
  n.id_bandera,
  n.id_sucursal,
  c.bandera_nombre AS bandera,
  d.sucursal_nombre AS sucursal,
  d.sucursal_tipo,
  d.localidad,
  d.provincia_iso,
  d.provincia_nombre AS provincia,
  d.latitud,
  d.longitud,
  n.n_precios,
  n.n_ean,
  CAST(n.n_ean_canasta AS INT) AS n_ean_canasta
FROM n
INNER JOIN sepa.gold.dim_sucursal d
  ON  d.id_comercio = n.id_comercio
  AND d.id_bandera  = n.id_bandera
  AND d.id_sucursal = n.id_sucursal
  AND n.fecha >= d.vigencia_desde
  AND (d.vigencia_hasta IS NULL OR n.fecha <= d.vigencia_hasta)
INNER JOIN sepa.gold.dim_comercio c
  ON  c.id_comercio = n.id_comercio
  AND c.id_bandera  = n.id_bandera;

-- COMMAND ----------

CREATE OR REPLACE VIEW sepa.semantic.v_locales
COMMENT 'Padron de locales vigentes, sin placeholder'
AS
SELECT
  s.id_comercio,
  s.id_bandera,
  s.id_sucursal,
  c.bandera_nombre  AS bandera,
  s.sucursal_nombre AS sucursal,
  s.sucursal_tipo,
  s.calle,
  s.numero,
  s.localidad,
  s.provincia_iso,
  s.provincia_nombre AS provincia,
  s.latitud,
  s.longitud
FROM sepa.gold.dim_sucursal s
INNER JOIN sepa.gold.dim_comercio c
  ON  c.id_comercio = s.id_comercio
  AND c.id_bandera  = s.id_bandera
WHERE s.es_vigente
  AND NOT s.es_placeholder;
