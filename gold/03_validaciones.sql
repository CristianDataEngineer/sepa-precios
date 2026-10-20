-- Databricks notebook source
CREATE WIDGET TEXT fecha_datos DEFAULT "2026-10-09";

-- COMMAND ----------

SELECT '01 fact_precio vs silver comparable' AS test;

SELECT
  (SELECT COUNT(*) FROM sepa.gold.fact_precio WHERE fecha_datos = DATE(:fecha_datos)) AS gold_fact,
  (
    SELECT COUNT(*)
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
    WHERE p.fecha_datos = DATE(:fecha_datos)
      AND c.tipo_comercio = 'supermercado'
      AND p.es_ean_global
      AND NOT p._is_outlier
      AND NOT s.es_placeholder
  ) AS silver_comparable;

-- COMMAND ----------

SELECT '02 estaciones en fact_precio (esperado 0)' AS test;

SELECT COUNT(*) AS estaciones_en_fact
FROM sepa.gold.fact_precio f
INNER JOIN sepa.silver.comercio c
  ON  c.id_comercio = f.id_comercio
  AND c.id_bandera  = f.id_bandera
WHERE f.fecha_datos = DATE(:fecha_datos)
  AND c.tipo_comercio <> 'supermercado';

-- COMMAND ----------

SELECT '03 unicidad estrella' AS test;

SELECT 'dim_sucursal_vigente' AS tabla,
       COUNT(*) AS filas,
       COUNT(*) - COUNT(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal)) AS dup
FROM sepa.gold.dim_sucursal
WHERE es_vigente
UNION ALL
SELECT 'dim_comercio',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT concat_ws('|', id_comercio, id_bandera))
FROM sepa.gold.dim_comercio
UNION ALL
SELECT 'dim_producto_vigente',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT id_producto)
FROM sepa.gold.dim_producto
WHERE es_vigente
UNION ALL
SELECT 'canasta_ean',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT id_producto)
FROM sepa.gold.canasta_ean
UNION ALL
SELECT 'dim_fecha',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT fecha)
FROM sepa.gold.dim_fecha;

-- COMMAND ----------

SELECT '04 canasta 8-15, una por categoria' AS test;

SELECT COUNT(*) AS n_ean,
       COUNT(DISTINCT categoria) AS n_cat,
       COUNT(*) - COUNT(DISTINCT categoria) AS categorias_repetidas
FROM sepa.gold.canasta_ean;

SELECT * FROM sepa.gold.canasta_ean ORDER BY categoria;

-- COMMAND ----------

SELECT '05 placeholder en fact_precio (esperado 0)' AS test;

SELECT COUNT(*) AS n_placeholder
FROM sepa.gold.fact_precio f
INNER JOIN sepa.gold.dim_sucursal d
  ON  d.id_comercio = f.id_comercio
  AND d.id_bandera  = f.id_bandera
  AND d.id_sucursal = f.id_sucursal
  AND f.fecha_datos >= d.vigencia_desde
  AND (d.vigencia_hasta IS NULL OR f.fecha_datos <= d.vigencia_hasta)
WHERE f.fecha_datos = DATE(:fecha_datos)
  AND d.es_placeholder;

-- COMMAND ----------

SELECT * FROM sepa.gold.dim_fecha ORDER BY fecha;
