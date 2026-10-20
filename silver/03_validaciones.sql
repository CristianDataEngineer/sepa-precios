-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver — validaciones
-- MAGIC
-- MAGIC Contrato: `silver/SPEC.md`. Correr **después** de `02_etl` para el mismo `fecha_datos`.
-- MAGIC
-- MAGIC No escribe. Solo SELECT. Si un esperado no cierra, no se sigue a Gold hasta entenderlo.

-- COMMAND ----------

CREATE WIDGET TEXT fecha_datos DEFAULT '2026-10-09';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Volumen vs Bronze
-- MAGIC
-- MAGIC `n_precio_diario` ≤ filas Bronze del día (dedup + bajas por precio inválido), mismo orden (~14 M).

-- COMMAND ----------

SELECT * FROM sepa.silver.lotes_procesados
WHERE fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

SELECT
  (SELECT count(*) FROM sepa.bronze.productos WHERE _fecha_datos = DATE(:fecha_datos)) AS bronze_productos,
  (SELECT n_precio_diario FROM sepa.silver.lotes_procesados WHERE fecha_datos = DATE(:fecha_datos)) AS silver_precios,
  (SELECT n_outlier FROM sepa.silver.lotes_procesados WHERE fecha_datos = DATE(:fecha_datos)) AS outlier,
  (SELECT n_placeholder FROM sepa.silver.lotes_procesados WHERE fecha_datos = DATE(:fecha_datos)) AS placeholder;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Unicidad

-- COMMAND ----------

SELECT 'precio_diario' AS tabla,
       count(*) AS filas,
       count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal_fuente, id_producto)) AS claves,
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal_fuente, id_producto)) AS dup
FROM sepa.silver.precio_diario WHERE fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'comercio',
       count(*),
       count(DISTINCT concat_ws('|', id_comercio, id_bandera)),
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera))
FROM sepa.silver.comercio
UNION ALL
SELECT 'sucursal_vigente',
       count(*),
       count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal)),
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal))
FROM sepa.silver.sucursal WHERE es_vigente
UNION ALL
SELECT 'producto_vigente',
       count(*),
       count(DISTINCT id_producto),
       count(*) - count(DISTINCT id_producto)
FROM sepa.silver.producto WHERE es_vigente;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. FK sucursal (placeholder cuenta)
-- MAGIC
-- MAGIC Esperado: **0** precios cuyo `id_sucursal` no esté vigente en `sucursal`.

-- COMMAND ----------

SELECT count(*) AS precios_sin_sucursal
FROM sepa.silver.precio_diario p
WHERE p.fecha_datos = DATE(:fecha_datos)
  AND NOT EXISTS (
    SELECT 1 FROM sepa.silver.sucursal s
    WHERE s.id_comercio = p.id_comercio
      AND s.id_bandera  = p.id_bandera
      AND s.id_sucursal = p.id_sucursal
      AND s.es_vigente
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Precio y outlier
-- MAGIC
-- MAGIC Esperado: 0 NULL, 0 negativos. Outlier incluye ceros. Un precio > 1 M **no** es outlier.

-- COMMAND ----------

SELECT
  count_if(precio_lista IS NULL) AS precio_nulo,
  count_if(precio_lista < 0) AS precio_neg,
  count_if(precio_lista = 0) AS precio_cero,
  count_if(precio_lista = 0 AND NOT _is_outlier) AS cero_sin_flag,
  count_if(precio_lista > 1000000 AND _is_outlier) AS millon_marcado_outlier,
  count_if(_is_outlier) AS n_outlier
FROM sepa.silver.precio_diario
WHERE fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. EAN
-- MAGIC
-- MAGIC Ningún `es_ean_global` de 13 dígitos con prefijo `2`.

-- COMMAND ----------

SELECT
  count_if(es_ean_global) AS ean_global,
  count_if(NOT es_ean_global) AS interno_o_invalido,
  count_if(es_ean_global AND id_producto RLIKE '^2[0-9]{12}$') AS prefijo2_marcados_global
FROM sepa.silver.precio_diario
WHERE fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. `tipo_comercio`
-- MAGIC
-- MAGIC Solo `supermercado` / `estacion` / `otro`. Ids de Bronze del día que no están en el mapeo
-- MAGIC (caen en `otro`) se listan abajo.

-- COMMAND ----------

SELECT tipo_comercio, count(*) AS banderas
FROM sepa.silver.comercio
GROUP BY ALL
ORDER BY tipo_comercio;

-- COMMAND ----------

SELECT DISTINCT b.id_comercio, max(b.comercio_razon_social) AS razon, c.tipo_comercio
FROM sepa.bronze.comercio b
LEFT JOIN sepa.silver.comercio c
  ON c.id_comercio = trim(b.id_comercio) AND c.id_bandera = trim(b.id_bandera)
WHERE b._fecha_datos = DATE(:fecha_datos)
  AND coalesce(c.tipo_comercio, 'otro') = 'otro'
  AND trim(b.id_comercio) NOT IN ('3', '4', '23', '24')
GROUP BY ALL
ORDER BY try_cast(b.id_comercio AS INT);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Placeholder y SCD2 (control visual)

-- COMMAND ----------

SELECT es_placeholder, count(*) AS vigentes
FROM sepa.silver.sucursal
WHERE es_vigente
GROUP BY ALL;

-- COMMAND ----------

SELECT
  count(*) AS versiones,
  count_if(es_vigente) AS vigentes,
  count_if(NOT es_vigente) AS cerradas
FROM sepa.silver.sucursal
WHERE NOT es_placeholder;

-- COMMAND ----------

SELECT
  count(*) AS versiones,
  count_if(es_vigente) AS vigentes,
  count_if(NOT es_vigente) AS cerradas
FROM sepa.silver.producto;

-- COMMAND ----------


DESCRIBE TABLE sepa.silver