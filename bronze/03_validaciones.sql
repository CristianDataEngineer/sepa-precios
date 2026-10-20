-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze — validaciones
-- MAGIC
-- MAGIC Contrato: `bronze/SPEC.md`. Correr **después** de `02_etl` para el mismo `fecha_datos`.
-- MAGIC
-- MAGIC No escribe tablas. Solo SELECT. Si un número "esperado" no cierra, no se sigue a Silver
-- MAGIC hasta entenderlo.
-- MAGIC
-- MAGIC Widget: `fecha_datos` (mismo día que cargó el ETL).

-- COMMAND ----------

CREATE WIDGET TEXT fecha_datos DEFAULT '2026-10-09';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Volumen
-- MAGIC
-- MAGIC Un día completo: decenas de banderas, miles de sucursales, ~12–15 millones de precios.
-- MAGIC `n_cuarentena` 0 = día perfecto; 1+ = `parcial` (aceptable si el motivo es ZIP vacío).

-- COMMAND ----------

SELECT * FROM sepa.bronze.lotes_procesados
WHERE fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

SELECT 'comercio' AS tabla, count(*) AS filas FROM sepa.bronze.comercio WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'sucursales', count(*) FROM sepa.bronze.sucursales WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'productos', count(*) FROM sepa.bronze.productos WHERE _fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Footer y IDs
-- MAGIC
-- MAGIC Esperado: **0** footer, **0** `id_comercio` nulo, **0** no numérico.
-- MAGIC Si aparece footer, el filtro del ETL falló.

-- COMMAND ----------

SELECT 'comercio' AS tabla,
       count_if(id_comercio RLIKE '(?i)ltima actualizaci') AS footer,
       count_if(id_comercio IS NULL OR trim(id_comercio) = '') AS id_nulo,
       count_if(NOT id_comercio RLIKE '^[0-9]+$') AS id_no_num,
       count_if(_lote_id IS NULL) AS sin_lote
FROM sepa.bronze.comercio WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'sucursales',
       count_if(id_comercio RLIKE '(?i)ltima actualizaci'),
       count_if(id_comercio IS NULL OR trim(id_comercio) = '' OR id_sucursal IS NULL OR trim(id_sucursal) = ''),
       count_if(NOT id_comercio RLIKE '^[0-9]+$'),
       count_if(_lote_id IS NULL)
FROM sepa.bronze.sucursales WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'productos',
       count_if(id_comercio RLIKE '(?i)ltima actualizaci'),
       count_if(id_comercio IS NULL OR trim(id_comercio) = '' OR id_producto IS NULL OR trim(id_producto) = ''),
       count_if(NOT id_comercio RLIKE '^[0-9]+$'),
       count_if(_lote_id IS NULL)
FROM sepa.bronze.productos WHERE _fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Duplicados de clave
-- MAGIC
-- MAGIC Claves:
-- MAGIC - comercio: `(id_comercio, id_bandera)`
-- MAGIC - sucursales: `(id_comercio, id_bandera, id_sucursal)`
-- MAGIC - productos: `(id_comercio, id_bandera, id_sucursal, id_producto)`
-- MAGIC
-- MAGIC Esperado: **0** en comercio y sucursales.
-- MAGIC En productos, el 2026-10-09 tuvo **49** claves duplicadas, todas California (id 5), copias exactas.

-- COMMAND ----------

SELECT 'comercio' AS tabla,
       count(*) AS filas,
       count(DISTINCT concat_ws('|', id_comercio, id_bandera)) AS claves,
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera)) AS dup
FROM sepa.bronze.comercio WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'sucursales',
       count(*),
       count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal)),
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal))
FROM sepa.bronze.sucursales WHERE _fecha_datos = DATE(:fecha_datos)
UNION ALL
SELECT 'productos',
       count(*),
       count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal, id_producto)),
       count(*) - count(DISTINCT concat_ws('|', id_comercio, id_bandera, id_sucursal, id_producto))
FROM sepa.bronze.productos WHERE _fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- Detalle: qué comercio duplica y si el precio/descripcion cambian (copia vs conflicto).
WITH d AS (
  SELECT id_comercio, id_bandera, id_sucursal, id_producto,
         count(*) AS n,
         count(DISTINCT productos_precio_lista) AS precios,
         count(DISTINCT productos_descripcion) AS descs
  FROM sepa.bronze.productos
  WHERE _fecha_datos = DATE(:fecha_datos)
  GROUP BY ALL
  HAVING n > 1
)
SELECT id_comercio, count(*) AS claves_dup, sum(n) AS filas, max(n) AS max_n,
       count_if(precios > 1) AS precio_distinto, count_if(descs > 1) AS desc_distinta
FROM d
GROUP BY ALL
ORDER BY try_cast(id_comercio AS INT);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Precios y textos de producto
-- MAGIC
-- MAGIC Esperado:
-- MAGIC - precio vacío / no numérico / negativo = **0**
-- MAGIC - precio cero: puede haber (California 56 el 09/10) → Silver outlier
-- MAGIC - descripción vacía = **0**
-- MAGIC - flag EAN solo `0` o `1`
-- MAGIC - padding: miles de filas, no es error de carga → Silver `trim`

-- COMMAND ----------

SELECT
  count_if(productos_precio_lista IS NULL OR trim(productos_precio_lista) = '') AS precio_vacio,
  count_if(try_cast(productos_precio_lista AS DOUBLE) IS NULL) AS precio_no_num,
  count_if(try_cast(productos_precio_lista AS DOUBLE) = 0) AS precio_cero,
  count_if(try_cast(productos_precio_lista AS DOUBLE) < 0) AS precio_neg,
  count_if(productos_descripcion IS NULL OR trim(productos_descripcion) = '') AS desc_vacia,
  count_if(productos_ean NOT IN ('0', '1') OR productos_ean IS NULL) AS ean_flag_raro,
  count_if(id_producto NOT RLIKE '^[0-9]+$') AS id_producto_no_num,
  count_if(productos_descripcion <> trim(productos_descripcion)) AS desc_padding
FROM sepa.bronze.productos
WHERE _fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

SELECT p.id_comercio, c.comercio_bandera_nombre, count(*) AS n_precio_cero
FROM sepa.bronze.productos p
LEFT JOIN sepa.bronze.comercio c
  ON  c.id_comercio = p.id_comercio
  AND c.id_bandera  = p.id_bandera
  AND c._fecha_datos = p._fecha_datos
WHERE p._fecha_datos = DATE(:fecha_datos)
  AND try_cast(p.productos_precio_lista AS DOUBLE) = 0
GROUP BY ALL
ORDER BY n_precio_cero DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Sucursales: geo, provincia, padding, tipo
-- MAGIC
-- MAGIC Esperado:
-- MAGIC - sin nombre = **0**
-- MAGIC - provincia no ISO (`AR-B`): pocas (DIA eCommerce escribió `Buenos Aires`)
-- MAGIC - sin provincia: Comodín y parte de Carrefour
-- MAGIC - tipos con mayúsculas mezcladas (`Supermercado` / `supermercado`) → Silver

-- COMMAND ----------

SELECT
  count(*) AS sucursales,
  count_if(sucursales_nombre IS NULL OR trim(sucursales_nombre) = '') AS sin_nombre,
  count_if(sucursales_nombre <> trim(sucursales_nombre)) AS nombre_padding,
  count_if(sucursales_provincia IS NULL OR trim(sucursales_provincia) = '') AS sin_prov,
  count_if(sucursales_provincia NOT RLIKE '^AR-[A-Z]$') AS prov_no_iso,
  count_if(try_cast(sucursales_latitud AS DOUBLE) IS NULL
        OR try_cast(sucursales_longitud AS DOUBLE) IS NULL) AS sin_geo,
  collect_set(sucursales_tipo) AS tipos
FROM sepa.bronze.sucursales
WHERE _fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

SELECT id_comercio,
       count(*) AS sucursales,
       count_if(sucursales_provincia IS NULL OR trim(sucursales_provincia) = '') AS sin_prov,
       count_if(try_cast(sucursales_latitud AS DOUBLE) IS NULL
             OR try_cast(sucursales_longitud AS DOUBLE) IS NULL) AS sin_geo
FROM sepa.bronze.sucursales
WHERE _fecha_datos = DATE(:fecha_datos)
GROUP BY ALL
HAVING sin_prov > 0 OR sin_geo > 0
ORDER BY sucursales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Integridad productos ↔ sucursales
-- MAGIC
-- MAGIC Precios de una sucursal que no está en `sucursales` (huérfanas) y sucursales sin ningún precio.
-- MAGIC No se descartan en Bronze. Silver usa sucursal placeholder.

-- COMMAND ----------

WITH p AS (
  SELECT DISTINCT id_comercio, id_bandera, id_sucursal
  FROM sepa.bronze.productos WHERE _fecha_datos = DATE(:fecha_datos)
),
s AS (
  SELECT DISTINCT id_comercio, id_bandera, id_sucursal
  FROM sepa.bronze.sucursales WHERE _fecha_datos = DATE(:fecha_datos)
)
SELECT
  coalesce(p.id_comercio, s.id_comercio) AS id_comercio,
  coalesce(p.id_bandera, s.id_bandera) AS id_bandera,
  count_if(p.id_sucursal IS NOT NULL AND s.id_sucursal IS NOT NULL) AS en_ambos,
  count_if(s.id_sucursal IS NULL) AS precios_sin_sucursal,
  count_if(p.id_sucursal IS NULL) AS sucursal_sin_precios
FROM p
FULL OUTER JOIN s
  ON  p.id_comercio = s.id_comercio
  AND p.id_bandera  = s.id_bandera
  AND p.id_sucursal = s.id_sucursal
GROUP BY ALL
ORDER BY try_cast(id_comercio AS INT), id_bandera;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Cuarentena
-- MAGIC
-- MAGIC El ZIP original no se toca. Motivos válidos: `zip_corrupto`, `csv_ausente`, `parse_bajo`, `sin_filas_validas`.

-- COMMAND ----------

SELECT * FROM sepa.bronze.cuarentena_archivos
WHERE fecha_datos = DATE(:fecha_datos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Banderas cargadas (control visual)
-- MAGIC
-- MAGIC Padding en el nombre (Unicoop) y `version_sepa` vacía o `1` vs `1.0`: no son error de Bronze.

-- COMMAND ----------

SELECT id_comercio, id_bandera, comercio_cuit, comercio_razon_social, comercio_bandera_nombre,
       comercio_version_sepa,
       length(comercio_bandera_nombre) - length(trim(comercio_bandera_nombre)) AS espacios_nombre
FROM sepa.bronze.comercio
WHERE _fecha_datos = DATE(:fecha_datos)
ORDER BY try_cast(id_comercio AS INT), id_bandera;
