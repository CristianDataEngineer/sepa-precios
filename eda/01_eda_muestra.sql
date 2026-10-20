-- Databricks notebook source
-- MAGIC %md
-- MAGIC # EDA SEPA — paso 1: exploración de la muestra (SQL)
-- MAGIC
-- MAGIC Muestra: día 2026-10-09, comercios 2 (La Anónima/Topsy/Bomba), 6 (Comodín/Maxi Comodín),
-- MAGIC 9 (Vea/Disco/Jumbo), 12 (Coto), 21 (Toledo), 23 (Axion) y 36 (ZIP corrupto).
-- MAGIC
-- MAGIC Fuente: `/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/comercio_*/{comercio,sucursales,productos}.csv.gz`
-- MAGIC (UTF-8, generados por `00_extraer_y_perfilar_muestra.py`).
-- MAGIC
-- MAGIC Las tablas `sepa.eda.*` son desechables: todo STRING, sin limpieza, con `_comercio_dir` y `_source_path`.
-- MAGIC Resultados y conclusiones en `eda/HALLAZGOS.md`.

-- COMMAND ----------

-- MAGIC %md ## 0. Materializar la muestra (todo STRING)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sepa.eda
COMMENT 'Tablas desechables de exploracion sobre la muestra 2026-10-09. No forman parte del pipeline.';

-- COMMAND ----------

CREATE OR REPLACE TABLE sepa.eda.productos_muestra AS
SELECT * EXCEPT (_rescued_data),
       regexp_extract(_metadata.file_path, 'comercio_(\\d+)/', 1) AS _comercio_dir,
       _metadata.file_path AS _source_path
FROM read_files('/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/*/productos.csv.gz',
                format => 'csv', sep => '|', header => true, inferSchema => false);

CREATE OR REPLACE TABLE sepa.eda.sucursales_muestra AS
SELECT * EXCEPT (_rescued_data),
       regexp_extract(_metadata.file_path, 'comercio_(\\d+)/', 1) AS _comercio_dir,
       _metadata.file_path AS _source_path
FROM read_files('/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/*/sucursales.csv.gz',
                format => 'csv', sep => '|', header => true, inferSchema => false);

CREATE OR REPLACE TABLE sepa.eda.comercio_muestra AS
SELECT * EXCEPT (_rescued_data),
       regexp_extract(_metadata.file_path, 'comercio_(\\d+)/', 1) AS _comercio_dir,
       _metadata.file_path AS _source_path
FROM read_files('/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/*/comercio.csv.gz',
                format => 'csv', sep => '|', header => true, inferSchema => false);

-- COMMAND ----------

-- MAGIC %md ## 1. Footer: cada CSV termina con una línea `Ultima actualizacion: <ts>` que entra como fila

-- COMMAND ----------

SELECT _comercio_dir, count(*) AS filas,
       count_if(_rescued_data IS NULL) AS sin_rescued,
       count_if(id_comercio RLIKE '(?i)ltima actualizaci') AS filas_footer,
       max(CASE WHEN NOT (id_comercio RLIKE '^[0-9]+$') THEN id_comercio END) AS texto_footer
FROM read_files('/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/*/productos.csv.gz',
                format => 'csv', sep => '|', header => true, inferSchema => false)
GROUP BY ALL ORDER BY _comercio_dir;

-- COMMAND ----------

-- MAGIC %md ## 2. comercio.csv: una fila por bandera; cadenas multi-bandera

-- COMMAND ----------

SELECT id_comercio, id_bandera, comercio_cuit, comercio_razon_social, comercio_bandera_nombre,
       comercio_ultima_actualizacion, comercio_version_sepa
FROM sepa.eda.comercio_muestra
WHERE id_comercio RLIKE '^[0-9]+$'
ORDER BY try_cast(id_comercio AS INT), id_bandera;

-- COMMAND ----------

-- MAGIC %md ## 3. sucursales.csv: cardinalidad, geo, provincia ISO, padding, tipos

-- COMMAND ----------

WITH s AS (SELECT * FROM sepa.eda.sucursales_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, id_bandera, count(*) AS sucursales, count(DISTINCT id_sucursal) AS ids_distintos,
       count_if(try_cast(sucursales_latitud AS DOUBLE) IS NULL OR try_cast(sucursales_longitud AS DOUBLE) IS NULL) AS sin_geo,
       count_if(try_cast(sucursales_latitud AS DOUBLE) NOT BETWEEN -56 AND -21
             OR try_cast(sucursales_longitud AS DOUBLE) NOT BETWEEN -74 AND -53) AS geo_fuera_arg,
       count_if(sucursales_provincia NOT RLIKE '^AR-[A-Z]$') AS provincia_no_iso,
       count_if(sucursales_nombre <> trim(sucursales_nombre)) AS nombre_con_padding,
       collect_set(sucursales_tipo) AS tipos
FROM s GROUP BY ALL ORDER BY try_cast(id_comercio AS INT), id_bandera;

-- COMMAND ----------

-- Comodín (6): registros multilínea sin comillas. multiLine => true NO lo arregla.
SELECT count(*) AS filas,
       count_if(id_comercio RLIKE '^[0-9]+$') AS filas_con_id_numerico,
       count_if(id_comercio RLIKE '^[0-9]+$' AND sucursales_provincia RLIKE '^AR-[A-Z]$') AS filas_completas
FROM read_files('/Volumes/sepa/landing/eda/fecha_datos=2026-10-09/comercio_6/sucursales.csv.gz',
                format => 'csv', sep => '|', header => true, inferSchema => false, multiLine => true);

-- COMMAND ----------

-- MAGIC %md ## 4. Integridad referencial productos ↔ sucursales

-- COMMAND ----------

WITH p AS (SELECT DISTINCT id_comercio, id_bandera, id_sucursal FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$'),
     s AS (SELECT DISTINCT id_comercio, id_bandera, id_sucursal FROM sepa.eda.sucursales_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT coalesce(p.id_comercio, s.id_comercio) AS id_comercio, coalesce(p.id_bandera, s.id_bandera) AS id_bandera,
       count_if(p.id_sucursal IS NOT NULL AND s.id_sucursal IS NOT NULL) AS en_ambos,
       count_if(s.id_sucursal IS NULL) AS en_productos_sin_sucursal,
       count_if(p.id_sucursal IS NULL) AS en_sucursales_sin_precios
FROM p FULL OUTER JOIN s USING (id_comercio, id_bandera, id_sucursal)
GROUP BY ALL ORDER BY try_cast(id_comercio AS INT), id_bandera;

-- COMMAND ----------

-- MAGIC %md ## 5. productos.csv: volumen, clave natural y flag EAN

-- COMMAND ----------

WITH p AS (SELECT * FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, id_bandera, count(*) AS filas, count(DISTINCT id_sucursal) AS sucursales,
       count(DISTINCT id_producto) AS productos_distintos,
       round(count(*) / count(DISTINCT id_sucursal)) AS filas_por_sucursal,
       count_if(productos_ean = '1') AS ean_flag_1, count_if(productos_ean = '0') AS ean_flag_0,
       count_if(productos_ean NOT IN ('0', '1') OR productos_ean IS NULL) AS ean_flag_otro,
       count_if(id_producto NOT RLIKE '^[0-9]{8}$|^[0-9]{12,14}$') AS id_producto_len_invalida,
       count_if(productos_ean = '1' AND id_producto LIKE '2%' AND length(id_producto) = 13) AS flag1_prefijo2_pesable
FROM p GROUP BY ALL ORDER BY try_cast(id_comercio AS INT), id_bandera;

-- COMMAND ----------

-- Duplicados sobre la clave natural (comercio, bandera, sucursal, producto). Esperado: 0 filas.
WITH p AS (SELECT * FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, id_bandera, id_sucursal, id_producto, count(*) AS n
FROM p GROUP BY ALL HAVING n > 1;

-- COMMAND ----------

-- MAGIC %md ## 6. Precios: formato, nulos, extremos

-- COMMAND ----------

WITH p AS (SELECT *, try_cast(productos_precio_lista AS DECIMAL(18,4)) AS precio
           FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, count(*) AS filas,
       count_if(productos_precio_lista IS NULL OR trim(productos_precio_lista) = '') AS precio_vacio,
       count_if(productos_precio_lista IS NOT NULL AND precio IS NULL) AS precio_no_numerico,
       count_if(productos_precio_lista LIKE '%,%') AS con_coma,
       count_if(precio = 0) AS precio_cero, count_if(precio < 0) AS precio_negativo,
       round(percentile_approx(precio, 0.01), 2) AS p01, round(percentile_approx(precio, 0.5), 2) AS p50,
       round(percentile_approx(precio, 0.99), 2) AS p99, max(precio) AS max_precio,
       count_if(precio > 0 AND precio < 100) AS menor_100, count_if(precio > 1000000) AS mayor_1M
FROM p GROUP BY ALL ORDER BY try_cast(id_comercio AS INT);

-- COMMAND ----------

-- Qué hay en los extremos: >1M son electro (legítimos); <100 son pesables con precio simbólico y envases.
WITH p AS (SELECT *, try_cast(productos_precio_lista AS DECIMAL(18,4)) AS precio
           FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
(SELECT 'mayor_1M' AS grupo, id_comercio, id_producto, left(productos_descripcion, 60) AS descripcion, precio, count(*) AS sucursales
 FROM p WHERE precio > 1000000 GROUP BY ALL ORDER BY precio DESC LIMIT 8)
UNION ALL
(SELECT 'menor_100', id_comercio, id_producto, left(productos_descripcion, 60), precio, count(*)
 FROM p WHERE precio > 0 AND precio < 100 GROUP BY ALL ORDER BY precio LIMIT 8);

-- COMMAND ----------

-- MAGIC %md ## 7. Promociones

-- COMMAND ----------

WITH p AS (SELECT *, try_cast(productos_precio_unitario_promo1 AS DECIMAL(18,4)) AS promo1,
                     try_cast(productos_precio_unitario_promo2 AS DECIMAL(18,4)) AS promo2,
                     try_cast(productos_precio_lista AS DECIMAL(18,4)) AS precio
           FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, count(*) AS filas,
       round(100 * count_if(promo1 IS NOT NULL) / count(*), 1) AS pct_promo1,
       round(100 * count_if(promo2 IS NOT NULL) / count(*), 1) AS pct_promo2,
       count_if(promo1 IS NOT NULL AND (productos_leyenda_promo1 IS NULL OR trim(productos_leyenda_promo1) = '')) AS promo1_sin_leyenda,
       count_if(promo1 >= precio) AS promo1_no_menor_que_lista,
       round(percentile_approx(CASE WHEN promo1 IS NOT NULL THEN 100 * (1 - promo1 / precio) END, 0.5), 1) AS descuento_mediano_pct,
       count(DISTINCT productos_leyenda_promo1) AS leyendas_distintas
FROM p GROUP BY ALL ORDER BY try_cast(id_comercio AS INT);

-- COMMAND ----------

-- MAGIC %md ## 8. Unidades y cantidades de presentación / referencia

-- COMMAND ----------

WITH p AS (SELECT * FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT lower(trim(productos_unidad_medida_presentacion)) AS unidad_pres, count(*) AS filas,
       collect_set(id_comercio) AS en_comercios,
       slice(collect_set(productos_cantidad_presentacion), 1, 6) AS ej_cantidades
FROM p GROUP BY ALL ORDER BY filas DESC;

-- COMMAND ----------

WITH p AS (SELECT * FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$')
SELECT id_comercio, count(*) AS filas,
       count_if(productos_marca IS NULL OR trim(productos_marca) = '') AS marca_vacia,
       count_if(upper(trim(productos_marca)) IN ('SIN MARCA', 'S/M', 'GENERICO', 'VARIOS', '-', '.')) AS marca_generica,
       count_if(productos_descripcion <> trim(productos_descripcion)) AS desc_con_padding,
       count_if(productos_descripcion RLIKE '\\s{2,}') AS desc_espacios_dobles,
       count_if(try_cast(productos_cantidad_presentacion AS DECIMAL(18,3)) IS NULL) AS cant_no_numerica,
       collect_set(lower(trim(productos_unidad_medida_referencia))) AS unidades_ref,
       collect_set(productos_cantidad_referencia) AS cantidades_ref
FROM p GROUP BY ALL ORDER BY try_cast(id_comercio AS INT);

-- COMMAND ----------

-- MAGIC %md ## 9. Solapamiento de EAN entre cadenas y consistencia de descripción / presentación

-- COMMAND ----------

WITH p AS (SELECT * FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$' AND productos_ean = '1'),
     e AS (SELECT id_producto, count(DISTINCT id_comercio) AS comercios,
                  count(DISTINCT lower(trim(productos_descripcion))) AS descripciones,
                  count(DISTINCT lower(trim(productos_marca))) AS marcas
           FROM p GROUP BY id_producto)
SELECT comercios, count(*) AS eans,
       count_if(descripciones > 1) AS con_desc_distinta, count_if(marcas > 1) AS con_marca_distinta
FROM e GROUP BY comercios ORDER BY comercios;

-- COMMAND ----------

-- Presentación con unidades normalizadas (g / ml / un / m / m2). Aun así ~40-60 % inconsistente: ver HALLAZGOS.
WITH p AS (SELECT *, try_cast(productos_cantidad_presentacion AS DECIMAL(18,3)) AS cant,
                     lower(trim(productos_unidad_medida_presentacion)) AS u
           FROM sepa.eda.productos_muestra WHERE id_comercio RLIKE '^[0-9]+$' AND productos_ean = '1'),
     n AS (SELECT *,
                  CASE WHEN u IN ('gr','gr.','grm','g','kg','kgm','kgr') THEN 'g'
                       WHEN u IN ('ml','ml.','cm3','cc','cc.','cmq','mi','lt','lt.','ltr','l','dm3') THEN 'ml'
                       WHEN u IN ('un','un.','uni','cu','pc','par','pie','hjs') THEN 'un'
                       WHEN u IN ('m','mt','mt.','mtr','cmt') THEN 'm'
                       WHEN u = 'm2' THEN 'm2' END AS u_canon,
                  CASE WHEN u IN ('kg','kgm','kgr','lt','lt.','ltr','l','dm3') THEN cant * 1000
                       WHEN u = 'cmt' THEN cant / 100 ELSE cant END AS cant_canon
           FROM p),
     e AS (SELECT id_producto, count(DISTINCT id_comercio) AS comercios,
                  count(DISTINCT concat_ws('|', cant_canon, u_canon)) AS presentaciones
           FROM n GROUP BY id_producto HAVING comercios >= 2)
SELECT comercios, count(*) AS eans, count_if(presentaciones > 1) AS con_presentacion_distinta,
       round(100 * count_if(presentaciones = 1) / count(*), 1) AS pct_consistentes
FROM e GROUP BY comercios ORDER BY comercios;

-- COMMAND ----------

-- MAGIC %md ## 10. La pregunta de negocio: dispersión de precio del mismo EAN entre banderas

-- COMMAND ----------

WITH p AS (SELECT p.*, c.comercio_bandera_nombre AS bandera, try_cast(productos_precio_lista AS DECIMAL(18,2)) AS precio
           FROM sepa.eda.productos_muestra p
           JOIN sepa.eda.comercio_muestra c ON c.id_comercio = p.id_comercio AND c.id_bandera = p.id_bandera
           WHERE p.id_comercio RLIKE '^[0-9]+$' AND productos_ean = '1'),
     top AS (SELECT id_producto FROM p GROUP BY id_producto HAVING count(DISTINCT id_comercio) = 5 ORDER BY count(*) DESC LIMIT 6),
     b AS (SELECT id_producto, bandera, percentile_approx(precio, 0.5) AS mediana, min(precio) AS min_p, max(precio) AS max_p,
                  count(*) AS sucursales, any_value(productos_descripcion) AS descripcion
           FROM p JOIN top USING (id_producto) GROUP BY ALL)
SELECT id_producto, left(descripcion, 45) AS descripcion, bandera, mediana, min_p, max_p, sucursales,
       round(100 * (mediana / min(mediana) OVER (PARTITION BY id_producto) - 1), 1) AS pct_sobre_mas_barata
FROM b ORDER BY id_producto, mediana;
