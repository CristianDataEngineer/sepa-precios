-- Databricks notebook source
CREATE WIDGET TEXT fecha_datos DEFAULT "2026-10-09";

-- COMMAND ----------

SELECT '01 unicidad views' AS test;

SELECT 'v_precio_bandera_dia' AS vista,
       COUNT(*) AS filas,
       COUNT(*) - COUNT(DISTINCT concat_ws('|', CAST(fecha AS STRING), id_comercio, id_bandera, id_producto)) AS dup
FROM sepa.semantic.v_precio_bandera_dia
WHERE fecha = DATE(:fecha_datos)
UNION ALL
SELECT 'v_indice_canasta',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT concat_ws('|', CAST(fecha AS STRING), id_comercio, id_bandera))
FROM sepa.semantic.v_indice_canasta
WHERE fecha = DATE(:fecha_datos)
UNION ALL
SELECT 'v_sucursal_dia',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT concat_ws('|', CAST(fecha AS STRING), id_comercio, id_bandera, id_sucursal))
FROM sepa.semantic.v_sucursal_dia
WHERE fecha = DATE(:fecha_datos);

-- COMMAND ----------

SELECT '02 indice 100 en dia base si entra_ranking' AS test;

SELECT
  COUNT(*) AS banderas_ranking,
  COUNT_IF(ABS(indice_canasta - 100) > 0.01) AS distintas_de_100
FROM sepa.semantic.v_indice_canasta i
INNER JOIN sepa.gold.dim_fecha d
  ON d.fecha = i.fecha
WHERE d.es_fecha_base
  AND i.entra_ranking
  AND i.indice_canasta IS NOT NULL;

-- COMMAND ----------

SELECT fecha, COUNT(*) AS n_banderas, COUNT_IF(entra_ranking) AS n_ranking
FROM sepa.semantic.v_indice_canasta
GROUP BY fecha
ORDER BY fecha;
