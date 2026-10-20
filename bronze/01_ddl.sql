-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze — DDL
-- MAGIC
-- MAGIC Contrato: `bronze/SPEC.md`.
-- MAGIC
-- MAGIC Crea el schema, el volume de extractos (carpeta de trabajo) y las cinco tablas.
-- MAGIC `IF NOT EXISTS`: se puede correr siempre; no pisa datos.
-- MAGIC
-- MAGIC Correr **antes** de `02_etl`. En el Workflow: task DDL → task ETL.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sepa.bronze
COMMENT 'Raw tabular SEPA. Contrato en bronze/SPEC.md';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS sepa.bronze.extract
COMMENT 'CSV extraidos del ZIP diario. Se reescriben por fecha_datos. No es la fuente de verdad.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.bronze.comercio (
  id_comercio                      STRING,
  id_bandera                       STRING,
  comercio_cuit                    STRING,
  comercio_razon_social            STRING,
  comercio_bandera_nombre          STRING,
  comercio_bandera_url             STRING,
  comercio_ultima_actualizacion    STRING,
  comercio_version_sepa            STRING,
  _fecha_datos                     DATE,
  _lote_id                         STRING,
  _source_path                     STRING,
  _ingest_ts                       TIMESTAMP,
  _row_hash                        STRING,
  _footer_ts                       STRING
)
USING DELTA
PARTITIONED BY (_fecha_datos)
COMMENT 'Bronze SEPA: banderas x dia. Columnas fuente STRING.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.bronze.sucursales (
  id_comercio                              STRING,
  id_bandera                               STRING,
  id_sucursal                              STRING,
  sucursales_nombre                        STRING,
  sucursales_tipo                          STRING,
  sucursales_calle                         STRING,
  sucursales_numero                        STRING,
  sucursales_latitud                       STRING,
  sucursales_longitud                      STRING,
  sucursales_observaciones                 STRING,
  sucursales_barrio                        STRING,
  sucursales_codigo_postal                 STRING,
  sucursales_localidad                     STRING,
  sucursales_provincia                     STRING,
  sucursales_lunes_horario_atencion        STRING,
  sucursales_martes_horario_atencion       STRING,
  sucursales_miercoles_horario_atencion    STRING,
  sucursales_jueves_horario_atencion       STRING,
  sucursales_viernes_horario_atencion      STRING,
  sucursales_sabado_horario_atencion       STRING,
  sucursales_domingo_horario_atencion      STRING,
  _fecha_datos                             DATE,
  _lote_id                                 STRING,
  _source_path                             STRING,
  _ingest_ts                               TIMESTAMP,
  _row_hash                                STRING,
  _footer_ts                               STRING
)
USING DELTA
PARTITIONED BY (_fecha_datos)
COMMENT 'Bronze SEPA: sucursales x dia. Columnas fuente STRING.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.bronze.productos (
  id_comercio                              STRING,
  id_bandera                               STRING,
  id_sucursal                              STRING,
  id_producto                              STRING,
  productos_ean                            STRING,
  productos_descripcion                    STRING,
  productos_cantidad_presentacion          STRING,
  productos_unidad_medida_presentacion     STRING,
  productos_marca                          STRING,
  productos_precio_lista                   STRING,
  productos_precio_referencia              STRING,
  productos_cantidad_referencia            STRING,
  productos_unidad_medida_referencia       STRING,
  productos_precio_unitario_promo1         STRING,
  productos_leyenda_promo1                 STRING,
  productos_precio_unitario_promo2         STRING,
  productos_leyenda_promo2                 STRING,
  _fecha_datos                             DATE,
  _lote_id                                 STRING,
  _source_path                             STRING,
  _ingest_ts                               TIMESTAMP,
  _row_hash                                STRING,
  _footer_ts                               STRING
)
USING DELTA
PARTITIONED BY (_fecha_datos)
COMMENT 'Bronze SEPA: producto x sucursal x dia. Columnas fuente STRING.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.bronze.cuarentena_archivos (
  lote_id            STRING,
  fecha_datos        DATE,
  id_comercio_zip    STRING,
  archivo            STRING,
  motivo             STRING,
  detalle            STRING,
  ruta_origen        STRING,
  ts_cuarentena      TIMESTAMP
)
COMMENT 'Archivos de un lote que no se pudieron parsear. El ZIP original sigue en Landing.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.bronze.lotes_procesados (
  lote_id        STRING,
  fecha_datos    DATE,
  ts_proceso     TIMESTAMP,
  n_comercio     BIGINT,
  n_sucursales   BIGINT,
  n_productos    BIGINT,
  n_cuarentena   BIGINT,
  estado         STRING
)
COMMENT 'Control: que lotes de Landing ya estan en Bronze.';
