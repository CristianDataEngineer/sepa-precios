-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Landing — DDL
-- MAGIC
-- MAGIC Contrato: `landing/SPEC.md`.
-- MAGIC
-- MAGIC Crea el schema `sepa.landing` y la tabla de control `lotes` (índice de los ZIP).
-- MAGIC `IF NOT EXISTS`: se puede correr siempre; no pisa datos.
-- MAGIC
-- MAGIC El volume `raw` se crea una vez por UI (o por `CREATE VOLUME`). No se toca acá.
-- MAGIC
-- MAGIC Correr **antes** de `02_etl`. En el Workflow: task DDL → task ETL.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sepa.landing
COMMENT 'Landing SEPA: ZIP crudos + indice de lotes. Contrato en landing/SPEC.md';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.landing.lotes (
  lote_id       STRING    NOT NULL COMMENT 'sha256 del ZIP. Clave del lote',
  fecha_datos   DATE               COMMENT 'Carpeta de primer nivel dentro del ZIP. NULL si ilegible',
  dia_semana    STRING             COMMENT 'Del nombre del archivo: lunes..domingo',
  archivo       STRING             COMMENT 'Nombre final del ZIP',
  ruta          STRING             COMMENT 'Path completo en el volume',
  bytes         BIGINT             COMMENT 'Tamano del ZIP',
  sha256        STRING             COMMENT 'Huella del contenido',
  origen        STRING             COMMENT 'manual (UI) o job (descarga automatica)',
  url_descarga  STRING             COMMENT 'URL del recurso. NULL si manual',
  fecha_pagina  DATE               COMMENT 'Fecha que decia la web al descargar. Informativa',
  ts_llegada    TIMESTAMP          COMMENT 'Modificacion del archivo en el volume',
  ts_registro   TIMESTAMP          COMMENT 'Cuando corrio este registro',
  estado        STRING             COMMENT 'registrado / rectificativo / ilegible'
)
COMMENT 'Landing SEPA: un lote por ZIP diario. Contrato en landing/SPEC.md';
