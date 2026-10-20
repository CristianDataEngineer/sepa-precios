-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS sepa.silver
COMMENT 'Entidades y calidad SEPA. Contrato en silver/SPEC.md';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.provincia (
  provincia_iso     STRING    NOT NULL COMMENT 'PK. AR-B, AR-C, ...',
  provincia_nombre  STRING             COMMENT 'Nombre de la referencia'
)
USING DELTA
COMMENT 'Traductor ISO 3166-2. Carga desde maestro-provincias.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.unidad_medida (
  unidad_raw      STRING    NOT NULL COMMENT 'PK. Ya normalizada (minusculas, sin punto)',
  unidad_canon    STRING             COMMENT 'g / ml / un / m / m2',
  factor_a_base   DECIMAL(18, 6)     COMMENT 'Multiplicador a la unidad base'
)
USING DELTA
COMMENT 'Mapeo unidad cruda SEPA -> unidad canon. Contrato silver/SPEC.md';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.comercio (
  id_comercio        STRING    NOT NULL,
  id_bandera         STRING    NOT NULL,
  comercio_cuit      STRING,
  razon_social       STRING,
  bandera_nombre     STRING,
  bandera_url        STRING,
  tipo_comercio      STRING    COMMENT 'supermercado / estacion / otro',
  fecha_ultima_foto  DATE,
  _lote_id           STRING
)
USING DELTA
COMMENT 'Silver SEPA: una bandera, ultimo estado (tipo 1).';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.sucursal (
  id_comercio      STRING    NOT NULL,
  id_bandera       STRING    NOT NULL,
  id_sucursal      STRING    NOT NULL COMMENT 'BK. _DESCONOCIDA si placeholder',
  sucursal_nombre  STRING,
  sucursal_tipo    STRING             COMMENT 'Hipermercado / Supermercado / Autoservicio / Web / otro',
  calle            STRING,
  numero           STRING,
  latitud          DECIMAL(12, 8),
  longitud         DECIMAL(12, 8),
  observaciones    STRING,
  barrio           STRING,
  codigo_postal    STRING,
  localidad        STRING,
  provincia_iso    STRING             COMMENT 'FK a provincia. NULL si no se mapeo',
  horario_lunes    STRING,
  horario_martes   STRING,
  horario_miercoles STRING,
  horario_jueves   STRING,
  horario_viernes  STRING,
  horario_sabado   STRING,
  horario_domingo  STRING,
  es_placeholder   BOOLEAN,
  vigencia_desde   DATE,
  vigencia_hasta   DATE              COMMENT 'NULL = vigente',
  es_vigente       BOOLEAN,
  _lote_id         STRING
)
USING DELTA
COMMENT 'Silver SEPA: un local x version (SCD2). Horarios versionan aunque Gold no los grafique.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.producto (
  id_producto        STRING    NOT NULL COMMENT 'BK = EAN',
  descripcion_canon  STRING,
  marca_canon        STRING,
  cantidad_canon     DECIMAL(18, 6),
  unidad_canon       STRING,
  vigencia_desde     DATE,
  vigencia_hasta     DATE,
  es_vigente         BOOLEAN,
  _lote_id           STRING
)
USING DELTA
COMMENT 'Silver SEPA: un EAN global x version (SCD2). Sin codigos internos.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.producto_comercio (
  id_comercio        STRING    NOT NULL,
  id_bandera         STRING    NOT NULL,
  id_producto        STRING    NOT NULL,
  es_ean_global      BOOLEAN,
  descripcion        STRING,
  marca              STRING,
  cantidad_canon     DECIMAL(18, 6),
  unidad_canon       STRING,
  fecha_ultima_foto  DATE,
  _lote_id           STRING
)
USING DELTA
COMMENT 'Silver SEPA: producto como lo publica una bandera (tipo 1). Incluye internos.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.precio_diario (
  fecha_datos              DATE      NOT NULL,
  id_comercio              STRING    NOT NULL,
  id_bandera               STRING    NOT NULL,
  id_sucursal_fuente       STRING    NOT NULL COMMENT 'Codigo que mando SEPA',
  id_sucursal              STRING    NOT NULL COMMENT 'Join a sucursal: fuente o _DESCONOCIDA',
  id_producto              STRING    NOT NULL,
  es_ean_global            BOOLEAN,
  precio_lista             DECIMAL(18, 4),
  precio_promo             DECIMAL(18, 4),
  precio_efectivo          DECIMAL(18, 4),
  precio_por_unidad_canon  DECIMAL(18, 4),
  cantidad_canon           DECIMAL(18, 6),
  unidad_canon             STRING,
  _is_outlier              BOOLEAN,
  _tiene_conflicto_precio  BOOLEAN,
  _lote_id                 STRING
)
USING DELTA
PARTITIONED BY (fecha_datos)
COMMENT 'Silver SEPA: producto x sucursal x dia.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.silver.lotes_procesados (
  fecha_datos         DATE    NOT NULL COMMENT 'PK',
  lote_id             STRING,
  ts_proceso          TIMESTAMP,
  n_comercio          BIGINT,
  n_sucursal_vigente  BIGINT,
  n_producto_vigente  BIGINT,
  n_precio_diario     BIGINT,
  n_outlier           BIGINT,
  n_placeholder       BIGINT,
  estado              STRING
)
USING DELTA
COMMENT 'Control: que fecha_datos de Bronze ya estan en Silver.';