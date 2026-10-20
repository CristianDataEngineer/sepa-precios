-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS sepa.gold
COMMENT 'Estrella SEPA. Contrato en gold/SPEC.md';

-- COMMAND ----------

DROP TABLE IF EXISTS sepa.gold.agg_precio_bandera_dia;
DROP TABLE IF EXISTS sepa.gold.agg_indice_canasta_dia;
DROP TABLE IF EXISTS sepa.gold.agg_sucursal_dia;
DROP TABLE IF EXISTS sepa.gold.fact_precio_bandera_dia;
DROP TABLE IF EXISTS sepa.gold.fact_indice_canasta_dia;
DROP TABLE IF EXISTS sepa.gold.fact_sucursal_dia;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.dim_fecha (
  fecha          DATE     NOT NULL COMMENT 'PK',
  anio           INT,
  mes            INT,
  dia            INT,
  dia_semana     INT               COMMENT '1=lunes ... 7=domingo',
  nombre_dia     STRING,
  es_finde       BOOLEAN,
  es_fecha_base  BOOLEAN           COMMENT 'Primer dia Silver = indice 100'
)
USING DELTA
COMMENT 'Gold SEPA: calendario SCD0';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.dim_comercio (
  id_comercio        STRING    NOT NULL,
  id_bandera         STRING    NOT NULL,
  bandera_nombre     STRING,
  razon_social       STRING,
  tipo_comercio      STRING
)
USING DELTA
COMMENT 'Gold SEPA: una bandera, ultimo estado (tipo 1)';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.dim_sucursal (
  id_comercio       STRING    NOT NULL,
  id_bandera        STRING    NOT NULL,
  id_sucursal       STRING    NOT NULL,
  sucursal_nombre   STRING,
  sucursal_tipo     STRING,
  calle             STRING,
  numero            STRING,
  latitud           DECIMAL(12, 8),
  longitud          DECIMAL(12, 8),
  localidad         STRING,
  provincia_iso     STRING,
  provincia_nombre  STRING,
  es_placeholder    BOOLEAN,
  vigencia_desde    DATE,
  vigencia_hasta    DATE,
  es_vigente        BOOLEAN
)
USING DELTA
COMMENT 'Gold SEPA: version de local (SCD2). Sin nombre de cadena: eso es dim_comercio';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.dim_producto (
  id_producto        STRING    NOT NULL,
  descripcion_canon  STRING,
  marca_canon        STRING,
  cantidad_canon     DECIMAL(18, 6),
  unidad_canon       STRING,
  vigencia_desde     DATE,
  vigencia_hasta     DATE,
  es_vigente         BOOLEAN
)
USING DELTA
COMMENT 'Gold SEPA: version de EAN (SCD2)';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.canasta_ean (
  id_producto           STRING    NOT NULL COMMENT 'PK = EAN',
  categoria             STRING,
  descripcion           STRING,
  n_banderas_dia_base   INT,
  fecha_base            DATE
)
USING DELTA
COMMENT 'Gold SEPA: semilla de canasta. Se congela.';

-- COMMAND ----------

-- MAGIC %python
# Si fact_precio sigue como vista, hay que bajarla para crear la tabla.
n = spark.sql("""
  SELECT 1
  FROM system.information_schema.tables
  WHERE table_catalog = 'sepa'
    AND table_schema  = 'gold'
    AND table_name    = 'fact_precio'
    AND table_type    = 'VIEW'
""").count()
if n:
    spark.sql("DROP VIEW sepa.gold.fact_precio")
    print("vista fact_precio bajada")
else:
    print("fact_precio: no hay vista previa")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sepa.gold.fact_precio (
  fecha_datos               DATE           NOT NULL COMMENT 'FK dim_fecha',
  id_comercio               STRING         NOT NULL COMMENT 'FK dim_comercio',
  id_bandera                STRING         NOT NULL COMMENT 'FK dim_comercio',
  id_sucursal               STRING         NOT NULL COMMENT 'FK dim_sucursal',
  id_sucursal_fuente        STRING                  COMMENT 'codigo SEPA',
  id_producto               STRING         NOT NULL COMMENT 'FK dim_producto',
  precio_lista              DECIMAL(18, 4),
  precio_promo              DECIMAL(18, 4),
  precio_efectivo           DECIMAL(18, 4),
  precio_por_unidad_canon   DECIMAL(18, 4),
  cantidad_canon            DECIMAL(18, 6),
  unidad_canon              STRING,
  _lote_id                  STRING
)
USING DELTA
PARTITIONED BY (fecha_datos)
COMMENT 'Fact atomica: FKs + medidas. Nombres en las dims.';
