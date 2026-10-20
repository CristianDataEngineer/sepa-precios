# Bronze — SPEC

Capa **raw estructurada**. Abre los ZIP de Landing, los convierte en tablas Delta consultables y agrega
linaje. Las columnas que vienen de SEPA quedan **todas STRING**. No hay reglas de negocio: no se corrige
un EAN, no se traduce una provincia, no se compara una cadena con otra. Eso es Silver.

Contrato de entrada: `landing/SPEC.md`. Bronze lee `sepa.landing.lotes` y el ZIP en `ruta`.
Nunca escribe en el volume `raw`.

## Propósito

- Pasar de archivo (ZIP anidado) a tabla SQL.
- Registrar de qué lote y de qué archivo salió cada fila.
- Aislar archivos **ilegibles** (ZIP vacío, CSV multilínea roto) sin frenar el resto del día.

## Qué entra

Lotes de `sepa.landing.lotes` con `estado IN ('registrado', 'rectificativo')`.
Si hay dos lotes para la misma `fecha_datos`, se usa el de `ts_registro` más reciente (rectificativo gana).

Cada ZIP diario contiene una carpeta `yyyy-mm-dd/` y adentro un ZIP por comercio, con tres CSV
separados por `|`: `comercio.csv`, `sucursales.csv`, `productos.csv`.

## Transformaciones permitidas

- Descomprimir ZIP diario → ZIP por comercio → CSV.
- Parseo de formato (CSV `|` → filas). Encoding UTF-8 / UTF-8 BOM / cp1252: Spark lo resuelve.
- Filtrar el **footer** (`Ultima actualizacion: ...`): no es una fila de datos. El texto se guarda
  como `_footer_ts` (STRING) en las filas válidas de ese archivo.
- Nombres de columnas tal cual el header (ya vienen `snake_case`).
- Columnas de control: `_fecha_datos`, `_lote_id`, `_source_path`, `_ingest_ts`, `_row_hash`, `_footer_ts`.
- Partición física por `_fecha_datos`.

## Qué NO se hace acá

- Castear precios, fechas ni IDs a numérico/date.
- Deduplicar productos, unificar EAN, geocodificar, mapear unidades, calcular precio/kg.
- Filtrar “precios raros” de negocio (un TV de $11 M entra; Silver decide).
- Joins entre comercio / sucursales / productos.
- Reconstruir a mano un CSV multilínea (Comodín sucursales): va a cuarentena.

## Cuarentena (archivo, no fila)

Un archivo va a `sepa.bronze.cuarentena_archivos` y **no se carga** a la tabla destino cuando:

| Motivo | Ejemplo |
|---|---|
| `zip_corrupto` | ZIP de comercio de 0 bytes (comercio 36 en el EDA) |
| `csv_ausente` | El ZIP de comercio no trae uno de los tres CSV |
| `parse_bajo` | Menos del 80 % de las filas (sin contar footer) tienen `id_comercio` numérico |
| `sin_filas_validas` | El CSV se abre pero no queda ninguna fila con `id_comercio` numérico |

El resto de los archivos del mismo día se cargan igual (`estado` del lote = `parcial` si hubo cuarentena,
`ok` si no). El ZIP original en Landing no se mueve ni se borra.

Umbral de parseo: parámetro `umbral_parse` (default `0.80`). El footer no cuenta en el denominador.

## Tablas

Catálogo `sepa`, schema `bronze`. Volumen de trabajo (extractos, se reescriben):
`/Volumes/sepa/bronze/extract/fecha_datos=<yyyy-mm-dd>/comercio_<id>/<archivo>.csv.gz`

### Columnas de control (las tres entidades)

| Columna | Tipo | Descripción |
|---|---|---|
| `_fecha_datos` | DATE | Día de los precios. Partición. Viene de `landing.lotes.fecha_datos` |
| `_lote_id` | STRING | FK a `landing.lotes.lote_id` (sha256 del ZIP diario) |
| `_source_path` | STRING | Path del CSV extraído (incluye id de comercio) |
| `_ingest_ts` | TIMESTAMP | Cuándo corrió esta carga a Bronze |
| `_row_hash` | STRING | SHA-256 de las columnas de SEPA concatenadas. Detecta cambios de contenido |
| `_footer_ts` | STRING | Texto del footer del CSV, tal cual. NULL si el archivo no tenía |

### `sepa.bronze.comercio`

Grain: una bandera × día. Columnas SEPA (STRING): `id_comercio`, `id_bandera`, `comercio_cuit`,
`comercio_razon_social`, `comercio_bandera_nombre`, `comercio_bandera_url`,
`comercio_ultima_actualizacion`, `comercio_version_sepa` + columnas de control.

### `sepa.bronze.sucursales`

Grain: una sucursal × día. Columnas SEPA (STRING): `id_comercio`, `id_bandera`, `id_sucursal`,
`sucursales_nombre`, `sucursales_tipo`, `sucursales_calle`, `sucursales_numero`, `sucursales_latitud`,
`sucursales_longitud`, `sucursales_observaciones`, `sucursales_barrio`, `sucursales_codigo_postal`,
`sucursales_localidad`, `sucursales_provincia`, `sucursales_lunes_horario_atencion`,
`sucursales_martes_horario_atencion`, `sucursales_miercoles_horario_atencion`,
`sucursales_jueves_horario_atencion`, `sucursales_viernes_horario_atencion`,
`sucursales_sabado_horario_atencion`, `sucursales_domingo_horario_atencion` + control.

### `sepa.bronze.productos`

Grain: producto × sucursal × día. Columnas SEPA (STRING): `id_comercio`, `id_bandera`, `id_sucursal`,
`id_producto`, `productos_ean`, `productos_descripcion`, `productos_cantidad_presentacion`,
`productos_unidad_medida_presentacion`, `productos_marca`, `productos_precio_lista`,
`productos_precio_referencia`, `productos_cantidad_referencia`, `productos_unidad_medida_referencia`,
`productos_precio_unitario_promo1`, `productos_leyenda_promo1`, `productos_precio_unitario_promo2`,
`productos_leyenda_promo2` + control.

### `sepa.bronze.cuarentena_archivos`

Una fila por archivo rechazado.

| Columna | Tipo | Descripción |
|---|---|---|
| `lote_id` | STRING | Lote Landing |
| `fecha_datos` | DATE | Día |
| `id_comercio_zip` | STRING | Id del ZIP de comercio, si se pudo leer el nombre |
| `archivo` | STRING | `comercio.csv` / `sucursales.csv` / `productos.csv` / ZIP |
| `motivo` | STRING | `zip_corrupto` / `csv_ausente` / `parse_bajo` / `sin_filas_validas` |
| `detalle` | STRING | Números o mensaje (filas leídas, % válido, excepción) |
| `ruta_origen` | STRING | Path del ZIP diario o del extracto |
| `ts_cuarentena` | TIMESTAMP | Cuándo se registró |

### `sepa.bronze.lotes_procesados`

Una fila por lote ya cargado. Bronze no vuelve a procesar un `lote_id` salvo que se pida explícito
(`forzar = true`).

| Columna | Tipo | Descripción |
|---|---|---|
| `lote_id` | STRING | Clave |
| `fecha_datos` | DATE | Día procesado |
| `ts_proceso` | TIMESTAMP | Fin de la carga |
| `n_comercio` | BIGINT | Filas cargadas |
| `n_sucursales` | BIGINT | Filas cargadas |
| `n_productos` | BIGINT | Filas cargadas |
| `n_cuarentena` | BIGINT | Archivos en cuarentena |
| `estado` | STRING | `ok` / `parcial` |

## Idempotencia

Correr dos veces el mismo `fecha_datos` con el mismo `lote_id`:

- Si `forzar = false` y el lote ya está en `lotes_procesados` → no hace nada.
- Si `forzar = true` (o el lote es nuevo): `DELETE` de las tres tablas y de cuarentena para esa
  `_fecha_datos`, vuelve a extraer y cargar.

No es append ciego: un día tiene una sola versión vigente en Bronze.

## Contrato hacia Silver

- Tablas particionadas por `_fecha_datos`, columnas SEPA STRING, linaje completo.
- Silver lee solo filas con `id_comercio` numérico (Bronze ya filtró el footer).
- Archivos en cuarentena no están en las tablas de entidad: Silver no los “arregla”.
- Tests mínimos de Bronze (post-carga): `id_comercio` no nulo, `_lote_id` no nulo, conteo de
  cuarentena visible.

## Artefactos

| Archivo | Qué hace |
|---|---|
| `bronze/01_ddl.sql` | Crea schema, volume `extract` y las cinco tablas |
| `bronze/02_etl.py` | Carga los lotes pendientes, o un solo `fecha_datos`. Parsea CSV, cuarentena, Delta |
| `bronze/03_validaciones.sql` | Chequeos post-carga (duplicados, nulos, footer, geo). No escribe |

## Owner

Cristian Barabas (Data Engineering).
