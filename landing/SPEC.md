# Landing — SPEC

Capa de **crudo inmutable**. Guarda los archivos exactamente como llegan de la fuente y deja constancia de qué llegó.
No abre los CSV, no tipa, no valida contenido, no deduplica filas. Si algo de eso hace falta, es Bronze.

## Fuente

| | |
|---|---|
| Dataset | [Precios Claros – Base SEPA](https://datos.produccion.gob.ar/dataset/sepa-precios), Secretaría de Comercio |
| Recursos | 7 ZIP, uno por día de la semana (`sepa_lunes.zip` … `sepa_domingo.zip`), ~310 MB cada uno |
| Rotación | Cada recurso se **sobrescribe** cuando se publica el mismo día de la semana siguiente. No hay histórico oficial: lo construye esta capa |
| Publicación | Con 2–3 días de demora, a media mañana (observado 2026-10-13 10:43 ART). Puede haber reenvíos rectificativos |
| Referencia | `maestro-provincias.xlsx` (traductor ISO 3166-2 → nombre de provincia). Estático |

**La fecha del dato se toma solo del contenido del ZIP** (carpeta de primer nivel `yyyy-mm-dd/`).
La descripción del recurso en la web y el `last_modified` de la API CKAN quedan desactualizados; se guardan como
metadato informativo, nunca como fuente de verdad.

## Qué entra

- El ZIP diario completo, tal cual se descarga. No se guardan los ZIP internos por comercio sueltos.
- El Excel de referencia de provincias.

## Rutas (volume `sepa.landing.raw`)

```text
/Volumes/sepa/landing/raw/
├── sepa_minorista/
│   └── fecha_datos=<yyyy-mm-dd>/
│       ├── sepa_<dia>.zip                 ← original, byte por byte
│       └── sepa_<dia>.manifest.json       ← metadatos del lote
└── referencia/
    └── maestro-provincias.xlsx
```

Los archivos que llegan a mano se depositan en la raíz del volume; el notebook de registro los mueve a su carpeta
por fecha. El job de descarga automática (fase de orquestación) los deja directamente en la carpeta por fecha.

## Reglas

- **Append-only.** Nunca se modifica ni se borra un ZIP ya registrado. La carpeta por `fecha_datos` existe porque los
  nombres de archivo se repiten cada semana.
- **Un lote = un hash.** El `sha256` del ZIP es el `lote_id`. Mismo hash → mismo lote, no se registra dos veces.
- **Rectificativos.** Si para una `fecha_datos` ya registrada llega un ZIP con hash distinto, se guarda como
  `sepa_<dia>__<hash8>.zip` con `estado = 'rectificativo'`. Bronze decide cuál usar (por defecto, el último).
- **Ilegibles.** Si el ZIP no se puede abrir para leer la fecha, se registra con `estado = 'ilegible'` y se deja donde
  está. Landing no tiene cuarentena: la detección de archivos rotos es de Bronze.

## Manifiesto (`sepa_<dia>.manifest.json`)

Un JSON por ZIP con los mismos campos que la fila de `lotes` (abajo). Es la copia legible-sin-Databricks del registro.

## Tabla `sepa.landing.lotes`

Una fila por ZIP registrado. Es lo que Bronze consulta para saber qué lotes existen.

| Columna | Tipo | Descripción |
|---|---|---|
| `lote_id` | STRING | `sha256` del ZIP. Clave |
| `fecha_datos` | DATE | Carpeta de primer nivel dentro del ZIP. NULL si `ilegible` |
| `dia_semana` | STRING | Del nombre del archivo (`sabado`, `domingo`…) |
| `archivo` | STRING | Nombre final del ZIP |
| `ruta` | STRING | Path completo en el volume |
| `bytes` | BIGINT | Tamaño |
| `sha256` | STRING | Huella del contenido |
| `origen` | STRING | `manual` (subido por UI) o `job` (descargado por el pipeline) |
| `url_descarga` | STRING | URL del recurso. NULL si `manual` |
| `fecha_pagina` | DATE | Fecha que decía la web al descargar. Informativa. NULL si `manual` |
| `ts_llegada` | TIMESTAMP | Modificación del archivo en el volume (cuándo llegó) |
| `ts_registro` | TIMESTAMP | Cuándo corrió el registro |
| `estado` | STRING | `registrado` / `rectificativo` / `ilegible` |

Carga por `MERGE` sobre `lote_id`: correr el registro dos veces deja la tabla igual.

## Qué NO se hace acá

- Descomprimir los ZIP internos ni leer los CSV.
- Tipar, limpiar, filtrar footer, deduplicar.
- Decidir si un archivo es "bueno" o "malo" más allá de poder leerle la fecha.

## Contrato hacia Bronze

- Bronze procesa lotes de `sepa.landing.lotes` con `estado IN ('registrado', 'rectificativo')` que no estén en su
  propia tabla de control. Lee el ZIP desde `ruta`.
- Bronze nunca escribe en el volume `raw`.

## Artefactos

| Archivo | Qué hace |
|---|---|
| `landing/01_ddl.sql` | Crea schema `sepa.landing` y tabla `lotes` |
| `landing/02_etl.py` | Mueve ZIP a carpeta por fecha, manifiesto, `MERGE` en `lotes` |
| `landing/00_descargar_sepa.py` | *(fase de orquestación)* Baja los 7 recursos del portal al volume y los registra con `origen = 'job'` |

## Owner

Cristian Barabas (Data Engineering).
