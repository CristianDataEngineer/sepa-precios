# Gold — SPEC

Capa de **estrella**. Lee solo Silver. No re-limpia EAN, unidades ni outliers.
La fact trae **FKs + medidas**. Los nombres viven en las dims.
El `GROUP BY` (mediana, índice) vive en **semantic** (views).

Contrato de entrada: `silver/SPEC.md`.

## Propósito

Responder: **¿cuánto subió la canasta y qué cadena la vende más barata?**
Y poder ver los locales sin copiar 100 millones de precios.

## Estrella

```text
     dim_fecha          dim_comercio          dim_producto
         \                   |                     /
                     fact_precio
                         |
                   dim_sucursal
```

Semantic (no es otra fact): `v_precio_bandera_dia`, `v_indice_canasta`, `v_sucursal_dia`, `v_locales`.

## Decisiones (cerradas)

| # | Decisión |
|---|---|
| 1 | `fact_precio` es **tabla**. Cada corrida copia el comparable de Silver (FKs + medidas) |
| 2 | Fact = ids + precios. **No** `bandera_nombre` ni descripción |
| 3 | Dims: `dim_fecha`, `dim_comercio`, `dim_sucursal`, `dim_producto` |
| 4 | Comparable = supermercado + EAN global + no outlier + no placeholder |
| 5 | Canasta ~15 EAN, peso igual, ≥ 5 banderas el día base. Se congela |
| 6 | Índice 100 = primer día. `variacion_7d` vs 7 días atrás (semantic) |
| 7 | Dashboard lee **semantic** |
| 8 | Nombre de cadena = tipo 1 en `dim_comercio` |

## Qué NO se hace acá

- Recalcular GS1, unidades, outliers.
- `GROUP BY` / mediana / índice (semantic).
- Pegar textos de dim en la fact.

---

## `gold.fact_precio` (tabla)

Grano: producto × sucursal × día. FKs + medidas.

Filtro: supermercado, EAN global, no outlier, no placeholder.
Join sucursal solo para filtrar vigencia / placeholder; **no** selecciona nombre ni dirección.

| Columna | Tipo | Rol |
|---|---|---|
| `fecha_datos` | DATE | FK `dim_fecha` |
| `id_comercio` + `id_bandera` | STRING | FK `dim_comercio` |
| `id_sucursal` | STRING | FK `dim_sucursal` |
| `id_sucursal_fuente` | STRING | código SEPA |
| `id_producto` | STRING | FK `dim_producto` |
| `precio_lista` / `precio_promo` / `precio_efectivo` | DECIMAL | medidas |
| `precio_por_unidad_canon` | DECIMAL | medida |
| `cantidad_canon` / `unidad_canon` | | presentación de **esa** fila (puede variar por local) |
| `_lote_id` | STRING | linaje |

## `gold.dim_fecha` (SCD0)

Calendario entre la primera y la última fecha de `fact_precio`. Cada corrida lo rearma; los atributos de una fecha no cambian.

| Columna | Tipo |
|---|---|
| `fecha` | DATE PK |
| `anio` / `mes` / `dia` | INT |
| `dia_semana` | INT 1=lunes … 7=domingo |
| `nombre_dia` | STRING |
| `es_finde` | BOOLEAN |
| `es_fecha_base` | BOOLEAN primer día = índice 100 |

## `gold.dim_comercio` (tipo 1)

Una bandera, último nombre. Copia de `silver.comercio`.

| Columna | Tipo |
|---|---|
| `id_comercio` + `id_bandera` | BK |
| `bandera_nombre` | cartel actual |
| `razon_social` | |
| `tipo_comercio` | |

## `gold.dim_sucursal` (SCD2)

Una **versión** de un local. **No** trae `bandera_nombre` (eso es `dim_comercio`).
Provincia pegada (sin copo de nieve).

| Columna | Tipo |
|---|---|
| `id_comercio` + `id_bandera` + `id_sucursal` | BK |
| `sucursal_nombre` / `sucursal_tipo` | |
| `calle` / `numero` / lat / long | |
| `localidad` / `provincia_iso` / `provincia_nombre` | |
| `es_placeholder` | |
| `vigencia_desde` / `vigencia_hasta` / `es_vigente` | |

## `gold.dim_producto` (SCD2)

Una versión de un EAN. Copia de `silver.producto`.

| Columna | Tipo |
|---|---|
| `id_producto` | BK |
| `descripcion_canon` / `marca_canon` | |
| `cantidad_canon` / `unidad_canon` | presentación canónica |
| `vigencia_desde` / `vigencia_hasta` / `es_vigente` | |

## `gold.canasta_ean`

Semilla. No se recalcula salvo `forzar_canasta=true`.

Regla: día base = min fecha de `fact_precio`; ≥ 5 banderas; un EAN por categoría de almacén.

| Columna | Tipo |
|---|---|
| `id_producto` | PK |
| `categoria` / `descripcion` | |
| `n_banderas_dia_base` / `fecha_base` | |

## Idempotencia

Cada corrida hace `INSERT OVERWRITE` de `fact_precio` y de las dims desde Silver.
`canasta_ean` se conserva si ya tiene filas. `forzar_canasta=true` la vacía y la vuelve a elegir.

## Contrato hacia semantic

Semantic lee Gold. JOIN fact → dims para los textos.
Métricas en `semantic/SPEC.md`.

## Tests mínimos

| Test | Esperado |
|---|---|
| `fact_precio` vs Silver comparable | mismas filas |
| Estaciones en `fact_precio` | 0 |
| Unicidad dims vigentes | 1 por BK |
| Canasta | 8–15 EAN, 1 por categoría |
| Placeholder en `fact_precio` | 0 |

## Artefactos

| Archivo | Qué hace |
|---|---|
| `gold/01_ddl.sql` | Schema, dims, canasta y tabla `fact_precio`. Si quedó la vista vieja, la baja |
| `gold/02_etl.sql` | `INSERT OVERWRITE` de fact y dims. Canasta solo si está vacía |
| `gold/03_validaciones.sql` | Tests de la estrella |
| `gold/MAPA.md` | Diagrama |

## Owner

Cristian Barabas (Data Engineering).
