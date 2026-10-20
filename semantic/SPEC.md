# Semantic — SPEC

Capa de **métricas**. No es ETL de tablas: son **views** con `GROUP BY` sobre Gold.
No lee Silver ni Bronze. El dashboard no hace `AVG` distinto.

Contrato de entrada: `gold/SPEC.md`.

## Propósito

Publicar las 4 métricas del video con **una** fórmula cada una.
Si alguien toca Silver, se rompe Gold; semantic sigue exponiendo las mismas columnas.

## Qué NO se hace acá

- Re-limpiar EAN / unidades / outliers.
- Elegir la canasta (eso es `gold.canasta_ean`).
- Exponer `_lote_id` ni flags internos.

## Views

Schema `semantic`.

### `semantic.v_precio_bandera_dia`

`GROUP BY` de `gold.fact_precio`. Textos desde `dim_comercio` y `dim_producto` (no van en la fact).

`precio_mediano_bandera` = mediana de `precio_efectivo` entre sucursales.
`brecha_entre_banderas_pct` = `(mediana − min mercado ese EAN ese día) / min`.

| Columna | Qué es |
|---|---|
| `fecha` | |
| `id_comercio` / `id_bandera` / `bandera` | cadena (nombre actual) |
| `id_producto` / `producto` / `marca` | EAN + texto |
| `en_canasta` | está en `canasta_ean` |
| `precio_mediano_bandera` | métrica |
| `n_sucursales` | |
| `share_en_promo` | |
| `brecha_entre_banderas_pct` | métrica |

### `semantic.v_indice_canasta`

Sobre `v_precio_bandera_dia` ∩ canasta.

Índice (peso igual): promedio de `precio_hoy / precio_base` × 100,
solo EAN que esa bandera tenía **el día base y ese día**.

- Día base → 100.
- `entra_ranking` si cobertura ≥ **0,70**.
- `variacion_7d` = `indice_hoy / indice_t-7 − 1`. Sin día −7 → NULL.
- `brecha_canasta_pct` vs el índice mínimo entre las que `entra_ranking` ese día.

El ranking del video filtra `entra_ranking = true`.

| Columna | Qué es |
|---|---|
| `fecha` | |
| `id_comercio` / `id_bandera` / `bandera` | |
| `cobertura_pct` | |
| `entra_ranking` | |
| `indice_canasta` | métrica |
| `variacion_7d` | métrica |
| `brecha_canasta_pct` | |

### `semantic.v_sucursal_dia`

`GROUP BY` de `gold.fact_precio`: local × día. Nombre del local desde `dim_sucursal`; cadena desde `dim_comercio`.

### `semantic.v_locales`

`dim_sucursal` vigente × `dim_comercio`, sin placeholder. Padrón / mapa.

## Métricas (diccionario)

| Nombre | View | Fórmula |
|---|---|---|
| `precio_mediano_bandera` | `v_precio_bandera_dia` | mediana de `precio_efectivo` |
| `brecha_entre_banderas_pct` | `v_precio_bandera_dia` | `(mediana − min) / min` |
| `indice_canasta` | `v_indice_canasta` | 100 = día base; peso igual |
| `variacion_7d` | `v_indice_canasta` | `indice_hoy / indice_t-7 − 1` |

## Artefactos

| Archivo | Qué hace |
|---|---|
| `semantic/01_views.sql` | Las 4 views |
| `semantic/02_validaciones.sql` | Unicidad + índice 100 en día base |

## Owner

Cristian Barabas (Data Engineering).
