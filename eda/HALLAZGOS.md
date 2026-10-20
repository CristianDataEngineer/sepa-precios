# EDA SEPA — Hallazgos de la muestra (2026-10-09)

Fuente: [Precios Claros – Base SEPA](https://datos.produccion.gob.ar/dataset/sepa-precios), ZIP del viernes
(`sepa_viernes.zip`, 323 MB, datos del 2026-10-09). El dataset publica 7 recursos (uno por día de la semana)
que se **sobrescriben cada 7 días**: no hay histórico oficial, lo construye el pipeline.

Muestra: 7 de los 17 ZIP por comercio del día.

| id | Razón social | Banderas | Filas precios | Sucursales | Motivo de inclusión |
|---|---|---|---|---|---|
| 2 | S.A. Imp. y Exp. de la Patagonia | La Anónima, Topsy, Bomba | 1.015.416 | 184 | Regional (Patagonia), multi-bandera |
| 6 | Alberdi S.A. | Comodín, Maxi Comodín | 18.642 | 26 | Chico (NOA); `sucursales.csv` roto |
| 9 | Cencosud S.A. | Vea, Disco, Jumbo | 1.277.563 | 268 | Nacional, 3 banderas con posicionamiento distinto |
| 12 | Coto CICSA | Coto | 1.063.362 | 120 | Nacional AMBA, mayor surtido (14.516 productos) |
| 21 | Supermercados Toledo | Toledo | 61.703 | 25 | Regional chico (Mar del Plata) |
| 23 | Pan American Energy | Axion Energy | 6.371 | 54 | Caso borde: tiendas de estación de servicio |
| 36 | — | — | — | — | ZIP de 0 bytes (corrupto) |

Total muestra: **3.443.057 filas** de precios. Proyección del día completo (sumando tamaños de los 17
`productos.csv`): ~1,5 GB sin comprimir ≈ **13 M filas/día**, consistente con los "12 millones" que declara la fuente.
Los grandes ausentes de la muestra son DIA (493 MB), Carrefour (372 MB) y Changomas (202 MB).

---

## 1. Formato de archivos

| Hallazgo | Detalle | Implicancia |
|---|---|---|
| ZIP anidado | ZIP diario → carpeta `yyyy-mm-dd/` → un ZIP por comercio `sepa_<n>_comercio-sepa-<id>_<fecha>_<hora>.zip` → `comercio.csv`, `sucursales.csv`, `productos.csv` | Landing guarda el ZIP diario tal cual; Bronze descomprime (PySpark/Python, no hay `read_files` para zip) |
| Separador `\|` | Sin comillas. Nunca aparece `,` como decimal | `sep => '\|'` |
| Esquema homogéneo | 8 / 21 / 17 columnas, mismos nombres en los 6 comercios, `comercio_version_sepa` = `1.0` (Toledo `1.00`, Mariano Max `1`) | Un solo schema Bronze por entidad; versionar igual |
| **Footer** | Todos los CSV terminan con línea vacía + `Ultima actualizacion: <ts>` (variantes: `Última actualización`, `Ultima Actualizacion`) | Entra como fila con `id_comercio` no numérico → filtrar en Bronze con `id_comercio RLIKE '^[0-9]+$'` y guardar el timestamp como metadato del lote |
| BOM UTF-8 | Presente en 4 de 6 comercios (no en Coto ni Axion) | `read_files` lo maneja; si se parsea a mano, usar `utf-8-sig` |
| CRLF | En todos | Sin impacto con `read_files` |
| **ZIP corrupto** | Comercio 36: 0 bytes | Cuarentena a nivel archivo; no debe abortar el lote |
| **Registros multilínea** | Comodín `sucursales.csv`: campos de horario/observaciones con saltos de línea sin comillas. 113 filas leídas, solo 1 completa. `multiLine => true` no lo arregla | Cuarentena de archivo con `parse_success_rate` por debajo del umbral. Reconstrucción manual queda fuera de alcance |
| Timestamps | `2026-10-09T04:00:00-03:00` y `-0300` (Axion) | `try_to_timestamp` con dos patrones |
| Datos viejos | `comercio_ultima_actualizacion` de Changomas es 2017/2021 y Unicoop 2025-06 (fuera de la muestra, visto en inventario) | No usar ese campo como fecha del dato; la fecha del dato es la carpeta `yyyy-mm-dd` del ZIP |

## 2. `comercio.csv`

- Una fila por **bandera**: Cencosud (Vea/Disco/Jumbo), Carrefour (Hiper/Market/Maxi), La Anónima (3), Comodín (2), Changomas (3). Clave natural `(id_comercio, id_bandera)`.
- CUIT presente y consistente dentro del comercio.
- Padding en nombres (Unicoop: `Sup. Unicoop        `).
- Incluye no-supermercados: Axion Energy (tiendas de estación), Deheza, Estación Lima. Hay que clasificar `tipo_comercio` para no mezclar en comparaciones.

## 3. `sucursales.csv`

- `id_sucursal` único dentro de `(id_comercio, id_bandera)`. Es un código interno del comercio, **no** global.
- Provincia en **ISO 3166-2** (`AR-B`, `AR-C`, `AR-A`…). El dataset provee `maestro-provincias.xlsx` como traductor → tabla de referencia.
- Lat/long: 100 % presente y dentro de Argentina en los comercios bien formados.
- `sucursales_tipo`: `Hipermercado`, `Supermercado`, `Autoservicio`, `Web` (La Anónima online), con variantes de mayúsculas (`supermercado`, `autoservicio` en Comodín).
- Coto: 115 de 122 nombres con padding de espacios.
- **Integridad referencial rota en ambos sentidos**:
  - Cencosud: 8 sucursales Vea y 4 Jumbo tienen precios pero no existen en `sucursales.csv` (huérfanas).
  - Coto (2), Toledo (3), Axion (1): sucursales declaradas sin ningún precio.
  - Silver: `fact_precio` con FK a sucursal "desconocida" (fila placeholder por comercio/bandera), no descartar los precios.

## 4. `productos.csv`

### Claves

- Clave natural `(id_comercio, id_bandera, id_sucursal, id_producto)`: **0 duplicados** en los 3,4 M de filas. Buena noticia: el `MERGE` diario no necesita dedup previo.
- `productos_ean` es un **flag 0/1**, no el código. El código está en `id_producto`.
- El flag no es confiable: Coto marca `1` en 33 productos con prefijo `2` (pesables: `HUESO XKG`, `SEBO XKG`) y Cencosud en 1.360. Silver debe recalcular `es_ean_global` por regla propia: 8/12/13/14 dígitos, dígito verificador GS1 válido y prefijo ≠ `2` (13 dígitos) ≠ `0000000…`.
- Códigos internos (flag 0): 7 % en Coto (76 k), 0,7 % en La Anónima y Cencosud. Comparables solo dentro de la misma cadena.

### Precios

- `productos_precio_lista`: 100 % numérico, punto decimal, sin nulos, sin ceros, sin negativos.
- Mediana ~ $4.500. P99 $35 k – $75 k.
- `> $1 M`: 3.676 filas, todos electro (TV 85" 8K $11,4 M, heladeras). **Legítimos**, no outliers.
- `< $100`: 94 filas: pesables con precio simbólico (`HUESO XKG $0,30`), envases (`botella coca cola $0,01`). Regla: `precio_lista < 100 AND es_ean_global` → `_is_outlier`; pesables excluidos de comparaciones.
- `precio_referencia` (precio por unidad de medida) siempre presente, pero **no comparable entre cadenas sin normalizar**: Cencosud lo expresa por **100 g/ml**, el resto por **1 kg/L** (`cantidad_referencia` = 100 vs 1 vs 1000). Recalcularlo en Silver a partir de `precio_lista / cantidad_canon`.

### Promociones

- Solo La Anónima (5,7 %), Toledo (1,4 %) y Coto (0,7 %) informan `promo1`. Cencosud, Comodín y Axion: 0 %. `promo2`: nunca.
- Donde hay promo, la leyenda siempre está y `promo1 < lista` (1 excepción en Toledo). Descuento mediano 19–29 %.
- Conclusión: la pregunta de promociones es **débil** con esta cobertura. Se mantiene como métrica secundaria (`share_en_promo`) y se reevalúa cuando entren DIA y Carrefour.

### Presentación y unidades

- 33 variantes de unidad para 5 conceptos: `gr/gr./grm/g/kg/kgm/kgr`, `ml/ml./cm3/cc/cc./cmq/mi/lt/lt./ltr/l/dm3`, `un/un./uni/cu/pc/par/pie/hjs`, `m/mt/mt./mtr/cmt`, `m2`. Cada cadena usa su propio vocabulario → tabla de mapeo `unidad_raw → (unidad_canon, factor)` en Silver.
- **Error sistemático Cencosud**: cantidad en kg/L con unidad `gr.`/`ml.` (`0.500 gr.` = 500 g; `0.200 ml.` = 200 ml). Regla por comercio: si `id_comercio = 9` y `cant < 1` con unidad g/ml → multiplicar por 1000.
- La Anónima usa `cu` (count units) en 143 k filas donde otros ponen gramos.
- Aerosoles: masa vs volumen (`89 gr` vs `150 cm3`), legítimamente distinto.
- Resultado: tras normalizar, la presentación coincide entre cadenas en 68 % (2 cadenas) a 40 % (4 cadenas) de los EAN compartidos. **Comparar precios por EAN no depende de la presentación**; la presentación se usa solo para precio por kg/L y reduflación, con regla "presentación canónica = moda entre cadenas".

### Descripción y marca

- Descripción distinta en el **100 %** de los EAN compartidos (esperado). `dim_producto` necesita descripción canónica (moda) + descripción por cadena aparte.
- Padding y espacios dobles en Coto y Cencosud (`HUESO .   XKG    `).
- Marca vacía: solo Coto (24 k, 2,3 %). Marca genérica (`SIN MARCA`): Cencosud 30 k, La Anónima 3 k, Coto 5,6 k.

## 5. Solapamiento entre cadenas (habilita la pregunta rectora)

| EAN presentes en N cadenas | EAN | % del total con flag EAN |
|---|---|---|
| 1 | 14.178 | 59 % |
| 2 | 4.998 | 21 % |
| 3 | 3.364 | 14 % |
| 4 | 1.352 | 6 % |
| 5 | 132 | 0,5 % |

**9.846 EAN (41 %) están en 2 o más cadenas** con solo 5 comercios en la muestra. Con los 17 del día la masa comparable crece.

Dispersión real observada el mismo día (mediana por bandera, % sobre la más barata):

| Producto | Más barata | Más cara | Brecha |
|---|---|---|---|
| Sal fina Celusal 500 g | Comodín $1.344 | La Anónima $2.000 | 49 % |
| Acondicionador Dove 400 ml | Comodín $7.824 | La Anónima $11.750 | 50 % |
| Café Cabrales 250 g | Comodín $12.268 | La Anónima $16.800 | 37 % |
| Fideos Matarazzo 500 g | Comodín $2.053 | La Anónima $2.850 | 39 % |
| Mayonesa Hellmann's 475 g | La Anónima $2.190 | Toledo $3.299 | 51 % |

Y dentro de una misma bandera también: Vea vende el mismo fideo entre $1.690 y $2.490 según sucursal.
La pregunta "¿quién vende más barato y cuánto varía entre sucursales?" tiene señal de sobra.

---

## 6. Implicancias para el pipeline

### Landing

- Volume `sepa.landing.raw`, ruta `sepa_minorista/fecha_datos=<yyyy-mm-dd>/sepa_<dia>.zip` (el ZIP diario completo, inmutable) + `_manifest.json` (url, resource_id, sha256, bytes, `last_modified`, timestamp de descarga).
- Job diario obligatorio: la fuente rota a los 7 días.
- `referencia/maestro-provincias.xlsx` como tabla de referencia.

### Bronze (PySpark: unzip + parseo; el resto SQL)

- Descomprimir ZIP diario → ZIP por comercio → 3 CSV. Un ZIP corrupto o un CSV con `parse_success_rate` bajo va a `bronze.cuarentena_archivos`, el resto del lote sigue.
- Tres tablas, todo STRING, append-only, particionadas por `fecha_datos`: `bronze.comercio`, `bronze.sucursales`, `bronze.productos`.
- Filtrar footer (`id_comercio RLIKE '^[0-9]+$'`) y guardar el timestamp del footer como `_footer_ts`.
- Columnas de control: `_fecha_datos`, `_lote_id`, `_source_path` (zip diario + zip comercio + csv), `_ingest_ts`, `_row_hash`.
- Volumen estimado completo: 13 M filas/día, ~90 M filas/semana. Con Free Edition conviene decidir un **filtro de alcance** (ver pendientes).

### Silver

- `silver.comercio` (grain bandera), `silver.sucursal` (**SCD2**: nombre, tipo, dirección, geo, horarios), `silver.producto` (**SCD2** por `id_producto` global: descripción canónica, marca, presentación canónica), `silver.producto_comercio` (descripción/presentación tal como la publica cada cadena), `silver.precio_diario` (grain producto × sucursal × día).
- Reglas: `es_ean_global` propia; mapeo de unidades; corrección Cencosud; `precio_ref_canon`; `tipo_comercio` (supermercado / estación / otro); `_is_outlier` (< $100 con EAN global); sucursal placeholder para huérfanas; provincia ISO → nombre.
- Tests: unicidad de clave natural, FK sucursal (con placeholder), rango de precio, EAN válido.

### Gold

- `dim_fecha`, `dim_comercio_bandera`, `dim_sucursal` (SCD2), `dim_producto` (SCD2), `dim_ubicacion` (provincia/localidad), `fact_precio` (producto × sucursal × día: `precio_lista`, `precio_promo`, `precio_efectivo`, `precio_por_unidad_canon`), `agg_precio_bandera_dia` (mediana por EAN × bandera × día) y `agg_indice_canasta_dia`.

### Semantic (métricas candidatas)

`precio_mediano_bandera`, `brecha_entre_banderas_pct`, `indice_bandera_vs_mercado`, `variacion_7d`, `indice_canasta`, `dispersion_intra_bandera`, `share_en_promo`, `sucursales_reportando`, `tasa_registros_cuarentena`.

---

## 7. Decisiones pendientes (para definir antes de los SPEC)

1. **Alcance de carga completa**: ¿todo el país y los 17 comercios (~13 M filas/día), o un filtro (p. ej. provincias AR-C, AR-B, AR-X, AR-S y las cadenas nacionales)? Afecta costo en Free Edition y tiempo de los jobs.
2. **Canasta rectora**: lista de EAN/rubros para el índice. Propuesta: los EAN presentes en ≥ 3 cadenas de alimentos, bebidas y limpieza (~4.800 con 5 comercios; más con 17).
3. **Cuarentena vs. reconstrucción** de archivos multilínea (Comodín): propuesta cuarentena.
4. **Histórico**: arrancar a acumular ya (un job diario desde hoy) para tener semanas de serie al momento del dashboard. Cada día que pasa sin bajar el ZIP es un día perdido.
