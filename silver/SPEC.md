# Silver — SPEC

Capa de **entidades y calidad**. Toma el crudo tabular de Bronze, lo tipa, le pone claves de negocio
y aplica reglas para que Gold pueda comparar precios. No calcula KPIs, índices ni cubos.

Contrato de entrada: `bronze/SPEC.md`. Silver lee `comercio`, `sucursales` y `productos` de Bronze
y la referencia de provincias del dataset (`maestro-provincias`). No abre ZIP. No reconstruye
archivos en cuarentena.

## Propósito

- Una fila de negocio = una entidad o un precio, con tipos y BK.
- Corregir lo que la fuente miente o mezcla (flag EAN, unidades, padding, duplicados).
- Dejar historial cuando cambia una sucursal o un producto comparable (SCD2).
- Marcar lo que Gold no debe meter en una comparación, sin borrarlo.

## Alcance (cerrado)

- **Todo** lo que está en Bronze: país completo, todas las cadenas, todos los días cargados.
- No hay recorte por provincia ni por “cadenas grandes”.
- Estaciones de servicio y farmacias **entran** a las tablas Silver. Gold las saca de brecha /
  canasta filtrando `tipo_comercio = 'supermercado'`.
- La **canasta** de productos del índice no se define acá. Acá solo se calcula `es_ean_global`.

## Qué entra

Filas de Bronze con `id_comercio` numérico (el footer ya no está). Un día = el `_fecha_datos` de
Bronze. Si Bronze tiene el lote en `parcial` (ZIP vacío de un comercio), Silver procesa igual lo
que sí cargó; no rellena al comercio ausente.

## Qué NO se hace acá

- Mediana, brecha entre banderas, variación 7d, índice de canasta, dashboards.
- Reconstruir un CSV multilínea o un ZIP de 0 bytes.
- Usar `comercio_ultima_actualizacion` ni la fecha de la página web como día del dato.
- Confiar en el flag `productos_ean` de la fuente.
- Borrar precios huérfanos (sucursal que no está en `sucursales`): van a una sucursal placeholder.

## Decisiones de diseño (cerradas)

| # | Decisión |
|---|---|
| 1 | Alcance = Bronze completo |
| 2 | `tipo_comercio`: `supermercado` / `estacion` / `otro`. Comparar precios = solo supermercado |
| 3 | Archivo ilegible = no se repara |
| 4 | SCD2 en `sucursal` y en `producto` (EAN global). `comercio` y `producto_comercio` son tipo 1 (último estado) |
| 5 | Canasta = Gold |
| 6 | Misma clave, filas iguales → una. Si el precio difiere → una, con `_tiene_conflicto_precio` |

---

## Reglas

### Limpieza de texto

`trim` y espacios internos múltiples → uno, en nombres, descripciones, marcas, tipos, provincias.
Vacío después de trim → NULL.

### Deduplicación (por día, antes de tipar)

Clave del día = grano de Bronze **sin** contar `_fecha_datos` (el recorte ya es un día):

| Tabla Bronze | Clave |
|---|---|
| comercio | `id_comercio`, `id_bandera` |
| sucursales | `id_comercio`, `id_bandera`, `id_sucursal` |
| productos | `id_comercio`, `id_bandera`, `id_sucursal`, `id_producto` |

Queda **una** fila por clave. Si todas las columnas de negocio coinciden, cualquiera. Si
`productos_precio_lista` (u otra columna de negocio) difiere: se queda la de `_row_hash`
lexicográficamente menor y `_tiene_conflicto_precio = true` en `precio_diario`.

Observado: 49 claves/día en California, copias exactas.

### Tipos

IDs se quedan **texto** (un EAN no es un número: se pierden ceros).
Precios y cantidades: número decimal. Lo que no casteá = NULL.
`fecha_datos`: fecha. Flags: booleano.

Una fila de `precio_diario` **no entra** si `precio_lista` es NULL o negativo después del casteo.

### `tipo_comercio`

Mapeo por `id_comercio`. Id no listado → `otro` (no se mezcla en comparaciones hasta revisarlo).

| `id_comercio` | `tipo_comercio` | Ejemplos |
|---|---|---|
| `3`, `4`, `23` | `estacion` | Deheza, Estación Lima, Axion |
| `24` | `otro` | Farmcity |
| el resto visto en Bronze | `supermercado` | Coto, Cencosud, DIA, Comodín, … |

Validación: listar ids de Bronze que no estén en esta tabla.

### Provincia

1. Si `sucursales_provincia` parece ISO (`AR-` + una letra, p. ej. `AR-B`) → ese código.
2. Si no, buscar el nombre en `maestro-provincias` (sin mayúsculas/espacios) → código ISO.
3. Si no hay match → `provincia_iso` NULL (Comodín: casi todas; DIA eCommerce escribió `Buenos Aires`).

Nombre de provincia: join a la referencia por ISO. No inventar provincia.

### Sucursal placeholder

Precios cuya clave `(id_comercio, id_bandera, id_sucursal)` no está en `sucursales` ese día
**no se descartan**. Se enganchan a una sucursal sintética:

- `id_sucursal` = `_DESCONOCIDA`
- una por `(id_comercio, id_bandera)`
- `sucursal_nombre` = `Sucursal desconocida`
- `es_placeholder` = true
- no hace SCD2: siempre la misma fila vigente

En `precio_diario` el `id_sucursal` **sigue siendo el de la fuente** (para no perder el código
huérfano). La FK de negocio a `sucursal` usa el placeholder cuando no hay match. Gold une
precio → sucursal por `(id_comercio, id_bandera, id_sucursal)` y, si no hay versión vigente,
cae al placeholder de esa bandera.

Forma operativa (para no dejar precios sin dimensión):

- `precio_diario.id_sucursal_sk` / join: si existe sucursal real, esa; si no, `_DESCONOCIDA`.
- Columna `id_sucursal_fuente`: código original, siempre.

### `es_ean_global`

Se **recalcula**. Se ignora el flag `productos_ean`.

`id_producto` (solo dígitos, sin espacios) es EAN global si **todo** esto vale:

1. Largo 8, 12, 13 o 14.
2. Dígito verificador GS1 correcto (el último; los demás se ponderan 3,1,3,1… desde la derecha;
   el verificador es `(10 - (suma % 10)) % 10`).
3. No es código interno de pesables: si tiene 13 dígitos, el primero no es `2`.
4. No es un relleno de ceros (no empieza con siete o más `0`).,

Si no pasa: código interno, comparable **solo dentro de la misma cadena**.

### Unidades y cantidad canónica

La fuente mezcla vocabulario (`gr.`, `grm`, `kgm`, `cc.`, `cu`, …). Mapeo `unidad_raw` →
`(unidad_canon, factor_a_base)`. Base: `g`, `ml`, `un`, `m`, `m2`.

| `unidad_raw` (minúsculas, sin punto final) | `unidad_canon` | factor a base |
|---|---|---|
| `g`, `gr`, `grm` | `g` | 1 |
| `kg`, `kgm`, `kgr` | `g` | 1000 |
| `ml`, `cm3`, `cc`, `cmq`, `mi` | `ml` | 1 |
| `l`, `lt`, `ltr`, `dm3` | `ml` | 1000 |
| `un`, `uni`, `cu`, `pc`, `par`, `pie`, `hjs` | `un` | 1 |
| `m`, `mt`, `mtr` | `m` | 1 |
| `cmt`, `cm` | `m` | 0.01 |
| `m2` | `m2` | 1 |

`unidad_raw` se normaliza: minúsculas, trim, se saca el punto final. Lo que no esté en la tabla
→ `unidad_canon` NULL, `cantidad_canon` NULL.

**Cencosud (`id_comercio = 9`)**: si la cantidad de presentación es `> 0` y `< 1` y la unidad
canónica de esa fila es `g` o `ml`, multiplicar la cantidad por 1000 **antes** de aplicar el
factor (mandan `0.500 gr.` queriendo 500 g; `0.200 ml.` queriendo 200 ml).

`cantidad_canon` = cantidad corregida × factor (en la unidad base).

`precio_por_unidad_canon`:

- masa (`g`): `precio_lista / (cantidad_canon / 1000)` → precio por kg
- volumen (`ml`): `precio_lista / (cantidad_canon / 1000)` → precio por L
- `un` / `m` / `m2`: `precio_lista / cantidad_canon`
- si cantidad o unidad es NULL o cantidad ≤ 0 → NULL

No usar `productos_precio_referencia` de la fuente para comparar cadenas: cada una usa 1 kg,
100 g o 1 L. Siempre recalcular.

Presentación canónica de un EAN (para `producto`): moda de `(cantidad_canon, unidad_canon)` entre
filas `tipo_comercio = supermercado`, `es_ean_global`, no outlier. Empate: la menor en texto.

### Precio, promo, outlier

- `precio_lista`: casteo de `productos_precio_lista`.
- `precio_promo`: casteo de `productos_precio_unitario_promo1` (promo2 se ignora: no viene).
- `precio_efectivo`: `precio_promo` si es not NULL, `> 0` y `< precio_lista`; si no, `precio_lista`.
- `_is_outlier`: `precio_lista <= 0` **o** (`precio_lista < 100` **y** `es_ean_global`).
  TV / electro de millones **no** son outlier.
- `_tiene_conflicto_precio`: ver dedup.

### SCD2 — `sucursal`

Clave de negocio: `id_comercio` + `id_bandera` + `id_sucursal` (el de la fuente, no el placeholder).

Atributos que abren versión: nombre, tipo (normalizado), dirección (calle + número), geo,
observaciones, barrio, CP, localidad, `provincia_iso`, los siete horarios.

- Primera vez que aparece → `vigencia_desde = fecha_datos`, `vigencia_hasta` NULL, `es_vigente = true`.
- Mismo hash de atributos que la vigente → no se toca.
- Hash distinto → se cierra la vigente (`vigencia_hasta = fecha_datos - 1 día`, `es_vigente = false`)
  y se abre otra con `vigencia_desde = fecha_datos`.
- **No** se cierra porque un comercio no mandó ese día (La Agrícola, Farmcity). Gold ve el hueco
  en `precio_diario`.

Tipo de sucursal normalizado: `Hipermercado` / `Supermercado` / `Autoservicio` / `Web` / `otro`
(minúsculas y variantes → esos cuatro; el resto `otro`).

### SCD2 — `producto`

Solo EAN globales. Clave de negocio: `id_producto` (dígitos del código).

Atributos que abren versión: `descripcion_canon`, `marca_canon`, `cantidad_canon` + `unidad_canon`
de la presentación canónica.

Canónicos de un día = moda entre filas de ese EAN en supermercados, no outlier. Empate: menor texto.
Marca vacía o `SIN MARCA` no entra a la moda; si no queda ninguna, `marca_canon` NULL.

Misma lógica de vigencia que sucursal. Un código interno **no** tiene fila acá.

### Tipo 1 — `comercio` y `producto_comercio`

Último estado visto (el día más reciente en Bronze). Sin historial de versiones.

`producto_comercio`: cómo publica **esa bandera** ese `id_producto` (descripcion, marca,
presentación, `es_ean_global`). Sirve para internos y para el texto de cada cadena. Grain:
`id_comercio` + `id_bandera` + `id_producto`.

---

## Tablas

Schema `silver`. Nombres `snake_case`.

### `silver.provincia`

Grain: un código ISO. Carga desde `maestro-provincias`. Estática.

| Columna | Tipo | Descripción |
|---|---|---|
| `provincia_iso` | STRING | PK. `AR-B`, `AR-C`, … |
| `provincia_nombre` | STRING | Nombre de la referencia |

### `silver.unidad_medida`

Grain: una unidad cruda. Estática, según la tabla de mapeo de este SPEC.

| Columna | Tipo | Descripción |
|---|---|---|
| `unidad_raw` | STRING | PK. Ya normalizada |
| `unidad_canon` | STRING | `g` / `ml` / `un` / `m` / `m2` |
| `factor_a_base` | DECIMAL | Multiplicador a la unidad base |

### `silver.comercio`

Grain: una bandera (último estado).

| Columna | Tipo | Descripción |
|---|---|---|
| `id_comercio` | STRING | BK |
| `id_bandera` | STRING | BK |
| `comercio_cuit` | STRING | |
| `razon_social` | STRING | trim |
| `bandera_nombre` | STRING | trim |
| `bandera_url` | STRING | |
| `tipo_comercio` | STRING | `supermercado` / `estacion` / `otro` |
| `fecha_ultima_foto` | DATE | Último `_fecha_datos` de Bronze para esta BK |
| `_lote_id` | STRING | Lote de esa última foto |

### `silver.sucursal`

Grain: una versión de un local. SCD2.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_comercio` | STRING | BK |
| `id_bandera` | STRING | BK |
| `id_sucursal` | STRING | BK. `_DESCONOCIDA` si placeholder |
| `sucursal_nombre` | STRING | |
| `sucursal_tipo` | STRING | Hipermercado / Supermercado / Autoservicio / Web / otro |
| `calle` | STRING | |
| `numero` | STRING | |
| `latitud` | DECIMAL | |
| `longitud` | DECIMAL | |
| `observaciones` | STRING | |
| `barrio` | STRING | |
| `codigo_postal` | STRING | |
| `localidad` | STRING | |
| `provincia_iso` | STRING | FK a `provincia`. NULL si no se pudo mapear |
| `es_placeholder` | BOOLEAN | |
| `vigencia_desde` | DATE | |
| `vigencia_hasta` | DATE | NULL = vigente |
| `es_vigente` | BOOLEAN | |
| `_lote_id` | STRING | Lote que abrió esta versión |

### `silver.producto`

Grain: una versión de un EAN global. SCD2. No hay internos.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_producto` | STRING | BK = EAN |
| `descripcion_canon` | STRING | Moda entre cadenas |
| `marca_canon` | STRING | |
| `cantidad_canon` | DECIMAL | Presentación canónica, unidad base |
| `unidad_canon` | STRING | |
| `vigencia_desde` | DATE | |
| `vigencia_hasta` | DATE | NULL = vigente |
| `es_vigente` | BOOLEAN | |
| `_lote_id` | STRING | |

### `silver.producto_comercio`

Grain: un producto tal como lo publica una bandera (último estado). Incluye internos.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_comercio` | STRING | BK |
| `id_bandera` | STRING | BK |
| `id_producto` | STRING | BK |
| `es_ean_global` | BOOLEAN | |
| `descripcion` | STRING | Texto de esa cadena |
| `marca` | STRING | |
| `cantidad_canon` | DECIMAL | |
| `unidad_canon` | STRING | |
| `fecha_ultima_foto` | DATE | |
| `_lote_id` | STRING | |

### `silver.precio_diario`

Grain: producto × sucursal × día.

| Columna | Tipo | Descripción |
|---|---|---|
| `fecha_datos` | DATE | BK. Día de los precios |
| `id_comercio` | STRING | BK |
| `id_bandera` | STRING | BK |
| `id_sucursal_fuente` | STRING | BK. Código que mandó SEPA |
| `id_sucursal` | STRING | Para join a `sucursal`: fuente si existe ese día, si no `_DESCONOCIDA` |
| `id_producto` | STRING | BK |
| `es_ean_global` | BOOLEAN | |
| `precio_lista` | DECIMAL | |
| `precio_promo` | DECIMAL | NULL si no hay promo1 usable |
| `precio_efectivo` | DECIMAL | |
| `precio_por_unidad_canon` | DECIMAL | Por kg / L / unidad según `unidad_canon` |
| `cantidad_canon` | DECIMAL | |
| `unidad_canon` | STRING | |
| `_is_outlier` | BOOLEAN | |
| `_tiene_conflicto_precio` | BOOLEAN | Dedup con precios distintos |
| `_lote_id` | STRING | Lote Bronze de ese día |

### `silver.lotes_procesados`

Una fila por `fecha_datos` ya aplicada en Silver.

| Columna | Tipo | Descripción |
|---|---|---|
| `fecha_datos` | DATE | PK |
| `lote_id` | STRING | El de Bronze / Landing |
| `ts_proceso` | TIMESTAMP | Fin de esta corrida |
| `n_comercio` | BIGINT | Banderas vigentes |
| `n_sucursal_vigente` | BIGINT | Locales vigentes (incluye placeholders) |
| `n_producto_vigente` | BIGINT | EAN vigentes |
| `n_precio_diario` | BIGINT | Filas de ese día |
| `n_outlier` | BIGINT | |
| `n_placeholder` | BIGINT | Precios enganchados a `_DESCONOCIDA` |
| `estado` | STRING | `ok` |

---

## Idempotencia

- Días pendientes = `fecha_datos` en Bronze (`lotes_procesados`) que aún no están en
  `silver.lotes_procesados`. Se aplican **en orden de fecha**.
- `precio_diario`: ese día se borra y se vuelve a escribir. Los otros días no se tocan.
- Dimensiones SCD2: se alimentan con la foto de ese día (abrir / cerrar versiones). No rebobinan
  solas. `forzar` un día ya procesado implica **reconstruir** `sucursal` y `producto` desde el
  primer día de Bronze y reescribir `precio_diario` de los días forzados.
- Correr dos veces sin días nuevos: no cambia nada.

## Contrato hacia Gold

- Gold lee **solo** Silver. No re-limpia EAN, unidades ni outliers.
- Comparaciones entre cadenas: `precio_diario` ⋈ `comercio` donde `tipo_comercio = 'supermercado'`
  y `es_ean_global` y `NOT _is_outlier`.
- Join sucursal: `id_comercio`, `id_bandera`, `id_sucursal` y `fecha_datos` entre `vigencia_desde`
  y `vigencia_hasta` (NULL = vigente). Placeholder cubre huérfanos.
- Join producto canónico: `id_producto` + fecha en el rango SCD2, solo si `es_ean_global`.
- La lista de EAN de la canasta y los nombres de métrica (`precio_mediano_bandera`, etc.) son Gold
  / semantic.

## Tests mínimos (post-carga)

Correr sobre un `fecha_datos` (y vigentes, donde aplique):

| Test | Esperado |
|---|---|
| Unicidad `precio_diario` | 0 duplicados de la BK |
| Unicidad `comercio` | 1 fila por `(id_comercio, id_bandera)` |
| Unicidad sucursal vigente | 1 `es_vigente` por BK real + 1 placeholder por bandera |
| Unicidad producto vigente | 1 `es_vigente` por `id_producto` |
| FK sucursal | 0 precios cuyo `id_sucursal` no esté en `sucursal` (placeholder cuenta) |
| `precio_lista` | 0 NULL, 0 negativos |
| `_is_outlier` | incluye ceros; no marca un precio > 1 M |
| `es_ean_global` | ningún código de 13 dígitos con prefijo `2` |
| `tipo_comercio` | solo los tres valores del SPEC |
| Ids de comercio nuevos | 0, o listados para sumar al mapeo |
| Conteos vs Bronze | `n_precio_diario` ≤ filas Bronze del día (dedup + bajas por precio inválido); mismo orden de magnitud (~14 M) |

## Artefactos

| Archivo | Qué hace |
|---|---|
| `silver/01_ddl.sql` | Crea schema y tablas vacías |
| `silver/02_etl.py` | Tipa, reglas, SCD2, `precio_diario`. Pendientes en orden de fecha |
| `silver/03_validaciones.sql` | Tests de arriba. No escribe |

## Owner

Cristian Barabas (Data Engineering).
