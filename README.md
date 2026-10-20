# sepa_precios

Pipeline en Databricks que procesa ~14 millones de precios por día del ZIP de [Precios Claros](https://datos.produccion.gob.ar/dataset/sepa-precios), los transforma en medallion (landing → bronze → silver → gold), implementa SCD Type 2 en sucursal y producto, y lo orquesta un Workflow diario.

La Secretaría de Comercio publica ese ZIP (~310 MB) y a la semana lo pisa. Este pipeline guarda cada archivo y publica una canasta comparable.

La pregunta que responde: **¿cuánto subió la canasta y qué cadena la vende más barata?**

## Cómo está pensado

La fecha del dato se lee de adentro del ZIP, no de la página. El `lote_id` es el hash SHA-256 del archivo: si el mismo ZIP entra dos veces, no se vuelve a guardar. Si el mismo día vuelve con otro contenido, el anterior no se pisa: queda al lado como rectificativo.

Bronze deja todo en texto, partido por día. Un comercio con el archivo roto va a cuarentena y el resto del día sigue. Silver tipa, deja una fila por producto, local y día, y no borra lo que no sirve: lo marca. El EAN lo decide esta capa, porque el flag de la fuente a veces miente. SCD2 solo en `sucursal` y `producto`: si cambia el nombre, se cierra la fila vieja y se abre otra. `comercio` queda en la última versión.

Gold compara solo supermercados, con EAN global y sin precios outlier. `fact_precio` guarda ids y precios. El nombre de la cadena y la descripción viven en las dimensiones y se juntan al leer. La canasta son 15 productos elegidos una vez. El primer día vale 100. Si se rearmara cada semana, ese 100 dejaría de servir. El precio por cadena es la mediana, no el promedio: un local con un precio raro no arrastra a la cadena.

## Qué corre

Job `sepa-diario`, todos los días a las 8 (hora de Buenos Aires): landing → bronze → silver → gold. Un día ya cargado no se vuelve a procesar. La canasta no se toca. El dashboard lee vistas de semantic sobre Gold. No está en el job: se refresca después, porque no guarda una copia.

Hoy hay 13 días en la fact (4 al 10 y 13 al 19 de octubre de 2026), unos 177 millones de filas. El 11 y el 12 no se llegaron a bajar antes de que la página los pisara.

La bajada desde el portal está escrita en `landing/00_descargar_sepa.py`. Esta cuenta no tiene un cluster con salida a internet, así que el ZIP se sube a mano a `/Volumes/sepa/landing/raw/` y landing lo registra.

## Capas

```text
landing/     ZIP cerrado + hash + lista de lotes
bronze/      tres tablas en texto, por día, con cuarentena
silver/      precio comparable; SCD2 en sucursal y producto
gold/        fact_precio + dimensiones + canasta congelada
semantic/    índice, mediana y brecha para el dashboard
```

Catálogo Unity Catalog: `sepa`. Cada capa tiene `SPEC.md` (el contrato), `01_ddl` y `02_etl`. Las validaciones se corren a mano.
