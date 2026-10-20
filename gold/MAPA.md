# Mapa Gold — copiar a Excalidraw

Fact = ids + $. Nombres = dims. El `GROUP BY` es semantic.

```text
     dim_fecha          dim_comercio          dim_producto
         \                   |                     /
          \                  |                    /
                     fact_precio
                  (este EAN, este local,
                   este día, este precio)
                         |
                   dim_sucursal
                         |
                    provincia
                 (pegada al local)
```

Semantic (no es otra fact): `v_precio_bandera_dia`, `v_indice_canasta`, `v_sucursal_dia`, `v_locales`.
