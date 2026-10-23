-- 1. Cantidad de filas por comercio
SELECT comercio, COUNT(*) AS filas
FROM sepa.bronze.productos
GROUP BY comercio
ORDER BY filas DESC;

-- 2. Filas con datos rescatados (deberia dar vacio)
SELECT comercio, COUNT(*) AS filas_con_problemas
FROM sepa.bronze.productos
WHERE _rescued_data IS NOT NULL
GROUP BY comercio;

-- 3. Columnas de la tabla
DESCRIBE TABLE sepa.bronze.productos;

-- 4. Duplicados por clave
SELECT id_comercio, id_bandera, id_sucursal, id_producto, COUNT(*) AS veces
FROM sepa.bronze.productos
GROUP BY ALL
HAVING COUNT(*) > 1
ORDER BY veces DESC
LIMIT 20;
-- 5. Tamaño real del problema: claves duplicadas por comercio (sin límite)
SELECT id_comercio, COUNT(*) AS claves_duplicadas
FROM (
  SELECT id_comercio, id_bandera, id_sucursal, id_producto
  FROM sepa.bronze.productos
  GROUP BY ALL
  HAVING COUNT(*) > 1
)
GROUP BY id_comercio
ORDER BY claves_duplicadas DESC;
---6. Ver un caso duplicado de cerca
SELECT id_producto, productos_descripcion, productos_precio_lista,
       productos_cantidad_presentacion, productos_unidad_medida_presentacion
FROM sepa.bronze.productos
WHERE id_comercio = '5'
  AND id_sucursal = '1'
  AND id_producto = '0041789001918';

  -- 7. Duplicados con el nombre del producto
SELECT id_comercio,
       id_sucursal,
       id_producto,
       array_join(collect_set(productos_descripcion), ' / ') AS descripcion,
       COUNT(*) AS veces,
       COUNT(DISTINCT productos_precio_lista) AS precios_distintos
FROM sepa.bronze.productos
GROUP BY id_comercio, id_bandera, id_sucursal, id_producto
HAVING COUNT(*) > 1
ORDER BY id_comercio, id_producto, id_sucursal;

-- 8. Duplicados resumidos por producto (versión con CTE)
WITH claves_duplicadas AS (
    SELECT id_comercio, id_bandera, id_sucursal, id_producto
    FROM sepa.bronze.productos
    GROUP BY ALL
    HAVING COUNT(*) > 1
)
SELECT p.id_comercio,
       p.id_producto,
       array_join(collect_set(p.productos_descripcion), ' / ') AS descripcion,
       COUNT(DISTINCT p.id_sucursal) AS sucursales_afectadas,
       COUNT(*) AS filas_totales
FROM sepa.bronze.productos p
JOIN claves_duplicadas d
  ON  p.id_comercio = d.id_comercio
  AND p.id_bandera  = d.id_bandera
  AND p.id_sucursal = d.id_sucursal
  AND p.id_producto = d.id_producto
GROUP BY p.id_comercio, p.id_producto
ORDER BY sucursales_afectadas DESC;