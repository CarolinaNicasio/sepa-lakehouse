-- 1. Cantidad de filas en cada tabla silver
SELECT 'precios_historial' AS tabla, COUNT(*) AS filas FROM sepa.silver.precios_historial
UNION ALL
SELECT 'sucursales', COUNT(*) FROM sepa.silver.sucursales
UNION ALL
SELECT 'comercio', COUNT(*) FROM sepa.silver.comercio;

-- 2. Verificar que no quedaron duplicados (deberia dar vacio)
SELECT id_comercio, id_bandera, id_sucursal, id_producto, COUNT(*) AS veces
FROM sepa.silver.precios_historial
WHERE __END_AT IS NULL
GROUP BY ALL
HAVING COUNT(*) > 1;

-- 3. Ver como quedaron los datos limpios (comercio 4)
SELECT *
FROM sepa.silver.precios_historial
WHERE id_comercio = '4'
LIMIT 10;

-- 4. Ver como quedo el codigo postal limpio (antes: C1073AAQ)
SELECT id_comercio, id_sucursal, nombre, codigo_postal, provincia, latitud, longitud
FROM sepa.silver.sucursales
WHERE id_comercio = '4';