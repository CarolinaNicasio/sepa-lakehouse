-- 1. Que cadena es mas barata (indice: 100 = mercado)
SELECT bandera_nombre, productos_comparados, indice_precios
FROM sepa.gold.indice_precios_cadena
ORDER BY indice_precios;

-- 2. Los 20 productos con mas diferencia de precio entre cadenas
SELECT descripcion, cadenas, precio_minimo, cadena_mas_barata,
       precio_maximo, cadena_mas_cara, diferencia_pct
FROM sepa.gold.dispersion_productos
ORDER BY diferencia_pct DESC
LIMIT 20;

-- 3. Sucursales por region (control de la dimension)
SELECT region, provincia_nombre, COUNT(*) AS sucursales
FROM sepa.gold.dim_sucursal
GROUP BY ALL
ORDER BY sucursales DESC;