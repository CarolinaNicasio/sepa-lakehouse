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

-- 4. Precios atipicos: los errores de carga mas grandes
SELECT bandera_nombre, descripcion, precio_cadena, precio_mercado, relacion
FROM sepa.gold.precios_atipicos
ORDER BY relacion
LIMIT 20;

-- 5. Que porcentaje de precios atipicos tiene cada cadena
SELECT
  a.bandera_nombre,
  COUNT(*) AS atipicos,
  i.productos_comparados,
  i.indice_precios,
  round(100 * COUNT(*) / (COUNT(*) + i.productos_comparados), 1) AS pct_atipicos
FROM sepa.gold.precios_atipicos a
LEFT JOIN sepa.gold.indice_precios_cadena i
  ON a.bandera_nombre = i.bandera_nombre
GROUP BY a.bandera_nombre, i.productos_comparados, i.indice_precios
ORDER BY pct_atipicos DESC;

-- 6. Sucursales con provincia que no coincide con las coordenadas
SELECT ubicacion_consistente, COUNT(*) AS sucursales
FROM sepa.gold.dim_sucursal
GROUP BY ubicacion_consistente;

-- 7. Detalle por cadena de las sucursales a revisar
SELECT c.bandera_nombre, COUNT(*) AS sucursales_a_revisar
FROM sepa.gold.dim_sucursal s
LEFT JOIN sepa.gold.dim_comercio c
  ON s.id_comercio = c.id_comercio
 AND s.id_bandera = c.id_bandera
WHERE s.region = 'Revisar ubicación'
GROUP BY c.bandera_nombre
ORDER BY sucursales_a_revisar DESC;

-- 8. Sucursales por region (debe aparecer "Revisar ubicación" y bajar CABA)
SELECT region, COUNT(*) AS sucursales
FROM sepa.gold.dim_sucursal
GROUP BY region
ORDER BY sucursales DESC;