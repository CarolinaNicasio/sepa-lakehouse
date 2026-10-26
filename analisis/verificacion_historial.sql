-- 1. Filas cargadas por dia en bronze
SELECT fecha, COUNT(*) AS filas
FROM sepa.bronze.productos
GROUP BY fecha
ORDER BY fecha;

-- 2. Que comercios no informan todos los dias
SELECT comercio, COUNT(DISTINCT fecha) AS dias_informados
FROM sepa.bronze.productos
GROUP BY comercio
ORDER BY dias_informados, comercio;

-- 3. Tamaño del historial SCD2: precios vigentes vs. precios que cambiaron
SELECT
  COUNT(*) AS versiones_totales,
  COUNT_IF(__END_AT IS NULL) AS precios_vigentes,
  COUNT_IF(__END_AT IS NOT NULL) AS precios_reemplazados
FROM sepa.silver.precios_historial;

-- 4. Cuantos precios cambiaron cada dia
SELECT __START_AT AS fecha, COUNT(*) AS precios_nuevos_o_cambiados
FROM sepa.silver.precios_historial
GROUP BY __START_AT
ORDER BY fecha;

 -- 5. Inflacion de la semana por cadena
  SELECT *
  FROM sepa.gold.variacion_precios_cadena
  ORDER BY variacion_promedio_pct DESC;