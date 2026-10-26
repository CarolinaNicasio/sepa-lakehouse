-- =============================================================
-- GOLD - Tablas de analisis
-- Comparacion de precios entre cadenas (solo productos con EAN)
-- =============================================================

-- 1. Precio de cada producto en cada cadena (resume sus sucursales)
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.precio_producto_cadena
COMMENT 'Precio mediano de cada producto EAN en cada cadena'
AS
SELECT
  id_producto,
  any_value(descripcion) AS descripcion,
  id_comercio,
  id_bandera,
  bandera_nombre,
  count(*) AS sucursales,
  percentile_approx(precio_lista, 0.5) AS precio_mediano
FROM sepa.gold.fct_precios_vigentes
WHERE es_ean
GROUP BY id_producto, id_comercio, id_bandera, bandera_nombre;


-- 2. Precio de mercado de cada producto (mediana entre cadenas)
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.precio_mercado
COMMENT 'Precio de mercado de cada producto vendido por al menos 3 cadenas'
AS
SELECT
  id_producto,
  any_value(descripcion) AS descripcion,
  count(*) AS cadenas,
  percentile_approx(precio_mediano, 0.5) AS precio_mercado
FROM sepa.gold.precio_producto_cadena
GROUP BY id_producto
HAVING count(*) >= 3;


-- 3. Precios atipicos: probables errores de carga (se registran, no se ocultan)
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.precios_atipicos
COMMENT 'Precios que se alejan mas de 5 veces del precio de mercado'
AS
SELECT
  pc.bandera_nombre,
  pc.id_producto,
  pc.descripcion,
  pc.precio_mediano AS precio_cadena,
  m.precio_mercado,
  round(pc.precio_mediano / m.precio_mercado, 4) AS relacion
FROM sepa.gold.precio_producto_cadena pc
JOIN sepa.gold.precio_mercado m
  ON pc.id_producto = m.id_producto
WHERE pc.precio_mediano / m.precio_mercado NOT BETWEEN 0.2 AND 5;


-- 4. Indice de precios por cadena: 100 = mercado
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.indice_precios_cadena
COMMENT 'Indice de precios por cadena. 100 = mercado; menos de 100 = mas barata'
AS
SELECT
  pc.id_comercio,
  pc.id_bandera,
  pc.bandera_nombre,
  count(*) AS productos_comparados,
  round(100 * avg(pc.precio_mediano / m.precio_mercado), 1) AS indice_precios
FROM sepa.gold.precio_producto_cadena pc
JOIN sepa.gold.precio_mercado m
  ON pc.id_producto = m.id_producto
WHERE pc.precio_mediano / m.precio_mercado BETWEEN 0.2 AND 5
GROUP BY pc.id_comercio, pc.id_bandera, pc.bandera_nombre;


-- 5. Dispersion: diferencia de precio entre cadenas (sin precios atipicos)
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.dispersion_productos
COMMENT 'Precio minimo y maximo de cada producto EAN entre cadenas, sin atipicos'
AS
SELECT
  pc.id_producto,
  any_value(pc.descripcion) AS descripcion,
  count(*) AS cadenas,
  min(pc.precio_mediano) AS precio_minimo,
  max(pc.precio_mediano) AS precio_maximo,
  min_by(pc.bandera_nombre, pc.precio_mediano) AS cadena_mas_barata,
  max_by(pc.bandera_nombre, pc.precio_mediano) AS cadena_mas_cara,
  round(100 * (max(pc.precio_mediano) - min(pc.precio_mediano)) / min(pc.precio_mediano), 1) AS diferencia_pct
FROM sepa.gold.precio_producto_cadena pc
JOIN sepa.gold.precio_mercado m
  ON pc.id_producto = m.id_producto
WHERE pc.precio_mediano / m.precio_mercado BETWEEN 0.2 AND 5
GROUP BY pc.id_producto
HAVING count(*) >= 3;


-- 6. Variacion de precios por cadena entre el primer y el ultimo dia cargado
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.variacion_precios_cadena
COMMENT 'Variacion de precios por cadena entre el primer y el ultimo dia (misma canasta)'
AS
WITH rango AS (
  SELECT min(__START_AT) AS fecha_inicio, max(__START_AT) AS fecha_fin
  FROM sepa.silver.precios_historial
),
precios_inicio_fin AS (
  SELECT
    h.id_comercio,
    h.id_bandera,
    h.id_sucursal,
    h.id_producto,
    max(CASE WHEN h.__START_AT <= r.fecha_inicio
              AND (h.__END_AT IS NULL OR h.__END_AT > r.fecha_inicio)
             THEN h.precio_lista END) AS precio_inicio,
    max(CASE WHEN h.__START_AT <= r.fecha_fin
              AND (h.__END_AT IS NULL OR h.__END_AT > r.fecha_fin)
             THEN h.precio_lista END) AS precio_fin
  FROM sepa.silver.precios_historial h
  CROSS JOIN rango r
  GROUP BY h.id_comercio, h.id_bandera, h.id_sucursal, h.id_producto
)
SELECT
  c.bandera_nombre,
  count(*) AS productos_comparados,
  round(100 * (avg(p.precio_fin / p.precio_inicio) - 1), 2) AS variacion_promedio_pct,
  round(100 * avg(CASE WHEN p.precio_fin > p.precio_inicio THEN 1 ELSE 0 END), 1) AS pct_subieron,
  round(100 * avg(CASE WHEN p.precio_fin < p.precio_inicio THEN 1 ELSE 0 END), 1) AS pct_bajaron
FROM precios_inicio_fin p
LEFT JOIN sepa.gold.dim_comercio c
  ON p.id_comercio = c.id_comercio
 AND p.id_bandera = c.id_bandera
WHERE p.precio_inicio IS NOT NULL
  AND p.precio_fin IS NOT NULL
  AND p.precio_fin / p.precio_inicio BETWEEN 0.2 AND 5
GROUP BY c.bandera_nombre;