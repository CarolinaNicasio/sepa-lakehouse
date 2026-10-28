-- =============================================================
-- GOLD - Modelo dimensional
-- Dimensiones (comercio, sucursal) y hecho (precios vigentes)
-- =============================================================

-- Dimension de cadenas
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.dim_comercio
COMMENT 'Cadenas y banderas de supermercados'
AS
SELECT
  id_comercio,
  id_bandera,
  bandera_nombre,
  razon_social,
  cuit
FROM sepa.silver.comercio;


-- Dimension de sucursales con nombre de provincia y region
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.dim_sucursal
COMMENT 'Sucursales con provincia y region geografica'
AS
SELECT
  id_comercio,
  id_bandera,
  id_sucursal,
  nombre,
  tipo,
  localidad,
  codigo_postal,
  provincia AS provincia_codigo,
  CASE provincia
    WHEN 'AR-C' THEN 'Ciudad Autonoma de Buenos Aires'
    WHEN 'AR-B' THEN 'Buenos Aires'
    WHEN 'AR-K' THEN 'Catamarca'
    WHEN 'AR-H' THEN 'Chaco'
    WHEN 'AR-U' THEN 'Chubut'
    WHEN 'AR-X' THEN 'Cordoba'
    WHEN 'AR-W' THEN 'Corrientes'
    WHEN 'AR-E' THEN 'Entre Rios'
    WHEN 'AR-P' THEN 'Formosa'
    WHEN 'AR-Y' THEN 'Jujuy'
    WHEN 'AR-L' THEN 'La Pampa'
    WHEN 'AR-F' THEN 'La Rioja'
    WHEN 'AR-M' THEN 'Mendoza'
    WHEN 'AR-N' THEN 'Misiones'
    WHEN 'AR-Q' THEN 'Neuquen'
    WHEN 'AR-R' THEN 'Rio Negro'
    WHEN 'AR-A' THEN 'Salta'
    WHEN 'AR-J' THEN 'San Juan'
    WHEN 'AR-D' THEN 'San Luis'
    WHEN 'AR-Z' THEN 'Santa Cruz'
    WHEN 'AR-S' THEN 'Santa Fe'
    WHEN 'AR-G' THEN 'Santiago del Estero'
    WHEN 'AR-V' THEN 'Tierra del Fuego'
    WHEN 'AR-T' THEN 'Tucuman'
    ELSE 'Sin dato'
  END AS provincia_nombre,
  CASE
    WHEN NOT ubicacion_consistente THEN 'Revisar ubicación'
    WHEN provincia = 'AR-C' THEN 'CABA'
    WHEN provincia IN ('AR-B', 'AR-X', 'AR-S', 'AR-E', 'AR-L') THEN 'Pampeana'
    WHEN provincia IN ('AR-A', 'AR-Y', 'AR-K', 'AR-F', 'AR-G', 'AR-T') THEN 'NOA'
    WHEN provincia IN ('AR-H', 'AR-W', 'AR-P', 'AR-N') THEN 'NEA'
    WHEN provincia IN ('AR-M', 'AR-J', 'AR-D') THEN 'Cuyo'
    WHEN provincia IN ('AR-Q', 'AR-R', 'AR-U', 'AR-Z', 'AR-V') THEN 'Patagonia'
    ELSE 'Sin dato'
  END AS region,
  latitud,
  longitud,
  ubicacion_consistente
FROM sepa.silver.sucursales;


-- Hecho: precio vigente de cada producto en cada sucursal
CREATE OR REFRESH MATERIALIZED VIEW sepa.gold.fct_precios_vigentes
COMMENT 'Precio vigente de cada producto en cada sucursal, con cadena y ubicacion'
AS
SELECT
  p.id_comercio,
  p.id_bandera,
  p.id_sucursal,
  p.id_producto,
  p.es_ean,
  upper(p.descripcion) AS descripcion,
  p.marca,
  p.cantidad_presentacion,
  p.unidad_presentacion,
  p.precio_lista,
  least(p.precio_lista, p.precio_promo1, p.precio_promo2) AS precio_minimo_con_promo,
  p.__START_AT AS vigente_desde,
  c.bandera_nombre,
  s.nombre AS sucursal_nombre,
  s.localidad,
  s.provincia_nombre,
  s.region
FROM sepa.silver.precios_historial p
LEFT JOIN sepa.gold.dim_comercio c
  ON p.id_comercio = c.id_comercio
 AND p.id_bandera = c.id_bandera
LEFT JOIN sepa.gold.dim_sucursal s
  ON p.id_comercio = s.id_comercio
 AND p.id_bandera = s.id_bandera
 AND p.id_sucursal = s.id_sucursal
WHERE p.__END_AT IS NULL;